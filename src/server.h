#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <ctime>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <signal.h>
#include <thread>
#include <mutex>
#include <grpc++/grpc++.h>
#include <chrono>
#include <vector>
#include <arpa/inet.h>
#include <unordered_set>
#include <unordered_map>

#include "blockStorage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using namespace BlockStorage;
using namespace std;
 
const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;
const int MAX_SIZE_BYTES = one_gb;
const int BLOCK_SIZE_BYTES = 4 * one_kb;
const int MAX_NUM_RETRIES = 3;
const int INITIAL_BACKOFF_MS = 10;
const int MULTIPLIER = 1.5;
const int numBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

string  currentWorkDir,
        dataDirPath,
        writeTxLogsDirPath;

static string   role,
                other_address,
                my_address;

static vector<std::mutex> blockLock(numBlocks);

void    get_time(struct timespec* ts);
double  get_time_diff(struct timespec* before, struct timespec* after);
string  getCurrentWorkingDir();
void    makeFolderAndBlocks();
void    makeBlocks(int block_id_start, int block_id_end);
void    createOneBlock(int block_id, const string &dataDirPath);
void    rollbackUncommittedWrites();
int     copyFile(const char *to, const char *from);
int     logWriteTransaction(int address);
int     unLogWriteTransaction(int address);
int     localWrite(const WriteRequest* wr);
string  parseArgument(const string & argumentString, const string & option);
bool    isRoleValid();
bool    isIPValid(const string & address);
int     localRead(const int address, char * buf);
int     msleep(long msec);

struct BackupOutOfSync {
    bool isOutOfSync;
    vector<bool> outOfSyncBlocks;
    
    BackupOutOfSync() : isOutOfSync(false),
        outOfSyncBlocks(numBlocks, false) {}

    void logOutOfSync(const int address);

    int sync();
} backupSyncState;

struct NotificationInfo {
    unordered_map<string, bool> subscriberShouldRun;
    unordered_map<int, unordered_set<string>> subscribedClients;
    unordered_map<string, ServerWriter<ClientCacheNotify>*> clientWriters;

    void Subscribe(int address, const string &id) {
        cout << __func__ << " for address " << address << " id " << id << endl;
        subscribedClients[address].insert(id);
    }

    void UnSubscribe(int address, const string &id) {
        cout << __func__ << " for address " << address << " id " << id << endl;
        subscribedClients[address].erase(id);
    }

    void AddClient(const string &clientId, ServerWriter<ClientCacheNotify>* clientWriter) {
        cout << __func__ << " Adding Client " << " id " << clientId << endl;
        clientWriters[clientId] = clientWriter;
        subscriberShouldRun[clientId] = true;
    }

    void RemoveClient(const string &clientId) {
        cout << __func__ << " Removing Client " << " id " << clientId << endl;
        clientWriters.erase(clientId);
        subscriberShouldRun[clientId] = false;
    }

    bool ShouldKeepAlive(const string &clientId) {
        return subscriberShouldRun[clientId];
    }

    void Notify(int address){
        auto clientIds = subscribedClients[address];
        if (clientIds.empty()) {
            return;
        }
        for (auto clientId : clientIds) {
            NotifySingleClient(clientId, address);
            UnSubscribe(address, clientId);
        }
    }

    void NotifySingleClient(const string &id, const int &address){
        cout << __func__ << " Notify Client for address " << address << " id " << id << endl;
        try{
            ServerWriter<ClientCacheNotify>* writer = clientWriters[id];
            ClientCacheNotify notifyReply;
            notifyReply.set_address(address);

            writer->Write(notifyReply);
        } catch(const std::exception& ex){
            std::ostringstream sts;
            sts << "Error contacting client " << id << endl;
            std::cerr << sts.str() << endl;
        }
    }
};

static NotificationInfo notificationManager;

class ServerReplication final : public BlockStorageService::Service {
   public:

    ServerReplication() { }

    ServerReplication(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}
    
    int rpc_write(uint32_t address, const string & buf) {
        // printf("%s : Address = %d\n", __func__, address);
        WriteResult wres;

        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_address(address);
            wreq.set_buffer(buf);
            wreq.set_offset(0);
            wreq.set_size(4096);

            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            ctx.set_wait_for_ready(true);
            ctx.set_deadline(deadline);
            
            Status status = stub_->rpc_write(&ctx, wreq, &wres);
            currentBackoff *= MULTIPLIER;
            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED || numRetriesLeft-- == 0) {
                if (numRetriesLeft <= 0) {
                    printf("%s \t : Server seems offline. Error Code = %d, numRetriesLeft = %d\n", 
                    __func__, status.error_code(), numRetriesLeft);
                    return -1;
                }
                isDone = true;
            }
            else {
                // printf("%s \t : Timed out to contact server. Retrying...\n", __func__);
            }
        }
        
        if (wres.err() == 0) {
            return wres.nbytes();
        } else {
            return -wres.err();
        }
    }

    Status rpc_read(ServerContext* context, const ReadRequest* rr,
                        ReadResult* reply) override {
        printf("%s : Address = %u\n", __func__, rr->address());

        bool isCachingRequested = rr->requirecache();

        if (isCachingRequested) {
            string clientId = rr->identifier();
            notificationManager.Subscribe(rr->address(), clientId);
        }

        char* buf = new char[rr->size() + 1];

        string blockAddress = dataDirPath + "/" + to_string(rr->address());

        int fd = open(blockAddress.c_str(), O_RDONLY);

        if (fd == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        int res = pread(fd, buf, rr->size(), rr->offset());
        if (res == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        buf[min(res, (int)rr->size())] = '\0';
        
        reply->set_bytesread(res);
        reply->set_buffer(buf);
        reply->set_err(0);

        if (fd > 0) close(fd);
        free(buf);

        return Status::OK;
    }

    Status rpc_write(ServerContext* context, const WriteRequest* wr,
                         WriteResult* reply) override {
        printf("%s : Address = %u\n", __func__, wr->address());
        lock_guard<mutex> guard(blockLock[wr->address()]);
        
        if (role == "primary") {
            notificationManager.Notify(wr->address());
        }

        int res  = logWriteTransaction(wr->address());

        int bytes_written_local = localWrite(wr);

        if(bytes_written_local == -1){
            reply->set_err(errno);
            reply->set_nbytes(0);
            printf("%s \n", __func__);
            perror(strerror(errno));
        } else {
            reply->set_nbytes(bytes_written_local);
            reply->set_err(0);
        }

        if (bytes_written_local != -1 && role == "primary") {
            if (backupSyncState.isOutOfSync) {
                backupSyncState.logOutOfSync(wr->address());
                backupSyncState.sync();
            }
            else {
                int bytes_written_backup = this->rpc_write(wr->address(), wr->buffer());
                if (bytes_written_local != bytes_written_backup){ 
                    backupSyncState.logOutOfSync(wr->address());
                }
            }
        }
        
        res  = unLogWriteTransaction(wr->address());

        if (res == -1) {
            printf("%s : Error : Failed to unlog the write the transaction.", 
                    __func__);
        }
        
        return Status::OK;
    }

    Status rpc_subscribeForNotifications(
        ServerContext* context, const SubscribeForNotifications* subscribeMessage,
        ServerWriter<ClientCacheNotify>* writer) override {
        printf("%s : Client = %s\n", __func__, subscribeMessage->identifier().c_str());

        const string & clientId = subscribeMessage->identifier();
        notificationManager.AddClient(clientId, writer);

        while (notificationManager.ShouldKeepAlive(clientId)) {
            msleep(500);
        }

        return Status::OK;
    }

    Status rpc_unSubscribeForNotifications(ServerContext* context, const SubscribeForNotifications* unSubReq,
        SubscribeForNotifications* reply) override
    {
        const string & clientId = unSubReq->identifier();

        notificationManager.RemoveClient(clientId);

        reply->set_identifier("Unsubscribed!");

        return Status::OK;
    }
   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

static ServerReplication *serverReplication;

void BackupOutOfSync::logOutOfSync(const int address) {
    isOutOfSync = true;
    // const int four_kb = 4 * one_kb;
    outOfSyncBlocks[address] = true;
    // if (address % four_kb != 0) {
    //     if ((address / four_kb) + 1 < numBlocks) {
    //         outOfSyncBlocks[(address / four_kb) + 1] = true;
    //     }
    // }
}

int BackupOutOfSync::sync() {
    int res = 0;
    
    for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
        if (outOfSyncBlocks[blockIdx] == false) {
            continue;
        }
        const int four_kb = 4 * one_kb;
        char buf[four_kb + 1];
        res = localRead(blockIdx, buf);
        if (res == -1) {
            return -1;
        }
        
        res = serverReplication->rpc_write(blockIdx, buf);
        if (res < 0) {
            // cout <<  __func__ << " Error sending file" << endl;
            return -1;
        }
        
        outOfSyncBlocks[blockIdx] = false;
    }

    cout <<  __func__ << __LINE__ << " Successfully sync'd changed files, res =  " << res << endl;

    isOutOfSync = false;

    return 0;
}

int logWriteTransaction(int address){
    string destPath = writeTxLogsDirPath + "/" + to_string(address);
    string sourcePath = dataDirPath + "/" + to_string(address);

    int res = copyFile(destPath.c_str(), sourcePath.c_str());
    if(res == -1){
        printf("%s: Error: Dest Path = %s, Source Path = %s\n", __func__,
            destPath.c_str(), sourcePath.c_str());
        perror(strerror(errno));   
    }
    
    return res;
}

int unLogWriteTransaction(int address){
    string filePath = writeTxLogsDirPath + "/" + to_string(address);

    int res = unlink(filePath.c_str());
    if(res == -1){
        printf("%s: Error: File Path = %s\n", __func__,
            filePath.c_str());
        perror(strerror(errno));   
    }

    return res;
}

int localWrite(const WriteRequest* wr) {
    string blockAddress = dataDirPath + "/" + to_string(wr->address());

    int fd = open(blockAddress.c_str(), O_WRONLY);
    if (fd == -1) {
        return fd;
    }

    int res = pwrite(fd, wr->buffer().c_str(), wr->size(), wr->offset());
    fsync(fd); 
    close(fd);
    
    return res;
}

int makeFolder(const string & folderPath) {
    struct stat buffer;

    if (stat(folderPath.c_str(), &buffer) == 0) {
        printf("%s : Folder %s exists.\n", __func__, folderPath.c_str());
    } 
    else {
        int res = mkdir(folderPath.c_str(), 0777);
        if (res == 0) {
            printf("%s : Folder %s created successfully!\n", __func__,
                   folderPath.c_str());
        } 
        else {
            printf("%s : Failed to create folder %s!\n", __func__,
                   folderPath.c_str());
            return -1;
        }
    }

    return 0;
}

void makeFolderAndBlocks() {
    currentWorkDir = getCurrentWorkingDir();

    printf("%s : Current Working Dir found as %s \n Trying to"
           " make blocks in ./data/ folder.\n", __func__, currentWorkDir.c_str());

    dataDirPath = currentWorkDir + "/data";
    writeTxLogsDirPath = currentWorkDir + "/writeTxLogs";

    int res = makeFolder(dataDirPath);
    if (res == -1) {
        return;
    }
    res = makeFolder(writeTxLogsDirPath);
    if (res == -1) {
        return;
    }

    int num_workers = 8;
    std::vector<std::thread> workers;

    int totalBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

    for (int t_id = 0; t_id < num_workers; t_id++) {
        int block_id_start = (totalBlocks / num_workers) * t_id;
        int block_id_end = block_id_start + (totalBlocks / num_workers) - 1;
        if (t_id + 1 == num_workers) {
            block_id_end = totalBlocks - 1;
        }
        workers.push_back(std::thread(makeBlocks, block_id_start, block_id_end));
    }

    for (auto &th : workers) {
        th.join();
    }
}

void makeBlocks(int block_id_start, int block_id_end) {
    for (int numBlock = block_id_start; numBlock <= block_id_end; numBlock++) {
        createOneBlock(numBlock, dataDirPath);
    }
}

void createOneBlock(int block_id, const string &dataDirPath) {
    const string blockPath = dataDirPath + "/" + to_string(block_id);
    struct stat buffer;
    
    if (stat(blockPath.c_str(), &buffer) == 0) {
        return; // block already exists
    }

    static const string init_block_data = string(4 * one_kb, '0');
    static const char *data = init_block_data.c_str();

    FILE *fp = fopen(blockPath.c_str() ,"a");
    if (fp < 0) {
        printf("%s : Error creating block %d\n", __func__, block_id);
    }

    fputs(data, fp);
    fclose(fp);
}

void rollbackUncommittedWrites() {
    DIR *dir = opendir(writeTxLogsDirPath.c_str());
    if (dir == NULL) {
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        string fileName(entry->d_name);
        if(fileName == "." || fileName == "..")
            continue;
        string sourcePath = writeTxLogsDirPath + "/" + fileName;
        string destPath = dataDirPath + "/" + fileName;
        string command = "mv " + sourcePath + " " + destPath;
        int res = system(command.c_str());
        if(res != 0){
            printf("%s : Error - failed to rename the file %s \n", 
                __func__, sourcePath.c_str());
            perror(strerror(errno));
        }
    }

    closedir(dir);
}

string getCurrentWorkingDir() {
    char arg1[20];
    char exepath[PATH_MAX + 1] = {0};

    sprintf(arg1, "/proc/%d/exe", getpid());
    
    int res = readlink(arg1, exepath, 1024);
    std::string s_path(exepath);
    std::size_t lastPos = s_path.find_last_of("/");
    return s_path.substr(0, lastPos);
}

int copyFile(const char *to, const char *from) {
    int fd_to, fd_from;
    char buf[4096];
    ssize_t nread;
    int saved_errno;

    fd_from = open(from, O_RDONLY);
    if (fd_from < 0)
        return -1;

    fd_to = open(to, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_to < 0)
        goto out_error;

    while (nread = read(fd_from, buf, sizeof buf), nread > 0) {
        char *out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);

            if (nwritten >= 0) {
                nread -= nwritten;
                out_ptr += nwritten;
            }
            else if (errno != EINTR) {
                goto out_error;
            }
        } while (nread > 0);
    }

    if (nread == 0) {
        if (close(fd_to) < 0) {
            fd_to = -1;
            goto out_error;
        }
        close(fd_from);

        /* Success! */
        return 0;
    }

  out_error:
    saved_errno = errno;

    close(fd_from);
    if (fd_to >= 0)
        close(fd_to);

    errno = saved_errno;
    return -1;
}

string parseArgument(const string & argumentString, const string & option) {
    string value;
    
    size_t pos = argumentString.find(option);
    if (pos != string::npos) {
        pos += option.length();
        size_t endPos = argumentString.find(' ', pos);
        value = argumentString.substr(pos, endPos - pos);
    }
    
    return value;
}

bool isRoleValid() {
    return role == "primary" || role == "backup";
}

bool isIPValid(const string & address) {
    size_t ipEndPos = address.find(":");
    string ipAddress = address.substr(0, ipEndPos);
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ipAddress.c_str(), &(sa.sin_addr));
    if (result == 0) {
        return false;
    }
    string portAddress = address.substr(ipEndPos + 1);
    int port = stoi(portAddress);
    return (port > 0 && port <= 65535);
}

inline void get_time(struct timespec* ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

inline double get_time_diff(struct timespec* before, struct timespec* after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
}

int localRead(const int address, char * buf) {
    string blockAddress = dataDirPath + "/" + to_string(address);
    int fd = open(blockAddress.c_str(), O_RDONLY);
        
    if (fd == -1) {
        printf("%s \n", __func__);
        perror(strerror(errno));
        return -1;
    }

    int res = pread(fd, buf, one_kb * 4, 0);
    if (res == -1) {
        printf("%s \n", __func__);
        perror(strerror(errno));
        return -1;
    }

    close(fd);
    
    return 0;
}

int msleep(long msec) {
    struct timespec ts;
    int res;
    if (msec < 0) {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}