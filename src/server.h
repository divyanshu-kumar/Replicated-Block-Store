#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <grpc++/grpc++.h>
#include <signal.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

enum DebugLevel { LevelInfo = 0, LevelError = 1, LevelNone = 2 };
const DebugLevel debugMode = LevelError;

const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;
const int MAX_SIZE_BYTES = one_gb / 10;
const int BLOCK_SIZE_BYTES = 4 * one_kb;
const int MAX_NUM_RETRIES = 4;
const int INITIAL_BACKOFF_MS = 10;
const int MULTIPLIER = 1;
const int numBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;
int backReadStalenessLimit = 10;

string currentWorkDir, dataDirPath, writeTxLogsDirPath;

static string role, other_address, my_address;

static vector<std::mutex> blockLock(numBlocks);
unordered_map<int, struct timespec> backupLastWriteTime;

thread heartbeatThread;
bool heartbeatShouldRun;

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
bool    checkIfOffsetIsAligned(unsigned int offset);
void    backupBlockRead(int address, bool isReadAligned);

struct timespec* max_time(struct timespec *t1, struct timespec *t2);

struct BackupOutOfSync {
    bool isOutOfSync;
    vector<bool> outOfSyncBlocks;

    BackupOutOfSync() : isOutOfSync(false), outOfSyncBlocks(numBlocks, false) {}

    void logOutOfSync(const int address);

    int sync();
} backupSyncState;

struct NotificationInfo {
    unordered_map<string, bool> subscriberShouldRun;
    unordered_map<int, unordered_set<string>> subscribedClients;
    unordered_map<string, ServerWriter<ClientCacheNotify>*> clientWriters;

    void Subscribe(int address, const string& id) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : for address " << address << " id " << id
                 << " with role " << role << endl;
        }
        subscribedClients[address].insert(id);
    }

    void UnSubscribe(int address, const string& id) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : for address " << address << " id " << id
                 << " with role " << role << endl;
        }
        subscribedClients[address].erase(id);
    }

    void AddClient(const string& clientId,
                   ServerWriter<ClientCacheNotify>* clientWriter) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << clientId << " with role "
                 << role << endl;
        }
        clientWriters[clientId] = clientWriter;
        subscriberShouldRun[clientId] = true;
    }

    void RemoveClient(const string& clientId) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << clientId << " with role "
                 << role << endl;
        }
        clientWriters.erase(clientId);
        subscriberShouldRun[clientId] = false;
    }

    bool ShouldKeepAlive(const string& clientId) {
        return subscriberShouldRun[clientId];
    }

    void Notify(int address, const string& writerClientId) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address " << address << " with role "
                 << role << endl;
        }
        auto clientIds = subscribedClients[address];
        if (clientIds.empty()) {
            return;
        }
        for (auto clientId : clientIds) {
            if (clientWriters.find(clientId) == clientWriters.end()) {
                continue;
            }
            if (writerClientId != clientId) {
                NotifySingleClient(clientId, address);
            }
            UnSubscribe(address, clientId);
        }
    }

    void NotifySingleClient(const string& id, const int& address) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << id << ", address " << address
                 << endl;
        }
        try {
            ServerWriter<ClientCacheNotify>* writer = clientWriters[id];
            ClientCacheNotify notifyReply;
            notifyReply.set_address(address);

            writer->Write(notifyReply);
        } catch (const std::exception& ex) {
            std::ostringstream sts;
            if (debugMode <= DebugLevel::LevelError) {
                sts << __func__ << "\t : Error contacting client " << id
                    << endl;
            }
            std::cerr << sts.str() << endl;
        }
    }
};

static NotificationInfo notificationManager;

class ServerReplication final : public BlockStorageService::Service {
   public:
    ServerReplication() {}

    ServerReplication(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}

    int rpc_write(uint32_t address, const string& buf, uint32_t offset) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address " << address << ", offset "
                 << offset << ", role " << role << endl;
        }

        WriteResult wres;
        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_address(address);
            wreq.set_buffer(buf);
            wreq.set_offset(offset);
            wreq.set_size(BLOCK_SIZE_BYTES);

            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            ctx.set_wait_for_ready(true);
            ctx.set_deadline(deadline);

            Status status = stub_->rpc_write(&ctx, wreq, &wres);
            currentBackoff *= MULTIPLIER;
            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED ||
                numRetriesLeft-- == 0) {
                if (numRetriesLeft <= 0) {
                    printf(
                        "%s \t : Server seems offline. Error Code = %d, "
                        "numRetriesLeft = %d\n",
                        __func__, status.error_code(), numRetriesLeft);
                    return -1;
                }
                isDone = true;
            } else {
                // printf("%s \t : Timed out to contact server. Retrying...\n",
                // __func__);
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
        const string currentRole = role;
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address " << rr->address() << ", offset "
                 << rr->offset() << ", role " << currentRole << endl;
        }
        bool isReadAligned = checkIfOffsetIsAligned(rr->offset());
        if (currentRole == "primary") {
            blockLock[rr->address()].lock();
            if (!isReadAligned) {
                blockLock[rr->address() + 1].lock();
            }
        } else {
            backupBlockRead(rr->address(), isReadAligned);
        }

        bool isCachingRequested =
            rr->requirecache() && (currentRole == "primary");

        if (isCachingRequested) {
            string clientId = rr->identifier();
            notificationManager.Subscribe(rr->address(), clientId);
            if (!isReadAligned) {
                notificationManager.Subscribe(rr->address() + 1, clientId);
            }
        }

        char* buf = new char[rr->size() + 1];
        int res = 0;

        for (int idx = 0; idx < rr->size() / BLOCK_SIZE_BYTES; idx++) {
            string blockAddress =
                dataDirPath + "/" + to_string(rr->address() + idx);
            int fd = open(blockAddress.c_str(), O_RDONLY);

            if (fd == -1) {
                reply->set_err(errno);
                printf("%s \t : ", __func__);
                perror(strerror(errno));
                return Status::OK;
            }

            res =
                pread(fd, buf + (idx * BLOCK_SIZE_BYTES), BLOCK_SIZE_BYTES, 0);
            if (res == -1) {
                reply->set_err(errno);
                printf("%s \t : ", __func__);
                perror(strerror(errno));
                return Status::OK;
            }
            if (fd > 0) close(fd);
        }

        buf[(int)rr->size()] = '\0';

        reply->set_bytesread(res);
        reply->set_buffer(buf);
        reply->set_err(0);
        free(buf);

        blockLock[rr->address()].unlock();

        if (currentRole == "primary") {
            blockLock[rr->address()].unlock();
            if (!isReadAligned) {
                blockLock[rr->address() + 1].unlock();
            }
        }

        return Status::OK;
    }

    Status rpc_write(ServerContext* context, const WriteRequest* wr,
                     WriteResult* reply) override {
        const string currentRole = role;
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address " << wr->address() << ", offset "
                 << wr->offset() << ", role " << currentRole << endl;
        }

        bool isAlignedWrite = checkIfOffsetIsAligned(wr->offset());

        lock_guard<mutex> guard(blockLock[wr->address()]);
        if (!isAlignedWrite) {
            blockLock[wr->address() + 1].lock();
        }

        if (currentRole == "primary") {
            notificationManager.Notify(wr->address(), wr->identifier());
            if (!isAlignedWrite) {
                notificationManager.Notify(wr->address() + 1, wr->identifier());
            }
        }

        int res = logWriteTransaction(wr->address());
        if (!isAlignedWrite) {
            res = logWriteTransaction(wr->address() + 1);
        }

        int bytes_written_local = localWrite(wr);

        if (currentRole == "backup") {
            struct timespec currentTime;
            get_time(&currentTime);
            backupLastWriteTime[wr->address()] = currentTime;
            if (!isAlignedWrite) {
                backupLastWriteTime[wr->address() + 1] = currentTime;
            }
        }

        if (bytes_written_local == -1) {
            reply->set_err(errno);
            reply->set_nbytes(0);
            printf("%s \t : ", __func__);
            perror(strerror(errno));
        } else {
            reply->set_nbytes(bytes_written_local);
            reply->set_err(0);
        }

        if ((bytes_written_local != -1) && (currentRole == "primary")) {
            if (backupSyncState.isOutOfSync) {
                backupSyncState.logOutOfSync(wr->address());
                if (!isAlignedWrite) {
                    backupSyncState.logOutOfSync(wr->address() + 1);
                }
                backupSyncState.sync();
            } else {
                int bytes_written_backup =
                    this->rpc_write(wr->address(), wr->buffer(), wr->offset());
                if (bytes_written_local != bytes_written_backup) {
                    backupSyncState.logOutOfSync(wr->address());
                    if (!isAlignedWrite) {
                        backupSyncState.logOutOfSync(wr->address() + 1);
                    }
                }
            }
        }

        res = unLogWriteTransaction(wr->address());
        if (!isAlignedWrite) {
            res = unLogWriteTransaction(wr->address() + 1);
        }

        if (res == -1) {
            printf("%s \t : Error : Failed to unlog the write the transaction.",
                   __func__);
        }

        if (!isAlignedWrite) {
            blockLock[wr->address() + 1].unlock();
        }

        return Status::OK;
    }

    Status rpc_subscribeForNotifications(
        ServerContext* context,
        const SubscribeForNotifications* subscribeMessage,
        ServerWriter<ClientCacheNotify>* writer) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << subscribeMessage->identifier()
                 << ", role " << role << endl;
        }

        const string& clientId = subscribeMessage->identifier();
        notificationManager.AddClient(clientId, writer);

        while (notificationManager.ShouldKeepAlive(clientId)) {
            msleep(500);
        }

        return Status::OK;
    }

    Status rpc_unSubscribeForNotifications(
        ServerContext* context, const SubscribeForNotifications* unSubReq,
        SubscribeForNotifications* reply) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << unSubReq->identifier()
                 << ", role " << role << endl;
        }
        const string& clientId = unSubReq->identifier();

        notificationManager.RemoveClient(clientId);

        reply->set_identifier("Unsubscribed!");

        return Status::OK;
    }

    void rpc_heartbeatSender() {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }

        ClientContext context;
        Heartbeat heartbeatReq, heartbeatRes;

        heartbeatReq.set_msg("");

        std::unique_ptr<ClientReader<Heartbeat>> reader(
            stub_->rpc_heartbeatListener(&context, heartbeatReq));

        while (reader->Read(&heartbeatRes)) {
            auto message = heartbeatRes.msg();
            cout << __func__ << "\t : Heartbeat connection broken." << endl;
        }

        Status status = reader->Finish();
        if (!status.ok()) {
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
    }

    Status rpc_heartbeatListener(ServerContext* context,
                                 const Heartbeat* heartbeatMessage,
                                 ServerWriter<Heartbeat>* writer) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }

        while (true) {
            msleep(500);
        }

        return Status::OK;
    }

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

static ServerReplication* serverReplication;

void BackupOutOfSync::logOutOfSync(const int address) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Out of sync address = " << address << endl;
    }
    isOutOfSync = true;
    outOfSyncBlocks[address] = true;
}

int BackupOutOfSync::sync() {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << endl;
    }
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

        res = serverReplication->rpc_write(blockIdx, buf, 0);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__
                     << "\t : Failed to sync files to backup server." << endl;
            }
            return -1;
        }

        outOfSyncBlocks[blockIdx] = false;
    }

    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Successfully sync'd changed files!" << endl;
    }

    isOutOfSync = false;

    return 0;
}

int logWriteTransaction(int address) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Address = " << address << endl;
    }
    string destPath = writeTxLogsDirPath + "/" + to_string(address);
    string sourcePath = dataDirPath + "/" + to_string(address);

    int res = copyFile(destPath.c_str(), sourcePath.c_str());
    if (res == -1) {
        if (debugMode <= DebugLevel::LevelError) {
            printf("%s\t : Error: Dest Path = %s, Source Path = %s\n", __func__,
                   destPath.c_str(), sourcePath.c_str());
        }
        perror(strerror(errno));
    }

    return res;
}

int unLogWriteTransaction(int address) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Address = " << address << endl;
    }
    string filePath = writeTxLogsDirPath + "/" + to_string(address);

    int res = unlink(filePath.c_str());
    if (res == -1) {
        if (debugMode <= DebugLevel::LevelError) {
            printf("%s\t : Error: File Path = %s\n", __func__,
                   filePath.c_str());
        }
        perror(strerror(errno));
    }

    return res;
}

int localWrite(const WriteRequest* wr) {
    bool isWriteAligned = checkIfOffsetIsAligned(wr->offset());
    string blockAddress = dataDirPath + "/" + to_string(wr->address());

    int fd = open(blockAddress.c_str(), O_WRONLY);
    if (fd == -1) {
        return fd;
    }

    int startIdx = isWriteAligned ? 0 : (wr->offset() % BLOCK_SIZE_BYTES);
    int res = pwrite(
        fd, wr->buffer().substr(0, (BLOCK_SIZE_BYTES - startIdx)).c_str(),
        BLOCK_SIZE_BYTES - startIdx, startIdx);
    fsync(fd);
    close(fd);

    if (!isWriteAligned) {
        blockAddress = dataDirPath + "/" + to_string(wr->address() + 1);

        fd = open(blockAddress.c_str(), O_WRONLY);
        if (fd == -1) {
            return fd;
        }

        res = pwrite(fd,
                     wr->buffer().substr((BLOCK_SIZE_BYTES - startIdx)).c_str(),
                     startIdx, 0);
        fsync(fd);
        close(fd);
    }
    return BLOCK_SIZE_BYTES;
}

int makeFolder(const string& folderPath) {
    struct stat buffer;

    if (stat(folderPath.c_str(), &buffer) == 0) {
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s\t : Folder %s exists.\n", __func__, folderPath.c_str());
        }
    } else {
        int res = mkdir(folderPath.c_str(), 0777);
        if (res == 0) {
            printf("%s\t : Folder %s created successfully!\n", __func__,
                   folderPath.c_str());
        } else {
            printf("%s\t : Failed to create folder %s!\n", __func__,
                   folderPath.c_str());
            return -1;
        }
    }

    return 0;
}

void makeFolderAndBlocks() {
    currentWorkDir = getCurrentWorkingDir();

    if (debugMode <= DebugLevel::LevelInfo) {
        printf(
            "%s\t : Current Working Dir found as %s \n Trying to"
            " make blocks in ./data/ folder.\n",
            __func__, currentWorkDir.c_str());
    }

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
        workers.push_back(
            std::thread(makeBlocks, block_id_start, block_id_end));
    }

    for (auto& th : workers) {
        th.join();
    }
}

void makeBlocks(int block_id_start, int block_id_end) {
    for (int numBlock = block_id_start; numBlock <= block_id_end; numBlock++) {
        createOneBlock(numBlock, dataDirPath);
    }
}

void createOneBlock(int block_id, const string& dataDirPath) {
    const string blockPath = dataDirPath + "/" + to_string(block_id);
    struct stat buffer;

    if (stat(blockPath.c_str(), &buffer) == 0) {
        return;  // block already exists
    }

    static const string init_block_data = string(4 * one_kb, '0');
    static const char* data = init_block_data.c_str();

    FILE* fp = fopen(blockPath.c_str(), "a");
    if (fp < 0) {
        if (debugMode <= DebugLevel::LevelError) {
            printf("%s\t : Error creating block %d\n", __func__, block_id);
        }
    }

    fputs(data, fp);
    fclose(fp);
}

void rollbackUncommittedWrites() {
    DIR* dir = opendir(writeTxLogsDirPath.c_str());
    if (dir == NULL) {
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string fileName(entry->d_name);
        if (fileName == "." || fileName == "..") continue;
        string sourcePath = writeTxLogsDirPath + "/" + fileName;
        string destPath = dataDirPath + "/" + fileName;
        string command = "mv " + sourcePath + " " + destPath;
        int res = system(command.c_str());
        if (res != 0) {
            if (debugMode <= DebugLevel::LevelError) {
                printf("%s : Error - failed to rename the file %s \n", __func__,
                       sourcePath.c_str());
            }
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

int copyFile(const char* to, const char* from) {
    int fd_to, fd_from;
    char buf[4096];
    ssize_t nread;
    int saved_errno;

    fd_from = open(from, O_RDONLY);
    if (fd_from < 0) return -1;

    fd_to = open(to, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_to < 0) goto out_error;

    while (nread = read(fd_from, buf, sizeof buf), nread > 0) {
        char* out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);

            if (nwritten >= 0) {
                nread -= nwritten;
                out_ptr += nwritten;
            } else if (errno != EINTR) {
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
    if (fd_to >= 0) close(fd_to);

    errno = saved_errno;
    return -1;
}

string parseArgument(const string& argumentString, const string& option) {
    string value;

    size_t pos = argumentString.find(option);
    if (pos != string::npos) {
        pos += option.length();
        size_t endPos = argumentString.find(' ', pos);
        value = argumentString.substr(pos, endPos - pos);
    }

    return value;
}

bool isRoleValid() { return role == "primary" || role == "backup"; }

bool isIPValid(const string& address) {
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

int localRead(const int address, char* buf) {
    string blockAddress = dataDirPath + "/" + to_string(address);
    int fd = open(blockAddress.c_str(), O_RDONLY);

    if (fd == -1) {
        printf("%s \n", __func__);
        perror(strerror(errno));
        return -1;
    }

    int res = pread(fd, buf, one_kb * 4, 0);
    if (res == -1) {
        printf("%s\t : ", __func__);
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

bool checkIfOffsetIsAligned(unsigned int offset) {
    return offset % BLOCK_SIZE_BYTES == 0;
}

void runHeartbeat() {
    if (debugMode <= DebugLevel::LevelError) {
        cout << __func__ << "\t : Starting heartbeat service!" << endl;
    }
    while (heartbeatShouldRun) {
        serverReplication->rpc_heartbeatSender();
        msleep(5);
        if (role == "backup") {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__
                     << "\t : Backup is switching to Primary mode now." << endl;
            }
            role = "primary";
            backupLastWriteTime.clear();
        }
    }
}

struct timespec* max_time(struct timespec* t1, struct timespec* t2) {
    int diff = get_time_diff(t1, t2);
    if (diff >= 0) {
        return t1;
    }
    return t2;
}

void backupBlockRead(int address, bool isReadAligned) {
    bool shouldWait = false;
    auto it1 = backupLastWriteTime.find(address);
    auto it2 = it1;
    if (!isReadAligned) {
        it2 = backupLastWriteTime.find(address + 1);
        ;
    }
    struct timespec lastWriteTime, *t1(nullptr), *t2(nullptr), currentTime;
    get_time(&currentTime);
    if (it1 != backupLastWriteTime.end()) {
        t1 = &it1->second;
    }
    if (it2 != backupLastWriteTime.end()) {
        t2 = &it2->second;
    }
    if (t1) {
        lastWriteTime = *t1;
    }
    if (t2) {
        lastWriteTime = t1 ? *max_time(t1, t2) : *t2;
    }
    if ((t1 == nullptr) && (t2 == nullptr)) {
        return;
    }
    if (get_time_diff(&lastWriteTime, &currentTime) < backReadStalenessLimit) {
        int diff = backReadStalenessLimit -
                   get_time_diff(&lastWriteTime, &currentTime);
        if (debugMode <= DebugLevel::LevelError) {
            cout << __func__ << "\t : Sleeping for (ms) " << diff << endl;
        }
        msleep(diff);
    }
}