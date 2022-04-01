#include "utils.h"
#include "fs_utils.h"
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
const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 10;
const int MULTIPLIER = 2;
string currentWorkDir, dataDirPath, writeTxLogsDirPath;

static string role, other_address, my_address;

static vector<std::mutex> blockLock(numBlocks);
unordered_map<int, struct timespec> backupLastWriteTime;

thread heartbeatThread;
bool heartbeatShouldRun;
bool isBackupAvailable;

void    rollbackUncommittedWrites();
int     logWriteTransaction(int address);
int     unLogWriteTransaction(int address);
void    backupBlockRead(int address, bool isReadAligned);
struct timespec* max_time(struct timespec *t1, struct timespec *t2);

struct BackupOutOfSync {
    mutex m_lock;
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

    ReadCache readCache;

    ServerReplication() { }

    ServerReplication(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) { }

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
                    if (debugMode <= DebugLevel::LevelError) {
                        printf("%s \t : Backup server seems offline\n", __func__);
                    }
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
            // backupBlockRead(rr->address(), isReadAligned);
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
            int blockId = rr->address() + idx;

            if(readCache.isEnabled() && readCache.isPresent(blockId)) {
                // load buffer from inMemoryCache and avoid fopen call
                std::strcpy(buf + (idx * BLOCK_SIZE_BYTES), readCache.getCached(blockId).c_str());
                res = BLOCK_SIZE_BYTES;
            } else {
                // not present inMemoryCache, so perform file open and cache it as well
                string blockAddress = dataDirPath + "/" + to_string(blockId);
                int fd = open(blockAddress.c_str(), O_RDONLY);

                if (fd == -1) {
                    reply->set_err(errno);
                    printf("%s \t : ", __func__);
                    perror(strerror(errno));
                    return Status::OK;
                }

                res = pread(fd, buf + (idx * BLOCK_SIZE_BYTES), BLOCK_SIZE_BYTES, 0);
                if (res == -1) {
                    reply->set_err(errno);
                    printf("%s \t : ", __func__);
                    perror(strerror(errno));
                    return Status::OK;
                }
                if (fd > 0) close(fd);
            }
        }

        buf[(int)rr->size()] = '\0';

        reply->set_bytesread(res);
        reply->set_buffer(buf);
        reply->set_err(0);
        delete[] buf;

        // blockLock[rr->address()].unlock();  TODO

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
                 << wr->offset() << ", role " << currentRole 
                 << ", isBackupAvailable = " << isBackupAvailable << endl;
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

        int bytes_written_local = localWrite(wr->address(), wr->offset(), wr->buffer(), dataDirPath,
                                             readCache);

        if (currentRole == "backup") {
            // struct timespec currentTime;
            // get_time(&currentTime);
            // backupLastWriteTime[wr->address()] = currentTime;
            // if (!isAlignedWrite) {
            //     backupLastWriteTime[wr->address() + 1] = currentTime;
            // }
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
                if (isBackupAvailable && backupSyncState.isOutOfSync) {
                    backupSyncState.sync();
                }
            } else {
                int bytes_written_backup = isBackupAvailable ? 
                    this->rpc_write(wr->address(), wr->buffer(), wr->offset()) : 0;
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
            if (!message.empty() && message == "OK") {
                cout << __func__ << "\t : Successfully sync'd with backup server!" << endl;
                backupSyncState.sync();
                isBackupAvailable = true;
            }
            else {
                cout << __func__ << "\t : Heartbeat connection broken." << endl;
            }
        }

        Status status = reader->Finish();
        if (!status.ok()) {
            // if (debugMode <= DebugLevel::LevelInfo) {
            //     cout << __func__ << "\t : Status returned as "
            //          << status.error_message() << endl;
            // }
        }
    }

    Status rpc_heartbeatListener(ServerContext* context,
                                 const Heartbeat* heartbeatMessage,
                                 ServerWriter<Heartbeat>* writer) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        Heartbeat heartbeatResult;
        heartbeatResult.set_msg("OK");
        bool isFirstMsgSent(false);

        while (true) {
            if (!isFirstMsgSent) {
                writer->Write(heartbeatResult);
                isFirstMsgSent = true;
            }
            msleep(500);
        }

        return Status::OK;
    }

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

static ServerReplication* serverReplication;

void BackupOutOfSync::logOutOfSync(const int address) {
    lock_guard<mutex> guard(m_lock);
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Out of sync address = " << address << endl;
    }
    isOutOfSync = true;
    outOfSyncBlocks[address] = true;
}

int BackupOutOfSync::sync() {
    lock_guard<mutex> guard(m_lock);
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
        res = localRead(blockIdx, buf, dataDirPath);
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

    int res = copyFile(destPath, sourcePath);
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


void runHeartbeat() {
    if (debugMode <= DebugLevel::LevelError) {
        cout << __func__ << "\t : Starting heartbeat service!" << endl;
    }
    while (heartbeatShouldRun) {
        serverReplication->rpc_heartbeatSender();
        isBackupAvailable = false;
        if (role == "backup") {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__
                     << "\t : Backup is switching to Primary mode now." << endl;
            }
            role = "primary";
            backupLastWriteTime.clear();
        }
        msleep(500);
    }
}

void backupBlockRead(int address, bool isReadAligned) {
    bool shouldWait = false;
    auto it1 = backupLastWriteTime.find(address);
    auto it2 = it1;
    if (!isReadAligned) {
        it2 = backupLastWriteTime.find(address + 1);
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