#include "utils.h"
#include "blockStorage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using namespace BlockStorage;
using namespace std;

const int MAX_NUM_RETRIES = 6;
const int INITIAL_BACKOFF_MS = 50;
const int MULTIPLIER = 2;

static string clientIdentifier;
std::thread notificationThread;

static int currentServerIdx = 0;

int switchServerConnection();
string getServerName(int index);

struct CacheInfo {
    bool isCached;
    string data;
    struct timespec lastRefreshTs;
    CacheInfo() : isCached(false) { get_time(&lastRefreshTs); }
    bool isStale();
    void cacheData(const string &data);
    void invalidateCache() { isCached = false; }
};

vector<CacheInfo> cacheMap(numBlocks);

class BlockStorageClient {
   public:
    BlockStorageClient(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}

    int rpc_read(uint32_t address, string &buf, uint32_t size,
                 uint32_t offset) {
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s : Address = %d\n", __func__, address);
        }

        ReadResult rres;

        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        int error_code = 0;
        while (!isDone) {
            ClientContext clientContext;
            ReadRequest rr;
            rr.set_address(address);
            rr.set_offset(offset);
            rr.set_size(size);
            rr.set_requirecache(isCachingEnabled);
            rr.set_identifier(clientIdentifier);

            // Set timeout for API
            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            clientContext.set_wait_for_ready(true);
            clientContext.set_deadline(deadline);

            Status status = stub_->rpc_read(&clientContext, rr, &rres);
            error_code = status.error_code();
            currentBackoff *= MULTIPLIER;

            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED ||
                numRetriesLeft-- == 0) {
                isDone = true;
            } else {
                if (debugMode <= DebugLevel::LevelInfo) {
                    printf("%s \t : Timed out to contact server.\n", __func__);
                    cout << __func__
                         << "\t : Error code = " << status.error_message()
                         << endl;
                }
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Retrying to " 
                         << getServerName(currentServerIdx) << " with timeout (ms) of "
                         << currentBackoff << " MULTIPLIER = " << MULTIPLIER << endl;
                }
            }
        }

        // case where server is not responding/offline
        if (error_code == grpc::StatusCode::DEADLINE_EXCEEDED) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed because of timeout!" << endl;
            }
            return SERVER_OFFLINE_ERROR_CODE;
        }

        if (rres.err() == 0) {
            buf = rres.buffer();
            return rres.bytesread();
        } else {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed with read error returned as "
                     << rres.err() << endl;
            }
            return -rres.err();
        }
    }

    int rpc_write(uint32_t address, const string &buf, uint32_t offset) {
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s : Address = %d\n", __func__, address);
        }

        WriteResult wres;

        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        int error_code = 0;
        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_address(address);
            wreq.set_buffer(buf);
            wreq.set_offset(offset);
            wreq.set_size(BLOCK_SIZE_BYTES);
            wreq.set_identifier(clientIdentifier);

            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            ctx.set_wait_for_ready(true);
            ctx.set_deadline(deadline);

            Status status = stub_->rpc_write(&ctx, wreq, &wres);
            error_code = status.error_code();
            currentBackoff *= MULTIPLIER;
            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED ||
                numRetriesLeft-- == 0) {
                isDone = true;
            } else {
                if (debugMode <= DebugLevel::LevelInfo) {
                    printf("%s \t : Timed out to contact server.\n", __func__);
                    cout << __func__
                         << "\t : Error code = " << status.error_message()
                         << endl;
                }
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Retrying to " 
                         << getServerName(currentServerIdx) << " with timeout (ms) of "
                         << currentBackoff << endl;
                }
            }
        }

        if (error_code == grpc::StatusCode::DEADLINE_EXCEEDED) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed because of timeout!" << endl;
            }
            return SERVER_OFFLINE_ERROR_CODE;
        }

        if (wres.err() == 0) {
            return wres.nbytes();
        } else {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed with write error returned as "
                     << wres.err() << endl;
            }
            return -wres.err();
        }
    }

    Status rpc_subscribeForNotifications() {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        ClientContext context;
        ClientCacheNotify notifyMessage;
        SubscribeForNotifications subReq;

        subReq.set_identifier(clientIdentifier);

        std::unique_ptr<ClientReader<ClientCacheNotify> > reader(
            stub_->rpc_subscribeForNotifications(&context, subReq));

        while (reader->Read(&notifyMessage)) {
            int address = notifyMessage.address();
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__
                     << "\t : Invalidate cache with address: " << address
                     << endl;
            }
            if (isCachingEnabled) {
                cacheMap[address].invalidateCache();
            }
        }

        Status status = reader->Finish();
        if (!status.ok()) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
        return status;
    }

    void rpc_unSubscribeForNotifications() {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        ClientContext context;
        SubscribeForNotifications unSubReq, unSubRes;

        unSubReq.set_identifier(clientIdentifier);

        Status status = stub_->rpc_unSubscribeForNotifications(
            &context, unSubReq, &unSubRes);

        if (status.ok() == false) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
    }

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

struct ServerInfo {
    string address;
    BlockStorageClient *connection;

    void init(string address) {
        this->address = address;
        cout << __func__ << "\t : Initialize connection from client to "
             << address << endl;
        this->connection = new BlockStorageClient(grpc::CreateChannel(
            address.c_str(), grpc::InsecureChannelCredentials()));
    }
};

static vector<ServerInfo> serverInfos;

void cacheInvalidationListener() {
    cout << __func__ << "\t : Listening for notifications.." << endl;
    Status status = grpc::Status::OK;
    do {
        status = (serverInfos[currentServerIdx].connection)
                     ->rpc_subscribeForNotifications();
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Error code = " << status.error_code()
                 << " and message = " << status.error_message() << endl;
        }
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
            currentServerIdx = (currentServerIdx + 1) % serverInfos.size();
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Changing to backup server "
                     << getServerName(currentServerIdx) << endl;
            }
        }
    } while (grpc::StatusCode::UNAVAILABLE == status.error_code());
    cout << __func__ << "\t : Stopped listening for notifications now." << endl;
}

void initServerInfo(vector<string> addresses) {
    for (string address : addresses) {
        ServerInfo serverInfo;
        serverInfo.init(address);
        serverInfos.push_back(serverInfo);
    }
    notificationThread = (std::thread(cacheInvalidationListener));
    msleep(1);
}

bool CacheInfo::isStale() {
    struct timespec curTime;
    get_time(&curTime);
    double timeDiff = get_time_diff(&lastRefreshTs, &curTime);
    isCached = isCached && (timeDiff < stalenessLimit);
    return !isCached;
}

void CacheInfo::cacheData(const string &data) {
    this->data = data;
    isCached = true;
    get_time(&lastRefreshTs);
}

int switchServerConnection() {
    cout << __func__ << "\t : Primary server is offline!" << endl;

    // Commenting out as this functionality is being handled by subscription
    // service now. But discuss it with team - **TODO**.
    //currentServerIdx = (currentServerIdx + 1) % serverInfos.size();

    //notificationThread.join();
    //notificationThread = (std::thread(cacheInvalidationListener));
    msleep(1);

    cout << __func__ << "\t : Changing to server at "
         << getServerName(currentServerIdx) << endl;
}

string getServerName(int index) {
    if (index < 0 || index > serverInfos.size()) {
        return "";
    }
    return serverInfos[index].address;
}