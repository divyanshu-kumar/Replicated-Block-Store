#include <grpc++/grpc++.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <vector>

#include "blockStorage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;


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

#define MAX_NUM_RETRIES (5)
#define INITIAL_BACKOFF_MS (50)
#define MULTIPLIER (1.5)
 
const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;

const int MAX_SIZE_BYTES = one_gb;
const int BLOCK_SIZE_BYTES = 4 * one_kb;

string  currentWorkDir,
        dataDirPath,
        writeTxLogsDirPath;

static string   role,
                other_address,
                my_address;

inline void get_time(struct timespec* ts);
inline double get_time_diff(struct timespec* before, struct timespec* after);

string getCurrentWorkingDir();
void makeFolderAndBlocks();
void makeBlocks(int block_id_start, int block_id_end);
void createOneBlock(int block_id, const string &dataDirPath);
void rollbackUncommittedWrites();
int copyFile(const char *to, const char *from);
int logWriteTransaction(int address);
int unLogWriteTransaction(int address);
int localWrite(const WriteRequest* wr);

class ServerReplication final : public BlockStorageService::Service {
   public:

    ServerReplication(){
        
    }


    ServerReplication(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}

    
    int rpc_write(uint32_t address, const string & buf) {
        printf("%s : rpc_write with address and buf Address = %d\n", __func__, address);

        WriteResult wres;

        bool isDone = false;
        unsigned int numRetriesLeft = MAX_NUM_RETRIES;
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
                isDone = true;
            }
            else {
                printf("%s \t : Timed out to contact server. Retrying...\n", __func__);
            }
        }

        if (isDone == false) {
            printf("%s \t : Timed out to contact server.\n", __func__);
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
            printf("%s : Primary calling write on Backup!\n", __func__);
            // intitate write inside backup 
            int bytes_written_backup = this->rpc_write(wr->address(), wr->buffer());
            if (bytes_written_local != bytes_written_backup){
                // TODO : enter a log locally for later sync up
            }
        }
        
        res  = unLogWriteTransaction(wr->address());

        if (res == -1) {
            printf("%s : Error : Failed to unlog the write the transaction.", 
                    __func__);
        }
        
        return Status::OK;
    }

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

static ServerReplication *serverReplication;