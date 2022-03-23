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

using namespace BlockStorage;

using namespace std;

#define MAX_NUM_RETRIES (1)
#define INITIAL_BACKOFF_MS (50)
#define MULTIPLIER (1.5)

class BlockStorageClient {
   public:
    BlockStorageClient(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}

    int rpc_read(off_t address) {
        ReadResult rres;

        bool isDone = false;
        unsigned int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        while (!isDone) {
            ClientContext clientContext;
            ReadRequest rr;
            rr.set_address(address);

            // Set timeout for API
            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            clientContext.set_wait_for_ready(true);
            clientContext.set_deadline(deadline);

            Status status = stub_->rpc_read(&clientContext, rr, &rres);
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
        
        char buf[4096];

        if (rres.err() == 0) {
            strcpy(buf, rres.buffer().c_str());
            return rres.bytesread();
        } else {
            return -rres.err();
        }
    }

    int rpc_write(const char* buf, off_t address) {
        WriteResult wres;

        bool isDone = false;
        unsigned int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_address(address);
            wreq.set_buffer(buf);

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

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};