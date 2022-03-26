#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <numeric>
#include <grpc++/grpc++.h>
#include <sys/stat.h>
#include <sys/types.h>
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

enum DebugLevel { LevelInfo = 0, LevelError = 1, LevelNone = 2 };
const DebugLevel debugMode = LevelNone;

const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;
const int MAX_SIZE_BYTES = one_gb;
const int BLOCK_SIZE_BYTES = 4 * one_kb;
const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 50;
const int MULTIPLIER = 1.5;

class BlockStorageClient {
   public:
    BlockStorageClient(std::shared_ptr<Channel> channel)
        : stub_(BlockStorageService::NewStub(channel)) {}

    int rpc_read(uint32_t address, string & buf) {
        // printf("%s : Address = %d\n", __func__, address);

        ReadResult rres;

        bool isDone = false;
        unsigned int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        while (!isDone) {
            ClientContext clientContext;
            ReadRequest rr;
            rr.set_address(address);
            rr.set_offset(0);
            rr.set_size(4096);
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
        
        if (rres.err() == 0) {
            buf = rres.buffer();
            return rres.bytesread();
        } else {
            return -rres.err();
        }
    }

    int rpc_write(uint32_t address, const string & buf) {
        // printf("%s : Address = %d\n", __func__, address);

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

   private:
    std::unique_ptr<BlockStorageService::Stub> stub_;
};

inline void get_time(struct timespec *ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

inline double get_time_diff(struct timespec *before, struct timespec *after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
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