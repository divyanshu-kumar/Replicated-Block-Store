#define FUSE_USE_VERSION 30

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

#include "client.h"

enum DebugLevel { LevelInfo = 0, LevelError = 1, LevelNone = 2 };

const DebugLevel debugMode = LevelNone;

inline void get_time(struct timespec *ts);
inline double get_time_diff(struct timespec *before, struct timespec *after);
int benchmark();

static struct options {
    BlockStorageClient *blockStorageClient;
    int show_help;
} options;

#define OPTION(t, p) \
    { t, offsetof(struct options, p), 1 }

static void show_help(const char *progname) {
    printf("%s \n", __func__);
    std::cout
        << "usage: " << progname
        << " [-s -d] <mountpoint> [--server=ip:port, Default = localhost]\n\n";
}

static int client_read(off_t offset) {
    return options.blockStorageClient->rpc_read(offset);
}

static int client_write(const char *buf,
                        off_t offset) {
    return options.blockStorageClient->rpc_write(buf, offset);
}

int main(int argc, char *argv[]) {
    // ios::sync_with_stdio(false);
    // cin.tie(nullptr);
    // cout.tie(nullptr);
    srand(time(NULL));
    
    if (debugMode <= DebugLevel::LevelInfo) {
        printf("%s \t: %s\n", __func__, argv[0]);
    }

    
    std::string server_address = "localhost:50051";
    // if (argc > 2) {
    //     size_t pos = std::string(argv[argc - 2]).rfind("--server=", 0);
    //     if (pos == 0) {
    //         isServerArgPassed = true;
    //         server_address = std::string(argv[argc - 2]).substr(string("--server=").length());
    //     }

    //     pos = std::string(argv[argc - 1]).rfind("--crash=", 0);
    //     if (pos == 0) {
    //         isCrashSiteArgPassed = true;
    //         crashSite = stoi(std::string(argv[argc - 1]).substr(string("--crash=").length()));
    //     }        
    // } else if (argc > 1) {
    //     size_t pos = std::string(argv[argc - 1]).rfind("--server=", 0);
    //     if (pos == 0) {
    //         isServerArgPassed = true;
    //         server_address = std::string(argv[argc - 1])
    //                              .substr(string("--server=").length());
    //     }
    // }

    printf("%s \t: Connecting to server at %s...\n", __func__,
           server_address.c_str());

    // if (isServerArgPassed) {
    //     argc -= 1;
    // }
    // if (isCrashSiteArgPassed) {
    //     argc -= 1;
    // }

    options.blockStorageClient = new BlockStorageClient(grpc::CreateChannel(
        server_address.c_str(), grpc::InsecureChannelCredentials()));

    // if (options.show_help) {
    //     show_help(argv[0]);
    //     assert(fuse_opt_add_arg(&args, "--help") == 0);
    //     args.argv[0] = (char *)"";
    // }

    return benchmark();
}

inline void get_time(struct timespec *ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

inline double get_time_diff(struct timespec *before, struct timespec *after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
}

int benchmark() {
    return 0;
}