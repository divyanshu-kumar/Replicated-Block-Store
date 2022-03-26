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
#include <numeric>

#include "client.h"

enum DebugLevel { LevelInfo = 0, LevelError = 1, LevelNone = 2 };

const DebugLevel debugMode = LevelNone;

const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;

const int MAX_SIZE_BYTES = one_gb;
const int BLOCK_SIZE_BYTES = 4 * one_kb;

inline void get_time(struct timespec *ts);
inline double get_time_diff(struct timespec *before, struct timespec *after);
int run_application();
int msleep(long msec);

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

static int client_read(uint32_t address, string & buf) {
    return options.blockStorageClient->rpc_read(address, buf);
}

static int client_write(uint32_t address, const string & buf) {
    return options.blockStorageClient->rpc_write(address, buf);
}

int main(int argc, char *argv[]) {
    srand(time(NULL));
    
    if (debugMode <= DebugLevel::LevelInfo) {
        printf("%s \t: %s\n", __func__, argv[0]);
    }

    
    std::string server_address = "localhost:50051";

    bool isServerArgPassed = false;
    bool isCrashSiteArgPassed = false;
    int crashSite = 0;
    if (argc > 2) {
        size_t pos = std::string(argv[argc - 2]).rfind("--server=", 0);
        if (pos == 0) {
            isServerArgPassed = true;
            server_address = std::string(argv[argc - 2]).substr(string("--server=").length());
        }

        pos = std::string(argv[argc - 1]).rfind("--crash=", 0);
        if (pos == 0) {
            isCrashSiteArgPassed = true;
            crashSite = stoi(std::string(argv[argc - 1]).substr(string("--crash=").length()));
        }        
    } 
    else if (argc > 1) {
        size_t pos = std::string(argv[argc - 1]).rfind("--server=", 0);
        if (pos == 0) {
            isServerArgPassed = true;
            server_address = std::string(argv[argc - 1])
                                 .substr(string("--server=").length());
        }
    }

    printf("%s \t: Connecting to server at %s...\n", __func__,
           server_address.c_str());

    options.blockStorageClient = new BlockStorageClient(grpc::CreateChannel(
        server_address.c_str(), grpc::InsecureChannelCredentials()));

    return run_application();
}

int run_application() {
    vector<double> readTimes, writeTimes;

    string write_data = string(4096, 'x');

    int totalBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

    for (int i = 0; i < 500; i++) {
        string buf;
        uint32_t address = max(0, rand()) % totalBlocks;
        
        struct timespec read_start, read_end;
        get_time(&read_start);
        
        int num_bytes_read = client_read(address, buf);
        
        get_time(&read_end);
        readTimes.push_back(get_time_diff(&read_start, &read_end));
        
        if (num_bytes_read != 4096) {
            printf("Didn't read 4k bytes from this file!\n");
        }

        address = max(0, rand()) % totalBlocks;
        
        struct timespec write_start, write_end;
        get_time(&write_start);
        
        int num_bytes_write = client_write(address, write_data);
        
        get_time(&write_end);
        writeTimes.push_back(get_time_diff(&write_start, &write_end));
        
        if (num_bytes_write != 4096) {
            printf("Didn't write 4k bytes to this file!\n");
        }
        
        msleep(max(0, rand() % 10));
    }

    double meanReadTime = std::accumulate(readTimes.begin(), readTimes.end(), 0.0) 
        / static_cast<double>(readTimes.size());

    double meanWriteTime = std::accumulate(writeTimes.begin(), writeTimes.end(), 0.0) 
        / static_cast<double>(writeTimes.size());
    
    sort(readTimes.begin(), readTimes.end());
    sort(writeTimes.begin(), writeTimes.end());
    
    double medianReadTime = readTimes[readTimes.size() / 2];
    double medianWriteTime = writeTimes[writeTimes.size() / 2];

    printf("%s : *****STATS (milliseconds) *****\n"
            "meanRead   = %f \t meanWrite   = %f \n"
            "medianRead = %f \t medianWrite = %f\n"
            "minRead    = %f \t minWrite    = %f\n"
            "maxRead    = %f \t maxWrite    = %f\n",
            __func__, meanReadTime, meanWriteTime, medianReadTime, medianWriteTime,
            readTimes.front(), writeTimes.front(), readTimes.back(), writeTimes.back());
    return 0;
}

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

