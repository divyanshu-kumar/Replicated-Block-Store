#include <dirent.h>
#include <fcntl.h>
#include <grpc++/grpc++.h>
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

#include "blockStorage.grpc.pb.h"

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

inline void get_time(struct timespec* ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}
inline double get_time_diff(struct timespec* before, struct timespec* after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
}

class BlockStorageServer final : public BlockStorageService::Service {

    Status rpc_read(ServerContext* context, const ReadRequest* rr,
                        ReadResult* reply) override {
        // char path[512];
        char* buf = new char[4096];
        // translatePath(rr->path().c_str(), path);
        // // cout<<"[DEBUG] : afsfuse_read: "<<path<<endl;

        // int fd = open(path, O_RDONLY);
        // // cout<<"[DEBUG] : afsfuse_read: fd "<<fd<<endl;
        // if (fd == -1) {
        //     reply->set_err(errno);
        //     printf("%s \n", __func__);
        //     perror(strerror(errno));
        //     return Status::OK;
        // }
        cout << "Read Call" << endl;

        int res = 0;//pread(fd, buf, rr->size(), rr->offset());
        if (res == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        reply->set_bytesread(0);//res);
        reply->set_buffer(buf);
        reply->set_err(0);

        // if (fd > 0) close(fd);
        free(buf);

        return Status::OK;
    }

    Status rpc_write(ServerContext* context, const WriteRequest* wr,
                         WriteResult* reply) override {
        // printf("%s \n", __func__);
        char path[512] = {0};
        // translatePath(wr->path().c_str(), path);
        // int fd = open(path, O_WRONLY);
        // cout<<"[DEBUG] : afsfuse_write: path "<<path<<endl;
        // cout<<"[DEBUG] : afsfuse_write: fd "<<fd<<endl;
        // if (fd == -1) {
        //     reply->set_err(errno);
        //     printf("%s \n", __func__);
        //     perror(strerror(errno));
        //     return Status::OK;
        // }

        cout << "RPC Write called" << endl;

        int res = 0;//pwrite(fd, wr->buffer().c_str(), wr->size(), wr->offset());
        // cout<<"[DEBUG] : afsfuse_write: res"<<res<<endl;

        // fsync(fd);

        if (res == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        reply->set_nbytes(res);
        reply->set_err(0);

        // if (fd > 0) close(fd);

        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    BlockStorageServer service;
    // printf("%s \n", __func__);
    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    // ios::sync_with_stdio(false);
    // cin.tie(nullptr);
    // cout.tie(nullptr);
    srand(time(NULL));
    RunServer();
    return 0;
}