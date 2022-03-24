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
#include <thread>

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

const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;

const int MAX_SIZE_BYTES = one_gb;
const int BLOCK_SIZE_BYTES = 4 * one_kb;

string currentWorkDir,
        dataDirPath;

inline void get_time(struct timespec* ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}
inline double get_time_diff(struct timespec* before, struct timespec* after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
}
string getCurrentWorkingDir();
void makeFolderAndBlocks();
void makeBlocks(int block_id_start, int block_id_end);
void createOneBlock(int block_id, const string &dataDirPath);


class BlockStorageServer final : public BlockStorageService::Service {

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

        string blockAddress = dataDirPath + "/" + to_string(wr->address());
        int fd = open(blockAddress.c_str(), O_WRONLY);
        if (fd == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        int res = pwrite(fd, wr->buffer().c_str(), wr->size(), wr->offset());

        fsync(fd);

        if (res == -1) {
            reply->set_err(errno);
            printf("%s \n", __func__);
            perror(strerror(errno));
            return Status::OK;
        }

        reply->set_nbytes(res);
        reply->set_err(0);

        if (fd > 0) close(fd);

        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    BlockStorageServer service;
    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    srand(time(NULL));

    makeFolderAndBlocks();

    RunServer();

    return 0;
}

void makeFolderAndBlocks() {
    currentWorkDir = getCurrentWorkingDir();

    printf("%s : Current Working Dir found as %s \n Trying to"
           " make blocks in ./data/ folder.\n", __func__, currentWorkDir.c_str());

    string dataFolderPath = currentWorkDir + "/data";

    struct stat buffer;

    if (stat(dataFolderPath.c_str(), &buffer) == 0) {
        printf("%s : Folder %s exists.\n", __func__, dataFolderPath.c_str());
    } else {
        int res = mkdir(dataFolderPath.c_str(), 0777);
        if (res == 0) {
            printf("%s : Folder %s created successfully!\n", __func__,
                   dataFolderPath.c_str());
        } else {
            printf("%s : Failed to create folder %s!\n", __func__,
                   dataFolderPath.c_str());
        }
    }

    dataDirPath = dataFolderPath;

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
    else {
        // printf("%s : Creating block %d\n", __func__, block_id);
    }
    fputs(data, fp);
    fclose(fp);
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