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

#include "server.h"
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


// class BlockStorageServer final : public BlockStorageService::Service {

//     Status rpc_read(SserverContext* context, const ReadRequest* rr,
//                         ReadResult* reply) override {
//         printf("%s : Address = %u\n", __func__, rr->address());

//         char* buf = new char[rr->size() + 1];
        
//         string blockAddress = dataDirPath + "/" + to_string(rr->address());
        
//         int fd = open(blockAddress.c_str(), O_RDONLY);
        
//         if (fd == -1) {
//             reply->set_err(errno);
//             printf("%s \n", __func__);
//             perror(strerror(errno));
//             return Status::OK;
//         }

//         int res = pread(fd, buf, rr->size(), rr->offset());
//         if (res == -1) {
//             reply->set_err(errno);
//             printf("%s \n", __func__);
//             perror(strerror(errno));
//             return Status::OK;
//         }

//         reply->set_bytesread(res);
//         reply->set_buffer(buf);
//         reply->set_err(0);

//         if (fd > 0) close(fd);
//         free(buf);

//         return Status::OK;
//     }

//     Status rpc_write(ServerContext* context, const WriteRequest* wr,
//                          WriteResult* reply) override {
//         printf("%s : Address = %u\n", __func__, wr->address());
        
//         int res  = logWriteTransaction(wr->address());

//         int bytes_written_local = localWrite(wr);

//         if(bytes_written_local == -1){
//             reply->set_err(errno);
//             reply->set_nbytes(0);
//             printf("%s \n", __func__);
//             perror(strerror(errno));
//         } else {
//             reply->set_nbytes(bytes_written_local);
//             reply->set_err(0);
//         }

//         if (bytes_written_local != -1) {
//             // intitate write inside backup 
//             int bytes_written_backup = serverReplication->rpc_write(address, buf);
//             if (bytes_written_local != bytes_written_backup){
//                 // TODO : enter a log locally for later sync up
//             }
//         }
        
//         res  = unLogWriteTransaction(wr->address());

//         if (res == -1) {
//             printf("%s : Error : Failed to unlog the write the transaction.", 
//                     __func__);
//         }
        
//         return Status::OK;
//     }
// };

void RunServer() {
    ServerBuilder builder;

    builder.AddListeningPort(my_address, grpc::InsecureServerCredentials());

    builder.RegisterService(serverReplication);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << my_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    srand(time(NULL));

    string argumentString;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }
        size_t pos = argumentString.find("--role=");
        
        if (pos != string::npos) {
            pos += string("--role=").length();
            size_t endPos = pos + argumentString.substr(pos).find(' ');
            role = argumentString.substr(pos, endPos - pos);
            if (!(role == "primary" || role == "backup")) {
                printf("Please enter either primary or backup in --role=\n");
                cout<<role<< "####"<<endl;
                return 0;
            }
        }

        pos = argumentString.find("--my_address=") + string("--my_address=").length();
        
        if (pos != string::npos) {
            size_t endPos = pos + argumentString.substr(pos).find(' ');
            my_address = argumentString.substr(pos, endPos - pos);
            // TODO : Validate IP
        }
        
        pos = argumentString.find("--other_address=") + string("--other_address=").length();
        
        if (pos != string::npos) {
            size_t endPos = pos + argumentString.substr(pos).find(' ');
            other_address = argumentString.substr(pos, endPos - pos);
            // TODO : Validate IP
        }
    }

    cout << "My Address = " << my_address << " Other Address = "<< other_address << endl;
    switch(role[0]) {
        case 'p': {
            serverReplication = new ServerReplication(grpc::CreateChannel(
                other_address.c_str(), grpc::InsecureChannelCredentials()));
            break;
        }
        case 'b': {
            serverReplication = new ServerReplication();
            break;
        }
    }

    // serverReplication = new ServerReplication(grpc::CreateChannel(
    //         other_address.c_str(), grpc::InsecureChannelCredentials()));

    makeFolderAndBlocks();

    rollbackUncommittedWrites();

    RunServer();

    return 0;
}

int makeFolder(const string & folderPath) {
    struct stat buffer;

    if (stat(folderPath.c_str(), &buffer) == 0) {
        printf("%s : Folder %s exists.\n", __func__, folderPath.c_str());
    } else {
        int res = mkdir(folderPath.c_str(), 0777);
        if (res == 0) {
            printf("%s : Folder %s created successfully!\n", __func__,
                   folderPath.c_str());
        } else {
            printf("%s : Failed to create folder %s!\n", __func__,
                   folderPath.c_str());
            return -1;
        }
    }

    return 0;
}

void makeFolderAndBlocks() {
    currentWorkDir = getCurrentWorkingDir();

    printf("%s : Current Working Dir found as %s \n Trying to"
           " make blocks in ./data/ folder.\n", __func__, currentWorkDir.c_str());

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

void rollbackUncommittedWrites() {
    
    struct dirent *entry;
    DIR *dir = opendir(writeTxLogsDirPath.c_str());
   
    if (dir == NULL) {
        return;
    }
    while ((entry = readdir(dir)) != NULL) {
        string fileName(entry->d_name);
        if(fileName == "." || fileName == "..")
            continue;
        cout << fileName << endl;
        string sourcePath = writeTxLogsDirPath + "/" + fileName;
        string destPath = dataDirPath + "/" + fileName;
        string command = "mv " + sourcePath + " " + destPath;
        int res = system(command.c_str());
        if(res != 0){
            printf("Error: %s failed to rename \n", __func__);
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

int logWriteTransaction(int address){
    string destPath = writeTxLogsDirPath + "/" + to_string(address);
    string sourcePath = dataDirPath + "/" + to_string(address);
    cout<<"Dest = "<<destPath<<" Source = "<<sourcePath<<endl;
    cout<<"Role is "<<role<<endl;
    int res = copyFile(destPath.c_str(), sourcePath.c_str());
    if(res == -1){
        printf("%s: Error: \n", __func__);
        perror(strerror(errno));   
    }
    return res;
}

int unLogWriteTransaction(int address){
    string filePath = writeTxLogsDirPath + "/" + to_string(address);
    cout<<"Dest = "<<filePath<<endl;
    cout<<"Role is "<<role<<endl;
    int res = unlink(filePath.c_str());
    if(res == -1){
        printf("%s: Error: \n", __func__);
        perror(strerror(errno));   
    }
    return res;
}

int localWrite(const WriteRequest* wr) {
    string blockAddress = dataDirPath + "/" + to_string(wr->address());
    int fd = open(blockAddress.c_str(), O_WRONLY);
    if (fd == -1) {
        return fd;
    }

    int res = pwrite(fd, wr->buffer().c_str(), wr->size(), wr->offset());
    fsync(fd); 
    close(fd);

    return res;
}

int copyFile(const char *to, const char *from) {
    int fd_to, fd_from;
    char buf[4096];
    ssize_t nread;
    int saved_errno;

    fd_from = open(from, O_RDONLY);
    if (fd_from < 0)
        return -1;

    fd_to = open(to, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_to < 0)
        goto out_error;

    while (nread = read(fd_from, buf, sizeof buf), nread > 0)
    {
        char *out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);

            if (nwritten >= 0)
            {
                nread -= nwritten;
                out_ptr += nwritten;
            }
            else if (errno != EINTR)
            {
                goto out_error;
            }
        } while (nread > 0);
    }

    if (nread == 0)
    {
        if (close(fd_to) < 0)
        {
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
    if (fd_to >= 0)
        close(fd_to);

    errno = saved_errno;
    return -1;
}
