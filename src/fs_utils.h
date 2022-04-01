#include <experimental/filesystem>

// do a force copy of file from source to destination
int copyFile(const string &to, const string &from){
    std::experimental::filesystem::path sourceFile = from;
    std::experimental::filesystem::path target = to;

    try
    {
        copy_file(sourceFile, target, std::experimental::filesystem::copy_options::overwrite_existing);
    }
    catch (std::exception& e) // Not using fs::filesystem_error since std::bad_alloc can throw too.  
    {
        std::cout << e.what() << "Error occured while copying the file " << from << " to destinatoon " << to << endl;
        return -1;
    }
    return 0;
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
    if (fd_to < 0) {
        goto out_error;
    }

    while (nread = read(fd_from, buf, sizeof buf), nread > 0) {
        char *out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);
            if (nwritten >= 0) {
                nread -= nwritten;
                out_ptr += nwritten;
            }
            else if (errno != EINTR) {
                goto out_error;
            }
        } while (nread > 0);
    }

    if (nread == 0) {
        if (close(fd_to) < 0) {
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

int localWrite(const int &address, const int &offset, const string &buffer, const string &dataDirPath,
                bool inMemoryCacheEnable, unordered_map<int, string> &inMemoryCachedBlocks) {
    bool isWriteAligned = checkIfOffsetIsAligned(offset);
    string blockAddress = dataDirPath + "/" + to_string(address);

    int fd = open(blockAddress.c_str(), O_RDWR);
    if (fd == -1) {
        return fd;
    }

    int startIdx = isWriteAligned ? 0 : (offset % BLOCK_SIZE_BYTES);
    int res = pwrite(fd, buffer.substr(0, (BLOCK_SIZE_BYTES - startIdx)).c_str(), BLOCK_SIZE_BYTES - startIdx, startIdx);
    if(res < 0){
        cout<<__func__ << " : Error in pwrite for address " << address << " and offset: " << offset << endl;
        close(fd);
        return -1;
    }
    fsync(fd);
    char* buf = new char[one_kb * 4 + 1];
    if(inMemoryCacheEnable){

        res = pread(fd, buf, one_kb * 4, 0);
        if(res < 0){
            cout<<__func__ << " : Error while inMemoryCaching, pread for address " << address << " and offset: " << offset << endl;
            return -1;  // TODO: check if this is needed
        }
        buf[one_kb * 4] = '\0';

        if(inMemoryCachedBlocks.find(address) == inMemoryCachedBlocks.end()){
            inMemoryCachedBlocks.insert({address, std::string(buf)});
        } else {
            // just change the part of inMemoryCach
            inMemoryCachedBlocks[address] = std::string(buf);
        }
    }
    close(fd);
    
    if (!isWriteAligned) {
        blockAddress = dataDirPath + "/" + to_string(address+1);

        fd = open(blockAddress.c_str(), O_WRONLY);
        if (fd == -1) {
            return fd;
        }

        res = pwrite(fd, buffer.substr((BLOCK_SIZE_BYTES - startIdx)).c_str(), startIdx, 0);
        fsync(fd); 
        if(inMemoryCacheEnable){
            int res = pread(fd, buf, one_kb * 4, 0);
            if(res < 0){
                cout<<__func__ << " : Error while inMemoryCaching, pread for address " << address << " and offset: " << offset << endl;
                return -1;  // TODO: check if this is needed
            }
            buf[one_kb * 4] = '\0';

            if(inMemoryCachedBlocks.find(address+1) == inMemoryCachedBlocks.end()){
                inMemoryCachedBlocks.insert({address+1, std::string(buf)});
            } else {
                // just change the part of inMemoryCach
                inMemoryCachedBlocks[address+1] = std::string(buf);
            }
        }
        close(fd);
    }
    delete[] buf;
    return BLOCK_SIZE_BYTES;
}

int localRead(const int address, char * buf, const string &dataDirPath) {
    string blockAddress = dataDirPath + "/" + to_string(address);
    int fd = open(blockAddress.c_str(), O_RDONLY);
        
    if (fd == -1) {
        printf("%s \n", __func__);
        perror(strerror(errno));
        return -1;
    }

    int res = pread(fd, buf, one_kb * 4, 0);
    if (res == -1) {
        printf("%s \n", __func__);
        perror(strerror(errno));
        return -1;
    }

    close(fd);
    
    return 0;
}