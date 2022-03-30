#include "client.h"

int run_application(bool isReadOnlyMode);


bool checkIfOffsetIsAligned(uint32_t &offset){
    return offset % BLOCK_SIZE_BYTES == 0;
}

bool cacheStalenessValidation(const vector<uint32_t> &addressVector){
    for(auto address : addressVector){
        if(!cacheMap[address].isCached || cacheMap[address].isStale())
            return false;
    }
    return true;
}

static int client_read(uint32_t offset, string & buf, bool readFromBackup = false) {
    // printf("%s : Address = %d\n", __func__, offset);
    int res = 0;

    uint32_t address = offset  / BLOCK_SIZE_BYTES;
    vector<uint32_t> addressVector = {address};
    bool isAlignedRead = checkIfOffsetIsAligned(offset);
    uint32_t readSize = BLOCK_SIZE_BYTES;
    if(!isAlignedRead){
        addressVector.push_back(address+1);
        readSize = 2 * BLOCK_SIZE_BYTES;
    }

    if (isCachingEnabled) {
        if (cacheStalenessValidation(addressVector)) {
            cout << __func__ << " : Cached data found for file with Address " << address << endl;
            int startIdx = offset % BLOCK_SIZE_BYTES;
            buf = cacheMap[address].data;
            if(!isAlignedRead)
                buf += cacheMap[address+1].data;
            buf = buf.substr(startIdx, BLOCK_SIZE_BYTES);
            return res = buf.length();
        }
        else {
            // cout << __func__ << "Cached data not found for file " << address << endl;
        }
    }

    int serverToContactIdx = readFromBackup ? (currentServerIdx + 1) % 2 : currentServerIdx;

    res = (serverInfos[serverToContactIdx].connection)->rpc_read(address, buf, readSize, offset);
    cout << __func__ << " res: " << res << " from server id "<<serverToContactIdx << endl;
    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (!readFromBackup) {
            switchServerConnection();
        }

        res = (serverInfos[currentServerIdx].connection)->rpc_read(address, buf, readSize, offset);
        cout << __func__ << " res: " << res << " from server id "<<currentServerIdx << endl;
        if(res < 0){
            cout << __func__ << " : Both servers are offline!" << endl;
            return -1;
        }
    }
    if (isCachingEnabled) {
        // cout << __func__ << "Cached data successfully for file " << address << endl;
        cacheMap[address].cacheData(buf.substr(0,BLOCK_SIZE_BYTES));
        if(!isAlignedRead) {
            cacheMap[address+1].cacheData(buf.substr(BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES));
            buf = buf.substr(offset, BLOCK_SIZE_BYTES);
        }
    }
    return res;
}

static int client_write(uint32_t offset, const string & buf) {

    uint32_t address = offset  / BLOCK_SIZE_BYTES;
    bool isAlignedWrite = checkIfOffsetIsAligned(offset);

    if (isCachingEnabled) {
        cout << __func__ << " : Invalidating cache for file " << address << endl;
        cacheMap[address].invalidateCache();
        if(!isAlignedWrite)
            cacheMap[address+1].invalidateCache();
    }
    
    int res = (serverInfos[currentServerIdx].connection)->rpc_write(address, buf, offset);
    if(res == SERVER_OFFLINE_ERROR_CODE) {
        switchServerConnection();
        int res = (serverInfos[currentServerIdx].connection)->rpc_write(address, buf, offset);
        if(res < 0){
            cout << __func__ << " : Both servers are offline!" << endl;
            return -1;
        }
    }
    return res;
}

int main(int argc, char *argv[]) {
    srand(time(NULL));
    
    if (debugMode <= DebugLevel::LevelInfo) {
        printf("%s \t: %s\n", __func__, argv[0]);
    }

    vector<string> addresses;

    bool isServerArgPassed = false;
    bool isCrashSiteArgPassed = false;
    int crashSite = 0;
    string argumentString;
    bool isReadOnlyMode = false;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }

        string address;

        do {
            string addressArg = "--address" + to_string(addresses.size() + 1) + "=";
            address = parseArgument(argumentString, addressArg);
            if (!address.empty() && !isIPValid(address)) {
                cout << "Enter a valid IP address and try again!"
                     << " Invalid IP = " << address << endl;
                return 0;
            }
            if (!address.empty()) {
                addresses.push_back(address);
            }
        } while (!address.empty());

        isReadOnlyMode = !parseArgument(argumentString, "--readOnly=").empty();
        cout << "Read Only = " << isReadOnlyMode << endl;
    }

    if (addresses.empty()) {
        addresses = {"localhost:50051", "localhost:50053"};
    }

    generateClientIdentifier();

    initServerInfo(addresses);

    printf("%s \t: Connecting to server at %s...\n", __func__,
           serverInfos[currentServerIdx].address.c_str());

    return run_application(isReadOnlyMode);
}


int run_application(bool isReadOnlyMode) {
    vector<pair<double, int>> readTimes, writeTimes;

    string write_data = string(4096, 'x');

    int totalBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(0,50);

    const int NUM_RUNS = 500;
    for (int i = 0; i < NUM_RUNS; i++) {
        string buf;
        uint32_t address = i % NUM_RUNS;//max(0, rand()) % totalBlocks;
        
        struct timespec read_start, read_end;
        get_time(&read_start);
        
        int num_bytes_read = client_read(address * BLOCK_SIZE_BYTES, buf, isReadOnlyMode);
        
        get_time(&read_end);
        readTimes.push_back(make_pair(get_time_diff(&read_start, &read_end), address));
        
        if (num_bytes_read != 4096) {
            printf("Didn't read 4k bytes from this file! Instead read %d bytes!\n", num_bytes_read);
        }
        
        msleep((int)dist6(rng));

        // Testing cache
        get_time(&read_start);
        
        num_bytes_read = client_read(address * BLOCK_SIZE_BYTES, buf, isReadOnlyMode);
        
        get_time(&read_end);
        readTimes.push_back(make_pair(get_time_diff(&read_start, &read_end), address));
        
        if (num_bytes_read != 4096) {
            printf("Didn't read 4k bytes from this file! Instead read %d bytes!\n", num_bytes_read);
        }

        msleep((int)dist6(rng));

        address = i % NUM_RUNS;//max(0, rand()) % totalBlocks;
        
        struct timespec write_start, write_end;
        get_time(&write_start);
        
        int num_bytes_write = 0;
        
        if (!isReadOnlyMode)
            num_bytes_write = client_write(address * BLOCK_SIZE_BYTES, write_data);
        
        get_time(&write_end);
        writeTimes.push_back(make_pair(get_time_diff(&write_start, &write_end), address));
        
        if (!isReadOnlyMode && num_bytes_write != 4096) {
            printf("Didn't write 4k bytes to this file!\n");
        }
        
        msleep((int)dist6(rng));
    }

    double meanReadTime = 0;
    for (auto & readTime : readTimes) {
        meanReadTime += readTime.first;
    }
    meanReadTime /= readTimes.size();

    double meanWriteTime = 0;
    for (auto & writeTime : writeTimes) {
        meanWriteTime += writeTime.first;
    }
    meanWriteTime /= writeTimes.size();
  
    sort(readTimes.begin(), readTimes.end());
    sort(writeTimes.begin(), writeTimes.end());

    double medianReadTime = readTimes[readTimes.size() / 2].first;
    double medianWriteTime = writeTimes[writeTimes.size() / 2].first;

    printf("%s : *****STATS (milliseconds) *****\n"
            "meanRead   = %f \t meanWrite   = %f \n"
            "medianRead = %f \t medianWrite = %f\n"
            "minRead    = %f \t minWrite    = %f\n"
            "minAddress = %d \t minAddress  = %d\n"
            "maxRead    = %f \t maxWrite    = %f\n"
            "maxAddress = %d \t maxAddress  = %d\n",
            __func__, meanReadTime, meanWriteTime,
            medianReadTime, medianWriteTime,
            readTimes.front().first, writeTimes.front().first,
            readTimes.front().second, writeTimes.front().second,
            readTimes.back().first, writeTimes.back().first,
            readTimes.back().second, writeTimes.back().second);

    (serverInfos[currentServerIdx].connection)->rpc_unSubscribeForNotifications();
    notificationThread.join();

    return 0;
}