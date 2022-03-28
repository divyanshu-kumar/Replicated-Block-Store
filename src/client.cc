#include "client.h"

int run_application();

int switchServerConnection() {
    cout << __func__ << " : Primary server is offline!" << endl;
    
    currentServerIdx = (currentServerIdx+1)%serverInfos.size();
    
    notificationThread.join();
    notificationThread = (std::thread(cacheInvalidationListener));
    msleep(1);
    
    printf("%s \t: Changing to server at %s...\n", __func__,
           serverInfos[currentServerIdx].address.c_str());
}

static int client_read(uint32_t address, string & buf) {
    int res = 0;

    if (isCachingEnabled) {
        if (cacheMap[address].isCached && !cacheMap[address].isStale()) {
            cout << __func__ << " : Cached data found for file " << address << endl;
            buf = cacheMap[address].data;
            return res = buf.length();
        }
        else {
            // cout << __func__ << "Cached data not found for file " << address << endl;
        }
    }
    res = (serverInfos[currentServerIdx].connection)->rpc_read(address, buf);
    
    if (res == SERVER_OFFLINE_ERROR_CODE) {
        switchServerConnection();
        res = (serverInfos[currentServerIdx].connection)->rpc_read(address, buf);
        if(res < 0){
            cout << __func__ << " : Both servers are offline!" << endl;
            return -1;
        }
    }
    if (isCachingEnabled) {
        // cout << __func__ << "Cached data successfully for file " << address << endl;
        cacheMap[address].cacheData(buf);
    }
    return res;
}

static int client_write(uint32_t address, const string & buf) {
    if (isCachingEnabled) {
        cout << __func__ << " : Invalidating cache for file " << address << endl;
        cacheMap[address].invalidateCache();
    }
    int res = (serverInfos[currentServerIdx].connection)->rpc_write(address, buf);
    if(res == SERVER_OFFLINE_ERROR_CODE) {
        switchServerConnection();
        int res = (serverInfos[currentServerIdx].connection)->rpc_write(address, buf);
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
    }

    if (addresses.empty()) {
        addresses = {"localhost:50051", "localhost:50053"};
    }

    generateClientIdentifier();

    initServerInfo(addresses);

    printf("%s \t: Connecting to server at %s...\n", __func__,
           serverInfos[currentServerIdx].address.c_str());

    return run_application();
}


int run_application() {
    vector<pair<double, int>> readTimes, writeTimes;

    string write_data = string(4096, 'x');

    int totalBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

    for (int i = 0; i < 100; i++) {
        string buf;
        uint32_t address = i % 5;//max(0, rand()) % totalBlocks;
        
        struct timespec read_start, read_end;
        get_time(&read_start);
        
        int num_bytes_read = client_read(address, buf);
        
        get_time(&read_end);
        readTimes.push_back(make_pair(get_time_diff(&read_start, &read_end), address));
        
        if (num_bytes_read != 4096) {
            printf("Didn't read 4k bytes from this file! Instead read %d bytes!\n", num_bytes_read);
        }

        // Testing cache
        get_time(&read_start);
        
        num_bytes_read = client_read(address, buf);
        
        get_time(&read_end);
        readTimes.push_back(make_pair(get_time_diff(&read_start, &read_end), address));
        
        if (num_bytes_read != 4096) {
            printf("Didn't read 4k bytes from this file! Instead read %d bytes!\n", num_bytes_read);
        }

        msleep(100 * max(0, rand() % 5));

        address = i % 5;//max(0, rand()) % totalBlocks;
        
        struct timespec write_start, write_end;
        get_time(&write_start);
        
        int num_bytes_write = client_write(address, write_data);
        
        get_time(&write_end);
        writeTimes.push_back(make_pair(get_time_diff(&write_start, &write_end), address));
        
        if (num_bytes_write != 4096) {
            printf("Didn't write 4k bytes to this file!\n");
        }
        
        msleep(max(0, rand() % 10));
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