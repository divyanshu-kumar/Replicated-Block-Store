#include "client.h"

int run_application(bool isReadOnlyMode);
void printStats();

vector<vector<pair<double, int>>> allReadTimes, allWriteTimes;

bool cacheStalenessValidation(const vector<uint32_t> &addressVector, 
    vector<CacheInfo> & cacheMap) {
    for (auto &address : addressVector) {
        if (!cacheMap[address].isCached || cacheMap[address].isStale())
            return false;
    }
    return true;
}

int Client::client_read(uint32_t offset, string &buf) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Offset = " << offset
             << ", ReadFromBackup = " << readFromBackup << endl;
    }

    uint32_t address = offset / BLOCK_SIZE_BYTES;
    vector<uint32_t> addressVector = {address};
    bool isAlignedRead = checkIfOffsetIsAligned(offset);
    uint32_t readSize = BLOCK_SIZE_BYTES;

    if (!isAlignedRead) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Non-Aligned Read Case" << endl;
        }
        addressVector.push_back(address + 1);
        readSize = 2 * BLOCK_SIZE_BYTES;
    }

    if (isCachingEnabled && cacheStalenessValidation(addressVector, cacheMap)) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Cached data found!" << endl;
        }
        int startIdx = offset % BLOCK_SIZE_BYTES;
        buf = cacheMap[address].data;
        if (!isAlignedRead) {
            buf.append(cacheMap[address + 1].data.substr(
                0, BLOCK_SIZE_BYTES - buf.length()));
        }
        return buf.length();
    }

    int serverToContactIdx =
        readFromBackup ? (currentServerIdx + 1) % 2 : currentServerIdx;

    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Contacting server "
             << serverInfos[serverToContactIdx]->address << endl;
    }

    int res = (serverInfos[serverToContactIdx]->connection)
                  ->rpc_read(address, buf, readSize, offset, isCachingEnabled, 
                  clientIdentifier, currentServerIdx);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__
                 << "\t : Read request timed-out, trying to contact other "
                    "server now."
                 << endl;
        }
        res = (serverInfos[currentServerIdx]->connection)
                  ->rpc_read(address, buf, readSize, offset, isCachingEnabled, 
                  clientIdentifier, currentServerIdx);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    if (!readFromBackup && isCachingEnabled) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Caching address : " << address;
        }
        cacheMap[address].cacheData(buf.substr(0, BLOCK_SIZE_BYTES));
        if (!isAlignedRead) {
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << " and " << address + 1;
            }
            cacheMap[address + 1].cacheData(buf.substr(BLOCK_SIZE_BYTES));
            buf = buf.substr(offset, BLOCK_SIZE_BYTES);
        }
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << endl;
        }
    }

    return res;
}

int Client::client_write(uint32_t offset, const string &buf) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Offset = " << offset << endl;
    }

    uint32_t address = offset / BLOCK_SIZE_BYTES;
    bool isAlignedWrite = checkIfOffsetIsAligned(offset);

    if (!isAlignedWrite && debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Unaligned write. Addresses = " << address
             << " and " << address + 1 << endl;
    }

    if (isCachingEnabled) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Invalidating cache for address "
                 << address << endl;
        }
        cacheMap[address].invalidateCache();
        if (!isAlignedWrite) {
            cacheMap[address + 1].invalidateCache();
        }
    }

    int res = (serverInfos[currentServerIdx]->connection)
                  ->rpc_write(address, buf, offset, clientIdentifier, 
                  currentServerIdx);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__
                 << "\t : Write request timed-out, trying to contact other "
                    "server now."
                 << endl;
        }
        res = (serverInfos[currentServerIdx]->connection)
                  ->rpc_write(address, buf, offset, clientIdentifier, 
                  currentServerIdx);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    return res;
}

void cacheInvalidationListener(vector<ServerInfo*> & serverInfos, int & currentServerIdx,
    bool isCachingEnabled, string clientIdentifier, vector<CacheInfo> & cacheMap) {
    cout << __func__ << "\t : Listening for notifications.." << endl;
    Status status = grpc::Status::OK;
    do {
        status = (serverInfos[currentServerIdx]->connection)
                     ->rpc_subscribeForNotifications(isCachingEnabled, clientIdentifier, cacheMap);
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Error code = " << status.error_code()
                 << " and message = " << status.error_message() << endl;
        }
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
            for (auto &cachedEntry : cacheMap) {
                cachedEntry.invalidateCache();
            }
            if (debugMode <= DebugLevel::LevelNone) {
                cout << __func__ << "\t : Invalidated all cached entries as changing server!" << endl;
            }
            currentServerIdx = (currentServerIdx + 1) % serverInfos.size();
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Changing to backup server "
                     << serverInfos[currentServerIdx]->address << endl;
            }
        }
    } while (grpc::StatusCode::UNAVAILABLE == status.error_code());
    cout << __func__ << "\t : Stopped listening for notifications now." << endl;
}


int main(int argc, char *argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    cout.tie(nullptr);

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
    int numClients = 1;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }

        string address;

        do {
            string addressArg =
                "--address" + to_string(addresses.size() + 1) + "=";
            address = parseArgument(argumentString, addressArg);
            if (!address.empty() && !isIPValid(address)) {
                cout << __func__
                     << "\t : Enter a valid IP address and try again!"
                     << " Invalid IP = " << address << endl;
                return 0;
            }
            if (!address.empty()) {
                addresses.push_back(address);
            }
        } while (!address.empty());

        isReadOnlyMode = !parseArgument(argumentString, "--readOnly=").empty();
        cout << __func__ << "\t : Read Only Mode = " << isReadOnlyMode << endl;

        string clientArg = parseArgument(argumentString, "--numClients=");
        if (!clientArg.empty()) {
            int numClientsPassed = stoi(clientArg);
            if (numClientsPassed > 0 && numClientsPassed < 1000) {
                numClients = numClientsPassed;
            }
        }
    }

    if (addresses.empty()) {
        addresses = {"localhost:50051", "localhost:50053"};
    }
    
    const bool isCachingEnabled = true;

    cout << "Num Clients = " << numClients << endl;

    vector<Client*> ourClients;
    for (int i = 0; i < numClients; i++) {
        allReadTimes.push_back({});
        allWriteTimes.push_back({});
        ourClients.push_back(new Client(addresses, isCachingEnabled, i, isReadOnlyMode));    
    }
    vector<thread> threads;
    for (int i = 0; i < numClients; i++) {
        threads.push_back(thread(&Client::run_application, ourClients[i]));
    }
    for (int i = 0; i < numClients; i++) {
        threads[i].join();
        delete ourClients[i];
    }

    printStats();
    cout << "Finished with the threads!" << endl;
    return 0;
}

int Client::run_application() {
    vector<pair<double, int>> &readTimes = allReadTimes[clientThreadId],
                              &writeTimes = allWriteTimes[clientThreadId];

    string write_data = string(4096, 'x');

    int totalBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(0, 100);

    std::uniform_int_distribution<std::mt19937::result_type> dist7(0, (int)((MAX_SIZE_BYTES / BLOCK_SIZE_BYTES) - 1));
    const int NUM_RUNS = 50;

    for (int i = 0; i < NUM_RUNS; i++) {
        string buf;
        uint32_t address = (int)dist7(rng);

        struct timespec read_start, read_end;
        get_time(&read_start);

        int num_bytes_read =
            client_read(address * BLOCK_SIZE_BYTES, buf);

        get_time(&read_end);
        readTimes.push_back(
            make_pair(get_time_diff(&read_start, &read_end), address));

        if ((num_bytes_read != 4096) && (debugMode <= DebugLevel::LevelError)) {
            printf(
                "Didn't read 4k bytes from this file! Instead read %d bytes!\n",
                num_bytes_read);
        }

        msleep((int)dist6(rng));

        // Testing cache
        get_time(&read_start);

        num_bytes_read =
            client_read(address * BLOCK_SIZE_BYTES, buf);

        get_time(&read_end);
        readTimes.push_back(
            make_pair(get_time_diff(&read_start, &read_end), address));

        if ((num_bytes_read != 4096) && (debugMode <= DebugLevel::LevelError)) {
            printf(
                "Didn't read 4k bytes from this file! Instead read %d bytes!\n",
                num_bytes_read);
        }

        msleep((int)dist6(rng));

        struct timespec write_start, write_end;
        get_time(&write_start);

        int num_bytes_write = 0;

        if (!readFromBackup)
            num_bytes_write =
                client_write(address * BLOCK_SIZE_BYTES, write_data);

        get_time(&write_end);
        writeTimes.push_back(
            make_pair(get_time_diff(&write_start, &write_end), address));

        if ((!readFromBackup) && (num_bytes_write != 4096) &&
            (debugMode <= DebugLevel::LevelError)) {
            printf("Didn't write 4k bytes to this file! Instead wrote %d bytes.\n", num_bytes_write);
        }

        msleep((int)dist6(rng));
    }

    return 0;
}

void printPercentileTimes(const vector<pair<double, int>> &readTimes, const vector<pair<double, int>> &writeTimes){
    cout << "----------------------------------" << endl;
    cout<<"Percentile \t Read(ms) \t Write(ms)" << endl;
    vector<int> percentiles = {10,20,30,40,50,60,70,80,90,95,96,97,98,99,100};
    for(int percentile : percentiles) {
        int readItr = ((readTimes.size()-1) * percentile)/100, writeItr = ((writeTimes.size()-1) * percentile)/100;
        double readTime = readTimes[readItr].first , writeTime = writeTimes[writeItr].first;
        printf("%d \t \t %f \t %f \n", percentile, readTime, writeTime);
    }
}

void printStats() {
    vector<pair<double, int>> readTimes, writeTimes;
    for (auto readTime : allReadTimes) {
        for (auto p : readTime) {
            readTimes.push_back(p);
        }
    }
    
    for (auto writeTime : allWriteTimes) {
        for (auto p : writeTime) {
            writeTimes.push_back(p);
        }
    }
    
    double meanReadTime = 0;
    for (auto &readTime : readTimes) {
        meanReadTime += readTime.first;
    }
    meanReadTime /= readTimes.size();

    double meanWriteTime = 0;
    for (auto &writeTime : writeTimes) {
        meanWriteTime += writeTime.first;
    }
    meanWriteTime /= writeTimes.size();

    sort(readTimes.begin(), readTimes.end());
    sort(writeTimes.begin(), writeTimes.end());

    double medianReadTime = readTimes[readTimes.size() / 2].first;
    double medianWriteTime = writeTimes[writeTimes.size() / 2].first;

    printf(
        "%s : *****STATS (milliseconds) *****\n"
        "meanRead   = %f \t meanWrite   = %f \n"
        "medianRead = %f \t medianWrite = %f\n"
        "minRead    = %f \t minWrite    = %f\n"
        "minAddress = %d \t minAddress  = %d\n"
        "maxRead    = %f \t maxWrite    = %f\n"
        "maxAddress = %d \t maxAddress  = %d\n",
        __func__, meanReadTime, meanWriteTime, medianReadTime, medianWriteTime,
        readTimes.front().first, writeTimes.front().first,
        readTimes.front().second, writeTimes.front().second,
        readTimes.back().first, writeTimes.back().first,
        readTimes.back().second, writeTimes.back().second);

    printPercentileTimes(readTimes, writeTimes);
}
