#include "client.h"

int run_application(bool isReadOnlyMode);

bool checkIfOffsetIsAligned(uint32_t &offset) {
    return offset % BLOCK_SIZE_BYTES == 0;
}

bool cacheStalenessValidation(const vector<uint32_t> &addressVector) {
    for (auto &address : addressVector) {
        if (!cacheMap[address].isCached || cacheMap[address].isStale())
            return false;
    }
    return true;
}

static int client_read(uint32_t offset, string &buf,
                       bool readFromBackup = false) {
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

    if (isCachingEnabled && cacheStalenessValidation(addressVector)) {
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
             << serverInfos[serverToContactIdx].address << endl;
    }

    int res = (serverInfos[serverToContactIdx].connection)
                  ->rpc_read(address, buf, readSize, offset);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__
                 << "\t : Read request timed-out, trying to contact other "
                    "server now."
                 << endl;
        }
        if (!readFromBackup) {
            switchServerConnection();
        }
        res = (serverInfos[currentServerIdx].connection)
                  ->rpc_read(address, buf, readSize, offset);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    if (isCachingEnabled) {
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

static int client_write(uint32_t offset, const string &buf) {
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

    int res = (serverInfos[currentServerIdx].connection)
                  ->rpc_write(address, buf, offset);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__
                 << "\t : Write request timed-out, trying to contact other "
                    "server now."
                 << endl;
        }
        switchServerConnection();
        res = (serverInfos[currentServerIdx].connection)
                  ->rpc_write(address, buf, offset);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    return res;
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
    std::uniform_int_distribution<std::mt19937::result_type> dist6(0, 50);

    const int NUM_RUNS = 50;

    for (int i = 0; i < NUM_RUNS; i++) {
        string buf;
        uint32_t address = i % NUM_RUNS;  // max(0, rand()) % totalBlocks;

        struct timespec read_start, read_end;
        get_time(&read_start);

        int num_bytes_read =
            client_read(address * BLOCK_SIZE_BYTES, buf, isReadOnlyMode);

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
            client_read(address * BLOCK_SIZE_BYTES, buf, isReadOnlyMode);

        get_time(&read_end);
        readTimes.push_back(
            make_pair(get_time_diff(&read_start, &read_end), address));

        if ((num_bytes_read != 4096) && (debugMode <= DebugLevel::LevelError)) {
            printf(
                "Didn't read 4k bytes from this file! Instead read %d bytes!\n",
                num_bytes_read);
        }

        msleep((int)dist6(rng));

        address = i % NUM_RUNS;  // max(0, rand()) % totalBlocks;

        struct timespec write_start, write_end;
        get_time(&write_start);

        int num_bytes_write = 0;

        if (!isReadOnlyMode)
            num_bytes_write =
                client_write(address * BLOCK_SIZE_BYTES, write_data);

        get_time(&write_end);
        writeTimes.push_back(
            make_pair(get_time_diff(&write_start, &write_end), address));

        if ((!isReadOnlyMode) && (num_bytes_write != 4096) &&
            (debugMode <= DebugLevel::LevelError)) {
            printf("Didn't write 4k bytes to this file! Instead wrote %d bytes.\n", num_bytes_write);
        }

        msleep((int)dist6(rng));
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

    (serverInfos[currentServerIdx].connection)
        ->rpc_unSubscribeForNotifications();
    notificationThread.join();

    return 0;
}