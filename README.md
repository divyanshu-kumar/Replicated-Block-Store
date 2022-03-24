# Replicated-Block-Store
A replicated network-based block storage system

To build:
```
source make.sh
```

First run server:
```
./server
```

Then run client:
```
./client
```

For now, the client does a bunch of read and writes of 4KB logical blocks on the server and prints out the statistics of the time it took.
