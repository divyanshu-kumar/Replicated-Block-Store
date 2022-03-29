#include "server.h"

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

        role = parseArgument(argumentString, "--role=");
        my_address = parseArgument(argumentString, "--my_address=");
        other_address = parseArgument(argumentString, "--other_address=");

        if (!isRoleValid() || !isIPValid(my_address) || !isIPValid(other_address)) {
            cout << "Role = " << role << "\nMy Address = " << my_address << 
            "\nOther Address = " << other_address << endl;
            role = "";
        }
    }

     if (role.empty() || my_address.empty()) {
        printf( "Enter arguments like below and try again - \n"
                "./server --role=[primary or backup] "
                "--my_address=[IP:PORT] --other_address=[IP:PORT]\n");
        return 0;
    }

    cout << "Role = " << role << "\nMy Address = " << my_address << 
            "\nOther Address = " << other_address << endl;

    serverReplication = new ServerReplication(grpc::CreateChannel(other_address.c_str(),
                                                grpc::InsecureChannelCredentials()));

    makeFolderAndBlocks();

    rollbackUncommittedWrites();

    RunServer();

    return 0;
}

/*
Example server commands:
For primary, in src folder : ./server --role=primary --my_address=0.0.0.0:50051 --other_address=0.0.0.0:50053
For backup, in folder BackupServer: ./server --role=backup --my_address=0.0.0.0:50053 --other_address=0.0.0.0:50051
*/