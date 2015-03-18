
#include "HS2Client.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;
namespace po = boost::program_options;

// check whether given string is an empty query
inline bool IsEmptyQuery(const std::string& q) {
    for(int i = 0; i< q.size() ; i++){
        if (!isspace(q[i])) return false;
    }
    return true;
}

int main(int argc, const char *argv[]) {

    try {


        /**
        * Parse command option
        *
        * querySubmitter
        *       --host [host]
        *       --port [port]
        *       --query [query]
        *       --compressor [compressor]
        */
        std::string host, compressor, query;
        int port;
        po::options_description desc(
                "Usage: qh [--host  host-name] [--port port-name] [--compressor compressor-name] --query query-string"
                "\n\nOptions"
                );

        desc.add_options()
            ("help", "Help on args options")
            ("host", po::value<std::string>(&host)->default_value("localhost"), "HiveServer2 hostname/IP address")
            ("port", po::value<int>(&port)->default_value(10000), "HiveServer2 port number")
            ("compressor", po::value<std::string>(&compressor)->default_value("PIN"), "Compressor name")
            ("query", po::value<std::string>(&query)->default_value("show tables"), "Queries, seperated by semi-colon");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return 1;
        }

        // Create a new client and set compressor
        HS2Client* client = HS2Client::Create(host, port);
        if (!client->SetDecompressor(compressor)) {
            std::cerr << "Unknown decompressor: " << compressor << std::endl;
            delete client;
            return -1;
        }

        if (client->OpenSession()) {

            // parse query string into queries
            std::vector<std::string> queries;
            boost::trim(query);
            boost::split(queries, query, boost::is_any_of(";"));

            // run query one by one
            for (size_t i = 0; i < queries.size(); i++) {

                if ( IsEmptyQuery(queries[i]) ) continue;
                std::cout << "[Query]: " << queries[i] << std::endl;
                OpHandle opHandle;
                bool bSuccess = true;
                bSuccess = client->SubmitQuery(queries[i], opHandle);
                if (!bSuccess) {
                    std::cerr << "Error while submitting query: " << queries[i]
                            << std::endl;
                    continue;
                }

                bSuccess = client->GetResultsetMetaData(opHandle);
                if (!bSuccess){
                    std::cerr << "Error while getting resultset metadata"
                            << std::endl;
                    continue;
                }

                bSuccess = client->FetchResultSet(opHandle);
                if (!bSuccess) {
                    std::cerr << "Error while getting result back: "
                            << queries[i] << std::endl;
                    continue;
                }

            }
        } else {
            std::cerr << "Error while opening session" << std::endl;
        }
        client->CloseSession();
        delete client;
    } catch (exception &e) {
        std::cerr << "Error: " << e.what() << endl;
        return -1;
    }
    return 0;
}
