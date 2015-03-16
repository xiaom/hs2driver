
#include "HS2Client.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <AddressBook/AddressBook.h>

using namespace std;
namespace po = boost::program_options;

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
        po::options_description desc("Options");
        desc.add_options()
            ("help", "Produce help messages")
            ("host", po::value<std::string>(&host)->default_value("localhost"), "hostname")
            ("port", po::value<int>(&port)->default_value(10000), "port number")
            ("compressor", po::value<std::string>(&compressor)->default_value("PIN"), "compressor name")
            ("query", po::value<std::string>(&query)->default_value("select * from Integer_table"), "queries");

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

                std::cerr << "Running query: " << queries[i] << std::endl;
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
        cerr << "Error: " << e.what() << endl;
        return -1;
    }
    return 0;
}
