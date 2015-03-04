#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include "HS2Client.hpp"
#include "utils.hpp"
class SimpleDecompressor;

using namespace std;
namespace po = boost::program_options;


#ifdef _WIN32
#define bswap_16 _byteswap_ushort
#define bswap_32 _byteswap_ulong
#define bswap_64 _byteswap_uint64
#elif defined(__linux__)
#include <byteswap.h> //for bswap_16,32,64
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>

#define bswap_16 OSSwapInt16
#define bswap_32 OSSwapInt32
#define bswap_64 OSSwapInt64
#endif


// HardyTCLIServiceClient is a wrapper
int main(int argc, const char *argv[]) {

    try {

        string host, compressor, query;
        int port;

        // Sample
        // ./querySubmitter --host localhost --query "show tables;"
        // On Boost::ProgramOptions
        // http://www.boost.org/doc/libs/1_57_0/doc/html/program_options/tutorial.html
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
        HS2Client client(host, port);
        client.SetDecompressor(compressor);

        bool bSessionSuccess = client.OpenSession();
        if (bSessionSuccess) {


            std::vector<std::string> queries;
            boost::trim(query);
            boost::split(queries, query, boost::is_any_of(";"));

            for (size_t i = 0; i < queries.size(); i++) {

                std::cerr << "Running query: " << queries[i] << std::endl;
                OpHandle opHandle;
                bool bSuccess = client.SubmitQuery(queries[i], opHandle);
                if (bSuccess) {
                    client.GetResultsetMetaData(opHandle);
                    client.FetchResultSet(opHandle);
                }

            }
        } else {
            std::cerr << "Error while opening session" << std::endl;
        }
        client.CloseSession();
    } catch (exception &e) {
        cerr << "Error: " << e.what() << endl;
        return -1;
    }
    return 0;
}
