#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/program_options.hpp>
#include <cstdint>
// http://stackoverflow.com/questions/9018443/using-thrift-c-library-in-xcode
#ifndef _WIN32
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "TCLIService.h"
#include "TCLIService_types.h"

using namespace std;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;
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

string LogTTableSchemaToString(TTableSchema &schema);
string
LogTStatusToString(apache::hive::service::cli::thrift::TStatus &in_status);

ostream &operator<<(ostream &os, const TColumn &col) {
    os << "[";
    if (col.__isset.byteVal) {
        copy(col.binaryVal.values.begin(), col.binaryVal.values.end(),
             ostream_iterator<string>(os, ", "));
    } else if (col.__isset.boolVal) {
        copy(col.boolVal.values.begin(), col.boolVal.values.end(),
             ostream_iterator<bool>(os, ", "));
    } else if (col.__isset.doubleVal) {
        copy(col.doubleVal.values.begin(), col.doubleVal.values.end(),
             ostream_iterator<double>(os, ", "));
    } else if (col.__isset.i16Val) {
        copy(col.i16Val.values.begin(), col.i16Val.values.end(),
             ostream_iterator<int16_t>(os, ", "));
    } else if (col.__isset.i32Val) {
        copy(col.i32Val.values.begin(), col.i32Val.values.end(),
             ostream_iterator<int32_t>(os, ", "));
    } else if (col.__isset.i64Val) {
        copy(col.i64Val.values.begin(), col.i64Val.values.end(),
             ostream_iterator<int64_t>(os, ", "));
    } else if (col.__isset.stringVal) {
        copy(col.stringVal.values.begin(), col.stringVal.values.end(),
             ostream_iterator<string>(os, ", "));
    } else {
        std::cerr << "Invalid column type" << endl;
        exit(-1);
    }
    os << "]";
    return os;
}

class Decoder {
  public:
    static void decode(TRowSet &resultBuffer);
};

class HS2Client {
    TCLIServiceClient *m_client;
    TSessionHandle m_SessionHandle;
    Decoder m_decoder;

  public:
    HS2Client(const string &host, const int port) {
        boost::shared_ptr<TTransport> trans(new TSocket(host, port));
        trans.reset(new TBufferedTransport(trans));
        try {
            trans->open();
        } catch (TTransportException &e) {
            std::cerr << "Cannot open the socket: " << e.what() << endl;
            exit(-1);
        }
        boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(trans));
        m_client = new TCLIServiceClient(protocol);
    }

    // return false if failed
    bool OpenSession() {
        TOpenSessionReq openSessionReq;
        TOpenSessionResp openSessionResp;

        openSessionReq.__set_client_protocol(
            TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V8);
        // openSessionReq.__set_username("cloudera");
        std::ifstream ifs("compressorInfo.json");
        if(!ifs.good()) {
            std::cerr << "Cannot open configuration file: compressorInfo.json" << endl;
            return false;
        }
        std::stringstream compressorInfo;
        compressorInfo << ifs.rdbuf();
        std::map<std::string, std::string> conf;
        conf["CompressorInfo"] = compressorInfo.str();
        openSessionReq.__set_configuration(conf);

        m_client->OpenSession(openSessionResp, openSessionReq);

        std::cerr << "Open Session Status: "
                  << LogTStatusToString(openSessionResp.status) << endl;
        std::cerr << "Server Protocol Version: "
                  << openSessionResp.serverProtocolVersion << endl;

        if (openSessionReq.client_protocol !=
            openSessionResp.serverProtocolVersion) {
            std::cerr << "ERROR: protocol version does not match" << endl;
            return false;
        }

        m_SessionHandle = openSessionResp.sessionHandle;
        return (openSessionResp.status.statusCode ==
                TStatusCode::SUCCESS_STATUS) ||
               (openSessionResp.status.statusCode ==
                TStatusCode::SUCCESS_WITH_INFO_STATUS);
    }

    bool SubmitQuery(const string &in_query, TOperationHandle &out_OpHandle) {

        // execute the query
        TExecuteStatementReq execStmtReq;
        TExecuteStatementResp execStmtResp;
        execStmtReq.__set_sessionHandle(m_SessionHandle);
        execStmtReq.__set_statement(in_query);
        m_client->ExecuteStatement(execStmtResp, execStmtReq);
        std::cerr << "Execute Statement Status: "
                  << LogTStatusToString(execStmtResp.status) << endl;
        out_OpHandle = execStmtResp.operationHandle;
        return (execStmtResp.status.statusCode ==
                TStatusCode::SUCCESS_STATUS) ||
               (execStmtResp.status.statusCode ==
                TStatusCode::SUCCESS_WITH_INFO_STATUS);
    }

    void GetResultsetMetaData(const TOperationHandle &opHandle) {
        TGetResultSetMetadataReq metadataReq;
        TGetResultSetMetadataResp metadataResp;
        metadataReq.__set_operationHandle(opHandle);
        m_client->GetResultSetMetadata(metadataResp, metadataReq);
        std::cerr << "Get Result Set Metadata Status: "
                  << LogTStatusToString(metadataResp.status) << endl;
        std::cout << "Column Metadata: "
                  << LogTTableSchemaToString(metadataResp.schema) << endl;
    }

    void FetchResultSet(const TOperationHandle &opHandle) {
        // fetch results, blocking call
        TFetchResultsReq fetchResultsReq;
        TFetchResultsResp fetchResultsResp;
        fetchResultsReq.__set_operationHandle(opHandle);
        fetchResultsReq.__set_maxRows(10000);

        m_client->FetchResults(fetchResultsResp, fetchResultsReq);

        std::cerr << "Fetch Result Status: "
                  << LogTStatusToString(fetchResultsResp.status) << endl;
        std::cerr << "hasMoreRows: " << fetchResultsResp.hasMoreRows << endl;

        bool compressed = true;

        if (compressed) {
            // do the decoding here
            m_decoder.decode(fetchResultsResp.results);
        }
        const vector<TColumn> &cols = fetchResultsResp.results.columns;
        size_t nColumns = cols.size();
        for (size_t i = 0; i < nColumns; i++) {
            std::cout << cols[i] << std::endl;
        }
    }

    void CloseSession() {
        TCloseSessionReq closeSessionReq;
        TCloseSessionResp closeSessionResp;
        closeSessionReq.__set_sessionHandle(m_SessionHandle);
        m_client->CloseSession(closeSessionResp, closeSessionReq);
        std::cerr << "Close Session: "
                  << LogTStatusToString(closeSessionResp.status) << endl;
    }
};

void Decoder::decode(TRowSet &rs) {

    try {

        // clean the old column and decoding the encoded columns into the old
        // column
        std::vector<apache::hive::service::cli::thrift::TColumn> old_col =
            rs.columns;
        if (rs.columns.size() != 0) {
            rs.columns.clear();
        }

        int cols = rs.enColumns.size() + old_col.size();

        const uint8_t *compressorBitmap =
            (const uint8_t *)rs.compressorBitmap.c_str();

        uint8_t mask[8] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};

        std::vector<TEnColumn>::iterator itr = rs.enColumns.begin();
        std::vector<TColumn>::iterator col_itr = old_col.begin();
        for (int i = 0; i < cols; i++) {
            int index = i / 8;
            int offset = i % 8;
            if ((compressorBitmap[index] & mask[offset]) == (uint8_t)0) {
                rs.columns.push_back(*col_itr);
                col_itr++;
                continue;
            }
            apache::hive::service::cli::thrift::TColumn tColumn;
            if (itr->compressorName == "PIN") {
                switch (itr->type) {

                case TTypeId::INT_TYPE: {
                    apache::hive::service::cli::thrift::TI32Column tI32Column;
                    tI32Column.values.reserve(
                        rs.columns[0].i32Val.values.size());
                    int size = itr->size;
                    const uint8_t *encodedData =
                        (const uint8_t *)itr->enData.c_str();
                    uint8_t *ptr = (uint8_t *)encodedData;

                    ptr++;
                    int bitsPerLen = *(uint32_t *)ptr;
                    ptr += 4;
                    int min_len = *(uint32_t *)ptr;
                    ptr += 4;

                    unsigned int lenmask = ~(-1 << bitsPerLen);
                    int valPos = 9 + (size * bitsPerLen + 7) / 8;
                    unsigned int lenAcc = 0;
                    unsigned int valAcc = 0;
                    int lenBits = 0;
                    int valBits = 0;
                    int valWid = 0;
                    unsigned int value = 0;

                    for (int i = 0; i < size; i++) {
                        for (; lenBits < bitsPerLen; lenBits += 8) {
                            lenAcc |= *ptr++ << lenBits;
                        }
                        valWid = (lenAcc & lenmask) + min_len - 1;
                        lenAcc >>= bitsPerLen;
                        lenBits -= bitsPerLen;

                        if (valWid == -1) {
                            tI32Column.values.push_back(0);
                            continue;
                        }

                        for (; valBits < valWid; valBits += 8) {
                            valAcc |= encodedData[valPos++] << valBits;
                        }
                        valBits -= valWid;

                        value = valAcc & ~(-1 << valWid) ^ (1 << valWid);

                        valAcc = (unsigned int)encodedData[valPos - 1] >>
                                 (8 - valBits);
                        value = value & 1 ? 1 + (value >> 1) : -(value >> 1);
                        tI32Column.values.push_back((int32_t)value);
                    }

                    tI32Column.__set_nulls(itr->nulls);
                    tColumn.__set_i32Val(tI32Column);
                    tColumn.__isset.i32Val = true;
                    break;
                }
                default:
                    throw "Not Implemented Decoding Type";
                }
                rs.columns.push_back(tColumn);
                itr++;
            }
        }
        rs.enColumns.clear();
        rs.__isset.columns = true;
        rs.__isset.enColumns = false;
    } catch (...) {
        throw "exception while decoding data";
    }
}

// The TStatusCode names.
static const char *TSTATUS_CODE_NAMES[] = {
    "SUCCESS_STATUS", "SUCCESS_WITH_INFO_STATUS", "STILL_EXECUTING_STATUS",
    "ERROR_STATUS", "INVALID_HANDLE_STATUS"};

string LogTTableSchemaToString(TTableSchema &schema) {
    ostringstream ss;
    size_t sz = schema.columns.size();
    ss << "TableSchema {\n";
    for (size_t i = 0; i < sz; i++) {
        ss << "\t[name:" << schema.columns[i].columnName;
        ss << ", position: " << schema.columns[i].position << "]\n";
    }
    ss << "}\n";
    return ss.str();
}
string
LogTStatusToString(apache::hive::service::cli::thrift::TStatus &in_status) {
    ostringstream ss;

    ss << "\nTStatus {";
    ss << "\n  statusCode=" << TSTATUS_CODE_NAMES[in_status.statusCode];
    ss << "\n  infoMessages=";

    for (std::vector<string>::iterator currInfoMsg =
             in_status.infoMessages.begin();
         in_status.infoMessages.end() != currInfoMsg; ++currInfoMsg) {
        ss << "\"" << *currInfoMsg << "\"";
    }
    ss << "\n  sqlState=" << in_status.sqlState;
    ss << "\n  errorCode=" << in_status.errorCode;
    ss << "\n  errorMessage=\"" << in_status.errorMessage + "\"";
    ss << "\n  __isset.errorCode: " << in_status.__isset.errorCode;
    ss << "\n  __isset.errorMessage: " << in_status.__isset.errorMessage;
    ss << "\n  __isset.infoMessages: " << in_status.__isset.infoMessages;
    ss << "\n  __isset.sqlState: " << in_status.__isset.sqlState;
    ss << "\n}\n";
    return ss.str();
}

// HardyTCLIServiceClient is a wrapper
int main(int argc, char *argv[]) {

    try {
        // Sample
        // ./querySubmitter --host localhost --query "show tables;"
        // On Boost::ProgramOptions
        // http://www.boost.org/doc/libs/1_57_0/doc/html/program_options/tutorial.html
        po::options_description desc("Options");
        desc.add_options()
            ("help", "Produce help messages")
            ("host", po::value<std::string>(), "hostname")
            ("port", po::value<int>(), "port number, default 10000")
            ("query", po::value<std::string>(), "queries");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        // string host = "localhost";
        string host = "192.168.33.10";
        if (vm.count("host")) {
            std::cerr << "Host = " << vm["host"].as<string>() << std::endl;
            host = vm["host"].as<string>();
        }

        int port = 10000;
        if (vm.count("port")) {
            port = vm["port"].as<int>();
        }
        HS2Client client(host, port);

        bool bSessionSuccess = client.OpenSession();

        if (bSessionSuccess) {
            const char *queries[] = {"select * from Integer_table"};
            size_t sz = sizeof(queries) / sizeof(char *);
            for (size_t i = 0; i < sz; i++) {
                std::cerr << "Running query " << queries[i] << std::endl;
                TOperationHandle opHandle;
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
