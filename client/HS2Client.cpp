#include "HS2Client.hpp"
#include "TCLIService.h"

#include <fstream>
#include <sstream>
#include <iostream>

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

/*
namespace apache { namespace hive { namespace service { namespace cli { namespace thrift {
    class TSessionHandle;
    class TOperationHandle;
    class TCLIServiceClient;
    class TSessionHandle;
} } } } }
*/

#include "Decompressor.hpp"
#include "utils.hpp"

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

/**
* Helper Inline Functions
*/
// Does the status code mean success?
inline bool IsSucess(const TStatusCode::type& t) {
    return (t==TStatusCode::SUCCESS_STATUS) ||
            (t==TStatusCode::SUCCESS_WITH_INFO_STATUS);
}

// Is the given column is comprressed?
inline bool isCompressed(const std::string& compressorBitmap, size_t col) {
    const uint8_t * pBitmap = (const uint8_t *)compressorBitmap.c_str();
    static uint8_t mask[8] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
    return pBitmap[col/8] & mask[col % 8];
}

/**
* HS2Client Implementation
*/
class HS2ClientImpl: public HS2Client{
public:
    HS2ClientImpl(const std::string &host, const int port);
    bool SetDecompressor(const std::string& compressorName);
    bool OpenSession();
    bool SubmitQuery(const std::string &in_query, OpHandle& out_OpHandle);
    bool GetResultsetMetaData(const OpHandle& opHandle);
    bool FetchResultSet(const OpHandle&  opHandle);
    bool CloseSession();

private:
    TCLIServiceClient *m_client;
    TSessionHandle m_SessionHandle;
    Decompressor* m_decompressor;
};



HS2Client* HS2Client::Create(const std::string &host, const int port) {
    return  new HS2ClientImpl(host, port);
}


HS2ClientImpl::HS2ClientImpl(const std::string &host, const int port)
        : m_decompressor(NULL) {
    boost::shared_ptr<TTransport> trans(new TSocket(host, port));
    trans.reset(new TBufferedTransport(trans));
    try {
        trans->open();
    } catch (TTransportException &e) {
        std::cerr << "Cannot open the socket: " << e.what() << std::endl;
        exit(-1);
    }
    boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(trans));
    m_client = new TCLIServiceClient(protocol);
}

// One approach to implement stragery pattern: http://sourcemaking.com/design_patterns/strategy/cpp/1
bool HS2ClientImpl::SetDecompressor(const std::string &compressorName) {

    if(m_decompressor!=NULL) delete m_decompressor;
    m_decompressor = Decompressor::Create(compressorName);
    if(m_decompressor == NULL) return false;
    else return true;

}

// return false if failed
bool HS2ClientImpl::OpenSession() {
    TOpenSessionReq openSessionReq;
    TOpenSessionResp openSessionResp;

    // @todo: query system table to get hive version and set protocol properly
    openSessionReq.__set_client_protocol(
            TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V8);

    // @todo: json negotiation
    // @todo: validate json file format
    std::ifstream ifs("compressorInfo.json");
    if(!ifs.good()) {
        std::cerr << "Cannot open configuration file: compressorInfo.json" <<
                std::endl;
        return false;
    }
    std::stringstream compressorInfo;
    compressorInfo << ifs.rdbuf();
    std::map<std::string, std::string> conf;
    conf["CompressorInfo"] = compressorInfo.str();
    openSessionReq.__set_configuration(conf);

    m_client->OpenSession(openSessionResp, openSessionReq);

    std::cerr << "Open Session Status: "
            << LogTStatusToString(openSessionResp.status) << std::endl;
    std::cerr << "Server Protocol Version: "
            << openSessionResp.serverProtocolVersion + 1 << std::endl;

    if (openSessionReq.client_protocol !=
            openSessionResp.serverProtocolVersion) {
        std::cerr << "ERROR: protocol version does not match" << std::endl;
        return false;
    }

    m_SessionHandle = openSessionResp.sessionHandle;
    return (openSessionResp.status.statusCode ==
            TStatusCode::SUCCESS_STATUS) ||
            (openSessionResp.status.statusCode ==
                    TStatusCode::SUCCESS_WITH_INFO_STATUS);
}

bool HS2ClientImpl::SubmitQuery(const std::string &in_query,
        TOperationHandle &out_OpHandle) {

    // execute the query
    TExecuteStatementReq execStmtReq;
    TExecuteStatementResp execStmtResp;
    execStmtReq.__set_sessionHandle(m_SessionHandle);
    execStmtReq.__set_statement(in_query);
    m_client->ExecuteStatement(execStmtResp, execStmtReq);
    std::cerr << "Execute Statement Status: "
            << LogTStatusToString(execStmtResp.status) << std::endl;
    out_OpHandle = execStmtResp.operationHandle;
    return IsSucess(execStmtResp.status.statusCode);
}

bool HS2ClientImpl::GetResultsetMetaData(const TOperationHandle &opHandle) {
    TGetResultSetMetadataReq metadataReq;
    TGetResultSetMetadataResp metadataResp;
    metadataReq.__set_operationHandle(opHandle);
    m_client->GetResultSetMetadata(metadataResp, metadataReq);
    std::cerr << "Get Result Set Metadata Status: "
            << LogTStatusToString(metadataResp.status) << std::endl;
    std::cout << "Column Metadata: "
            << LogTTableSchemaToString(metadataResp.schema) << std::endl;
    return  IsSucess(metadataResp.status.statusCode);
}

bool HS2ClientImpl::FetchResultSet(const TOperationHandle &opHandle) {
    // fetch results, blocking call
    TFetchResultsReq fetchResultsReq;
    TFetchResultsResp fetchResultsResp;
    fetchResultsReq.__set_operationHandle(opHandle);
    fetchResultsReq.__set_maxRows(10000);

    m_client->FetchResults(fetchResultsResp, fetchResultsReq);

    std::cerr << "Fetch Result Status: "
            << LogTStatusToString(fetchResultsResp.status) << std::endl;
    std::cerr << "hasMoreRows: " << fetchResultsResp.hasMoreRows << std::endl;


    const std::vector<TColumn> &cols = fetchResultsResp.results.columns;
    const std::vector<TEnColumn> &encols = fetchResultsResp.results.enColumns;
    size_t nColumns = cols.size() + encols.size();

    size_t idx_col = 0;     // index for TColumn
    size_t idx_encol = 0;   // index for TEnColumn
    for (size_t i = 0; i < nColumns; i++) {

        if (isCompressed(fetchResultsResp.results.compressorBitmap, i)){
            TColumn out_col;
            m_decompressor->Decompress(encols[idx_encol], out_col);
            std::cout << "Column " << i+1  << std::endl;
            std::cout <<  out_col << std::endl;
            idx_encol++;

        } else {
            std::cout << "Column " << i+1 << std::endl;
            std::cout << cols[idx_col] << std::endl;
            idx_col++;
        }
    }
    return IsSucess(fetchResultsResp.status.statusCode);
}

bool HS2ClientImpl::CloseSession() {
    TCloseSessionReq closeSessionReq;
    TCloseSessionResp closeSessionResp;
    closeSessionReq.__set_sessionHandle(m_SessionHandle);
    m_client->CloseSession(closeSessionResp, closeSessionReq);
    std::cerr << "Close Session: "
            << LogTStatusToString(closeSessionResp.status) << std::endl;
    return IsSucess(closeSessionResp.status.statusCode);
}
