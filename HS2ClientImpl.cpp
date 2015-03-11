#include "HS2Client.hpp"
#include <map>
#include <boost/shared_ptr.hpp>
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
#include "IDecompressor.hpp"
#include <TCLIService_types.h>
#include "TCLIService.h"
#include "IDecompressor.hpp"

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;



class HS2ClientImpl: public HS2Client{
    HS2ClientImpl(const std::string &host, const int port);
    void SetDecompressor(const std::string& compressorName);

    // return false if failed
    bool OpenSession();

    bool SubmitQuery(const std::string &in_query, OpHandle& out_OpHandle);
    void GetResultsetMetaData(const OpHandle& opHandle);
    void FetchResultSet(const OpHandle&  opHandle);
    void CloseSession();

private:
    apache::hive::service::cli::thrift::TCLIServiceClient *m_client;
    apache::hive::service::cli::thrift::TSessionHandle m_SessionHandle;
    IDecompressor* m_decompressor;
};



// Is the given column is comprressed
inline bool isCompressed(const string& compressorBitmap, size_t col) {
    const uint8_t * pBitmap = (const uint8_t *)compressorBitmap.c_str();
    static uint8_t mask[8] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
    return pBitmap[col/8] & mask[col % 8];
}

/**
*
* HS2Client: wrapper on top of HS2ClientImpl
*/

HS2Client::HS2Client(const string &host, const int port):m_decompressor(NULL){
    m_client = new HS2ClientImpl(host, port);
}

HS2Client::~HS2Client() {
    delete m_client;
}

bool HS2Client::OpenSession() {
    return m_client->OpenSession();
}

bool HS2Client::SubmitQuery(const std::string &in_query, OpHandle&
out_OpHandle) {
    return m_client->SubmitQuery(in_query, out_OpHandle);
}
void HS2Client::GetResultsetMetaData(const OpHandle& opHandle){

    return m_client->GetResultsetMetaData(opHandle);
}
void FetchResultSet(const OpHandle&  opHandle){

    return m_client->FetchResultSet(opHandle);
}
bool CloseSession() {
    return m_client->CloseSession();
}


/**
*
* HS2ClientImpl
*/

HS2ClientImpl::HS2ClientImpl(const std::string &host, const int port) {
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

// One approach to implement stragery pattern: http://sourcemaking.com/design_patterns/strategy/cpp/1
void HS2ClientImpl::SetDecompressor(const string& compressorName) {

    if(m_decompressor!=NULL) delete m_decompressor;
    if (compressorName == "PIN") {
        m_decompressor = new SimpleDecompressor();
    }
    else {
        std::cerr << "Unknown compressor " << compressorName << std::endl;
    }
}

// return false if failed
bool HS2ClientImpl::OpenSession() {
    TOpenSessionReq openSessionReq;
    TOpenSessionResp openSessionResp;

    // @todo: query system table to get hive version and set protocol properly
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
            << openSessionResp.serverProtocolVersion + 1 << endl;

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

bool HS2ClientImpl::SubmitQuery(const string &in_query, TOperationHandle
&out_OpHandle) {

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

void HS2ClientImpl::GetResultsetMetaData(const TOperationHandle &opHandle) {
    TGetResultSetMetadataReq metadataReq;
    TGetResultSetMetadataResp metadataResp;
    metadataReq.__set_operationHandle(opHandle);
    m_client->GetResultSetMetadata(metadataResp, metadataReq);
    std::cerr << "Get Result Set Metadata Status: "
            << LogTStatusToString(metadataResp.status) << endl;
    std::cout << "Column Metadata: "
            << LogTTableSchemaToString(metadataResp.schema) << endl;
}

void HS2ClientImpl::FetchResultSet(const TOperationHandle &opHandle) {
    // fetch results, blocking call
    TFetchResultsReq fetchResultsReq;
    TFetchResultsResp fetchResultsResp;
    fetchResultsReq.__set_operationHandle(opHandle);
    fetchResultsReq.__set_maxRows(10000);

    m_client->FetchResults(fetchResultsResp, fetchResultsReq);

    std::cerr << "Fetch Result Status: "
            << LogTStatusToString(fetchResultsResp.status) << endl;
    std::cerr << "hasMoreRows: " << fetchResultsResp.hasMoreRows << endl;


    const vector<TColumn> &cols = fetchResultsResp.results.columns;
    const vector<TEnColumn> &encols = fetchResultsResp.results.enColumns;
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
}

bool HS2ClientImpl::CloseSession() {
    TCloseSessionReq closeSessionReq;
    TCloseSessionResp closeSessionResp;
    closeSessionReq.__set_sessionHandle(m_SessionHandle);
    m_client->CloseSession(closeSessionResp, closeSessionReq);
    std::cerr << "Close Session: "
            << LogTStatusToString(closeSessionResp.status) << endl;
    return true;
}
