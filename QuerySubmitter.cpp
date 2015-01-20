#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <boost/shared_ptr.hpp>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "TCLIService.h"
#include "TCLIService_types.h"

using namespace std;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;


string LogTTableSchemaToString(TTableSchema& schema);
string LogTStatusToString(apache::hive::service::cli::thrift::TStatus& in_status);

ostream& operator<< (ostream& os, const TColumn& col){
    os << "[";
    if (col.__isset.byteVal){
    copy(col.binaryVal.values.begin(), col.binaryVal.values.end(), ostream_iterator<string>( os, ", "));
    }
    else if (col.__isset.boolVal){
    copy(col.boolVal.values.begin(), col.boolVal.values.end(), ostream_iterator<bool>( os, ", "));
    }
    else if (col.__isset.doubleVal) {
    copy(col.doubleVal.values.begin(), col.doubleVal.values.end(), ostream_iterator<double>( os, ", "));
    }
    else if (col.__isset.i16Val) {
    copy(col.i16Val.values.begin(), col.i16Val.values.end(), ostream_iterator<int16_t>( os, ", "));
    }
    else if (col.__isset.i32Val) {
    copy(col.i32Val.values.begin(), col.i32Val.values.end(), ostream_iterator<int32_t>( os, ", "));
    }
    else if (col.__isset.i64Val) {
    copy(col.i64Val.values.begin(), col.i64Val.values.end(), ostream_iterator<int64_t>( os, ", "));
    }
    else if (col.__isset.stringVal) {
    copy(col.stringVal.values.begin(), col.stringVal.values.end(), ostream_iterator<string>( os, ", "));
    }
    else {
    std::cerr << "Invalid column type" << endl;
    exit(-1);
    }
    os << "]";
    return os;
}
class HS2Client {
    TCLIServiceClient* m_client;
    TSessionHandle m_SessionHandle;

public:
    HS2Client(const string& host, const int port){
        boost::shared_ptr<TTransport> trans(new TSocket(host, port));
        trans.reset(new TBufferedTransport(trans));
        try{
            trans->open();
        }
        catch (TTransportException& e){
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


        openSessionReq.__set_client_protocol(TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V7);
        openSessionReq.__set_username("cloudera");

        m_client->OpenSession(openSessionResp, openSessionReq);

        std::cerr << "Open Session Status: " << LogTStatusToString(openSessionResp.status) << endl;
        std::cerr << "Server Protocol Version: " << openSessionResp.serverProtocolVersion << endl;

        if (openSessionReq.client_protocol != openSessionResp.serverProtocolVersion) {
            std::cerr << "ERROR: protocol version does not match" << endl;
            return false;
        }

        m_SessionHandle = openSessionResp.sessionHandle;
        return true;
    }

    bool SubmitQuery(const string& in_query, TOperationHandle& out_OpHandle){

        // execute the query
        TExecuteStatementReq execStmtReq;
        TExecuteStatementResp execStmtResp;
        execStmtReq.__set_sessionHandle(m_SessionHandle);
        execStmtReq.__set_statement(in_query);
        m_client->ExecuteStatement(execStmtResp, execStmtReq);
        std::cerr << "Execute Statement Status: " << LogTStatusToString(execStmtResp.status) << endl;
        out_OpHandle = execStmtResp.operationHandle;
        return  (execStmtResp.status.statusCode == TStatusCode::SUCCESS_STATUS) ||
            (execStmtResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS);
    }

    void GetResultsetMetaData(const TOperationHandle& opHandle){
        TGetResultSetMetadataReq metadataReq;
        TGetResultSetMetadataResp metadataResp;
        metadataReq.__set_operationHandle(opHandle);
        m_client->GetResultSetMetadata(metadataResp, metadataReq);
        std::cerr << "Get Result Set Metadata Status: " << LogTStatusToString(metadataResp.status) << endl;
        std::cout << "Column Metadata: " << LogTTableSchemaToString(metadataResp.schema) << endl;
    }

    void FetchResultSet(const TOperationHandle& opHandle){
         // fetch results, blocking call
        TFetchResultsReq fetchResultsReq;
        TFetchResultsResp fetchResultsResp;
        fetchResultsReq.__set_operationHandle(opHandle);
        fetchResultsReq.__set_maxRows(10000);

        m_client->FetchResults(fetchResultsResp, fetchResultsReq);

        std::cerr << "Fetch Result Status: " << LogTStatusToString(fetchResultsResp.status) << endl;
        std::cerr << "hasMoreRows: " << fetchResultsResp.hasMoreRows << endl;

        const vector<TColumn>& cols = fetchResultsResp.results.columns;
        size_t nColumns = cols.size();
        ostream_iterator<TColumn> t(std::cout, "\n");
        for (size_t i = 0; i < nColumns; i++) {
            std::cout << cols[i] << endl;
        }


        //copy(cols.begin(), cols.end(), t);
        //copy(cols.begin(), cols.end(), ostream_iterator<TColumn>(std::cout, ", "));
    
    }

    void CloseSession(){
        TCloseSessionReq closeSessionReq;
        TCloseSessionResp closeSessionResp;
        closeSessionReq.__set_sessionHandle(m_SessionHandle);
        m_client->CloseSession(closeSessionResp, closeSessionReq);
        std::cerr << "Close Session: " << LogTStatusToString(closeSessionResp.status) << endl;
    }
};



// The TStatusCode names.
static const char* TSTATUS_CODE_NAMES[] =
{
    "SUCCESS_STATUS",
    "SUCCESS_WITH_INFO_STATUS",
    "STILL_EXECUTING_STATUS",
    "ERROR_STATUS",
    "INVALID_HANDLE_STATUS"
};


string LogTTableSchemaToString(TTableSchema& schema){
    ostringstream ss;
    size_t sz = schema.columns.size();
    ss << "TableSchema {\n";
    for (size_t i = 0; i < sz; i++){
        ss << "\t[name:" << schema.columns[i].columnName;
        ss << ", position: " << schema.columns[i].position << "]\n";
    }
    ss << "}\n";
    return ss.str();
}
string LogTStatusToString(apache::hive::service::cli::thrift::TStatus& in_status)
{
    ostringstream ss;

    ss << "\nTStatus {";
    ss << "\n  statusCode=" << TSTATUS_CODE_NAMES[in_status.statusCode];
    ss << "\n  infoMessages=";

    for (std::vector<string>::iterator currInfoMsg = in_status.infoMessages.begin();
        in_status.infoMessages.end() != currInfoMsg;
        ++currInfoMsg)
    {
        ss <<  "\"" << *currInfoMsg << "\"";
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
int main(int argc, char* argv[]) {

    string host = "192.168.202.101";
    int port = 10000;
    HS2Client client(host, port);

    client.OpenSession();


    const char* queries[] = {
        "select * from bigint_table",
        "show tables"
    };
    size_t sz = sizeof(queries) / sizeof(char*);
    for (size_t i = 0; i < sz; i++) {
        std::cerr << "Running query " << queries[i] << std::endl;
        TOperationHandle opHandle;
        bool bSuccess = client.SubmitQuery(queries[i], opHandle);
        if (bSuccess) {
            client.GetResultsetMetaData(opHandle);
            client.FetchResultSet(opHandle);
        }
    }
    client.CloseSession();
	return 0;
}
