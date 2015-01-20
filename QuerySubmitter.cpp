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

class SessionManager {
	// Session Management
	/// @brief Gets the HS2 session.
	///
	/// @param in_hiveCxn               The HS2 connection used to open a new session if needed.
	///
	/// @param The HS2 session.
	const TSessionHandle& GetSession(TCLIServiceIf& in_hiveCxn);


};

/**
class HS2Client {
    TCLIServiceClient m_client;

public:
    HS2Client():
    void OpenSession(){
        TOpenSessionReq openSessionReq;
        TOpenSessionResp openSessionResp;

    }

    void CloseSession(){

    }
	// Let us do execute first and then take a look at ExecuteWithAsync
    void Execute(string& query){



        if ( execStmtReq.runAsync &&
            (execStmtResp.status == TStatusCode::SUCCESS_STATUS ||
            execStmtResp.status == TStatusCode::SUCCESS_WITH_INFO_STATUS) )
        {
            // If the execution is asynchronous then we will poll for the status until either success or
            // failure is returned.
            TGetOperationStatusReq getOpStatReq;
            TGetOperationStatusResp getOpStatResp;
            getOpStatReq.operationHandle = execStmtResp.operationHandle;

            while (((TOperationState::INITIALIZED_STATE == getOpStatResp.operationState) ||
                (TOperationState::RUNNING_STATE == getOpStatResp.operationState) ||
                (TOperationState::PENDING_STATE == getOpStatResp.operationState)) &&
                HARDY_IS_SUCCESS_STATUS(getOpStatResp.status))
            {
                CheckCancel(&execStmtResp.operationHandle);
                sleep(m_connectionSettings.m_asyncExecPollInterval);
                hiveCxn.GetOperationStatus(getOpStatResp, getOpStatReq);
            }

            execStmtResp.status = getOpStatResp.status;

            // MAINTENANCE NOTE: When connected to an async execute enabled cluster the status code is
            // SUCCESS_STATUS even when the query failed to execute. We can only find out that a query
            // failed to execute by checking the operation state, i.e. if we finished polling with a
            // operation state that is not FINISHED_STATE. If the query fail to execute then we will
            // need to manually set the status code and error message so that the calling method can
            // handle the error appropriately. Unfortunately due to
            // https://issues.apache.org/jira/browse/HIVE-5230 Hive currently doesn't return any
            // meaningful error message when the query fails to execute, so the best we can do for now
            // is just to report that the operation has failed along with the name of the operation
            // state.
            if (TOperationState::FINISHED_STATE != getOpStatResp.operationState)
        		m_fetchResultsReq.operationHandle = execStmtResp.operationHandle;

			m_isExecuted = true;
		}

	}
};
*/


/// @brief Find out how many rows there are in the current chunk of HS2 columnar result set.
///
/// @param in_currRowCtx                Contains information about he current Hive result set
///                                     buffer and the current row in the Hive result set
///                                     buffer.
static inline size_t GetNumRowsInColumnarTResultChunk(
    const apache::hive::service::cli::thrift::TColumn& tColumn)
{
    if (tColumn.__isset.binaryVal) {
        return tColumn.binaryVal.values.size();
    }
    else if (tColumn.__isset.boolVal) {
        return tColumn.boolVal.values.size();
    }
    else if (tColumn.__isset.byteVal) {
        return tColumn.byteVal.values.size();
    }
    else if (tColumn.__isset.doubleVal) {
        return tColumn.doubleVal.values.size();
    }
    else if (tColumn.__isset.i16Val) {
        return tColumn.i16Val.values.size();
    }
    else if (tColumn.__isset.i32Val) {
        return tColumn.i32Val.values.size();
    }
    else if (tColumn.__isset.i64Val) {
        return tColumn.i64Val.values.size();
    }
    else if (tColumn.__isset.stringVal) {
        return tColumn.stringVal.values.size();
    }
    else {
        cerr << "Invalid column type" << endl;
        exit(-1);
    }
}

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
string LogTStatusToString(
    apache::hive::service::cli::thrift::TStatus& in_status)
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
        cerr << "Invalid column type" << endl;
        exit(-1);
    }
    os << "]";
    return os;
}


// HardyTCLIServiceClient is a wrapper
int main(int argc, char* argv[]) {

    string placeholder;
    cerr << "start";
    // create the client
    boost::shared_ptr<TTransport> trans(new TSocket("192.168.202.101", 10000));
    trans.reset(new TBufferedTransport(trans));
    try{
        trans->open();
    }
    catch (TTransportException& e){
        cerr << "Cannot open the socket: " << e.what() << endl;
        cin >> placeholder;
        exit(-1);
    }
    boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(trans));

    TCLIServiceClient client(protocol);

    // open the session
    TOpenSessionReq openSessionReq;
    TOpenSessionResp openSessionResp;


    openSessionReq.__set_client_protocol(TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V7);
    openSessionReq.__set_username("cloudera");

    client.OpenSession(openSessionResp, openSessionReq);
    cerr << "Open Session Status: " << LogTStatusToString(openSessionResp.status) << endl;
    cerr << "Server Protocol Version: " << openSessionResp.serverProtocolVersion << endl;

    if (openSessionReq.client_protocol != openSessionResp.serverProtocolVersion) {
        cerr << "ERROR: protocol version does not match" << endl;
        exit(-1);
    }
    TSessionHandle& sessionHandle = openSessionResp.sessionHandle;

    // execute the query
    string query = "select * from bigint_table";
    TExecuteStatementReq execStmtReq;
    TExecuteStatementResp execStmtResp;
    execStmtReq.__set_sessionHandle(sessionHandle);
    execStmtReq.__set_statement(query);
    // MAINTENANCE NOTE: The confOverlay flag must be set to true for compatibility reasons as some
    // builds of HS2 will return error if this is set to false.
    // execStmtReq.__isset.confOverlay=true;

    client.ExecuteStatement(execStmtResp, execStmtReq);
    cerr << "Execute Statement Status: " << LogTStatusToString(execStmtResp.status) << endl;

    if (execStmtResp.status.statusCode == TStatusCode::SUCCESS_STATUS ||
        execStmtResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS){

        TOperationHandle& opHandle = execStmtResp.operationHandle;

        TGetResultSetMetadataReq metadataReq;
        TGetResultSetMetadataResp metadataResp;
        metadataReq.__set_operationHandle(opHandle);
        client.GetResultSetMetadata(metadataResp, metadataReq);
        cerr << "Get Result Set Metadata Status: " << LogTStatusToString(metadataResp.status) << endl;
        cout << "Column Metadata: " << LogTTableSchemaToString(metadataResp.schema) << endl;

        // fetch results, blocking call
        TFetchResultsReq fetchResultsReq;
        TFetchResultsResp fetchResultsResp;
        fetchResultsReq.__set_operationHandle(opHandle);
        fetchResultsReq.__set_maxRows(10000);
        client.FetchResults(fetchResultsResp, fetchResultsReq);

        cerr << "Fetch Result Status: " << LogTStatusToString(fetchResultsResp.status) << endl;
        cerr << "hasMoreRows: " << fetchResultsResp.hasMoreRows << endl;

        const vector<TColumn>& cols = fetchResultsResp.results.columns;
        int nColumns = cols.size();
        int nRows = GetNumRowsInColumnarTResultChunk(cols[0]);
        ostream_iterator<TColumn> t(cout, "\n");
        for (size_t i = 0; i < nColumns; i++) {
            cout << cols[i] << endl;
        }


        //copy(cols.begin(), cols.end(), t);
        //copy(cols.begin(), cols.end(), ostream_iterator<TColumn>(cout, ", "));


    }
    //cerr << "Get String value: " << fetchResultsResp.results.columns[0].stringVal.values[0] << endl;
    // decompression should go here


    // get the response and we need to decode it now.

    // close the session
    TCloseSessionReq closeSessionReq;
    TCloseSessionResp closeSessionResp;
    closeSessionReq.__set_sessionHandle(sessionHandle);
    client.CloseSession(closeSessionResp, closeSessionReq);
    cerr << "Close Session: " << LogTStatusToString(closeSessionResp.status) << endl;
    cin >> placeholder;
	return 0;
}
