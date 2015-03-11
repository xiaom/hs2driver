#ifndef UTILS_HPP
#define UTILS_HPP

namespace apache { namespace hive { namespace service { namespace cli { namespace thrift {
    class TTableSchema;
    class TSessionHandle;
} } } } }


std::string LogTTableSchemaToString
        (apache::hive::service::cli::thrift::TTableSchema
        &schema);
std::string LogTStatusToString(apache::hive::service::cli::thrift::TStatus
&in_status);

ostream &operator<<(std::ostream &os, const
apache::hive::service::cli::thrift::TColumn &col) {
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

    for (std::vector<std::string>::iterator currInfoMsg = in_status.infoMessages.begin();
         in_status.infoMessages.end() != currInfoMsg;
         ++currInfoMsg) {
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

#endif
