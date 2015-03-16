//
// Created by Xiao Meng on 2015-03-16.
//

#include "utils.hpp"
#include <iostream>
#include <sstream>
#include "TCLIService.h"
using namespace apache::hive::service::cli::thrift;

std::ostream &operator<<(std::ostream &os, const
apache::hive::service::cli::thrift::TColumn &col) {
    os << "[";
    if (col.__isset.byteVal) {
        copy(col.binaryVal.values.begin(), col.binaryVal.values.end(),
                std::ostream_iterator<std::string>(os, ", "));
    } else if (col.__isset.boolVal) {
        copy(col.boolVal.values.begin(), col.boolVal.values.end(),
                std::ostream_iterator<bool>(os, ", "));
    } else if (col.__isset.doubleVal) {
        copy(col.doubleVal.values.begin(), col.doubleVal.values.end(),
                std::ostream_iterator<double>(os, ", "));
    } else if (col.__isset.i16Val) {
        copy(col.i16Val.values.begin(), col.i16Val.values.end(),
                std::ostream_iterator<int16_t>(os, ", "));
    } else if (col.__isset.i32Val) {
        copy(col.i32Val.values.begin(), col.i32Val.values.end(),
                std::ostream_iterator<int32_t>(os, ", "));
    } else if (col.__isset.i64Val) {
        copy(col.i64Val.values.begin(), col.i64Val.values.end(),
                std::ostream_iterator<int64_t>(os, ", "));
    } else if (col.__isset.stringVal) {
        copy(col.stringVal.values.begin(), col.stringVal.values.end(),
                std::ostream_iterator<std::string>(os, ", "));
    } else {
        std::cerr << "Invalid column type" << std::endl;
        exit(-1);
    }
    os << "]";
    return os;
}

// The TStatusCode names.
static const char *TSTATUS_CODE_NAMES[] = {
        "SUCCESS_STATUS", "SUCCESS_WITH_INFO_STATUS", "STILL_EXECUTING_STATUS",
        "ERROR_STATUS", "INVALID_HANDLE_STATUS"};

std::string LogTTableSchemaToString(const TTableSchema &schema) {
    std::ostringstream ss;
    size_t sz = schema.columns.size();
    ss << "TableSchema {\n";
    for (size_t i = 0; i < sz; i++) {
        ss << "\t[name:" << schema.columns[i].columnName;
        ss << ", position: " << schema.columns[i].position << "]\n";
    }
    ss << "}\n";
    return ss.str();
}

std::string LogTStatusToString(const TStatus &status) {
    std::ostringstream ss;
    ss << "\nTStatus {";
    ss << "\n  statusCode=" << TSTATUS_CODE_NAMES[status.statusCode];
    ss << "\n  infoMessages=";

    for (std::vector<std::string>::const_iterator currInfoMsg = status
            .infoMessages.begin();
         status.infoMessages.end() != currInfoMsg;
         ++currInfoMsg) {
        ss << "\"" << *currInfoMsg << "\"";
    }
    ss << "\n  sqlState=" << status.sqlState;
    ss << "\n  errorCode=" << status.errorCode;
    ss << "\n  errorMessage=\"" << status.errorMessage + "\"";
    ss << "\n  __isset.errorCode: " << status.__isset.errorCode;
    ss << "\n  __isset.errorMessage: " << status.__isset.errorMessage;
    ss << "\n  __isset.infoMessages: " << status.__isset.infoMessages;
    ss << "\n  __isset.sqlState: " << status.__isset.sqlState;
    ss << "\n}\n";
    return ss.str();
}

