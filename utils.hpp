#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <ostream>

namespace apache { namespace hive { namespace service { namespace cli { namespace thrift {
    class TTableSchema;
    class TStatus;
                    class TColumn;
} } } } }


std::ostream &operator<<(std::ostream &os, const
    apache::hive::service::cli::thrift::TColumn &col);
std::string LogTTableSchemaToString
        (const apache::hive::service::cli::thrift::TTableSchema& schema);

std::string LogTStatusToString
        (const apache::hive::service::cli::thrift::TStatus& status);

#endif
