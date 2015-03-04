#ifndef HS2CLIENT_HPP
#define HS2CLIENT_HPP

#include <TCLIService_types.h>
#include "TCLIService.h"
#include "IDecompressor.hpp"


namespace apache {
    namespace hive {
        namespace service {
            namespace cli {
                namespace thrift {
                    class TOperationHandle;
                    class TCLIServiceClient;
                    class TSessionHandle;
                }
            }

        }
    }
}


typedef const apache::hive::service::cli::thrift::TOperationHandle OpHandle;

class HS2Client{
public:
    HS2Client(const std::string &host, const int port);
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


#endif