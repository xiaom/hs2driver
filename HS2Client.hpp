#ifndef HS2CLIENT_HPP
#define HS2CLIENT_HPP

class IDecompressor;

namespace apache { namespace hive { namespace service { namespace cli { namespace thrift {
    class TSessionHandle;
    class TOperationHandle;
    class TCLIServiceClient;
    class TSessionHandle;
} } } } }

typedef const apache::hive::service::cli::thrift::TOperationHandle OpHandle;

class HS2ClientImpl;

class HS2Client{
public:
    HS2Client(const std::string &host, const int port);
    ~HS2Client() {}
    void SetDecompressor(const std::string& compressorName);
    // return false if failed
    bool OpenSession();

    bool SubmitQuery(const std::string &in_query, OpHandle&
    out_OpHandle);
    void GetResultsetMetaData(const OpHandle& opHandle);
    void FetchResultSet(const OpHandle&  opHandle) ;
    bool CloseSession();

private:
    HS2ClientImpl* m_client;
};


#endif