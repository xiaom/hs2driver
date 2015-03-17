#ifndef _HS2DRIVER_HS2CLIENT_HPP_
#define _HS2DRIVER_HS2CLIENT_HPP_

#include "TCLIService_types.h"

typedef apache::hive::service::cli::thrift::TOperationHandle OpHandle;

class HS2Client{
public:
    // Factory method
    static HS2Client* Create(const std::string &host, const int port);

    virtual ~HS2Client() {}

    // return false if error happens

    virtual bool SetDecompressor(const std::string &compressorName) = 0;

    virtual bool OpenSession() = 0;

    virtual bool SubmitQuery(const std::string &in_query, OpHandle&
    out_OpHandle) = 0;


    virtual bool GetResultsetMetaData(const OpHandle& opHandle) = 0;

    virtual bool FetchResultSet(const OpHandle&  opHandle) = 0;

    virtual bool CloseSession() = 0;

};


#endif
