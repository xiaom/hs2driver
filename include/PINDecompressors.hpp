#ifndef HS_PINDECOMPRESSOR
#define HS_PINDECOMPRESSOR

#include "Decompressor.hpp"
#include "TCLIService_types.h"

using namespace apache::hive::service::cli::thrift;

class PINDecompressors :  public Decompressor{
  public:
    virtual bool Decompress(const TEnColumn& in_col, TColumn& out_col);
  private:
    // do the decoding job: turn binary `enData` into integers
    void decode(const std::string &enData, int size, TI32Column &tI32Column);
};

#endif
