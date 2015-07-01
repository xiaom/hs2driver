#include "Decompressor.hpp"
#include <iostream>
#include <snappy.h>
#include "TCLIService_types.h"

using namespace apache::hive::service::cli::thrift;

class IntegerDecompressor;

class IntegerDecompressor :  public Decompressor{
  public:
    virtual bool Decompress(const TEnColumn& in_col, TColumn& out_col);
  private:
    // do the decoding job: turn binary `enData` into integers
    void decode(const std::string &enData, int size, TI32Column &tI32Column);
};


bool IntegerDecompressor::Decompress(const TEnColumn& in_column, TColumn&
out_column) {

    // @todo: remove compressorName from TEnColumn?
    assert(in_column.compressorName == "snappy" && in_column.type == TTypeId::INT_TYPE);

    switch(in_column.type) {
        case TTypeId::INT_TYPE:
            out_column.__isset.i32Val = true;
            out_column.i32Val.__set_nulls(in_column.nulls);
            std::cerr << "Decompressor is activated"<< std::endl; 
            decode(in_column.enData, in_column.size, out_column.i32Val);
            break;
        default:
            std::cerr << "Decompressor is not implemented for the type: " <<
                    in_column.type << std::endl;
            return false;
    }
    return true;
}

void IntegerDecompressor::decode(
        const std::string& enData,
        int size,
        TI32Column &tI32Column) {
    tI32Column.values.reserve(size);
    // NOTES C++11 can use method data() to access internal array
    // used by vector, which is more efficient
    std::string buf;
    snappy::Uncompress(enData.c_str(), size, &buf);
    int32_t* pv = (int32_t*) buf.c_str();
    for (int i = 0; i < size; pv++) {
      tI32Column.values.push_back(*pv);
    }
    return;
}


Decompressor* Decompressor::Create(const std::string& name){
    if (name == "PIN") {
         return new IntegerDecompressor();
    }
    else {
         return NULL;
    }
}
