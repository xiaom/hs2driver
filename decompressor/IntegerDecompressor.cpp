#include "Decompressor.hpp"
#include "TCLIService_types.h"
#include <iostream>

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
    assert(in_column.compressorName == "PIN" && in_column.type == TTypeId::INT_TYPE);

    switch(in_column.type) {
        case TTypeId::INT_TYPE:
            out_column.__isset.i32Val = true;
            out_column.i32Val.__set_nulls(in_column.nulls);
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
    const uint8_t *encodedData = (const uint8_t *)enData.c_str();

    uint8_t *ptr = (uint8_t *)encodedData;

    ptr++;
    uint32_t bitsPerLen = *(uint8_t *)ptr;

    ptr += 1;
    uint32_t min_len = *(uint8_t *)ptr;

    ptr += 1;
    unsigned int lenmask = ~(-1 << bitsPerLen);

    int valPos = 3 + (size * bitsPerLen + 7) / 8;

    unsigned int lenAcc = 0;
    unsigned int valAcc = 0;
    int lenBits = 0;
    int valBits = 0;
    int valWid = 0;
    unsigned int value = 0;

    for (int i = 0; i < size; i++) {

        for (; lenBits < bitsPerLen; lenBits += 8) {
            lenAcc |= *ptr++ << lenBits;
        }
        valWid = (lenAcc & lenmask) + min_len - 1;
        lenAcc >>= bitsPerLen;
        lenBits -= bitsPerLen;

        if (valWid == -1) {
            tI32Column.values.push_back(0);
            continue;
        }

        for (; valBits < valWid; valBits += 8) {
            valAcc |= encodedData[valPos++] << valBits;
        }
        valBits -= valWid;

        value = valAcc & ~(-1 << valWid) ^ (1 << valWid);

        valAcc = (unsigned int)encodedData[valPos - 1] >> (8 - valBits);

        value = value & 1 ? 1 + (value >> 1) : -(value >> 1);
        tI32Column.values.push_back((int32_t)value);
    }
}


Decompressor* Decompressor::Create(const std::string& name){
    if (name == "PIN") {
         return new IntegerDecompressor();
    }
    else {
         return NULL;
    }
}
