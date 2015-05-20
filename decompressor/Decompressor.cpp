#include "Decompressor.hpp"
#include "PINDecompressors.hpp"
#include "TCLIService_types.h"
#include <iostream>

Decompressor* Decompressor::Create(const std::string& name){
    if (name == "PIN") {
         return new PINDecompressors();
    }
    else {
         return NULL;
    }
}
