#ifndef HS_IDECOMPRESSOR
#define HS_IDECOMPRESSOR

#include <string>
namespace apache { namespace hive { namespace service { namespace cli {
                namespace thrift {
                    class TEnColumn;
                    class TColumn;
} } } } }

/**
* Interface for decompressor
*/
class Decompressor {
public:
    static Decompressor* Create(const std::string& name);
    virtual ~Decompressor() {}
    // return false if in_col cannot be decompressed
    virtual bool Decompress(
            const apache::hive::service::cli::thrift::TEnColumn& in_col,
            apache::hive::service::cli::thrift::TColumn& out_col) = 0;
};

#endif