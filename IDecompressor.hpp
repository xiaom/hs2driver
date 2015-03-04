#ifndef HS_IDECOMPRESSOR
#define HS_IDECOMPRESSOR


namespace apache {
    namespace hive {
        namespace service {
            namespace cli {
                namespace thrift {
                    class TEnColumn;
                    class TColumn;
                }
            }

        }
    }
}
/**
* Interface for decompressor
*/
class IDecompressor {
public:
    virtual ~IDecompressor() {}
    virtual void Decompress(
            const apache::hive::service::cli::thrift::TEnColumn& in_col,
            apache::hive::service::cli::thrift::TColumn& out_col) = 0;
};

#endif