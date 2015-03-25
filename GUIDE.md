# Guide on How to Write a Decompressor


The `Decompressor` interface is defined in `include/Decompressor.hpp`:

```cpp
class Decompressor {
public:
    static Decompressor* Create(const std::string& name);
    virtual ~Decompressor() {}
    virtual bool Decompress(const TEnColumn& in_col, TColumn& out_col) = 0;
};
```

We will talk about the static `Create` method [later](#register-a-decompressor) and focus on the `Decompressor` method first.


The `Decompress` method decompress a compressed column (type `TEnColumn`) to the original column (type `TColumn`).
The `TEnColumn` class and `TColumn` class are generated according to the thrift IDL `service/if/TCLIService.thrift`: 

```thrift
struct TEnColumn {
    1: required binary enData
    2: required binary nulls
    3: required Type type
    4: required i32 size
    5: required String compressorName
}

union TColumn {
  1: TBoolColumn   boolVal      // BOOLEAN
  2: TByteColumn   byteVal      // TINYINT
  3: TI16Column    i16Val       // SMALLINT
  4: TI32Column    i32Val       // INT
  5: TI64Column    i64Val       // BIGINT, TIMESTAMP
  6: TDoubleColumn doubleVal    // FLOAT, DOUBLE
  7: TStringColumn stringVal    // STRING, LIST, MAP, STRUCT, UNIONTYPE, DECIMAL, NULL
  8: TBinaryColumn binaryVal    // BINARY
}
```

## Implement the Decompress Method

To implement the decompress method, we need to do three things:

1. Map from Hive's type system (i.e., `TEnColumn.type`)  to Thrift's type system (by
setting `TColumn.__isset` properly).
2. Copy the `nulls` bitmap from `TEnColumn` to `TColumn`.
3. Decode the compressed binary data `TEnColumn.enData` into an array `TColumn.*Val`, where the type `*` is derived from step 1 and the size of array is specified to `TColumn.size`.

The file `decompressor/IntegerDecompressor.cpp` provides an example implementation for integer data types. We show each steps in the following code snippet:

```cpp
// const TEnColumn& in_column
// TColumn& out_column
switch(in_column.type) {
    case TTypeId::INT_TYPE :
            out_column.__isset.i32Val = true; // step 1
            out_column.i32Val.__set_nulls(in_column.nulls); // step 2
            decode(in_column.enData, in_column.size, out_column.i32Val); // step 3
            ...
}
```

The semantic of step 3 is clear enough. We omit technical details of step 3 here, which depends on the implementation of compressor on server side.

> TODO: Step 1 and Step 2 is actually universal. We should simplify the work for decompressor developers. 

## Register a Decompressor

The decompressor must be registered according to a static factory method `Decompressor::Create` which returns a instance of decompressor.

In our example, it is implemented in `decompressor/IntegerDecompressor.cpp`.

```cpp
Decompressor* Decompressor::Create(const std::string& name){
    if (name == "PIN") {
         return new IntegerDecompressor();
    }
    else {
         return NULL;
    }
}
```

> TODO: a better way that automatic register a decompressor. The plan is to read the
> configuration file and register the class automatically. Need more thoughts for implementing it in C++

## Compressor Configuration

The client configures the compressor according to `compressorInfo.json`. The format is specified in the compressor protocol proposal.

```json
{
	"INT_TYPE": {
		"vendor": "simba",
		"compressorSet": "PIN", 
		"entryClass": "ColumnCompressorImpl"
	}
}
```
 
# Link with the Decompressor

The target of this project is the command-line tool `qh`, which is dynamically linked with two dynamic libraries `libhs2client.[so,dylib]` and `libdecompressor.[so,dylib]`. 

The typical workflow for build new decompressor method would be:

```sh
# Build the query submitter `qh` for the first time
$ cd build
$ cmake ..
$ make

# Develop new compression methods in HiveServer2
# and
# Implement a new decompressor, update `compressorInfo.json` and `compressor/CMakeLists.txt`
# You only need to compile the decompressor 
$ make decompressor
```

