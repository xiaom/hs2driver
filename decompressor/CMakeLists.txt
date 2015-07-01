if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_CXX_FLAGS "-fPIC -std=c++0x")
endif()

# Find Thrift
find_package(snappy REQUIRED )
include_directories(${snappy_INCLUDE_DIRS})
message("snappy_INCLUDE_DIRS: " ${snappy_INCLUDE_DIRS})
message("snappy_LIBRARIES: " ${snappy_LIBRARIES})

set (DECOMPRESSOR_SOURCE
    ${CMAKE_CURRENT_SOURCE_DIR}/IntegerDecompressor.cpp
    )

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../include ${snappy_INCLUDE_DIRS})
add_library(decompressor SHARED ${DECOMPRESSOR_SOURCE})
target_link_libraries(decompressor ${snappy_LIBRARIES})
install(TARGETS decompressor DESTINATION lib)
