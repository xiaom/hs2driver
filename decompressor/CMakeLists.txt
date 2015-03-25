if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_CXX_FLAGS "-fPIC -std=c++0x")
endif()

set (DECOMPRESSOR_SOURCE
    ${CMAKE_CURRENT_SOURCE_DIR}/IntegerDecompressor.cpp
    )

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../include )
add_library(decompressor SHARED ${DECOMPRESSOR_SOURCE})
install(TARGETS decompressor DESTINATION lib)
