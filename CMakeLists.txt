cmake_minimum_required(VERSION 2.6)

project(hs2driver)

# turn rpath off by default
SET(CMAKE_SKIP_RPATH  TRUE)

message("Project Dir = ${PROJECT_SOURCE_DIR}")
message("Source Dir = ${CMAKE_SOURCE_DIR} ")



if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=c++0x")
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmakeModules/")

# Find Boost
find_package(Boost 1.53.0 REQUIRED COMPONENTS regex system date_time chrono thread program_options)
include_directories(${Boost_INCLUDE_DIRS})
message("Boost_INCLUDE_DIRS: " ${Boost_INCLUDE_DIRS})
message("Boost_LIBRARIES: " ${Boost_LIBRARIES})

# Find Thrift
find_package(Thrift REQUIRED )
include_directories(${THRIFT_INCLUDE_DIR})
message("THRIFT_INCLUDE_DIR: " ${THRIFT_INCLUDE_DIR})
message("THRIFT_LIBS: " ${THRIFT_LIBS})

include_directories(include)
add_subdirectory(${CMAKE_SOURCE_DIR}/decompressor)
add_subdirectory(${CMAKE_SOURCE_DIR}/client)

# qh is short for QuerySubmitter for Hive. 
add_executable(qh examples/QuerySubmitter.cpp)
target_link_libraries(qh ${Boost_LIBRARIES} hs2client ${THRIFT_LIBS} decompressor)
# Copy the compressorInfo.json file into the build directory
execute_process(COMMAND cp ${CMAKE_SOURCE_DIR}/compressorInfo.json ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/)
