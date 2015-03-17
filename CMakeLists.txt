cmake_minimum_required(VERSION 2.6)

project(hs2driver)

message("Project Dir = ${PROJECT_SOURCE_DIR}")
message("Source Dir = ${CMAKE_SOURCE_DIR} ")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmakeModules/")
# Find Boost
if(MSVC)
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
else()
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
endif()

find_package(Boost 1.53.0 REQUIRED COMPONENTS regex system date_time chrono thread program_options)
include_directories(${Boost_INCLUDE_DIRS})

if(CMAKE_COMPILER_IS_GNUCXX)
    #set(CMAKE_EXE_LINKER_FLAGS "-lrt -lpthread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=c++0x")
endif()
if(MSVC)
    set(CMAKE_CXX_FLAGS "/EHsc")
endif()

if(MSVC)
    # ask boost to not to auto link any libraries
    add_definitions(-DBOOST_ALL_NO_LIB)
    # use static libs with zookeeper
    add_definitions(-DUSE_STATIC_LIB)
else()
    # use static version for now
    # ---
    # add_definitions(-DBOOST_ALL_DYN_LINK)
    # ---
    add_definitions(-DBOOST_ALL_NO_LIB)
    add_definitions(-DUSE_STATIC_LIB)
endif()

# Find Thrift
find_package(Thrift REQUIRED )
include_directories(${THRIFT_INCLUDE_DIR})

include_directories(include)

# Copy the compressorInfo.json file into the build directory
execute_process(COMMAND cp ${CMAKE_SOURCE_DIR}/compressorInfo.json ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/)

add_subdirectory(${CMAKE_SOURCE_DIR}/decompressor)
add_subdirectory(${CMAKE_SOURCE_DIR}/client)

add_executable(QuerySubmitter examples/QuerySubmitter.cpp)
target_link_libraries(QuerySubmitter ${Boost_LIBRARIES} ${THRIFT_LIBS} hs2client decompressor)
