#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 2.6)

set(CMAKE_CXX_FLAGS "-stdlib=libc++")
project(hs2driver)

message("Project Dir = ${PROJECT_SOURCE_DIR}")
message("Source Dir = ${CMAKE_SOURCE_DIR} ")

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
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
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
include_directories(${THRIFT_INCLUDE_DIR} TCLIServiceClient)

# TCLIService
set (TCLISERVICE_CPP_FILES
    TCLIServiceClient/TCLIService.cpp
    TCLIServiceClient/TCLIService_constants.cpp
    TCLIServiceClient/TCLIService_types.cpp
    )

# TARGETS
add_executable(QuerySubmitter QuerySubmitter.cpp ${TCLISERVICE_CPP_FILES})

target_link_libraries(QuerySubmitter ${Boost_LIBRARIES} ${THRIFT_LIBS})
