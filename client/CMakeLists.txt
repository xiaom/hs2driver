if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_CXX_FLAGS "-fPIC -std=c++0x" )
endif()

set (HS2CLIENT_SRC_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/HS2Client.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/TCLIService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/TCLIService_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/TCLIService_types.cpp
    )

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../include )
add_library(hs2client SHARED ${HS2CLIENT_SRC_FILES} )
