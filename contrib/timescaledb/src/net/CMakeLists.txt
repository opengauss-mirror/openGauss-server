set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/conn.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/conn_plain.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/http.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/http_response.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/http_request.cpp
)

if (USE_OPENSSL)
  list(APPEND SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/conn_ssl.cpp)
endif (USE_OPENSSL)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})