set(SOURCES
  init.cpp
  ${PROJECT_SOURCE_DIR}/src/extension.cpp
  ${PROJECT_SOURCE_DIR}/src/guc.cpp
)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/src)

add_library(${PROJECT_NAME}-mock-1 MODULE ${SOURCES} config.h)
add_library(${PROJECT_NAME}-mock-2 MODULE ${SOURCES} config.h)
add_library(${PROJECT_NAME}-mock-3 MODULE ${SOURCES} config.h)
#mock-4 will be broken mismatched .so
add_library(${PROJECT_NAME}-mock-4 MODULE ${SOURCES} config.h)
add_library(${PROJECT_NAME}-mock-5 MODULE ${SOURCES} config.h)
add_library(${PROJECT_NAME}-mock-broken MODULE ${SOURCES} config.h)
add_library(${PROJECT_NAME}-mock-6 MODULE ${SOURCES} config.h)


target_compile_definitions(${PROJECT_NAME}-mock-1 PRIVATE TIMESCALEDB_VERSION_MOD="mock-1" BROKEN=0)
target_compile_definitions(${PROJECT_NAME}-mock-2 PRIVATE TIMESCALEDB_VERSION_MOD="mock-2" BROKEN=0)
target_compile_definitions(${PROJECT_NAME}-mock-3 PRIVATE TIMESCALEDB_VERSION_MOD="mock-3" BROKEN=0)
#mock 4 is intentionally incorrect version mod
target_compile_definitions(${PROJECT_NAME}-mock-4 PRIVATE TIMESCALEDB_VERSION_MOD="mock-4-mismatch" BROKEN=0)
target_compile_definitions(${PROJECT_NAME}-mock-5 PRIVATE TIMESCALEDB_VERSION_MOD="mock-5" BROKEN=0)
target_compile_definitions(${PROJECT_NAME}-mock-broken PRIVATE TIMESCALEDB_VERSION_MOD="mock-broken" BROKEN=1)
target_compile_definitions(${PROJECT_NAME}-mock-6 PRIVATE TIMESCALEDB_VERSION_MOD="mock-6" BROKEN=0)

foreach(MOCK_VERSION mock-1 mock-2 mock-3 mock-4 mock-broken mock-5 mock-6)
  set_target_properties(${PROJECT_NAME}-${MOCK_VERSION} PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}-${MOCK_VERSION}
    PREFIX "")

  install(
    TARGETS ${PROJECT_NAME}-${MOCK_VERSION}
    DESTINATION ${PG_PKGLIBDIR} OPTIONAL)
endforeach(MOCK_VERSION)
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})