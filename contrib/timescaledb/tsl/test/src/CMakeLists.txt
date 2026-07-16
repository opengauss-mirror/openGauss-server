set(SOURCES
  test_auto_policy.cpp
  test_chunk_stats.cpp
  test_compression.cpp
  test_ddl_hook.cpp
  test_continuous_aggs.cpp
)
include(${PROJECT_SOURCE_DIR}/tsl/src/build-defs.cmake)

add_library(${TSL_TESTS_LIB_NAME} OBJECT ${SOURCES})

# Since the test library will be linked into the loadable extension
# module, it needs to be compiled as position-independent code
# (e.g., the -fPIC compiler flag for GCC)
set_target_properties(${TSL_TESTS_LIB_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_compile_definitions(${TSL_TESTS_LIB_NAME} PUBLIC TS_SUBMODULE)
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})