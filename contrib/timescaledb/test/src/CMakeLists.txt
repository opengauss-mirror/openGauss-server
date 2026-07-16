set(SOURCES
  adt_tests.cpp
  symbol_conflict.cpp
  test_time_to_internal.cpp
  test_with_clause_parser.cpp
)

include(${PROJECT_SOURCE_DIR}/src/build-defs.cmake)

add_library(${TESTS_LIB_NAME} OBJECT ${SOURCES})

# Since the test library will be linked into the loadable extension
# module, it needs to be compiled as position-independent code
# (e.g., the -fPIC compiler flag for GCC)
set_target_properties(${TESTS_LIB_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Set the MODULE_NAME for the symbol conflict test (see symbol_conflict.c)
target_compile_definitions(${TESTS_LIB_NAME} PUBLIC MODULE_NAME=timescaledb)
target_include_directories(${TESTS_LIB_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(APACHE_ONLY)
  add_definitions(-DAPACHE_ONLY)
endif()

add_subdirectory(bgw)
add_subdirectory(net)
add_subdirectory(telemetry)

add_subdirectory(loader)
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})