set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/metadata.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/test_privacy.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/test_telemetry.cpp
)

target_sources(${TESTS_LIB_NAME} PRIVATE ${SOURCES})
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})