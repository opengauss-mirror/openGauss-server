set(SOURCES
agg_bookend.cpp
base64_compat.cpp
tsdb_dsm.cpp
tsdb_shm.cpp
tsdb_extension.cpp
tsdb_head.cpp
tsdb_static.cpp
tsdb_static2.cpp
tsdb.cpp
func_cache.cpp
cache.cpp
cache_invalidate.cpp
catalog.cpp
continuous_agg.cpp
chunk.cpp
chunk_adaptive.cpp
chunk_constraint.cpp
chunk_dispatch.cpp
chunk_dispatch_plan.cpp
chunk_dispatch_state.cpp
chunk_index.cpp
chunk_insert_state.cpp
constraint_aware_append.cpp
cross_module_fn.cpp
copy.cpp
compression_chunk_size.cpp
compression_with_clause.cpp
debug_wait.cpp
dimension.cpp
dimension_slice.cpp
dimension_vector.cpp
estimate.cpp
event_trigger.cpp
extension.cpp
gapfill.cpp
guc.cpp
histogram.cpp
hypercube.cpp
hypertable.cpp
hypertable_cache.cpp
hypertable_compression.cpp
hypertable_insert.cpp
hypertable_restrict_info.cpp
indexing.cpp
init.cpp
interval.cpp
metadata.cpp
jsonb_utils.cpp
license_guc.cpp
partitioning.cpp
planner.cpp
plan_expand_hypertable.cpp
plan_add_hashagg.cpp
plan_agg_bookend.cpp
plan_partialize.cpp
process_utility.cpp
scanner.cpp
scan_iterator.cpp
sort_transform.cpp
subspace_store.cpp
tablespace.cpp
time_bucket.cpp
custom_type_cache.cpp
trigger.cpp
utils.cpp
version.cpp
with_clause_parser.cpp
)

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
  list(APPEND SOURCES
    debug_wait.cpp)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

include(build-defs.cmake)
set(GITCOMMIT_H ${CMAKE_CURRENT_BINARY_DIR}/gitcommit.h)

if (WIN32)
  add_custom_command(
    OUTPUT ${GITCOMMIT_H}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} -E echo_append "#define EXT_GIT_COMMIT " > ${GITCOMMIT_H}
    COMMAND (${GIT_EXECUTABLE} describe --abbrev=4 --dirty --always --tags 2> $null || call && echo "${PROJECT_VERSION_MOD}") >> ${GITCOMMIT_H}
    COMMENT "Generating gitcommit.h"
    VERBATIM)
else ()
  add_custom_command(
    OUTPUT ${GITCOMMIT_H}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} -E echo_append "#define EXT_GIT_COMMIT " > ${GITCOMMIT_H}
    COMMAND sh -c "if `${GIT_EXECUTABLE} status > /dev/null 2>&1`; then ${GIT_EXECUTABLE} describe --abbrev=4 --dirty --always --tags ; else echo ${PROJECT_VERSION_MOD} ; fi" >> ${GITCOMMIT_H}
    COMMENT "Generating gitcommit.h"
    VERBATIM)
endif (WIN32)

if (CMAKE_BUILD_TYPE MATCHES Debug)
add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H} $<TARGET_OBJECTS:${TESTS_LIB_NAME}>)
else ()
add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H})
endif ()

if (SEND_TELEMETRY_DEFAULT)
  set(TELEMETRY_DEFAULT TELEMETRY_BASIC)
else ()
  set(TELEMETRY_DEFAULT TELEMETRY_OFF)
endif ()

set_target_properties(${PROJECT_NAME} PROPERTIES
  OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
  PREFIX "")

install(
  TARGETS ${PROJECT_NAME}
  DESTINATION ${PG_PKGLIBDIR})

if (USE_OPENSSL)
  set(TS_USE_OPENSSL ${USE_OPENSSL})
  target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
  target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})
endif (USE_OPENSSL)

configure_file(config.h.in config.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(bgw)
add_subdirectory(chunk_append)
add_subdirectory(net)
add_subdirectory(telemetry)
add_subdirectory(loader)
add_subdirectory(bgw_policy)
add_subdirectory(compat)
add_subdirectory(import)

set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${CMAKE_SOURCE_DIR}/tsl/src ${Third_party_library})