set(SOURCES
  init.cpp
  license.cpp
  reorder.cpp
  telemetry.cpp
  partialize_finalize.cpp
  planner.cpp
  tsdb_tsl.cpp
  ${PROJECT_SOURCE_DIR}/src/agg_bookend.cpp
  ${PROJECT_SOURCE_DIR}/src/base64_compat.cpp
  ${PROJECT_SOURCE_DIR}/src/cache.cpp
  ${PROJECT_SOURCE_DIR}/src/cache_invalidate.cpp
  ${PROJECT_SOURCE_DIR}/src/catalog.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_adaptive.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_constraint.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_dispatch.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_dispatch_plan.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_dispatch_state.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_index.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_insert_state.cpp
  ${PROJECT_SOURCE_DIR}/src/compression_chunk_size.cpp
  ${PROJECT_SOURCE_DIR}/src/compression_with_clause.cpp
  ${PROJECT_SOURCE_DIR}/src/constraint_aware_append.cpp
  ${PROJECT_SOURCE_DIR}/src/continuous_agg.cpp
  ${PROJECT_SOURCE_DIR}/src/copy.cpp
  ${PROJECT_SOURCE_DIR}/src/cross_module_fn.cpp
  ${PROJECT_SOURCE_DIR}/src/custom_type_cache.cpp
  ${PROJECT_SOURCE_DIR}/src/debug_wait.cpp
  ${PROJECT_SOURCE_DIR}/src/dimension.cpp
  ${PROJECT_SOURCE_DIR}/src/dimension_slice.cpp
  ${PROJECT_SOURCE_DIR}/src/dimension_vector.cpp
  ${PROJECT_SOURCE_DIR}/src/estimate.cpp
  ${PROJECT_SOURCE_DIR}/src/event_trigger.cpp
  ${PROJECT_SOURCE_DIR}/src/extension.cpp
  ${PROJECT_SOURCE_DIR}/src/extension_utils.cpp
  ${PROJECT_SOURCE_DIR}/src/func_cache.cpp

  ${PROJECT_SOURCE_DIR}/src/guc.cpp
  ${PROJECT_SOURCE_DIR}/src/histogram.cpp
  ${PROJECT_SOURCE_DIR}/src/hypercube.cpp
  ${PROJECT_SOURCE_DIR}/src/hypertable.cpp
  ${PROJECT_SOURCE_DIR}/src/hypertable_cache.cpp
  ${PROJECT_SOURCE_DIR}/src/hypertable_compression.cpp
  ${PROJECT_SOURCE_DIR}/src/hypertable_insert.cpp
  ${PROJECT_SOURCE_DIR}/src/hypertable_restrict_info.cpp
  ${PROJECT_SOURCE_DIR}/src/indexing.cpp

  ${PROJECT_SOURCE_DIR}/src/interval.cpp
  ${PROJECT_SOURCE_DIR}/src/jsonb_utils.cpp
  ${PROJECT_SOURCE_DIR}/src/license_guc.cpp
  ${PROJECT_SOURCE_DIR}/src/metadata.cpp
  ${PROJECT_SOURCE_DIR}/src/partitioning.cpp
  ${PROJECT_SOURCE_DIR}/src/planner.cpp
  ${PROJECT_SOURCE_DIR}/src/plan_add_hashagg.cpp
  ${PROJECT_SOURCE_DIR}/src/plan_agg_bookend.cpp
  ${PROJECT_SOURCE_DIR}/src/plan_expand_hypertable.cpp
  ${PROJECT_SOURCE_DIR}/src/plan_partialize.cpp
  ${PROJECT_SOURCE_DIR}/src/process_utility.cpp
  ${PROJECT_SOURCE_DIR}/src/scanner.cpp
  ${PROJECT_SOURCE_DIR}/src/scan_iterator.cpp
  ${PROJECT_SOURCE_DIR}/src/sort_transform.cpp
  ${PROJECT_SOURCE_DIR}/src/subspace_store.cpp
  ${PROJECT_SOURCE_DIR}/src/tablespace.cpp
  ${PROJECT_SOURCE_DIR}/src/time_bucket.cpp

  ${PROJECT_SOURCE_DIR}/src/tsdb.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_dsm.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_extension.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_head.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_shm.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_static.cpp
  ${PROJECT_SOURCE_DIR}/src/tsdb_static2.cpp
  ${PROJECT_SOURCE_DIR}/src/utils.cpp
  ${PROJECT_SOURCE_DIR}/src/version.cpp
  ${PROJECT_SOURCE_DIR}/src/with_clause_parser.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw/job.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw/job_stat.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw/launcher_interface.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw/scheduler.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw/timer.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw_policy/chunk_stats.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw_policy/compress_chunks.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw_policy/drop_chunks.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw_policy/policy.cpp
  ${PROJECT_SOURCE_DIR}/src/bgw_policy/reorder.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_append/chunk_append.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_append/exec.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_append/explain.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_append/planner.cpp
  ${PROJECT_SOURCE_DIR}/src/chunk_append/transform.cpp
  ${PROJECT_SOURCE_DIR}/src/compat/fkeylist.cpp
  ${PROJECT_SOURCE_DIR}/src/compat/tableam.cpp
  ${PROJECT_SOURCE_DIR}/src/compat/tupconvert.cpp
  ${PROJECT_SOURCE_DIR}/src/compat/tuptable.cpp

  ${PROJECT_SOURCE_DIR}/src/import/planner.cpp
  ${PROJECT_SOURCE_DIR}/src/loader/bgw_counter.cpp
  ${PROJECT_SOURCE_DIR}/src/loader/bgw_interface.cpp
  ${PROJECT_SOURCE_DIR}/src/loader/bgw_launcher.cpp
  ${PROJECT_SOURCE_DIR}/src/loader/bgw_message_queue.cpp

  ${PROJECT_SOURCE_DIR}/src/loader/lwlocks.cpp

  ${PROJECT_SOURCE_DIR}/src/net/conn.cpp
  ${PROJECT_SOURCE_DIR}/src/net/conn_plain.cpp

  ${PROJECT_SOURCE_DIR}/src/net/http.cpp
  ${PROJECT_SOURCE_DIR}/src/net/http_request.cpp
  ${PROJECT_SOURCE_DIR}/src/net/http_response.cpp
  ${PROJECT_SOURCE_DIR}/src/telemetry/telemetry.cpp
  ${PROJECT_SOURCE_DIR}/src/telemetry/telemetry_metadata.cpp
  ${PROJECT_SOURCE_DIR}/src/telemetry/uuid.cpp
)

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set(TSL_LIBRARY_NAME ${PROJECT_NAME}-tsl)

include(build-defs.cmake)

if (CMAKE_BUILD_TYPE MATCHES Debug)
add_library(${TSL_LIBRARY_NAME} MODULE ${SOURCES} $<TARGET_OBJECTS:${TSL_TESTS_LIB_NAME}>)
else ()
add_library(${TSL_LIBRARY_NAME} MODULE ${SOURCES})
endif ()

set_target_properties(${TSL_LIBRARY_NAME} PROPERTIES
  OUTPUT_NAME ${TSL_LIBRARY_NAME}-${PROJECT_VERSION_MOD}
  PREFIX "")

target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_TSL)
target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_SUBMODULE)

install(
  TARGETS ${TSL_LIBRARY_NAME}
  DESTINATION ${PG_PKGLIBDIR})

# if (WIN32)
#   target_link_libraries(${PROJECT_NAME} ${PROJECT_NAME}-${PROJECT_VERSION_MOD}.lib)
# endif(WIN32)

add_subdirectory(bgw_policy)
add_subdirectory(compression)
add_subdirectory(continuous_aggs)
add_subdirectory(nodes)
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR}/../..)
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR} CACHE INTERNAL "")
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")
set(Third_party_library $ENV{BINARYLIBS}/kernel/dependency/libobs/comm/include)
add_definitions(-DPGXC)
add_definitions(-DUSE_SPQ)
include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR} ${Third_party_library})