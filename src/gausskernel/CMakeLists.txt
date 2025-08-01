#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/bootstrap
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/ddes
    ${CMAKE_CURRENT_SOURCE_DIR}/cbb
    ${CMAKE_CURRENT_SOURCE_DIR}/dbmind
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer
    ${CMAKE_CURRENT_SOURCE_DIR}/process
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime
    ${CMAKE_CURRENT_SOURCE_DIR}/security
)

add_subdirectory(optimizer)
add_subdirectory(storage)
add_subdirectory(cbb)
add_subdirectory(dbmind)
add_subdirectory(bootstrap)
add_subdirectory(process)
add_subdirectory(runtime)
add_subdirectory(security)
add_subdirectory(ddes)

# gaussdb
SET(llvm_libs_var "")
if(${ENABLE_LLVM_COMPILE})
    execute_process(
        COMMAND ${LLVM_CONFIG} --libs
        RESULT_VARIABLE llvm_libs_ret
        OUTPUT_VARIABLE llvm_libs_var
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif()
separate_arguments(llvm_libs UNIX_COMMAND ${llvm_libs_var})

set(gaussdb_objects
    $<TARGET_OBJECTS:common_backend_catalog>
    $<TARGET_OBJECTS:common_backend_client_logic>
    $<TARGET_OBJECTS:common_backend_lib>
    $<TARGET_OBJECTS:common_backend_libpq>
    $<TARGET_OBJECTS:common_backend_nodes>
    $<TARGET_OBJECTS:common_backend_parser>
    $<TARGET_OBJECTS:common_backend_port>
    $<TARGET_OBJECTS:common_backend_regex>
    $<TARGET_OBJECTS:common_backend_tsearch>
    $<TARGET_OBJECTS:common_backend_utils_adt>
    $<TARGET_OBJECTS:common_backend_utils_cache>
    $<TARGET_OBJECTS:common_backend_utils_error>
    $<TARGET_OBJECTS:common_backend_utils_fmgr>
    $<TARGET_OBJECTS:common_backend_utils_gsplsql>
    $<TARGET_OBJECTS:common_backend_utils_hash>
    $<TARGET_OBJECTS:common_backend_utils_init>
    $<TARGET_OBJECTS:common_backend_utils_matrix>
    $<TARGET_OBJECTS:common_backend_utils_mb>
    $<TARGET_OBJECTS:common_backend_utils_misc>
    $<TARGET_OBJECTS:common_backend_utils_mmgr>
    $<TARGET_OBJECTS:common_backend_utils_resowner>
    $<TARGET_OBJECTS:common_backend_utils_sort>
    $<TARGET_OBJECTS:common_backend_utils_time>
    $<TARGET_OBJECTS:common_backend_utils>
    $<TARGET_OBJECTS:common_backend_utils_misc_guc>
    $<TARGET_OBJECTS:gausskernel_bootstrap>
    $<TARGET_OBJECTS:gausskernel_cbb_extension_connector>
    $<TARGET_OBJECTS:gausskernel_cbb_extension_connector_odbc>
    $<TARGET_OBJECTS:gausskernel_cbb_extension_foreign>
    $<TARGET_OBJECTS:gausskernel_cbb_grpc>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_utils>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_unique_sql>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_workload>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_event>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_user>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_percentile>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_wdr>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_slow_query>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_ash>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_capture_view>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_statement>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_func_control>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_gs_stack>
    $<TARGET_OBJECTS:gausskernel_cbb_instruments_trace>
    $<TARGET_OBJECTS:gausskernel_cbb_communication_libcomm_core>
    $<TARGET_OBJECTS:gausskernel_cbb_communication_lib_hcom4db>
    $<TARGET_OBJECTS:gausskernel_cbb_communication_comm_proxy>
    $<TARGET_OBJECTS:gausskernel_cbb_communication_libcomm_utils>
    $<TARGET_OBJECTS:gausskernel_cbb_communication>
    $<TARGET_OBJECTS:gausskernel_cbb_workload>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_aes>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_biginteger>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_bloomfilter>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_debug>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_gssignal>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_hll>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_partition>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_ssl>
    $<TARGET_OBJECTS:gausskernel_cbb_utils_zfiles>
    $<TARGET_OBJECTS:gausskernel_cbb_bbox>
    $<TARGET_OBJECTS:gausskernel_optimizer_geqo>
    $<TARGET_OBJECTS:gausskernel_optimizer_path>
    $<TARGET_OBJECTS:gausskernel_optimizer_plan>
    $<TARGET_OBJECTS:gausskernel_optimizer_prep>
    $<TARGET_OBJECTS:gausskernel_optimizer_util_learn>
    $<TARGET_OBJECTS:gausskernel_optimizer_util>
    $<TARGET_OBJECTS:gausskernel_optimizer_rewrite>
    $<TARGET_OBJECTS:gausskernel_optimizer_aioptimizer>
    $<TARGET_OBJECTS:gausskernel_optimizer_commands>
    $<TARGET_OBJECTS:gausskernel_optimizer_commands_sequence>
    $<TARGET_OBJECTS:gausskernel_optimizer_sqladvisor>
    $<TARGET_OBJECTS:gausskernel_optimizer_sqlpatch>
    $<TARGET_OBJECTS:gausskernel_optimizer_gplanmgr>
    $<TARGET_OBJECTS:gausskernel_process_datasource>
    $<TARGET_OBJECTS:gausskernel_process_job>
    $<TARGET_OBJECTS:gausskernel_process_main>
    $<TARGET_OBJECTS:gausskernel_process_postmaster>
    $<TARGET_OBJECTS:gausskernel_process_tcop>
    $<TARGET_OBJECTS:gausskernel_process_threadpool>
    $<TARGET_OBJECTS:gausskernel_process_globalplancache>
    $<TARGET_OBJECTS:gausskernel_process_stream>
    $<TARGET_OBJECTS:gausskernel_dbmind_kernel>
    $<TARGET_OBJECTS:gausskernel_db4ai_commands>
    $<TARGET_OBJECTS:gausskernel_db4ai_catalog>
    $<TARGET_OBJECTS:gausskernel_db4ai_executor>
    $<TARGET_OBJECTS:gausskernel_db4ai_executor_algorithms>
    $<TARGET_OBJECTS:gausskernel_db4ai_executor_algorithm_bayes>
    $<TARGET_OBJECTS:gausskernel_db4ai_executor_optimize_gd>
    $<TARGET_OBJECTS:gausskernel_runtime_executor>
    $<TARGET_OBJECTS:gausskernel_runtime_opfusion>
    $<TARGET_OBJECTS:gausskernel_runtime_vecexecutor_vecnode>
    $<TARGET_OBJECTS:gausskernel_runtime_vecexecutor_vectorsonic_sonicarray>
    $<TARGET_OBJECTS:gausskernel_runtime_vecexecutor_vectorsonic>
    $<TARGET_OBJECTS:gausskernel_runtime_vecexecutor>
    $<TARGET_OBJECTS:gausskernel_security_iprange>
    $<TARGET_OBJECTS:gausskernel_security_query_anomaly>
    $<TARGET_OBJECTS:gausskernel_security_gs_policy>
    $<TARGET_OBJECTS:gausskernel_storage_access_cbtree>
    $<TARGET_OBJECTS:gausskernel_storage_access_common>
    $<TARGET_OBJECTS:gausskernel_security_gs_ledger>
    $<TARGET_OBJECTS:gausskernel_security_tde_key_management>
    $<TARGET_OBJECTS:gausskernel_ddes_dms_adapter>
)

if("${ENABLE_BBOX}" STREQUAL "OFF")
list(REMOVE_ITEM gaussdb_objects 
    $<TARGET_OBJECTS:gausskernel_cbb_bbox>
)
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
list(APPEND gaussdb_objects
    $<TARGET_OBJECTS:gausskernel_storage_replication_dcf>
)
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    list(APPEND gaussdb_objects
    $<TARGET_OBJECTS:gausskernel_optimizer_util>
    $<TARGET_OBJECTS:common_backend_pgxc_single_locator>
    $<TARGET_OBJECTS:common_backend_pgxc_single_nodemgr>
    $<TARGET_OBJECTS:common_backend_pgxc_single_pool>
    $<TARGET_OBJECTS:contrib_gc_fdw>
    )
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    list(APPEND gaussdb_objects
    $<TARGET_OBJECTS:gausskernel_optimizer_statistics>
    )
endif()

list(APPEND gaussdb_objects
    $<TARGET_OBJECTS:common_backend_pgxc_single_barrier>
    $<TARGET_OBJECTS:common_backend_pgxc_single_copy>
    $<TARGET_OBJECTS:gausskernel_storage_access_heap>
    $<TARGET_OBJECTS:gausskernel_storage_access_index>
    $<TARGET_OBJECTS:gausskernel_storage_access_nbtree>
    $<TARGET_OBJECTS:gausskernel_storage_access_psort>
    $<TARGET_OBJECTS:gausskernel_storage_access_rmgrdesc>
    $<TARGET_OBJECTS:gausskernel_storage_access_transam_parallel_recovery>
    $<TARGET_OBJECTS:gausskernel_storage_access_transam_extreme_rto>
    $<TARGET_OBJECTS:gausskernel_storage_access_transam_ondemand_extreme_rto>
    $<TARGET_OBJECTS:gausskernel_storage_access_transam>
    $<TARGET_OBJECTS:gausskernel_storage_access_obs>
    $<TARGET_OBJECTS:gausskernel_storage_access_hash>
    $<TARGET_OBJECTS:gausskernel_storage_access_spgist>
    $<TARGET_OBJECTS:gausskernel_storage_access_gist>
    $<TARGET_OBJECTS:gausskernel_storage_access_gin>
    $<TARGET_OBJECTS:gausskernel_storage_access_hbstore>
    $<TARGET_OBJECTS:gausskernel_storage_access_redo>
    $<TARGET_OBJECTS:gausskernel_storage_access_redo_standby_read>
    $<TARGET_OBJECTS:gausskernel_storage_access_table>
    $<TARGET_OBJECTS:gausskernel_storage_bulkload>
    $<TARGET_OBJECTS:gausskernel_storage_replication_logical>
    $<TARGET_OBJECTS:gausskernel_storage_replication_heartbeat_libpq>
    $<TARGET_OBJECTS:gausskernel_storage_replication_heartbeat>
    $<TARGET_OBJECTS:gausskernel_storage_replication>
    $<TARGET_OBJECTS:gausskernel_storage_buffer>
    $<TARGET_OBJECTS:gausskernel_storage_nvm>
    $<TARGET_OBJECTS:gausskernel_storage_gs_uwal>
    $<TARGET_OBJECTS:gausskernel_storage_cmgr>
    $<TARGET_OBJECTS:gausskernel_storage_cstore_compression>
    $<TARGET_OBJECTS:gausskernel_storage_cstore>
    $<TARGET_OBJECTS:gausskernel_storage_file>
    $<TARGET_OBJECTS:gausskernel_storage_freespace>
    $<TARGET_OBJECTS:gausskernel_storage_ipc>
    $<TARGET_OBJECTS:gausskernel_storage_large_object>
    $<TARGET_OBJECTS:gausskernel_storage_lmgr>
    $<TARGET_OBJECTS:gausskernel_storage_page>
    $<TARGET_OBJECTS:gausskernel_storage_remote>
    $<TARGET_OBJECTS:gausskernel_storage_smgr>
    $<TARGET_OBJECTS:gausskernel_storage_dss>
    $<TARGET_OBJECTS:gausskernel_storage_smgr_segment>
    $<TARGET_OBJECTS:gausskernel_storage_smgr_cfs>
    $<TARGET_OBJECTS:gausskernel_storage_access_transam_smb_recovery>
    $<TARGET_OBJECTS:gausskernel_storage_access_ustore>
    $<TARGET_OBJECTS:gausskernel_storage_access_ustore_undo>
    $<TARGET_OBJECTS:gausskernel_storage_access_ubtree>
    $<TARGET_OBJECTS:gausskernel_storage_access_ubtree_pcr>
    $<TARGET_OBJECTS:gausskernel_storage_access_datavec>
    $<TARGET_OBJECTS:gausskernel_storage_tcap>
    $<TARGET_OBJECTS:gausskernel_storage_sync>
    $<TARGET_OBJECTS:gausskernel_storage_dorado_operation>
    $<TARGET_OBJECTS:gausskernel_storage_xlog_share_storage>
    $<TARGET_OBJECTS:common_timezone>
    $<TARGET_OBJECTS:contrib_file_fdw>
    $<TARGET_OBJECTS:contrib_log_fdw>
    $<TARGET_OBJECTS:contrib_jiebatokenizer>
)

if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND gaussdb_objects
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_concurrency_control>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_infra>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_infra_config>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_infra_containers>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_infra_stats>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_infra_synchronization>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_memory>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_memory_garbage_collector>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_storage>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_storage_index>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_storage_index_masstree>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_storage_sentinel>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_checkpoint>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_common>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_recovery>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_statistics>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_transaction>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_transaction_logger>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_transaction_logger_group_synchronous_redo_log>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_system_transaction_logger_synchronous_redo_log>
        $<TARGET_OBJECTS:gausskernel_storage_mot_core_utils>
        $<TARGET_OBJECTS:gausskernel_storage_mot_fdw_adapter>
        $<TARGET_OBJECTS:gausskernel_storage_mot_jit_exec>
    )
endif()

if("${ENABLE_HTAP}" STREQUAL "ON")
    list(APPEND gaussdb_objects
        $<TARGET_OBJECTS:gausskernel_storage_htap>
    )
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    list(APPEND gaussdb_objects
        $<TARGET_OBJECTS:gausskernel_storage_test>
    )
endif()

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND gaussdb_objects
        $<TARGET_OBJECTS:gausskernel_runtime_codegen_executor>
        $<TARGET_OBJECTS:gausskernel_runtime_codegen_vecexecutor>
        $<TARGET_OBJECTS:gausskernel_runtime_codegen_codegenutil>
        $<TARGET_OBJECTS:gausskernel_runtime_codegen>
    )
endif()

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
   list(APPEND gaussdb_objects
       $<TARGET_OBJECTS:gausskernel_storage_access_archive>
   )
endif()

set(gaussdb_objects ${gaussdb_objects} ${gaussdb_server_objects})

list(APPEND gaussdb_objects
    $<TARGET_OBJECTS:common_interfaces_libpq>
    $<TARGET_OBJECTS:lib_gstrace_common>
    $<TARGET_OBJECTS:gausskernel_storage_matrix_mem>
)
set(gaussdb_LINK_OPTIONS -fopenmp ${BIN_LINK_OPTIONS} ${HOTPATCH_ATOMIC_LDS} -Wl,-E)

set(gaussdb_LINK_DIRS "")
message("******${gaussdb_server_lib}******")
set(gaussdb_DEPEND_LIST config_static pgport_srv_static alarmclient_static plpgsql_static pq_static)
set(gaussdb_LINK_LIBS libconfig.a libpgport_srv.a libalarmclient.a libpq.a libplpgsql.a -fopenmp -lgomp -l${SECURE_C_CHECK})
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" OR "${ENABLE_PRIVATEGAUSS}" STREQUAL "ON")
        list(APPEND gaussdb_LINK_LIBS ${gaussdb_server_lib} libhotpatchclient.a)
        list(APPEND gaussdb_DEPEND_LIST ${gaussdb_server_target} hotpatchclient_static)
    endif()
endif()
list(APPEND gaussdb_LINK_LIBS -lcrypt -ldl -lm -lssl -lcrypto -pthread -lrt)

if(${ENABLE_MEMORY_CHECK})
    list(APPEND gaussdb_LINK_OPTIONS ${MEMCHECK_FLAGS})
    list(APPEND gaussdb_LINK_LIBS ${MEMCHECK_LIBS})
    list(APPEND gaussdb_LINK_DIRS ${MEMCHECK_LINK_DIRECTORIES})
endif()

if(${USE_LIBXML})
    include_directories(${LIBXML_INCLUDE_PATH})
    list(APPEND gaussdb_LINK_LIBS -lxml2)
    list(APPEND gaussdb_LINK_DIRS ${LIBXML_LIB_PATH})
endif()

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON" AND "${ENABLE_OBS}" STREQUAL "ON")
    list(APPEND gaussdb_LINK_LIBS -leSDKOBS -leSDKLogAPI -lpcre -liconv -lnghttp2 -llog4cpp)
endif()

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND gaussdb_LINK_LIBS -lz -lminiunz -lcurl -llz4 -lcjson -l${JEMALLOC_LIB_NAME} -lcgroup -lzstd -lcom_err_gauss -lgssapi_krb5_gauss -lkrb5_gauss -lgssrpc_gauss -lk5crypto_gauss -lkadm5clnt_mit -lkadm5srv_mit -lkdb5 -lkrb5support_gauss -lstdc++ -lboost_thread -lboost_chrono -lboost_system -lboost_atomic -lxml2 -laio -lncurses -ltinfo -latomic)
else()
    list(APPEND gaussdb_LINK_LIBS -lz -lminiunz -lcurl -llz4 -lcjson -l${JEMALLOC_LIB_NAME} -lcgroup -lzstd -lncurses -ltinfo -lboost_thread -lboost_chrono -lboost_system -lboost_atomic)
endif()

if(NOT "${ENABLE_JEMALLOC}" STREQUAL "ON")
    list(REMOVE_ITEM gaussdb_LINK_LIBS -l${JEMALLOC_LIB_NAME})
endif()

include_directories(
    ${LIBCURL_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LIBLLVM_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBHOTPATCH_INCLUDE_PATH}
    ${LZ4_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${JEMALLOC_INCLUDE_PATH}
    ${ZSTD_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
    ${BOOST_INCLUDE_PATH}
    ${NUMA_INCLUDE_PATH}
    ${XGBOOST_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_OPENGS_DIR}/contrib/gc_fdw
    ${PROJECT_OPENGS_DIR}/gausskernel/storage/replication/dcf
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/bin/gs_log
    ${PROJECT_SRC_DIR}/common/timezone
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication/comm_proxy
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication/libcomm_utils
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/query_anomaly
    ${PROJECT_SRC_DIR}/gausskernel/storage/replication
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    include_directories(      
        ${LIBOBS_INCLUDE_PATH}
        ${KERBEROS_INCLUDE_PATH}
    )
endif()

list(APPEND gaussdb_LINK_DIRS 
    ${LIBLLVM_LIB_PATH}
    ${LIBCURL_LIB_PATH}
    ${LIBOPENSSL_LIB_PATH}
    ${ZLIB_LIB_PATH}
    ${LZ4_LIB_PATH}
    ${CJSON_LIB_PATH}
    ${JEMALLOC_LIB_PATH}
    ${ZSTD_LIB_PATH}
    ${LIBCGROUP_LIB_PATH}
    ${NCURSES_LIB_PATH}
    ${CMAKE_BINARY_DIR}/lib
    ${DCF_LIB_PATH}
    ${NUMA_LIB_PATH}
    ${SECURE_LIB_PATH}
    ${BOOST_LIB_PATH}
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND gaussdb_LINK_DIRS
        ${LIBOBS_LIB_PATH}
        ${KERBEROS_LIB_PATH}
    )
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    list(APPEND gaussdb_LINK_LIBS dcf)
endif()
list(FIND MACRO_OPTIONS "-D__USE_NUMA" RET_NUMA)
if(NOT ${RET_NUMA} EQUAL -1)
    list(APPEND gaussdb_LINK_LIBS numa)
endif()
list(APPEND gaussdb_LINK_LIBS ${llvm_libs})

if("${ENABLE_MOT}" STREQUAL "ON")
    include_directories(${MASSTREE_INCLUDE_PATH})
    list(APPEND gaussdb_LINK_DIRS ${MASSTREE_LIB_PATH})
    list(APPEND gaussdb_LINK_LIBS atomic masstree)
endif()

if("${SUPPORT_HOTPATCH}" STREQUAL "yes")
    list(APPEND gaussdb_LINK_LIBS doprapatch)
    list(APPEND gaussdb_LINK_DIRS ${LIBHOTPATCH_LIB_PATH})
endif()
if(NOT "${ENABLE_UT}" STREQUAL "ON")
    list(APPEND gaussdb_LINK_DIRS ${LIBUWAL_LINK_DIRS})
    list(APPEND gaussdb_LINK_OPTIONS ${LIBUWAL_LINK_OPTIONS})

    add_executable(gaussdb ${gaussdb_objects})
    target_link_options(gaussdb PRIVATE ${gaussdb_LINK_OPTIONS} "-Wl,-rpath=/usr/local/softbus/ctrlbus/lib")
if("${ENABLE_RELOCATION}" STREQUAL "ON")
    target_link_options(gaussdb PRIVATE ${gaussdb_LINK_OPTIONS} "LINKER:-q")
endif()
    target_link_directories(gaussdb PRIVATE ${gaussdb_LINK_DIRS})
    target_link_libraries(gaussdb PRIVATE ${gaussdb_LINK_LIBS})
    add_dependencies(gaussdb ${gaussdb_DEPEND_LIST})
    if("${SUPPORT_HOTPATCH}" STREQUAL "yes")
        add_custom_command(TARGET gaussdb POST_BUILD
            COMMAND ${LIBHOTPATCH_TOOL_PATH}/makepatch -CHECK -CRC ${CMAKE_CURRENT_BINARY_DIR}/gaussdb > crc.log
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    endif()
    if(NOT "${TEST_LINK_OPTIONS}" STREQUAL "")
        target_link_libraries(gaussdb PRIVATE ${TEST_LINK_OPTIONS})
    endif()

    install(TARGETS gaussdb RUNTIME DESTINATION bin)
if("${ENABLE_RELOCATION}" STREQUAL "ON")
        install(CODE "execute_process(COMMAND cp gaussdb gaussdb.map WORKING_DIRECTORY \$ENV\{DESTDIR\}${CMAKE_INSTALL_PREFIX}/bin)")
endif()
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(CODE "execute_process(COMMAND ln -sf gaussdb gs_encrypt WORKING_DIRECTORY \$ENV\{DESTDIR\}${CMAKE_INSTALL_PREFIX}/bin)")
    install(CODE "message(\"-- Created symlink: gs_encrypt -> gaussdb\")")
endif()
    if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
        install_symlink(gaussdb gaussmaster ${prefix_home}/bin)
    endif()
else()
    add_library(db SHARED ${gaussdb_objects})
    target_link_options(db PRIVATE ${gaussdb_LINK_OPTIONS})
    target_link_directories(db PRIVATE ${gaussdb_LINK_DIRS})
    target_link_libraries(db PRIVATE ${gaussdb_LINK_LIBS})
    add_dependencies(db ${gaussdb_DEPEND_LIST})
    if(NOT "${TEST_LINK_OPTIONS}" STREQUAL "")
        target_link_libraries(db PRIVATE ${TEST_LINK_OPTIONS})
    endif()
    install(TARGETS db RUNTIME DESTINATION lib)
endif()

