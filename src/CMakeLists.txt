#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(EREPORT_FILE ${PROJECT_OPENGS_DIR}/ereport.txt)
execute_process(COMMAND find src/common/backend -name *.l COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(WRITE ${EREPORT_FILE} ${ereport})
execute_process(COMMAND find src/gausskernel -name *.l COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(APPEND ${EREPORT_FILE} ${ereport})
execute_process(COMMAND find src/common/backend -name *.y COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(APPEND ${EREPORT_FILE} ${ereport})
execute_process(COMMAND find src/gausskernel -name *.y COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(APPEND ${EREPORT_FILE} ${ereport})
execute_process(COMMAND find src/common/backend -name *.cpp COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(APPEND ${EREPORT_FILE} ${ereport})
execute_process(COMMAND find src/gausskernel -name *.cpp COMMAND sort
    WORKING_DIRECTORY ${PROJECT_OPENGS_DIR} OUTPUT_VARIABLE ereport)
file(APPEND ${EREPORT_FILE} ${ereport})

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/include/port/linux.h ${PROJECT_SRC_DIR}/include/pg_config_os.h
)

INCLUDE_DIRECTORIES(${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH} ${ZSTD_INCLUDE_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/bin
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/gausskernel
    ${CMAKE_CURRENT_SOURCE_DIR}/test
)

add_definitions(-DZSTD_STATIC_LINKING_ONLY)

add_subdirectory(bin)
add_subdirectory(common)
add_subdirectory(lib)
add_subdirectory(gausskernel)
add_subdirectory(test)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/ssl/openssl_gsql.cnf DESTINATION share/sslcert/gds RENAME openssl.cnf)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/ssl/openssl_om.cnf DESTINATION share/sslcert/om RENAME openssl.cnf)
endif()
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/ssl/openssl_etcd.cnf DESTINATION share/sslcert/etcd RENAME openssl.cnf)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/ssl/openssl_gsql.cnf DESTINATION share/sslcert/gsql RENAME openssl.cnf)
install(FILES ${CMAKE_BINARY_DIR}/${openGauss}/src/makefiles/pgxs.mk DESTINATION lib/postgresql/pgxs/src/makefiles)
install(FILES ${PROJECT_SRC_DIR}/Makefile.shlib DESTINATION lib/postgresql/pgxs/src/)
install(FILES ${PROJECT_OPENGS_DIR}/config/install-sh DESTINATION lib/postgresql/pgxs/config)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/cm/ DESTINATION include PATTERN "*.h"
    PATTERN "cm_server" EXCLUDE
    PATTERN "cm_agent" EXCLUDE
    PATTERN "libpq-fe.h" EXCLUDE)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/cm/etcdapi.h
    DESTINATION include/postgresql/server/cm)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq DESTINATION include/postgresql/server
    PATTERN "libpq-fs.h libpq-events.h pqexpbuffer.h pqcomm.h libpq-int.h libpq-fe.h" EXCLUDE PATTERN "*.h")
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" OR "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(FILES ${PROJECT_SRC_DIR}/include/libpq/libpq-fs.h DESTINATION include/libpq)
endif()
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq/libpq-events.h 
    ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq/libpq-fe.h
    DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq/pqexpbuffer.h 
    ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq/libpq-int.h
    DESTINATION include/postgresql/internal)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/libpq/pqcomm.h DESTINATION include/postgresql/internal/libpq)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/executor DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/threadpool DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/distributelayer DESTINATION include/postgresql/server
    PATTERN "streamTransportComm.h" EXCLUDE
    PATTERN "streamMain.h" EXCLUDE
)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/rewrite DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ssl DESTINATION include/postgresql/server 
    PATTERN "*.h" PATTERN "*.cnf" EXCLUDE)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/gtm DESTINATION include/postgresql/server PATTERN "utils" EXCLUDE)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/parser DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/fmgr DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/lib DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/bootstrap DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/replication DESTINATION include/postgresql/server PATTERN "heartbeat" EXCLUDE)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/portability DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/foreign DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/gssignal DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/vecexecutor DESTINATION include/postgresql/server PATTERN "vectorbatch.inl" EXCLUDE)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/postmaster DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/streaming DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/gs_policy DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/pgxc DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/workload DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/iprange DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/libcomm DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/db4ai DESTINATION include/postgresql/server)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/access DESTINATION include/postgresql/server
        PATTERN "*.h")
else("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/access DESTINATION include/postgresql/server
    PATTERN "extreme_rto" EXCLUDE
    PATTERN "ondemand_extreme_rto" EXCLUDE
    PATTERN "*.h")
endif()
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/mb DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/catalog DESTINATION include/postgresql/server 
    PATTERN "*.h" 
    PATTERN "upgrade_sql" EXCLUDE
    PATTERN "unused_oids" EXCLUDE
    PATTERN "pg_proc.h_for_llt" EXCLUDE
    PATTERN "duplicate_oids" EXCLUDE
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_uverify.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundotype.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundoapi.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_uheap.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_utuple.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_utype.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_upage.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_uredo.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_uundovec.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/knl_uundorecord.h DESTINATION include/postgresql/server/access/ustore)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundoxlog.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundotxn.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundozone.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/ustore/undo/knl_uundospace.h DESTINATION include/postgresql/server/access/ustore/undo)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/communication/commproxy_basic.h DESTINATION include/postgresql/server/communication)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/parallel_recovery/page_redo.h DESTINATION include/postgresql/server/access/parallel_recovery)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/access/parallel_recovery/spsc_blocking_queue.h DESTINATION include/postgresql/server/access/parallel_recovery)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/executor/exec/execdesc.h DESTINATION include/postgresql/server/executor/exec)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/client_logic DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/nodes DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/hotpatch DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/tsearch DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/datatype DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/regex DESTINATION include/postgresql/server)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/instruments/list.h DESTINATION include/postgresql/server/instruments)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/instruments/instr_event.h DESTINATION include/postgresql/server/instruments)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/instruments/gs_stack.h DESTINATION include/postgresql/server/instruments)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/instruments/instr_mfchain.h DESTINATION include/postgresql/server/instruments)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/tde_key_management/data_common.h DESTINATION include/postgresql/server/tde_key_management)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/bulkload DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/gstrace DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/knl DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/optimizer DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/utils DESTINATION include/postgresql/server 
    PATTERN "errcodes.h" EXCLUDE
    PATTERN "fmgroids.h" EXCLUDE
    PATTERN "probes.h" EXCLUDE
)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/snowball DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/tcop DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/smgr DESTINATION include/postgresql/server/storage)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/buf DESTINATION include/postgresql/server/storage)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/item DESTINATION include/postgresql/server/storage)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/lock DESTINATION include/postgresql/server/storage)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/cstore/ DESTINATION include/postgresql/server/storage)
if("${ENABLE_MOT}" STREQUAL "ON")
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/storage/mot DESTINATION include/postgresql/server/storage)
endif()
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/commands DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/port DESTINATION include/postgresql/server)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/lite DESTINATION include/postgresql/server)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/alarm/alarm.h DESTINATION include/postgresql/server/alarm)
install_file(${CMAKE_CURRENT_SOURCE_DIR}/include/storage include/postgresql/server/storage)
install_file(${CMAKE_CURRENT_SOURCE_DIR}/include include/postgresql/server)
install(FILES ${CMAKE_BINARY_DIR}/pg_config.h DESTINATION include/postgresql/server)
install(FILES ${CMAKE_BINARY_DIR}/ecpg_config.h DESTINATION include)
install(FILES ${CMAKE_BINARY_DIR}/pg_config.h DESTINATION include)
install_file(${CMAKE_CURRENT_SOURCE_DIR}/include include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/common/config/cm_config.h DESTINATION include)

# special
install(CODE "execute_process(
    COMMAND rm ${prefix_home}/include/pg_config_os.h
    COMMAND rm ${prefix_home}/include/postgresql/server/pg_config_os.h)"
)
install(CODE "execute_process(
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/include/port/linux.h ${prefix_home}/include/pg_config_os.h
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/include/port/linux.h ${prefix_home}/include/postgresql/server/pg_config_os.h
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/include/cm/libpq-fe.h ${prefix_home}/include/cm-libpq-fe.h)"
)

# open source install part
file(GLOB JAVA_PATHS "/usr/lib/jvm/java-1.8.0-openjdk-*/jre")
list(SORT JAVA_PATHS)
list(REVERSE JAVA_PATHS)
list(GET JAVA_PATHS 0 LATEST_JAVA_JRE)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(DIRECTORY ${LATEST_JAVA_JRE} DESTINATION . FILE_PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE OWNER_READ GROUP_READ)
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    install(DIRECTORY ${DCF_LIB_PATH} DESTINATION .)
endif()
if(${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF AND NOT ${ENABLE_LITE_MODE} STREQUAL ON)
    if(EXISTS ${DMS_LIB_PATH})
        install(DIRECTORY ${DMS_LIB_PATH} DESTINATION .)
    endif()
    if(EXISTS ${DSS_LIB_PATH})
        install(DIRECTORY ${DSS_LIB_PATH} DESTINATION .)
    endif()
    if(EXISTS ${DSS_BIN_PATH})
        install(DIRECTORY ${DSS_BIN_PATH} DESTINATION . FILE_PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ OWNER_WRITE)
    endif()
    if(EXISTS ${HCOM4DB_LIB_PATH})
        install(DIRECTORY ${HCOM4DB_LIB_PATH} DESTINATION .)
    endif()
endif()

install(DIRECTORY ${ZSTD_LIB_PATH} DESTINATION . PATTERN "*.a" EXCLUDE)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON" AND "${ENABLE_OBS}" STREQUAL "ON")
    install(DIRECTORY ${LIBOBS_LIB_PATH} DESTINATION .)
    install(DIRECTORY ${LIBOBS_INCLUDE_PATH} DESTINATION include/postgresql/server/access/obs)
endif()
install(DIRECTORY ${CJSON_LIB_PATH} DESTINATION .)
install(DIRECTORY ${CJSON_INCLUDE_PATH}/cjson DESTINATION include/postgresql/server)
if(NOT ${ENABLE_LITE_MODE} STREQUAL ON)
if(NOT ${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF)
    install(DIRECTORY ${KMC_LIB_PATH} DESTINATION .)
endif()
endif()
install(DIRECTORY ${LIBCURL_LIB_PATH} DESTINATION .)
install(DIRECTORY ${AWSSDK_LIB_PATH} DESTINATION .)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(DIRECTORY ${KERBEROS_SBIN_PATH}/ DESTINATION bin)
    install(DIRECTORY ${KERBEROS_BIN_PATH} DESTINATION .)
    install(DIRECTORY ${KERBEROS_LIB_PATH} DESTINATION .)
endif()
install(DIRECTORY ${LIBOPENSSL_BIN_PATH} DESTINATION .)
install(DIRECTORY ${LIBOPENSSL_LIB_PATH} DESTINATION . PATTERN "*.a" EXCLUDE )
install(DIRECTORY ${ZLIB_LIB_PATH} DESTINATION . FILES_MATCHING PATTERN "libz.*")

list(FIND MACRO_OPTIONS "-D__USE_NUMA" RET_NUMA)

if("${ENABLE_MOT}" STREQUAL "ON")
install(DIRECTORY ${MASSTREE_LIB_PATH} DESTINATION .)
install(CODE "execute_process(
     COMMAND cp ${GCC_LIB_PATH}/lib64/libatomic.so.1.2.0 ${prefix_home}/lib/libatomic.so.1.2.0
     COMMAND ln -fs libatomic.so.1.2.0 libatomic.so
     COMMAND ln -fs libatomic.so.1.2.0 libatomic.so.1
     WORKING_DIRECTORY ${prefix_home}/lib)"
)
install(CODE "message(\"-- Created symlink: libatomic.so -> libatomic.so.1.2.0\")")
install(CODE "message(\"-- Created symlink: libatomic.so.1 -> libatomic.so.1.2.0\")")
endif()

install(FILES ${SECUREDYNAMICLIB_HOME}/libsecurec.so DESTINATION lib)
install(FILES ${GCC_LIB_PATH}/lib64/libgcc_s.so.1 DESTINATION lib)
install(FILES ${GCC_LIB_PATH}/lib64/libgomp.so DESTINATION lib)
install(FILES ${GCC_LIB_PATH}/lib64/libgomp.so.1 DESTINATION lib)
install(FILES ${GCC_LIB_PATH}/lib64/libgomp.so.1.0.0 DESTINATION lib)
install(FILES ${XGBOOST_LIB_PATH}/libxgboost.so DESTINATION lib)
if(NOT ${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF)
    if("${SUPPORT_HOTPATCH}" STREQUAL "yes")
        install(FILES ${LIBHOTPATCH_LIB_PATH}/libdoprapatch.a DESTINATION lib)
    endif()
endif()

if("${ENABLE_MOT}" STREQUAL "ON")
    install(DIRECTORY ${MASSTREE_LIB_PATH} DESTINATION .)
    install(DIRECTORY ${GCC_LIB_PATH}/lib64/ DESTINATION lib
        FILES_MATCHING PATTERN "libatomic.so*")
endif()

install(FILES ${GCC_LIB_PATH}/lib64/libgcc_s.so.1 DESTINATION lib)
install(DIRECTORY ${GCC_LIB_PATH}/lib64/ DESTINATION lib
    FILES_MATCHING PATTERN "libgomp.so*")

file(GLOB libstdc_files ${GCC_LIB_PATH}/lib64/libstdc++.so.6.0.*)
file(GLOB to_remove ${GCC_LIB_PATH}/lib64/libstdc++.so.6.*.py)
list(REMOVE_ITEM libstdc_files ${to_remove})
install(CODE "execute_process(
    COMMAND rm -f ${prefix_home}/lib/libstdc++.so.6
    COMMAND cp ${libstdc_files} ${prefix_home}/lib/libstdc++.so.6)"
)

# install(DIRECTORY ${LIBCGROUP_LIB_PATH} DESTINATION . FILES_MATCHING PATTERN "libcgroup.so*")
install(CODE "execute_process(
COMMAND cp ${LIBCGROUP_LIB_PATH}/libcgroup.so.2.0.3 ${prefix_home}/lib/libcgroup.so
COMMAND ln -fs libcgroup.so libcgroup.so.2
WORKING_DIRECTORY ${prefix_home}/lib)"
)
install(CODE "message(\"-- Created symlink: libcgroup.so.2 -> libcgroup.so\")")
 
# fastcheck part
install(FILES ${PROJECT_SRC_DIR}/test/regress/stub/roach_api_stub/roach_api_stub.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${PROJECT_SRC_DIR}/test/regress/stub/roach_api_stub/roach_api_stub--1.0.sql
    DESTINATION share/postgresql/extension/
)
