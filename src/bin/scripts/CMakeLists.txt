#This is the main CMAKE for build all components.
# scripts bins
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_dump/keywords.cpp ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_dump/dumpmem.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/psql/mbprint.cpp ${CMAKE_CURRENT_SOURCE_DIR}/mbprint.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/psql/print.cpp ${CMAKE_CURRENT_SOURCE_DIR}/print.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_dump/dumputils.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/parser/kwlookup.cpp ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
)

set(TGT_createdb_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/createdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_vacuumdb_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/vacuumdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
)
set(TGT_reindexdb_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/reindexdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_clusterdb_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/clusterdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_dropuser_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/dropuser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_createlang_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/createlang.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/print.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mbprint.cpp
)
set(TGT_droplang_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/droplang.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/print.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mbprint.cpp
)
set(TGT_dropdb_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/dropdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_createuser_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/createuser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
)
set(TGT_scripts_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/pg_dump 
    ${PROJECT_SRC_DIR}/bin/psql 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(scripts_DEF_OPTIONS ${MACRO_OPTIONS})
set(scripts_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(scripts_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(scripts_LINK_LIBS libelog.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto  -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND scripts_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()
add_bintarget(createdb TGT_createdb_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(createdb elog_static pgport_static pq)
target_link_directories(createdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS createdb RUNTIME DESTINATION bin)

add_bintarget(createlang TGT_createlang_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(createlang elog_static pgport_static pq)
target_link_directories(createlang PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS createlang RUNTIME DESTINATION bin)

add_bintarget(createuser TGT_createuser_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(createuser elog_static pgport_static pq)
target_link_directories(createuser PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS createuser RUNTIME DESTINATION bin)

add_bintarget(dropdb TGT_dropdb_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(dropdb elog_static pgport_static pq)
target_link_directories(dropdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS dropdb LIBRARY DESTINATION bin)

add_bintarget(droplang TGT_droplang_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(droplang elog_static pgport_static pq)
target_link_directories(droplang PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS droplang RUNTIME DESTINATION bin)

add_bintarget(dropuser TGT_dropuser_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(dropuser elog_static pgport_static pq)
target_link_directories(dropuser PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS dropuser RUNTIME DESTINATION bin)

add_bintarget(clusterdb TGT_clusterdb_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(clusterdb elog_static pgport_static pq)
target_link_directories(clusterdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS clusterdb RUNTIME DESTINATION bin)

add_bintarget(vacuumdb TGT_vacuumdb_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(vacuumdb elog_static pgport_static pq)
target_link_directories(vacuumdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS vacuumdb RUNTIME DESTINATION bin)

add_bintarget(reindexdb TGT_reindexdb_SRC TGT_scripts_INC "${scripts_DEF_OPTIONS}" "${scripts_COMPILE_OPTIONS}" "${scripts_LINK_OPTIONS}" "${scripts_LINK_LIBS}")
add_dependencies(reindexdb elog_static pgport_static pq)
target_link_directories(reindexdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS reindexdb RUNTIME DESTINATION bin)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/run_drop_cache.sh DESTINATION bin)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/runsessionstat.sh DESTINATION bin)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/drop_caches.sh DESTINATION bin)
if(${BUILD_TUPLE} STREQUAL "aarch64")
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/setArmOptimization.sh DESTINATION bin)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/bind_net_irq.sh DESTINATION bin)
endif()
