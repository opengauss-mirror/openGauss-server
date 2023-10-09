#This is the main CMAKE for build all components.
# gs_basebackup bin
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/transam/xlogreader.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/gs_uwal/gs_uwal_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_uwal_adaptor.cpp
)

set(TGT_basebackup_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_basebackup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/receivelog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/streamutil.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_uwal_adaptor.cpp
)
if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND TGT_basebackup_SRC ${PROJECT_SRC_DIR}/bin/pg_ctl/fetchmot.cpp)
endif()
set(TGT_basebackup_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/bin/pg_basebackup
    ${PROJECT_SRC_DIR}/bin/pg_rewind
    ${PROJECT_SRC_DIR}/bin/pg_ctl
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_SRC_DIR}/lib/page_compression
    ${PROJECT_SRC_DIR}/include/gs_uwal
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${KERBEROS_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(basebackup_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DFRONTEND)
set(basebackup_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(basebackup_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(basebackup_LINK_LIBS lib_string_obj libelog.a libbuildquery.a libpgcommon.a libhotpatchclient.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -llz4 -lzstd -lpagecompression)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND basebackup_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()

list(APPEND basebackup_LINK_DIRS ${LIBUWAL_LINK_DIRS})
list(APPEND basebackup_LINK_OPTIONS ${LIBUWAL_LINK_OPTIONS})

add_bintarget(gs_basebackup TGT_basebackup_SRC TGT_basebackup_INC "${basebackup_DEF_OPTIONS}" "${basebackup_COMPILE_OPTIONS}" "${basebackup_LINK_OPTIONS}" "${basebackup_LINK_LIBS}")
add_dependencies(gs_basebackup lib_string_obj elog_static buildquery_static pgcommon_static hotpatchclient_static pgport_static pagecompression)
target_link_directories(gs_basebackup PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBCGROUP_LIB_PATH}
    ${LIBCGROUP_LIB_PATH} ${LZ4_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib ${SECURE_LIB_PATH}
    ${ZSTD_LIB_PATH} ${PROJECT_SRC_DIR}/lib/page_compression ${basebackup_LINK_DIRS}
)
install(TARGETS gs_basebackup RUNTIME DESTINATION bin)
install_symlink(gs_basebackup gs_tar ${prefix_home}/bin)

# pg_receivexlog bin
set(TGT_receivexlog_SRC
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/pg_receivexlog.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/receivelog.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/streamutil.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/xlogreader.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/gs_uwal_adaptor.cpp
)
if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND TGT_receivexlog_SRC ${PROJECT_SRC_DIR}/bin/pg_ctl/fetchmot.cpp)
endif()

set(TGT_receivexlog_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/bin/pg_basebackup
    ${PROJECT_SRC_DIR}/bin/pg_rewind
    ${PROJECT_SRC_DIR}/bin/pg_ctl
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_SRC_DIR}/include/gs_uwal
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${KERBEROS_INCLUDE_PATH}
)

set(receivexlog_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DFRONTEND)
set(receivexlog_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(receivexlog_LINK_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -pie)
set(receivexlog_LINK_LIBS lib_string_obj libelog.a libbuildquery.a libpgcommon.a libhotpatchclient.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -llz4)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND receivexlog_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()

list(APPEND receivexlog_LINK_DIRS ${LIBUWAL_LINK_DIRS})
list(APPEND receivexlog_LINK_OPTIONS ${LIBUWAL_LINK_OPTIONS})

add_bintarget(pg_receivexlog TGT_receivexlog_SRC TGT_receivexlog_INC "${receivexlog_DEF_OPTIONS}" "${receivexlog_COMPILE_OPTIONS}" "${receivexlog_LINK_OPTIONS}" "${receivexlog_LINK_LIBS}")
add_dependencies(pg_receivexlog lib_string_obj elog_static buildquery_static pgcommon_static hotpatchclient_static pgport_static pq)
target_link_directories(pg_receivexlog PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${LIBEDIT_LIB_PATH}
    ${LZ4_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib ${receivexlog_LINK_DIRS}
)
# BIND_NOW 
target_link_options(pg_receivexlog PRIVATE "-Wl,-z,relro,-z,now")

install(TARGETS pg_receivexlog RUNTIME DESTINATION bin)

# pg_recvlogical bin
set(TGT_recvlogical_SRC
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/pg_recvlogical.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/receivelog.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/streamutil.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/xlogreader.cpp
    ${PROJECT_SRC_DIR}/bin/pg_basebackup/gs_uwal_adaptor.cpp
)
if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND TGT_recvlogical_SRC ${PROJECT_SRC_DIR}/bin/pg_ctl/fetchmot.cpp)
endif()

set(TGT_recvlogical_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/bin/pg_basebackup
    ${PROJECT_SRC_DIR}/bin/pg_rewind
    ${PROJECT_SRC_DIR}/bin/pg_ctl
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_SRC_DIR}/include/gs_uwal
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${KERBEROS_INCLUDE_PATH}
)

set(recvlogical_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DFRONTEND)
set(recvlogical_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(recvlogical_LINK_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -pie)
set(recvlogical_LINK_LIBS lib_string_obj libelog.a libbuildquery.a libpgcommon.a libhotpatchclient.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -llz4)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND recvlogical_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()

list(APPEND recvlogical_LINK_DIRS ${LIBUWAL_LINK_DIRS})
list(APPEND recvlogical_LINK_OPTIONS ${LIBUWAL_LINK_OPTIONS})

add_bintarget(pg_recvlogical TGT_recvlogical_SRC TGT_recvlogical_INC "${recvlogical_DEF_OPTIONS}" "${recvlogical_COMPILE_OPTIONS}" "${recvlogical_LINK_OPTIONS}" "${recvlogical_LINK_LIBS}")
add_dependencies(pg_recvlogical lib_string_obj elog_static buildquery_static pgcommon_static hotpatchclient_static pgport_static pq)
target_link_directories(pg_recvlogical PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH}
    ${LZ4_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib ${SECURE_LIB_PATH} ${recvlogical_LINK_DIRS}
)
# BIND_NOW 
target_link_options(pg_recvlogical PRIVATE "-Wl,-z,relro,-z,now")
install(TARGETS pg_recvlogical RUNTIME DESTINATION bin)
