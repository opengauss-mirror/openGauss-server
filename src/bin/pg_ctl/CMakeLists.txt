#This is the main CMAKE for build all components.
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/transam/xlogreader.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/redo/xlogreader_common.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader_common.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gsctl_SRC)

if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND TGT_gsctl_SRC ${CMAKE_CURRENT_SOURCE_DIR}/fetchmot.cpp)
endif()

set(TGT_gsctl_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/bin/pg_rewind 
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${LIBHOTPATCH_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${ZSTD_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/lib/page_compression
)

set(gsctl_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DFRONTEND)
set(gsctl_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gsctl_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(gsctl_LINK_LIBS libelog.a libbuildquery.a pg_rewind.a libpgcommon.a libhotpatchclient.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz -lpagecompression -lzstd)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND gsctl_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()
add_bintarget(gs_ctl TGT_gsctl_SRC TGT_gsctl_INC "${gsctl_DEF_OPTIONS}" "${gsctl_COMPILE_OPTIONS}" "${gsctl_LINK_OPTIONS}" "${gsctl_LINK_LIBS}")
add_dependencies(gs_ctl elog_static buildquery_static pg_rewind_static pgcommon_static hotpatchclient_static pgport_static pq pagecompression)
target_link_directories(gs_ctl PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH}
    ${LIBHOTPATCH_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib ${ZSTD_LIB_PATH}
)
install(TARGETS gs_ctl RUNTIME DESTINATION bin)