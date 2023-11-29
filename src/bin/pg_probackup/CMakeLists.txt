#This is the main CMAKE for build all components.
# gs_probackup bin
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_rewind/datapagemap.h ${CMAKE_CURRENT_SOURCE_DIR}/datapagemap.h
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_basebackup/receivelog.h ${CMAKE_CURRENT_SOURCE_DIR}/receivelog.h
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_basebackup/streamutil.h ${CMAKE_CURRENT_SOURCE_DIR}/streamutil.h
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_rewind/datapagemap.cpp ${CMAKE_CURRENT_SOURCE_DIR}/datapagemap.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_rewind/logging.h ${CMAKE_CURRENT_SOURCE_DIR}/logging.h
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_basebackup/receivelog.cpp ${CMAKE_CURRENT_SOURCE_DIR}/receivelog.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_basebackup/streamutil.cpp ${CMAKE_CURRENT_SOURCE_DIR}/streamutil.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/transam/xlogreader.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/dss_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/gs_uwal/gs_uwal_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_uwal_adaptor.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_probackup_SRC)

set(TGT_probackup_INC ${ZLIB_INCLUDE_PATH} ${LZ4_INCLUDE_PATH} ${ZSTD_INCLUDE_PATH} ${PROJECT_SRC_DIR}/lib/page_compression ${PROJECT_SRC_DIR}/include/storage/gs_uwal)

set(probackup_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DHAVE_LIBZ)
set(probackup_COMPILE_OPTIONS ${PROTECT_OPTIONS} ${BIN_SECURE_OPTIONS} ${OS_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS})
set(probackup_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(probackup_LINK_LIBS libpgcommon.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -llz4 -lpq -lpagecompression -lzstd)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND probackup_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()

list(APPEND probackup_LINK_DIRS ${LIBUWAL_LINK_DIRS})
list(APPEND probackup_LINK_OPTIONS ${LIBUWAL_LINK_OPTIONS})

add_bintarget(gs_probackup TGT_probackup_SRC TGT_probackup_INC "${probackup_DEF_OPTIONS}" "${probackup_COMPILE_OPTIONS}" "${probackup_LINK_OPTIONS}" "${probackup_LINK_LIBS}")
add_dependencies(gs_probackup pq pgport_static pagecompression)
target_link_directories(gs_probackup PUBLIC 
    ${CMAKE_BINARY_DIR}/lib ${LIBOPENSSL_LIB_PATH} ${LIBEDIT_LIB_PATH} ${ZSTD_LIB_PATH}
    ${ZLIB_LIB_PATH} ${KERBEROS_LIB_PATH} ${LZ4_LIB_PATH} ${SECURE_LIB_PATH} ${probackup_LINK_DIRS}
)
install(TARGETS gs_probackup RUNTIME DESTINATION bin)
