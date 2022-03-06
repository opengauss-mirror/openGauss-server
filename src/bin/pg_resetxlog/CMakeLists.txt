#This is the main CMAKE for build all components.
# pg_resetxlog bin
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_resetxlog_SRC)

set(TGT_resetxlog_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/pg_resetxlog 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(resetxlog_DEF_OPTIONS ${MACRO_OPTIONS})
set(resetxlog_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(resetxlog_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(resetxlog_LINK_LIBS libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(pg_resetxlog TGT_resetxlog_SRC TGT_resetxlog_INC "${resetxlog_DEF_OPTIONS}" "${resetxlog_COMPILE_OPTIONS}" "${resetxlog_LINK_OPTIONS}" "${resetxlog_LINK_LIBS}")
add_dependencies(pg_resetxlog pgport_static)
target_link_directories(pg_resetxlog PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${GRPC_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH} 
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS pg_resetxlog RUNTIME DESTINATION bin)

