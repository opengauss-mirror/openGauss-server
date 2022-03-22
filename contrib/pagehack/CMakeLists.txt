#This is the main CMAKE for build all pagehack.
# pagehack
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pagehack_SRC)
set(TGT_pagehack_INC
    ${TGT_pq_INC} ${ZSTD_INCLUDE_PATH} ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SRC_DIR}/lib/gstrace ${PROJECT_SRC_DIR}/lib/page_compression
)

set(pagehack_DEF_OPTIONS ${MACRO_OPTIONS})
if(${ENABLE_DEBUG} STREQUAL "ON")
    set(pagehack_DEF_OPTIONS ${pagehack_DEF_OPTIONS} -DDEBUG -DFRONTEND)
endif()
set(pagehack_COMPILE_OPTIONS ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS} ${OPTIMIZE_OPTIONS})
set(pagehack_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(pagehack_LINK_LIBS -lpgport -lcrypt -ldl -lm -ledit -lssl -lcrypto -lsecurec -lrt -lz -lminiunz -lzstd -lpagecompression)
add_bintarget(pagehack TGT_pagehack_SRC TGT_pagehack_INC "${pagehack_DEF_OPTIONS}" "${pagehack_COMPILE_OPTIONS}" "${pagehack_LINK_OPTIONS}" "${pagehack_LINK_LIBS}")
add_dependencies(pagehack pgport_static pagecompression)
target_link_directories(pagehack PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH} ${SECURE_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${ZSTD_LIB_PATH} ${PROJECT_SRC_DIR}/lib/page_compression
)
install(TARGETS pagehack RUNTIME DESTINATION bin)

