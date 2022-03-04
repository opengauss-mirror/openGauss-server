#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_parquet_SRC)

set(TGT_parquet_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROTOBUF_INCLUDE_PATH}
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")    
    ${LIBORC_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
endif()    
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
)

set(parquet_DEF_OPTIONS ${MACRO_OPTIONS})
set(parquet_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(parquet_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_dfs_parquet TGT_parquet_SRC TGT_parquet_INC "${parquet_DEF_OPTIONS}" "${parquet_COMPILE_OPTIONS}" "${parquet_LINK_OPTIONS}")

