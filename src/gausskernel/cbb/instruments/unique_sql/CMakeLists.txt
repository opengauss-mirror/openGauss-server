#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_unique_sql_SRC)

set(TGT_unique_sql_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(unique_sql_DEF_OPTIONS ${MACRO_OPTIONS})
set(unique_sql_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(unique_sql_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_instruments_unique_sql TGT_unique_sql_SRC TGT_unique_sql_INC "${unique_sql_DEF_OPTIONS}" "${unique_sql_COMPILE_OPTIONS}" "${unique_sql_LINK_OPTIONS}")

