#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_bloomfilter_SRC)

set(TGT_bloomfilter_INC 
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(bloomfilter_DEF_OPTIONS ${MACRO_OPTIONS})
set(bloomfilter_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(bloomfilter_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_utils_bloomfilter TGT_bloomfilter_SRC TGT_bloomfilter_INC "${bloomfilter_DEF_OPTIONS}" "${bloomfilter_COMPILE_OPTIONS}" "${bloomfilter_LINK_OPTIONS}")

