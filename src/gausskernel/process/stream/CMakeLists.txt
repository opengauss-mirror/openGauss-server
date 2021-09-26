#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_stream_SRC)

set(TGT_stream_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(stream_DEF_OPTIONS ${MACRO_OPTIONS} -D__STDC_FORMAT_MACROS)
set(stream_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(stream_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_process_stream TGT_stream_SRC TGT_stream_INC "${stream_DEF_OPTIONS}" "${stream_COMPILE_OPTIONS}" "${stream_LINK_OPTIONS}")

