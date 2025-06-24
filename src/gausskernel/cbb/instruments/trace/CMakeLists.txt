#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_trace_SRC)

set(TGT_trace_INC
    ${PROJECT_SRC_DIR}/include
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${ZLIB_INCLUDE_PATH} )
set(trace_DEF_OPTIONS ${MACRO_OPTIONS})
set(trace_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(trace_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_instruments_trace TGT_trace_SRC TGT_trace_INC "${trace_DEF_OPTIONS}" "${trace_COMPILE_OPTIONS}" "${trace_LINK_OPTION}")
