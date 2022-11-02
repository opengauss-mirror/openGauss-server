#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gs_stack_SRC)

set(TGT_gs_stack_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${ZLIB_INCLUDE_PATH}
)

set(gs_stack_DEF_OPTIONS ${MACRO_OPTIONS})
set(gs_stack_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gs_stack_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_instruments_gs_stack TGT_gs_stack_SRC TGT_gs_stack_INC "${gs_stack_DEF_OPTIONS}" "${gs_stack_COMPILE_OPTIONS}" "${gs_stack_LINK_OPTIONS}")