#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_vecnode_SRC)

set(TGT_vecnode_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBLLVM_INCLUDE_PATH}
)

set(vecnode_DEF_OPTIONS ${MACRO_OPTIONS} -D__STDC_FORMAT_MACROS )
set(vecnode_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(vecnode_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_vecexecutor_vecnode TGT_vecnode_SRC TGT_vecnode_INC "${vecnode_DEF_OPTIONS}" "${vecnode_COMPILE_OPTIONS}" "${vecnode_LINK_OPTIONS}")

