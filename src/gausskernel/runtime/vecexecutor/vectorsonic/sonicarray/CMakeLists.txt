#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_sonicarray_SRC)

set(TGT_sonicarray_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBLLVM_INCLUDE_PATH}
)

set(sonicarray_DEF_OPTIONS ${MACRO_OPTIONS})
set(sonicarray_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(sonicarray_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_vecexecutor_vectorsonic_sonicarray TGT_sonicarray_SRC TGT_sonicarray_INC "${sonicarray_DEF_OPTIONS}" 
    "${sonicarray_COMPILE_OPTIONS}" "${sonicarray_LINK_OPTIONS}")

