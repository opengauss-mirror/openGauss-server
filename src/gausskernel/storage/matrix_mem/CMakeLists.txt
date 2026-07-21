#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_matrix_mem_SRC)

set(TGT_matrix_mem_INC
        ${PROJECT_SRC_DIR}/include
)

set(matrix_mem_DEF_OPTIONS ${MACRO_OPTIONS})
set(matrix_mem_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(matrix_mem_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_matrix_mem TGT_matrix_mem_SRC TGT_matrix_mem_INC "${matrix_mem_DEF_OPTIONS}" "${matrix_mem_COMPILE_OPTIONS}" "${matrix_mem_LINK_OPTIONS}")