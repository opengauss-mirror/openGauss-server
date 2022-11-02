#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_workload_SRC)

set(TGT_workload_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${ZLIB_INCLUDE_PATH}
)

set(workload_DEF_OPTIONS ${MACRO_OPTIONS})
set(workload_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(workload_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_instruments_workload TGT_workload_SRC TGT_workload_INC "${workload_DEF_OPTIONS}" "${workload_COMPILE_OPTIONS}" "${workload_LINK_OPTIONS}")

