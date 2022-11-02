#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_grpc_SRC)

set(TGT_grpc_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${ZLIB_INCLUDE_PATH}
)

set(grpc_DEF_OPTIONS ${MACRO_OPTIONS})
set(grpc_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(grpc_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_grpc TGT_grpc_SRC TGT_grpc_INC "${grpc_DEF_OPTIONS}" "${grpc_COMPILE_OPTIONS}" "${grpc_LINK_OPTIONS}")

