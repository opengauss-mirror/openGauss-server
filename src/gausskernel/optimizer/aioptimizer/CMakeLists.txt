# aioptimizer.cmake
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_aioptimizer_SRC)

set(TGT_aioptimizer_INC
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)
set(aioptimizer_DEF_OPTIONS ${MACRO_OPTIONS})
set(aioptimizer_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(aioptimizer_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_aioptimizer TGT_aioptimizer_SRC TGT_aioptimizer_INC "${aioptimizer_DEF_OPTIONS}" "${aioptimizer_COMPILE_OPTIONS}" "${aioptimizer_LINK_OPTIONS}")

