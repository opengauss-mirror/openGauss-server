#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pool_SRC)

set(TGT_pool_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(pool_DEF_OPTIONS ${MACRO_OPTIONS})
set(pool_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(pool_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_pgxc_single_pool TGT_pool_SRC TGT_pool_INC "${pool_DEF_OPTIONS}" "${pool_COMPILE_OPTIONS}" "${pool_LINK_OPTIONS}")
