#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_nodes_SRC)

set(TGT_nodes_INC 
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

set(nodes_DEF_OPTIONS ${MACRO_OPTIONS})
set(nodes_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(nodes_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_nodes TGT_nodes_SRC TGT_nodes_INC "${nodes_DEF_OPTIONS}" "${nodes_COMPILE_OPTIONS}" "${nodes_LINK_OPTIONS}")
