#This is the main CMAKE for build all components.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_catalog_SRC)

set(TGT_catalog_INC 
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

set(catalog_DEF_OPTIONS ${MACRO_OPTIONS})
set(catalog_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(catalog_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_catalog TGT_catalog_SRC TGT_catalog_INC "${catalog_DEF_OPTIONS}" "${catalog_COMPILE_OPTIONS}" "${catalog_LINK_OPTIONS}")
