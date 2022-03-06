#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_adt_SRC)
list(REMOVE_ITEM TGT_adt_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/like_match.cpp
)

set(TGT_adt_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
)

set(adt_DEF_OPTIONS ${MACRO_OPTIONS})
set(adt_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(adt_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_adt TGT_adt_SRC TGT_adt_INC "${adt_DEF_OPTIONS}" "${adt_COMPILE_OPTIONS}" "${adt_LINK_OPTIONS}")
