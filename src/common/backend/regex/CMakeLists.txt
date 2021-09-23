#This is the main CMAKE for build all components.
list(APPEND TGT_regex_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/regcomp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regerror.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regexec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regfree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regprefix.cpp
)

set(TGT_regex_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(regex_DEF_OPTIONS ${MACRO_OPTIONS})
set(regex_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(regex_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_regex TGT_regex_SRC TGT_regex_INC "${regex_DEF_OPTIONS}" "${regex_COMPILE_OPTIONS}" "${regex_LINK_OPTIONS}")
