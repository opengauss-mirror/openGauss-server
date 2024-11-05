#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_adt_SRC)

execute_process(
    COMMAND bison -d -o jsonpath_gram.cpp jsonpath_gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PARSER_GRAM
)
execute_process(
    COMMAND sed -i "s/YY_NULL nullptr/YY_NULL 0/g" jsonpath_gram.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PARSER_GRAM
)

set(jsonpath_cmd_src
"${PROJECT_SRC_DIR}/common/backend/utils/adt|||flex -CF -b -p -p -o 'jsonpath_scan.inc' jsonpath_scan.l|sed -i 's/YY_NULL/YY_ZERO/g' scan.inc"
)

add_cmd_gen_when_configure(flex_target jsonpath_cmd_src)

execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/jsonpath_gram.hpp ${CMAKE_CURRENT_SOURCE_DIR}/../include/utils/jsonpath_gram.hpp
)

list(REMOVE_ITEM TGT_adt_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/like_match.cpp
)

list(APPEND TGT_adt_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/jsonpath_gram.cpp)

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
