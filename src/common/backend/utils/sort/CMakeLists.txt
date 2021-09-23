#This is the main CMAKE for build all components.
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/qsort_tuple.inc
    COMMAND perl gen_qsort_tuple.pl gen_qsort_tuple.pl > ${CMAKE_CURRENT_SOURCE_DIR}/qsort_tuple.inc
    DEPENDS gen_qsort_tuple.pl 
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Now Generating qsort_tuple.inc"
)

set(TGT_sort_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/qsort_tuple.inc
    ${CMAKE_CURRENT_SOURCE_DIR}/batchsort.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/batchstore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logtape.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rowstore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sortsupport.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tuplesort.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tuplestore.cpp
)

set(TGT_sort_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(sort_DEF_OPTIONS ${MACRO_OPTIONS})
set(sort_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(sort_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_sort TGT_sort_SRC TGT_sort_INC "${sort_DEF_OPTIONS}" "${sort_COMPILE_OPTIONS}" "${sort_LINK_OPTIONS}")
