#This is the main CMAKE for build all components.
execute_process(COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/page_compression.sh ${PROJECT_SRC_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
MESSAGE( STATUS "pagehack sh ${CMAKE_CURRENT_SOURCE_DIR}/page_compression.sh ${PROJECT_SRC_DIR} ${CMAKE_CURRENT_SOURCE_DIR}.")
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pagecompression_SRC)
set(TGT_pagecompression_INC ${ZSTD_INCLUDE_PATH})
set(pagecompression_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND)
set(pagecompreesion_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(pagecompression_LINK_OPTIONS ${LIB_LINK_OPTIONS})
set(pagecompression_LINK_LIBS -lzstd)
list(REMOVE_ITEM pagecompression_LINK_OPTIONS -pthread)
add_shared_libtarget(pagecompression TGT_pagecompression_SRC TGT_pagecompression_INC "${pagecompression_DEF_OPTIONS}" "${pagecompreesion_COMPILE_OPTIONS}" "${pagecompression_LINK_OPTIONS}" "${pagecompression_LINK_LIBS}")
SET_TARGET_PROPERTIES(pagecompression PROPERTIES VERSION 1)


install(TARGETS pagecompression LIBRARY DESTINATION lib)
