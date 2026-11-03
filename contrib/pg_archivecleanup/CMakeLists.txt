AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pg_archivecleanup_SRC)
set(TGT_pg_archivecleanup_INC
    ${TGT_pq_INC} ${ZSTD_INCLUDE_PATH} ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SRC_DIR}/lib/gstrace ${PROJECT_SRC_DIR}/lib/page_compression
)

set(pg_archivecleanup_DEF_OPTIONS ${MACRO_OPTIONS})
if(${ENABLE_DEBUG} STREQUAL "ON")
    set(pg_archivecleanup_DEF_OPTIONS ${pg_archivecleanup_DEF_OPTIONS} -DDEBUG -DFRONTEND)
endif()
set(pg_archivecleanup_COMPILE_OPTIONS ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS} ${OPTIMIZE_OPTIONS})
set(pg_archivecleanup_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(pg_archivecleanup_LINK_LIBS -lpgport -lcrypt -ldl -lm -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(pg_archivecleanup TGT_pg_archivecleanup_SRC TGT_pg_archivecleanup_INC "${pg_archivecleanup_DEF_OPTIONS}" "${pg_archivecleanup_COMPILE_OPTIONS}" "${pg_archivecleanup_LINK_OPTIONS}" "${pg_archivecleanup_LINK_LIBS}")
add_dependencies(pg_archivecleanup pgport_static pagecompression)
target_link_directories(pg_archivecleanup PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${SECURE_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${ZSTD_LIB_PATH} ${LZ4_LIB_PATH} ${PROJECT_SRC_DIR}/lib/page_compression
)
install(TARGETS pg_archivecleanup RUNTIME DESTINATION bin)

