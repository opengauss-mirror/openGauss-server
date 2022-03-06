#This is the main CMAKE for build all components.
# pg_format_cu bin
set(TGT_format_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_format_cu.cpp
)

set(TGT_format_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(format_DEF_OPTIONS ${MACRO_OPTIONS})
set(format_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(format_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(format_LINK_LIBS libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(pg_format_cu TGT_format_SRC TGT_format_INC "${format_DEF_OPTIONS}" "${format_COMPILE_OPTIONS}" "${format_LINK_OPTIONS}" "${format_LINK_LIBS}")
add_dependencies(pg_format_cu pgport_static)
target_link_directories(pg_format_cu PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)

install(TARGETS pg_format_cu RUNTIME DESTINATION bin)
