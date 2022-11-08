#This is the main CMAKE for build all components.
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/dss_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
)
set(TGT_controldata_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_controldata.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
)

set(TGT_controldata_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(controldata_DEF_OPTIONS ${MACRO_OPTIONS})
set(controldata_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(controldata_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(controldata_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(pg_controldata TGT_controldata_SRC TGT_controldata_INC "${controldata_DEF_OPTIONS}" "${controldata_COMPILE_OPTIONS}" "${controldata_LINK_OPTIONS}" "${controldata_LINK_LIBS}")
add_dependencies(pg_controldata elog_static pgport_static)
target_link_directories(pg_controldata PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)

install(TARGETS pg_controldata RUNTIME DESTINATION bin)

