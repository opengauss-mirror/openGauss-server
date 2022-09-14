#This is the main CMAKE for build all components.
# gs_clean bin
set(TGT_clean_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgxc_clean.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/txninfo.cpp
)

set(TGT_clean_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)
set(clean_DEF_OPTIONS ${MACRO_OPTIONS} -D_FORTIFY_SOURCE=2)
set(clean_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -O2)
set(clean_LINK_OPTIONS ${BIN_LINK_OPTIONS} -s)
set(clean_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -lpq)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND clean_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()
add_bintarget(gs_clean TGT_clean_SRC TGT_clean_INC "${clean_DEF_OPTIONS}" "${clean_COMPILE_OPTIONS}" "${clean_LINK_OPTIONS}" "${clean_LINK_LIBS}")
add_dependencies(gs_clean elog_static pgport_static pq)
target_link_directories(gs_clean PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS gs_clean RUNTIME DESTINATION bin)

