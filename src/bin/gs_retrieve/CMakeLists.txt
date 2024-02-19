#This is the main CMAKE for build all components.
# gs_retrieve bin

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/transam/xlogreader.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/redo/xlogreader_common.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader_common.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/dss_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
)

set(TGT_retrieve_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader_common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_retrieve.cpp
)

set(TGT_retrieve_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(retrieve_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DFRONTEND)
set(retrieve_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(retrieve_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(retrieve_LINK_LIBS libelog.a libpgcommon.a libpgport.a -lpq -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND retrieve_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()

add_bintarget(gs_retrieve TGT_retrieve_SRC TGT_retrieve_INC "${retrieve_DEF_OPTIONS}" "${retrieve_COMPILE_OPTIONS}" "${retrieve_LINK_OPTIONS}" "${retrieve_LINK_LIBS}")
add_dependencies(gs_retrieve elog_static pgport_static pgcommon_static pq)
target_link_directories(gs_retrieve PUBLIC ${LIBOPENSSL_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)

install(TARGETS gs_retrieve RUNTIME DESTINATION bin)

