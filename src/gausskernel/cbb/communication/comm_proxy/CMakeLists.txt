#This is the main CMAKE for build object.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_comm_proxy_SRC)

set(TGT_comm_proxy_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${NUMA_INCLUDE_PATH}
)

set(comm_proxy_DEF_OPTIONS ${MACRO_OPTIONS})
set(comm_proxy_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(comm_proxy_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_communication_comm_proxy TGT_comm_proxy_SRC TGT_comm_proxy_INC "${comm_proxy_DEF_OPTIONS}" "${comm_proxy_COMPILE_OPTIONS}" "${comm_proxy_LINK_OPTIONS}")
install(FILES ${PROJECT_SRC_DIR}/gausskernel/cbb/communication/comm_proxy/gs_gazelle.conf.sample
    DESTINATION share/postgresql/
)
