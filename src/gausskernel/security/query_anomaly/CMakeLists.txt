#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_query_anomaly_SRC)

set(TGT_query_anomaly_INC
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/include/query_anomaly
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
)

set(gs_query_anomaly_DEF_OPTIONS ${MACRO_OPTIONS})
set(gs_query_anomaly_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gs_query_anomaly_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_security_query_anomaly TGT_query_anomaly_SRC TGT_query_anomaly_INC "${gs_query_anomaly_DEF_OPTIONS}" "${gs_query_anomaly_COMPILE_OPTIONS}" "${gs_query_anomaly_LINK_OPTIONS}")

