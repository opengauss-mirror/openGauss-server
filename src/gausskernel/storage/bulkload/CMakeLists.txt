#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_bulkload_SRC)

set(TGT_bulkload_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(bulkload_DEF_OPTIONS ${MACRO_OPTIONS} -DOBS_SERVER)
set(bulkload_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(bulkload_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_bulkload TGT_bulkload_SRC TGT_bulkload_INC "${bulkload_DEF_OPTIONS}" "${bulkload_COMPILE_OPTIONS}" "${bulkload_LINK_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/dist_fdw--1.0.sql
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/dist_fdw.control
    DESTINATION share/postgresql/extension/
)
