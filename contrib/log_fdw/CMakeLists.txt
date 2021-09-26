#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_log_fdw_SRC)

set(TGT_log_fdw_INC 
    ${ZLIB_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/bin/gs_log
    ${PROJECT_SRC_DIR}/common/timezone
)

set(log_fdw_DEF_OPTIONS ${MACRO_OPTIONS} -D_GSLOG_ITERATOR_MODE_)
set(log_fdw_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(log_fdw_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(contrib_log_fdw TGT_log_fdw_SRC TGT_log_fdw_INC "${log_fdw_DEF_OPTIONS}" "${log_fdw_COMPILE_OPTIONS}" "${log_fdw_LINK_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/log_fdw.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/log_fdw--1.0.sql
    DESTINATION share/postgresql/extension/
)

