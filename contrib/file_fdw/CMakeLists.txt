#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_file_fdw_SRC)

set(TGT_file_fdw_INC 
    ${PROJECT_OPENGS_DIR}/contrib/file_fdw
    ${PROJECT_OPENGS_DIR}/contrib
)

set(file_fdw_DEF_OPTIONS ${MACRO_OPTIONS})
set(file_fdw_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(file_fdw_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(contrib_file_fdw TGT_file_fdw_SRC TGT_file_fdw_INC "${file_fdw_DEF_OPTIONS}" "${file_fdw_COMPILE_OPTIONS}" "${file_fdw_LINK_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/file_fdw--1.0.sql
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/file_fdw.control
    DESTINATION share/postgresql/extension/
)

