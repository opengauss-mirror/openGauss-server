#This is the main CMAKE for build all gms_sql.
# gms_sql
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_sql_SRC)
set(TGT_gms_sql_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_sql
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_sql_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_sql_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_sql_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_sql TGT_gms_sql_SRC TGT_gms_sql_INC "${gms_sql_DEF_OPTIONS}" "${gms_sql_COMPILE_OPTIONS}" "${gms_sql_LINK_OPTIONS}")
set_target_properties(gms_sql PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_sql.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_sql--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_sql DESTINATION lib/postgresql)
