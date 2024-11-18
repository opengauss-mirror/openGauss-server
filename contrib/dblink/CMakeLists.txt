#This is the main CMAKE for build all gms_stats.
# gms_stats
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dblink_SRC)
set(TGT_dblink_INC 
    ${PROJECT_OPENGS_DIR}/contrib/dblink
    ${PROJECT_OPENGS_DIR}/contrib
)

set(dblink_DEF_OPTIONS ${MACRO_OPTIONS})
set(dblink_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dblink_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(dblink TGT_dblink_SRC TGT_dblink_INC "${dblink_DEF_OPTIONS}" "${dblink_COMPILE_OPTIONS}" "${dblink_LINK_OPTIONS}")
target_link_libraries(dblink PUBLIC -lodbc)
target_link_directories(dblink PUBLIC ${ODBC_HOME}/lib)
set_target_properties(dblink PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/dblink.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/dblink--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS dblink DESTINATION lib/postgresql)