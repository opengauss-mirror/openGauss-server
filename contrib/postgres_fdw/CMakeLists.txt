#This is the main CMAKE for build all postgres_fdw.
# postgres_fdw
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_postgres_fdw_SRC)
set(TGT_postgres_fdw_INC ${PROJECT_SRC_DIR}/common/interfaces/libpq ${PROJECT_SRC_DIR}/lib/gstrace ${PROJECT_SRC_DIR}/distribute/include)
set(postgres_fdw_DEF_OPTIONS ${MACRO_OPTIONS})
set(postgres_fdw_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(postgres_fdw_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(postgres_fdw TGT_postgres_fdw_SRC TGT_postgres_fdw_INC "${postgres_fdw_DEF_OPTIONS}" "${postgres_fdw_COMPILE_OPTIONS}" "${postgres_fdw_LINK_OPTIONS}")
set_target_properties(postgres_fdw PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgres_fdw.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgres_fdw--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS postgres_fdw DESTINATION lib/postgresql)
