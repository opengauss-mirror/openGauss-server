set(TGT_pg_trgm_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/trgm_op.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/trgm_gist.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/trgm_gin.cpp
)
set(TGT_pg_trgm_INC ${CMAKE_CURRENT_SOURCE_DIR})

set(pg_trgm_DEF_OPTIONS ${MACRO_OPTIONS})
set(pg_trgm_COMPILE_OPTIONS
    ${OPTIMIZE_OPTIONS}
    ${OS_OPTIONS}
    ${PROTECT_OPTIONS}
    ${WARNING_OPTIONS}
    ${LIB_SECURE_OPTIONS}
    ${CHECK_OPTIONS}
)
set(pg_trgm_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pg_trgm TGT_pg_trgm_SRC TGT_pg_trgm_INC
    "${pg_trgm_DEF_OPTIONS}"
    "${pg_trgm_COMPILE_OPTIONS}"
    "${pg_trgm_LINK_OPTIONS}")
set_target_properties(pg_trgm PROPERTIES PREFIX "")

install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_trgm.control
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_trgm--1.0.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_trgm--unpackaged--1.0.sql
    DESTINATION share/postgresql/extension/)
install(TARGETS pg_trgm DESTINATION lib/postgresql)
