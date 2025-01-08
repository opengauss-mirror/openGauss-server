#This is the main CMAKE for build all gms_assert.
# gms_assert
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_assert_SRC)
set(TGT_gms_assert_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_assert
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_assert_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_assert_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_assert_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_assert TGT_gms_assert_SRC TGT_gms_assert_INC "${gms_assert_DEF_OPTIONS}" "${gms_assert_COMPILE_OPTIONS}" "${gms_assert_LINK_OPTIONS}")
set_target_properties(gms_assert PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_assert.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_assert--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_assert DESTINATION lib/postgresql)
