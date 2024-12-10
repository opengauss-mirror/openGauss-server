# This is the main CMAKE for build all gms_match.
# gms_match
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_match_SRC)
set(TGT_gms_match_INC
    ${PROJECT_OPENGS_DIR}/contrib/gms_match
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_match_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_match_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_match_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_match TGT_gms_match_SRC TGT_gms_match_INC "${gms_match_DEF_OPTIONS}" "${gms_match_COMPILE_OPTIONS}" "${gms_match_LINK_OPTIONS}")
set_target_properties(gms_match PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_match.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_match--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_match DESTINATION lib/postgresql)
