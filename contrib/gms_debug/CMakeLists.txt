#This is the main CMAKE for build all gms_debug.
# gms_debug
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_debug_SRC)
set(TGT_gms_debug_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_debug
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_debug_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_debug_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_debug_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_debug TGT_gms_debug_SRC TGT_gms_debug_INC "${gms_debug_DEF_OPTIONS}" "${gms_debug_COMPILE_OPTIONS}" "${gms_debug_LINK_OPTIONS}")
set_target_properties(gms_debug PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_debug.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_debug--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_debug DESTINATION lib/postgresql)
