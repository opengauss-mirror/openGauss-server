#This is the main CMAKE for build all gms_utility.
# gms_utility
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_utility_SRC)
set(TGT_gms_utility_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_utility
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_utility_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_utility_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_utility_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_utility TGT_gms_utility_SRC TGT_gms_utility_INC "${gms_utility_DEF_OPTIONS}" "${gms_utility_COMPILE_OPTIONS}" "${gms_utility_LINK_OPTIONS}")
set_target_properties(gms_utility PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_utility.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_utility--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_utility DESTINATION lib/postgresql)