#This is the main CMAKE for build all gms_stats.
# gms_stats
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_stats_SRC)
set(TGT_gms_stats_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_stats
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_stats_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_stats_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_stats_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_stats TGT_gms_stats_SRC TGT_gms_stats_INC "${gms_stats_DEF_OPTIONS}" "${gms_stats_COMPILE_OPTIONS}" "${gms_stats_LINK_OPTIONS}")
set_target_properties(gms_stats PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_stats.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_stats--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_stats DESTINATION lib/postgresql)
