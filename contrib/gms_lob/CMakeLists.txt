#This is the main CMAKE for build all gms_stats.
# gms_stats
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_lob_SRC)
set(TGT_gms_lob_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_lob
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_lob_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_lob_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_lob_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_lob TGT_gms_lob_SRC TGT_gms_lob_INC "${gms_lob_DEF_OPTIONS}" "${gms_lob_COMPILE_OPTIONS}" "${gms_lob_LINK_OPTIONS}")
set_target_properties(gms_lob PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_lob.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_lob--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_lob DESTINATION lib/postgresql)
