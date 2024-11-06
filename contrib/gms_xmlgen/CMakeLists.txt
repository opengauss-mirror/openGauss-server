#This is the main CMAKE for build all gms_xmlgen.
# gms_xmlgen
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_xmlgen_SRC)
set(TGT_gms_xmlgen_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_xmlgen
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_xmlgen_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_xmlgen_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_xmlgen_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_xmlgen TGT_gms_xmlgen_SRC TGT_gms_xmlgen_INC "${gms_xmlgen_DEF_OPTIONS}" "${gms_xmlgen_COMPILE_OPTIONS}" "${gms_xmlgen_LINK_OPTIONS}")
set_target_properties(gms_xmlgen PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_xmlgen.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_xmlgen--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_xmlgen DESTINATION lib/postgresql)
