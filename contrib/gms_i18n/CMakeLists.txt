#This is the main CMAKE for build all gms_i18n.
# gms_i18n
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_i18n_SRC)
set(TGT_gms_i18n_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_i18n
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_i18n_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_i18n_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_i18n_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_i18n TGT_gms_i18n_SRC TGT_gms_i18n_INC "${gms_i18n_DEF_OPTIONS}" "${gms_i18n_COMPILE_OPTIONS}" "${gms_i18n_LINK_OPTIONS}")
set_target_properties(gms_i18n PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_i18n.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_i18n--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_i18n DESTINATION lib/postgresql)
