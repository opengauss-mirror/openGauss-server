#This is the main CMAKE for build all gms_output.
# gms_output
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_output_SRC)
set(TGT_gms_output_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_output
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_output_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_output_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_output_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_output TGT_gms_output_SRC TGT_gms_output_INC "${gms_output_DEF_OPTIONS}" "${gms_output_COMPILE_OPTIONS}" "${gms_output_LINK_OPTIONS}")
set_target_properties(gms_output PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_output.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_output--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_output DESTINATION lib/postgresql)
