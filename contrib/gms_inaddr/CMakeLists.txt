#This is the main CMAKE for build all components.
# gms_inaddr.so
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/gms_inaddr TGT_gms_inaddr_SRC)

set(gms_inaddr_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_inaddr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_inaddr_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_inaddr TGT_gms_inaddr_SRC "" "${gms_inaddr_DEF_OPTIONS}" "${gms_inaddr_COMPILE_OPTIONS}" "${gms_inaddr_LINK_OPTIONS}")
set_target_properties(gms_inaddr PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_inaddr.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_inaddr--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_inaddr LIBRARY DESTINATION lib/postgresql)

