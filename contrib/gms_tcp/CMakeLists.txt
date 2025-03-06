#This is the main CMAKE for build all gms_tcp.
# gms_tcp
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_tcp_SRC)
set(TGT_gms_tcp_INC
    ${PROJECT_OPENGS_DIR}/contrib/gms_tcp
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_tcp_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_tcp_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_tcp_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_tcp TGT_gms_tcp_SRC TGT_gms_tcp_INC "${gms_tcp_DEF_OPTIONS}" "${gms_tcp_COMPILE_OPTIONS}" "${gms_tcp_LINK_OPTIONS}")
set_target_properties(gms_tcp PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_tcp.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_tcp--1.1.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_tcp--1.0--1.1.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_tcp--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_tcp DESTINATION lib/postgresql)
