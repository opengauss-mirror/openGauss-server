#This is the main CMAKE for build all security_plugin.
# security_plugin.so
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_security_plugin_SRC)
set(security_plugin_DEF_OPTIONS ${MACRO_OPTIONS})
set(security_plugin_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(security_plugin_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(security_plugin TGT_security_plugin_SRC "" "${security_plugin_DEF_OPTIONS}" "${security_plugin_COMPILE_OPTIONS}" "${security_plugin_LINK_OPTIONS}")
set_target_properties(security_plugin PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/security_plugin.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/security_plugin--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS security_plugin DESTINATION lib/postgresql)
