#This is the main CMAKE for build all ndpplugin.
# ndpplugin.so

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_ndpplugin_SRC)

SET(TGT_ndpplugin_INC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(ndpplugin_DEF_OPTIONS ${MACRO_OPTIONS} -DNDP_CLIENT -DGlobalCache)
set(ndpplugin_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(ndpplugin_LINK_OPTIONS ${LIB_LINK_OPTIONS})

add_shared_libtarget(ndpplugin TGT_ndpplugin_SRC TGT_ndpplugin_INC "${ndpplugin_DEF_OPTIONS}" "${ndpplugin_COMPILE_OPTIONS}" "${ndpplugin_LINK_OPTIONS}")
set_target_properties(ndpplugin PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/ndpplugin.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/ndpplugin--1.0.sql
    DESTINATION share/postgresql/extension/
)

install(TARGETS ndpplugin DESTINATION lib/postgresql)
