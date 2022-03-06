#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pgoutput_SRC)

set(pgoutput_DEF_OPTIONS ${MACRO_OPTIONS})
set(pgoutput_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
set(pgoutput_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pgoutput TGT_pgoutput_SRC "" "${pgoutput_DEF_OPTIONS}" "${pgoutput_COMPILE_OPTIONS}" "${pgoutput_LINK_OPTIONS}")
set_target_properties(pgoutput PROPERTIES PREFIX "")
install(TARGETS pgoutput LIBRARY DESTINATION lib/postgresql)
