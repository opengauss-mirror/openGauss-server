#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client TGT_hotpatchclient_SRC)

set(hotpatchclient_DEF_OPTIONS ${MACRO_OPTIONS})
set(hotpatchclient_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(hotpatchclient TGT_hotpatchclient_SRC "" "${hotpatchclient_DEF_OPTIONS}" "${hotpatchclient_COMPILE_OPTIONS}")
