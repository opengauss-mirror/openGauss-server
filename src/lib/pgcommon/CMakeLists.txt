#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pgcommon_SRC)

set(pgcommon_DEF_OPTIONS ${MACRO_OPTIONS})
set(pgcommon_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(pgcommon TGT_pgcommon_SRC "" "${pgcommon_DEF_OPTIONS}" "${pgcommon_COMPILE_OPTIONS}")
