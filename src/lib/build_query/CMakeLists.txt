#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_buildquery_SRC)

set(buildquery_DEF_OPTIONS ${MACRO_OPTIONS})
set(buildquery_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(buildquery TGT_buildquery_SRC "" "${buildquery_DEF_OPTIONS}" "${buildquery_COMPILE_OPTIONS}")
