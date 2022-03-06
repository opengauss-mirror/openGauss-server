#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_elog_SRC)

set(TGT_elog_INC 
    ${PROJECT_SRC_DIR}/common/backend
)

set(elog_DEF_OPTIONS ${MACRO_OPTIONS})
set(elog_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(elog TGT_elog_SRC TGT_elog_INC "${elog_DEF_OPTIONS}" "${elog_COMPILE_OPTIONS}")

