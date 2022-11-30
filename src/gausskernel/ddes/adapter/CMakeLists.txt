#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dms_adapter_SRC)

set(TGT_dms_adapter_INC 
    ${PROJECT_SRC_DIR}/include
)

set(dms_adapter_DEF_OPTIONS ${MACRO_OPTIONS} -DOPENGAUSS)
set(dms_adapter_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dms_adapter_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_ddes_dms_adapter TGT_dms_adapter_SRC TGT_dms_adapter_INC "${dms_adapter_DEF_OPTIONS}" "${dms_adapter_COMPILE_OPTIONS}" "${dms_adapter_LINK_OPTIONS}")