#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_storage_test_SRC)

set(TGT_storage_test_INC 
    ${PROJECT_SRC_DIR}/include
)

set(storage_test_DEF_OPTIONS ${MACRO_OPTIONS})
set(storage_test_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(storage_test_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_test TGT_storage_test_SRC TGT_storage_test_INC "${storage_test_DEF_OPTIONS}" "${storage_test_COMPILE_OPTIONS}" "${storage_test_LINK_OPTIONS}")
