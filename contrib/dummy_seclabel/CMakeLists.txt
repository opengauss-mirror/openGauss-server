#This is the main CMAKE for build all dummy_seclabel.
# dummy_seclabel.so
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dummy_seclabel_SRC)

set(dummy_seclabel_DEF_OPTIONS ${MACRO_OPTIONS})
set(dummy_seclabel_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dummy_seclabel_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(dummy_seclabel TGT_dummy_seclabel_SRC "" "${dummy_seclabel_DEF_OPTIONS}" "${dummy_seclabel_COMPILE_OPTIONS}" "${dummy_seclabel_LINK_OPTIONS}")
set_target_properties(dummy_seclabel PROPERTIES PREFIX "")

