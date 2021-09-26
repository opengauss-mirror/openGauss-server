#This is the main CMAKE for build all components.
# test_decoding.so
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_test_decoding_SRC)

set(test_decoding_DEF_OPTIONS ${MACRO_OPTIONS})
set(test_decoding_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(test_decoding_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(test_decoding TGT_test_decoding_SRC "" "${test_decoding_DEF_OPTIONS}" "${test_decoding_COMPILE_OPTIONS}" "${test_decoding_LINK_OPTIONS}")
set_target_properties(test_decoding PROPERTIES PREFIX "")
install(TARGETS test_decoding DESTINATION lib/postgresql)
