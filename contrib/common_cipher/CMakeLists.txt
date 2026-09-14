#This is the main CMAKE for build all common_cipher.
# common_cipher.so
# Keep the source list aligned with Makefile; do not glob test.cpp.
set(TGT_common_cipher_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/common_err.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_algo.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_internal_interfaces.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_cipher.cpp
)
set(common_cipher_DEF_OPTIONS ${MACRO_OPTIONS})
set(common_cipher_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(common_cipher_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(common_cipher TGT_common_cipher_SRC "" "${common_cipher_DEF_OPTIONS}" "${common_cipher_COMPILE_OPTIONS}" "${common_cipher_LINK_OPTIONS}")
set_target_properties(common_cipher PROPERTIES PREFIX "")
install(TARGETS common_cipher DESTINATION lib/postgresql)
