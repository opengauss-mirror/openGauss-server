# roach_api_stub.so
set(TGT_roach_api_stub_SRC ${CMAKE_CURRENT_SOURCE_DIR}/roach_api_stub.cpp)
SET(TGT_roach_api_stub_INC 
    ${PROJECT_SRC_DIR}/include/libpq
    ${LIBOPENSSL_INCLUDE_PATH}
)
set(roach_api_stub_DEF_OPTIONS ${MACRO_OPTIONS})
set(roach_api_stub_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(roach_api_stub_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(roach_api_stub TGT_roach_api_stub_SRC TGT_roach_api_stub_INC "${roach_api_stub_DEF_OPTIONS}" "${roach_api_stub_COMPILE_OPTIONS}" "${roach_api_stub_LINK_OPTIONS}")
