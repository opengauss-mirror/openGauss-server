#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_comm_SRC)

set(TGT_comm_INC 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
)

set(comm_DEF_OPTIONS ${MACRO_OPTIONS})
set(comm_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(comm_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_communication TGT_comm_SRC TGT_comm_INC "${comm_DEF_OPTIONS}" "${comm_COMPILE_OPTIONS}" "${comm_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/communication/libcomm_core
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/communication/libcomm_utils
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/communication/comm_proxy
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/communication/lib_hccom4db
)

add_subdirectory(libcomm_core)
add_subdirectory(libcomm_utils)
add_subdirectory(comm_proxy)
add_subdirectory(lib_hcom4db)
