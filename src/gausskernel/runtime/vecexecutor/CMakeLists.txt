#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_vecexecutor_SRC)

set(TGT_vecexecutor_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(vecexecutor_DEF_OPTIONS ${MACRO_OPTIONS})
set(vecexecutor_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(vecexecutor_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_vecexecutor TGT_vecexecutor_SRC TGT_vecexecutor_INC "${vecexecutor_DEF_OPTIONS}" "${vecexecutor_COMPILE_OPTIONS}" "${vecexecutor_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/vectorsonic
    ${CMAKE_CURRENT_SOURCE_DIR}/vecnode
)

add_subdirectory(vectorsonic)
add_subdirectory(vecnode)
