#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_vectorsonic_SRC)

set(TGT_vectorsonic_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(vectorsonic_DEF_OPTIONS ${MACRO_OPTIONS})
set(vectorsonic_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(vectorsonic_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_vecexecutor_vectorsonic TGT_vectorsonic_SRC TGT_vectorsonic_INC "${vectorsonic_DEF_OPTIONS}" "${vectorsonic_COMPILE_OPTIONS}" "${vectorsonic_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/runtime/vecexecutor/vectorsonic/sonicarray
)

add_subdirectory(sonicarray)
