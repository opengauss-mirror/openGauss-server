#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_lib_SRC)
set(lib_string_obj_SRC ${CMAKE_CURRENT_SOURCE_DIR}/string.cpp )
set(TGT_lib_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(lib_DEF_OPTIONS ${MACRO_OPTIONS})
set(lib_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(lib_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_lib TGT_lib_SRC TGT_lib_INC "${lib_DEF_OPTIONS}" "${lib_COMPILE_OPTIONS}" "${lib_LINK_OPTIONS}")
add_static_objtarget(lib_string_obj lib_string_obj_SRC TGT_lib_INC "${lib_DEF_OPTIONS}" "${lib_COMPILE_OPTIONS}" "${lib_LINK_OPTIONS}")
