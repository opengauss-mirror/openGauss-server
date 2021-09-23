#This is the main CMAKE for build all components.
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/client_logic_common/cstrings_map.cpp ${CMAKE_CURRENT_SOURCE_DIR}/cstrings_map.cpp
)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_client_logic_SRC)

set(TGT_client_logic_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
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

set(client_logic_DEF_OPTIONS ${MACRO_OPTIONS})
set(client_logic_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(client_logic_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_client_logic TGT_client_logic_SRC TGT_client_logic_INC "${client_logic_DEF_OPTIONS}" "${client_logic_COMPILE_OPTIONS}" "${client_logic_LINK_OPTIONS}")
add_dependencies(common_backend_client_logic libpq_cstrings_map)

