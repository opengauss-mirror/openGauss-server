#This is the main CMAKE for build object.
set(TGT_connector_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/connector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/odbc_connector.cpp
)
set(TGT_connector_odbc_SRC ${CMAKE_CURRENT_SOURCE_DIR}/odbc.cpp)

set(TGT_connector_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBODBC_INCLUDE_PATH}
)

set(connector_DEF_OPTIONS ${MACRO_OPTIONS})
set(connector_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(connector_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_extension_connector TGT_connector_SRC TGT_connector_INC "${connector_DEF_OPTIONS}" "${connector_COMPILE_OPTIONS}" "${connector_LINK_OPTIONS}")
set(connector_odbc_COMPILE_OPTIONS ${connector_COMPILE_OPTIONS})
add_static_objtarget(gausskernel_cbb_extension_connector_odbc TGT_connector_odbc_SRC TGT_connector_INC "${connector_DEF_OPTIONS}" "${connector_odbc_COMPILE_OPTIONS}" "${connector_LINK_OPTIONS}")

