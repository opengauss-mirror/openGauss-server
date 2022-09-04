#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_commands_SRC)

if("${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(REMOVE_ITEM TGT_commands_SRC ${CMAKE_CURRENT_SOURCE_DIR}/obs_stream.cpp)
endif()

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/sequence
)

set(TGT_commands_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
)

add_subdirectory(sequence)

set(commands_DEF_OPTIONS ${MACRO_OPTIONS} -DOBS_SERVER)
set(commands_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(commands_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_commands TGT_commands_SRC TGT_commands_INC "${commands_DEF_OPTIONS}" "${commands_COMPILE_OPTIONS}" "${commands_LINK_OPTIONS}")
