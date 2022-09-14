#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_sequence_SRC)

set(TGT_sequence_INC
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

set(sequence_DEF_OPTIONS ${MACRO_OPTIONS} -DOBS_SERVER)
set(sequence_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(sequence_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_commands_sequence TGT_sequence_SRC TGT_sequence_INC "${sequence_DEF_OPTIONS}" "${sequence_COMPILE_OPTIONS}" "${sequence_LINK_OPTIONS}")
