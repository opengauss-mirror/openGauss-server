#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_comm_utils_SRC)
list(REMOVE_ITEM TGT_comm_utils_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/libcomm_lock_free_queue.cpp
)

set(TGT_comm_utils_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
)

set(comm_utils_DEF_OPTIONS ${MACRO_OPTIONS})
set(comm_utils_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(comm_utils_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_communication_libcomm_utils TGT_comm_utils_SRC TGT_comm_utils_INC "${comm_utils_DEF_OPTIONS}" "${comm_utils_COMPILE_OPTIONS}" "${comm_utils_LINK_OPTIONS}")

