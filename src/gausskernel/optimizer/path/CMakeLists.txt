#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_path_SRC)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
list(REMOVE_ITEM TGT_path_SRC
   ${CMAKE_CURRENT_SOURCE_DIR}/pgxcpath_single.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/streampath_single.cpp
)
endif()

set(TGT_path_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(path_DEF_OPTIONS ${MACRO_OPTIONS})
set(path_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(path_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_path TGT_path_SRC TGT_path_INC "${path_DEF_OPTIONS}" "${path_COMPILE_OPTIONS}" "${path_LINK_OPTIONS}")

