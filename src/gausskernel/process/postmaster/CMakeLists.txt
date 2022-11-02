#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_postmaster_SRC)

if("${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(REMOVE_ITEM TGT_postmaster_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/barrier_creator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/barrier_arch.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/aiocompleter.cpp
    )
endif()

set(TGT_postmaster_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
    ${LICENSE_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${NUMA_INCLUDE_PATH}
)

set(postmaster_DEF_OPTIONS ${MACRO_OPTIONS})
set(postmaster_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(postmaster_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_process_postmaster TGT_postmaster_SRC TGT_postmaster_INC "${postmaster_DEF_OPTIONS}" "${postmaster_COMPILE_OPTIONS}" "${postmaster_LINK_OPTIONS}")

