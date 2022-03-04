#This is the main CMAKE for build bin.
list(APPEND TGT_transam_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/cbmfuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cbmparsexlog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/clog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/csnlog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/double_write.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/single_double_write.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/multi_redo_api.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/multi_redo_settings.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/multixact.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/redo_statistic.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rmgr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/seg_double_write.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/slru.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/transam.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/twophase.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/twophase_rmgr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/varsup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xact.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogfuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xloginsert.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xlogutils.cpp
)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    list(APPEND TGT_transam_SRC ${CMAKE_CURRENT_SOURCE_DIR}/gtm_single.cpp)
endif()

set(TGT_transam_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
    ${DCF_INCLUDE_PATH}
    ${NUMA_INCLUDE_PATH} 
)

set(transam_DEF_OPTIONS ${MACRO_OPTIONS})
set(transam_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(transam_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_transam TGT_transam_SRC TGT_transam_INC "${transam_DEF_OPTIONS}" 
    "${transam_COMPILE_OPTIONS}" "${transam_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/parallel_recovery
    ${CMAKE_CURRENT_SOURCE_DIR}/extreme_rto
)

add_subdirectory(parallel_recovery)
add_subdirectory(extreme_rto)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/recovery.conf.sample
    DESTINATION share/postgresql/
)
