#This is the main CMAKE for build bin.
execute_process(
    COMMAND perl generate-lwlocknames.pl ${CMAKE_CURRENT_SOURCE_DIR}/lwlocknames.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE LWLOCKNAMES
)

execute_process(
    COMMAND cp -f ${CMAKE_CURRENT_SOURCE_DIR}/lwlocknames.h ${PROJECT_SRC_DIR}/include/storage/lwlocknames.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE LWLOCKNAMES
)

set(TGT_lmgr_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/deadlock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lmgr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lwlock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lwlock_be.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predicate.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/proc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/s_lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/spin.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lwlocknames.cpp
)

set(TGT_lmgr_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${NUMA_INCLUDE_PATH}
)

set(lmgr_DEF_OPTIONS ${MACRO_OPTIONS})
set(lmgr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(lmgr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_lmgr TGT_lmgr_SRC TGT_lmgr_INC "${lmgr_DEF_OPTIONS}" "${lmgr_COMPILE_OPTIONS}" "${lmgr_LINK_OPTIONS}")

