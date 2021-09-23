#This is the main CMAKE for build all components.
execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/dynloader/linux.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dynloader.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/sysv_sema.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pg_sema.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/sysv_shmem.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pg_shmem.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/unix_latch.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pg_latch.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/tas/dummy.s ${CMAKE_CURRENT_SOURCE_DIR}/tas.s
)

execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/dynloader/linux.h ${PROJECT_SRC_DIR}/include/dynloader.h
)

list(APPEND TGT_port_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/dynloader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_sema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_shmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_latch.cpp
)

set(TGT_port_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(port_DEF_OPTIONS ${MACRO_OPTIONS})
set(port_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(port_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_port TGT_port_SRC TGT_port_INC "${port_DEF_OPTIONS}" "${port_COMPILE_OPTIONS}" "${port_LINK_OPTIONS}")
