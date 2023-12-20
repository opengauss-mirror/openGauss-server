#This is the main CMAKE for build all components.
# port.cmake
# port_srv.a it is needed to create a path for port srv
execute_process(COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/port_srv)
execute_process(COMMAND mkdir -p ${CMAKE_CURRENT_SOURCE_DIR}/port_srv)
execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/fls.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/fls.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/strlcat.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/strlcat.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/strlcpy.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/getpeereid.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sse42.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sse42.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sb8.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sb8.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_choose.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_choose.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/chklocale.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/dirmod.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/dirmod.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/erand48.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/erand48.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/exec.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/exec.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/inet_net_ntop.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/noblock.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/path.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pg_bitutils.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_bitutils.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pgcheckdir.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgcheckdir.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pgmkdirp.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgmkdirp.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgsleep.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgstrcasecmp.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/qsort.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/qsort.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/qsort_arg.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/qsort_arg.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/sprompt.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/sprompt.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/thread.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/flock.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/flock.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasestr.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgstrcasestr.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_thread.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_thread.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_env_r.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_getopt_r.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_getopt_r.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_readdir.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_strerror.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_syscall_lock.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/gs_system.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_system.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/cipher.cpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/crc32_arm_parallel.S ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/crc32_arm_parallel.S
)

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/file/fio_device.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/fio_dss.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)

list(APPEND TGT_port_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/fls.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcat.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sse42.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sb8.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_choose.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/dirmod.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/erand48.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/exec.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_bitutils.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgcheckdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgmkdirp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/qsort.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/qsort_arg.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/sprompt.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/flock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasestr.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_getopt_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_system.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/tool_common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)

# aarch64
if("${BUILD_TUPLE}" STREQUAL "aarch64")
    list(REMOVE_ITEM TGT_port_SRC ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sb8.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sse42.cpp)
    set(TGT_crc32_arm_parallel_SRC ${CMAKE_CURRENT_SOURCE_DIR}/crc32_arm_parallel.S)
endif()

if("${BUILD_TUPLE}" STREQUAL "loongarch64")
    list(REMOVE_ITEM TGT_port_SRC ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_choose.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pg_crc32c_sse42.cpp)
endif()

SET(TGT_pgport_INC 
    ${PROJECT_SRC_DIR}/common/backend
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/include
    ${LIBOPENSSL_INCLUDE_PATH}
)

enable_language(CXX ASM)
set(pgport_DEF_OPTIONS -DFRONTEND ${MACRO_OPTIONS})
set(pgport_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
add_static_libtarget(pgport TGT_port_SRC TGT_pgport_INC "${pgport_DEF_OPTIONS}" "${pgport_COMPILE_OPTIONS}")
if("${BUILD_TUPLE}" STREQUAL "aarch64")
    set(pgport_crc32_COMPILE_OPTIONS ${pgport_COMPILE_OPTIONS} -march=armv8-a+crc+crypto)
    add_static_objtarget(pgport_crc32 TGT_crc32_arm_parallel_SRC TGT_pgport_INC "${pgport_DEF_OPTIONS}" "${pgport_crc32_COMPILE_OPTIONS}" "")
    add_dependencies(pgport_static pgport_crc32)
    add_custom_command(TARGET pgport_static POST_BUILD
        COMMAND /usr/bin/ar rs ${CMAKE_BINARY_DIR}/lib/libpgport.a ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/pgport_crc32.dir/crc32_arm_parallel.S.o
    )
endif()
enable_language(CXX)
install(TARGETS pgport_static ARCHIVE DESTINATION lib)
set(TGT_port_pgsleep_SRC ${PROJECT_SRC_DIR}/common/port/pgsleep.cpp)
add_static_objtarget(port_pgsleep TGT_port_pgsleep_SRC TGT_pgport_INC "${pgport_DEF_OPTIONS}" "${pgport_COMPILE_OPTIONS}" "")

# pgport_srv.cmake
list(APPEND TGT_pgport_srv_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/fls.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/strlcat.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/strlcpy.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/getpeereid.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sse42.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sb8.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_choose.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/chklocale.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/dirmod.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/erand48.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/exec.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/inet_net_ntop.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/noblock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/path.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_bitutils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgcheckdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgmkdirp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgsleep.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgstrcasecmp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/qsort.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/qsort_arg.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/sprompt.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/flock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pgstrcasestr.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_env_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_getopt_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_readdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_strerror.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_syscall_lock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/gs_system.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/cipher.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)

# aarch64
if("${BUILD_TUPLE}" STREQUAL "aarch64")
    list(REMOVE_ITEM TGT_pgport_srv_SRC ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sb8.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sse42.cpp)
    set(TGT_crc32_arm_parallel_srv_SRC ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/crc32_arm_parallel.S)
endif()

if("${BUILD_TUPLE}" STREQUAL "loongarch64")
    list(REMOVE_ITEM TGT_pgport_srv_SRC ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_choose.cpp ${CMAKE_CURRENT_SOURCE_DIR}/port_srv/pg_crc32c_sse42.cpp)
endif()

SET(TGT_pgport_srv_INC 
    ${PROJECT_SRC_DIR}/common/backend
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/include
    ${LIBOPENSSL_INCLUDE_PATH}
)
enable_language(CXX ASM)
set(port_srv_DEF_OPTIONS ${MACRO_OPTIONS})
set(port_srv_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
list(REMOVE_ITEM port_srv_DEF_OPTIONS -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE)
list(REMOVE_ITEM port_srv_COMPILE_OPTIONS -pthread -fstack-protector)
add_static_libtarget(pgport_srv TGT_pgport_srv_SRC TGT_pgport_srv_INC "${port_srv_DEF_OPTIONS}" "${port_srv_COMPILE_OPTIONS}")
if("${BUILD_TUPLE}" STREQUAL "aarch64")
    set(pgport_srv_crc32_COMPILE_OPTIONS ${port_srv_COMPILE_OPTIONS} -march=armv8-a+crc+crypto)
    add_static_objtarget(pgport_srv_crc32 TGT_crc32_arm_parallel_srv_SRC TGT_pgport_srv_INC "${port_srv_DEF_OPTIONS}" "${pgport_srv_crc32_COMPILE_OPTIONS}" "")
    add_dependencies(pgport_srv_static pgport_srv_crc32)
    add_custom_command(TARGET pgport_srv_static POST_BUILD
        COMMAND /usr/bin/ar rs ${CMAKE_BINARY_DIR}/lib/libpgport_srv.a ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/pgport_srv_crc32.dir/port_srv/crc32_arm_parallel.S.o
    )
endif()
enable_language(CXX)
install(TARGETS pgport_srv_static ARCHIVE DESTINATION lib)

# pgport_tool.cmake
set(TGT_pgport_tool_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/fls.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcat.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/dirmod.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/erand48.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/exec.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_bitutils.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgcheckdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgmkdirp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/qsort.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/qsort_arg.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/sprompt.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/flock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasestr.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_thread.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_getopt_r.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_system.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)
SET(TGT_pgport_tool_INC 
    ${PROJECT_SRC_DIR}/common/backend
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/include
    ${LIBOPENSSL_INCLUDE_PATH}
)
set(pgport_tool_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND)
set(pgport_tool_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all )
set(pgport_tool_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM pgport_tool_LINK_OPTIONS -fstack-protector -pthread)
add_shared_libtarget(pgport_tool TGT_pgport_tool_SRC TGT_pgport_tool_INC "${pgport_tool_DEF_OPTIONS}" "${pgport_tool_COMPILE_OPTIONS}" "${pgport_tool_LINK_OPTIONS}")
SET_TARGET_PROPERTIES(pgport_tool PROPERTIES VERSION 1)
install(TARGETS pgport_tool LIBRARY DESTINATION lib)

