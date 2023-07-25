#This is the main CMAKE for build all components.
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
set(CMAKE_MODULE_PATH 
    ${${CMAKE_CURRENT_SOURCE_DIR}}/jdbc
)
add_subdirectory(jdbc)
endif()
#execute_process(
#    COMMAND flex -CF -b -p -o scan.cpp scan.l
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser
#    OUTPUT_VARIABLE SCAN 
#)

set(frontend_parser_cmd_src
"${PROJECT_SRC_DIR}/common/interfaces/libpq/frontend_parser|||flex -CF -b -p -o'scan.cpp' scan.l|"
)

add_cmd_gen_when_configure(flex_target frontend_parser_cmd_src)

execute_process(
    COMMAND bison -d -o gram.cpp gram.y -t
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser
    OUTPUT_VARIABLE GRAM 
)

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/optimizer/commands/define.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-define.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/nodes/copyfuncs.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-copyfuncs.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/nodes/makefuncs.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-makefuncs.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/parser/parse_merge.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-parse_merge.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/nodes/value.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-value.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/utils/mb/wchar.cpp ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-wchar.cpp
)
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/chklocale.cpp ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/cipher.cpp ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/getpeereid.cpp ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/gs_env_r.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/gs_readdir.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/gs_strerror.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/gs_syscall_lock.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/inet_net_ntop.cpp ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/noblock.cpp ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/path.cpp ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/pgsleep.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/strlcpy.cpp ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/thread.cpp ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/utils/mb/encnames.cpp ${CMAKE_CURRENT_SOURCE_DIR}/encnames.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/libpq/ip.cpp ${CMAKE_CURRENT_SOURCE_DIR}/ip.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/libpq/md5.cpp ${CMAKE_CURRENT_SOURCE_DIR}/md5.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/libpq/sha2.cpp ${CMAKE_CURRENT_SOURCE_DIR}/sha2.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/pgstrcasecmp.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/file/fio_device.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/fio_dss.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)

# libpq.a
list(APPEND TGT_pq_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-auth.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-connect.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-exec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-misc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-print.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-lobj.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol2.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol3.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pqexpbuffer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pqsignal.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-secure.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libpq-events.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ip.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/md5.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sha2.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/encnames.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-wchar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/frontend_mbutils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/wstrncmp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp

)

set(TGT_pq_INC 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/common/port 
    ${KERBEROS_INCLUDE_PATH} 
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${LIBEDIT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/include/storage/file
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")    
    ${JAVA_HOME}/include 
    ${JAVA_HOME}/include/linux
endif()
)

set(pq_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DSO_MAJOR_VERSION=5)
set(pq_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
add_static_libtarget(pq TGT_pq_SRC TGT_pq_INC "${pq_DEF_OPTIONS}" "${pq_COMPILE_OPTIONS}")

# so pq
set(pq_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pq TGT_pq_SRC TGT_pq_INC "${pq_DEF_OPTIONS}" "${pq_COMPILE_OPTIONS}" "${pq_LINK_OPTIONS}")
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    target_link_libraries(pq PRIVATE com_err_gauss crypto gssapi_krb5_gauss gssrpc_gauss k5crypto_gauss krb5_gauss krb5support_gauss securec ssl)
else()
    target_link_libraries(pq PRIVATE crypto securec ssl)
endif()
target_link_directories(pq PUBLIC 
    ${LIBOPENSSL_LIB_PATH} ${KERBEROS_LIB_PATH} ${SECURE_LIB_PATH} 
    ${PROJECT_SRC_DIR}/common/port ${PROJECT_SRC_DIR}/gstrace/common
)
set_target_properties(pq PROPERTIES VERSION 5.5)
add_custom_command(TARGET pq POST_BUILD
    COMMAND ln -fs libpq.so.5.5 libpq.so.5
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS pq LIBRARY DESTINATION lib)
install(TARGETS pq_static ARCHIVE DESTINATION lib)

# libpq_ce.so
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
execute_process(COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce)
execute_process(COMMAND mkdir -p ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce)
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-auth.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-auth.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-connect.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-connect.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-exec.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-exec.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-lobj.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-lobj.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-misc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-misc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-print.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-print.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-protocol2.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-protocol2.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-protocol3.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-protocol3.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/fe-secure.cpp ${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce/fe-secure.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/file/fio_device.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/fio_dss.cpp ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp
)

SET(TGT_pq_ce_INC
    ${PROJECT_SRC_DIR}/include/libpq
    ${LIBOPENSSL_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_hooks
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_common
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_expressions
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_cache
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_processor
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_fmt
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_hooks/encryption_hooks
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_data_fetcher
    ${PROJECT_SRC_DIR}/gausskernel/security/keymgr/include
    ${JAVA_HOME}/include
    ${JAVA_HOME}/include/linux
)

list(APPEND TGT_pq_ce_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pqexpbuffer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pqsignal.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libpq-events.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/chklocale.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/inet_net_ntop.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/noblock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cipher.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgsleep.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ip.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/md5.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sha2.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/encnames.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_strerror.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_env_r.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_syscall_lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_readdir.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cl_state.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fio_dss.cpp

)
set(pq_ce_DEF_OPTIONS  ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DHAVE_CE -DWORDS_BIGENDIAN -DSO_MAJOR_VERSION=5)
set(pq_ce_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/libpq_ce TGT_libpq_ce_SRC)
set(libpq_ce_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DHAVE_CE -DWORDS_BIGENDIAN -DSO_MAJOR_VERSION=5)
list(REMOVE_ITEM libpq_ce_DEF_OPTIONS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS)
set(libpq_ce_COMPILE_OPTIONS ${pq_ce_COMPILE_OPTIONS})
list(REMOVE_ITEM libpq_ce_COMPILE_OPTIONS -fstack-protector -pthread)
add_static_objtarget(libpq_ce TGT_libpq_ce_SRC TGT_pq_ce_INC "${libpq_ce_DEF_OPTIONS}" "${libpq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_hooks/encryption_hooks TGT_encryption_hooks_SRC)
set(encryption_hooks_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
list(REMOVE_ITEM encryption_hooks_DEF_OPTIONS -D_GNU_SOURCE)
add_static_objtarget(encryption_hooks TGT_encryption_hooks_SRC TGT_pq_ce_INC "${encryption_hooks_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_common TGT_client_logic_common_SRC)
set(TGT_client_logic_common_cstrings_map_SRC ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_common/cstrings_map.cpp)
set(client_logic_common_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
add_static_objtarget(client_logic_common TGT_client_logic_common_SRC TGT_pq_ce_INC "${client_logic_common_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")
add_static_objtarget(libpq_cstrings_map TGT_client_logic_common_cstrings_map_SRC TGT_pq_ce_INC "${client_logic_common_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_expressions TGT_client_logic_expressions_SRC)
set(client_logic_expressions_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DHAVE_CE -DWORDS_BIGENDIAN -DSO_MAJOR_VERSION=5)
add_static_objtarget(client_logic_expressions TGT_client_logic_expressions_SRC TGT_pq_ce_INC "${client_logic_expressions_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_cache TGT_client_logic_cache_SRC)
set(client_logic_cache_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
add_static_objtarget(client_logic_cache TGT_client_logic_cache_SRC TGT_pq_ce_INC "${client_logic_cache_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_processor TGT_client_logic_processor_SRC)
set(client_logic_processor_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
list(REMOVE_ITEM client_logic_processor_DEF_OPTIONS -D_GNU_SOURCE)
add_static_objtarget(client_logic_processor TGT_client_logic_processor_SRC TGT_pq_ce_INC "${client_logic_processor_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_fmt TGT_client_logic_fmt_SRC)
set(client_logic_fmt_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DHAVE_CE -DWORDS_BIGENDIAN -DSO_MAJOR_VERSION=5)
add_static_objtarget(client_logic_fmt TGT_client_logic_fmt_SRC TGT_pq_ce_INC "${client_logic_fmt_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_hooks TGT_client_logic_hooks_SRC)
set(client_logic_hooks_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
list(REMOVE_ITEM client_logic_hooks_DEF_OPTIONS -D_GNU_SOURCE)
add_static_objtarget(client_logic_hooks TGT_client_logic_hooks_SRC TGT_pq_ce_INC "${client_logic_hooks_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_data_fetcher TGT_client_logic_data_fetcher_SRC)
set(client_logic_data_fetcher_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
add_static_objtarget(client_logic_data_fetcher TGT_client_logic_data_fetcher_SRC TGT_pq_ce_INC "${client_logic_data_fetcher_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

list(APPEND TGT_frontend_parser_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-copyfuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-define.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-makefuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-parse_merge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/feparser_memutils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-value.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/fe-wchar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/frontend_mbutils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/gram.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/list.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/parser_compatibility.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/scan.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/scansup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/frontend_parser/wstrncmp.cpp
)

set(pq_ce_DEF_OPTIONS  ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DHAVE_CE -DWORDS_BIGENDIAN -DSO_MAJOR_VERSION=5)
set(frontend_parser_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
list(REMOVE_ITEM frontend_parser_DEF_OPTIONS -D_GNU_SOURCE)
add_static_objtarget(frontend_parser TGT_frontend_parser_SRC TGT_pq_ce_INC "${frontend_parser_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")

set(pq_ce_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pq_ce TGT_pq_ce_SRC TGT_pq_ce_INC "${pq_ce_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "${pq_ce_LINK_OPTIONS}")
set(pq_ce_LINK_LIBS libpq_ce encryption_hooks client_logic_common client_logic_expressions client_logic_cache client_logic_processor client_logic_fmt client_logic_hooks client_logic_data_fetcher frontend_parser)
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        set(pq_ce_LINK_LIBS ${pq_ce_LINK_LIBS} -lgs_ktool -lkmc)
    endif()
endif()
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    target_link_libraries(pq_ce PRIVATE ${pq_ce_LINK_LIBS} libkeymgr.a -lcurl -lcjson -lssl -lcrypto -l${SECURE_C_CHECK} -lpthread -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
else()
    target_link_libraries(pq_ce PRIVATE ${pq_ce_LINK_LIBS} libkeymgr.a -lcurl -lcjson -lssl -lcrypto -l${SECURE_C_CHECK} -lpthread)
endif()
add_dependencies(pq_ce libpq_ce encryption_hooks client_logic_common client_logic_expressions client_logic_cache client_logic_processor client_logic_fmt client_logic_hooks client_logic_data_fetcher frontend_parser)
target_link_directories(pq_ce PUBLIC 
    ${SECURE_LIB_PATH}
    ${KMC_LIB_PATH}
    ${LIBOPENSSL_LIB_PATH}
    ${CJSON_LIB_PATH}
    ${LIBCURL_LIB_PATH}
    ${KERBEROS_LIB_PATH}
    ${CMAKE_BINARY_DIR}/lib
)
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        add_dependencies(pq_ce gs_ktool)
        add_dependencies(pq_ce keymgr)
    endif()
endif()
SET_TARGET_PROPERTIES(pq_ce PROPERTIES VERSION 5.5)
add_custom_command(TARGET pq_ce POST_BUILD
    COMMAND ln -fs libpq_ce.so.5.5 libpq_ce.so.5
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS pq_ce LIBRARY DESTINATION lib)
else()
SET(TGT_pq_ce_INC
    ${PROJECT_SRC_DIR}/include/libpq
    ${LIBOPENSSL_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(pq_ce_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/client_logic_common TGT_client_logic_common_SRC)
set(TGT_client_logic_common_cstrings_map_SRC ${CMAKE_CURRENT_SOURCE_DIR}/client_logic_common/cstrings_map.cpp)
set(client_logic_common_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DHAVE_CE)
add_static_objtarget(libpq_cstrings_map TGT_client_logic_common_cstrings_map_SRC TGT_pq_ce_INC "${client_logic_common_DEF_OPTIONS}" "${pq_ce_COMPILE_OPTIONS}" "")
endif() #end of not build pq_ce

# gauss pq ogject
list(APPEND TGT_libpq_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-connect.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-secure.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-misc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol3.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol2.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-exec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fe-auth.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pqexpbuffer.cpp
)

set(TGT_libpq_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
)

set(libpq_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND -DFRONTEND_PARSER -DUNSAFE_STAT_OK -DSO_MAJOR_VERSION=5)
set(libpq_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
set(libpq_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_interfaces_libpq TGT_libpq_SRC TGT_libpq_INC "${libpq_DEF_OPTIONS}" "${libpq_COMPILE_OPTIONS}" "${libpq_LINK_OPTIONS}")

if("${ENABLE_UT}" STREQUAL "ON")
    install_symlink(libpq_ce.so.5.5 libutlibpq_ce.so lib)
    install_symlink(libpq.so.5.5 libutlibpq.so lib)
endif()
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/pg_service.conf.sample
    DESTINATION share/postgresql/
)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install_symlink(libpq_ce.so.5.5 libpq_ce.so.5 ${prefix_home}/lib)
endif()
install_symlink(libpq.so.5.5 libpq.so.5 ${prefix_home}/lib)

