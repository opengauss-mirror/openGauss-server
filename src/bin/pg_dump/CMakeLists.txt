#This is the main CMAKE for build all components.
# gs_dump bin
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/parser/kwlookup.cpp ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
)

set(TGT_dump_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_dump.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_dump_sort.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_archiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_db.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_custom.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_null.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_tar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_directory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/compress_io.cpp
)

set(TGT_dumpall_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_dumpall.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
)

set(TGT_restore_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_restore.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/dumpmem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_archiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_db.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_custom.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_null.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_tar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_backup_directory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/compress_io.cpp
)
set(TGT_dump_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_SRC_DIR}/gausskernel/cbb/utils/aes
    ${LIBOPENSSL_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${BOOST_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(dump_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ -DHAVE_CE)
else()
    set(dump_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LIBZ)
endif()
set(dump_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dump_LINK_OPTIONS ${BIN_LINK_OPTIONS})
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(dump_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -lpq_ce -lcurl -lcjson)
else()
    set(dump_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz -lpq -lcurl -lcjson)
endif()
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND dump_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        set(dump_LINK_LIBS ${dump_LINK_LIBS} -lgs_ktool -lkmc)
    endif()
endif()
set(dump_LINK_LIBS utils_aes_aes ${dump_LINK_LIBS})
set(dump_LINK_DIRS  ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH}
    ${SECURE_LIB_PATH} ${KMC_LIB_PATH} ${CJSON_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(dump_LINK_DEPEND utils_aes_aes elog_static pgport_static pq_ce)
else()
    set(dump_LINK_DEPEND utils_aes_aes elog_static pgport_static pq)
endif()
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        list(APPEND dump_LINK_DEPEND gs_ktool)
    endif()
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(utgsdump TGT_dump_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}") 
    add_dependencies(utgsdump ${dump_LINK_DEPEND})
    target_link_libraries(utgsdump PRIVATE ${dump_LINK_LIBS}) 
    target_link_directories(utgsdump PUBLIC ${dump_LINK_DIRS})
    install(TARGETS utgsdump LIBRARY DESTINATION lib)
else()
    add_bintarget(gs_dump TGT_dump_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}" "${dump_LINK_LIBS}")
    add_dependencies(gs_dump ${dump_LINK_DEPEND})
    target_link_directories(gs_dump PUBLIC ${dump_LINK_DIRS})
    install(TARGETS gs_dump RUNTIME DESTINATION bin)
endif()

set(dumpall_LINK_LIBS utils_aes_aes ${dump_LINK_LIBS})
set(dumpall_LINK_DIRS ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH}
    ${SECURE_LIB_PATH} ${KMC_LIB_PATH} ${CJSON_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(dumpall_LINK_DEPEND utils_aes_aes elog_static pgport_static pq_ce)
else()
    set(dumpall_LINK_DEPEND utils_aes_aes elog_static pgport_static pq)
endif()
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        list(APPEND dumpall_LINK_DEPEND gs_ktool)
    endif()
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(utgsdumpall TGT_dumpall_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}") 
    add_dependencies(utgsdumpall ${dumpall_LINK_DEPEND})
    target_link_libraries(utgsdumpall PRIVATE ${dumpall_LINK_LIBS}) 
    target_link_directories(utgsdumpall PUBLIC ${dumpall_LINK_DIRS})
    install(TARGETS utgsdumpall LIBRARY DESTINATION lib)
else()
    add_bintarget(gs_dumpall TGT_dumpall_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}" "${dumpall_LINK_LIBS}")
    add_dependencies(gs_dumpall ${dumpall_LINK_DEPEND})
    target_link_directories(gs_dumpall PUBLIC ${dumpall_LINK_DIRS})
    install(TARGETS gs_dumpall RUNTIME DESTINATION bin)
endif()

set(restore_LINK_LIBS utils_aes_aes ${dump_LINK_LIBS})
set(restore_LINK_DIRS ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH}
    ${SECURE_LIB_PATH} ${KMC_LIB_PATH} ${CJSON_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(restore_LINK_DEPEND utils_aes_aes elog_static pgport_static pq_ce)
else()
    set(restore_LINK_DEPEND utils_aes_aes elog_static pgport_static pq)
endif()
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        list(APPEND restore_LINK_DEPEND gs_ktool)
    endif()
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(utgsrestore TGT_restore_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}") 
    add_dependencies(utgsrestore ${restore_LINK_DEPEND})
    target_link_libraries(utgsrestore PRIVATE ${restore_LINK_LIBS}) 
    target_link_directories(utgsrestore PUBLIC ${restore_LINK_DIRS})
    install(TARGETS utgsrestore LIBRARY DESTINATION lib)
else()
    add_bintarget(gs_restore TGT_restore_SRC TGT_dump_INC "${dump_DEF_OPTIONS}" "${dump_COMPILE_OPTIONS}" "${dump_LINK_OPTIONS}" "${restore_LINK_LIBS}")
    add_dependencies(gs_restore ${restore_LINK_DEPEND})
    target_link_directories(gs_restore PUBLIC ${restore_LINK_DIRS})
    install(TARGETS gs_restore RUNTIME DESTINATION bin)
endif()
