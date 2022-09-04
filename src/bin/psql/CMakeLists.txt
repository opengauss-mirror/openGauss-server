#This is the main CMAKE for build all components.
# gsql bin
#add_custom_command(
#    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/psqlscan.inc ${CMAKE_CURRENT_SOURCE_DIR}/mainloop.cpp
#    COMMAND flex -Cfe -b -p -o'${CMAKE_CURRENT_SOURCE_DIR}/psqlscan.inc' ${CMAKE_CURRENT_SOURCE_DIR}/psqlscan.l
#    COMMAND rm -fr ${CMAKE_CURRENT_SOURCE_DIR}/lex.backup
#    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/psqlscan.l
#    COMMENT "Now Generating psqlscan.inc"
#)

set(psql_cmd_src
"${PROJECT_SRC_DIR}/bin/psql|||flex -Cfe -b -p -p -o'psqlscan.inc' psqlscan.l|rm -fr lex.backup"
)

add_cmd_gen_when_configure(flex_target psql_cmd_src)

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_dump/dumputils.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/pg_dump/keywords.cpp ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/parser/kwlookup.cpp ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/sql_help.cpp ${CMAKE_CURRENT_SOURCE_DIR}/sql_help.h
    COMMAND perl create_help.pl ${PROJECT_OPENGS_DIR}/doc/src/sgml/ref sql_help
    DEPENDS create_help.pl makesgml
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Now Generating sql_help.h and sql_help.cpp"
)

set(TGT_gsql_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/command.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/help.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/input.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stringutils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mainloop.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/copy.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/startup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/prompt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/variables.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/large_obj.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/print.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/describe.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mbprint.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dumputils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kwlookup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tab-complete.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sql_help.cpp
)

set(TGT_gsql_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/pg_dump 
    ${PROJECT_TRUNK_DIR}/distribute/bin/gs_ktool 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${LIBEDIT_INCLUDE_PATH}
)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(gsql_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_CE)
else()
    set(gsql_DEF_OPTIONS ${MACRO_OPTIONS})
endif()
set(gsql_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gsql_LINK_OPTIONS ${BIN_LINK_OPTIONS})
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(gsql_LINK_LIBS utils_aes_aes libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz -ledit -lcjson -lcurl -lpq_ce -lncurses)
else()
    set(gsql_LINK_LIBS utils_aes_aes libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz -ledit -lcjson -lcurl -lpq -lncurses)
endif()
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND gsql_LINK_LIBS -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss)
endif()
set(gsql_LINK_DIRS ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${KMC_LIB_PATH} ${CJSON_LIB_PATH} ${LIBCURL_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        set(gsql_LINK_LIBS ${gsql_LINK_LIBS} -lgs_ktool -lkmc)
    endif()
endif()
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
set(gsql_LINK_DEPEND utils_aes_aes elog_static pq_ce pgport_static makesgml)
else()
set(gsql_LINK_DEPEND utils_aes_aes elog_static pq pgport_static makesgml)
endif()
if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(sqla TGT_gsql_SRC TGT_gsql_INC "${gsql_DEF_OPTIONS}" "${gsql_COMPILE_OPTIONS}" "${gsql_LINK_OPTIONS}")
    add_dependencies(sqla ${gsql_LINK_DEPEND})
    target_link_libraries(sqla PRIVATE ${gsql_LINK_LIBS})
    target_link_directories(sqla PUBLIC ${gsql_LINK_DIRS})
    install(TARGETS sqla LIBRARY DESTINATION lib)
else()
    add_bintarget(gsql TGT_gsql_SRC TGT_gsql_INC "${gsql_DEF_OPTIONS}" "${gsql_COMPILE_OPTIONS}" "${gsql_LINK_OPTIONS}" "${gsql_LINK_LIBS}")
    add_dependencies(gsql ${gsql_LINK_DEPEND})
    target_link_directories(gsql PUBLIC ${gsql_LINK_DIRS})

    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/psqlrc.sample
        DESTINATION share/postgresql/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/retry_errcodes.conf
        DESTINATION bin/)
    install(TARGETS gsql RUNTIME DESTINATION bin)
endif()
