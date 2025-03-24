#This is the main CMAKE for build contrib.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/gsredistribute
    ${CMAKE_CURRENT_SOURCE_DIR}/hstore
    ${CMAKE_CURRENT_SOURCE_DIR}/test_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/mppdb_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/sql_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/spi
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_upgrade_support
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_output
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_inaddr
    ${CMAKE_CURRENT_SOURCE_DIR}/security_plugin
    ${CMAKE_CURRENT_SOURCE_DIR}/dummy_seclabel
    ${CMAKE_CURRENT_SOURCE_DIR}/pagehack
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_xlogdump
    ${CMAKE_CURRENT_SOURCE_DIR}/file_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/log_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/gc_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/ndpplugin
    ${CMAKE_CURRENT_SOURCE_DIR}/spq_plugin
    ${CMAKE_CURRENT_SOURCE_DIR}/datavec
    ${CMAKE_CURRENT_SOURCE_DIR}/chparser
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_stats
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_utility
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_profiler
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_lob
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_sql
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_tcp
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_i18n
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_debug
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_raw
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_match
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_assert
    ${CMAKE_CURRENT_SOURCE_DIR}/gms_compress
    ${CMAKE_CURRENT_SOURCE_DIR}/shark
    ${CMAKE_CURRENT_SOURCE_DIR}/age
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    INCLUDE_DIRECTORIES(${LIBODBC_INCLUDE_PATH})
    INCLUDE_DIRECTORIES(${LIBODBC_LIB_PATH})
       list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/dblink)
    add_subdirectory(dblink)
endif()

if("${USE_LIBXML}" STREQUAL "ON")
	INCLUDE_DIRECTORIES(${LIBXML_INCLUDE_PATH}/libxml2)
    INCLUDE_DIRECTORIES(${LIBXML_LIB_PATH})
	list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/gms_xmlgen)
    add_subdirectory(gms_xmlgen)
endif()

add_subdirectory(hstore)
add_subdirectory(test_decoding)
add_subdirectory(mppdb_decoding)
add_subdirectory(sql_decoding)
add_subdirectory(spi)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" OR "${ENABLE_PRIVATEGAUSS}" STREQUAL "ON")
    add_subdirectory(pg_upgrade_support)
endif()
add_subdirectory(postgres_fdw)
add_subdirectory(gms_output)
add_subdirectory(gms_inaddr)
add_subdirectory(security_plugin)
add_subdirectory(dummy_seclabel)
add_subdirectory(pagehack)
add_subdirectory(pg_xlogdump)
add_subdirectory(file_fdw)
add_subdirectory(log_fdw)
add_subdirectory(gms_utility)
add_subdirectory(gms_stats)
add_subdirectory(gms_sql)
add_subdirectory(gms_tcp)
add_subdirectory(gms_debug)
add_subdirectory(gms_raw)
add_subdirectory(gms_match)
add_subdirectory(gms_compress)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(gc_fdw)
endif()
add_subdirectory(ndpplugin)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/spq_plugin)
    add_subdirectory(spq_plugin)
endif()
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/datavec)
    add_subdirectory(datavec)
endif()
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/chparser)
    add_subdirectory(chparser)
endif()
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/age)
    add_subdirectory(age)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/jieba_tokenizer)
    add_subdirectory(jieba_tokenizer)
endif()
add_subdirectory(gms_profiler)
add_subdirectory(gms_lob)
add_subdirectory(gms_i18n)
add_subdirectory(gms_assert)
add_subdirectory(shark)
