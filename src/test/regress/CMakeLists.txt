# regress.so
set(TGT_regress_SRC ${CMAKE_CURRENT_SOURCE_DIR}/regress.cpp)

set(regress_DEF_OPTIONS ${MACRO_OPTIONS})
set(regress_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(regress_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(regress TGT_regress_SRC "" "${regress_DEF_OPTIONS}" "${regress_COMPILE_OPTIONS}" "${regress_LINK_OPTIONS}")
set_target_properties(regress PROPERTIES PREFIX "")

# pg_regress
set(TGT_pg_regress_single_SRC ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.cpp)
set(TGT_pg_regress_single_main_SRC ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress_main.cpp)
set(TGT_pg_regress_single_INC ${CMAKE_BINARY_DIR} ${SECURE_INCLUDE_PATH})
set(pg_regress_single_DEF_OPTIONS ${MACRO_OPTIONS} -DHOST_TUPLE="${HOST_TUPLE}" -DMAKEPROG="make" -DSHELLPROG="/bin/sh" -DDLSUFFIX=".so")
set(pg_regress_single_main_DEF_OPTIONS ${MACRO_OPTIONS})
set(pg_regress_single_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(pg_regress_single_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(pg_regress_single_LINK_LIBS pg_regress_single_obj -lpgport -lcrypt -ldl -lm  -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz)
add_static_objtarget(pg_regress_single_obj TGT_pg_regress_single_SRC TGT_pg_regress_single_INC "${pg_regress_single_DEF_OPTIONS}" "${pg_regress_single_COMPILE_OPTIONS}" "${pg_regress_single_LINK_OPTIONS}")
add_bintarget(pg_regress_single TGT_pg_regress_single_main_SRC TGT_pg_regress_single_INC "${pg_regress_single_main_DEF_OPTIONS}" "${pg_regress_single_COMPILE_OPTIONS}" "${pg_regress_single_LINK_OPTIONS}" "${pg_regress_single_LINK_LIBS}")
add_dependencies(pg_regress_single pg_regress_single_obj pgport_static)
target_link_directories(pg_regress_single PRIVATE ${LIBEDIT_LIB_PATH} ${ZLIB_LIB_PATH} ${CMAKE_BINARY_DIR}/lib ${LIBOPENSSL_LIB_PATH} ${SECURE_LIB_PATH})

add_subdirectory(stub/roach_api_stub)
add_subdirectory(jdbc_client)


#fastcheck
function(add_func_target_withargs_fastcheck _name _cmdargs)
    separate_arguments(cmd_args UNIX_COMMAND "${_cmdargs}")
    add_custom_target(${_name} ${CMAKE_CURRENT_SOURCE_DIR}/single_check.sh --cmd-withargs ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR} ${3RD_PATH} ${CMAKE_INSTALL_PREFIX} ${cmd_args} ${ENABLE_MEMORY_CHECK} ${CMAKE_MAKE_PROGRAM} ${SUPPORT_HOTPATCH} ${openGauss} c=\$\(c\) d=\$\(d\) n=\$\(n\) p=\$\(p\) s=\$\(s\) runtest=\$\(runtest\) part=\$\(part\) MAXCONNOPT=\$\(MAXCONNOPT\) keep_last_data=\$\(keep_last_data\) level=\$\(level\) hdfshostname=\$\(hdfshostname\) OUTPUT_FILE ${CMAKE_BINARY_DIR}/${_name}.log
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
endfunction(add_func_target_withargs_fastcheck)

MESSAGE(STATUS ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR} ${CMAKE_INSTALL_PREFIX})

add_func_target_withargs_fastcheck(fastcheck_single fastcheck_single)
add_func_target_withargs_fastcheck(fastcheck_single_audit fastcheck_single_audit)
add_func_target_withargs_fastcheck(fastcheck_lite fastcheck_lite)
add_func_target_withargs_fastcheck(fastcheck_single_mot fastcheck_single_mot)
add_func_target_withargs_fastcheck(fastcheck_ce_single fastcheck_ce_single)
add_func_target_withargs_fastcheck(execute_fastcheck_ce_single_jdbc execute_fastcheck_ce_single_jdbc)
add_func_target_withargs_fastcheck(redischeck_single redischeck_single)
add_func_target_withargs_fastcheck(wlmcheck_single wlmcheck_single)
add_func_target_withargs_fastcheck(2pccheck_single 2pccheck_single)
add_func_target_withargs_fastcheck(hacheck_single_all hacheck_single_all)
add_func_target_withargs_fastcheck(hacheck_single hacheck_single)
add_func_target_withargs_fastcheck(hacheck_decode hacheck_decode)
add_func_target_withargs_fastcheck(hacheck_multi_single hacheck_multi_single)
add_func_target_withargs_fastcheck(hacheck_multi_single_mot hacheck_multi_single_mot)
add_func_target_withargs_fastcheck(decodecheck_single decodecheck_single)
add_func_target_withargs_fastcheck(upgradecheck_single upgradecheck_single)

