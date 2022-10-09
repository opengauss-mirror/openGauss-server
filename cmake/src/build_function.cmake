
function(install_lib_run target_name _todir)
    install(TARGETS ${target_name} 
        LIBRARY DESTINATION ${_todir}
        PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE OWNER_READ GROUP_READ)
endfunction(install_lib_run)

function(install_bin target_name)
    install(TARGETS ${target_name}
        RUNTIME DESTINATION bin
        PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE OWNER_READ GROUP_READ)
endfunction(install_bin)

function(install_timezone)
    set(TZDATA "@africa @antarctica @asia @australasia @europe @northamerica @southamerica @pacificnew @etcetera @factory @backward @systemv @solar87 @solar88 @solar89")
    string(REPLACE "@" "${PROJECT_SRC_DIR}/timezone/data/" TZDATA_FILES ${TZDATA})
    if("${with_system_tzdata}" STREQUAL "")
        separate_arguments(ZIC_COMMAND UNIX_COMMAND "COMMAND ./zic -d ${PROJECT_SRC_DIR}/share/postgresql/timezone -p 'US/Eastern' ${TZDATA_FILES}")
    else()
        separate_arguments(ZIC_COMMAND UNIX_COMMAND "COMMAND zic -d ${PROJECT_SRC_DIR}/share/postgresql/timezone -p 'US/Eastern' ${TZDATA_FILES}")
    endif()
    execute_process(${ZIC_COMMAND})
endfunction(install_timezone)

#The chaotic use of a compiler is a dangerous behavior, but GaussAP's Roach temporarily does not allow modification of the suffix name. 
#CMake cannot simply change the language compiler. I set a switch to change the source language attribute.
MACRO(check_c_to_cxx _srcs)
    set(src_ext "")
    if(${BUILD_C_WITH_CXX})
        foreach(af ${${_srcs}})
            get_source_file_property(src_ext ${af} LANGUAGE)
            if("${src_ext}" STREQUAL "C")
                set_source_files_properties(${af} PROPERTIES LANGUAGE "CXX")
            endif()
        endforeach()
    endif()
ENDMACRO()

function(redefine_file_macro targetname)
    get_target_property(source_files "${targetname}" SOURCES)
    foreach(sourcefile ${source_files})
        get_property(defs SOURCE "${sourcefile}"
            PROPERTY COMPILE_DEFINITIONS)
        get_filename_component(file_name "${sourcefile}" NAME)
        list(APPEND defs "__FILE__=\"${file_name}\"")
        set_property(
            SOURCE "${sourcefile}"
            PROPERTY COMPILE_DEFINITIONS ${defs}
            )
    endforeach()
endfunction()

function(add_bintarget target_name target_src_list target_inc_list target_define_list target_compile_options_list target_link_options_list target_link_list)
    add_executable(${target_name} ${${target_src_list}})
    redefine_file_macro(${target_name})
    target_include_directories(${target_name} PUBLIC ${${target_inc_list}} ${PROJECT_INCLUDE_DIR})
    if(target_link_list)
        target_link_libraries(${target_name} PRIVATE ${target_link_list})
    endif()
    if(target_define_list)
        target_compile_definitions(${target_name} PRIVATE ${target_define_list})
    endif()
    if(target_compile_options_list)
        target_compile_options(${target_name} PRIVATE ${target_compile_options_list})
    endif()
    if(target_link_options_list)
        target_link_options(${target_name} PRIVATE ${target_link_options_list})
    endif()
    if(${ENABLE_MEMORY_CHECK})
        target_compile_options(${target_name} PRIVATE ${MEMCHECK_FLAGS})
        target_link_options(${target_name} PRIVATE ${MEMCHECK_FLAGS})
        target_link_libraries(${target_name} PRIVATE ${MEMCHECK_LIBS})
        target_link_directories(${target_name} PRIVATE ${MEMCHECK_LINK_DIRECTORIES})
    endif()
    if(NOT "${TEST_LINK_OPTIONS}" STREQUAL "")
        target_link_libraries(${target_name} PRIVATE ${TEST_LINK_OPTIONS})
    endif()
endfunction(add_bintarget)

function(add_static_libtarget target_name target_src_list target_inc_list target_define_list target_compile_options_list)
    add_library(${target_name}_static STATIC ${${target_src_list}})
    if(NOT "${target_name}" STREQUAL "pgport_srv")
    if(${ENABLE_MEMORY_CHECK})
        target_compile_options(${target_name}_static PRIVATE ${MEMCHECK_FLAGS})
    endif()
    endif()
    redefine_file_macro(${target_name}_static)
    set_target_properties(${target_name}_static PROPERTIES OUTPUT_NAME ${target_name})
    if(target_inc_list)
        target_include_directories(${target_name}_static PUBLIC ${${target_inc_list}} ${PROJECT_INCLUDE_DIR})
    endif()
    if(target_define_list)
        target_compile_definitions(${target_name}_static PRIVATE ${target_define_list})
    endif()
    if(target_compile_options_list)
        target_compile_options(${target_name}_static PRIVATE ${target_compile_options_list})
    endif()
endfunction(add_static_libtarget)

function(add_static_objtarget target_name target_src_list target_inc_list target_define_list target_compile_list target_link_list)
    add_library(${target_name} OBJECT ${${target_src_list}})
    redefine_file_macro(${target_name})
    if(${ENABLE_MEMORY_CHECK})
        target_compile_options(${target_name} PRIVATE ${MEMCHECK_FLAGS})
    endif()
    if(target_inc_list)
        target_include_directories(${target_name} PUBLIC ${${target_inc_list}} ${PROJECT_INCLUDE_DIR})
    endif()
    if(target_define_list)
        target_compile_definitions(${target_name} PRIVATE ${target_define_list})
    endif()
    if(target_compile_list)
        target_compile_options(${target_name} PRIVATE ${target_compile_list})
    endif()
    if(target_link_list)
        target_link_options(${target_name} PRIVATE ${target_link_list})
    endif()
endfunction(add_static_objtarget)

function(add_shared_libtarget target_name target_src_list target_inc_list target_define_list target_compile_options_list target_link_options_list)
    add_library(${target_name} SHARED ${${target_src_list}})
    redefine_file_macro(${target_name})
    target_include_directories(${target_name} PUBLIC ${${target_inc_list}} ${PROJECT_INCLUDE_DIR})
    if(target_define_list)
        target_compile_definitions(${target_name} PRIVATE ${target_define_list})
    endif()
    if(target_compile_options_list)
        target_compile_options(${target_name} PRIVATE ${target_compile_options_list})
    endif()
    if(target_link_options_list)
        target_link_options(${target_name} PRIVATE ${target_link_options_list})
    endif()
    if(${ENABLE_MEMORY_CHECK})
        target_compile_options(${target_name} PRIVATE ${MEMCHECK_FLAGS})
    endif()
    if(NOT "${TEST_LINK_OPTIONS}" STREQUAL "")
        target_link_libraries(${target_name} PRIVATE ${TEST_LINK_OPTIONS})
    endif()
    set_target_properties(${target_name} PROPERTIES DEFINE_SYMBOL "")
endfunction(add_shared_libtarget)

function(add_cmd_gen_when_configure _target_name src_list)
    list(LENGTH ${src_list} src_length1)
    math(EXPR src_length "${src_length1} -1")
    set(targe_depend "")
    if(${src_length1})
    foreach(id RANGE 0 ${src_length} 1)
        list(GET ${src_list} ${id} var)
        string(REPLACE "|" ";" cmd_args_list "${var}")
        set(after_cmd "")
        set(dependces "")
        set(before_cmd "")
        set(main_cmd "")
        set(work_dir "")
        list(GET cmd_args_list 0 work_dir)
        list(GET cmd_args_list 1 before_cmd)
        list(GET cmd_args_list 2 thread_cmd)
        list(GET cmd_args_list 3 main_cmd)
        list(GET cmd_args_list 4 after_cmd)
        execute_process(
            COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --runscript ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR} "${before_cmd}" "${main_cmd}" "${thread_cmd}" "${after_cmd}"
            WORKING_DIRECTORY ${work_dir}
            OUTPUT_VARIABLE LAST_CMD_RST
        )
    endforeach()
    endif()
endfunction(add_cmd_gen_when_configure)

MACRO(SUBDIRLIST ret curdir)
    file(GLOB children RELATIVE ${curdir} ${curdir}/*)
    set(dirlist "")
    foreach(child ${children})
        if(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        endif()
    endforeach()
    set(${ret} ${dirlist})
endmacro(SUBDIRLIST)


MACRO(CHECK_STRUCT check_struct check_header ret)
   set(_INCLUDE_FILES)
   FOREACH (it ${check_header})
      set(_INCLUDE_FILES "${_INCLUDE_FILES}#include <${it}>\n")
   ENDFOREACH (it)
   set(_CHECK_STRUCT_CODE "${_INCLUDE_FILES}\nint main(){\n\t${check_struct}* tmp;\n\treturn 0;\n}")
   CHECK_C_SOURCE_COMPILES("${_CHECK_STRUCT_CODE}" ${ret})
ENDMACRO(CHECK_STRUCT)

MACRO(CHECK_CC_ENABLE check_cc_str check_header ret)
   set(_INCLUDE_FILES)
   FOREACH (it ${check_header})
      set(_INCLUDE_FILES "${_INCLUDE_FILES}#include <${it}>\n")
   ENDFOREACH (it)
   set(_CC_ENABLE_CODE "${_INCLUDE_FILES}\nint main(){\n${check_cc_str};\n\treturn 0;\n}")
   CHECK_C_SOURCE_COMPILES("${_CC_ENABLE_CODE}" ${ret})
ENDMACRO(CHECK_CC_ENABLE)

function(GET_VERSIONSTR_FROMGIT ret)
    set(PG_VERSION "9.2.4")
    set(OPENGAUSS_VERSION "3.0.2")
    execute_process(
        COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --s ${PROJECT_TRUNK_DIR} OUTPUT_VARIABLE GS_VERSION_STR)
    set(PG_VERSION "${PG_VERSION}" PARENT_SCOPE)
    set(${ret} "${GS_VERSION_STR}" PARENT_SCOPE)
    set(OPENGAUSS_VERSION_NUM_STR, "${OPENGAUSS_VERSION}" PARENT_SCOPE)
    if(NOT ${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF)
        set(PG_VERSION_STR "openGauss ${OPENGAUSS_VERSION} ${GS_VERSION_STR}")
    else()
        set(PG_VERSION_STR "${GS_VERSION_STR}")
    endif()
    set(PG_VERSION_STR "${PG_VERSION_STR}" PARENT_SCOPE)
endfunction(GET_VERSIONSTR_FROMGIT)

function(GET_TIMESTR_FOR_ROACH)
    set(COMPILETIME "30 Jan 2021 10:45:57")
    execute_process(
        COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --get_time_for_roach ${PROJECT_TRUNK_DIR} OUTPUT_VARIABLE COMPILETIME_STR)
    set(COMPILETIME "${COMPILETIME_STR}" PARENT_SCOPE)
endfunction()

function(CHECK_BASIC_TYPE)
    CHECK_TYPE_SIZE("double" ALIGNOF_DOUBLE)
    CHECK_TYPE_SIZE("int" ALIGNOF_INT)
    CHECK_TYPE_SIZE("short" ALIGNOF_SHORT)
    CHECK_TYPE_SIZE("long" ALIGNOF_LONG)
    CHECK_TYPE_SIZE("long" SIZEOF_LONG)
    CHECK_TYPE_SIZE("off_t" SIZEOF_OFF_T)
    CHECK_TYPE_SIZE("size_t" SIZEOF_SIZE_T)
    CHECK_TYPE_SIZE("int64" HAVE_INT64)
    CHECK_TYPE_SIZE("intptr_t" HAVE_INTPTR_T)
    CHECK_TYPE_SIZE("int8" HAVE_INT8)
    CHECK_TYPE_SIZE("void*" SIZEOF_VOID_P)
    CHECK_TYPE_SIZE("long long int" HAVE_LONG_LONG_INT)
    CHECK_TYPE_SIZE("long int" HAVE_LONG_INT)
    if("${HAVE_LONG_INT}" EQUAL "8")
        CHECK_TYPE_SIZE("long int" HAVE_LONG_INT_64)
    endif()
    if("${HAVE_LONG_INT_64}" EQUAL "")
        CHECK_TYPE_SIZE("long long int" LONG_LONG_INT_64)
        if(NOT "${LONG_LONG_INT_64}" EQUAL "")
            set(HAVE_LONG_LONG_INT_64 1 PARENT_SCOPE)
        endif()
    endif()
    if("${HAVE_LONG_LONG_INT_64}" EQUAL "")
        set(ALIGNOF_LONG_LONG_INT 0 PARENT_SCOPE)
    endif()
    CHECK_TYPE_SIZE("uint64" HAVE_UINT64)
    CHECK_TYPE_SIZE("uintptr_t" HAVE_UINTPTR_T)
    CHECK_TYPE_SIZE("uint8" HAVE_UINT8)
    set(MAXIMUM_ALIGNOF_tmp ${ALIGNOF_LONG})
    if(${MAXIMUM_ALIGNOF_tmp} STRLESS ${HAVE_LONG_LONG_INT})
        set(MAXIMUM_ALIGNOF_tmp ${HAVE_LONG_LONG_INT})
    endif()
    set(MAXIMUM_ALIGNOF ${MAXIMUM_ALIGNOF_tmp} PARENT_SCOPE)
endfunction(CHECK_BASIC_TYPE)

function(SET_GCC_FLAGS _flags _defs)
    if($ENV{CONFIG_CROSS})
        set(CMAKE_TOOLCHAIN_FILE "$ENV{TO_3RD}/cmake_tool_chain.cfg" PARENT_SCOPE)
    endif()

    string(REGEX MATCHALL "([^ ]+)" flag_list ${${_flags}})
    foreach(flag ${flag_list})
        add_compile_options(${flag})
    endforeach()
#   add_compile_definitions("${${_defs}}")
    link_directories($ENV{TO_3RD}/lib .)
endfunction(SET_GCC_FLAGS)

function(CHECK_CCFLAGS_ENABLE check_item)
    foreach(it ${check_item})
        string(REPLACE "|" ";" check_list "${it}")
        list(GET check_list 0 check_fetrue)
        list(GET check_list 1 check_rst)
        list(FIND CMAKE_CXX_COMPILE_FEATURES ${check_fetrue} tmp_${check_rst})
        if(${tmp_${check_rst}})
            SET(${check_rst} 1)
        endif()
    endforeach()
endfunction(CHECK_CCFLAGS_ENABLE)

function(CHECK_INCLUDE check_include_files out_includes)
    set(INC_FILES_tmp "")
    foreach(inc_files ${${check_include_files}})
        string(TOUPPER ${inc_files} mydefine_var1)
        string(REPLACE "." "_" mydefine_var2 "${mydefine_var1}")
        string(REPLACE "/" "_" mydefine_out "${mydefine_var2}")
        check_include_file(${inc_files} HAVE_${mydefine_out})
        if(${HAVE_${mydefine_out}})
            list(APPEND INC_FILES_tmp ${inc_files})
        endif()
    endforeach()
    set(${out_includes} ${INC_FILES_tmp} PARENT_SCOPE)
endfunction(CHECK_INCLUDE)

function(CHECK_FUNCTIONS check_functions out_includes)
    foreach(f ${${check_functions}})
        string(TOUPPER ${f} mydefine_out)
        check_symbol_exists(${f} "${${out_includes}}" HAVE_${mydefine_out})
    endforeach()
endfunction(CHECK_FUNCTIONS)

function(CHECK_DECLARATIONS check_decl out_includes)
    foreach(decl ${${check_decl}})
        string(TOUPPER ${decl} mydefine_out)
        check_symbol_exists(${decl} "${${out_includes}}" HAVE_DECL_${mydefine_out})
        if(HAVE_DECL_${mydefine_out})
            set(HAVE_DECL_${mydefine_out}_RST 1 CACHE INTERNAL "")
        else()
            set(HAVE_DECL_${mydefine_out}_RST 0 CACHE INTERNAL "")
        endif()
    endforeach()
endfunction(CHECK_DECLARATIONS)

function(CHECK_VARIABLES check_variable out_includes)
    foreach(var ${${check_variable}})
        string(REPLACE "," ";" check_var_list "${var}")
        list(LENGTH check_var_list check_list_length)
        list(GET check_var_list 0 check_var)
        string(TOUPPER ${check_var} mydefine_out)
        if("${check_list_length}" GREATER "1")
            list(GET check_var_list 1 check_type_1)
            string(TOUPPER ${check_type_1} check_type_2)
            string(REPLACE " " "_" mydefine_type "${check_type_2}")
            check_symbol_exists(${check_var} "${${out_includes}}" HAVE_${mydefine_type}_${mydefine_out})
        else()
            check_symbol_exists(${check_var} "${${out_includes}}" HAVE_${mydefine_out})
        endif()
    endforeach()
endfunction(CHECK_VARIABLES)

function(CHECK_STRUCT_OR_MEMBER check_struct_has_member out_includes)
    foreach(s ${${check_struct_has_member}})
        string(REPLACE "," ";" check_struct_list "${s}")
        list(GET check_struct_list 0 check_type)
        string(TOUPPER ${check_type} check_type_2)
        string(REPLACE " " "_" mydefine_type "${check_type_2}")
        CHECK_STRUCT("${check_type}" "${${out_includes}}" HAVE_${mydefine_type})
        list(LENGTH check_struct_list check_list_length1)
        math(EXPR check_list_length "${check_list_length1} -1")
        if(${check_list_length})
            foreach(id RANGE 1 ${check_list_length} 1)
                list(GET check_struct_list ${id} check_member)
                string(TOUPPER ${check_member} check_member_U)
                check_struct_has_member("${check_type}" "${check_member}" "${${out_includes}}" HAVE_${mydefine_type}_${check_member_U})
            endforeach()
        endif()
    endforeach()
endfunction(CHECK_STRUCT_OR_MEMBER)

function(CHECK_FUNC_PROTOTYPE check_function_prototype out_includes)
    foreach(t ${${check_function_prototype}})
        string(REPLACE "|" ";" check_list "${t}")
        list(GET check_list 0 check_func)
        list(GET check_list 1 check_proto)
        list(GET check_list 2 check_ret)
        check_prototype_definition("${check_func}" "${check_proto}" "${check_ret}" "${${out_includes}}" IS_RIGHT_${check_func})
        if(${IS_RIGHT_${check_func}})
            list(LENGTH check_list check_list_length1)
            math(EXPR check_list_length "${check_list_length1} -1")
            foreach(id RANGE 3 ${check_list_length} 2)
                math(EXPR nextid "${id}+1")
                list(GET check_list ${id} OUT_PUT_VAR)
                list(GET check_list ${nextid} out_put_value)
                set(${OUT_PUT_VAR} ${out_put_value} CACHE INTERNAL "")
            endforeach()
        endif()
    endforeach()
endfunction(CHECK_FUNC_PROTOTYPE)

SET(HAVE_STRINGIZE_SRC "\t#define x(y) #y\n\tchar *s = x(teststring)")

function(CHECK_CCSRC check_ccstr out_includes)
    foreach(ccinfo ${${check_ccstr}})
        string(REPLACE "|" ";" cc_list "${ccinfo}")
        list(GET cc_list 0 cc_str_var)
        list(GET cc_list 1 cc_include)
        list(GET cc_list 2 cc_ret)
        CHECK_CC_ENABLE(${${cc_str_var}} "${${out_includes}}" ${cc_ret})
    endforeach()
endfunction(CHECK_CCSRC)

function(check_headers_func_c check_include_files check_functions check_decl check_variable check_struct_has_member check_function_prototype check_ccstr)
    set(include_file_list "")
    list(APPEND CMAKE_REQUIRED_INCLUDES "$ENV{TO_3RD}/include")
    CHECK_INCLUDE(${check_include_files} include_file_list)
    CHECK_FUNCTIONS(${check_functions} include_file_list)
    CHECK_DECLARATIONS(${check_decl} include_file_list)
    CHECK_VARIABLES(${check_variable} include_file_list)
    CHECK_STRUCT_OR_MEMBER(${check_struct_has_member} include_file_list)
    CHECK_FUNC_PROTOTYPE(${check_function_prototype} include_file_list)
    CHECK_CCSRC(${check_ccstr} include_file_list)
    list(REMOVE_ITEM CMAKE_REQUIRED_INCLUDES "$ENV{TO_3RD}/include")
endfunction(check_headers_func_c)

function(CHECK_FOR_MPPDB)
    set(CHECK_INC_FILES crtdefs.h crypt.h dld.h editline/history.h editline/readline.h fp_class.h getopt.h gssapi/gssapi.h gssapi.h history.h ieeefp.h ifaddrs.h inttypes.h langinfo.h ldap.h memory.h netinet/in.h netinet/tcp.h net/if.h ossp/uuid.h pam/pam_appl.h poll.h pwd.h readline.h readline/history.h readline/readline.h security/pam_appl.h stdint.h stdlib.h strings.h string.h sys/ioctl.h sys/ipc.h sys/poll.h sys/pstat.h sys/resource.h sys/select.h sys/sem.h sys/shm.h sys/socket.h sys/sockio.h sys/stat.h sys/tas.h sys/time.h sys/wait.h sys/types.h sys/ucred.h sys/un.h termios.h ucred.h unistd.h utime.h uuid.h wchar.h wctype.h winldap.h stdio.h stddef.h stdarg.h ac_nonexistent.h float.h zlib.h krb5.h libxml/parser.h libxslt/xslt.h windows.h dns_sd.h sys/param.h limits.h netdb.h locale.h xlocale.h fcntl.h machine/vmparam.h sys/exec.h math.h dbghelp.h setjmp.h signal.h syslog.h pthread.h cpuid.h intrin.h nmmintrin.h libintl.h tcl.h Python.h dlfcn.h errno.h)
    set(STDC_HEADERS 1 PARENT_SCOPE)

    set(CHECK_FUNCTIONS append_history cbrt class crypt dlopen fdatasync fls fpclass fp_class fp_class_d getaddrinfo gethostbyname_r getifaddrs getopt getopt_long getpeereid getpeerucred getpwuid_r getrlimit getrusage gettimeofday history_truncate_file inet_aton mbstowcs_l memmove poll posix_fadvise pstat random readlink rint rl_completion_matches rl_filename_completion_function setproctitle setsid sigprocmask snprintf srandom strerror strerror_r strlcat strlcpy strtoll strtoq strtoull strtouq symlink sync_file_range towlower unsetenv utime utimes vsnprintf waitpid wcstombs wcstombs_l fseeko rl_completion_append_character isinf sigsetjmp syslog)
    #the below set to ture by pg default.
    set(HAVE_RINT 1 PARENT_SCOPE)
    set(HAVE_CRYPT 1 PARENT_SCOPE)
    set(HAVE_DLOPEN 1 PARENT_SCOPE)
    set(HAVE_INET_ATON 1 PARENT_SCOPE)
    set(HAVE_CBRT 1 PARENT_SCOPE)
    set(HAVE_SYNC_FILE_RANGE 1 PARENT_SCOPE)

    set(CHECK_DECLARATION fdatasync F_FULLFSYNC posix_fadvise snprintf strlcat strlcpy sys_siglist vsnprintf)
    set(CHECK_VARIABLE "opterr,int" "optreset,int" "timezone,int")
    set(CHECK_STRUCT_OR_MEMBER "locale_t" "MINIDUMP_TYPE" "sig_atomic_t" "union semun" "struct addrinfo" "struct cmsgcred" "struct option" "struct sockaddr,sa_len" "struct sockaddr_storage,ss_family,ss_len,__ss_family,__ss_len" "struct tm,tm_zone" "struct sockaddr_in6" "struct sockaddr_un")

    # I can check this , but must set to zero, why? pg has some mistake ?
    set(HAVE_STRUCT_CMSGCRED 0 PARENT_SCOPE)
    set(HAVE_UNION_SEMUN 0 PARENT_SCOPE)

    set(CHECK_PROTOTYPE_DEF
        "getpwuid_r|int getpwuid_r(uid_t uid, struct passwd *pwd, char *buf, size_t buflen, struct passwd **result)|0|GETPWUID_R_5ARG|1"
        "gettimeofday|int gettimeofday(struct timeval *tv)|0|GETTIMEOFDAY_1ARG|1"
        "accept|int accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen)|0|ACCEPT_TYPE_ARG1|int|ACCEPT_TYPE_ARG2|struct sockaddr *|ACCEPT_TYPE_ARG3|socklen_t|ACCEPT_TYPE_RETURN|int"
        "strerror_r|int strerror_r(int errnum, char *buf, size_t buflen)|0|STRERROR_R_INT|1")
    #CHECK_CCFLAGS_ENABLE(${CHECK_COMPILER_FLAGS})

    set(HAVE_FUNCNAME__FUNC 1 PARENT_SCOPE)
    set(CHECK_CC_STR  "HAVE_STRINGIZE_SRC||HAVE_STRINGIZE")

    check_headers_func_c(CHECK_INC_FILES CHECK_FUNCTIONS CHECK_DECLARATION CHECK_VARIABLE CHECK_STRUCT_OR_MEMBER CHECK_PROTOTYPE_DEF CHECK_CC_STR)

    if(${HAVE_STRUCT_SOCKADDR_IN6})
        set(HAVE_IPV6 1 PARENT_SCOPE)
    endif()
    if(${HAVE_STRUCT_SOCKADDR_UN})
        set(HAVE_UNIX_SOCKETS 1 PARENT_SCOPE)
    endif()
endfunction(CHECK_FOR_MPPDB)

function(list_to_string input_list output_str add_str)
    foreach(s ${input_list})
        if("${${output_str}}" STREQUAL "")
            set(${output_str} "${add_str}${s}")
        else()
            set(${output_str} "${add_str}${s} ${${output_str}}")
        endif()
    endforeach()
    set(${output_str} ${${output_str}} PARENT_SCOPE)
endfunction(list_to_string)

function(build_mppdb_config_paths_h OUT_VAR)
SET(${OUT_VAR} "
#define PGBINDIR        \"${CMAKE_INSTALL_PREFIX}/bin\"
#define PGSHAREDIR      \"${CMAKE_INSTALL_PREFIX}/share/postgresql\"
#define SYSCONFDIR      \"${CMAKE_INSTALL_PREFIX}/etc/postgresql\"
#define INCLUDEDIR      \"${CMAKE_INSTALL_PREFIX}/include\"
#define PKGINCLUDEDIR   \"${CMAKE_INSTALL_PREFIX}/include/postgresql\"
#define INCLUDEDIRSERVER \"${CMAKE_INSTALL_PREFIX}/include/postgresql/server\"
#define LIBDIR          \"${CMAKE_INSTALL_PREFIX}/lib\"
#define PKGLIBDIR       \"${CMAKE_INSTALL_PREFIX}/lib/postgresql\"
#define LOCALEDIR       \"${CMAKE_INSTALL_PREFIX}/share/locale\"
#define DOCDIR          \"${CMAKE_INSTALL_PREFIX}/share/doc/postgresql\"
#define HTMLDIR         \"${CMAKE_INSTALL_PREFIX}/share/doc/postgresql\"
#define MANDIR          \"${CMAKE_INSTALL_PREFIX}/share/man\"
")
file(WRITE ${CMAKE_BINARY_DIR}/pg_config_paths.h "${${OUT_VAR}}")
endfunction(build_mppdb_config_paths_h)

#such as libldap-2.4.so.2.10.11, cann't be find. so I rewrite find-libs function
MACRO(FIND_PROJECT_LINK_LIBS _type)
    execute_process(COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --find-libs $ENV{TO_3RD}/lib/ "${alib}.${_type}" ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR} $ENV{DESTDIR}${CMAKE_INSTALL_PREFIX} ERROR_VARIABLE _errno RESULT_VARIABLE _errno_ret OUTPUT_VARIABLE _file_names)
    if("${_file_names}" STREQUAL "")
        execute_process(COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --find-libs ${FOUND_GCC_LIB_PATH} "${alib}.${_type}" ${PROJECT_TRUNK_DIR}  ${CMAKE_BINARY_DIR} $ENV{DESTDIR}${CMAKE_INSTALL_PREFIX} ERROR_VARIABLE _errno RESULT_VARIABLE _errno_ret OUTPUT_VARIABLE _file_names)
    endif()
ENDMACRO(FIND_PROJECT_LINK_LIBS)

MACRO(install_file srcdir dstdir)
  FILE(GLOB files LIST_DIRECTORIES false ${srcdir}/*.h)
  FOREACH(file ${files})
      INSTALL(FILES ${file} DESTINATION ${dstdir})
  ENDFOREACH()
ENDMACRO()

# create symbolic link
MACRO(install_symlink filepath sympath workdir)
    install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${filepath} ${sympath} 
            WORKING_DIRECTORY ${workdir})")
    install(CODE "message(\"-- Created symlink: ${sympath} -> ${filepath}\")")
ENDMACRO(install_symlink)

