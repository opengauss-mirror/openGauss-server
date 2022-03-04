include(CheckIncludeFile)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckFunctionExists)
include(CheckStructHasMember)
include(CheckCCompilerFlag)
include(CheckTypeSize)
include(CheckVariableExists)
include(CheckPrototypeDefinition)
include(build_function)

CHECK_BASIC_TYPE()

#this is the default port, the old is --with-pgport=5432, now -DPGPORT=5432
option(PGPORT 5432)
if(NOT ${PGPORT})
    set(PGPORT 5432)
endif()

GET_VERSIONSTR_FROMGIT(GET_PG_VERSION_STR)

#CMake does not allow g++ to compile C files. They are two different languages. You should understand what you are doing.
#Now because of ROACH, I support it, but It's Very Bad Thing! We should know the difference of C/C++ language, whether compiling or coding, sometime's it's not work.
SET(BUILD_C_WITH_CXX yes CACHE INTERNAL "Build C with g++")


#The current OS kernel and glibc library are compiled using low-version gcc. So we usually need to turn on this ability. 
#In the future, cross-compilation and ultimately operation independence will be implemented, which also be controlled by this options.
option(TO_PUBLISH_BASICLIBS "Turn on this option on non gcc-8.3 compiled OS kernel, maybe aways." ON)


#Set to check SSE aways, later I will change it to lib, real hot load it. x86_64 atuo USE_SSE, aarch64 without it.
option(USE_SSE42_CRC32C "close it aways, check runtimes" OFF)

#Do not change this.
option(FLEXIBLE_ARRAY_MEMBER "aways turn on" ON)
option(FLOAT4PASSBYVAL  "aways set to true" ON)
option(FLOAT8PASSBYVAL "aways set to true" ON)
option(USE_FLOAT4_BYVAL "use float4 by val, the old is --enable-float4-byval" ON)
option(USE_FLOAT8_BYVAL "use float8 by val, the old is --enable-float8-byval" ON)

option(ENABLE_GSS "enable gssapi, the old is --with-gssapi" ON)
option(ENABLE_NLS "enable nls, the old is --enable-nls" OFF)
option(HAVE_SPINLOCKS "enable_spinlocks, the old is enable_spinlocks" ON)
option(ENABLE_MEMORY_CHECK "enable memory check, the old is --enable-memory-check" OFF)
option(ENABLE_THREAD_CHECK "enable thread check, the old is --enable-thread-check" OFF)
option(ENABLE_LCOV "enable lcov, the old is --enable-lcov" OFF)

# new add
option(ENABLE_MULTIPLE_NODES "enable distribute,the old is --enable-multiple-nodes" OFF)
option(ENABLE_PRIVATEGAUSS "enable privategauss,the old is --enable-pribategauss" OFF)
option(ENABLE_LITE_MODE "enable lite in single_node mode,the old is --enable-lite-mode" OFF)
option(ENABLE_DEBUG "enable privategauss,the old is --enable-pribategauss" OFF)
option(ENABLE_MOT "enable mot in single_node mode,the old is --enable-mot" OFF)
option(ENABLE_MYSQL_FDW "enable export or import data with mysql,the old is --enable-mysql-fdw" OFF)
option(ENABLE_ORACLE_FDW "enable export or import data with oracle,the old is --enable-oracle-fdw" OFF)
option(BUILD_BY_CMAKE "the BUILD_BY_CMAKE is new,used in distribute pg_regress.cpp" ON)
option(DEBUG_UHEAP "collect USTORE statistics" OFF)

#No matter what to set, the old mppdb aways use ENABLE_THREAD_SAFETY=yes by default defined.
option(ENABLE_THREAD_SAFETY "enable thread safety, the old is --enable-thread-safety" ON)

#The following are basically no need to configure, because these libraries are necessary or must not be used in mppdb
option(USE_BONJOUR "enable bonjour, the old is --with-bonjour" OFF)
option(USE_LDAP "build with ldap, the old is --with-ldap" OFF)#ON
option(USE_ETCD "build with etcd libs, new option for old mppdb, after 8.1 close it" OFF)
option(USE_PROTOBUF "build with with protobuf's libs, we must need it" ON)
option(USE_LIBXML "build with libxml, the old is --with-libxml" OFF)
option(HAVE_LIBXML2 "build with libxml2, the old is --with-libxml2" OFF)
option(USE_LIBXSLT "build with libxslt, the old is --with-libxslt" OFF)
option(USE_PAM "build with pam, the old is --with-pam" OFF)
option(USE_SSL "build with openssl, the old is --with-openssl" ON)
option(ENABLE_DEFAULT_GCC "enable default gcc, the old is --enable-default-gcc" OFF)
option(USE_ASSERT_CHECKING "enable cassert, the old is --enable-cassert" OFF)
option(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK "aways to close check" OFF)

#in our code(such as auth.cpp:1313), do not allow define KRB5, but must build our source with krb5's lib, WHY???
option(KRB5 "enable KRB5, the old is --with-krb5" OFF)
option(ENABLE_LLVM_COMPILE "enable llvm, the old is --enable-llvm" ON)
option(LLVM_MAJRO_VERSION "llvm majar version" 10)
option(LLVM_MINOR_VERSION "llvm minor version" 0)
option(USE_INLINE "enable static inline, the old is no" ON)
option(USE_INTEGER_DATETIMES "enable integer datetimes, the old is --enable-integer-datetimes" ON)
option(HAVE_LIBM "enable -lm, this set to default -lm" ON)
option(HAVE_POSIX_SIGNALS "enable posix signals, this set to default" ON)
option(HAVE_GCC_INT_ATOMICS "enable gcc buildin atomics operations, this set to default" ON)
option(FLEXIBLE_ARRAY_MEMBER "pq_ce need" ON)

# we will differ compile flags and definitions by different vars
set(DB_COMMON_DEFINE "")
set(PROJECT_LDFLAGS "")
set(GAUSSDB_CONFIGURE "")

message(STATUS "status ENV{DEBUG_TYPE}" $ENV{DEBUG_TYPE})
if($ENV{DEBUG_TYPE} STREQUAL "debug" OR ${ENABLE_LLT} OR ${ENABLE_UT})
    #there are two definitions for debug in mppdb, but they are confilct sometimes. such as roach(DEBUG) and *.S(NO DEBUG)
    set(USE_ASSERT_CHECKING ON)
    set(OPTIMIZE_LEVEL -O0 -g)
elseif($ENV{DEBUG_TYPE} STREQUAL "release")
    #close something for release version.
    set(ENABLE_LLT OFF)
    set(ENABLE_UT OFF)
    set(OPTIMIZE_LEVEL -O2 -g3)
elseif($ENV{DEBUG_TYPE} STREQUAL "memcheck")
    message("DEBUG_TYPE:$ENV{DEBUG_TYPE}")
    set(ENABLE_MEMORY_CHECK ON)
    set(USE_ASSERT_CHECKING ON)
    set(OPTIMIZE_LEVEL -O0 -g)
endif()
if($ENV{DEBUG_TYPE} STREQUAL "debug" OR $ENV{DEBUG_TYPE} STREQUAL "memcheck")
    set(ENABLE_DEBUG ON)
endif()

if(${BUILD_TUPLE} STREQUAL "aarch64")
    if($ENV{DEBUG_TYPE} STREQUAL "release" AND  ${ENABLE_MULTIPLE_NODES} STREQUAL "OFF")
        set(DB_COMMON_DEFINE ${DB_COMMON_DEFINE} -D__ARM_LSE)
        set(OS_OPTIONS -march=armv8-a+crc+lse)
    else()
        set(OS_OPTIONS -march=armv8-a+crc)
    endif()
endif()

if(${BUILD_TUPLE} STREQUAL "aarch64")
    if(NOT $ENV{DEBUG_TYPE} STREQUAL "memcheck")
        set(DB_COMMON_DEFINE ${DB_COMMON_DEFINE} -D__USE_NUMA)
    endif()
endif()

if(${ENABLE_LITE_MODE} STREQUAL "ON")
    set(ENABLE_LLVM_COMPILE OFF)
    set(ENABLE_GSS OFF)
    set(KRB5 OFF)
endif()

set(PROTECT_OPTIONS -fwrapv -std=c++14 -fnon-call-exceptions ${OPTIMIZE_LEVEL})
set(WARNING_OPTIONS -Wall -Wendif-labels -Werror -Wformat-security)
set(OPTIMIZE_OPTIONS -pipe -pthread -fno-aggressive-loop-optimizations -fno-expensive-optimizations -fno-omit-frame-pointer -fno-strict-aliasing -freg-struct-return)
set(CHECK_OPTIONS -Wmissing-format-attribute -Wno-attributes -Wno-unused-but-set-variable -Wno-write-strings -Wpointer-arith)
set(MACRO_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0 -DENABLE_GSTRACE -D_GNU_SOURCE -DPGXC -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -DSTREAMPLAN -D_THREAD_SAFE ${DB_COMMON_DEFINE})

# libraries need secure options during compling
set(LIB_SECURE_OPTIONS -fPIC -fno-common -fstack-protector)
# libraries need link options during linking
set(LIB_LINK_OPTIONS -pthread -std=c++14 -Wl,-z,noexecstack -Wl,-z,relro,-z,now)
if(NOT "${ENABLE_UT}" STREQUAL "ON")
    # binaries need fPIE to satisfy security options during compling
    set(BIN_SECURE_OPTIONS -fPIE -fno-common -fstack-protector)
    # binaries need fPIE pie link options during linking
    set(BIN_LINK_OPTIONS -pthread -std=c++14 -fPIE -pie -Wl,-z,noexecstack -Wl,-z,relro,-z,now)
else()
    # UT test need change binaries to libraries,set  satisfy security -fPIC during compling
    set(BIN_SECURE_OPTIONS -fPIC -fno-common -fstack-protector)
    set(BIN_LINK_OPTIONS -pthread -std=c++14 -fPIC -Wl,-z,noexecstack -Wl,-z,relro,-z,now)
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
endif()
#Set optimization level
set(DB_COMMON_FLAGS "${PROJECT_LDFLAGS} -fsigned-char")
#The optimization ability of MPPDB is not good, I should continue to optimize if I have enough time to implement it.
if(${BUILD_TUPLE} STREQUAL "x86_64")
    set(OS_OPTIONS -msse4.2 -mcx16)
    set(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK ON)
    set(ARCH_LLVMIR "" CACHE INTERNAL "")
elseif(${BUILD_TUPLE} STREQUAL "aarch64")
    set(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK OFF)
    set(ARCH_LLVMIR "_aarch64" CACHE INTERNAL "")
endif()

#The two libraries are also connected in a dynamic library, for static link: change -lasan -ltsan to -l:libasan.a -l:libtsan.a
set(MEMCHECK_FLAGS "")
set(MEMCHECK_LIBS "")
set(MEMCHECK_LINK_DIRECTORIES "")
if(${ENABLE_MEMORY_CHECK})
    set(MEMCHECK_FLAGS ${MEMCHECK_FLAGS} -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer)
    set(MEMCHECK_LIBS ${MEMCHECK_LIBS} libasan.a)
    set(MEMCHECK_LINK_DIRECTORIES ${MEMCHECK_LINK_DIRECTORIES} ${MEMCHECK_LIB_PATH})
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DENABLE_MEMORY_CHECK")
    list(REMOVE_ITEM LIB_SECURE_OPTIONS -fstack-protector)
    list(REMOVE_ITEM BIN_SECURE_OPTIONS -fstack-protector)
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
endif()
set(THREAD_FLAGS "")
set(THREAD_LIBS "")
set(THREAD_LINK_DIRECTORIES "")
if(${ENABLE_THREAD_CHECK})
    set(THREAD_FLAGS ${THREAD_FLAGS} -fsanitize=thread -fno-omit-frame-pointer)
    set(THREAD_LIBS ${THREAD_LIBS} libtsan.a)
    set(THREAD_LINK_DIRECTORIES ${THREAD_LINK_DIRECTORIES} ${MEMCHECK_LIB_PATH})
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DENABLE_THREAD_CHECK")
endif()
if("${ENABLE_LCOV}" STREQUAL "ON")
    list(APPEND CHECK_OPTIONS -fprofile-arcs -ftest-coverage)
    set(TEST_LINK_OPTIONS -lgcov -L${LCOV_LIB_PATH})
endif()

if(${USE_LDAP})
    set(HAVE_LIBLDAP 1)
    set(LIBS "${LIBS} -lldap")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_LDAP")
endif()

if(${USE_LIBXML})
    if(${HAVE_LIBXML2})
        set(LIBS "${LIBS} -lxml2")
        set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DHAVE_LIBXML2")
    else()
        set(LIBS "${LIBS} -lxml")
        set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_LIBXML")
    endif()
endif()

if(${USE_LIBXSLT})
    set(LIBS "${LIBS} -lxslt")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_LIBXSLT")
endif()

if(${USE_SSL})
    set(LIBS "${LIBS} -lssl -lcrypto")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_SSL")
endif()

#after 8.1: we build without etcd 3rd, the DEFAULT is -DUSE_ETCD=no
if(${USE_ETCD})
    set(LIBS "${LIBS} -letcd -lyajl")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_ETCD")
    set(GS_ETCDLIBS "-letcd -lyajl")
    set(GS_ETCDDEPENDS "")
else()
    set(GS_ETCDLIBS "-lgscm_etcdapi")
    set(GS_ETCDDEPENDS "gscm_etcdapi")
endif()


if(${HAVE_LIBM})
    set(LIBS "${LIBS} -lm")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DHAVE_LIBM")
endif()


if(${USE_PROTOBUF})
    set(LIBS "${LIBS} -lprotobuf -lgrpc++ -lgrpc -lgpr")
    set(GAUSSDB_CONFIGURE "${GAUSSDB_CONFIGURE} -DUSE_PROTOBUF")
endif()
add_definitions(-Wno-builtin-macro-redefined)
SET_GCC_FLAGS(DB_COMMON_FLAGS "")

#hotpatch
set(HOTPATCH_PLATFORM_LIST suse11_sp1_x86_64 suse12_sp5_x86_64 euleros2.0_sp8_aarch64 euleros2.0_sp9_aarch64 euleros2.0_sp10_aarch64 euleros2.0_sp2_x86_64 euleros2.0_sp5_x86_64 euleros2.0_sp10_x86_64 kylinv10_sp1_aarch64 kylinv10_sp1_x86_64_intel)
set(HOTPATCH_ARM_LIST euleros2.0_sp8_aarch64 euleros2.0_sp9_aarch64 euleros2.0_sp10_aarch64 kylinv10_sp1_aarch64)
list(FIND HOTPATCH_PLATFORM_LIST "${PLAT_FORM_NAME}" RET_HOTPATCH)
list(FIND HOTPATCH_ARM_LIST "${PLAT_FORM_NAME}" RET_ARM_HOTPATCH)
if(NOT ${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF)
    if(NOT "${RET_HOTPATCH}" STREQUAL "-1")
        if("${GCC_VERSION}" STREQUAL "7.3.0")
            set(SUPPORT_HOTPATCH "yes")
            if(NOT ${RET_ARM_HOTPATCH} EQUAL -1)
                set(HOTPATCH_ATOMIC_LDS -Wl,-T${LIBHOTPATCH_TOOL_PATH}/atomic.lds)
            endif()
        else()
            set(SUPPORT_HOTPATCH "no")
        endif()
    else()
        set(SUPPORT_HOTPATCH "no")
    endif()
else()
    set(SUPPORT_HOTPATCH "no")
endif()

if(${ENABLE_LITE_MODE} STREQUAL "ON")
    set(SUPPORT_HOTPATCH "no")
endif()

if(${ENABLE_LLVM_COMPILE} STREQUAL "ON")
    # LLVM version
    execute_process(COMMAND ${LLVM_CONFIG} --version OUTPUT_VARIABLE LLVM_VERSION_STR OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "." ";" LLVM_VERSION_LIST ${LLVM_VERSION_STR})
    list(GET LLVM_VERSION_LIST 0 LLVM_MAJOR_VERSION)
    list(GET LLVM_VERSION_LIST 1 LLVM_MINOR_VERSION)
endif()

if(${NO_CHECK_CONFIG})
    string(SUBSTRING "${BUILD_TUPLE}" 0 6 BUILD_HOST_PLATFORM)
    if("${BUILD_HOST_PLATFORM}" STREQUAL "x86_64")
        set(CONFIG_IN_FILE pg_config.h.in.nocheck.x86_64)
    else()
        set(CONFIG_IN_FILE pg_config.h.in.nocheck.aarch64)
    endif()
else()
    CHECK_FOR_MPPDB()
    if("${ENABLE_GSS}" STREQUAL "ON")
        set(HAVE_GSSAPI_GSSAPI_H 0)
        set(HAVE_GSSAPI_H 0)
    endif()
    if(NOT "${PLAT_FORM_NAME}" STREQUAL "win32")
        set(HAVE_LDAP_H 0)
        set(HAVE_GETTIMEOFDAY 0)
    endif()
    if("${USE_PAM}" STREQUAL "OFF")
        set(HAVE_SECURITY_PAM_APPL_H 0)
    endif()
    if("${HAVE_STRTOLL}" EQUAL "1")
        set(HAVE_STRTOQ 0)
    endif()
    if("${HAVE_STRTOULL}" EQUAL "1")
        set(HAVE_STRTOUQ 0)
    endif()
    check_struct_has_member("struct tm" tm_zone time.h HAVE_TM_ZONE)
    check_symbol_exists("struct tm" tzname time.h HAVE_TZNAME)
    check_symbol_exists(__get_cpuid "cpuid.h" HAVE__GET_CPUID)
    check_symbol_exists(__get_cpuid "intrin.h" HAVE__CPUID)
    set(CONFIG_IN_FILE pg_config.h.in)
endif()
SET(EC_CONFIG_IN_FILE ecpg_config.h.in)

build_mppdb_config_paths_h(PG_CONFIG_PATH_H)
configure_file(${openGauss}/cmake/src/config-in/${CONFIG_IN_FILE} ${CMAKE_BINARY_DIR}/pg_config.h @ONLY)
configure_file(${openGauss}/cmake/src/config-in/${EC_CONFIG_IN_FILE} ${CMAKE_BINARY_DIR}/ecpg_config.h @ONLY)
#set host_cpu for pgxs.mk
set(HOST_CPU ${BUILD_TUPLE})
configure_file(${openGauss}/src/makefiles/pgxs.mk ${CMAKE_BINARY_DIR}/${openGauss}/src/makefiles/pgxs.mk @ONLY)
SET(PROJECT_INCLUDE_DIR ${PROJECT_INCLUDE_DIR} ${CMAKE_BINARY_DIR})

# 排斥项
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" AND "${ENABLE_MOT}" STREQUAL "ON")
    message(FATAL_ERROR "error: --enable-mot option is not supported with --enable-multiple-nodes option")
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" AND "${ENABLE_LITE_MODE}" STREQUAL "ON")
    message(FATAL_ERROR "error: --enable-lite-mode option is not supported with --enable-multiple-nodes option")
endif()
