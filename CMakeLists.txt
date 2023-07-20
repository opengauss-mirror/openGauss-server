# Copyright (c) Huawei Technologies Co., Ltd. 2020-2030. All rights reserved.
# description: CMake build main entrance
# date: 2021-02-08
# version: 1.0.0

cmake_minimum_required(VERSION 3.16.5)

#project name
project(openGauss)

if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
endif()

if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()

SET(CMAKE_VERBOSE_MAKEFILE OFF CACHE INTERNAL "")
SET(CMAKE_RULE_MESSAGES OFF CACHE INTERNAL "")
SET(CMAKE_SKIP_RPATH TRUE CACHE INTERNAL "")
#set CMAKE_AR parameters,set 'qc' to 'crs'
set(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> crs <TARGET> <LINK_FLAGS> <OBJECTS>")
set(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> crs <TARGET> <LINK_FLAGS> <OBJECTS>")
#forbidden the default fPIC option in cmake mechanism
set(CMAKE_POSITION_INDEPENDENT_CODE OFF)
#set LIBRARY_OUTPUT
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

#Set the options
option (OPT_BUILD_3RD "Build the with --3rd." OFF)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
    MESSAGE(FATAL_ERROR " The option ENABLE_MULTIPLE_NODE is not supported thus should be turned off.")
endif()
if("${ENABLE_PRIVATEGAUSS}" STREQUAL "ON")
    MESSAGE(FATAL_ERROR " The option ENABLE_PRIVATEGAUSS is not supported thus should be turned off.")
endif()

#FIXME: make it an argument
set(openGauss ".")
set(ENV{openGauss} ".")

#------------------------------------------------------------------------------------
set(PROJECT_TRUNK_DIR ${CMAKE_SOURCE_DIR})
set(PROJECT_OPENGS_DIR ${PROJECT_TRUNK_DIR}/${openGauss} CACHE INTERNAL "")

# PROJECT_TRUNK_DIR: ./
# PROJECT_OPENGS_DIR: ${PROJECT_TRUNK_DIR}/${openGauss}/

#set src dir
set(PROJECT_SRC_DIR ${PROJECT_OPENGS_DIR}/src CACHE INTERNAL "")

set(3RD_PATH $ENV{THIRD_BIN_PATH})
set(prefix_home $ENV{PREFIX_HOME})
set(CMAKE_INSTALL_PREFIX ${prefix_home} CACHE INTERNAL "")
if("$ENV{GCC_VERSION}" STREQUAL "")
    execute_process(COMMAND gcc --version OUTPUT_VARIABLE GCC_VERSION_INFO OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "\n" ";" GCC_VERSION_INFO_TMP ${GCC_VERSION_INFO})
    list(GET GCC_VERSION_INFO_TMP 0 GCC_VERSION_INFO_FIRSTLINE)
    string(REPLACE " " ";" GCC_VERSION_INFO_LIST ${GCC_VERSION_INFO_FIRSTLINE})
    list(POP_BACK GCC_VERSION_INFO_LIST GCC_VERSION)
else()
    set(GCC_VERSION $ENV{GCC_VERSION})
endif()
MESSAGE(STATUS "The current gcc version is: ${GCC_VERSION}")
if("${GCC_VERSION}" STREQUAL "7.3.0")
    set(GCC_VERSION_LIT 7.3)
    set(LIBSTD_SUB_VERSION 24)
elseif("${GCC_VERSION}" STREQUAL "10.3.1")
    set(GCC_VERSION_LIT 10.3)
    set(LIBSTD_SUB_VERSION 28)
elseif("${GCC_VERSION}" STREQUAL "10.3.0")
    set(GCC_VERSION_LIT 10.3)
    set(LIBSTD_SUB_VERSION 28)
endif()

if("$ENV{BUILD_TUPLE}" STREQUAL "")
    execute_process(COMMAND uname -p OUTPUT_VARIABLE CURRENT_TUPLE OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(BUILD_TUPLE ${CURRENT_TUPLE})
else()
    set(BUILD_TUPLE $ENV{BUILD_TUPLE})
endif()

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/src ${CMAKE_SOURCE_DIR}/cmake/check 
    ${CMAKE_SOURCE_DIR}/cmake/hotpatch ${CMAKE_SOURCE_DIR}/contrib CACHE INTERNAL "")

include(build_function)
include(set_thirdparty_path)
include(build_options)

include_directories(${PROJECT_SRC_DIR}/include ${PROJECT_INCLUDE_DIR})
include_directories(${KERBEROS_INCLUDE_PATH} ${LIBOBS_INCLUDE_PATH} ${LIBCURL_INCLUDE_PATH} ${LIBOPENSSL_INCLUDE_PATH})

add_subdirectory(${openGauss}/contrib)
add_subdirectory(${openGauss}/src)

execute_process(COMMAND rm -rf ${CMAKE_BINARY_DIR}/proc_srclib)
execute_process(COMMAND mkdir ${CMAKE_BINARY_DIR}/proc_srclib)
execute_process(COMMAND rm -rf ${CMAKE_BINARY_DIR}/pg_plugin)
execute_process(COMMAND mkdir ${CMAKE_BINARY_DIR}/pg_plugin)
execute_process(COMMAND rm -rf ${CMAKE_BINARY_DIR}/libsimsearch)
execute_process(COMMAND mkdir ${CMAKE_BINARY_DIR}/libsimsearch)

install(DIRECTORY ${CMAKE_BINARY_DIR}/proc_srclib DESTINATION lib/postgresql)
install(DIRECTORY ${CMAKE_BINARY_DIR}/pg_plugin DESTINATION lib/postgresql)
install(DIRECTORY ${CMAKE_BINARY_DIR}/libsimsearch DESTINATION lib)

if(EXISTS ${CMAKE_SOURCE_DIR}/contrib/dolphin)
    install(CODE "execute_process(COMMAND bash cmake.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/contrib/dolphin)")
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin.control
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--2.0.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--1.0.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--1.0--2.0.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--2.0--1.0.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--1.1--2.0.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/dolphin--2.0--1.1.sql
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/openGauss_expr_dolphin.ir
        DESTINATION share/postgresql/extension/
    )
    install(FILES ${CMAKE_SOURCE_DIR}/contrib/dolphin/tmp_build/lib/dolphin.so
        DESTINATION lib/postgresql
    )
endif()