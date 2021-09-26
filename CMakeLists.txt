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
execute_process(COMMAND sh ${PROJECT_SRC_DIR}/get_PlatForm_str.sh OUTPUT_VARIABLE PLAT_FORM_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
set(prefix_home $ENV{PREFIX_HOME})
set(CMAKE_INSTALL_PREFIX ${prefix_home} CACHE INTERNAL "")
if("$ENV{GCC_VERSION}" STREQUAL "")
    execute_process(COMMAND gcc --version OUTPUT_VARIABLE GCC_VERSION_INFO OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "\n" " " GCC_VERSION_INFO_TMP ${GCC_VERSION_INFO})
    string(REPLACE " " ";" GCC_VERSION_INFO_LIST ${GCC_VERSION_INFO_TMP})
    list(GET GCC_VERSION_INFO_LIST 2 GCC_VERSION)
else()
    set(GCC_VERSION $ENV{GCC_VERSION})
endif()
if("${GCC_VERSION}" STREQUAL "7.3.0")
    set(GCC_VERSION_LIT 7.3)
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
