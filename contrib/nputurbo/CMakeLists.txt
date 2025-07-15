cmake_minimum_required(VERSION 3.16)
project(NPU)
message(STATUS "build NPU ascendc_kernels.")
set(RUN_MODE "npu")
set(CMAKE_BUILD_TYPE "Release")
set(ASCEND_CANN_PACKAGE_PATH "/usr/local/Ascend/ascend-toolkit/latest" CACHE STRING "ASCEND CANN package install directory.")
set(SOC_VERSION "Ascend910B4" CACHE STRING "npu soc version.")
add_definitions(-DNPU_SOC_VERSION="${SOC_VERSION}")

message(STATUS "build nputurbo for datavec, npu type: ${SOC_VERSION}.")

include_directories(${ASCEND_CANN_PACKAGE_PATH}/aarch64-linux/include)

file(GLOB KERNEL_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/matmul_custom.cpp
)
include(cmake/npu_lib.cmake)
target_include_directories(ascendc_kernels PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_compile_options(ascendc_kernels PRIVATE
    -O2
    -std=c++17
)

add_library(nputurbo SHARED matmulmgr.cpp)
target_include_directories(nputurbo PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${ASCEND_CANN_PACKAGE_PATH}/aarch64-linux/include
)
target_compile_options(nputurbo PRIVATE
    -O2 -std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0 -Wall
)
target_link_libraries(nputurbo PRIVATE
    host_intf_pub
    ascendc_kernels
    tiling_api
    register
    platform
    graph
    ascendalog
    dl
)
