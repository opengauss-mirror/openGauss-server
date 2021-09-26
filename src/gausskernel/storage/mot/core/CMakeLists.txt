#This is the main CMAKE for building MOT core component.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(mot_core_DEF_OPTIONS ${MACRO_OPTIONS} -DMOT_SECURE)
if("${USE_ASSERT_CHECKING}" STREQUAL "ON")
    list(APPEND mot_core_DEF_OPTIONS -DDEBUG)
endif()
set(mot_core_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS}
    -faligned-new -fno-exceptions -fno-rtti -Wwrite-strings -Wcast-align -Wreturn-type -Wpointer-arith -Wlogical-op -Waddress
    -Wsizeof-pointer-memaccess -Winit-self -Wduplicated-cond -Wnon-virtual-dtor -std=gnu++11 -w -D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -pthread -Wall)
if(${BUILD_TUPLE} STREQUAL "aarch64")
    list(APPEND mot_core_COMPILE_OPTIONS -march=armv8-a+crc)
else()
    list(APPEND mot_core_COMPILE_OPTIONS -mcx16)
endif()
set(mot_core_LINK_OPTIONS ${BIN_LINK_OPTIONS})

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency_control
    ${CMAKE_CURRENT_SOURCE_DIR}/infra
    ${CMAKE_CURRENT_SOURCE_DIR}/memory
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/system
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
)

add_subdirectory(concurrency_control)
add_subdirectory(infra)
add_subdirectory(memory)
add_subdirectory(storage)
add_subdirectory(system)
add_subdirectory(utils)

