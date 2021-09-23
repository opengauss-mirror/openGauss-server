#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/isolation
)

add_subdirectory(isolation)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(regress)
endif()
