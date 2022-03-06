#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${LIBTHRIFT_INCLUDE_PATH} ${SNAPPY_INCLUDE_PATH} ${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/executor
    ${CMAKE_CURRENT_SOURCE_DIR}/opfusion
    ${CMAKE_CURRENT_SOURCE_DIR}/vecexecutor
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND CMAKE_MODULE_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/codegen
    )
    add_subdirectory(codegen)
endif()

add_subdirectory(executor)
add_subdirectory(opfusion)
add_subdirectory(vecexecutor)
