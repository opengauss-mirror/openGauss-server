#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/extension/connector
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/extension/foreign
)

add_subdirectory(connector)
add_subdirectory(foreign)
