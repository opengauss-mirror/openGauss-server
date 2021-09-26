#This is the main CMAKE for build all components.

SET(CMAKE_VERBOSE_MAKEFILE ON) #显示详细的原始编译信息
SET(CMAKE_RULE_MESSAGES OFF)   #减少一些不必要的输出
SET(CMAKE_SKIP_RPATH TRUE)     #TRUE表示在编译及安装阶段忽略RPATH

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/snowball
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog
    ${CMAKE_CURRENT_SOURCE_DIR}/client_logic
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/libpq
    ${CMAKE_CURRENT_SOURCE_DIR}/nodes
    ${CMAKE_CURRENT_SOURCE_DIR}/tsearch
    ${CMAKE_CURRENT_SOURCE_DIR}/pgxc_single
    ${CMAKE_CURRENT_SOURCE_DIR}/parser
    ${CMAKE_CURRENT_SOURCE_DIR}/port
    ${CMAKE_CURRENT_SOURCE_DIR}/regex
)

add_subdirectory(snowball)
add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(client_logic)
add_subdirectory(lib)
add_subdirectory(libpq)
add_subdirectory(nodes)
add_subdirectory(tsearch)
add_subdirectory(pgxc_single)
add_subdirectory(parser)
add_subdirectory(port)
add_subdirectory(regex)