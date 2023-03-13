#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})
INCLUDE_DIRECTORIES(${LIBXML_INCLUDE_PATH}/libxml2)
INCLUDE_DIRECTORIES(${LIBXML_LIB_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/timezone
    ${CMAKE_CURRENT_SOURCE_DIR}/interfaces
    ${CMAKE_CURRENT_SOURCE_DIR}/pl
    ${CMAKE_CURRENT_SOURCE_DIR}/port
    ${CMAKE_CURRENT_SOURCE_DIR}/backend
    ${CMAKE_CURRENT_SOURCE_DIR}/pgxc
)

add_subdirectory(timezone)
add_subdirectory(interfaces)
add_subdirectory(pl)
add_subdirectory(port)
add_subdirectory(backend)
add_subdirectory(pgxc)

