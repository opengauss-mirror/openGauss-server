#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog
    ${CMAKE_CURRENT_SOURCE_DIR}/commands
    ${CMAKE_CURRENT_SOURCE_DIR}/executor
)
add_subdirectory(catalog)
add_subdirectory(commands)
add_subdirectory(executor)

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/snapshots
    DESTINATION share/postgresql/db4ai/ OPTIONAL
)
endif()