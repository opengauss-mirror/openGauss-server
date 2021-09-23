#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_policy
    ${CMAKE_CURRENT_SOURCE_DIR}/iprange
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_ledger
    ${CMAKE_CURRENT_SOURCE_DIR}/tde_key_management
)

add_subdirectory(gs_policy)
add_subdirectory(iprange)
add_subdirectory(gs_ledger)
add_subdirectory(tde_key_management)

