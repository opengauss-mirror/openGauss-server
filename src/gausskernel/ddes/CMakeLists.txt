#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/ddes/adapter
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/script/dms_contrl.sh DESTINATION bin)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/script/dss_contrl.sh DESTINATION bin)
endif()

add_subdirectory(adapter)