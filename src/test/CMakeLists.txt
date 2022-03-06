#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/isolation
        )

add_subdirectory(isolation)
if ("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(regress)
endif ()

if ("${ENABLE_UT}" STREQUAL "ON" AND "${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(ut)
    install(
            DIRECTORY ${CMAKE_BINARY_DIR}/ut_bin/ DESTINATION ut_bin
            PATTERN "ut_bin/*"
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    )
endif ()

