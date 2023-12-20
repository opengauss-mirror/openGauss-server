#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/bbox
    ${CMAKE_CURRENT_SOURCE_DIR}/instruments
    ${CMAKE_CURRENT_SOURCE_DIR}/communication
    ${CMAKE_CURRENT_SOURCE_DIR}/extension
    ${CMAKE_CURRENT_SOURCE_DIR}/grpc
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/workload
)

if("${ENABLE_BBOX}" STREQUAL "ON")
	add_subdirectory(bbox)
else()
	list(REMOVE_ITEM CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bbox)
endif()
add_subdirectory(instruments)
add_subdirectory(communication)
add_subdirectory(extension)
add_subdirectory(grpc)
add_subdirectory(utils)
add_subdirectory(workload)
