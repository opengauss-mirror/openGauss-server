#This is the main CMAKE for building MOT core/infra component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_infra_SRC)

set(TGT_mot_core_infra_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_infra TGT_mot_core_infra_SRC TGT_mot_core_infra_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/config
    ${CMAKE_CURRENT_SOURCE_DIR}/containers
    ${CMAKE_CURRENT_SOURCE_DIR}/stats
    ${CMAKE_CURRENT_SOURCE_DIR}/synchronization
)

add_subdirectory(config)
add_subdirectory(containers)
add_subdirectory(stats)
add_subdirectory(synchronization)

