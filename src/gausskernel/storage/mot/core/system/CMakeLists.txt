#This is the main CMAKE for building MOT core/system component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_system_SRC)

set(TGT_mot_core_system_INC
    ${PROJECT_SRC_DIR}/include
    ${MASSTREE_INCLUDE_PATH}
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_system TGT_mot_core_system_SRC TGT_mot_core_system_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/recovery
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction_logger
)

add_subdirectory(checkpoint)
add_subdirectory(common)
add_subdirectory(recovery)
add_subdirectory(statistics)
add_subdirectory(transaction)
add_subdirectory(transaction_logger)

