#This is the main CMAKE for building MOT core/system/transaction_logger component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_system_transaction_logger_SRC)

set(TGT_mot_core_system_transaction_logger_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_system_transaction_logger TGT_mot_core_system_transaction_logger_SRC TGT_mot_core_system_transaction_logger_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/group_synchronous_redo_log
    ${CMAKE_CURRENT_SOURCE_DIR}/synchronous_redo_log
)

add_subdirectory(group_synchronous_redo_log)
add_subdirectory(synchronous_redo_log)

