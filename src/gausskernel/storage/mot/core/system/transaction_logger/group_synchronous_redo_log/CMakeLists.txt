#This is the main CMAKE for building MOT core/system/transaction_logger/group_synchronous_redo_log component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_system_transaction_logger_group_synchronous_redo_log_SRC)

set(TGT_mot_core_system_transaction_logger_group_synchronous_redo_log_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_system_transaction_logger_group_synchronous_redo_log
    TGT_mot_core_system_transaction_logger_group_synchronous_redo_log_SRC TGT_mot_core_system_transaction_logger_group_synchronous_redo_log_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

