#This is the main CMAKE for building MOT.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

#Common variables definitions (used by sub modules).
set(MOT_CORE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/core)
set(MOT_FDW_ADAPTER_PATH ${CMAKE_CURRENT_SOURCE_DIR}/fdw_adapter)
set(MOT_JIT_EXEC_PATH ${CMAKE_CURRENT_SOURCE_DIR}/jit_exec)
set(MOT_CORE_INCLUDE_PATH
    ${MOT_CORE_PATH}/concurrency_control
    ${MOT_CORE_PATH}/infra
    ${MOT_CORE_PATH}/infra/config
    ${MOT_CORE_PATH}/infra/containers
    ${MOT_CORE_PATH}/infra/stats
    ${MOT_CORE_PATH}/infra/synchronization
    ${MOT_CORE_PATH}/memory
    ${MOT_CORE_PATH}/memory/garbage_collector
    ${MOT_CORE_PATH}/storage
    ${MOT_CORE_PATH}/storage/index
    ${MOT_CORE_PATH}/storage/sentinel
    ${MOT_CORE_PATH}/system
    ${MOT_CORE_PATH}/system/checkpoint
    ${MOT_CORE_PATH}/system/common
    ${MOT_CORE_PATH}/system/recovery
    ${MOT_CORE_PATH}/system/statistics
    ${MOT_CORE_PATH}/system/transaction
    ${MOT_CORE_PATH}/system/transaction_logger
    ${MOT_CORE_PATH}/system/transaction_logger/group_synchronous_redo_log
    ${MOT_CORE_PATH}/system/transaction_logger/synchronous_redo_log
    ${MOT_CORE_PATH}/utils
)

set(CMAKE_MODULE_PATH
    ${MOT_CORE_PATH}
    ${MOT_FDW_ADAPTER_PATH}
    ${MOT_JIT_EXEC_PATH}
)

add_subdirectory(core)
add_subdirectory(fdw_adapter)
add_subdirectory(jit_exec)

