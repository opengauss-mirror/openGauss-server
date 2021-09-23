#This is the main CMAKE for building MOT core/memory_garbage_collector/garbage_collector component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_memory_garbage_collector_SRC)

set(TGT_mot_core_memory_garbage_collector_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_memory_garbage_collector TGT_mot_core_memory_garbage_collector_SRC TGT_mot_core_memory_garbage_collector_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

