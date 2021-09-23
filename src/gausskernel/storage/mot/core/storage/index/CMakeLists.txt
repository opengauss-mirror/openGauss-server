#This is the main CMAKE for building MOT core/storage/index component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_storage_index_SRC)

set(TGT_mot_core_storage_index_INC
    ${PROJECT_SRC_DIR}/include
    ${MASSTREE_INCLUDE_PATH}
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_storage_index TGT_mot_core_storage_index_SRC TGT_mot_core_storage_index_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/masstree
)

add_subdirectory(masstree)

