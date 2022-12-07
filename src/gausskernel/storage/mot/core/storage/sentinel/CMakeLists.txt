#This is the main CMAKE for building MOT core/storage/sentinel component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_storage_sentinel_SRC)

set(TGT_mot_core_storage_sentinel_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_storage_sentinel TGT_mot_core_storage_sentinel_SRC TGT_mot_core_storage_sentinel_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

