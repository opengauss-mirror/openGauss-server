#This is the main CMAKE for building MOT core/utils component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_core_utils_SRC)

set(TGT_mot_core_utils_INC
    ${PROJECT_SRC_DIR}/include
    ${MOT_CORE_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_mot_core_utils TGT_mot_core_utils_SRC TGT_mot_core_utils_INC
    "${mot_core_DEF_OPTIONS}" "${mot_core_COMPILE_OPTIONS}" "${mot_core_LINK_OPTIONS}")

