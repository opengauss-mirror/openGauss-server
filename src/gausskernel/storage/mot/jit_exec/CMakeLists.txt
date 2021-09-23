#This is the main CMAKE for building MOT jit_exec component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_jit_exec_SRC)

set(TGT_mot_jit_exec_INC
    ${PROJECT_SRC_DIR}/include
    ${LIBLLVM_INCLUDE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MOT_CORE_INCLUDE_PATH}
    ${MOT_FDW_ADAPTER_PATH}
)

set(mot_jit_exec_DEF_OPTIONS ${MACRO_OPTIONS} -DMOT_SECURE -D__STDC_FORMAT_MACROS)
if("${USE_ASSERT_CHECKING}" STREQUAL "ON")
    list(APPEND mot_jit_exec_DEF_OPTIONS -DDEBUG)
endif()
set(mot_jit_exec_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -faligned-new -fno-rtti)
set(mot_jit_exec_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_mot_jit_exec TGT_mot_jit_exec_SRC TGT_mot_jit_exec_INC
    "${mot_jit_exec_DEF_OPTIONS}" "${mot_jit_exec_COMPILE_OPTIONS}" "${mot_jit_exec_LINK_OPTIONS}")

