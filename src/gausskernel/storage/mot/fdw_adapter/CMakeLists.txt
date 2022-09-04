#This is the main CMAKE for building MOT fdw_adapter component.

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mot_fdw_adapter_SRC)

set(TGT_mot_fdw_adapter_INC
    ${PROJECT_SRC_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MOT_CORE_INCLUDE_PATH}
    ${MOT_JIT_EXEC_PATH}
)

set(mot_fdw_adapter_DEF_OPTIONS ${MACRO_OPTIONS} -DMOT_SECURE)
if("${USE_ASSERT_CHECKING}" STREQUAL "ON")
    list(APPEND mot_fdw_adapter_DEF_OPTIONS -DDEBUG)
endif()
set(mot_fdw_adapter_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -faligned-new)
set(mot_fdw_adapter_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_mot_fdw_adapter TGT_mot_fdw_adapter_SRC TGT_mot_fdw_adapter_INC
    "${mot_fdw_adapter_DEF_OPTIONS}" "${mot_fdw_adapter_COMPILE_OPTIONS}" "${mot_fdw_adapter_LINK_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/mot_fdw.control
    DESTINATION share/postgresql/extension
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/mot_fdw--1.0.sql
    DESTINATION share/postgresql/extension
)
install(FILES ${PROJECT_SRC_DIR}/gausskernel/storage/mot/core/mot.conf
    DESTINATION share/postgresql
    RENAME mot.conf.sample
)

