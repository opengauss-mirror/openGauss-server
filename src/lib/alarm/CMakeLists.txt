#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_alarmclient_SRC)

set(TGT_alarmclient_INC 
    ${PROJECT_SRC_DIR}/lib/alarm 
    ${SECURE_INCLUDE_PATH}
)

set(alarmclient_DEF_OPTIONS ${MACRO_OPTIONS})
set(alarmclient_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} -g3 -ggdb3 -l${SECURE_C_CHECK} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -O3)
list(REMOVE_ITEM alarmclient_COMPILE_OPTIONS -O2)
add_static_libtarget(alarmclient TGT_alarmclient_SRC TGT_alarmclient_INC "${alarmclient_DEF_OPTIONS}" "${alarmclient_COMPILE_OPTIONS}")
target_link_directories(alarmclient_static PRIVATE ${SECURE_LIB_PATH})

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/alarmItem.conf DESTINATION bin)

