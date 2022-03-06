#This is the main CMAKE for build all components.
# libcmclient.a
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_cmclient_SRC)
set(TGT_cmclient_INC 
    ${PROJECT_SRC_DIR}/include/cm 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${PROJECT_TRUNK_DIR}/distribute/cm/cm_etcdapi 
    ${LIBCGROUP_INCLUDE_PATH}
    ${KERBEROS_INCLUDE_PATH}
)

set(cmclient_DEF_OPTIONS ${MACRO_OPTIONS})
set(cmclient_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-strong)
list(REMOVE_ITEM cmclient_COMPILE_OPTIONS -fstack-protector)
add_static_libtarget(cmclient TGT_cmclient_SRC TGT_cmclient_INC "${cmclient_DEF_OPTIONS}" "${cmclient_COMPILE_OPTIONS}")
install(TARGETS cmclient_static ARCHIVE DESTINATION lib)

set(cmclient_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM cmclient_LINK_OPTIONS -pthread)

add_shared_libtarget(cmclient TGT_cmclient_SRC TGT_cmclient_INC "${cmclient_DEF_OPTIONS}" "${cmclient_COMPILE_OPTIONS}" "${cmclient_LINK_OPTIONS}")
set_target_properties(cmclient PROPERTIES VERSION 1)

install(TARGETS cmclient LIBRARY DESTINATION lib)
