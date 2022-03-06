#This is the main CMAKE for build all components.
# libcmpq.a
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_cmpq_SRC)
set(TGT_cmpq_INC 
    ${PROJECT_SRC_DIR}/include/cm 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${PROJECT_TRUNK_DIR}/distribute/cm/cm_etcdapi 
    ${LIBCGROUP_INCLUDE_PATH}
    ${KERBEROS_INCLUDE_PATH}
)

set(cmpq_DEF_OPTIONS ${MACRO_OPTIONS})
set(cmpq_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-strong)
list(REMOVE_ITEM cmpq_COMPILE_OPTIONS -fstack-protector)
add_static_libtarget(cmpq TGT_cmpq_SRC TGT_cmpq_INC "${cmpq_DEF_OPTIONS}" "${cmpq_COMPILE_OPTIONS}")

set(cmpq_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM cmpq_LINK_OPTIONS -pthread)
add_shared_libtarget(cmpq TGT_cmpq_SRC TGT_cmpq_INC "${cmpq_DEF_OPTIONS}" "${cmpq_COMPILE_OPTIONS}" "${cmpq_LINK_OPTIONS}")
set_target_properties(cmpq PROPERTIES VERSION 1)

install(TARGETS cmpq_static ARCHIVE DESTINATION lib)
install(TARGETS cmpq DESTINATION lib)
