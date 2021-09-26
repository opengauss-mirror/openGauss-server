#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_hbpq_SRC)
set(TGT_hbpq_INC 
    ${PROJECT_SRC_DIR}/gausskernel/storage/replication/heartbeat/libpq
)

set(hbpq_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(hbpq TGT_hbpq_SRC TGT_hbpq_INC "${MACRO_OPTIONS}" "${hbpq_COMPILE_OPTIONS}")

set(hbpq_DEF_OPTIONS ${MACRO_OPTIONS})
set(hbpq_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM hbpq_LINK_OPTIONS -fstack-protector)
add_shared_libtarget(hbpq TGT_hbpq_SRC TGT_hbpq_INC "${hbpq_DEF_OPTIONS}" "${hbpq_COMPILE_OPTIONS}" "${hbpq_LINK_OPTIONS}")
set_target_properties(hbpq PROPERTIES SOVERSION 1)
add_static_objtarget(gausskernel_storage_replication_heartbeat_libpq TGT_hbpq_SRC TGT_hbpq_INC "${hbpq_DEF_OPTIONS}" "${hbpq_COMPILE_OPTIONS}" "${hbpq_LINK_OPTIONS}")
