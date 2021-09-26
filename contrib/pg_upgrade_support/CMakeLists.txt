#This is the main CMAKE for build all components.
# pg_upgrade_support
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pg_upgrade_support_SRC)

set(pg_upgrade_support_DEF_OPTIONS ${MACRO_OPTIONS})
list(REMOVE_ITEM pg_upgrade_support_DEF_OPTIONS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS)
set(pg_upgrade_support_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
list(REMOVE_ITEM pg_upgrade_support_COMPILE_OPTIONS -fstack-protector -pthread)
set(pg_upgrade_support_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pg_upgrade_support TGT_pg_upgrade_support_SRC "" "${pg_upgrade_support_DEF_OPTIONS}" "${pg_upgrade_support_COMPILE_OPTIONS}" "${pg_upgrade_support_LINK_OPTIONS}")
set_target_properties(pg_upgrade_support PROPERTIES PREFIX "")

install(TARGETS pg_upgrade_support LIBRARY DESTINATION lib/postgresql)
