#This is the main CMAKE for build all components.
# hstore.so
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/hstore TGT_hstore_SRC)

set(hstore_DEF_OPTIONS ${MACRO_OPTIONS})
set(hstore_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(hstore_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(hstore TGT_hstore_SRC "" "${hstore_DEF_OPTIONS}" "${hstore_COMPILE_OPTIONS}" "${hstore_LINK_OPTIONS}")
set_target_properties(hstore PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hstore--unpackaged--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hstore--unpackaged--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hstore.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hstore--1.1.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hstore--1.0--1.1.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS hstore LIBRARY DESTINATION lib/postgresql)

