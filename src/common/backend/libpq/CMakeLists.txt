#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_libpq_SRC)

set(TGT_libpq_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(libpq_DEF_OPTIONS ${MACRO_OPTIONS})
set(libpq_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(libpq_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_libpq TGT_libpq_SRC TGT_libpq_INC "${libpq_DEF_OPTIONS}" "${libpq_COMPILE_OPTIONS}" "${libpq_LINK_OPTIONS}")

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/kerberos 
    DESTINATION etc/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/pg_hba.conf.sample
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/pg_ident.conf.sample
    DESTINATION share/postgresql/
)
