#This is the main CMAKE for build all components.
set(TGT_guc_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_guc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster_config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster_guc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_hba.cpp
)
set(TGT_guc_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/bin/pg_dump 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${LIBOPENSSL_INCLUDE_PATH}
)
set(guc_DEF_OPTIONS ${MACRO_OPTIONS})
set(guc_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(guc_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(guc_LINK_LIBS libelog.a libconfig.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
set(guc_LINK_DEPEND elog_static config_static pgport_static)
set(guc_LINK_DIRS ${LIBOPENSSL_LIB_PATH}  ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBORC_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${LIBEDIT_LIB_PATH} ${SECURE_LIB_PATH})

if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(utgsguc TGT_guc_SRC TGT_guc_INC "${guc_DEF_OPTIONS}" "${guc_COMPILE_OPTIONS}" "${guc_LINK_OPTIONS}") 
    add_dependencies(utgsguc ${guc_LINK_DEPEND})
    target_link_libraries(utgsguc PRIVATE ${guc_LINK_LIBS}) 
    target_link_directories(utgsguc PUBLIC ${guc_LINK_DIRS})
    install(TARGETS utgsguc LIBRARY DESTINATION lib)
else()
    add_bintarget(gs_guc TGT_guc_SRC TGT_guc_INC "${guc_DEF_OPTIONS}" "${guc_COMPILE_OPTIONS}" "${guc_LINK_OPTIONS}" "${guc_LINK_LIBS}")
    add_dependencies(gs_guc ${guc_LINK_DEPEND}) 
    target_link_directories(gs_guc PUBLIC ${guc_LINK_DIRS})
    if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/cluster_guc.conf DESTINATION bin)
    endif()
    install(TARGETS gs_guc RUNTIME DESTINATION bin)
endif() 


# encrypt bin
set(TGT_encrypt_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/encrypt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/crypt.cpp
)
set(TGT_encrypt_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/pg_dump 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)
set(encrypt_DEF_OPTIONS ${MACRO_OPTIONS})
set(encrypt_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(encrypt_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(encrypt_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(encrypt TGT_encrypt_SRC TGT_encrypt_INC "${encrypt_DEF_OPTIONS}" "${encrypt_COMPILE_OPTIONS}" "${encrypt_LINK_OPTIONS}" "${encrypt_LINK_LIBS}")
add_dependencies(encrypt elog_static pgport_static)
target_link_directories(encrypt PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH}
)
install(TARGETS encrypt RUNTIME DESTINATION bin)
