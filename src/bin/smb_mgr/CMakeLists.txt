#This is the main CMAKE for build all components.
# smb_mgr.bin
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/matrix_mem/matrix_mem.cpp ${CMAKE_CURRENT_SOURCE_DIR}/matrix_mem.cpp
)
set(TGT_smbmgr_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/smb_mgr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/matrix_mem.cpp
)

set(TGT_smbmgr_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/include/access
    ${PROJECT_SRC_DIR}/include/storage
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(smbmgr_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND)
set(smbmgr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(smbmgr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(smbmgr_LINK_LIBS libelog.a libpgport.a -lpgport -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(smb_mgr TGT_smbmgr_SRC TGT_smbmgr_INC "${smbmgr_DEF_OPTIONS}" "${smbmgr_COMPILE_OPTIONS}" "${smbmgr_LINK_OPTIONS}" "${smbmgr_LINK_LIBS}")
add_dependencies(smb_mgr elog_static pgport_static)
target_link_directories(smb_mgr PUBLIC 
    ${LIBOPENSSL_LIB_PATH} ${LIBEDIT_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib 
    ${PROJECT_SRC_DIR}/include/storage /usr/local/lib
)
target_link_options(smb_mgr PRIVATE
    "-Wl,-rpath=/usr/local/softbus/ctrlbus/lib"
)
install(TARGETS smb_mgr RUNTIME DESTINATION bin)