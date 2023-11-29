#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gs_uwal_SRC)

set(TGT_sync_INC
    ${PROJECT_SRC_DIR}/include/storage/gs_uwal
)

set(gs_uwal_DEF_OPTIONS ${MACRO_OPTIONS})
set(gs_uwal_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} "-fPIC")
set(gs_uwal_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_gs_uwal TGT_gs_uwal_SRC TGT_sync_INC "${gs_uwal_DEF_OPTIONS}" "${gs_uwal_COMPILE_OPTIONS}" "${gs_uwal_LINK_OPTIONS}")
