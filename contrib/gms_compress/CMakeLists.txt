#This is the main CMAKE for build all gms_compress.
# gms_compress
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_compress_SRC)
set(TGT_gms_compress_INC 
    ${PROJECT_OPENGS_DIR}/contrib/gms_compress
    ${PROJECT_OPENGS_DIR}/contrib
)

set(gms_compress_DEF_OPTIONS ${MACRO_OPTIONS})
set(gms_compress_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_compress_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gms_compress TGT_gms_compress_SRC TGT_gms_compress_INC "${gms_compress_DEF_OPTIONS}" "${gms_compress_COMPILE_OPTIONS}" "${gms_compress_LINK_OPTIONS}")
set_target_properties(gms_compress PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_compress.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_compress--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS gms_compress DESTINATION lib/postgresql)
