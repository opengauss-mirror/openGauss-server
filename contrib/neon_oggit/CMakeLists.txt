#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/neon_oggit TGT_neon_oggit_SRC)
set(neon_oggit_DEF_OPTIONS ${MACRO_OPTIONS})
set(neon_oggit_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(neon_oggit_LINK_OPTIONS ${LIB_LINK_OPTIONS})

add_shared_libtarget(neon_oggit TGT_neon_oggit_SRC "" "${neon_oggit_DEF_OPTIONS}" "${neon_oggit_COMPILE_OPTIONS}" "${neon_oggit_LINK_OPTIONS}")
set_target_properties(neon_oggit PROPERTIES PREFIX "")
install(TARGETS neon_oggit LIBRARY DESTINATION lib/postgresql)
