#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/mppdb_decoding TGT_mppdb_decoding_SRC)
set(mppdb_decoding_DEF_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0 -DSTREAMPLAN -DPGXC -DENABLE_GSTRACE -D_GNU_SOURCE)
set(mppdb_decoding_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
list(REMOVE_ITEM mppdb_decoding_COMPILE_OPTIONS -fstack-protector)
set(mppdb_decoding_LINK_OPTIONS ${LIB_LINK_OPTIONS})

add_shared_libtarget(mppdb_decoding TGT_mppdb_decoding_SRC "" "${mppdb_decoding_DEF_OPTIONS}" "${mppdb_decoding_COMPILE_OPTIONS}" "${mppdb_decoding_LINK_OPTIONS}")
set_target_properties(mppdb_decoding PROPERTIES PREFIX "")
install(TARGETS mppdb_decoding LIBRARY DESTINATION lib/postgresql)

