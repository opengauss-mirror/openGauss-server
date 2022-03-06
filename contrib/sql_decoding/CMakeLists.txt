#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/sql_decoding TGT_sql_decoding_SRC)
set(sql_decoding_DEF_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0 -DSTREAMPLAN -DPGXC -DENABLE_GSTRACE -D_GNU_SOURCE)
set(sql_decoding_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
list(REMOVE_ITEM sql_decoding_COMPILE_OPTIONS -fstack-protector)
set(sql_decoding_LINK_OPTIONS ${LIB_LINK_OPTIONS})

add_shared_libtarget(sql_decoding TGT_sql_decoding_SRC "" "${sql_decoding_DEF_OPTIONS}" "${sql_decoding_COMPILE_OPTIONS}" "${sql_decoding_LINK_OPTIONS}")
set_target_properties(sql_decoding PROPERTIES PREFIX "")
install(TARGETS sql_decoding LIBRARY DESTINATION lib/postgresql)

