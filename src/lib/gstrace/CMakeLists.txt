#This is the main CMAKE for build all components.
execute_process(
    COMMAND python ./script/trc_gen.py -s ./config/ -o ${CMAKE_CURRENT_SOURCE_DIR}/../../include/gstrace
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/tool TGT_trace_SRC)

set(TGT_trace_INC 
    ${ZLIB_INCLUDE_PATH} 
    ${PROJECT_SRC_DIR}/lib/gstrace/tool 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_SRC_DIR}/distribute/include
)

set(trace_DEF_OPTIONS ${MACRO_OPTIONS})
set(trace_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(trace_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(trace_LINK_LIBS libgstrace.a -lcgroup libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz)
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND trace_LINK_LIBS -lgssapi_krb5_gauss -lkrb5_gauss -lk5crypto_gauss -lcom_err_gauss -lkrb5support_gauss)
endif()
add_bintarget(gstrace TGT_trace_SRC TGT_trace_INC "${trace_DEF_OPTIONS}" "${trace_COMPILE_OPTIONS}" "${trace_LINK_OPTIONS}" "${trace_LINK_LIBS}")
add_dependencies(gstrace gstrace_static pgport_static)
target_link_directories(gstrace PUBLIC
    ${LIBCGROUP_LIB_PATH} ${LIBOPENSSL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBEDIT_LIB_PATH} 
    ${SECURE_LIB_PATH} ${LIBHOTPATCH_LIB_PATH} ${LIBODBC_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${LIBCURL_LIB_PATH} ${JEMALLOC_LIB_PATH} ${PROJECT_SRC_DIR}/gstrace/common
)
install(TARGETS gstrace RUNTIME DESTINATION bin)

# gstrace.a
set(TGT_gstrace_SRC ${CMAKE_CURRENT_SOURCE_DIR}/common/gstrace_infra.cpp)

set(gstrace_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_UNSETENV)
list(REMOVE_ITEM gstrace_DEF_OPTIONS -D_GNU_SOURCE)
set(gstrace_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} -ftls-model=initial-exec ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gstrace_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_static_libtarget(gstrace TGT_gstrace_SRC "" "${gstrace_DEF_OPTIONS}" "${gstrace_COMPILE_OPTIONS}")
add_dependencies(gstrace_static pgport_static)
add_custom_command(TARGET gstrace_static POST_BUILD 
    COMMAND /usr/bin/ar qs ${CMAKE_BINARY_DIR}/lib/libgstrace.a ${CMAKE_BINARY_DIR}/lib/libpgport.a
)
add_static_objtarget(lib_gstrace_common TGT_gstrace_SRC "" "${gstrace_DEF_OPTIONS}" "${gstrace_COMPILE_OPTIONS}" "${gstrace_LINK_OPTIONS}")

