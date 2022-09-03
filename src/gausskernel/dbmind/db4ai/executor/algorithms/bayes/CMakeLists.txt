# bayes.cmake
set(TGT_bayes_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/bayes.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bayes_network_internal.cpp
)
set(TGT_bayes_INC
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/bayess
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)
set(bayes_DEF_OPTIONS ${MACRO_OPTIONS})
set(bayes_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(bayes_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor_algorithm_bayes TGT_bayes_SRC TGT_bayes_INC "${bayes_DEF_OPTIONS}" "${bayes_COMPILE_OPTIONS}" "${bayes_LINK_OPTIONS}")

