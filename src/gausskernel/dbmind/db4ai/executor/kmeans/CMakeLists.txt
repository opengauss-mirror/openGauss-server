# kmeans.cmake
set(TGT_kmeans_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/kmeans.cpp
)
set(TGT_kmeans_INC
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/kmeanss
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
set(kmeans_DEF_OPTIONS ${MACRO_OPTIONS})
set(kmeans_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(kmeans_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor_kmeans TGT_kmeans_SRC TGT_kmeans_INC "${kmeans_DEF_OPTIONS}" "${kmeans_COMPILE_OPTIONS}" "${kmeans_LINK_OPTIONS}")
