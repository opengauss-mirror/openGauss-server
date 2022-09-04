# algorithms.cmake
set(TGT_algorithms_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/kmeans.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/linregr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logregr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pca.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/svm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xgboost.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/multiclass.cpp
)
set(TGT_algorithms_INC
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/algorithmss
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${XGBOOST_INCLUDE_PATH}
)
set(algorithms_DEF_OPTIONS ${MACRO_OPTIONS})
set(algorithms_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(algorithms_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor_algorithms TGT_algorithms_SRC TGT_algorithms_INC "${algorithms_DEF_OPTIONS}" "${algorithms_COMPILE_OPTIONS}" "${algorithms_LINK_OPTIONS}")

add_subdirectory(bayes)
