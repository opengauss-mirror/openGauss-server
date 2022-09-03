# gd.cmake
set(TGT_gd_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/gd.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_pca.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_ova.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_gd.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_ngd.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predict.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/shuffle_cache.cpp
)
set(TGT_gd_INC
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)
set(gd_DEF_OPTIONS ${MACRO_OPTIONS})
set(gd_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gd_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor_optimize_gd TGT_gd_SRC TGT_gd_INC "${gd_DEF_OPTIONS}" "${gd_COMPILE_OPTIONS}" "${gd_LINK_OPTIONS}")

