# executor.cmake
set(TGT_executor_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/distance_functions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fp_ops.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hyperparameter_validation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kernel.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/matrix.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/direct.cpp
)
set(TGT_executor_INC 
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
set(executor_DEF_OPTIONS ${MACRO_OPTIONS})
set(executor_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(executor_COMPILE_OPTIONS ${executor_COMPILE_OPTIONS} -mavx)
endif()
set(executor_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor TGT_executor_SRC TGT_executor_INC "${executor_DEF_OPTIONS}" "${executor_COMPILE_OPTIONS}" "${executor_LINK_OPTIONS}")

add_subdirectory(optimize)
add_subdirectory(algorithms)
