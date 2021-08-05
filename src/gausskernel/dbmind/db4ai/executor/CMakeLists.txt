# executor.cmake
set(TGT_executor_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/distance_functions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fp_ops.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hyperparameter_validation.cpp
)    
set(TGT_executor_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
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
set(executor_DEF_OPTIONS ${MACRO_OPTIONS})
set(executor_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${SECURE_OPTIONS} ${CHECK_OPTIONS})
list(REMOVE_ITEM executor_COMPILE_OPTIONS -fPIC)
if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(executor_COMPILE_OPTIONS ${executor_COMPILE_OPTIONS} -std=c++14 -fPIE -mavx)
else()
    set(executor_COMPILE_OPTIONS ${executor_COMPILE_OPTIONS} -std=c++14 -fPIE)
endif()
set(executor_LINK_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor TGT_executor_SRC TGT_executor_INC "${executor_DEF_OPTIONS}" "${executor_COMPILE_OPTIONS}" "${executor_LINK_OPTIONS}")

add_subdirectory(gd)
add_subdirectory(kmeans)
