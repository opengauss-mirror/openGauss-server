# Check VECTOR_HOME before building gv_index
if(NOT "$ENV{VECTOR_HOME}" STREQUAL "")
    set(VECTOR_HOME $ENV{VECTOR_HOME})
else()
    set(VECTOR_HOME "")
endif()
message(STATUS "VECTOR_HOME is ${VECTOR_HOME}")
if(NOT EXISTS "${VECTOR_HOME}" OR NOT IS_DIRECTORY "${VECTOR_HOME}")
    message(STATUS "VECTOR_HOME not found: ${VECTOR_HOME}, skipped gv_index.")
    return()
endif()

# GaussVector-component 对接目录（变异产物）
set(GAUSSVECTOR_INSTALL_DIR ${VECTOR_HOME})
include(cmake/gv_npu_component.cmake)
gv_detect_npu_component("${GAUSSVECTOR_INSTALL_DIR}" GV_INDEX_NPU_AVAILABLE)

if(GV_INDEX_NPU_AVAILABLE)
    set(ASCEND_KERNELS_LIB "${GAUSSVECTOR_INSTALL_DIR}/lib/libascend_kernels.so")
endif()

# Resolve Annlite from the active component without reusing a stale CMake cache entry.
set(ANNLITE_LIB "${GAUSSVECTOR_INSTALL_DIR}/lib/libannlite.so")
if(NOT EXISTS "${ANNLITE_LIB}")
    message(FATAL_ERROR "libannlite.so not found in ${GAUSSVECTOR_INSTALL_DIR}/lib, please add it first")
endif()
message(STATUS "Found annlite: ${ANNLITE_LIB}")

set(TGT_gv_index_INC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/env
    ${GAUSSVECTOR_INSTALL_DIR}/include
)
set(gv_index_DEF_OPTIONS ${MACRO_OPTIONS} -DANN_LITE_BINARY_MODE=1)
if(GV_INDEX_NPU_AVAILABLE)
    list(APPEND gv_index_DEF_OPTIONS -DENABLE_NPU -DGV_INDEX_NPU_AVAILABLE)
endif()

set(gv_index_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS}
    -std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -fstack-protector-all)
set(gv_index_LINK_OPTIONS ${BIN_LINK_OPTIONS})

FILE(GLOB_RECURSE TGT_gv_index_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)
add_shared_libtarget(gv_index TGT_gv_index_SRC TGT_gv_index_INC "${gv_index_DEF_OPTIONS}" "${gv_index_COMPILE_OPTIONS}" "${gv_index_LINK_OPTIONS}")

# 链接 libannlite.so
target_link_libraries(gv_index PRIVATE ${ANNLITE_LIB})
if(GV_INDEX_NPU_AVAILABLE)
    target_link_libraries(gv_index PRIVATE ${ASCEND_KERNELS_LIB})
endif()

# Use the component in the build tree and the copied libraries after installation.
set_target_properties(gv_index PROPERTIES
    BUILD_RPATH "${GAUSSVECTOR_INSTALL_DIR}/lib"
    INSTALL_RPATH "\$ORIGIN/.."
)

install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/gv_index.control
    ${CMAKE_CURRENT_SOURCE_DIR}/gv_index--1.0.sql
    DESTINATION share/postgresql/extension/
    PERMISSIONS OWNER_WRITE OWNER_READ)
install(TARGETS gv_index LIBRARY DESTINATION lib/postgresql
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
)
# 安装 libannlite.so 到 install/lib 目录
install(FILES ${ANNLITE_LIB} DESTINATION lib
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
)
if(GV_INDEX_NPU_AVAILABLE)
    install(FILES ${ASCEND_KERNELS_LIB} DESTINATION lib
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    )
    install(FILES
        ${GAUSSVECTOR_INSTALL_DIR}/lib/lib_ascend_ai_core_a.so
        ${GAUSSVECTOR_INSTALL_DIR}/lib/lib_ascend_vector_core_v.so
        DESTINATION lib
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    )
endif()
