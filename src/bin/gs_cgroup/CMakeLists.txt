#This is the main CMAKE for build all components.
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/cbb/workload/gsconfig.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gsconfig.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/cbb/workload/gsutil.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gsutil.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_cgroup_SRC)
set(TGT_cgroup_INC
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/bin/gs_cgroup 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${SECURE_INCLUDE_PATH} 
    ${LIBCGROUP_INCLUDE_PATH}
)
set(cgroup_DEF_OPTIONS ${MACRO_OPTIONS})
set(cgroup_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(cgroup_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(cgroup_LINK_LIBS libelog.a -lpgport -lcrypt -ldl -lm -l${SECURE_C_CHECK} -pthread -lrt -lminiunz -lcgroup)
add_bintarget(gs_cgroup TGT_cgroup_SRC TGT_cgroup_INC "${cgroup_DEF_OPTIONS}" "${cgroup_COMPILE_OPTIONS}" "${cgroup_LINK_OPTIONS}" "${cgroup_LINK_LIBS}")
add_dependencies(gs_cgroup elog_static pgport_static)
target_link_directories(gs_cgroup PUBLIC
    ${PROJECT_SRC_DIR}/common/port ${PROJECT_SRC_DIR}/gstrace/common ${CMAKE_BINARY_DIR}/lib
    ${LIBCGROUP_LIB_PATH} ${ZLIB_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH}
)
install(TARGETS gs_cgroup RUNTIME DESTINATION bin)
