#This is the main CMAKE for build all components.
# libcmpq.a
execute_process(
    COMMAND perl generate-cm-errcodes.pl cm_errcodes.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_FILE cm_errcodes.h
)
execute_process(COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/cm_errcodes.h ${PROJECT_SRC_DIR}/include/cm/cm_errcodes.h)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_cmcommon_SRC)

set(TGT_cmcommon_INC 
    ${PROJECT_SRC_DIR}/include/cm 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${PROJECT_TRUNK_DIR}/distribute/cm/cm_etcdapi 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${LIBCGROUP_INCLUDE_PATH}
)

set(cmcommon_DEF_OPTIONS ${MACRO_OPTIONS})
set(cmcommon_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-strong)
list(REMOVE_ITEM cmcommon_COMPILE_OPTIONS -fstack-protector)
set(cmcommon_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM cmcommon_LINK_OPTIONS -pthread)
add_shared_libtarget(cmcommon TGT_cmcommon_SRC TGT_cmcommon_INC "" "${cmcommon_COMPILE_OPTIONS}" "${cmcommon_LINK_OPTIONS}")
target_link_libraries(cmcommon PRIVATE -lcgroup)
target_link_directories(cmcommon PUBLIC 
    ${LIBCGROUP_LIB_PATH}
)
set_target_properties(cmcommon PROPERTIES VERSION 2)
install(TARGETS cmcommon DESTINATION lib)

add_static_libtarget(cmcommon TGT_cmcommon_SRC TGT_cmcommon_INC "${cmcommon_DEF_OPTIONS}" "${cmcommon_COMPILE_OPTIONS}")
install(TARGETS cmcommon_static ARCHIVE DESTINATION lib)

