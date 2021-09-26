#This is the main CMAKE for build all components.
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/orc.cpp 
    COMMAND protoc-c --c_out=${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/orc.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/orc.proto
    COMMENT "Now Generating orc.cpp"
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_hdfs_fdw_SRC)

set(TGT_hdfs_fdw_INC 
    ${LIBORC_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_OPENGS_DIR}/contrib/hdfs_fdw/include
    ${PROJECT_OPENGS_DIR}/contrib
)

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
    list(APPEND TGT_hdfs_fdw_INC ${PROJECT_TRUNK_DIR}/contrib/carbondata)
endif()

set(hdfs_fdw_DEF_OPTIONS ${MACRO_OPTIONS})
set(hdfs_fdw_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(hdfs_fdw_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(contrib_hdfs_fdw TGT_hdfs_fdw_SRC TGT_hdfs_fdw_INC "${hdfs_fdw_DEF_OPTIONS}" "${hdfs_fdw_COMPILE_OPTIONS}" "${hdfs_fdw_LINK_OPTIONS}")
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
    add_dependencies(contrib_hdfs_fdw carbondata_static)
endif()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hdfs_fdw.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/hdfs_fdw--1.0.sql
    DESTINATION share/postgresql/extension/
)
