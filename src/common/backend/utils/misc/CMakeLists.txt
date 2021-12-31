#This is the main CMAKE for build all components.
#add_custom_command(
#    OUTPUT guc-file.inc guc.cpp
#    COMMAND flex -CF -b -p -o'guc-file.inc' guc-file.l
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/guc-file.l
#    COMMENT "Now Generating guc-file.inc"
#)
set(misc_cmd_src
"${PROJECT_SRC_DIR}/common/backend/utils/misc|||flex -o'guc-file.inc' guc-file.l|"
)

add_cmd_gen_when_configure(flex_target misc_cmd_src)

set(TGT_misc_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/anls_opt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/elf_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/guc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/help_config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_controldata.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_rusage.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ps_status.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rbtree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sec_rls_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/superuser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tzparser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/oidrbtree.cpp
)

set(TGT_misc_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
)

set(misc_DEF_OPTIONS ${MACRO_OPTIONS} -DPG_KRB_SRVTAB=\"FILE:${prefix_home}/etc/postgresql/krb5.keytab\")
set(misc_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(misc_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_misc TGT_misc_SRC TGT_misc_INC "${misc_DEF_OPTIONS}" "${misc_COMPILE_OPTIONS}" "${misc_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/common/backend/utils/misc/guc
)

add_subdirectory(guc)

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgresql_distribute.conf.sample
    DESTINATION share/postgresql/ RENAME postgresql.conf.sample
)
else()
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgresql_single.conf.sample
    DESTINATION share/postgresql/ RENAME postgresql.conf.sample
)
endif()

