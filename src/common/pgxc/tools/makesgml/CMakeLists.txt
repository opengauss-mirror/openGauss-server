#This is the main CMAKE for build bin.
set(TGT_makesgml_SRC ${CMAKE_CURRENT_SOURCE_DIR}/makesgml.cpp) 
set(TGT_makesgml_INC 
    ${PROJECT_SRC_DIR}/common/pgxc/tools/makesgml 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
)

set(makesgml_DEF_OPTIONS ${MACRO_OPTIONS})
set(makesgml_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(makesgml_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(makesgml_LINK_LIBS "")

if(${ENABLE_MEMORY_CHECK} STREQUAL "ON")
    set(makesgml_LINK_LIBS ${makesgml_LINK_LIBS} -lpthread -ldl -lm -lrt)
elseif(${ENABLE_THREAD_CHECK} STREQUAL "ON")
    set(makesgml_LINK_LIBS ${makesgml_LINK_LIBS} -lpthread -ldl -lm -lrt)
endif()
add_bintarget(makesgml TGT_makesgml_SRC TGT_makesgml_INC "${makesgml_DEF_OPTIONS}" "${makesgml_COMPILE_OPTIONS}" "${makesgml_LINK_OPTIONS}" "${makesgml_LINK_LIBS}")
target_link_directories(makesgml PUBLIC
    ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
#1.memcheck版本makesgml生成文件sgml耗时15分钟
#2.编译生成临时makesgml_mem,去掉memcheck插桩
#3.生成的sgml文件保持一致,不影响sql_help.*文件的生成
if(${ENABLE_MEMORY_CHECK} STREQUAL "ON")
    add_executable(makesgml_mem ${TGT_makesgml_SRC})
    target_compile_definitions(makesgml_mem PRIVATE ${makesgml_DEF_OPTIONS})
    target_include_directories(makesgml_mem PRIVATE ${TGT_makesgml_INC})
    target_compile_options(makesgml_mem PRIVATE ${makesgml_COMPILE_OPTIONS})
    target_link_libraries(makesgml_mem PRIVATE ${makesgml_LINK_LIBS})
    target_link_options(makesgml_mem PRIVATE ${makesgml_LINK_OPTIONS})
    set(makesgml_tool makesgml_mem)
    add_dependencies(makesgml makesgml_mem)
else()
    set(makesgml_tool makesgml)
endif()

file(GLOB ref_sgmlin_files ${PROJECT_OPENGS_DIR}/doc/src/sgml/ref/*.sgmlin)
foreach(sgmlin_file ${ref_sgmlin_files})
    string(REGEX MATCH "ref/.*" sgml_file_tmp1 ${sgmlin_file})
    string(REPLACE "ref/" "" sgml_file_tmp2 ${sgml_file_tmp1})
    string(REPLACE ".sgmlin" "" sgml_file ${sgml_file_tmp2})
    add_custom_COMMAND(TARGET ${makesgml_tool} POST_BUILD
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${makesgml_tool} -i ${sgmlin_file} -o ${sgml_file}.sgml -I XC -I EN -E PG -E JP
        WORKING_DIRECTORY ${PROJECT_OPENGS_DIR}/doc/src/sgml/ref)
endforeach()

install(TARGETS makesgml RUNTIME DESTINATION bin)
