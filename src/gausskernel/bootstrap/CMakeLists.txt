#This is the main CMAKE for build object.
#add_custom_command(
#    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/bootscanner.inc 
#    COMMAND flex -o'bootscanner.inc' bootscanner.l
#    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl bootscanner.inc
#    COMMAND sed -i "s/YY_NULL/YY_ZERO/g" bootscanner.inc
#    DEPENDS bootscanner.l ${PROJECT_SRC_DIR}/mtlocal.pl 
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#    COMMENT "Now Generating bootscanner.inc"
#)

set(bootstrap_cmd_src
"${PROJECT_SRC_DIR}/gausskernel/bootstrap||perl ${PROJECT_SRC_DIR}/mtlocal.pl bootscanner.inc|flex -o'bootscanner.inc' bootscanner.l|sed -i 's/YY_NULL/YY_ZERO/g' bootscanner.inc"
)

add_cmd_gen_when_configure(flex_target bootstrap_cmd_src)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/bootparse.cpp 
    COMMAND bison -o bootparse.cpp bootparse.y
    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl bootparse.cpp 
    DEPENDS bootparse.y ${PROJECT_SRC_DIR}/mtlocal.pl bootscanner.inc
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Now Generating bootparse.cpp"
)

set (TGT_bootstrap_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/bootstrap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bootparse.cpp
)

set(TGT_bootstrap_INC 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(bootstrap_DEF_OPTIONS ${MACRO_OPTIONS})
set(bootstrap_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(bootstrap_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_bootstrap TGT_bootstrap_SRC TGT_bootstrap_INC "${bootstrap_DEF_OPTIONS}" "${bootstrap_COMPILE_OPTIONS}" "${bootstrap_LINK_OPTIONS}")

