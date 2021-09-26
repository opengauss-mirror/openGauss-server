#This is the main CMAKE for build bin.

set(isolation_cmd_src_bison
    "${PROJECT_SRC_DIR}/test/isolation|||bison -Wno-other -o specparse.cpp specparse.y|"
)
add_cmd_gen_when_configure(bison_target isolation_cmd_src_bison)

set(isolation_cmd_src_flex
    "${PROJECT_SRC_DIR}/test/isolation|||flex -o'specscanner.inc' specscanner.l|"
)
add_cmd_gen_when_configure(flex_target isolation_cmd_src_flex)


