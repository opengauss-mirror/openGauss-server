set(BEPARSERDIR "src/backend_parser")
set(BASECODEDIR "src")
set(PLDIR "src/pltsql")

execute_process(
    COMMAND perl ${BEPARSERDIR}/include.pl ${BEPARSERDIR}/ gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    INPUT_FILE ${PROJECT_SRC_DIR}/common/backend/parser/gram.y
    OUTPUT_FILE ${BEPARSERDIR}/gram-backend.y
)

execute_process(
    COMMAND bison -d -o ${BEPARSERDIR}/gram-backend.cpp ${BEPARSERDIR}/gram-backend.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PARSER_GRAM
)

execute_process(
    COMMAND sed -i "s/YY_NULL nullptr/YY_NULL 0/g" ${BEPARSERDIR}/gram-backend.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PARSER_GRAM
)

execute_process(
    COMMAND sed -i "s/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g" ${BEPARSERDIR}/gram-backend.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PARSER_GRAM
)

execute_process(
    COMMAND perl -I ${PROJECT_SRC_DIR}/tools/ ${PROJECT_SRC_DIR}/tools/gen_keywordlist.pl --extern ${BEPARSERDIR}/kwlist.h --varname pgtsql_ScanKeywords --output ${BEPARSERDIR}/
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

execute_process(
    COMMAND perl ${BEPARSERDIR}/include.pl ${BEPARSERDIR}/ ${PROJECT_SRC_DIR}/src/common/backend/parser/scan.l
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    INPUT_FILE ${PROJECT_SRC_DIR}/common/backend/parser/scan.l
    OUTPUT_FILE ${BEPARSERDIR}/scan-backend.l
)

execute_process(
    COMMAND bison -d -o ${PLDIR}/pl_gram.cpp ${PLDIR}/gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl ${PLDIR}/pl_gram.cpp
    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl ${PLDIR}/pl_gram.hpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND sed -i "s/plpgsql_yyparse/pltsql_yyparse/g" ${PLDIR}/pl_gram.cpp
    COMMAND sed -i "s/plpgsql_yyparse/pltsql_yyparse/g" ${PLDIR}/pl_gram.hpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND sed -i "s/plpgsql_yylex/pltsql_yylex/g" ${PLDIR}/pl_gram.cpp
    COMMAND sed -i "s/plpgsql_yylex/pltsql_yylex/g" ${PLDIR}/pl_gram.hpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND sed -i "s/plpgsql_yylex_single/pltsql_yylex_single/g" ${PLDIR}/pl_gram.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND sed -i "s/\# define YYINITDEPTH .*/\# define YYINITDEPTH 1000/g" ${PLDIR}/pl_gram.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND perl ${PROJECT_SRC_DIR}/common/pl/plpgsql/src/generate-plerrcodes.pl ${PROJECT_SRC_DIR}/common/backend/utils/errcodes.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/plerrcodes.h
)

execute_process(
    COMMAND perl -I ${PROJECT_SRC_DIR}/tools/ ${PROJECT_SRC_DIR}/tools/gen_keywordlist.pl --varname ReservedPLKeywords ${PLDIR}/pl_reserved_kwlist.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

execute_process(
    COMMAND perl -I ${PROJECT_SRC_DIR}/tools/ ${PROJECT_SRC_DIR}/tools/gen_keywordlist.pl --varname UnreservedPLKeywords ${PLDIR}/pl_unreserved_kwlist.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set(shark_parser_cmd_src
"${PROJECT_SRC_DIR}/../contrib/shark/${BEPARSERDIR}|rm -fr lex.backup||flex -CF -b -p -p -o'scan-backend.inc' scan-backend.l|sed -i 's/YY_NULL/YY_ZERO/g' scan-backend.inc"
)
add_cmd_gen_when_configure(flex_target shark_parser_cmd_src)

set(TGT_shark_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/shark.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dbcc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/varlena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlvariant.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/datefuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BEPARSERDIR}/gram-backend.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BEPARSERDIR}/keywords.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BEPARSERDIR}/varbinary.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BEPARSERDIR}/parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/pl_gram.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/pl_handler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/pl_comp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/pl_scanner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BASECODEDIR}/tablecmds.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${BASECODEDIR}/identitycmds.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLDIR}/pl_funcs.cpp
)
set(TGT_shark_INC 
    ${PROJECT_OPENGS_DIR}/contrib/shark
    ${PROJECT_OPENGS_DIR}/contrib/shark/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
)

set(shark_DEF_OPTIONS ${MACRO_OPTIONS})
set(shark_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(shark_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(shark TGT_shark_SRC TGT_shark_INC "${shark_DEF_OPTIONS}" "${shark_COMPILE_OPTIONS}" "${shark_LINK_OPTIONS}")
include_directories(
    ${PROJECT_OPENGS_DIR}/contrib/shark
    ${PROJECT_OPENGS_DIR}/contrib/shark/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
)
set_target_properties(shark PROPERTIES PREFIX "")

execute_process(
    COMMAND bash cmake.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/shark.control
    DESTINATION share/postgresql/extension/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/shark--2.0.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/upgrade_script/shark--1.0--2.0.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/rollback_script/shark--2.0--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(TARGETS shark DESTINATION lib/postgresql)
