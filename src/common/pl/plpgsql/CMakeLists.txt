#This is the main CMAKE for build all components.
# libplpgsql.a
execute_process(
    COMMAND bison -d -o pl_gram.cpp gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_gram.cpp
    COMMAND perl ${PROJECT_SRC_DIR}/mtlocal.pl ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_gram.hpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
    OUTPUT_VARIABLE PL_GRAM
)

execute_process(
    COMMAND perl generate-plerrcodes.pl ${PROJECT_SRC_DIR}/common/backend/utils/errcodes.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
    OUTPUT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/plerrcodes.h 
)

execute_process(
    COMMAND perl -I ${PROJECT_SRC_DIR}/tools/ ${PROJECT_SRC_DIR}/tools/gen_keywordlist.pl --varname ReservedPLKeywords ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_reserved_kwlist.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
)

execute_process(
    COMMAND perl -I ${PROJECT_SRC_DIR}/tools/ ${PROJECT_SRC_DIR}/tools/gen_keywordlist.pl --varname UnreservedPLKeywords ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_unreserved_kwlist.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set(TGT_plpgsql_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_gram.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_comp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_exec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_funcs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_handler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_debugger.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_debugger_api.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/plsql_packages.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_scanner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_sql_validator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_package.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pl_global_package_runtime_cache.cpp
)

set(plpgsql_INC 
    ${PROJECT_SRC_DIR}/common/backend
)

set(plpgsql_DEF_OPTIONS ${MACRO_OPTIONS})
set(plpgsql_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(plpgsql TGT_plpgsql_SRC plpgsql_INC "${plpgsql_DEF_OPTIONS}" "${plpgsql_COMPILE_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/plpgsql--1.0.sql
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/plpgsql.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/plpgsql--unpackaged--1.0.sql
    DESTINATION share/postgresql/extension/
)
