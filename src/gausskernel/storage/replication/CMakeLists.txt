#This is the main CMAKE for build bin.
#execute_process(
#    COMMAND rm -fr lex.backup
#    COMMAND flex -CF -b -p -i -o'repl_scanner.inc' repl_scanner.l
#    COMMAND sed -i "s/YY_NULL/YY_ZERO/g" repl_scanner.inc 
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#    OUTPUT_VARIABLE REPL_SCANNER  
#)

#execute_process(
#    COMMAND rm -fr lex.backup
#    COMMAND flex -CF -b -p -i -o'syncrep_scanner.inc' syncrep_scanner.l
#    COMMAND sed -i "s/YY_NULL/YY_ZERO/g" syncrep_scanner.inc
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#    OUTPUT_VARIABLE SYNCREP_SCANNER 
#)
execute_process(
    COMMAND bison -d -o repl_gram.cpp repl_gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE REPL_GRAM
)

execute_process(
    COMMAND bison -d -o syncrep_gram.cpp syncrep_gram.y
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE SYNCREP_GRAM
)

set(replication_cmd_src
"${PROJECT_SRC_DIR}/gausskernel/storage/replication|rm -fr lex.backup||flex -CF -b -p -i -o'repl_scanner.inc' repl_scanner.l|sed -i 's/YY_NULL/YY_ZERO/g' repl_scanner.inc"
"${PROJECT_SRC_DIR}/gausskernel/storage/replication|rm -fr lex.backup||flex -CF -b -p -i -o'syncrep_scanner.inc' syncrep_scanner.l|sed -i 's/YY_NULL/YY_ZERO/g' syncrep_scanner.inc"
)

add_cmd_gen_when_configure(flex_target replication_cmd_src)

execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/repl_gram.hpp ${PROJECT_SRC_DIR}/include/replication/repl_gram.hpp
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/syncrep_gram.hpp ${PROJECT_SRC_DIR}/include/replication/syncrep_gram.hpp
)

set(TGT_replication_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/basebackup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bcm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/catchup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dataqueue.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/datarcvwriter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/datareceiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/datasender.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/datasyncrep.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/heartbeat.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libpqwalreceiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/archive_walreceiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rto_statistic.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/slot.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/slotfuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/syncrep.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/walrcvwriter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/walreceiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/walreceiverfuncs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/walsender.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/repl_gram.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/syncrep_gram.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/shared_storage_walreceiver.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/subscription_walreceiver.cpp
)

set(TGT_replication_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
)

set(replication_DEF_OPTIONS ${MACRO_OPTIONS})
set(replication_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(replication_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_replication TGT_replication_SRC TGT_replication_INC "${replication_DEF_OPTIONS}" "${replication_COMPILE_OPTIONS}" "${replication_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/heartbeat
    ${CMAKE_CURRENT_SOURCE_DIR}/dcf
    ${CMAKE_CURRENT_SOURCE_DIR}/logical
    ${CMAKE_CURRENT_SOURCE_DIR}/pgoutput
)

add_subdirectory(heartbeat)
add_subdirectory(dcf)
add_subdirectory(logical)
add_subdirectory(pgoutput)

