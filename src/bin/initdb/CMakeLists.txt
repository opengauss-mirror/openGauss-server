#This is the main CMAKE for build all components.
# gs_initdb bin

execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/backend/utils/mb/encnames.cpp ${CMAKE_CURRENT_SOURCE_DIR}/encnames.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/timezone/localtime.cpp ${CMAKE_CURRENT_SOURCE_DIR}/localtime.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/interfaces/libpq/pqsignal.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pqsignal.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/dss/dss_adaptor.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
)

set(TGT_initdb_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/encnames.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/findtimezone.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/initdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/localtime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pqsignal.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ss_initdb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dss_adaptor.cpp
)

set(TGT_initdb_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/initdb 
    ${PROJECT_SRC_DIR}/common/timezone 
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(initdb_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND)
set(initdb_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(initdb_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(initdb_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(gs_initdb TGT_initdb_SRC TGT_initdb_INC "${initdb_DEF_OPTIONS}" "${initdb_COMPILE_OPTIONS}" "${initdb_LINK_OPTIONS}" "${initdb_LINK_LIBS}")
add_dependencies(gs_initdb elog_static pgport_static)
target_link_directories(gs_initdb PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)

install(TARGETS gs_initdb RUNTIME DESTINATION bin)

