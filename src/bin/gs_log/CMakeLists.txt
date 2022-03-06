#This is the main CMAKE for build all components.
# gs_log bin
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/bin/initdb/findtimezone.cpp ${CMAKE_CURRENT_SOURCE_DIR}/findtimezone.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/timezone/localtime.cpp ${CMAKE_CURRENT_SOURCE_DIR}/localtime.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/timezone/strftime.cpp ${CMAKE_CURRENT_SOURCE_DIR}/strftime.cpp
)
set(TGT_log_SRC ${CMAKE_CURRENT_SOURCE_DIR}/findtimezone.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_log_dump.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/localtime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strftime.cpp
)
set(TGT_log_INC
    ${PROJECT_SRC_DIR}/common/timezone
    ${PROJECT_SRC_DIR}/bin/gs_log
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include 
    ${LIBCGROUP_INCLUDE_PATH}
)
set(log_DEF_OPTIONS ${MACRO_OPTIONS})
set(log_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(log_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(log_LINK_LIBS libelog.a libpgport.a -lcrypt -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(gs_log TGT_log_SRC TGT_log_INC "${log_DEF_OPTIONS}" "${log_COMPILE_OPTIONS}" "${log_LINK_OPTIONS}" "${log_LINK_LIBS}")
add_dependencies(gs_log elog_static pgport_static)
target_link_directories(gs_log PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
    ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH} ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH}
    ${LIBCGROUP_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${KERBEROS_LIB_PATH} ${SECURE_LIB_PATH}
)
install(TARGETS gs_log RUNTIME DESTINATION bin)

