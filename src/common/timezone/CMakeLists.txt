#This is the main CMAKE for build all components.
set(TGT_zic_SRC 
    ${PROJECT_SRC_DIR}/common/timezone/zic.cpp
    ${PROJECT_SRC_DIR}/common/timezone/ialloc.cpp
    ${PROJECT_SRC_DIR}/common/timezone/scheck.cpp
    ${PROJECT_SRC_DIR}/common/timezone/localtime.cpp
)
set(TGT_zic_INC
    ${PROJECT_SRC_DIR}/common/timezone 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(zic_DEF_OPTIONS ${MACRO_OPTIONS})
set(zic_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(zic_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(zic_LINK_LIBS libpgport.a -ldl -lm -lssl -lcrypto -l${SECURE_C_CHECK} -pthread -lrt -lz -lminiunz)
add_bintarget(zic TGT_zic_SRC TGT_zic_INC "${zic_DEF_OPTIONS}" "${zic_COMPILE_OPTIONS}" "${zic_LINK_OPTIONS}" "${zic_LINK_LIBS}")
add_dependencies(zic pgport_static)
target_link_directories(zic PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${LIBCURL_LIB_PATH} ${KERBEROS_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
add_custom_command(TARGET zic POST_BUILD
    COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --export_ldpath ${LIBOPENSSL_LIB_PATH}
    COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --build_timezone ${PROJECT_SRC_DIR}/common/timezone/data ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${LIBOPENSSL_LIB_PATH}
)

# install timezone
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/timezone DESTINATION share/postgresql)

list(APPEND TGT_timezone_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/localtime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/strftime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgtz.cpp
)

set(TGT_timezone_INC 
    ${PROJECT_SRC_DIR}/common/timezone
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(timezone_DEF_OPTIONS ${MACRO_OPTIONS})
set(timezone_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(timezone_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_timezone TGT_timezone_SRC TGT_timezone_INC "${timezone_DEF_OPTIONS}" "${timezone_COMPILE_OPTIONS}" "${timezone_LINK_OPTIONS}")

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tznames/
    DESTINATION share/postgresql/timezonesets/
)

