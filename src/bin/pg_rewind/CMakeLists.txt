#This is the main CMAKE for build all components.
# pg_rewind.bin
set(TGT_rewind_SRC ${CMAKE_CURRENT_SOURCE_DIR}/datapagemap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fetch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/filemap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/file_ops.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logging.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/parsexlog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_rewind.cpp
)

set(TGT_rewind_INC 
    ${PROJECT_SRC_DIR}/bin/pg_ctl 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${LIBOPENSSL_INCLUDE_PATH} 
    ${ZSTD_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/lib/page_compression
)

set(rewind_DEF_OPTIONS ${MACRO_OPTIONS})
set(rewind_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(pg_rewind TGT_rewind_SRC TGT_rewind_INC "${rewind_DEF_OPTIONS}" "${rewind_COMPILE_OPTIONS}")
