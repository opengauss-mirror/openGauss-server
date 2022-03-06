#This is the main CMAKE for build all pg_xlogdump.
# pg_xlogdump
execute_process(
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/barrierdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/barrierdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/clogdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/clogdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/dbasedesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dbasedesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/gindesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gindesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/gistdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/gistdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/hashdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hashdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/heapdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/heapdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/motdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/motdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/mxactdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/mxactdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/nbtdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/nbtdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/relmapdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/relmapdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/seqdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/seqdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/slotdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/slotdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/smgrdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/smgrdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/spgdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/spgdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/standbydesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/standbydesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/tblspcdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/tblspcdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/xactdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xactdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/xlogdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/redo/xlogreader_common.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader_common.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/segpagedesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/segpagedesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/transam/xlogreader.cpp ${CMAKE_CURRENT_SOURCE_DIR}/xlogreader.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/uheapdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/uheapdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/undologdesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/undologdesc.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/gausskernel/storage/access/rmgrdesc/replorigindesc.cpp ${CMAKE_CURRENT_SOURCE_DIR}/replorigindesc.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_xlogdump_SRC)
SET(TGT_xlogdump_INC
    ${TGT_pq_INC} ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SRC_DIR}/lib/gstrace
)
SET(xlogdump_DEF_OPTIONS ${MACRO_OPTIONS} -DFRONTEND)
SET(xlogdump_COMPILE_OPTIONS ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS} ${OPTIMIZE_OPTIONS})
SET(xlogdump_LINK_OPTIONS ${BIN_LINK_OPTIONS})
SET(xlogdump_LINK_LIBS libpgcommon.a -lpgport -lcrypt -ldl -lm -ledit -lssl -lcrypto -l${SECURE_C_CHECK} -lrt -lz -lminiunz)
add_bintarget(pg_xlogdump TGT_xlogdump_SRC TGT_xlogdump_INC "${xlogdump_DEF_OPTIONS}" "${xlogdump_COMPILE_OPTIONS}" "${xlogdump_LINK_OPTIONS}" "${xlogdump_LINK_LIBS}")
add_dependencies(pg_xlogdump pgport_static pgcommon_static)
target_link_directories(pg_xlogdump PUBLIC
    ${LIBOPENSSL_LIB_PATH} ${PROTOBUF_LIB_PATH} ${LIBPARQUET_LIB_PATH} ${LIBCURL_LIB_PATH} ${SECURE_LIB_PATH}
    ${ZLIB_LIB_PATH} ${LIBOBS_LIB_PATH} ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)

install(TARGETS pg_xlogdump RUNTIME DESTINATION bin)