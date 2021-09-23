#This is the main CMAKE for build all components.
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
execute_process(
    COMMAND perl Gen_fmgrtab.pl ${PROJECT_SRC_DIR}/common/backend/catalog/builtin_funcs.ini 
            ${PROJECT_TRUNK_DIR}/distribute/kernel/catalog/distribute_builtin_funcs.ini
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
else()
execute_process(
    COMMAND perl Gen_fmgrtab.pl ${PROJECT_SRC_DIR}/common/backend/catalog/builtin_funcs.ini
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
endif()

execute_process(
    COMMAND perl generate-errcodes.pl ${PROJECT_SRC_DIR}/common/backend/utils/errcodes.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/errcodes.h
)

execute_process(
    COMMAND sed -f Gen_dummy_probes.sed probes.d
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/probes.h
)

#if("${ENABLE_DTRACE}" STREQUAL "ON")
#    SET(sed_cmd_src_probes "${PROJECT_SRC_DIR}/common/backend/utils| -C -h -s -o probes.d probes.h.tmp||sed -e 's/POSTGRESQL_/TRACE_POSTGRESQL_/g' probes.h.tmp >probes.h|rm -rf probes.h.tmp")
#else()
#    SET(sed_cmd_src_probes "${PROJECT_SRC_DIR}/common/backend/utils|||sed -f Gen_dummy_probes.sed probes.d >probes.h|")
#endif()

execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/errcodes.h ${PROJECT_SRC_DIR}/include/utils/errcodes.h
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/probes.h ${PROJECT_SRC_DIR}/include/utils/probes.h
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/fmgroids.h ${PROJECT_SRC_DIR}/include/utils/fmgroids.h
)

set(TGT_generate_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/fmgrtab.cpp
)

set(TGT_generate_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(generate_DEF_OPTIONS ${MACRO_OPTIONS})
set(generate_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(generate_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils TGT_generate_SRC TGT_generate_INC "${generate_DEF_OPTIONS}" "${generate_COMPILE_OPTIONS}" "${generate_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/adt
    ${CMAKE_CURRENT_SOURCE_DIR}/cache
    ${CMAKE_CURRENT_SOURCE_DIR}/error
    ${CMAKE_CURRENT_SOURCE_DIR}/fmgr
    ${CMAKE_CURRENT_SOURCE_DIR}/hash
    ${CMAKE_CURRENT_SOURCE_DIR}/init
    ${CMAKE_CURRENT_SOURCE_DIR}/mmgr
    ${CMAKE_CURRENT_SOURCE_DIR}/resowner
    ${CMAKE_CURRENT_SOURCE_DIR}/time
    ${CMAKE_CURRENT_SOURCE_DIR}/mb
    ${CMAKE_CURRENT_SOURCE_DIR}/misc
    ${CMAKE_CURRENT_SOURCE_DIR}/sort
)

add_subdirectory(adt)
add_subdirectory(cache)
add_subdirectory(error)
add_subdirectory(fmgr)
add_subdirectory(hash)
add_subdirectory(init)
add_subdirectory(mmgr)
add_subdirectory(resowner)
add_subdirectory(time)
add_subdirectory(mb)
add_subdirectory(misc)
add_subdirectory(sort)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/errcodes.h DESTINATION include/postgresql/server/utils)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/fmgroids.h DESTINATION include/postgresql/server/utils)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/probes.h DESTINATION include/postgresql/server/utils)
