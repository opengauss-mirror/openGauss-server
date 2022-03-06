#This is the main CMAKE for build all components.
# gsqlerr bin
set(TGT_scanEreport_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/scanEreport.cpp
)
set(TGT_scanEreport_INC 
    ${PROJECT_SRC_DIR}/bin/gsqlerr 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq
)

set(scanEreport_DEF_OPTIONS ${MACRO_OPTIONS})
set(scanEreport_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(scanEreport_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(scanEreport_LINK_LIBS -l${SECURE_C_CHECK})
if("${ENABLE_MEMORY_CHECK}" STREQUAL "ON")
    set(scanEreport_LINK_LIBS ${scanEreport_LINK_LIBS} -pthread -ldl -lm -lrt)
    set(scanEreport_DEF_OPTIONS ${scanEreport_DEF_OPTIONS} -D_REENTRANT)
elseif("${ENABLE_THREAD_CHECK}" STREQUAL "ON")
    set(scanEreport_LINK_LIBS ${scanEreport_LINK_LIBS} -pthread -ldl -lm -lrt)
    set(scanEreport_DEF_OPTIONS ${scanEreport_DEF_OPTIONS} -D_REENTRANT)
endif()

add_bintarget(scanEreport TGT_scanEreport_SRC TGT_scanEreport_INC "${scanEreport_DEF_OPTIONS}" "${scanEreport_COMPILE_OPTIONS}" "${scanEreport_LINK_OPTIONS}" "${scanEreport_LINK_LIBS}")
target_link_directories(scanEreport PUBLIC
    ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
add_custom_command(
    OUTPUT TARGET scanEreport POST_BUILD
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/scanEreport ${PROJECT_SRC_DIR}/bin/gsqlerr/scanEreport 
    COMMAND ./scanEreport ${PROJECT_TRUNK_DIR}/distribute ../distribute_ereport.txt 
        ${PROJECT_SRC_DIR}/common/backend/utils/errcodes.txt ../distribute_escan.txt distribute
    WORKING_DIRECTORY ${PROJECT_SRC_DIR}/bin/gsqlerr 
    COMMENT "generate distribute error message"
)
endif()

add_custom_command(
    OUTPUT TARGET scanEreport POST_BUILD
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/scanEreport ${PROJECT_SRC_DIR}/bin/gsqlerr/scanEreport 
    COMMAND ./scanEreport ${PROJECT_OPENGS_DIR} ereport.txt ${PROJECT_SRC_DIR}/common/backend/utils/errcodes.txt escan.txt
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "generate kernel error message"
)

set(TGT_gsqlerr_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/gsqlerr.cpp
)

set(TGT_gsqlerr_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/bin/gsqlerr 
    ${PROJECT_SRC_DIR}/lib/gstrace 
    ${PROJECT_TRUNK_DIR}/distribute/include
)

set(gsqlerr_DEF_OPTIONS ${MACRO_OPTIONS})
set(gsqlerr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gsqlerr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(gsqlerr_LINK_LIBS -l${SECURE_C_CHECK})
if("${ENABLE_MEMORY_CHECK}" STREQUAL "ON")
    set(gsqlerr_LINK_LIBS ${gsqlerr_LINK_LIBS} -pthread -ldl -lm -lrt)
    set(gsqlerr_DEF_OPTIONS ${gsqlerr_DEF_OPTIONS} -D_REENTRANT)
elseif("${ENABLE_THREAD_CHECK}" STREQUAL "ON")
    set(gsqlerr_LINK_LIBS ${gsqlerr_LINK_LIBS} -pthread -ldl -lm -lrt)
    set(gsqlerr_DEF_OPTIONS ${gsqlerr_DEF_OPTIONS} -D_REENTRANT)
endif()
add_bintarget(gsqlerr TGT_gsqlerr_SRC TGT_gsqlerr_INC "${gsqlerr_DEF_OPTIONS}" "${gsqlerr_COMPILE_OPTIONS}" "${gsqlerr_LINK_OPTIONS}" "${gsqlerr_LINK_LIBS}")
add_dependencies(gsqlerr scanEreport)
target_link_directories(gsqlerr PUBLIC
    ${LIBEDIT_LIB_PATH} ${LIBCGROUP_LIB_PATH} ${SECURE_LIB_PATH} ${KERBEROS_LIB_PATH} ${CMAKE_BINARY_DIR}/lib
)
install(TARGETS gsqlerr RUNTIME DESTINATION bin)

