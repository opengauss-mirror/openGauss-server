#This is the main CMAKE for build all components.
set(tgt_gssgpersist_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_persist.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scsi_persist.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scsi_persistent_reserve.cpp
)

set(tgt_gssgpersist_INC
    ${SECURE_INCLUDE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(gssgpersist_DEF_OPTIONS ${MACRO_OPTIONS})
set(gssgpersist_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gssgpersist_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(gssgpersist_LINK_LIBS -lsecurec -ldl -lrt)

if("${ENABLE_UT}" STREQUAL "ON")
    add_shared_libtarget(ut_gs_persist_lib tgt_gssgpersist_SRC tgt_gssgpersist_INC "${gssgpersist_DEF_OPTIONS}" "${gssgpersist_COMPILE_OPTIONS}" "${gssgpersist_LINK_OPTIONS}") 
    #add_dependencies(ut_gs_persist_lib ${guc_LINK_DEPEND})
    target_link_libraries(ut_gs_persist_lib PRIVATE ${gssgpersist_LINK_LIBS}) 
    target_link_directories(ut_gs_persist_lib PUBLIC ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)
    install(TARGETS ut_gs_persist_lib LIBRARY DESTINATION lib)
else()
    add_bintarget(gs_persist tgt_gssgpersist_SRC tgt_gssgpersist_INC "${gssgpersist_DEF_OPTIONS}" "${gssgpersist_COMPILE_OPTIONS}" "${gssgpersist_LINK_OPTIONS}" "${gssgpersist_LINK_LIBS}")
    target_link_directories(gs_persist PUBLIC ${SECURE_LIB_PATH} ${CMAKE_BINARY_DIR}/lib)

    install(TARGETS gs_persist RUNTIME DESTINATION bin)
endif() 
