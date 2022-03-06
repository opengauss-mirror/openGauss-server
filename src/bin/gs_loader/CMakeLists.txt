# gs_loader
set(TGT_gs_loader_SRC ${CMAKE_CURRENT_SOURCE_DIR}/gs_loader.cpp)

set(gs_loader_DEF_OPTIONS ${MACRO_OPTIONS})
set(gs_loader_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gs_loader_LINK_OPTIONS ${BIN_LINK_OPTIONS})
set(gs_loader_LINK_LIBS -lpthread -ldl -lm -lrt)
add_bintarget(gs_loader TGT_gs_loader_SRC "" "${gs_loader_DEF_OPTIONS}" "${gs_loader_COMPILE_OPTIONS}" "${gs_loader_LINK_OPTIONS}" "${gs_loader_LINK_LIBS}")

install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/gs_loader.sh
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ 
    DESTINATION bin
)
install(TARGETS gs_loader RUNTIME DESTINATION bin)
