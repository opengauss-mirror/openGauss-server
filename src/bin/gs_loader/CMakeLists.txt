add_executable(gs_loader ${CMAKE_CURRENT_SOURCE_DIR}/gs_loader.cpp)

install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/gs_loader.sh
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ 
    DESTINATION bin
)
install(TARGETS gs_loader RUNTIME DESTINATION bin)