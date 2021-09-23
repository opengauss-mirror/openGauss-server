#This is the main CMAKE for build all components.

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/compatlib
    ${CMAKE_CURRENT_SOURCE_DIR}/ecpglib
    ${CMAKE_CURRENT_SOURCE_DIR}/pgtypeslib
)

add_subdirectory(compatlib)
add_subdirectory(ecpglib)
add_subdirectory(pgtypeslib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include DESTINATION ./
        PATTERN "sqltypes.h decimal.h datetime.h" EXCLUDE PATTERN "*.h")
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/sqltypes.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/decimal.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/datetime.h 
        DESTINATION include/postgresql/informix/esql)
