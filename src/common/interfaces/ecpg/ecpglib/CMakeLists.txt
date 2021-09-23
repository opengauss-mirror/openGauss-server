#This is the main CMAKE for build all components.
execute_process( 
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/path.cpp ${CMAKE_CURRENT_SOURCE_DIR}/path.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/pgstrcasecmp.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/strlcpy.cpp ${CMAKE_CURRENT_SOURCE_DIR}/strlcpy.cpp
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/thread.cpp ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_ecpglib_SRC)
set(TGT_ecpglib_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/include
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
) 

set(ecpglib_DEF_OPTIONS ${MACRO_OPTIONS} -DSO_MAJOR_VERSION=6)
set(ecpglib_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(ecpg TGT_ecpglib_SRC TGT_ecpglib_INC "${ecpglib_DEF_OPTIONS}" "${ecpglib_COMPILE_OPTIONS}")

set(ecpglib_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(ecpg TGT_ecpglib_SRC TGT_ecpglib_INC "${ecpglib_DEF_OPTIONS}" "${ecpglib_COMPILE_OPTIONS}" "${ecpglib_LINK_OPTIONS}")
target_link_libraries(ecpg PUBLIC -lm -lpgtypes -lpq -lpthread)
target_link_directories(ecpg PUBLIC 
    ${CMAKE_BINARY_DIR}/lib
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/pgtypeslib 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
)
add_dependencies(ecpg pq pgtypes)
set_target_properties(ecpg PROPERTIES SOVERSION 6 VERSION 6.4)
install(TARGETS ecpg LIBRARY DESTINATION lib)

