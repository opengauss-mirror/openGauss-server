#This is the main CMAKE for build all components.
execute_process( 
    COMMAND ln -fs ${PROJECT_SRC_DIR}/common/port/pgstrcasecmp.cpp ${CMAKE_CURRENT_SOURCE_DIR}/pgstrcasecmp.cpp
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_pgtypes_SRC)
set(TGT_pgtypes_INC 
    ${PROJECT_SRC_DIR}/common/port
    ${PROJECT_SRC_DIR}/common/interfaces/libpq
    ${PROJECT_SRC_DIR}/include/libpq 
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/include 
    ${PROJECT_SRC_DIR}/include/utils
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${PROJECT_SRC_DIR}/lib/gstrace 
)

set(pgtypes_DEF_OPTIONS ${MACRO_OPTIONS} -DSO_MAJOR_VERSION=3)
set(pgtypes_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(pgtypes TGT_pgtypes_SRC TGT_pgtypes_INC "${pgtypes_DEF_OPTIONS}" "${pgtypes_COMPILE_OPTIONS}")
install(TARGETS pgtypes_static ARCHIVE DESTINATION lib)

set(pgtypes_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(pgtypes TGT_pgtypes_SRC TGT_pgtypes_INC "${pgtypes_DEF_OPTIONS}" "${pgtypes_COMPILE_OPTIONS}" "${pgtypes_LINK_OPTIONS}")
target_link_libraries(pgtypes PUBLIC -lm)
set_target_properties(pgtypes PROPERTIES SOVERSION 3 VERSION 3.3)

install(TARGETS pgtypes LIBRARY DESTINATION lib)

