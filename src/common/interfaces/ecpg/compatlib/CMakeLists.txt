#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_compatlib_SRC)

set(TGT_compatlib_INC 
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/include
)

set(compatlib_DEF_OPTIONS ${MACRO_OPTIONS} -DSO_MAJOR_VERSION=3)
set(compatlib_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
add_static_libtarget(ecpg_compat TGT_compatlib_SRC TGT_compatlib_INC "${compatlib_DEF_OPTIONS}" "${compatlib_COMPILE_OPTIONS}")

set(compatlib_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(ecpg_compat TGT_compatlib_SRC TGT_compatlib_INC "${compatlib_DEF_OPTIONS}" "${compatlib_COMPILE_OPTIONS}" "${compatlib_LINK_OPTIONS}")
target_link_directories(ecpg_compat PUBLIC 
    ${CMAKE_BINARY_DIR}/lib
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/ecpglib 
    ${PROJECT_SRC_DIR}/common/interfaces/ecpg/pgtypeslib 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${PROJECT_SRC_DIR}/common/port 
    ${PROJECT_SRC_DIR}/gstrace/common
)
target_link_libraries(ecpg_compat PUBLIC -lecpg -lm -lpgtypes -lpq -lpthread)
add_dependencies(ecpg_compat pq ecpg pgtypes)
set_target_properties(ecpg_compat PROPERTIES VERSION 3.4 SOVERSION 3)

install(TARGETS ecpg_compat LIBRARY DESTINATION lib)
install(TARGETS ecpg_compat_static ARCHIVE DESTINATION lib)

