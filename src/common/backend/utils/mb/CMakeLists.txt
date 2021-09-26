#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_mb_SRC)
list(REMOVE_ITEM TGT_mb_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/win1251.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/win866.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/iso.cpp
)

set(TGT_mb_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(mb_DEF_OPTIONS ${MACRO_OPTIONS})
set(mb_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(mb_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_mb TGT_mb_SRC TGT_mb_INC "${mb_DEF_OPTIONS}" "${mb_COMPILE_OPTIONS}" "${mb_LINK_OPTIONS}")

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/common/backend/utils/mb/conversion_procs
)

add_subdirectory(conversion_procs)

