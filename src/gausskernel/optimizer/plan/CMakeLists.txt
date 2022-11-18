#This is the main CMAKE for build all components.
FILE(GLOB TGT_plan_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
list(REMOVE_ITEM TGT_plan_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/dynsmp_single.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pgxcplan_single.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/planrecursive_single.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/streamplan_single.cpp
)
endif()

set(TGT_plan_INC 
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(plan_DEF_OPTIONS ${MACRO_OPTIONS})
set(plan_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(plan_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_plan TGT_plan_SRC TGT_plan_INC "${plan_DEF_OPTIONS}" "${plan_COMPILE_OPTIONS}" "${plan_LINK_OPTIONS}")
