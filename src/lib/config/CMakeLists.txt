#This is the main CMAKE for build all components.
set(TGT_config_SRC ${CMAKE_CURRENT_SOURCE_DIR}/cm_config.cpp)
set(config_DEF_OPTIONS ${MACRO_OPTIONS})
set(config_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(config_LINK_OPTIONS ${LIB_LINK_OPTIONS})
list(REMOVE_ITEM config_LINK_OPTIONS -pthread)
add_static_libtarget(config TGT_config_SRC "" "${config_DEF_OPTIONS}" "${config_COMPILE_OPTIONS}")

add_shared_libtarget(config TGT_config_SRC "" "${config_DEF_OPTIONS}" "${config_COMPILE_OPTIONS}" "${config_LINK_OPTIONS}")
SET_TARGET_PROPERTIES(config PROPERTIES VERSION 4)

install(TARGETS config_static ARCHIVE DESTINATION lib)
install(TARGETS config LIBRARY DESTINATION lib)
