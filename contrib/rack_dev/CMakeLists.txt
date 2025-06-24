#This is the main CMAKE for build all rack_dev.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_rack_dev_SRC)

set(TGT_rack_dev_INC 
    ${PROJECT_OPENGS_DIR}/contrib/rack_dev
    ${PROJECT_OPENGS_DIR}/contrib/rack_dev/include
    ${PROJECT_OPENGS_DIR}/contrib
)

set(rack_dev_DEF_OPTIONS ${MACRO_OPTIONS})
set(rack_dev_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -fPIC)
set(rack_dev_LINK_OPTIONS ${BIN_LINK_OPTIONS})
list(APPEND rack_dev_LINK_OPTIONS -fPIC)
add_shared_libtarget(rack_dev TGT_rack_dev_SRC TGT_rack_dev_INC "${rack_dev_DEF_OPTIONS}" "${rack_dev_COMPILE_OPTIONS}" "${rack_dev_LINK_OPTIONS}")
set_target_properties(rack_dev PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/rack_dev--1.0.sql
    DESTINATION share/postgresql/extension/
)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/rack_dev.control
    DESTINATION share/postgresql/extension/
)

install(TARGETS rack_dev DESTINATION lib/postgresql)
