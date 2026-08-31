set(TGT_fuzzystrmatch_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/fuzzystrmatch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dmetaphone.cpp
)

set(fuzzystrmatch_DEF_OPTIONS ${MACRO_OPTIONS})
set(fuzzystrmatch_COMPILE_OPTIONS
    ${OPTIMIZE_OPTIONS}
    ${OS_OPTIONS}
    ${PROTECT_OPTIONS}
    ${WARNING_OPTIONS}
    ${LIB_SECURE_OPTIONS}
    ${CHECK_OPTIONS}
)
set(fuzzystrmatch_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(fuzzystrmatch TGT_fuzzystrmatch_SRC ""
    "${fuzzystrmatch_DEF_OPTIONS}"
    "${fuzzystrmatch_COMPILE_OPTIONS}"
    "${fuzzystrmatch_LINK_OPTIONS}")
set_target_properties(fuzzystrmatch PROPERTIES PREFIX "")

install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/fuzzystrmatch.control
    ${CMAKE_CURRENT_SOURCE_DIR}/fuzzystrmatch--1.0.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/fuzzystrmatch--unpackaged--1.0.sql
    DESTINATION share/postgresql/extension/)
install(TARGETS fuzzystrmatch DESTINATION lib/postgresql)
