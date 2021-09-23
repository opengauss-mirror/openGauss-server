#This is the main CMAKE for build all components.
execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --create_snowballfile ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR}
)

set(TGT_snowball_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/api.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_danish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_dutch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_english.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_finnish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_french.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_german.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_hungarian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_italian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_norwegian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_porter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_portuguese.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_spanish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_1_swedish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_ISO_8859_2_romanian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_KOI8_R_russian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_danish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_dutch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_english.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_finnish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_french.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_german.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_hungarian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_italian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_norwegian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_porter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_portuguese.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_romanian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_russian.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_spanish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_swedish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/stem_UTF_8_turkish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/libstemmer/utilities.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dict_snowball.cpp
)

set(TGT_snowball_INC 
    ${PROJECT_SRC_DIR}/include/snowball 
    ${PROJECT_SRC_DIR}/include/snowball/libstemmer
)

set(snowball_DEF_OPTIONS -D_GNU_SOURCE)
set(snowball_COMPILE_OPTIONS ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${OS_OPTIONS} ${CHECK_OPTIONS} ${OPTIMIZE_OPTIONS} -fstack-protector-all -fsigned-char)
list(REMOVE_ITEM snowball_COMPILE_OPTIONS -fstack-protector)
set(snowball_LINK_OPTIONS -Wl,-z,relro,-z,now -Wl,-z,noexecstack -pthread -Wl,-Bsymbolic -rdynamic)
add_shared_libtarget(dict_snowball TGT_snowball_SRC TGT_snowball_INC "${snowball_DEF_OPTIONS}" "${snowball_COMPILE_OPTIONS}" "${snowball_LINK_OPTIONS}")
target_link_directories(dict_snowball PUBLIC 
    ${CMAKE_BINARY_DIR}/lib
    ${PROJECT_SRC_DIR}/common/port 
    ${PROJECT_SRC_DIR}/gstrace/common
)
set_target_properties(dict_snowball PROPERTIES PREFIX "")

install(FILES ${CMAKE_BINARY_DIR}/snowball_create.sql
    DESTINATION share/postgresql/
)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/stopwords/
    DESTINATION share/postgresql/tsearch_data/
)
install(TARGETS dict_snowball DESTINATION lib/postgresql)
