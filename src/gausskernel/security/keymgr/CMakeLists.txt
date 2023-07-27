#This is the main CMAKE for build all components.
set(ENABLE_KT "OFF")

if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    set(ENABLE_KT "ON")
endif()

if("${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(ENABLE_KT "OFF")
endif()

# 1. build libkeymgr.so and libkeymgr.a
# source
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/comm TGT_libkey_SRC)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/encrypt TGT_libkey_SRC)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/his TGT_libkey_SRC)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/hwc TGT_libkey_SRC)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/localkms TGT_libkey_SRC)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/api TGT_libkey_SRC)
if("${ENABLE_KT}" STREQUAL "ON")
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/ktool TGT_libkey_SRC)
endif()
# include
set(TGT_libkey_INC
    ${PROJECT_SRC_DIR}/incldue/keymgr
    ${PROJECT_SRC_DIR}/incldue/keymgr/encrypt
    ${PROJECT_TRUNK_DIR}/distribute/include
    ${CJSON_INCLUDE_PATH} 
    ${CURL_INCLUDE_PATH} 
    ${LIBOPENSSL_INCLUDE_PATH})
if("${ENABLE_KT}" STREQUAL "ON")
    list(APPEND TGT_libkey_INC ${KMC_INCLUDE_PATH})
endif()
# c flage
set(libkey_DEF_OPTIONS ${MACRO_OPTIONS})
set(libkey_COMPILE_OPTIONS
    ${OPTIMIZE_OPTIONS}
    ${OS_OPTIONS}
    ${PROTECT_OPTIONS}
    ${WARNING_OPTIONS}
    ${LIB_SECURE_OPTIONS}
    ${CHECK_OPTIONS}
    -DFRONTEND
    -fstack-protector-strong)
if("${ENABLE_KT}" STREQUAL "ON")
    list(APPEND libkey_COMPILE_OPTIONS -DENABLE_KT)
endif()
list(REMOVE_ITEM libkey_COMPILE_OPTIONS -fstack-protector)
# ld flags
set(libkey_LINK_OPTIONS -pthread -ftrapv -Wl,-z,relro,-z,now -Wl,-z,noexecstack)
# make
add_shared_libtarget(keymgr
    TGT_libkey_SRC
    TGT_libkey_INC
    "${libkey_DEF_OPTIONS}"
    "${libkey_COMPILE_OPTIONS}"
    "${libkey_LINK_OPTIONS}")
add_static_libtarget(keymgr
    TGT_libkey_SRC
    TGT_libkey_INC
    "${libkey_DEF_OPTIONS}"
    "${libkey_COMPILE_OPTIONS}")
# ld libs
if("${ENABLE_KT}" STREQUAL "ON")
    add_dependencies(keymgr gs_ktool)
endif()
set(libkey_LINKS -lcjson -lcurl -lsecurec -lssl -lcrypto -ldl -lrt)
if("${ENABLE_KT}" STREQUAL "ON")
    list(APPEND libkey_LINKS -lgs_ktool -lkmc)
endif()
target_link_libraries(keymgr PUBLIC ${libkey_LINKS})
# ld flags
set(libkey_LINKS_DIRS
    ${LIBOPENSSL_LIB_PATH}
    ${CJSON_LIB_PATH}
    ${LIBCURL_LIB_PATH}
    ${SECURE_LIB_PATH}
    ${CMAKE_BINARY_DIR}/lib)
if("${ENABLE_KT}" STREQUAL "ON")
    list(APPEND libkey_LINKS_DIRS ${KMC_LIB_PATH})
endif()
target_link_directories(keymgr PUBLIC ${libkey_LINKS_DIRS})
# install
install(TARGETS keymgr LIBRARY DESTINATION lib)
install(TARGETS keymgr ARCHIVE DESTINATION lib)

set(keymgr_DEF_OPTIONS ${MACRO_OPTIONS})
set(keymgr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(keymgr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_security_keymgr TGT_libkey_SRC TGT_libkey_INC "${keymgr_DEF_OPTIONS}" "${keymgr_COMPILE_OPTIONS}" "${keymgr_LINK_OPTIONS}")