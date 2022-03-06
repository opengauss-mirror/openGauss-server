# libgauss_cl_jni.so
AUX_SOURCE_DIRECTORY(${PROJECT_SRC_DIR}/common/interfaces/libpq/jdbc/client_logic_jni TGT_gauss_cl_jni_SRC)
SET(TGT_gauss_cl_jni_INC 
    ${PROJECT_SRC_DIR}/common/interfaces/libpq/jdbc/client_logic_jni 
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/include/libpq
    ${PROJECT_SRC_DIR}/common/interfaces/libpq 
    ${JAVA_HOME}/include 
    ${JAVA_HOME}/include/linux 
    ${LIBOPENSSL_INCLUDE_PATH} 
    ${KERBEROS_INCLUDE_PATH} 
    ${KMC_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)
set(gauss_cl_jni_DEF_OPTIONS  ${MACRO_OPTIONS} -DHAVE_CE)
list(REMOVE_ITEM gauss_cl_jni_DEF_OPTIONS -D_GNU_SOURCE)
set(gauss_cl_jni_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gauss_cl_jni_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(gauss_cl_jni TGT_gauss_cl_jni_SRC TGT_gauss_cl_jni_INC "${gauss_cl_jni_DEF_OPTIONS}" "${gauss_cl_jni_COMPILE_OPTIONS}" "${gauss_cl_jni_LINK_OPTIONS}")
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        add_dependencies(gauss_cl_jni gs_ktool)
    endif()
endif()
add_dependencies(gauss_cl_jni pq_ce)
target_link_libraries(gauss_cl_jni PRIVATE -l${SECURE_C_CHECK} -lrt -lz -lpq_ce -lcjson -lcurl)
if(NOT "${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_OFF")
    if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
        target_link_libraries(gauss_cl_jni PRIVATE -lgs_ktool -lkmc)
    endif()
endif()
target_link_directories(gauss_cl_jni PUBLIC ${KMC_LIB_PATH} ${CJSON_LIB_PATH} ${LIBCURL_LIB_PATH} ${LIBOPENSSL_LIB_PATH} ${SECURE_LIB_PATH}  ${CMAKE_BINARY_DIR}/lib ${ZLIB_LIB_PATH})
INSTALL(TARGETS gauss_cl_jni LIBRARY DESTINATION lib)
