# -D,  required option
add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0 PGXC)
set(UNIT_TEST_LINK_OPTIONS_LIB_LIST -Wl,-z,relro,-z,now -Wl,-z,noexecstack -Wl,-E -z muldefs)

# -I
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/include ${MOCKCPP_INCLUDE_PATH} ${GTEST_INCLUDE_PATH} ${MOCKCPP_3RDPARTY_PATH})

# -L
link_directories(${CMAKE_BINARY_DIR}/lib ${GTEST_LIB_PATH} ${MOCKCPP_LIB_PATH} 
${CJSON_LIB_PATH} ${DCF_LIB_PATH} ${XGBOOST_LIB_PATH}  ${ZSTD_LIB_PATH} 
${LIBPARQUET_LIB_PATH} ${ARROW_LIB_PATH} ${LIBOBS_LIB_PATH} ${LZ4_LIB_PATH})

# -l
set(UNIT_TEST_BASE_LIB_LIST pthread gtest_main gtest mockcpp cjson dcf xgboost zstd nghttp2 iconv eSDKOBS eSDKLogAPI log4cpp parquet arrow lz4 db)

add_subdirectory(demo)
add_subdirectory(db4ai)

set(UT_TEST_TARGET_LIST ut_demo_test ut_direct_ml_test)
add_custom_target(all_ut_test_opengauss DEPENDS ${UT_TEST_TARGET_LIST} COMMAND echo "end unit test all...")
