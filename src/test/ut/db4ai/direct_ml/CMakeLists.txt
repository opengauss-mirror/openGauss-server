set(TGT_ut_db4ai_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/gd_direct.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/kmeans_direct.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/xgboost_direct.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/svm_direct.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pg_mock.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/readers.cpp
)
add_library(ut_direct_ml_inner STATIC ${TGT_ut_db4ai_SRC})
target_link_libraries(ut_direct_ml_inner ${UNIT_TEST_BASE_LIB_LIST} ${UNIT_TEST_ut_direct_ml_LIB_LIST})
