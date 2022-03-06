set(TGT_ut_direct_ml_SRC ${CMAKE_CURRENT_SOURCE_DIR}/ut_direct_ml.cpp)
INCLUDE_DIRECTORIES(
        ${PROJECT_SRC_DIR}/include
)


set(ut_direct_ml_objects
        $<TARGET_OBJECTS:ut_direct_ml_inner>
        )

add_subdirectory(direct_ml)

add_executable(ut_direct_ml ${TGT_ut_direct_ml_SRC} ${ut_direct_ml_objects})
target_compile_options(ut_direct_ml PRIVATE ${OPTIMIZE_LEVEL})
target_link_options(ut_direct_ml PRIVATE ${UNIT_TEST_LINK_OPTIONS_LIB_LIST})

TARGET_LINK_LIBRARIES(ut_direct_ml ${UNIT_TEST_BASE_LIB_LIST})
add_custom_command(TARGET ut_direct_ml
        POST_BUILD
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/ut_bin
        COMMAND rm -rf ${CMAKE_BINARY_DIR}/ut_bin/ut_direct_ml
        COMMAND cp ${CMAKE_BINARY_DIR}/${openGauss}/src/test/ut/db4ai/ut_direct_ml ${CMAKE_BINARY_DIR}/ut_bin/ut_direct_ml
        COMMAND chmod +x ${CMAKE_BINARY_DIR}/ut_bin/ut_direct_ml
        COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/ut_bin/data
        COMMAND cp -r ${CMAKE_CURRENT_SOURCE_DIR}/data ${CMAKE_BINARY_DIR}/ut_bin/
        )
add_custom_target(ut_direct_ml_test
        DEPENDS ut_direct_ml
        COMMAND ${CMAKE_BINARY_DIR}/ut_bin/ut_direct_ml || sleep 0
        COMMENT "begin unit test..."
        )