#This is the CMAKE for build ut_demo components.
set(TGT_ut_demo_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/ut_demo.cpp
        )

INCLUDE_DIRECTORIES(
        ${PROJECT_SRC_DIR}/include
)
add_executable(ut_demo_opengauss ${TGT_ut_demo_SRC})
TARGET_LINK_LIBRARIES(ut_demo_opengauss ${UNIT_TEST_BASE_LIB_LIST})

target_compile_options(ut_demo_opengauss PRIVATE ${OPTIMIZE_LEVEL})
target_link_options(ut_demo_opengauss PRIVATE ${UNIT_TEST_LINK_OPTIONS_LIB_LIST})
add_custom_command(TARGET ut_demo_opengauss
        POST_BUILD
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/ut_bin
        COMMAND rm -rf ${CMAKE_BINARY_DIR}/ut_bin/ut_demo_opengauss
        COMMAND cp ${CMAKE_BINARY_DIR}/${openGauss}/src/test/ut/demo/ut_demo_opengauss ${CMAKE_BINARY_DIR}/ut_bin/ut_demo_opengauss
        COMMAND chmod +x ${CMAKE_BINARY_DIR}/ut_bin/ut_demo_opengauss
        )
# convenient to test
add_custom_target(ut_demo_test
        DEPENDS ut_demo_opengauss
        COMMAND ${CMAKE_BINARY_DIR}/ut_bin/ut_demo_opengauss || sleep 0
        COMMENT "begin unit test..."
        )
