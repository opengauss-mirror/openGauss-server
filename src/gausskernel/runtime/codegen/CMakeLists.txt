#This is the main CMAKE for build all components.
set(TGT_codegen_SRC ${CMAKE_CURRENT_SOURCE_DIR}/gscodegen.cpp)

set(TGT_codegen_INC 
    ${PROJECT_SRC_DIR}/include
if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")    
    ${LIBLLVM_INCLUDE_PATH}
endif()    
)

set(codegen_DEF_OPTIONS ${MACRO_OPTIONS} -D_DEBUG -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
set(codegen_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -fomit-frame-pointer -fvisibility-inlines-hidden -fexceptions -fno-rtti)
list(REMOVE_ITEM codegen_COMPILE_OPTIONS -fno-omit-frame-pointer)
set(codegen_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_codegen TGT_codegen_SRC TGT_codegen_INC "${codegen_DEF_OPTIONS}" "${codegen_COMPILE_OPTIONS}" "${codegen_LINK_OPTIONS}")
target_link_libraries(gausskernel_runtime_codegen PUBLIC -lncurses -lrt -ldl -lm)

SET(llvm_libs_var "")
if(${ENABLE_LLVM_COMPILE})
    execute_process(
        COMMAND ${LLVM_CONFIG} --libs
        RESULT_VARIABLE llvm_libs_ret
        OUTPUT_VARIABLE llvm_libs_var
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif()
separate_arguments(llvm_libs UNIX_COMMAND ${llvm_libs_var})

target_link_libraries(gausskernel_runtime_codegen PUBLIC ${llvm_libs})
target_link_directories(gausskernel_runtime_codegen PUBLIC ${LIBLLVM_LIB_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/codegenutil
    ${CMAKE_CURRENT_SOURCE_DIR}/executor
    ${CMAKE_CURRENT_SOURCE_DIR}/vecexecutor
)

add_subdirectory(codegenutil)
add_subdirectory(executor)
add_subdirectory(vecexecutor)

if(${BUILD_TUPLE} STREQUAL "aarch64")
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/llvmir/GaussDB_expr_aarch64.ir
        DESTINATION share/llvmir/
        RENAME GaussDB_expr.ir)
else()
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/llvmir/GaussDB_expr.ir
        DESTINATION share/llvmir/)
endif()
