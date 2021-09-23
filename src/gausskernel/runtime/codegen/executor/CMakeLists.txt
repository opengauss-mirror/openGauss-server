#This is the main CMAKE for build all components.
set(TGT_executor_SRC ${CMAKE_CURRENT_SOURCE_DIR}/foreignscancodegen.cpp)

set(TGT_executor_INC 
    ${PROJECT_SRC_DIR}/include
    ${LIBLLVM_INCLUDE_PATH}
)

set(executor_DEF_OPTIONS ${MACRO_OPTIONS} -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D_DEBUG)
set(executor_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS} -fomit-frame-pointer -fvisibility-inlines-hidden -fno-exceptions -fno-rtti)
list(REMOVE_ITEM executor_COMPILE_OPTIONS -fno-omit-frame-pointer)
set(executor_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_codegen_executor TGT_executor_SRC TGT_executor_INC "${executor_DEF_OPTIONS}" "${executor_COMPILE_OPTIONS}" "${executor_LINK_OPTIONS}")
target_link_libraries(gausskernel_runtime_codegen_executor PUBLIC -lncurses -lrt -ldl -lm)

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

target_link_libraries(gausskernel_runtime_codegen_executor PUBLIC  ${llvm_libs})
target_link_directories(gausskernel_runtime_codegen_executor PUBLIC ${LIBLLVM_LIB_PATH})
