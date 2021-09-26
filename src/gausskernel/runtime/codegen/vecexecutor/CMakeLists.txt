#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_vecexecutor_SRC)

set(TGT_vecexecutor_INC 
    ${PROJECT_SRC_DIR}/include
    ${LIBLLVM_INCLUDE_PATH}
)

set(vecexecutor_DEF_OPTIONS ${MACRO_OPTIONS} -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D_DEBUG)
set(vecexecutor_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(vecexecutor_COMPILE_OPTIONS ${vecexecutor_COMPILE_OPTIONS} -fomit-frame-pointer -fvisibility-inlines-hidden -fno-exceptions -fno-rtti)
list(REMOVE_ITEM vecexecutor_COMPILE_OPTIONS -fno-omit-frame-pointer)
set(vecexecutor_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_codegen_vecexecutor TGT_vecexecutor_SRC TGT_vecexecutor_INC "${vecexecutor_DEF_OPTIONS}" "${vecexecutor_COMPILE_OPTIONS}" "${vecexecutor_LINK_OPTIONS}")
target_link_libraries(gausskernel_runtime_codegen_vecexecutor PUBLIC -lncurses -lrt -ldl -lm)

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

target_link_libraries(gausskernel_runtime_codegen_vecexecutor PUBLIC  ${llvm_libs})
target_link_directories(gausskernel_runtime_codegen_vecexecutor PUBLIC ${LIBLLVM_LIB_PATH})
