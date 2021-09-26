#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_codegenutil_SRC)

set(TGT_codegenutil_INC 
    ${PROJECT_SRC_DIR}/include
    ${LIBLLVM_INCLUDE_PATH}
)

set(codegenutil_DEF_OPTIONS ${MACRO_OPTIONS} -D_DEBUG -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
set(codegenutil_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(codegenutil_COMPILE_OPTIONS ${codegenutil_COMPILE_OPTIONS} -fomit-frame-pointer -fvisibility-inlines-hidden -fno-exceptions -fno-rtti)
list(REMOVE_ITEM codegenutil_COMPILE_OPTIONS -fno-omit-frame-pointer)
set(codegenutil_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_runtime_codegen_codegenutil TGT_codegenutil_SRC TGT_codegenutil_INC "${codegenutil_DEF_OPTIONS}" "${codegenutil_COMPILE_OPTIONS}" "${codegenutil_LINK_OPTIONS}")
target_link_libraries(gausskernel_runtime_codegen_codegenutil PUBLIC -lncurses -lrt -ldl -lm)

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

target_link_libraries(gausskernel_runtime_codegen_codegenutil PUBLIC ${llvm_libs})
target_link_directories(gausskernel_runtime_codegen_codegenutil PUBLIC ${LIBLLVM_LIB_PATH})
