function(gv_detect_npu_component VECTOR_HOME OUT_VAR)
    set(_required_npu_libraries
        libascend_kernels.so
        lib_ascend_ai_core_a.so
        lib_ascend_vector_core_v.so
    )
    set(_missing_npu_libraries "")
    foreach(_library IN LISTS _required_npu_libraries)
        if(NOT EXISTS "${VECTOR_HOME}/lib/${_library}")
            list(APPEND _missing_npu_libraries "${_library}")
        endif()
    endforeach()

    if(_missing_npu_libraries)
        set(${OUT_VAR} OFF PARENT_SCOPE)
        message(STATUS "GaussVector CPU-only or incomplete NPU component detected; missing: ${_missing_npu_libraries}")
        return()
    endif()

    set(${OUT_VAR} ON PARENT_SCOPE)
    message(STATUS "GaussVector NPU component detected")
endfunction()
