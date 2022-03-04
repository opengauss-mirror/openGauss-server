# xgboost.cmake
set(TGT_xgboost_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/xgboost.cpp
        )
set(TGT_xgboost_INC
        ${XGBOOST_INCLUDE_PATH}
        )
set(xgboost_DEF_OPTIONS ${MACRO_OPTIONS})
set(xgboost_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(xgboost_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_db4ai_executor_xgboost TGT_xgboost_SRC TGT_xgboost_INC "${xgboost_DEF_OPTIONS}" "${xgboost_COMPILE_OPTIONS}" "${xgboost_LINK_OPTIONS}")