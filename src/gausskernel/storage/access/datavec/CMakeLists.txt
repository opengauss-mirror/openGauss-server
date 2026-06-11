#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_datavec_SRC)

set(TGT_datavec_INC
        ${PROJECT_SRC_DIR}/include
        ${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer
        ${LZ4_INCLUDE_PATH}
        ${LIBCGROUP_INCLUDE_PATH}
        ${EVENT_INCLUDE_PATH}
        ${ZLIB_INCLUDE_PATH}
        ${TOKENIZERS_INCLUDE_PATH}
        ${ONNXRUNTIME_INCLUDE_PATH}
        /usr/include/openblas
)
find_library(OPENBLAS_LIB openblas HINTS /usr/lib64 /usr/lib)

set(datavec_DEF_OPTIONS ${MACRO_OPTIONS})
set(datavec_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(datavec_LINK_OPTIONS ${BIN_LINK_OPTIONS} ${OPENBLAS_LIB})
add_static_objtarget(gausskernel_storage_access_datavec TGT_datavec_SRC TGT_datavec_INC "${datavec_DEF_OPTIONS}" "${datavec_COMPILE_OPTIONS}" "${datavec_LINK_OPTIONS}")

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ogai_sql/
    DESTINATION share/postgresql/ogai/
    FILES_MATCHING PATTERN "*.sql"
)
