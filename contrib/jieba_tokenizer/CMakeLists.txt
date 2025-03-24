#This is the CMAKE for build tokenizer.
# tokenizer.so
CMAKE_MINIMUM_REQUIRED (VERSION 3.5)
PROJECT(JIEBATOKENIZER)

AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer TGT_jiebatokenizer_SRC)

set(TGT_jiebatokenizer_INC
    ${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer
    ${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer/include
    ${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer/deps
)

set(jiebatokenizer_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(jiebatokenizer_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_static_objtarget(contrib_jiebatokenizer TGT_jiebatokenizer_SRC TGT_jiebatokenizer_INC "" "${jiebatokenizer_COMPILE_OPTIONS}" "${jiebatokenizer_LINK_OPTIONS}")

install(DIRECTORY ${PROJECT_OPENGS_DIR}/contrib/jieba_tokenizer/jieba_dict
        DESTINATION lib)
