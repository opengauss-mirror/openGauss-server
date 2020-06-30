/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * ts_zh_ngram.h
 *        N-gram algorithm functions for full-text search of Chinese
 * 
 * 
 * IDENTIFICATION
 *        src/include/tsearch/ts_zh_ngram.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TS_ZH_NGRAM_H
#define TS_ZH_NGRAM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"

#define MAX_CHAR_LEN 8
#define MAX_CACHED_CHAR 256

typedef uint8 (*zhParser)(const char* ch, uint8* len);

typedef struct {
    zhParser parser; /* text search parser */
    char* str;       /* text to parse */
    int len;         /* text length */
    char* next;      /* next char pointer */
    int post;        /* position of parsed string */

    char* cache;        /* pointe to parsed but not returned token */
    int max_cached_num; /* cached char number */
    uint8* char_width;  /* array record each char length */
    uint8* char_flag;   /* array record each char type */
    int cached_size;    /* already dealt bytes in cache  */
    int cached_counter; /* already dealt chars in cache  */
    bool cached;        /* if cahced tokens */

    char* token;      /* token pointer to return */
    int char_size;    /* bytes number to return */
    int char_counter; /* char number to return */
    int token_type;   /* token type to renturn */
} ZhParserState;

/* char type defined for text search */
#define CHAR_ILLEGALCHAR 0
#define CHAR_ZHWORD 1
#define CHAR_DIGHT 2
#define CHAR_ENLETTER 3
#define CHAR_BLANK 4
#define CHAR_RADIXPOINT 5
#define CHAR_PUNCTUATION 6
#define CHAR_GRAPSYMBOL 7

/* Output token categories */
#define ZHWORDS 1
#define ENWORD 2
#define NUMERIC 3
#define ALNUM 4
#define GRAPSYMBOL 5
#define MULTISYMBOL 6

const char* const zh_tok_alias[] = {"", "zh_words", "en_word", "numeric", "alnum", "grapsymbol", "multisymbol"};

const char* const zh_lex_descr[] = {
    "", "chinese words", "english word", "numeric data", "alnum string", "graphic symbol", "multiple symbol"};

extern uint8 ascii_matrix[8][16];

extern uint8 gbk_high_byte_matrix[8][16];

extern uint8 gbk_zh_word_matrix[3][16][16];

extern uint8 gbk_grap_symbol_matrix[10][16][16];

extern uint8 utf8_symbols_punctuation_matrix[4][16];

extern uint8 fullwidth_ascii_variants_matrix[4][4][16];

#define to_unsigned(ch) ((unsigned char)(*(ch)))
#define HFB(ch) ((to_unsigned(ch) & 0x00F0) >> 4) /* hight 4-bit value in a byte */
#define LFB(ch) (to_unsigned(ch) & 0x0F)          /* low 4-bit value in a byte */

/* maps to chinese word region if return true */
#define GBK_ZH_REGION(L, R) (gbk_high_byte_matrix[L][R] & 0x0F)

/* maps to graphic symbol region if return true */
#define GBK_GP_REGION(L, R) ((gbk_high_byte_matrix[L][R] & 0x00F0) >> 4)

/*
 * @Description: map a dword char to char type
 *
 * @in ch: dword char
 * @return: predefined char type
 */
inline uint8 gbk_dword_parser(const char* ch)
{
    uint8 HL = HFB(ch) & 0x07; /* (the fist byte's high-4-bit value)  - 8 */
    uint8 HR = LFB(ch);        /* fist byte's low-4-bit value */
    uint8 LL = HFB(ch + 1);    /* second byte's high-4-bit value */
    uint8 LR = LFB(ch + 1);    /* second byte's low-4-bit value */

    if (likely(GBK_ZH_REGION(HL, HR))) /* in chinese word region */
        return gbk_zh_word_matrix[GBK_ZH_REGION(HL, HR)][LL][LR];

    if (GBK_GP_REGION(HL, HR)) /* in graphic symbol region */
        return gbk_grap_symbol_matrix[GBK_GP_REGION(HL, HR)][LL][LR];

    return CHAR_ILLEGALCHAR;
}

/**************************** UTF-8 Encoding Info  *****************************/
/*
 * Unicode 8.0 Character Code Charts
 */

/*
 * General Punctuation
 *     Unnicode Range: 2000 - 206F
 *    binary: (00100000 00000000 ~ 00100000 01101111)
 *
 *     UTF8 Range: 0xE28080 - 0xE281AF
 *    binary: (11100010 10000000 10000000 ~ 11100010 10000001 10101111)
 */
#define GENERAL_PUNCTUATION_LOWER 0xE28080
#define GENERAL_PUNCTUATION_UPPER 0xE281AF

/*
 * CJK Symbols and Punctuation
 *  Unnicode Range: 3000 - 303F
 *  binary: (00110000 00000000 ~ 0011 0000 00111111)
 *
 *  UTF8 Range: 0xE38080 - 0xE380BF
 *  binary: (11100011 10000000 10000000 ~ 11100011 10000000 10111111)
 *
 *  Last byte's range is 10 00 0000 ~ 10 11 1111
 */
#define CJK_SYMBOLS_AND_PUNCTUATION_LOWER 0xE38080
#define CJK_SYMBOLS_AND_PUNCTUATION_UPPER 0xE380BF

#define UTF8_SYMBOLS_PUNCTUATION_REGION(ch) (utf8_symbols_punctuation_matrix[HFB(ch + 2) & 0x03][LFB(ch + 2)])

/*
 * UTF8 Chinese word region description
 *
 * CJK Unified Ideographs (3 bytes)
 *    Unnicode Range: 4E00 - 9FD5
 *    binary: (01001110 00000000 ~ 10011111 11010101)
 *    UTF8 Range: 0xE4B880 - 0xE9BF95
 *    binary: (11100100 10111000 10000000 ~ 11101001 10111111 10010101)
 *
 * CJK Extension-A (3 bytes)
 *    Unnicode Range: 0x3400 - 0x4DB5
 *    binary: (00110100 00000000 ~ 01001101 10110101)
 *    UTF8 Range: 0xE39080 - 0xE4B6B5
 *    binary: (11100011 10010000 10000000 ~ 11100100 10110110 10110101)
 *
 * CJK Extension-B  (4 bytes)
 *    Unnicode Range: 0x20000 - 0x2A6D6
 *    binary: (00000010 00000000 00000000 ~ 00000010 10100110 11010110)
 *    UTF8 Range: 0xF0A08080 - 0xF0AA9B96
 *    binary: (11110000 10100000  10000000 10000000 ~ 11110000 10101010 10011011 10010110)
 */
#define CJK_UNIFIED_IDEOGRAPHS_LOWER 0xE4B880
#define CJK_UNIFIED_IDEOGRAPHS_UPPER 0xE9BF95

#define CJK_EXTENSION_A_LOWER 0xE39080
#define CJK_EXTENSION_A_UPPER 0xE4B6B5

#define CJK_EXTENSION_B_LOWER 0xF0A08080
#define CJK_EXTENSION_B_UPPER 0xF0AA9B96

/*
 *
 * Halfwidth and Fullwidth Forms description:
 *    Unnicode Range: FF00 - FFEF
 *    binary:(11111111 00000000 ~  11111111 11101111)
 *        Fullwidth Dight:FF10-FF19
 *        binary: (11111111 00010000 ~ 11111111 00011001)
 *        Fullwidth Uppercase:FF21-FF3A
 *        binary: (11111111 00100001 ~ 11111111 00111010)
 *         Fullwidth Lowercase:FF41-FF5A
 *        binary: (11111111 01000001 ~ 11111111 01011010)
 *
 *     UTF8 Range: 0xEFBC80 - 0xEFBFAF
 *        binary: (11101111 10111100 10000000 ~  11101111 10111111 10101111)
 *        Fullwidth Dight:0xEFBC90-0xEFBC99
 *        binary: (11101111 10111100 10010000 ~ 11101111 10111100 10011001)
 *        Fullwidth Uppercase:0xEFBCA1-0xEFBCBA
 *        binary: (11101111 10111100 10100001 ~ 11101111 10111100 10111010)
 *        Fullwidth Lowercase:0xEFBE81-0xEFBD9A
 *        binary: (11101111 10111101 10000001 ~ 11101111 10111101 10011010)
 *
 * Last 2-byte's rang is 00 00 0000 ~ 11 10 1111
 *
 */
#define HALFWIDTH_AND_FULLWIDTH_FORMS_LOWER 0xEFBC80
#define HALFWIDTH_AND_FULLWIDTH_FORMS_UPPER 0xEFBFAF

#define UTF8_FULLWIDTH_ASCII_VARIANTS_REGION(ch) \
    fullwidth_ascii_variants_matrix[LFB(ch + 1) & 0x03][HFB(ch + 2) & 0x03][LFB(ch + 2)]

#define utf8_len(value, mask, ch) (value == (mask & to_unsigned(ch)))
/*
 * @Description: maps to char type for a UTF8 endcoding char
 *
 * @in ch: UTF8 endcoding char pointer to parser
 * @out len: UTF8 char length
 * @return: char type defined in front
 */
inline uint8 utf8_parser(const char* ch, uint8* len)
{
    if (likely(utf8_len(0xC0, 0xE0, ch))) { /* 2 byte */
        *len = 2;
        return CHAR_GRAPSYMBOL;
    } else if (likely(utf8_len(0xE0, 0xF0, ch))) { /* 3 byte */
        pg_wchar wchar = 0x00FFFFFF;
        *len = 3;
        wchar = wchar & ((to_unsigned(ch) << 16) | (to_unsigned(ch + 1) << 8) | to_unsigned(ch + 2));

        if (unlikely(wchar < GENERAL_PUNCTUATION_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (likely(wchar < GENERAL_PUNCTUATION_UPPER)) {
            return CHAR_PUNCTUATION;
        } else if (unlikely(wchar < CJK_SYMBOLS_AND_PUNCTUATION_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (likely(wchar <= CJK_SYMBOLS_AND_PUNCTUATION_UPPER)) { /* in CJK Symbols and Punctuation Region */
            return UTF8_SYMBOLS_PUNCTUATION_REGION(ch);
        } else if (unlikely(wchar < CJK_EXTENSION_A_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (likely(wchar <= CJK_EXTENSION_A_UPPER)) { /* in CJK Extension-A Region */
            return CHAR_ZHWORD;
        } else if (unlikely(wchar < CJK_UNIFIED_IDEOGRAPHS_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (likely(wchar <= CJK_UNIFIED_IDEOGRAPHS_UPPER)) { /* in CJK Unified Ideographs Region */
            return CHAR_ZHWORD;
        } else if (unlikely(wchar < HALFWIDTH_AND_FULLWIDTH_FORMS_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (likely(wchar <= HALFWIDTH_AND_FULLWIDTH_FORMS_UPPER)) { /* in Halfwidth and Fullwidth Forms region */
            return UTF8_FULLWIDTH_ASCII_VARIANTS_REGION(ch);
        } else {
            return CHAR_GRAPSYMBOL;
        }
    } else if (utf8_len(0xF0, 0xF8, ch)) { /* 4 byte */
        pg_wchar wchar =
            ((to_unsigned(ch) << 24) | (to_unsigned(ch + 1) << 16) | (to_unsigned(ch + 2) << 8) | to_unsigned(ch + 3));
        *len = 4;

        if (unlikely(wchar < CJK_EXTENSION_B_LOWER)) {
            return CHAR_GRAPSYMBOL;
        } else if (wchar <= CJK_EXTENSION_B_UPPER) {
            return CHAR_ZHWORD;
        } else {
            return CHAR_GRAPSYMBOL;
        }
#ifdef NOT_USED
    } else if (unlikely(utf8_len(0xF8, 0xFC, ch))) { /* 5 byte */
        *len = 5;
        return CHAR_GRAPSYMBOL;
    } else if (unlikely(utf8_len(0xFC, 0xFE, ch))) { /* 6 byte */
        *len = 6;
        return CHAR_GRAPSYMBOL;
#endif
    } else {
        *len = 1;
        return CHAR_GRAPSYMBOL;
    }
}

extern void zhParserStateInit(char* str, int len, ZhParserState* parserState);
extern void zhParserStateDestroy(ZhParserState* parserState);

/* ngram parser functions for chinese */
extern Datum ngram_start(PG_FUNCTION_ARGS);
extern Datum ngram_nexttoken(PG_FUNCTION_ARGS);
extern Datum ngram_end(PG_FUNCTION_ARGS);
extern Datum ngram_lextype(PG_FUNCTION_ARGS);

#endif /* TS_ZH_NGRAM_H */
