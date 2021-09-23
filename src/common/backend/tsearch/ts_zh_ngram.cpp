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
 * -------------------------------------------------------------------------
 *
 * ts_zh_ngram.cpp
 *    ngram algorithm functions for a full-text search of Chinese method, it is a independent
 *    module, and only can be executed when we use N-gram parser directly(such as ts_parse
 *    with ngram parser) or indirectly(such as to_tsvector with text seach configuration
 *    defined with N-gram parser).
 *
 *    As a parser we have got to abide by openGauss full-text search rules, and define 4
 *    functions ngram_start as start_function, ngram_nexttoken as gettoken_function,
 *    ngram_end as end_function, ngram_start as start_function, and use prsd_headline
 *    defined for default parser as N-gram's headline_function
 *
 * IDENTIFICATION
 *    src/common/backend/tsearch/ts_zh_ngram.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <ctype.h>
#include "securec.h"
#include "tsearch/ts_zh_ngram.h"
#include "tsearch/ts_public.h"
#include "nodes/bitmapset.h"
#include "tsearch/ts_cache.h"

/* ngram parser state */
typedef struct {
    ZhParserState parserState;
    int cur_size;    /* token bytes to retutrn */
    int cur_counter; /* token chars that will be returned */
    bool stat_tail;
    int gram_size;           /* configuration para for parser */
    bool punctuation_ignore; /* configuration para for parser */
    bool grapsymbol_ignore;  /* configuration para for parser */
} NgramZhParserState;

static THR_LOCAL NgramZhParserState ngram_parser_state;

/* char type defined for ngram text search */
#define NGRAM_TOKEN_NUM 6

/********************************ASCII encoding Info******************************/
/*
 *  Get character type in graphic symbol  region
 *
 * 0 for invisible symbol
 * 2 for digit
 * 3 for english letter
 * 4 for black space
 * 5 for radix point
 * 6 for punctuation
 * 7 for graphic symbol
 */
uint8 ascii_matrix[8][16] = {
    /*     0    1   2    3   4    5   6    7   8    9    A   B   C   D   E    F */
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
    {4, 6, 6, 7, 7, 7, 7, 6, 6, 6, 7, 7, 6, 7, 6, 7}, /* 0x2X */
    {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 6, 6, 7, 7, 7, 6}, /* 0x3X */
    {7, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0x4X */
    {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 6}, /* 0x5X */
    {6, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0x6X */
    {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 0}  /* 0x7X */
};

/*********************************  GBK Encoding Info  ****************************/
/*
 * Based on Chinese Internal Code Specification Version 1.0
 *
 * GBK encoding must abide by following rules in version 1.0
 * 1. encode by two bytes
 * 2. encoding is between 0x8140 and 0xFEFE, and low-byte must be not 0x7F
 *
 * As we check wchar with IS_HIGHBIT_SET, so we can do following check for GBK encoding
 * 1. high-byte value must  be between 0x81 and 0xFE
 * 2. low-byte value must be not 0x7F
 * 3. low-byte value must be not 0xFF
 * 4. low-byte value must be big than 0x03
 *
 * We use two-stage matrices to decide a char type. matrix defined with high-byte value
 * decide wihch region maps to. there are three defined regions: chinese character region,
 * invalid GBK encoding region, graphic symbol region. matrix defined with low-byte value
 * decide which type the char is
 */

/*
 * Matrix defined with high-byte value
 *
 * value 0x0X means map to chinese character region
 * value 0x00 means map to invalid GBK encoding region
 * value 0xX0 means map to graphic symbol region
 */
uint8 gbk_high_byte_matrix[8][16] = {
    /* 0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F */
    {0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0x8X */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0x9X */
    {0x01, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0x20, 0x02, 0x02, 0x02, 0x02, 0x02}, /* 0xAX */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0xBX */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0xCX */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0xDX */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, /* 0xEX */
    {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x00}  /* 0xFX */
};

/*
 *  Get character type in chinese character region
 *
 * 1 for Chinese word
 * 0 for invalid GBK encoding
 */
uint8 gbk_zh_word_matrix[3][16][16] = {
    {
        /* just for easy map */
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xAX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xBX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xCX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xDX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    {
        /* [0x81XX, 0xA0XX], [0xB0XX, 0xF7XX] */
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x4X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x5X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x6X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0}, /* 0x7X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x8X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x9X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0xAX */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0xBX */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0xCX */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0xDX */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0xEX */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0}  /* 0xFX */
    },
    {
        /* [0xAAXX-0xAFXX], [0xF8XX-0xFEXX] */
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x4X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x5X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x6X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0}, /* 0x7X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x8X */
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, /* 0x9X */
        {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xAX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xBX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xCX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xDX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    }};

/*
 *  Get character type in graphic symbol region
 *
 *  0 for invisible symbol
 *  1 for chinese word
 * 2 for digit
 * 3 for lowercase english letter
 * 4 for black space
 * 5 for radix point
 * 6 for punctuation
 * 7 for graphic symbol
 * 8 for uppercase english letter
 */
uint8 gbk_grap_symbol_matrix[10][16][16] = {
    /* just for easy map */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xAX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xBX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xCX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xDX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA1XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 4, 6, 6, 7, 7, 7, 7, 7, 7, 6, 7, 7, 6, 6, 6}, /* 0xAX */
        {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}, /* 0xBX */
        {7, 7, 7, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xEX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0}  /* 0xFX */
    },
    /* 0xA2XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0}, /* 0xAX */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0}, /* 0xEX */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA3XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 6, 6, 7, 7, 7, 7, 6, 6, 6, 7, 7, 6, 7, 6, 7}, /* 0xAX */
        {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 6, 6, 7, 7, 7, 6}, /* 0xBX */
        {7, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0xCX */
        {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 7}, /* 0xDX */
        {6, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0xEX */
        {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 0}  /* 0xFX */
    },
    /* 0xA4XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xEX */
        {7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA5XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xEX */
        {7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA6XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0}, /* 0xBX */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6}, /* 0xDX */
        {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}, /* 0xEX */
        {6, 6, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA7XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x4X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x5X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x6X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x7X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x8X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0xCX */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xEX */
        {7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA8XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x4X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x5X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x6X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0}, /* 0x7X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x8X */
        {7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 0, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0}, /* 0xEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    },
    /* 0xA9XX */
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x0X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x1X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x2X */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x3X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x4X */
        {7, 7, 7, 7, 7, 7, 7, 7, 0, 7, 7, 0, 7, 0, 0, 0}, /* 0x5X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 6}, /* 0x6X */
        {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 0}, /* 0x7X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x8X */
        {7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0}, /* 0x9X */
        {0, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xBX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xCX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xDX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFX */
    }};

uint8 utf8_symbols_punctuation_matrix[4][16] = {
    /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
    {0, 6, 6, 6, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 6}, /* 0x300X */
    {6, 6, 7, 7, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}, /* 0x301X */
    {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0x302X */
    {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0}  /* 0x303X */
};

uint8 fullwidth_ascii_variants_matrix[4][4][16] = {

    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 6, 6, 7, 7, 7, 7, 6, 6, 6, 7, 7, 6, 7, 6, 7}, /* 0xFF0X */
        {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 6, 6, 7, 7, 7, 6}, /* 0xFF1X */
        {7, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0xFF2X */
        {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 7}  /* 0xFF3X */
    },
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {7, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, /* 0xFF4X */
        {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 6, 7, 6, 7, 7}, /* 0xFF5X */
        {7, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xFF6X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xFF7X */
    },
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xFF8X */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xFF9X */
        {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, /* 0xFFAX */
        {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0}  /* 0xFFBX */
    },
    {
        /* 0    1    2    3    4    5    6    7    8    9    A    B    C   D    E    F */
        {0, 0, 7, 7, 7, 7, 7, 7, 0, 0, 7, 7, 7, 7, 7, 7}, /* 0xFFCX */
        {0, 0, 7, 7, 7, 7, 7, 7, 0, 0, 7, 7, 7, 0, 0, 0}, /* 0xFFDX */
        {7, 7, 7, 7, 7, 7, 7, 0, 7, 7, 7, 7, 7, 7, 7, 0}, /* 0xFFEX */
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}  /* 0xFFFX */
    }};

/**
 * @Description: map to char type with ascii encoding. without regard for extensition
 *               range, since we just deal with GBK/UTF8 encoding
 *
 * @in ch: ascii char pointer to parser
 * @return: char type defined in front
 */
static inline uint8 gbk_ascii_parser(const char* ch)
{
    Assert(0 == (to_unsigned(ch) & 0x80));

    return ascii_matrix[HFB(ch) & 0x07][LFB(ch)];
}

/*
 * @Description: maps to char type for a GBK endcoding char
 *
 * @in ch: GBK endcoding char
 * @return: predefined char type
 */
static inline uint8 gbk_parser(const char* ch, uint8* len)
{
    if (likely(IS_HIGHBIT_SET(*ch))) {
        *len = 2;
        return gbk_dword_parser(ch);
    } else {
        *len = 1;
        return gbk_ascii_parser(ch);
    }
}

/********************************* ngram parser  *******************************/
/*
 * @Description: show clearly the region that we have parsed with the starting pointer
 *                      and the legth, now we can use it just like a cache
 *
 * @in pst: ngram parse state
 */
static inline void cacheToken(NgramZhParserState* pst)
{
    Assert(pst->parserState.cached == false);
    Assert(pst->parserState.cached_counter && pst->parserState.cached_size);

    pst->parserState.cache = pst->parserState.token;
    pst->parserState.cached = true;
    pst->stat_tail = (pst->parserState.cached_counter < pst->gram_size);

    pst->cur_size = 0;
    pst->cur_counter = 0;
}

static inline uint8 ngram_utf8_parser(const char* ch, uint8* len)
{
    if (likely(utf8_len(0x00, 0x80, ch))) { /* 1 byte */
        *len = 1;
        return gbk_ascii_parser(ch);
    } else {
        return utf8_parser(ch, len);
    }
}

/*
 * @Description: pack up continuous N chars as a output package, no matter what kind it is
 *
 * @in pst: ngram parse state
 * @return: return true if succeed to fetch a token, else return false
 */
static inline bool packageTokenMulti(NgramZhParserState* pst)
{
    if (unlikely(!pst->parserState.cached))
        return false;

    pst->parserState.char_size = 0;
    pst->parserState.char_counter = 0;
    pst->parserState.token_type = MULTISYMBOL;

    /* enough cached chars */
    if (likely(pst->cur_counter + pst->gram_size <= pst->parserState.cached_counter)) {
        int tmp_counter = pst->cur_counter;

        /* start of output package */
        pst->parserState.token = pst->parserState.cache + pst->cur_size;

        /* start of next package */
        pst->cur_size += pst->parserState.char_width[pst->cur_counter];
        pst->cur_counter++;

        /* pack up chars for output package */
        while (pst->parserState.char_counter < pst->gram_size) {
            pst->parserState.char_size += pst->parserState.char_width[tmp_counter++];
            pst->parserState.char_counter++;
        }

        return true;
    }

    /* not enough cached chars, but we want to output the reset chars */
    if (pst->stat_tail && pst->cur_counter < pst->parserState.cached_counter) {
        pst->stat_tail = false;
        pst->parserState.token = pst->parserState.cache + pst->cur_size;

        while (pst->cur_counter < pst->parserState.cached_counter) {
            pst->parserState.char_size += pst->parserState.char_width[pst->parserState.char_counter++];
            pst->cur_counter++;
        }

        pst->parserState.cached = false;
        pst->parserState.cached_counter = 0;
        pst->parserState.cached_size = 0;

        return true;
    }

    /* fail to pack up, so clear up the mark of the cache  */
    pst->parserState.cache = NULL;
    pst->parserState.cached = false;
    pst->parserState.cached_size = 0;
    pst->parserState.cached_counter = 0;

    return false;
}

/*
 * @Description: fetch tokens from cahce region
 *
 * @in pst: ngram parser state
 */
static inline bool packageToken(NgramZhParserState* pst)
{
    return packageTokenMulti(pst);
}

void zhParserStateInit(char* str, int len, ZhParserState* parserState)
{
    parserState->str = str;
    parserState->len = len;
    parserState->next = NULL;
    parserState->post = 0;

    parserState->cache = NULL;
    parserState->max_cached_num = MAX_CACHED_CHAR;
    parserState->char_width = (uint8*)palloc0(parserState->max_cached_num + 1);
    parserState->char_flag = (uint8*)palloc0(parserState->max_cached_num + 1);

    parserState->cached_size = 0;
    parserState->cached_counter = 0;
    parserState->cached = false;

    parserState->token = NULL;
    parserState->char_size = 0;
    parserState->char_counter = 0;
    parserState->token_type = 0;
}

/*
 * @Description: init ngram parser with the source text and text length,
 *                      only support UTF8/GBK encoding now
 *
 * @in str: source text to parse
 * @in len: source text length
 */
static NgramZhParserState* ngramParserInit(char* str, int len, Oid cfgoid)
{
    NgramZhParserState* pst = &ngram_parser_state;
    TSConfigCacheEntry* entry = NULL;

    if (PG_GBK == GetDatabaseEncoding())
        pst->parserState.parser = gbk_parser;
    else if (PG_UTF8 == GetDatabaseEncoding())
        pst->parserState.parser = ngram_utf8_parser;
    else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ngram parser only support UTF8/GBK encoding")));

    zhParserStateInit(str, len, &pst->parserState);
    pst->cur_size = 0;
    pst->cur_counter = 0;
    pst->stat_tail = false;

    /*
     * Clear segmentation
     * read configuration's option to if we parse the string with a defined
     * configuration, or we use the guc para
     */
    if (OidIsValid(cfgoid))
        entry = lookup_ts_config_cache(cfgoid);

    if (entry != NULL && entry->opts != NULL) {
        NgramCfOpts* opts = (NgramCfOpts*)entry->opts;

        pst->gram_size = opts->gram_size;
        pst->punctuation_ignore = opts->punctuation_ignore;
        pst->grapsymbol_ignore = opts->grapsymbol_ignore;
    } else {
        pst->gram_size = u_sess->attr.attr_common.ngram_gram_size;
        pst->punctuation_ignore = u_sess->attr.attr_sql.ngram_punctuation_ignore;
        pst->grapsymbol_ignore = u_sess->attr.attr_sql.ngram_grapsymbol_ignore;
    }

    return pst;
}

/*
 * @Description: ngram parser
 *
 * @in ch:
 * @out len:
 * @return:
 */
static bool ngramParser(NgramZhParserState* pst)
{
    uint8 char_width;
    uint8 char_type;

    /* try to fectch output package from cache */
    if (likely(packageToken(pst)))
        return true;

    ZhParserState* parserState = &pst->parserState;
    /* done */
    if (unlikely(parserState->post == parserState->len))
        return false;

    Assert(parserState->cached == false);
    parserState->next = parserState->str + parserState->post; /* start to parse */
    do {
        /* done */
        if (unlikely(parserState->post + parserState->cached_size == parserState->len)) {
            if (unlikely(parserState->cached_size == 0))
                return false;

            parserState->post = parserState->len;
            cacheToken(pst);
            (void)packageToken(pst);

            return true;
        }

        char_type = (parserState->parser)(parserState->next, &char_width);
        if (likely(char_type == CHAR_ZHWORD || char_type == CHAR_DIGHT || char_type == CHAR_ENLETTER ||
                   char_type == CHAR_RADIXPOINT || (char_type == CHAR_GRAPSYMBOL && !pst->grapsymbol_ignore) ||
                   (char_type == CHAR_PUNCTUATION && !pst->punctuation_ignore))) {
            if (unlikely(parserState->cached_counter == 0))
                parserState->token = parserState->next;

            parserState->char_width[parserState->cached_counter] = char_width;
            parserState->char_flag[parserState->cached_counter] = char_type;
            parserState->cached_counter += 1;
            parserState->cached_size += char_width;
            parserState->next += char_width;

            if (likely(parserState->cached_counter < parserState->max_cached_num))
                continue;

            parserState->post += parserState->cached_size;

            for (int counter = 1; counter < pst->gram_size; counter++)
                parserState->post = parserState->post - parserState->char_width[parserState->cached_counter - counter];

            cacheToken(pst);
            (void)packageToken(pst);

            return true;
        } else if (parserState->cached_counter) { /* a separator */
            Assert(parserState->cached_size);
            parserState->next += char_width;
            parserState->post += parserState->cached_size;
            parserState->post += char_width;

            cacheToken(pst);
            (void)packageToken(pst);

            return true;
        } else { /* ignorable char */
            Assert(parserState->cached_size == 0);
            parserState->post += char_width;
            parserState->next += char_width;
        }
    } while (1);
}

/*
 * @Description: init ngram parser
 *
 * @in arg1: source text to parse
 * @in arg2: source text length
 * @in arg3: text search configuration
 * @return: ngram parser state
 */
Datum ngram_start(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(ngramParserInit((char*)PG_GETARG_POINTER(0), PG_GETARG_INT32(1), PG_GETARG_OID(2)));
}

/*
 * @Description: fetch next token
 *
 * @in arg1: ngram parser state
 * @out arg2: pointer to fetched package
 * @out arg3: length of the fetched package
 * @return: token type
 */
Datum ngram_nexttoken(PG_FUNCTION_ARGS)
{
    NgramZhParserState* pst = (NgramZhParserState*)PG_GETARG_POINTER(0);
    char** t = (char**)PG_GETARG_POINTER(1);
    int* tlen = (int*)PG_GETARG_POINTER(2);

    if (!ngramParser(pst))
        PG_RETURN_INT32(0);

    *t = pst->parserState.token;
    *tlen = pst->parserState.char_size;

    PG_RETURN_INT32(pst->parserState.token_type);
}

void zhParserStateDestroy(ZhParserState* parserState)
{
    parserState->parser = NULL;
    parserState->str = NULL;
    parserState->len = 0;
    parserState->next = 0;
    parserState->post = 0;

    parserState->cache = NULL;
    parserState->max_cached_num = 0;
    if (parserState->char_width != NULL) {
        pfree_ext(parserState->char_width);
        parserState->char_width = NULL;
    }
    if (parserState->char_flag != NULL) {
        pfree_ext(parserState->char_flag);
        parserState->char_flag = NULL;
    }

    parserState->cached_size = 0;
    parserState->cached_counter = 0;
    parserState->cached = false;

    parserState->token = NULL;
    parserState->char_size = 0;
    parserState->char_counter = 0;
    parserState->token_type = 0;
}

/*
 * @Description: cleanup ngram parser
 *
 * @in pst: ngram parser state
 */
Datum ngram_end(PG_FUNCTION_ARGS)
{
    NgramZhParserState* pst = (NgramZhParserState*)PG_GETARG_POINTER(0);

    zhParserStateDestroy(&pst->parserState);
    pst->cur_size = 0;
    pst->cur_counter = 0;
    pst->stat_tail = false;

    pst->gram_size = 2;
    pst->punctuation_ignore = true;
    pst->grapsymbol_ignore = false;

    PG_RETURN_VOID();
}

/*
 * @Description: returning type for prslextype method of parser
 *
 * @retutn: type for prslextype method of ngram
 */
Datum ngram_lextype(PG_FUNCTION_ARGS)
{
    LexDescr* descr = (LexDescr*)palloc(sizeof(LexDescr) * (NGRAM_TOKEN_NUM + 1));

    for (int i = 1; i <= NGRAM_TOKEN_NUM; i++) {
        descr[i - 1].lexid = i;
        descr[i - 1].alias = pstrdup(zh_tok_alias[i]);
        descr[i - 1].descr = pstrdup(zh_lex_descr[i]);
    }

    descr[NGRAM_TOKEN_NUM].lexid = 0;

    PG_RETURN_POINTER(descr);
}

