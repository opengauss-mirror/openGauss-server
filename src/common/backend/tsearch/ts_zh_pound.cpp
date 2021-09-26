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

 * ts_zh_pound.cpp
 *    '#' split functions for a full-text search of Chinese/English method, it is a independent
 *    module, and only can be executed when we use pound parser directly(such as ts_parse
 *    with pound parser) or indirectly(such as to_tsvector with text seach configuration
 *    defined with pound parser).
 *
 *    As a parser we have got to abide by openGauss full-text search rules, and define 4
 *    functions pound_start as start_function, pound_nexttoken as gettoken_function,
 *    pound_end as end_function, and pound_lextype. And, we use prsd_headline
 *    defined for default parser as pound'ss headline_function
 *
 * IDENTIFICATION
 *    src/common/backend/tsearch/ts_zh_pound.cpp
 *

 * Usage Attention:
 * Only support GBK/UTF8/ASCII encoding method. And Chinese is not supported by ASCII.
 *
 * -------------------------------------------------------------------------
 */

#include <ctype.h>
#include "tsearch/ts_zh_ngram.h"
#include "tsearch/ts_zh_pound.h"
#include "tsearch/ts_public.h"
#include "nodes/bitmapset.h"
#include "tsearch/ts_cache.h"

/* pound parser state */
typedef struct {
    ZhParserState parserState;
    char* split_flag;
} PoundZhParserState;

static THR_LOCAL PoundZhParserState pound_parser_state;

/* char type defined for pound text search */
#define POUND_TOKEN_NUM 6

/**
 * @Description: map to char type with ascii encoding. without regard for extensition
 *                      range, since we just deal with GBK/UTF8 encoding
 *
 * @in ch: ascii char pointer to parser
 * @return: char type defined in front
 */
static inline uint8 gbk_ascii_parser(const char* ch)
{
    if ((to_unsigned(ch) & 0x80) != 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Chinese is not supported by ASCII!")));
    }
    Assert((to_unsigned(ch) & 0x80) == 0);

    return ascii_matrix[HFB(ch) & 0x07][LFB(ch)];
}

/*
 * @Description: maps to char type for a ASCII endcoding char
 *
 * @in ch: ASCII endcoding char
 * @return: predefined char type
 */
static inline uint8 ascii_parser(const char* ch, uint8* len)
{
    *len = 1;
    return gbk_ascii_parser(ch);
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

/********************************* pound parser  *******************************/
/*
 * @Description: show clearly the region that we have parsed with the starting pointer
 *                      and the legth, now we can use it just like a cache
 *
 * @in pst: pound parse state
 */
static inline void cacheToken(PoundZhParserState* pst)
{
    Assert(pst->parserState.cached == false);
    Assert(pst->parserState.cached_counter && pst->parserState.cached_size);

    pst->parserState.token = pst->parserState.cache;
    pst->parserState.cached = true;
}

static inline uint8 pound_utf8_parser(const char* ch, uint8* len)
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
 * @in pst: pound parse state
 * @return: return true if succeed to fetch a token, else return false
 */
static inline bool packageTokenMulti(PoundZhParserState* pst)
{
    if (unlikely(!pst->parserState.cached)) {
        return false;
    }

    pst->parserState.char_size = pst->parserState.cached_size;
    pst->parserState.char_counter = pst->parserState.cached_counter;
    pst->parserState.token_type = MULTISYMBOL;

    pst->parserState.cache = NULL;
    pst->parserState.cached = false;
    pst->parserState.cached_size = 0;
    pst->parserState.cached_counter = 0;

    return true;
}

/*
 * @Description: fetch tokens from cahce region
 *
 * @in pst: pound parser state
 */
static inline bool packageToken(PoundZhParserState* pst)
{
    return packageTokenMulti(pst);
}

/*
 * @Description: init pound parser with the source text and text length,
 *                      only support UTF8/GBK/ASCII encoding now
 *
 * @in str: source text to parse
 * @in len: source text length
 */
static PoundZhParserState* poundParserInit(char* str, int len, Oid cfgoid)
{
    PoundZhParserState* pst = &pound_parser_state;
    TSConfigCacheEntry* entry = NULL;

    if (PG_GBK == GetDatabaseEncoding()) {
        pst->parserState.parser = gbk_parser;
    } else if (PG_UTF8 == GetDatabaseEncoding()) {
        pst->parserState.parser = pound_utf8_parser;
    } else if (PG_SQL_ASCII == GetDatabaseEncoding()) {
        pst->parserState.parser = ascii_parser;
    } else { 
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("pound parser only support UTF8/GBK/ASCII encoding")));
    }
    zhParserStateInit(str, len, &pst->parserState);

    /*
     * Clear segmentation
     * read configuration's option to if we parse the string with a defined
     * configuration, or we use the guc para
     */
    if (OidIsValid(cfgoid)) {
        entry = lookup_ts_config_cache(cfgoid);
        Assert(entry && entry->opts);
        PoundCfOpts* opts = (PoundCfOpts*)entry->opts;
        pst->split_flag = (char*)opts + (long)opts->split_flag;
    } else {
        pst->split_flag = "#";
    }
    return pst;
}

/*
 * @Description: pound parser
 *
 * @in ch:
 * @out len:
 * @return:
 */
static bool poundParser(PoundZhParserState* pst)
{
    uint8 char_width;
    uint8 char_type;

    /* try to fetch output package from cache */
    if (packageToken(pst)) {
        return true;
    }

    /* done */
    if (unlikely(pst->parserState.post == pst->parserState.len)) {
        return false;
    }

    Assert(pst->parserState.cached == false);
    pst->parserState.next = pst->parserState.str + pst->parserState.post; /* start to parse */
    do {
        /* done */
        if (unlikely(pst->parserState.post + pst->parserState.cached_size == pst->parserState.len)) {
            if (unlikely(pst->parserState.cached_size == 0)) {
                return false;
            }

            pst->parserState.post = pst->parserState.len;
            cacheToken(pst);
            (void)packageToken(pst);

            return true;
        }

        char_type = (pst->parserState.parser)(pst->parserState.next, &char_width);

        if (unlikely(pst->parserState.cached_counter == 0)) {
            pst->parserState.cache = pst->parserState.next;
        }

        if (*pst->parserState.next == *pst->split_flag ||
            pst->parserState.cached_counter >= pst->parserState.max_cached_num) {
            pst->parserState.post += pst->parserState.cached_size;
            if (*pst->parserState.next == *pst->split_flag) {
                pst->parserState.post += char_width;
                pst->parserState.next += char_width;
            }

            if (pst->parserState.cached_counter && pst->parserState.cached_size) {
                cacheToken(pst);
                (void)packageToken(pst);
                return true;
            }
        } else {
            pst->parserState.char_width[pst->parserState.cached_counter] = char_width;
            pst->parserState.char_flag[pst->parserState.cached_counter] = char_type;
            pst->parserState.cached_counter += 1;
            pst->parserState.cached_size += char_width;
            pst->parserState.next += char_width;
        }
    } while (1);
}

/*
 * @Description: init pound parser
 *
 * @in arg1: source text to parse
 * @in arg2: source text length
 * @in arg3: text search configuration
 *@return: pound parser state
 */
 
Datum pound_start(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(poundParserInit((char*)PG_GETARG_POINTER(0), PG_GETARG_INT32(1), PG_GETARG_OID(2)));
}

/*
 * @Description: fetch next token
 *
 * @in arg1: pound parser state
 * @out arg2: pointer to fetched package
 * @out arg3: length of the fetched package
 * @return: token type
 */
Datum pound_nexttoken(PG_FUNCTION_ARGS)
{
    PoundZhParserState* pst = (PoundZhParserState*)PG_GETARG_POINTER(0);
    char** t = (char**)PG_GETARG_POINTER(1);
    int* tlen = (int*)PG_GETARG_POINTER(2);

    if (!poundParser(pst)) {
        PG_RETURN_INT32(0);
    }

    *t = pst->parserState.token;
    *tlen = pst->parserState.char_size;

    PG_RETURN_INT32(pst->parserState.token_type);
}

/*
 * @Description: cleanup pound parser
 *
 * @in pst: pound parser state
 */
Datum pound_end(PG_FUNCTION_ARGS)
{

    PoundZhParserState* pst = (PoundZhParserState*)PG_GETARG_POINTER(0);

    zhParserStateDestroy(&pst->parserState);

    pst->split_flag = "#";

    PG_RETURN_VOID();
}

/*
 * @Description: returning type for prslextype method of parser
 *
 * @retutn: type for prslextype method of pound
 */
Datum pound_lextype(PG_FUNCTION_ARGS)
{
    LexDescr* descr = (LexDescr*)palloc(sizeof(LexDescr) * (POUND_TOKEN_NUM + 1));

    for (int i = 1; i <= POUND_TOKEN_NUM; i++) {
        descr[i - 1].lexid = i;
        descr[i - 1].alias = pstrdup(zh_tok_alias[i]);
        descr[i - 1].descr = pstrdup(zh_lex_descr[i]);
    }

    descr[POUND_TOKEN_NUM].lexid = 0;

    PG_RETURN_POINTER(descr);
}

