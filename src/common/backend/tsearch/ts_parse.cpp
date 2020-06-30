/* -------------------------------------------------------------------------
 *
 * ts_parse.c
 *		main parse functions for tsearch
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/ts_parse.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "tsearch/ts_cache.h"
#include "tsearch/ts_utils.h"

#define IGNORE_LONGLEXEME 1

/*
 * Lexize subsystem
 */
typedef struct ParsedLex {
    int type;
    char* lemm;
    int lenlemm;
    struct ParsedLex* next;
} ParsedLex;

typedef struct ListParsedLex {
    ParsedLex* head;
    ParsedLex* tail;
} ListParsedLex;

typedef struct {
    TSConfigCacheEntry* cfg;
    Oid curDictId;
    int posDict;
    DictSubState dictState;
    ParsedLex* curSub;
    ListParsedLex towork; /* current list to work */
    ListParsedLex waste;  /* list of lexemes that already lexized */

    /*
     * fields to store last variant to lexize (basically, thesaurus or similar
     * to, which wants	several lexemes
     */
    ParsedLex* lastRes;
    TSLexeme* tmpRes;
} LexizeData;

static void LexizeInit(LexizeData* ld, TSConfigCacheEntry* cfg)
{
    ld->cfg = cfg;
    ld->curDictId = InvalidOid;
    ld->posDict = 0;
    ld->towork.head = ld->towork.tail = ld->curSub = NULL;
    ld->waste.head = ld->waste.tail = NULL;
    ld->lastRes = NULL;
    ld->tmpRes = NULL;
}

static void LPLAddTail(ListParsedLex* list, ParsedLex* newpl)
{
    if (list->tail != NULL) {
        list->tail->next = newpl;
        list->tail = newpl;
    } else {
        list->head = list->tail = newpl;
    }
    newpl->next = NULL;
}

static ParsedLex* LPLRemoveHead(ListParsedLex* list)
{
    ParsedLex* res = list->head;

    if (list->head != NULL) {
        list->head = list->head->next;
    }

    if (list->head == NULL) {
        list->tail = NULL;
    }

    return res;
}

static void LexizeAddLemm(LexizeData* ld, int type, char* lemm, int lenlemm)
{
    ParsedLex* newpl = (ParsedLex*)palloc(sizeof(ParsedLex));

    newpl->type = type;
    newpl->lemm = lemm;
    newpl->lenlemm = lenlemm;
    LPLAddTail(&ld->towork, newpl);
    ld->curSub = ld->towork.tail;
}

static void RemoveHead(LexizeData* ld)
{
    LPLAddTail(&ld->waste, LPLRemoveHead(&ld->towork));

    ld->posDict = 0;
}

static void setCorrLex(LexizeData* ld, ParsedLex** correspondLexem)
{
    if (correspondLexem != NULL) {
        *correspondLexem = ld->waste.head;
    } else {
        ParsedLex *tmp = NULL;
        ParsedLex *ptr = ld->waste.head;

        while (ptr != NULL) {
            tmp = ptr->next;
            pfree_ext(ptr);
            ptr = tmp;
        }
    }
    ld->waste.head = ld->waste.tail = NULL;
}

static void moveToWaste(LexizeData* ld, ParsedLex* stop)
{
    bool go = true;

    while (ld->towork.head && go) {
        if (ld->towork.head == stop) {
            ld->curSub = stop->next;
            go = false;
        }
        RemoveHead(ld);
    }
}

static void setNewTmpRes(LexizeData* ld, ParsedLex* lex, TSLexeme* res)
{
    if (ld->tmpRes != NULL) {
        TSLexeme* ptr = NULL;

        for (ptr = ld->tmpRes; ptr->lexeme; ptr++) {
            pfree_ext(ptr->lexeme);
        }
        pfree_ext(ld->tmpRes);
    }
    ld->tmpRes = res;
    ld->lastRes = lex;
}

static TSLexeme* LexizeExec(LexizeData* ld, ParsedLex** correspondLexem)
{
    int i;
    ListDictionary* map = NULL;
    TSDictionaryCacheEntry* dict = NULL;
    TSLexeme* res = NULL;

    if (ld->curDictId == InvalidOid) {
        /*
         * usial mode: dictionary wants only one word, but we should keep in
         * mind that we should go through all stack
         */
        while (ld->towork.head) {
            ParsedLex* curVal = ld->towork.head;
            char* curValLemm = curVal->lemm;
            int curValLenLemm = curVal->lenlemm;

            map = ld->cfg->map + curVal->type;

            if (curVal->type == 0 || curVal->type >= ld->cfg->lenmap || map->len == 0) {
                /* skip this type of lexeme */
                RemoveHead(ld);
                continue;
            }

            for (i = ld->posDict; i < map->len; i++) {
                dict = lookup_ts_dictionary_cache(map->dictIds[i]);

                ld->dictState.isend = ld->dictState.getnext = false;
                ld->dictState.private_state = NULL;
                res = (TSLexeme*)DatumGetPointer(FunctionCall4(&(dict->lexize),
                    PointerGetDatum(dict->dictData),
                    PointerGetDatum(curValLemm),
                    Int32GetDatum(curValLenLemm),
                    PointerGetDatum(&ld->dictState)));

                if (ld->dictState.getnext) {
                    /*
                     * dictionary wants next word, so setup and store current
                     * position and go to multiword mode
                     */
                    ld->curDictId = DatumGetObjectId(map->dictIds[i]);
                    ld->posDict = i + 1;
                    ld->curSub = curVal->next;
                    if (res != NULL)
                        setNewTmpRes(ld, curVal, res);
                    return LexizeExec(ld, correspondLexem);
                }

                if (res == NULL) /* dictionary doesn't know this lexeme */
                    continue;

                if (res->flags & TSL_FILTER) {
                    curValLemm = res->lexeme;
                    curValLenLemm = strlen(res->lexeme);
                    continue;
                }

                RemoveHead(ld);
                setCorrLex(ld, correspondLexem);
                return res;
            }

            RemoveHead(ld);
        }
    } else { /* curDictId is valid */
        dict = lookup_ts_dictionary_cache(ld->curDictId); /* Dictionary ld->curDictId asks  us about following words */

        while (ld->curSub != NULL) {
            ParsedLex* curVal = ld->curSub;

            map = ld->cfg->map + curVal->type;

            if (curVal->type != 0) {
                bool dictExists = false;

                if (curVal->type >= ld->cfg->lenmap || map->len == 0) {
                    /* skip this type of lexeme */
                    ld->curSub = curVal->next;
                    continue;
                }

                /*
                 * We should be sure that current type of lexeme is recognized
                 * by our dictinonary: we just check is it exist in list of
                 * dictionaries ?
                 */
                for (i = 0; i < map->len && !dictExists; i++) {
                    if (ld->curDictId == DatumGetObjectId(map->dictIds[i])) {
                        dictExists = true;
                    }
                }

                if (!dictExists) {
                    /*
                     * Dictionary can't work with current tpe of lexeme,
                     * return to basic mode and redo all stored lexemes
                     */
                    ld->curDictId = InvalidOid;
                    return LexizeExec(ld, correspondLexem);
                }
            }

            ld->dictState.isend = (curVal->type == 0) ? true : false;
            ld->dictState.getnext = false;

            res = (TSLexeme*)DatumGetPointer(FunctionCall4(&(dict->lexize),
                PointerGetDatum(dict->dictData),
                PointerGetDatum(curVal->lemm),
                Int32GetDatum(curVal->lenlemm),
                PointerGetDatum(&ld->dictState)));

            if (ld->dictState.getnext) {
                /* Dictionary wants one more */
                ld->curSub = curVal->next;
                if (res != NULL)
                    setNewTmpRes(ld, curVal, res);
                continue;
            }

            if (res != NULL || ld->tmpRes != NULL) {
                /*
                 * Dictionary normalizes lexemes, so we remove from stack all
                 * used lexemes, return to basic mode and redo end of stack
                 * (if it exists)
                 */
                if (res != NULL) {
                    moveToWaste(ld, ld->curSub);
                } else {
                    res = ld->tmpRes;
                    moveToWaste(ld, ld->lastRes);
                }

                /* reset to initial state */
                ld->curDictId = InvalidOid;
                ld->posDict = 0;
                ld->lastRes = NULL;
                ld->tmpRes = NULL;
                setCorrLex(ld, correspondLexem);
                return res;
            }

            /*
             * Dict don't want next lexem and didn't recognize anything, redo
             * from ld->towork.head
             */
            ld->curDictId = InvalidOid;
            return LexizeExec(ld, correspondLexem);
        }
    }

    setCorrLex(ld, correspondLexem);
    return NULL;
}

/*
 * Parse string and lexize words.
 *
 * prs will be filled in.
 */
void parsetext(Oid cfgId, ParsedText* prs, const char* buf, int buflen)
{
    int type, lenlemm;
    char* lemm = NULL;
    LexizeData ldata;
    TSLexeme* norms = NULL;
    TSConfigCacheEntry* cfg = NULL;
    TSParserCacheEntry* prsobj = NULL;
    void* prsdata = NULL;

    cfg = lookup_ts_config_cache(cfgId);
    prsobj = lookup_ts_parser_cache(cfg->prsId);

    /* Add paramater configuration oid, so the parser can fetch parser options */
    prsdata = (void*)DatumGetPointer(
        FunctionCall3(&prsobj->prsstart, PointerGetDatum(buf), Int32GetDatum(buflen), ObjectIdGetDatum(cfgId)));

    LexizeInit(&ldata, cfg);

    do {
        type = DatumGetInt32(FunctionCall3(
            &(prsobj->prstoken), PointerGetDatum(prsdata), PointerGetDatum(&lemm), PointerGetDatum(&lenlemm)));

        if (type > 0 && lenlemm >= MAXSTRLEN) {
#ifdef IGNORE_LONGLEXEME
            ereport(NOTICE,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("word is too long to be indexed"),
                    errdetail("Words longer than %d characters are ignored.", MAXSTRLEN),
                    handle_in_client(true)));
            continue;
#else
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("word is too long to be indexed"),
                    errdetail("Words longer than %d characters are ignored.", MAXSTRLEN)));
#endif
        }

        LexizeAddLemm(&ldata, type, lemm, lenlemm);

        while ((norms = LexizeExec(&ldata, NULL)) != NULL) {
            TSLexeme* ptr = norms;

            prs->pos++; /* set pos */

            while (ptr->lexeme) {
                if (prs->curwords >= prs->lenwords) {
                    prs->lenwords = 2 * prs->curwords;
                    prs->words = (ParsedWord*)repalloc((void*)prs->words, prs->lenwords * sizeof(ParsedWord));
                }

                if (ptr->flags & TSL_ADDPOS)
                    prs->pos++;
                prs->words[prs->curwords].len = strlen(ptr->lexeme);
                prs->words[prs->curwords].word = ptr->lexeme;
                prs->words[prs->curwords].nvariant = ptr->nvariant;
                prs->words[prs->curwords].flags = ptr->flags & TSL_PREFIX;
                prs->words[prs->curwords].alen = 0;
                prs->words[prs->curwords].pos.pos = LIMITPOS(prs->pos);
                ptr++;
                prs->curwords++;
            }
            pfree_ext(norms);
        }
    } while (type > 0);

    FunctionCall1(&(prsobj->prsend), PointerGetDatum(prsdata));
}

/*
 * Headline framework
 */
static void hladdword(HeadlineParsedText* prs, const char* buf, int buflen, int type)
{
    errno_t errorno = EOK;
    while (prs->curwords >= prs->lenwords) {
        prs->lenwords *= 2;
        prs->words = (HeadlineWordEntry*)repalloc((void*)prs->words, prs->lenwords * sizeof(HeadlineWordEntry));
    }
    errorno = memset_s(&(prs->words[prs->curwords]), sizeof(HeadlineWordEntry), 0, sizeof(HeadlineWordEntry));
    securec_check(errorno, "\0", "\0");
    prs->words[prs->curwords].type = (uint8)type;
    prs->words[prs->curwords].len = buflen;
    prs->words[prs->curwords].word = (char*)palloc(buflen);
    errorno = memcpy_s(prs->words[prs->curwords].word, buflen, buf, buflen);
    securec_check(errorno, "\0", "\0");
    prs->curwords++;
}

static void hlfinditem(HeadlineParsedText* prs, TSQuery query, const char* buf, int buflen)
{
    int i;
    QueryItem* item = GETQUERY(query);
    HeadlineWordEntry* word = NULL;
    errno_t errorno = EOK;

    while (prs->curwords + query->size >= prs->lenwords) {
        prs->lenwords *= 2;
        prs->words = (HeadlineWordEntry*)repalloc((void*)prs->words, prs->lenwords * sizeof(HeadlineWordEntry));
    }

    word = &(prs->words[prs->curwords - 1]);
    for (i = 0; i < query->size; i++) {
        if (item->type == QI_VAL && tsCompareString(GETOPERAND(query) + item->qoperand.distance,item->qoperand.length,
                                                    buf, buflen, item->qoperand.prefix) == 0) {
            if (word->item) {
                errorno =
                    memcpy_s(&(prs->words[prs->curwords]), sizeof(HeadlineWordEntry), word, sizeof(HeadlineWordEntry));
                securec_check(errorno, "\0", "\0");
                prs->words[prs->curwords].item = &item->qoperand;
                prs->words[prs->curwords].repeated = 1;
                prs->curwords++;
            } else {
                word->item = &item->qoperand;
            }
        }
        item++;
    }
}

static void addHLParsedLex(HeadlineParsedText* prs, TSQuery query, ParsedLex* lexs, TSLexeme* norms)
{
    ParsedLex* tmplexs = NULL;
    TSLexeme* ptr = NULL;

    while (lexs != NULL) {

        if (lexs->type > 0) {
            hladdword(prs, lexs->lemm, lexs->lenlemm, lexs->type);
        }

        ptr = norms;
        while (ptr != NULL && ptr->lexeme != NULL) {
            hlfinditem(prs, query, ptr->lexeme, strlen(ptr->lexeme));
            ptr++;
        }
        tmplexs = lexs->next;
        pfree_ext(lexs);
        lexs = tmplexs;
    }

    if (norms != NULL) {
        ptr = norms;
        while (ptr->lexeme) {
            pfree_ext(ptr->lexeme);
            ptr++;
        }
        pfree_ext(norms);
    }
}

void hlparsetext(Oid cfgId, HeadlineParsedText* prs, TSQuery query, const char* buf, int buflen)
{
    int type, lenlemm;
    char* lemm = NULL;
    LexizeData ldata;
    TSLexeme* norms = NULL;
    ParsedLex* lexs = NULL;
    TSConfigCacheEntry* cfg = NULL;
    TSParserCacheEntry* prsobj = NULL;
    void* prsdata = NULL;

    cfg = lookup_ts_config_cache(cfgId);
    prsobj = lookup_ts_parser_cache(cfg->prsId);

    /* Add paramater configuration oid, so the parser can fetch parser options */
    prsdata = (void*)DatumGetPointer(
        FunctionCall3(&(prsobj->prsstart), PointerGetDatum(buf), Int32GetDatum(buflen), ObjectIdGetDatum(cfgId)));

    LexizeInit(&ldata, cfg);

    do {
        type = DatumGetInt32(FunctionCall3(
            &(prsobj->prstoken), PointerGetDatum(prsdata), PointerGetDatum(&lemm), PointerGetDatum(&lenlemm)));

        if (type > 0 && lenlemm >= MAXSTRLEN) {
#ifdef IGNORE_LONGLEXEME
            ereport(NOTICE,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("word is too long to be indexed"),
                    errdetail("Words longer than %d characters are ignored.", MAXSTRLEN),
                    handle_in_client(true)));
            continue;
#else
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("word is too long to be indexed"),
                    errdetail("Words longer than %d characters are ignored.", MAXSTRLEN)));
#endif
        }

        LexizeAddLemm(&ldata, type, lemm, lenlemm);

        do {
            if ((norms = LexizeExec(&ldata, &lexs)) != NULL) {
                addHLParsedLex(prs, query, lexs, norms);
            } else {
                addHLParsedLex(prs, query, lexs, NULL);
            }
        } while (norms != NULL);

    } while (type > 0);

    FunctionCall1(&(prsobj->prsend), PointerGetDatum(prsdata));
}

text* generateHeadline(HeadlineParsedText* prs)
{
    text* out = NULL;
    char* ptr = NULL;
    int len = 128;
    int numfragments = 0;
    int2 infrag = 0;

    HeadlineWordEntry* wrd = prs->words;

    out = (text*)palloc(len);
    ptr = ((char*)out) + VARHDRSZ;
    errno_t errorno = EOK;

    while (wrd - prs->words < prs->curwords) {
        while (wrd->len + prs->stopsellen + prs->startsellen + prs->fragdelimlen + (ptr - ((char*)out)) >= len) {
            int dist = ptr - ((char*)out);
            len *= 2;
            out = (text*)repalloc(out, len);
            ptr = ((char*)out) + dist;
        }

        if (wrd->in && !wrd->repeated) {
            if (!infrag) {

                /* start of a new fragment */
                infrag = 1;
                numfragments++;
                /* add a fragment delimitor if this is after the first one */
                if (numfragments > 1) {
                    errorno = memcpy_s(ptr, prs->fragdelimlen, prs->fragdelim, prs->fragdelimlen);
                    securec_check(errorno, "\0", "\0");
                    ptr += prs->fragdelimlen;
                }
            }
            if (wrd->replace) {
                *ptr = ' ';
                ptr++;
            } else if (!wrd->skip) {
                if (wrd->selected) {
                    errorno = memcpy_s(ptr, prs->startsellen, prs->startsel, prs->startsellen);
                    securec_check(errorno, "\0", "\0");
                    ptr += prs->startsellen;
                }
                errorno = memcpy_s(ptr, wrd->len, wrd->word, wrd->len);
                securec_check(errorno, "\0", "\0");
                ptr += wrd->len;
                if (wrd->selected) {
                    errorno = memcpy_s(ptr, prs->stopsellen, prs->stopsel, prs->stopsellen);
                    securec_check(errorno, "\0", "\0");
                    ptr += prs->stopsellen;
                }
            }
        } else if (!wrd->repeated) {
            if (infrag) {
                infrag = 0;
            }
            pfree_ext(wrd->word);
        }
        wrd++;
    }

    SET_VARSIZE(out, ptr - ((char*)out));
    return out;
}

