/* -------------------------------------------------------------------------
 *
 * wparser.c
 *		Standard interface to word parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/wparser.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"

typedef struct {
    int cur;
    LexDescr* list;
} TSTokenTypeStorage;

static void tt_setup_firstcall(FuncCallContext* funcctx, Oid prsid)
{
    TupleDesc tupdesc;
    MemoryContext oldcontext;
    TSTokenTypeStorage* st = NULL;
    TSParserCacheEntry* prs = lookup_ts_parser_cache(prsid);

    if (!OidIsValid(prs->lextypeOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("method lextype isn't defined for text search parser %u", prsid)));

    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    st = (TSTokenTypeStorage*)palloc(sizeof(TSTokenTypeStorage));
    st->cur = 0;
    /* lextype takes one dummy argument */
    st->list = (LexDescr*)DatumGetPointer(OidFunctionCall1(prs->lextypeOid, (Datum)0));
    funcctx->user_fctx = (void*)st;

    tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "tokid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "alias", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "description", TEXTOID, -1, 0);

    funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
    (void)MemoryContextSwitchTo(oldcontext);
}

static Datum tt_process_call(FuncCallContext* funcctx)
{
    TSTokenTypeStorage* st = NULL;

    st = (TSTokenTypeStorage*)funcctx->user_fctx;
    if (st->list && st->list[st->cur].lexid) {
        Datum result;
        char* values[3];
        char txtid[16];
        HeapTuple tuple;
        errno_t errorno = EOK;

        errorno = sprintf_s(txtid, sizeof(txtid), "%d", st->list[st->cur].lexid);
        securec_check_ss(errorno, "\0", "\0");
        values[0] = txtid;
        values[1] = st->list[st->cur].alias;
        values[2] = st->list[st->cur].descr;

        tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        pfree_ext(values[1]);
        pfree_ext(values[2]);
        st->cur++;
        return result;
    }
    if (st->list != NULL)
        pfree_ext(st->list);
    pfree_ext(st);
    return (Datum)0;
}

Datum ts_token_type_byid(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    FuncCallContext* funcctx = NULL;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        tt_setup_firstcall(funcctx, PG_GETARG_OID(0));
    }

    funcctx = SRF_PERCALL_SETUP();
    if ((result = tt_process_call(funcctx)) != (Datum)0)
        SRF_RETURN_NEXT(funcctx, result);
    SRF_RETURN_DONE(funcctx);
}

Datum ts_token_type_byname(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    FuncCallContext* funcctx = NULL;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        text* prsname = PG_GETARG_TEXT_P(0);
        Oid prsId;

        funcctx = SRF_FIRSTCALL_INIT();
        prsId = get_ts_parser_oid(textToQualifiedNameList(prsname), false);
        tt_setup_firstcall(funcctx, prsId);
    }

    funcctx = SRF_PERCALL_SETUP();
    if ((result = tt_process_call(funcctx)) != (Datum)0)
        SRF_RETURN_NEXT(funcctx, result);
    SRF_RETURN_DONE(funcctx);
}

typedef struct {
    int type;
    char* lexeme;
} LexemeEntry;

typedef struct {
    int cur;
    int len;
    LexemeEntry* list;
} PrsStorage;

static void prs_setup_firstcall(FuncCallContext* funcctx, Oid prsid, text* txt)
{
    TupleDesc tupdesc;
    MemoryContext oldcontext;
    PrsStorage* st = NULL;
    TSParserCacheEntry* prs = lookup_ts_parser_cache(prsid);
    char* lex = NULL;
    int llen = 0;
    int type = 0;
    void* prsdata = NULL;

    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    errno_t errorno = EOK;

    st = (PrsStorage*)palloc(sizeof(PrsStorage));
    st->cur = 0;
    st->len = 16;
    st->list = (LexemeEntry*)palloc(sizeof(LexemeEntry) * st->len);

    /*
     * Add paramater configuration oid, so the parser can fetch parser options, this
     * function is directly called by ts_parse(innner function ists_parse_byid/ts_parse_byname),
     * that means there is no given configuration for this parsing, in fact we will use
     * guc para to dominate the parsing
     */
    prsdata = (void*)DatumGetPointer(FunctionCall3(
        &prs->prsstart, PointerGetDatum(VARDATA(txt)), Int32GetDatum(VARSIZE(txt) - VARHDRSZ), ObjectIdGetDatum(0)));

    while ((type = DatumGetInt32(FunctionCall3(
                &prs->prstoken, PointerGetDatum(prsdata), PointerGetDatum(&lex), PointerGetDatum(&llen)))) != 0) {
        if (st->cur >= st->len) {
            st->len = 2 * st->len;
            st->list = (LexemeEntry*)repalloc(st->list, sizeof(LexemeEntry) * st->len);
        }
        st->list[st->cur].lexeme = (char*)palloc(llen + 1);
        errorno = memcpy_s(st->list[st->cur].lexeme, llen + 1, lex, llen);
        securec_check(errorno, "\0", "\0");
        st->list[st->cur].lexeme[llen] = '\0';
        st->list[st->cur].type = type;
        st->cur++;
    }

    FunctionCall1(&prs->prsend, PointerGetDatum(prsdata));

    st->len = st->cur;
    st->cur = 0;

    funcctx->user_fctx = (void*)st;
    tupdesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "tokid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "token", TEXTOID, -1, 0);

    funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
    (void)MemoryContextSwitchTo(oldcontext);
}

static Datum prs_process_call(FuncCallContext* funcctx)
{
    PrsStorage* st = NULL;

    st = (PrsStorage*)funcctx->user_fctx;
    if (st->cur < st->len) {
        Datum result;
        char* values[2];
        char tid[16];
        HeapTuple tuple;
        errno_t errorno = EOK;

        values[0] = tid;
        errorno = sprintf_s(tid, sizeof(tid), "%d", st->list[st->cur].type);
        securec_check_ss(errorno, "\0", "\0");
        values[1] = st->list[st->cur].lexeme;
        tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        pfree_ext(values[1]);
        st->cur++;
        return result;
    } else {
        pfree_ext(st->list);
        pfree_ext(st);
    }
    return (Datum)0;
}

Datum ts_parse_byid(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    FuncCallContext* funcctx = NULL;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        text* txt = PG_GETARG_TEXT_P(1);

        funcctx = SRF_FIRSTCALL_INIT();
        prs_setup_firstcall(funcctx, PG_GETARG_OID(0), txt);
        PG_FREE_IF_COPY(txt, 1);
    }

    funcctx = SRF_PERCALL_SETUP();
    if ((result = prs_process_call(funcctx)) != (Datum)0)
        SRF_RETURN_NEXT(funcctx, result);
    SRF_RETURN_DONE(funcctx);
}

Datum ts_parse_byname(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    FuncCallContext* funcctx = NULL;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        text* prsname = PG_GETARG_TEXT_P(0);
        text* txt = PG_GETARG_TEXT_P(1);
        Oid prsId;

        funcctx = SRF_FIRSTCALL_INIT();
        prsId = get_ts_parser_oid(textToQualifiedNameList(prsname), false);
        prs_setup_firstcall(funcctx, prsId, txt);
    }

    funcctx = SRF_PERCALL_SETUP();
    if ((result = prs_process_call(funcctx)) != (Datum)0)
        SRF_RETURN_NEXT(funcctx, result);
    SRF_RETURN_DONE(funcctx);
}

Datum ts_headline_byid_opt(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    text* in = PG_GETARG_TEXT_P(1);
    TSQuery query = PG_GETARG_TSQUERY(2);
    text* opt = (PG_NARGS() > 3 && PG_GETARG_POINTER(3)) ? PG_GETARG_TEXT_P(3) : NULL;
    HeadlineParsedText prs;
    List* prsoptions = NIL;
    text* out = NULL;
    TSConfigCacheEntry* cfg = NULL;
    TSParserCacheEntry* prsobj = NULL;

    cfg = lookup_ts_config_cache(PG_GETARG_OID(0));
    prsobj = lookup_ts_parser_cache(cfg->prsId);
    errno_t errorno = EOK;

    if (!OidIsValid(prsobj->headlineOid))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("text search parser does not support headline creation")));

    errorno = memset_s(&prs, sizeof(HeadlineParsedText), 0, sizeof(HeadlineParsedText));
    securec_check(errorno, "\0", "\0");
    prs.lenwords = 32;
    prs.words = (HeadlineWordEntry*)palloc(sizeof(HeadlineWordEntry) * prs.lenwords);

    hlparsetext(cfg->cfgId, &prs, query, VARDATA(in), VARSIZE(in) - VARHDRSZ);

    if (opt != NULL)
        prsoptions = deserialize_deflist(PointerGetDatum(opt));
    else
        prsoptions = NIL;

    FunctionCall3(&(prsobj->prsheadline), PointerGetDatum(&prs), PointerGetDatum(prsoptions), PointerGetDatum(query));

    out = generateHeadline(&prs);

    PG_FREE_IF_COPY(in, 1);
    PG_FREE_IF_COPY(query, 2);
    if (opt != NULL)
        PG_FREE_IF_COPY(opt, 3);
    pfree_ext(prs.words);
    pfree_ext(prs.startsel);
    pfree_ext(prs.stopsel);

    PG_RETURN_POINTER(out);
}

Datum ts_headline_byid(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    PG_RETURN_DATUM(
        DirectFunctionCall3(ts_headline_byid_opt, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1), PG_GETARG_DATUM(2)));
}

Datum ts_headline(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    PG_RETURN_DATUM(DirectFunctionCall3(
        ts_headline_byid_opt, ObjectIdGetDatum(getTSCurrentConfig(true)), PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)));
}

Datum ts_headline_opt(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    PG_RETURN_DATUM(DirectFunctionCall4(ts_headline_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM(0),
        PG_GETARG_DATUM(1),
        PG_GETARG_DATUM(2)));
}
