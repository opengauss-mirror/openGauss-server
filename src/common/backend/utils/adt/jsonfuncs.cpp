/* -------------------------------------------------------------------------
 *
 * jsonfuncs.c
 *      Functions to process JSON data types.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/jsonfuncs.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/jsonapi.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/array.h"

/* Operations available for setPath */
#define JB_PATH_CREATE                  0x0001
#define JB_PATH_DELETE                  0x0002
#define JB_PATH_REPLACE                 0x0004
#define JB_PATH_INSERT_BEFORE           0x0008
#define JB_PATH_INSERT_AFTER            0x0010
#define JB_PATH_CREATE_OR_INSERT        (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE)
#define JB_PATH_FILL_GAPS               0x0020
#define JB_PATH_CONSISTENT_POSITION     0x0040

/* semantic action functions for json_object_keys */
static void okeys_object_field_start(void *state, char *fname, bool isnull);
static void okeys_array_start(void *state);
static void okeys_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for json_get* functions */
static void get_object_start(void *state);
static void get_object_field_start(void *state, char *fname, bool isnull);
static void get_object_field_end(void *state, char *fname, bool isnull);
static void get_array_start(void *state);
static void get_array_element_start(void *state, bool isnull);
static void get_array_element_end(void *state, bool isnull);
static void get_scalar(void *state, char *token, JsonTokenType tokentype);

/* common worker function for json getter functions */
static inline Datum get_path_all(FunctionCallInfo fcinfo, bool as_text);
static inline text *get_worker(text *json, char *field, int elem_index,
    char **tpath, int *ipath, int npath, bool normalize_results);
static inline Datum get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text);

/* semantic action functions for json_array_length */
static void alen_object_start(void *state);
static void alen_scalar(void *state, char *token, JsonTokenType tokentype);
static void alen_array_element_start(void *state, bool isnull);

/* common workers for json{b}_each* functions */
static inline Datum each_worker(FunctionCallInfo fcinfo, bool as_text);
static inline Datum each_worker_jsonb(FunctionCallInfo fcinfo, bool as_text);

/* semantic action functions for json_each */
static void each_object_field_start(void *state, char *fname, bool isnull);
static void each_object_field_end(void *state, char *fname, bool isnull);
static void each_array_start(void *state);
static void each_scalar(void *state, char *token, JsonTokenType tokentype);

/* common workers for json{b}_array_elements_* functions */
static inline Datum elements_worker(FunctionCallInfo fcinfo, bool as_text);
static inline Datum elements_worker_jsonb(FunctionCallInfo fcinfo, bool as_text);

/* semantic action functions for json_array_elements */
static void elements_object_start(void *state);
static void elements_array_element_start(void *state, bool isnull);
static void elements_array_element_end(void *state, bool isnull);
static void elements_scalar(void *state, char *token, JsonTokenType tokentype);

/* turn a json object into a hash table */
static HTAB *get_json_object_as_hash(text *json, char *funcname, bool use_json_as_text);

/* common worker for populate_record and to_record */
static inline Datum populate_record_worker(FunctionCallInfo fcinfo, bool have_record_arg);

/* semantic action functions for get_json_object_as_hash */
static void hash_object_field_start(void *state, char *fname, bool isnull);
static void hash_object_field_end(void *state, char *fname, bool isnull);
static void hash_array_start(void *state);
static void hash_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for populate_recordset */
static void populate_recordset_object_field_start(void *state, char *fname, bool isnull);
static void populate_recordset_object_field_end(void *state, char *fname, bool isnull);
static void populate_recordset_scalar(void *state, char *token, JsonTokenType tokentype);
static void populate_recordset_object_start(void *state);
static void populate_recordset_object_end(void *state);
static void populate_recordset_array_start(void *state);
static void populate_recordset_array_element_start(void *state, bool isnull);

/* worker function for populate_recordset and to_recordset */
static inline Datum populate_recordset_worker(FunctionCallInfo fcinfo, bool have_record_arg);
/* Worker that takes care of common setup for us */
static JsonbValue *findJsonbValueFromSuperHeaderLen(JsonbSuperHeader sheader, uint32 flags, char *key,  uint32 keylen);

/* functions supporting jsonb_delete, jsonb_set and jsonb_concat */
static void addJsonbToParseState(JsonbParseState **pstate, Jsonb *jb);
static JsonbValue *setPath(JsonbIterator **it, Datum *path_elems, bool *path_nulls, int path_len,
                            JsonbParseState **st, int level, Jsonb *newval, int op_type);
static void setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls, int path_len, JsonbParseState **st,
                            int level, Jsonb *newval, uint32 npairs, int op_type);
static void setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls, int path_len, JsonbParseState **st,
                            int level, Jsonb *newval, uint32 nelems, int op_type);

/* search type classification for json_get* functions */
typedef enum {
    JSON_SEARCH_OBJECT = 1,
    JSON_SEARCH_ARRAY,
    JSON_SEARCH_PATH
} JsonSearch;

/* state for json_object_keys */
typedef struct OkeysState {
    JsonLexContext *lex;
    char      **result;
    int         result_size;
    int         result_count;
    int         sent_count;
} OkeysState;

/* state for json_get* functions */
typedef struct GetState {
    JsonLexContext *lex;
    JsonSearch  search_type;
    int         search_index;
    int         array_index;
    char       *search_term;
    char       *result_start;
    text       *tresult;
    bool        result_is_null;
    bool        normalize_results;
    bool        next_scalar;
    char      **path;
    int         npath;
    char      **current_path;
    bool       *pathok;
    int        *array_level_index;
    int        *path_level_index;
} GetState;

/* state for json_array_length */
typedef struct AlenState {
    JsonLexContext *lex;
    int             count;
} AlenState;

/* state for json_each */
typedef struct EachState {
    JsonLexContext  *lex;
    Tuplestorestate *tuple_store;
    TupleDesc        ret_tdesc;
    MemoryContext    tmp_cxt;
    char            *result_start;
    bool             normalize_results;
    bool             next_scalar;
    char            *normalized_scalar;
} EachState;

/* state for json_array_elements */
typedef struct ElementsState {
    JsonLexContext  *lex;
    Tuplestorestate *tuple_store;
    TupleDesc        ret_tdesc;
    MemoryContext    tmp_cxt;
    char            *result_start;
    bool             normalize_results;
    bool             next_scalar;
    char            *normalized_scalar;
} ElementsState;

/* state for get_json_object_as_hash */
typedef struct JhashState {
    JsonLexContext *lex;
    HTAB           *hash;
    char           *saved_scalar;
    char           *save_json_start;
    bool            use_json_as_text;
    char           *function_name;
} JHashState;

/* used to build the hashtable */
typedef struct JsonHashEntry {
    char        fname[NAMEDATALEN];
    char       *val;
    char       *json;
    bool        isnull;
} JsonHashEntry;

/* these two are stolen from hstore / record_out, used in populate_record* */
typedef struct ColumnIOData {
    Oid         column_type;
    Oid         typiofunc;
    Oid         typioparam;
    FmgrInfo    proc;
} ColumnIOData;

typedef struct RecordIOData {
    Oid          record_type;
    int32        record_typmod;
    int          ncolumns;
    ColumnIOData columns[1];    /* VARIABLE LENGTH ARRAY */
} RecordIOData;

/* state for populate_recordset */
typedef struct PopulateRecordsetState {
    JsonLexContext  *lex;
    HTAB            *json_hash;
    char            *saved_scalar;
    char            *save_json_start;
    bool             use_json_as_text;
    Tuplestorestate *tuple_store;
    TupleDesc        ret_tdesc;
    HeapTupleHeader  rec;
    RecordIOData    *my_extra;
    MemoryContext    fn_mcxt;      /* used to stash IO funcs */
} PopulateRecordsetState;

/* Turn a jsonb object into a record */
static void make_row_from_rec_and_jsonb(Jsonb *element, PopulateRecordsetState *state);

/*
 * SQL function json_object_keys
 *
 * Returns the set of keys for the object argument.
 *
 * This SRF operates in value-per-call mode. It processes the
 * object during the first call, and the keys are simply stashed
 * in an array, whose size is expanded as necessary. This is probably
 * safe enough for a list of keys of a single object, since they are
 * limited in size to NAMEDATALEN and the number of keys is unlikely to
 * be so huge that it has major memory implications.
 */
Datum jsonb_object_keys(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    OkeysState      *state = NULL;
    int i;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        Jsonb        *jb = PG_GETARG_JSONB(0);
        bool          skipNested = false;
        JsonbIterator *it = NULL;
        JsonbValue  v;
        int         r;

        if (JB_ROOT_IS_SCALAR(jb)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call jsonb_object_keys on a scalar")));
        } else if (JB_ROOT_IS_ARRAY(jb)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call jsonb_object_keys on an array")));
        }

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = (OkeysState *)palloc(sizeof(OkeysState));

        state->result_size = JB_ROOT_COUNT(jb);
        state->result_count = 0;
        state->sent_count = 0;
        state->result = (char **)palloc(state->result_size * sizeof(char *));

        it = JsonbIteratorInit(VARDATA_ANY(jb));

        while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
            skipNested = true;

            if (r == WJB_KEY) {
                char *cstr = NULL;
                cstr = (char *)palloc(v.string.len + 1 * sizeof(char));
                errno_t rc = memcpy_s(cstr, v.string.len + 1 * sizeof(char), v.string.val, v.string.len);
                securec_check(rc, "\0", "\0");
                cstr[v.string.len] = '\0';
                state->result[state->result_count++] = cstr;
            }
        }

        MemoryContextSwitchTo(oldcontext);
        funcctx->user_fctx = (void *) state;
    }

    funcctx = SRF_PERCALL_SETUP();
    state = (OkeysState *) funcctx->user_fctx;
    if (state->sent_count < state->result_count) {
        char       *nxt = state->result[state->sent_count++];
        SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
    }

    /* cleanup to reduce or eliminate memory leaks */
    for (i = 0; i < state->result_count; i++) {
        pfree(state->result[i]);
    }
    pfree(state->result);
    pfree(state);

    SRF_RETURN_DONE(funcctx);
}

Datum json_object_keys(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    OkeysState *state = NULL;
    int         i;

    if (SRF_IS_FIRSTCALL()) {
        text           *json = PG_GETARG_TEXT_P(0);
        JsonLexContext *lex = makeJsonLexContext(json, true);
        JsonSemAction  *sem = NULL;

        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = (OkeysState *)palloc(sizeof(OkeysState));
        sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));

        state->lex = lex;
        state->result_size = 256;
        state->result_count = 0;
        state->sent_count = 0;
        state->result = (char **)palloc(256 * sizeof(char *));

        sem->semstate = (void *) state;
        sem->array_start = okeys_array_start;
        sem->scalar = okeys_scalar;
        sem->object_field_start = okeys_object_field_start;
        /* remainder are all NULL, courtesy of palloc0 above */
        pg_parse_json(lex, sem);
        /* keys are now in state->result */
        pfree(lex->strval->data);
        pfree(lex->strval);
        pfree(lex);
        pfree(sem);

        MemoryContextSwitchTo(oldcontext);
        funcctx->user_fctx = (void *) state;
    }

    funcctx = SRF_PERCALL_SETUP();
    state = (OkeysState *) funcctx->user_fctx;
    if (state->sent_count < state->result_count) {
        char       *nxt = state->result[state->sent_count++];
        SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
    }

    /* cleanup to reduce or eliminate memory leaks */
    for (i = 0; i < state->result_count; i++) {
        pfree(state->result[i]);
    }
    pfree(state->result);
    pfree(state);

    SRF_RETURN_DONE(funcctx);
}

static void okeys_object_field_start(void *state, char *fname, bool isnull)
{
    OkeysState *_state = (OkeysState *) state;

    /* only collecting keys for the top level object */
    if (_state->lex->lex_level != 1) {
        return;
    }

    /* enlarge result array if necessary */
    if (_state->result_count >= _state->result_size) {
        _state->result_size *= 2;
        _state->result = (char **)repalloc(_state->result, sizeof(char *) * _state->result_size);
    }

    /* save a copy of the field name */
    _state->result[_state->result_count++] = pstrdup(fname);
}

static void okeys_array_start(void *state)
{
    OkeysState *_state = (OkeysState *) state;

    /* top level must be a json object */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_object_keys on an array")));
    }
}

static void okeys_scalar(void *state, char *token, JsonTokenType tokentype)
{
    OkeysState *_state = (OkeysState *) state;

    /* top level must be a json object */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_object_keys on a scalar")));
    }
}

/*
 * json and jsonb getter functions
 * these implement the -> ->> #> and #>> operators
 * and the json{b?}_extract_path*(json, text, ...) functions
 */
Datum json_object_field(PG_FUNCTION_ARGS)
{
    text       *json = PG_GETARG_TEXT_P(0);
    text       *result = NULL;
    text       *fname = PG_GETARG_TEXT_P(1);
    char       *fnamestr = text_to_cstring(fname);

    result = get_worker(json, fnamestr, -1, NULL, NULL, -1, false);

    if (result != NULL) {
        PG_RETURN_TEXT_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

Datum jsonb_object_field(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    char       *key = text_to_cstring(PG_GETARG_TEXT_P(1));
    int         klen = strlen(key);
    JsonbIterator *it = NULL;
    JsonbValue  v;
    int         r;
    bool        skipNested = false;

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_object_field (jsonb -> text operator) on a scalar")));
    } else if (JB_ROOT_IS_ARRAY(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_object_field (jsonb -> text operator) on an array")));
    }

    Assert(JB_ROOT_IS_OBJECT(jb));
    it = JsonbIteratorInit(VARDATA_ANY(jb));
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if (r == WJB_KEY) {
            if (klen == v.string.len && strncmp(key, v.string.val, klen) == 0) {
                /*
                 * The next thing the iterator fetches should be the value, no
                 * matter what shape it is.
                 */
                (void) JsonbIteratorNext(&it, &v, skipNested);
                PG_RETURN_JSONB(JsonbValueToJsonb(&v));
            }
        }
    }
    PG_RETURN_NULL();
}

Datum json_object_field_text(PG_FUNCTION_ARGS)
{
    text       *json = PG_GETARG_TEXT_P(0);
    text       *result = NULL;
    text       *fname = PG_GETARG_TEXT_P(1);
    char       *fnamestr = text_to_cstring(fname);

    result = get_worker(json, fnamestr, -1, NULL, NULL, -1, true);

    if (result != NULL) {
        PG_RETURN_TEXT_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

Datum jsonb_object_field_text(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    char       *key = text_to_cstring(PG_GETARG_TEXT_P(1));
    int         klen = strlen(key);
    JsonbIterator *it = NULL;
    JsonbValue  v;
    int         r;
    bool        skipNested = false;

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_object_field_text (jsonb ->> text operator) on a scalar")));
    } else if (JB_ROOT_IS_ARRAY(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_object_field_text (jsonb ->> text operator) on an array")));
    }

    Assert(JB_ROOT_IS_OBJECT(jb));
    it = JsonbIteratorInit(VARDATA_ANY(jb));
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if (r == WJB_KEY) {
            if (klen == v.string.len && strncmp(key, v.string.val, klen) == 0) {
                text       *result = NULL;

                /*
                 * The next thing the iterator fetches should be the value, no
                 * matter what shape it is.
                 */
                r = JsonbIteratorNext(&it, &v, skipNested);

                /*
                 * if it's a scalar string it needs to be de-escaped,
                 * otherwise just return the text
                 */
                if (v.type == jbvString) {
                    result = cstring_to_text_with_len(v.string.val, v.string.len);
                } else if (v.type == jbvNull) {
                    PG_RETURN_NULL();
                } else {
                    StringInfo  jtext = makeStringInfo();
                    Jsonb      *tjb = JsonbValueToJsonb(&v);
                    (void) JsonbToCString(jtext, VARDATA(tjb), -1);
                    result = cstring_to_text_with_len(jtext->data, jtext->len);
                }
                PG_RETURN_TEXT_P(result);
            }
        }
    }

    PG_RETURN_NULL();
}

Datum json_array_element(PG_FUNCTION_ARGS)
{
    text  *json = PG_GETARG_TEXT_P(0);
    text  *result = NULL;
    int    element = PG_GETARG_INT32(1);

    result = get_worker(json, NULL, element, NULL, NULL, -1, false);

    if (result != NULL) {
        PG_RETURN_TEXT_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

Datum jsonb_array_element(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    int         element = PG_GETARG_INT32(1);
    JsonbIterator *it = NULL;
    JsonbValue  v;
    int         r;
    bool        skipNested = false;
    int         element_number = 0;

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_array_element (jsonb -> int operator) on a scalar")));
    } else if (JB_ROOT_IS_OBJECT(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_array_element (jsonb -> int operator) on an object")));
    }

    Assert(JB_ROOT_IS_ARRAY(jb));

    it = JsonbIteratorInit(VARDATA_ANY(jb));

    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if (r == WJB_ELEM) {
            if (element_number++ == element) {
                PG_RETURN_JSONB(JsonbValueToJsonb(&v));
            }
        }
    }

    PG_RETURN_NULL();
}

Datum json_array_element_text(PG_FUNCTION_ARGS)
{
    text       *json = PG_GETARG_TEXT_P(0);
    text       *result = NULL;
    int         element = PG_GETARG_INT32(1);

    result = get_worker(json, NULL, element, NULL, NULL, -1, true);

    if (result != NULL) {
        PG_RETURN_TEXT_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

Datum jsonb_array_element_text(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    int         element = PG_GETARG_INT32(1);
    JsonbIterator *it = NULL;
    JsonbValue  v;
    int         r;
    bool        skipNested = false;
    int         element_number = 0;

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_array_element_text on a scalar")));
    } else if (JB_ROOT_IS_OBJECT(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errmsg("cannot call jsonb_array_element_text on an object")));
    }

    Assert(JB_ROOT_IS_ARRAY(jb));
    it = JsonbIteratorInit(VARDATA_ANY(jb));
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;
        if (r == WJB_ELEM) {
            if (element_number++ == element) {
                /*
                 * if it's a scalar string it needs to be de-escaped,
                 * otherwise just return the text
                 */
                text       *result = NULL;
                if (v.type == jbvString) {
                    result = cstring_to_text_with_len(v.string.val, v.string.len);
                } else if (v.type == jbvNull) {
                    PG_RETURN_NULL();
                } else {
                    StringInfo  jtext = makeStringInfo();
                    Jsonb      *tjb = JsonbValueToJsonb(&v);
                    (void) JsonbToCString(jtext, VARDATA(tjb), -1);
                    result = cstring_to_text_with_len(jtext->data, jtext->len);
                }
                PG_RETURN_TEXT_P(result);
            }
        }
    }

    PG_RETURN_NULL();
}

Datum json_extract_path(PG_FUNCTION_ARGS)
{
    return get_path_all(fcinfo, false);
}

Datum json_extract_path_text(PG_FUNCTION_ARGS)
{
    return get_path_all(fcinfo, true);
}

/*
 * common routine for extract_path functions
 */
static inline Datum get_path_all(FunctionCallInfo fcinfo, bool as_text)
{
    text       *json = NULL;
    ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
    text       *result = NULL;
    Datum      *pathtext = NULL;
    bool       *pathnulls = NULL;
    int         npath;
    char      **tpath = NULL;
    int        *ipath = NULL;
    int         i;
    long        ind;
    char       *endptr = NULL;

    json = PG_GETARG_TEXT_P(0);

    if (array_contains_nulls(path)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call function with null path elements")));
    }

    deconstruct_array(path, TEXTOID, -1, false, 'i', &pathtext, &pathnulls, &npath);
    /*
     * If the array is empty, return NULL; this is dubious but it's what 9.3
     * did.
     */
    if (npath <= 0)
        PG_RETURN_NULL();

    tpath = (char **)palloc(npath * sizeof(char *));
    ipath = (int *)palloc(npath * sizeof(int));

    for (i = 0; i < npath; i++) {
        tpath[i] = TextDatumGetCString(pathtext[i]);
        if (*tpath[i] == '\0') {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call function with empty path elements")));
        }

        /*
         * we have no idea at this stage what structure the document is so
         * just convert anything in the path that we can to an integer and set
         * all the other integers to -1 which will never match.
         */
        ind = strtol(tpath[i], &endptr, 10);
        if (*endptr == '\0' && ind <= INT_MAX && ind >= 0) {
            ipath[i] = (int) ind;
        } else {
            ipath[i] = -1;
        }
    }

    result = get_worker(json, NULL, -1, tpath, ipath, npath, as_text);

    if (result != NULL) {
        PG_RETURN_TEXT_P(result);
    } else {
        /* null is NULL, regardless */
        PG_RETURN_NULL();
    }
}

/*
 * get_worker
 *
 * common worker for all the json getter functions
 */
static inline text *get_worker(text *json, char *field, int elem_index, char **tpath,
    int *ipath, int npath, bool normalize_results)
{
    GetState       *state = NULL;
    JsonLexContext *lex = makeJsonLexContext(json, true);
    JsonSemAction  *sem = NULL;

    /* only allowed to use one of these */
    Assert(elem_index < 0 || (tpath == NULL && ipath == NULL && field == NULL));
    Assert(tpath == NULL || field == NULL);

    state = (GetState *)palloc0(sizeof(GetState));
    sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));

    state->lex = lex;
    /* is it "_as_text" variant? */
    state->normalize_results = normalize_results;
    if (field != NULL) {
        /* single text argument */
        state->search_type = JSON_SEARCH_OBJECT;
        state->search_term = field;
    } else if (tpath != NULL) {
        /* path array argument */
        state->search_type = JSON_SEARCH_PATH;
        state->path = tpath;
        state->npath = npath;
        state->current_path = (char **)palloc(sizeof(char *) * npath);
        state->pathok = (bool *)palloc0(sizeof(bool) * npath);
        state->pathok[0] = true;
        state->array_level_index = (int *)palloc(sizeof(int) * npath);
        state->path_level_index = ipath;
    } else {
        /* single integer argument */
        state->search_type = JSON_SEARCH_ARRAY;
        state->search_index = elem_index;
        state->array_index = -1;
    }
    sem->semstate = (void *) state;

    /*
     * Not all  variants need all the semantic routines. only set the ones
     * that are actually needed for maximum efficiency.
     */
    sem->object_start = get_object_start;
    sem->array_start = get_array_start;
    sem->scalar = get_scalar;
    if (field != NULL || tpath != NULL) {
        sem->object_field_start = get_object_field_start;
        sem->object_field_end = get_object_field_end;
    }
    if (field == NULL) {
        sem->array_element_start = get_array_element_start;
        sem->array_element_end = get_array_element_end;
    }
    pg_parse_json(lex, sem);
    return state->tresult;
}

static void get_object_start(void *state)
{
    GetState    *_state = (GetState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0 && _state->search_type == JSON_SEARCH_ARRAY) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot extract array element from a non-array")));
    }
}

static void get_object_field_start(void *state, char *fname, bool isnull)
{
    GetState   *_state = (GetState *) state;
    bool        get_next = false;
    int         lex_level = _state->lex->lex_level;

    if (lex_level == 1 && _state->search_type == JSON_SEARCH_OBJECT &&
        strcmp(fname, _state->search_term) == 0) {
        _state->tresult = NULL;
        _state->result_start = NULL;
        get_next = true;
    } else if (_state->search_type == JSON_SEARCH_PATH &&
               lex_level <= _state->npath &&
               _state->pathok[_state->lex->lex_level - 1] &&
               strcmp(fname, _state->path[lex_level - 1]) == 0) {
        /* path search, path so far is ok,  and we have a match */
        /* this object overrides any previous matching object */
        _state->tresult = NULL;
        _state->result_start = NULL;

        /* if not at end of path just mark path ok */
        if (lex_level < _state->npath) {
            _state->pathok[lex_level] = true;
        }

        /* end of path, so we want this value */
        if (lex_level == _state->npath) {
            get_next = true;
        }
    }

    if (get_next) {
        if (_state->normalize_results && _state->lex->token_type == JSON_TOKEN_STRING) {
            /* for as_text variants, tell get_scalar to set it for us */
            _state->next_scalar = true;
        } else {
            /* for non-as_text variants, just note the json starting point */
            _state->result_start = _state->lex->token_start;
        }
    }
}

static void get_object_field_end(void *state, char *fname, bool isnull)
{
    GetState   *_state = (GetState *) state;
    bool        get_last = false;
    int         lex_level = _state->lex->lex_level;

    /* same tests as in get_object_field_start, mutatis mutandis */
    if (lex_level == 1 && _state->search_type == JSON_SEARCH_OBJECT && strcmp(fname, _state->search_term) == 0) {
        get_last = true;
    } else if (_state->search_type == JSON_SEARCH_PATH &&
               lex_level <= _state->npath &&
               _state->pathok[lex_level - 1] &&
               strcmp(fname, _state->path[lex_level - 1]) == 0) {
        /* done with this field so reset pathok */
        if (lex_level < _state->npath) {
            _state->pathok[lex_level] = false;
        }

        if (lex_level == _state->npath) {
            get_last = true;
        }
    }

    /* for as_test variants our work is already done */
    if (get_last && _state->result_start != NULL) {
        /*
         * make a text object from the string from the prevously noted json
         * start up to the end of the previous token (the lexer is by now
         * ahead of us on whatever came after what we're interested in).
         */
        int len = _state->lex->prev_token_terminator - _state->result_start;

        if (isnull && _state->normalize_results) {
            _state->tresult = (text *) NULL;
        } else {
            _state->tresult = cstring_to_text_with_len(_state->result_start, len);
        }
    }

    /*
     * don't need to reset _state->result_start b/c we're only returning one
     * datum, the conditions should not occur more than once, and this lets us
     * check cheaply that they don't (see object_field_start() )
     */
}

static void get_array_start(void *state)
{
    GetState   *_state = (GetState *) state;
    int         lex_level = _state->lex->lex_level;

    /* json structure check */
    if (lex_level == 0 && _state->search_type == JSON_SEARCH_OBJECT) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot extract field from a non-object")));
    }

    /*
     * initialize array count for this nesting level Note: the lex_level seen
     * by array_start is one less than that seen by the elements of the array.
     */
    if (_state->search_type == JSON_SEARCH_PATH && lex_level < _state->npath) {
        _state->array_level_index[lex_level] = -1;
    }
}

static void get_array_element_start(void *state, bool isnull)
{
    GetState   *_state = (GetState *) state;
    bool        get_next = false;
    int         lex_level = _state->lex->lex_level;

    if (lex_level == 1 && _state->search_type == JSON_SEARCH_ARRAY) {
        /* single integer search */
        _state->array_index++;
        if (_state->array_index == _state->search_index) {
            get_next = true;
        }
    } else if (_state->search_type == JSON_SEARCH_PATH &&
               lex_level <= _state->npath &&
               _state->pathok[lex_level - 1]) {
        /*
         * path search, path so far is ok
         *
         * increment the array counter. no point doing this if we already know
         * the path is bad.
         *
         * then check if we have a match.
         */
        if (++_state->array_level_index[lex_level - 1] == _state->path_level_index[lex_level - 1]) {
            if (lex_level == _state->npath) {
                /* match and at end of path, so get value */
                get_next = true;
            } else {
                /* not at end of path just mark path ok */
                _state->pathok[lex_level] = true;
            }
        }

    }

    /* same logic as for objects */
    if (get_next) {
        if (_state->normalize_results &&
            _state->lex->token_type == JSON_TOKEN_STRING) {
            _state->next_scalar = true;
        } else {
            _state->result_start = _state->lex->token_start;
        }
    }
}

static void get_array_element_end(void *state, bool isnull)
{
    GetState   *_state = (GetState *) state;
    bool        get_last = false;
    int         lex_level = _state->lex->lex_level;

    /* same logic as in get_object_end, modified for arrays */
    if (lex_level == 1 && _state->search_type == JSON_SEARCH_ARRAY &&
        _state->array_index == _state->search_index) {
        get_last = true;
    } else if (_state->search_type == JSON_SEARCH_PATH &&
               lex_level <= _state->npath &&
               _state->pathok[lex_level - 1] &&
               _state->array_level_index[lex_level - 1] ==
               _state->path_level_index[lex_level - 1]) {
        /* done with this element so reset pathok */
        if (lex_level < _state->npath) {
            _state->pathok[lex_level] = false;
        }

        if (lex_level == _state->npath) {
            get_last = true;
        }
    }
    if (get_last && _state->result_start != NULL) {
        int len = _state->lex->prev_token_terminator - _state->result_start;

        if (isnull && _state->normalize_results) {
            _state->tresult = (text *) NULL;
        } else {
            _state->tresult = cstring_to_text_with_len(_state->result_start, len);
        }
    }
}

static void get_scalar(void *state, char *token, JsonTokenType tokentype)
{
    GetState   *_state = (GetState *) state;

    if (_state->lex->lex_level == 0 && _state->search_type != JSON_SEARCH_PATH) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot extract element from a scalar")));
    }
    if (_state->next_scalar) {
        /* a de-escaped text value is wanted, so supply it */
        _state->tresult = cstring_to_text(token);
        /* make sure the next call to get_scalar doesn't overwrite it */
        _state->next_scalar = false;
    }

}

Datum jsonb_extract_path(PG_FUNCTION_ARGS)
{
    return get_jsonb_path_all(fcinfo, false);
}

Datum jsonb_extract_path_text(PG_FUNCTION_ARGS)
{
    return get_jsonb_path_all(fcinfo, true);
}

static inline Datum get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
    Datum      *pathtext = NULL;
    bool       *pathnulls = NULL;
    int         npath;
    int         i;
    Jsonb      *res = NULL;
    bool        have_object = false,
                have_array = false;
    JsonbValue *jbvp = NULL;
    JsonbValue  tv;
    JsonbSuperHeader superHeader;

    if (array_contains_nulls(path)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call function with null path elements")));
    }

    deconstruct_array(path, TEXTOID, -1, false, 'i', &pathtext, &pathnulls, &npath);
    /*
     * If the array is empty, return NULL; this is dubious but it's what 9.3
     * did.
     */
    if (npath <= 0)
        PG_RETURN_NULL();

    if (JB_ROOT_IS_OBJECT(jb)) {
        have_object = true;
    } else if (JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb)) {
        have_array = true;
    }

    superHeader = (JsonbSuperHeader) VARDATA(jb);

    for (i = 0; i < npath; i++) {
        if (have_object) {
            jbvp = findJsonbValueFromSuperHeaderLen(superHeader, JB_FOBJECT, VARDATA_ANY(pathtext[i]),
                                                    VARSIZE_ANY_EXHDR(pathtext[i]));
        } else if (have_array) {
            long        lindex;
            uint32      index;
            char       *indextext = TextDatumGetCString(pathtext[i]);
            char       *endptr = NULL;

            lindex = strtol(indextext, &endptr, 10);
            if (*endptr != '\0' || lindex > INT_MAX || lindex < 0) {
                PG_RETURN_NULL();
            }
            index = (uint32) lindex;
            jbvp = getIthJsonbValueFromSuperHeader(superHeader, index);
        } else {
            if (i == 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("cannot call extract path from a scalar")));
            }
            PG_RETURN_NULL();
        }

        if (jbvp == NULL) {
            PG_RETURN_NULL();
        } else if (i == npath - 1) {
            break;
        }

        if (jbvp->type == jbvBinary) {
            JsonbIterator  *it = JsonbIteratorInit(jbvp->binary.data);
            int             r;

            r = JsonbIteratorNext(&it, &tv, true);
            superHeader = (JsonbSuperHeader) jbvp->binary.data;
            have_object = r == WJB_BEGIN_OBJECT;
            have_array = r == WJB_BEGIN_ARRAY;
        } else {
            have_object = jbvp->type == jbvObject;
            have_array = jbvp->type == jbvArray;
        }
    }

    if (as_text) {
        if (jbvp->type == jbvString) {
            PG_RETURN_TEXT_P(cstring_to_text_with_len(jbvp->string.val, jbvp->string.len));
        } else if (jbvp->type == jbvNull) {
            PG_RETURN_NULL();
        }
    }

    res = JsonbValueToJsonb(jbvp);

    if (as_text) {
        PG_RETURN_TEXT_P(cstring_to_text(JsonbToCString(NULL, VARDATA(res), VARSIZE(res))));
    } else {
        /* not text mode - just hand back the jsonb */
        PG_RETURN_JSONB(res);
    }
}

/*
 * SQL function json_array_length(json) -> int
 */
Datum json_array_length(PG_FUNCTION_ARGS)
{
    text       *json = NULL;

    AlenState  *state = NULL;
    JsonLexContext *lex = NULL;
    JsonSemAction *sem = NULL;

    json = PG_GETARG_TEXT_P(0);
    lex = makeJsonLexContext(json, false);
    state = (AlenState *)palloc0(sizeof(AlenState));
    sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));

    state->lex = lex;

    sem->semstate = (void *) state;
    sem->object_start = alen_object_start;
    sem->scalar = alen_scalar;
    sem->array_element_start = alen_array_element_start;

    pg_parse_json(lex, sem);

    PG_RETURN_INT32(state->count);
}

Datum jsonb_array_length(PG_FUNCTION_ARGS)
{
    Jsonb *jb = PG_GETARG_JSONB(0);

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot get array length of a scalar")));
    } else if (!JB_ROOT_IS_ARRAY(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot get array length of a non-array")));
    }

    PG_RETURN_INT32(JB_ROOT_COUNT(jb));
}

/*
 * These next two check ensure that the json is an array (since it can't be
 * a scalar or an object).
 */

static void alen_object_start(void *state)
{
    AlenState *_state = (AlenState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot get array length of a non-array")));
    }
}

static void alen_scalar(void *state, char *token, JsonTokenType tokentype)
{
    AlenState *_state = (AlenState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot get array length of a scalar")));
    }
}

static void alen_array_element_start(void *state, bool isnull)
{
    AlenState  *_state = (AlenState *) state;

    /* just count up all the level 1 elements */
    if (_state->lex->lex_level == 1) {
        _state->count++;
    }
}

/*
 * SQL function json_each and json_each_text
 *
 * decompose a json object into key value pairs.
 *
 * Unlike json_object_keys() these SRFs operate in materialize mode,
 * stashing results into a Tuplestore object as they go.
 * The construction of tuples is done using a temporary memory context
 * that is cleared out after each tuple is built.
 */
Datum json_each(PG_FUNCTION_ARGS)
{
    return each_worker(fcinfo, false);
}

Datum jsonb_each(PG_FUNCTION_ARGS)
{
    return each_worker_jsonb(fcinfo, false);
}

Datum json_each_text(PG_FUNCTION_ARGS)
{
    return each_worker(fcinfo, true);
}

Datum jsonb_each_text(PG_FUNCTION_ARGS)
{
    return each_worker_jsonb(fcinfo, true);
}

static inline Datum each_worker_jsonb(FunctionCallInfo fcinfo, bool as_text)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    ReturnSetInfo *rsi = NULL;
    Tuplestorestate *tuple_store = NULL;
    TupleDesc   tupdesc;
    TupleDesc   ret_tdesc;
    MemoryContext old_cxt,
                tmp_cxt;
    bool        skipNested = false;
    JsonbIterator *it = NULL;
    JsonbValue  v;
    int         r;

    if (!JB_ROOT_IS_OBJECT(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call jsonb_each%s on a non-object",
                        as_text ? "_text" : "")));
    }

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    if (!rsi || !IsA(rsi, ReturnSetInfo) ||
        ((uint32)rsi->allowedModes & SFRM_Materialize) == 0 ||
        rsi->expectedDesc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
        }

    rsi->returnMode = SFRM_Materialize;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));
    }

    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

    ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(ret_tdesc);
    tuple_store = tuplestore_begin_heap((uint32)rsi->allowedModes & SFRM_Materialize_Random,
                                        false, u_sess->attr.attr_memory.work_mem);

    MemoryContextSwitchTo(old_cxt);

    tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                    "jsonb_each temporary cxt",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);

    it = JsonbIteratorInit(VARDATA_ANY(jb));
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;
        if (r == WJB_KEY) {
            text       *key = NULL;
            HeapTuple   tuple;
            Datum       values[2];
            bool        nulls[2] = {false, false};

            /* Use the tmp context so we can clean up after each tuple is done */
            old_cxt = MemoryContextSwitchTo(tmp_cxt);
            key = cstring_to_text_with_len(v.string.val, v.string.len);

            /*
             * The next thing the iterator fetches should be the value, no
             * matter what shape it is.
             */
            r = JsonbIteratorNext(&it, &v, skipNested);
            values[0] = PointerGetDatum(key);
            if (as_text) {
                if (v.type == jbvNull) {
                    /* a json null is an sql null in text mode */
                    nulls[1] = true;
                    values[1] = (Datum) NULL;
                } else {
                    text *sv = NULL;

                    if (v.type == jbvString) {
                        /* In text mode, scalar strings should be dequoted */
                        sv = cstring_to_text_with_len(v.string.val, v.string.len);
                    } else {
                        /* Turn anything else into a json string */
                        StringInfo  jtext = makeStringInfo();
                        Jsonb      *jb = JsonbValueToJsonb(&v);
                        (void) JsonbToCString(jtext, VARDATA(jb), 2 * v.estSize);
                        sv = cstring_to_text_with_len(jtext->data, jtext->len);
                    }
                    values[1] = PointerGetDatum(sv);
                }
            } else {
                /* Not in text mode, just return the Jsonb */
                Jsonb      *val = JsonbValueToJsonb(&v);
                values[1] = PointerGetDatum(val);
            }
            tuple = heap_form_tuple(ret_tdesc, values, nulls);
            tuplestore_puttuple(tuple_store, tuple);

            /* clean up and switch back */
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }
    MemoryContextDelete(tmp_cxt);
    rsi->setResult = tuple_store;
    rsi->setDesc = ret_tdesc;
    PG_RETURN_NULL();
}


static inline Datum each_worker(FunctionCallInfo fcinfo, bool as_text)
{
    text           *json = NULL;
    JsonLexContext *lex = NULL;
    JsonSemAction  *sem = NULL;
    ReturnSetInfo  *rsi = NULL;
    MemoryContext   old_cxt;
    TupleDesc       tupdesc;
    EachState      *state = NULL;

    json = PG_GETARG_TEXT_P(0);
    lex = makeJsonLexContext(json, true);
    state = (EachState *)palloc0(sizeof(EachState));
    sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));
    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    if (!rsi || !IsA(rsi, ReturnSetInfo) ||
        ((uint32)rsi->allowedModes & SFRM_Materialize) == 0 ||
        rsi->expectedDesc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
    }

    rsi->returnMode = SFRM_Materialize;
    (void) get_call_result_type(fcinfo, NULL, &tupdesc);
    if (tupdesc == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("tupdesc should not be NULL value"),
                errdetail("N/A"),
                errcause("An error occurred when obtaining the value of tupdesc."),
                erraction("Contact Huawei Engineer.")));
    }

    /* make these in a sufficiently long-lived memory context */
    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
    state->ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(state->ret_tdesc);
    state->tuple_store = tuplestore_begin_heap((uint32)rsi->allowedModes & SFRM_Materialize_Random,
                                               false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(old_cxt);

    sem->semstate = (void *) state;
    sem->array_start = each_array_start;
    sem->scalar = each_scalar;
    sem->object_field_start = each_object_field_start;
    sem->object_field_end = each_object_field_end;
    state->normalize_results = as_text;
    state->next_scalar = false;
    state->lex = lex;
    state->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                           "json_each temporary cxt",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE);
    pg_parse_json(lex, sem);
    MemoryContextDelete(state->tmp_cxt);
    rsi->setResult = state->tuple_store;
    rsi->setDesc = state->ret_tdesc;
    PG_RETURN_NULL();
}

static void each_object_field_start(void *state, char *fname, bool isnull)
{
    EachState  *_state = (EachState *) state;

    /* save a pointer to where the value starts */
    if (_state->lex->lex_level == 1) {
        /*
         * next_scalar will be reset in the object_field_end handler, and
         * since we know the value is a scalar there is no danger of it being
         * on while recursing down the tree.
         */
        if (_state->normalize_results && _state->lex->token_type == JSON_TOKEN_STRING) {
            _state->next_scalar = true;
        } else {
            _state->result_start = _state->lex->token_start;
        }
    }
}

static void each_object_field_end(void *state, char *fname, bool isnull)
{
    EachState  *_state = (EachState *) state;
    MemoryContext old_cxt;
    int         len;
    text       *val = NULL;
    HeapTuple   tuple;
    Datum       values[2];
    bool        nulls[2] = {false, false};

    /* skip over nested objects */
    if (_state->lex->lex_level != 1) {
        return;
    }

    /* use the tmp context so we can clean up after each tuple is done */
    old_cxt = MemoryContextSwitchTo(_state->tmp_cxt);

    values[0] = CStringGetTextDatum(fname);

    if (isnull && _state->normalize_results) {
        nulls[1] = true;
        values[1] = (Datum) NULL;
    } else if (_state->next_scalar) {
        values[1] = CStringGetTextDatum(_state->normalized_scalar);
        _state->next_scalar = false;
    } else {
        len = _state->lex->prev_token_terminator - _state->result_start;
        val = cstring_to_text_with_len(_state->result_start, len);
        values[1] = PointerGetDatum(val);
    }

    tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);
    tuplestore_puttuple(_state->tuple_store, tuple);

    /* clean up and switch back */
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset(_state->tmp_cxt);
}

static void each_array_start(void *state)
{
    EachState  *_state = (EachState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot deconstruct an array as an object")));
    }
}

static void each_scalar(void *state, char *token, JsonTokenType tokentype)
{
    EachState  *_state = (EachState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot deconstruct a scalar")));
    }

    /* supply de-escaped value if required */
    if (_state->next_scalar) {
        _state->normalized_scalar = token;
    }
}

/*
 * SQL functions json_array_elements and json_array_elements_text
 *
 * get the elements from a json array
 *
 * a lot of this processing is similar to the json_each* functions
 */
Datum jsonb_array_elements(PG_FUNCTION_ARGS)
{
    return elements_worker_jsonb(fcinfo, false);
}

Datum jsonb_array_elements_text(PG_FUNCTION_ARGS)
{
    return elements_worker_jsonb(fcinfo, true);
}

static inline Datum elements_worker_jsonb(FunctionCallInfo fcinfo, bool as_text)
{
    Jsonb           *jb = PG_GETARG_JSONB(0);
    ReturnSetInfo   *rsi = NULL;
    Tuplestorestate *tuple_store = NULL;
    TupleDesc        tupdesc;
    TupleDesc        ret_tdesc;
    MemoryContext    old_cxt,
                     tmp_cxt;
    bool             skipNested = false;
    JsonbIterator   *it = NULL;
    JsonbValue       v;
    int              r;

    if (JB_ROOT_IS_SCALAR(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot extract elements from a scalar")));
    } else if (!JB_ROOT_IS_ARRAY(jb)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot extract elements from an object")));
    }

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    if (!rsi || !IsA(rsi, ReturnSetInfo) ||
        (rsi->allowedModes & SFRM_Materialize) == 0 ||
        rsi->expectedDesc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
        }

    rsi->returnMode = SFRM_Materialize;
    /* it's a simple type, so don't use get_call_result_type() */
    tupdesc = rsi->expectedDesc;
    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
    ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(ret_tdesc);
    tuple_store =
        tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(old_cxt);

    tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                    "jsonb_each temporary cxt",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);
    it = JsonbIteratorInit(VARDATA_ANY(jb));
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;
        if (r == WJB_ELEM) {
            HeapTuple   tuple;
            Datum       values[1];
            bool        nulls[1] = {false};
            /* use the tmp context so we can clean up after each tuple is done */
            old_cxt = MemoryContextSwitchTo(tmp_cxt);
            if (!as_text) {
                Jsonb      *val = JsonbValueToJsonb(&v);
                values[0] = PointerGetDatum(val);
            } else {
                if (v.type == jbvNull) {
                    /* a json null is an sql null in text mode */
                    nulls[0] = true;
                    values[0] = (Datum) NULL;
                } else {
                    text       *sv = NULL;
                    if (v.type == jbvString) {
                        /* in text mode scalar strings should be dequoted */
                        sv = cstring_to_text_with_len(v.string.val, v.string.len);
                    } else {
                        /* turn anything else into a json string */
                        StringInfo  jtext = makeStringInfo();
                        Jsonb      *jb = JsonbValueToJsonb(&v);
                        (void) JsonbToCString(jtext, VARDATA(jb), 2 * v.estSize);
                        sv = cstring_to_text_with_len(jtext->data, jtext->len);
                    }
                    values[0] = PointerGetDatum(sv);
                }
            }

            tuple = heap_form_tuple(ret_tdesc, values, nulls);
            tuplestore_puttuple(tuple_store, tuple);

            /* clean up and switch back */
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }
    MemoryContextDelete(tmp_cxt);

    rsi->setResult = tuple_store;
    rsi->setDesc = ret_tdesc;

    PG_RETURN_NULL();
}

Datum json_array_elements(PG_FUNCTION_ARGS)
{
    return elements_worker(fcinfo, false);
}

Datum json_array_elements_text(PG_FUNCTION_ARGS)
{
    return elements_worker(fcinfo, true);
}

static inline Datum elements_worker(FunctionCallInfo fcinfo, bool as_text)
{
    text       *json = PG_GETARG_TEXT_P(0);

    /* elements only needs escaped strings when as_text */
    JsonLexContext *lex = makeJsonLexContext(json, as_text);
    JsonSemAction *sem = NULL;
    ReturnSetInfo *rsi = NULL;
    MemoryContext old_cxt;
    TupleDesc   tupdesc;
    ElementsState *state = NULL;

    state = (ElementsState *)palloc0(sizeof(ElementsState));
    sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));
    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    if (!rsi || !IsA(rsi, ReturnSetInfo) ||
        ((uint32)rsi->allowedModes & SFRM_Materialize) == 0 ||
        rsi->expectedDesc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
    }
    rsi->returnMode = SFRM_Materialize;

    /* it's a simple type, so don't use get_call_result_type() */
    tupdesc = rsi->expectedDesc;

    /* make these in a sufficiently long-lived memory context */
    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
    state->ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(state->ret_tdesc);
    state->tuple_store =
        tuplestore_begin_heap((uint32)rsi->allowedModes & SFRM_Materialize_Random,
                              false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(old_cxt);

    sem->semstate = (void *) state;
    sem->object_start = elements_object_start;
    sem->scalar = elements_scalar;
    sem->array_element_start = elements_array_element_start;
    sem->array_element_end = elements_array_element_end;
    state->normalize_results = as_text;
    state->next_scalar = false;
    state->lex = lex;
    state->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                           "json_array_elements temporary cxt",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE);
    pg_parse_json(lex, sem);
    MemoryContextDelete(state->tmp_cxt);

    rsi->setResult = state->tuple_store;
    rsi->setDesc = state->ret_tdesc;
    PG_RETURN_NULL();
}

static void elements_array_element_start(void *state, bool isnull)
{
    ElementsState *_state = (ElementsState *) state;

    /* save a pointer to where the value starts */
    if (_state->lex->lex_level == 1) {
        /*
         * next_scalar will be reset in the array_element_end handler, and
         * since we know the value is a scalar there is no danger of it being
         * on while recursing down the tree.
         */
        if (_state->normalize_results && _state->lex->token_type == JSON_TOKEN_STRING) {
            _state->next_scalar = true;
        } else {
            _state->result_start = _state->lex->token_start;
        }
    }
}

static void elements_array_element_end(void *state, bool isnull)
{
    ElementsState *_state = (ElementsState *) state;
    MemoryContext old_cxt;
    int         len;
    text       *val = NULL;
    HeapTuple   tuple;
    Datum       values[1];
    bool nulls[1] = {false};

    /* skip over nested objects */
    if (_state->lex->lex_level != 1) {
        return;
    }

    /* use the tmp context so we can clean up after each tuple is done */
    old_cxt = MemoryContextSwitchTo(_state->tmp_cxt);

    if (isnull && _state->normalize_results) {
        nulls[0] = true;
        values[0] = (Datum) NULL;
    } else if (_state->next_scalar) {
        values[0] = CStringGetTextDatum(_state->normalized_scalar);
        _state->next_scalar = false;
    } else {
        len = _state->lex->prev_token_terminator - _state->result_start;
        val = cstring_to_text_with_len(_state->result_start, len);
        values[0] = PointerGetDatum(val);
    }

    tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);
    tuplestore_puttuple(_state->tuple_store, tuple);

    /* clean up and switch back */
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset(_state->tmp_cxt);
}

static void elements_object_start(void *state)
{
    ElementsState *_state = (ElementsState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_array_elements on a non-array")));
    }
}

static void elements_scalar(void *state, char *token, JsonTokenType tokentype)
{
    ElementsState *_state = (ElementsState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_array_elements on a scalar")));
    }

    /* supply de-escaped value if required */
    if (_state->next_scalar) {
        _state->normalized_scalar = token;
    }
}

/*
 * SQL function json_populate_record
 *
 * set fields in a record from the argument json
 *
 * Code adapted shamelessly from hstore's populate_record
 * which is in turn partly adapted from record_out.
 *
 * The json is decomposed into a hash table, in which each
 * field in the record is then looked up by name. For jsonb
 * we fetch the values direct from the object.
 */
Datum jsonb_populate_record(PG_FUNCTION_ARGS)
{
    return populate_record_worker(fcinfo, true);
}

Datum json_populate_record(PG_FUNCTION_ARGS)
{
    return populate_record_worker(fcinfo, true);
}

Datum json_to_record(PG_FUNCTION_ARGS)
{
    return populate_record_worker(fcinfo, false);
}

static inline Datum populate_record_worker(FunctionCallInfo fcinfo, bool have_record_arg)
{
    Oid         argtype;
    Oid         jtype = get_fn_expr_argtype(fcinfo->flinfo, have_record_arg ? 1 : 0);
    text       *json = NULL;
    Jsonb      *jb = NULL;
    bool        use_json_as_text = false;
    HTAB       *json_hash = NULL;
    HeapTupleHeader rec = NULL;
    Oid         tupType = InvalidOid;
    int32       tupTypmod = -1;
    TupleDesc   tupdesc;
    HeapTupleData tuple;
    HeapTuple   rettuple;
    RecordIOData *my_extra = NULL;
    int         ncolumns;
    int         i;
    Datum      *values = NULL;
    bool       *nulls = NULL;
    errno_t rc = 0;

    Assert(jtype == JSONOID || jtype == JSONBOID);

    use_json_as_text = PG_ARGISNULL(have_record_arg ? 2 : 1) ? false : PG_GETARG_BOOL(have_record_arg ? 2 : 1);

    if (have_record_arg) {
        argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);

        if (!type_is_rowtype(argtype)) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("first argument of json%s_populate_record must be a row type",
                            jtype == JSONBOID ? "b" : "")));
        }

        if (PG_ARGISNULL(0)) {
            if (PG_ARGISNULL(1)) {
                PG_RETURN_NULL();
            }
            /*
             * have no tuple to look at, so the only source of type info is
             * the argtype. The lookup_rowtype_tupdesc call below will error
             * out if we don't have a known composite type oid here.
             */
            tupType = argtype;
            tupTypmod = -1;
        } else {
            rec = PG_GETARG_HEAPTUPLEHEADER(0);
            if (PG_ARGISNULL(1)) {
                PG_RETURN_POINTER(rec);
            }
            /* Extract type info from the tuple itself */
            tupType = HeapTupleHeaderGetTypeId(rec);
            tupTypmod = HeapTupleHeaderGetTypMod(rec);
        }
        tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    } else {                            /* json{b}_to_record case */
        use_json_as_text = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
        if (PG_ARGISNULL(0)) {
            PG_RETURN_NULL();
        }
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record"),
                     errhint("Try calling the function in the FROM clause "
                             "using a column definition list.")));
        }
    }

    if (jtype == JSONOID) {
        /* just get the text */
        json = PG_GETARG_TEXT_P(have_record_arg ? 1 : 0);
        json_hash = get_json_object_as_hash(json, "json_populate_record", use_json_as_text);
        /*
         * if the input json is empty, we can only skip the rest if we were
         * passed in a non-null record, since otherwise there may be issues
         * with domain nulls.
         */
        if (hash_get_num_entries(json_hash) == 0 && rec) {
            PG_RETURN_POINTER(rec);
        }
    } else {
        jb = PG_GETARG_JSONB(have_record_arg ? 1 : 0);
        /* same logic as for json */
        if (!have_record_arg && rec) {
            PG_RETURN_POINTER(rec);
        }
    }
    ncolumns = tupdesc->natts;
    if (rec) {
        /* Build a temporary HeapTuple control structure */
        tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid = InvalidOid;
        tuple.t_data = rec;
    }

    /*
     * We arrange to look up the needed I/O info just once per series of
     * calls, assuming the record type doesn't change underneath us.
     */
    my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->ncolumns != ncolumns) {
        fcinfo->flinfo->fn_extra = MemoryContextAllocZero(
            fcinfo->flinfo->fn_mcxt, sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData));
        my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
    }

    if (have_record_arg && (my_extra->record_type != tupType ||
                            my_extra->record_typmod != tupTypmod)) {
        rc = memset_s(my_extra, sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData),
                      0, sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData));
        securec_check(rc, "\0", "\0");
        my_extra->record_type = tupType;
        my_extra->record_typmod = tupTypmod;
        my_extra->ncolumns = ncolumns;
    }

    values = (Datum *) palloc(ncolumns * sizeof(Datum));
    nulls = (bool *) palloc(ncolumns * sizeof(bool));

    if (rec) {
        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);
    } else {
        for (i = 0; i < ncolumns; ++i) {
            values[i] = (Datum) 0;
            nulls[i] = true;
        }
    }

    for (i = 0; i < ncolumns; ++i) {
        ColumnIOData *column_info = &my_extra->columns[i];
        Oid         column_type = tupdesc->attrs[i].atttypid;
        JsonbValue *v = NULL;
        char        fname[NAMEDATALEN];
        JsonHashEntry *hashentry = NULL;

        /* Ignore dropped columns in datatype */
        if (tupdesc->attrs[i].attisdropped) {
            nulls[i] = true;
            continue;
        }

        if (jtype == JSONOID) {
            rc = memset_s(fname, NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            rc = strncpy_s(fname, NAMEDATALEN, NameStr(tupdesc->attrs[i].attname), NAMEDATALEN - 1);
            securec_check(rc, "\0", "\0");
            hashentry = (JsonHashEntry *)hash_search(json_hash, fname, HASH_FIND, NULL);
        } else {
            char       *key = NameStr(tupdesc->attrs[i].attname);
            v = findJsonbValueFromSuperHeaderLen(VARDATA(jb), JB_FOBJECT, key, strlen(key));
        }

        /*
         * we can't just skip here if the key wasn't found since we might have
         * a domain to deal with. If we were passed in a non-null record
         * datum, we assume that the existing values are valid (if they're
         * not, then it's not our fault), but if we were passed in a null,
         * then every field which we don't populate needs to be run through
         * the input function just in case it's a domain type.
         */
        if (((jtype == JSONOID && hashentry == NULL) ||
             (jtype == JSONBOID && v == NULL)) && rec) {
            continue;
        }

        /*
         * Prepare to convert the column value from text
         */
        if (column_info->column_type != column_type) {
            getTypeInputInfo(column_type, &column_info->typiofunc, &column_info->typioparam);
            fmgr_info_cxt(column_info->typiofunc, &column_info->proc, fcinfo->flinfo->fn_mcxt);
            column_info->column_type = column_type;
        }
        if ((jtype == JSONOID && (hashentry == NULL || hashentry->isnull)) ||
            (jtype == JSONBOID && (v == NULL || v->type == jbvNull))) {
            /*
             * need InputFunctionCall to happen even for nulls, so that domain
             * checks are done
             */
            values[i] = InputFunctionCall(&column_info->proc, NULL, column_info->typioparam,
                                          tupdesc->attrs[i].atttypmod);
            nulls[i] = true;
        } else {
            char       *s = NULL;

            if (jtype == JSONOID) {
                /* already done the hard work in the json case */
                s = hashentry->val;
            } else {
                if (v->type == jbvString) {
                    s = pnstrdup(v->string.val, v->string.len);
                } else if (v->type == jbvBool) {
                    s = pnstrdup((v->boolean) ? "t" : "f", 1);
                } else if (v->type == jbvNumeric) {
                    s = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v->numeric)));
                } else if (!use_json_as_text) {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("cannot populate with a nested object unless use_json_as_text is true")));
                } else if (v->type == jbvBinary) {
                    s = JsonbToCString(NULL, v->binary.data, v->binary.len);
                } else {
                    elog(ERROR, "invalid jsonb type");
                }
            }

            values[i] = InputFunctionCall(&column_info->proc, s,
                                          column_info->typioparam, tupdesc->attrs[i].atttypmod);
            nulls[i] = false;
        }
    }

    rettuple = heap_form_tuple(tupdesc, values, nulls);
    ReleaseTupleDesc(tupdesc);
    PG_RETURN_DATUM(HeapTupleGetDatum(rettuple));
}

/*
 * get_json_object_as_hash
 *
 * decompose a json object into a hash table.
 *
 * Currently doesn't allow anything but a flat object. Should this
 * change?
 *
 * funcname argument allows caller to pass in its name for use in
 * error messages.
 */
static HTAB *get_json_object_as_hash(text *json, char *funcname, bool use_json_as_text)
{
    HASHCTL         ctl;
    HTAB           *tab = NULL;
    JHashState     *state = NULL;
    JsonLexContext *lex = makeJsonLexContext(json, true);
    JsonSemAction  *sem = NULL;

    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(JsonHashEntry);
    ctl.hcxt = CurrentMemoryContext;
    tab = hash_create("json object hashtable", 100, &ctl, HASH_ELEM | HASH_CONTEXT);

    state = (JHashState *)palloc0(sizeof(JHashState));
    sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));

    state->function_name = funcname;
    state->hash = tab;
    state->lex = lex;
    state->use_json_as_text = use_json_as_text;

    sem->semstate = (void *) state;
    sem->array_start = hash_array_start;
    sem->scalar = hash_scalar;
    sem->object_field_start = hash_object_field_start;
    sem->object_field_end = hash_object_field_end;

    pg_parse_json(lex, sem);

    return tab;
}

static void hash_object_field_start(void *state, char *fname, bool isnull)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level > 1) {
        return;
    }

    if (_state->lex->token_type == JSON_TOKEN_ARRAY_START || _state->lex->token_type == JSON_TOKEN_OBJECT_START) {
        if (!_state->use_json_as_text) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call %s on a nested object",
                            _state->function_name)));
        }
        _state->save_json_start = _state->lex->token_start;
    } else {
        /* must be a scalar */
        _state->save_json_start = NULL;
    }
}

static void hash_object_field_end(void *state, char *fname, bool isnull)
{
    JHashState    *_state = (JHashState *) state;
    JsonHashEntry *hashentry = NULL;
    bool           found = false;
    char           name[NAMEDATALEN];

    /*
     * ignore field names >= NAMEDATALEN - they can't match a record field
     * ignore nested fields.
     */
    if (_state->lex->lex_level > 2 || strlen(fname) >= NAMEDATALEN) {
        return;
    }

    errno_t rc = memset_s(&name, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(name, NAMEDATALEN, fname, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    hashentry = (JsonHashEntry *)hash_search(_state->hash, name, HASH_ENTER, &found);

    /*
     * found being true indicates a duplicate. We don't do anything about
     * that, a later field with the same name overrides the earlier field.
     */

    hashentry->isnull = isnull;
    if (_state->save_json_start != NULL) {
        int   len = _state->lex->prev_token_terminator - _state->save_json_start;
        char *val = (char *)palloc((len + 1) * sizeof(char));

        rc = memcpy_s(val, (len + 1) * sizeof(char), _state->save_json_start, len);
        securec_check(rc, "\0", "\0");
        val[len] = '\0';
        hashentry->val = val;
    } else {
        /* must have had a scalar instead */
        hashentry->val = _state->saved_scalar;
    }
}

static void hash_array_start(void *state)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call %s on an array", _state->function_name)));
    }
}

static void hash_scalar(void *state, char *token, JsonTokenType tokentype)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call %s on a scalar", _state->function_name)));
    }

    if (_state->lex->lex_level == 1) {
        _state->saved_scalar = token;
    }
}


/*
 * SQL function json_populate_recordset
 *
 * set fields in a set of records from the argument json,
 * which must be an array of objects.
 *
 * similar to json_populate_record, but the tuple-building code
 * is pushed down into the semantic action handlers so it's done
 * per object in the array.
 */
Datum jsonb_populate_recordset(PG_FUNCTION_ARGS)
{
    return populate_recordset_worker(fcinfo, true);
}

static void make_row_from_rec_and_jsonb(Jsonb *element, PopulateRecordsetState *state)
{
    Datum        *values = NULL;
    bool         *nulls = NULL;
    int           i;
    RecordIOData *my_extra = state->my_extra;
    int           ncolumns = my_extra->ncolumns;
    TupleDesc     tupdesc = state->ret_tdesc;
    HeapTupleHeader rec = state->rec;
    HeapTuple     rettuple;

    values = (Datum *) palloc(ncolumns * sizeof(Datum));
    nulls = (bool *) palloc(ncolumns * sizeof(bool));

    if (state->rec) {
        HeapTupleData tuple;
        /* Build a temporary HeapTuple control structure */
        tuple.t_len = HeapTupleHeaderGetDatumLength(state->rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid = InvalidOid;
        tuple.t_data = state->rec;

        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);
    } else {
        for (i = 0; i < ncolumns; ++i) {
            values[i] = (Datum) 0;
            nulls[i] = true;
        }
    }

    for (i = 0; i < ncolumns; ++i) {
        ColumnIOData *column_info = &my_extra->columns[i];
        Oid           column_type = tupdesc->attrs[i].atttypid;
        JsonbValue   *v = NULL;
        char         *key = NULL;

        /* Ignore dropped columns in datatype */
        if (tupdesc->attrs[i].attisdropped) {
            nulls[i] = true;
            continue;
        }
        key = NameStr(tupdesc->attrs[i].attname);
        v = findJsonbValueFromSuperHeaderLen(VARDATA(element), JB_FOBJECT, key, strlen(key));

        /*
         * We can't just skip here if the key wasn't found since we might have
         * a domain to deal with. If we were passed in a non-null record
         * datum, we assume that the existing values are valid (if they're
         * not, then it's not our fault), but if we were passed in a null,
         * then every field which we don't populate needs to be run through
         * the input function just in case it's a domain type.
         */
        if (v == NULL && rec) {
            continue;
        }

        /*
         * Prepare to convert the column value from text
         */
        if (column_info->column_type != column_type) {
            getTypeInputInfo(column_type, &column_info->typiofunc, &column_info->typioparam);
            fmgr_info_cxt(column_info->typiofunc, &column_info->proc, state->fn_mcxt);
            column_info->column_type = column_type;
        }
        if (v == NULL || v->type == jbvNull) {
            /*
             * Need InputFunctionCall to happen even for nulls, so that domain
             * checks are done
             */
            values[i] = InputFunctionCall(&column_info->proc, NULL, column_info->typioparam,
                                          tupdesc->attrs[i].atttypmod);
            nulls[i] = true;
        } else {
            char *s = NULL;

            if (v->type == jbvString) {
                s = pnstrdup(v->string.val, v->string.len);
            } else if (v->type == jbvBool) {
                s = pnstrdup((v->boolean) ? "t" : "f", 1);
            } else if (v->type == jbvNumeric) {
                s = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v->numeric)));
            } else if (!state->use_json_as_text) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("cannot populate with a nested object unless use_json_as_text is true")));
            } else if (v->type == jbvBinary) {
                s = JsonbToCString(NULL, v->binary.data, v->binary.len);
            } else {
                elog(ERROR, "invalid jsonb type");
            }

            values[i] = InputFunctionCall(&column_info->proc, s, column_info->typioparam, tupdesc->attrs[i].atttypmod);
            nulls[i] = false;
        }
    }

    rettuple = heap_form_tuple(tupdesc, values, nulls);
    tuplestore_puttuple(state->tuple_store, rettuple);
}

Datum json_populate_recordset(PG_FUNCTION_ARGS)
{
    return populate_recordset_worker(fcinfo, true);
}

Datum json_to_recordset(PG_FUNCTION_ARGS)
{
    return populate_recordset_worker(fcinfo, false);
}

/*
 * common worker for json_populate_recordset() and json_to_recordset()
 */
static inline Datum populate_recordset_worker(FunctionCallInfo fcinfo, bool have_record_arg)
{
    Oid            argtype;
    Oid            jtype = get_fn_expr_argtype(fcinfo->flinfo, have_record_arg ? 1 : 0);
    bool           use_json_as_text = false;
    ReturnSetInfo *rsi = NULL;
    MemoryContext  old_cxt;
    Oid            tupType;
    int32          tupTypmod;
    HeapTupleHeader rec;
    TupleDesc      tupdesc;
    bool           needforget = false;
    RecordIOData  *my_extra = NULL;
    int            ncolumns;
    PopulateRecordsetState *state = NULL;

    if (have_record_arg) {
        argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
        use_json_as_text = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
        if (!type_is_rowtype(argtype)) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("first argument of json_populate_recordset must be a row type")));
        }
    } else {
        argtype = InvalidOid;
        use_json_as_text = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
    }

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    if (!rsi || !IsA(rsi, ReturnSetInfo) ||
        ((uint32)rsi->allowedModes & SFRM_Materialize) == 0 ||
        rsi->expectedDesc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
    }
    rsi->returnMode = SFRM_Materialize;
    /*
     * get the tupdesc from the result set info - it must be a record type
     * because we already checked that arg1 is a record type, or we're in a
     * to_record function which returns a setof record.
     */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));
    }

    /* if the json is null send back an empty set */
    if (have_record_arg) {
        if (PG_ARGISNULL(1)) {
            PG_RETURN_NULL();
        }
        if (PG_ARGISNULL(0)) {
            rec = NULL;
        } else {
            /* using the arg tupdesc, because it may not be the same as the result tupdesc. */
            rec = PG_GETARG_HEAPTUPLEHEADER(0);
            tupdesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(rec), HeapTupleHeaderGetTypMod(rec));
            needforget = true;
        }
    } else {
        if (PG_ARGISNULL(1)) {
            PG_RETURN_NULL();
        }
        rec = NULL;
    }

    tupType = tupdesc->tdtypeid;
    tupTypmod = tupdesc->tdtypmod;
    ncolumns = tupdesc->natts;

    /*
     * We arrange to look up the needed I/O info just once per series of
     * calls, assuming the record type doesn't change underneath us.
     */
    my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->ncolumns != ncolumns) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
            sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData));
        my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
        my_extra->record_type = InvalidOid;
        my_extra->record_typmod = 0;
    }

    if (my_extra->record_type != tupType || my_extra->record_typmod != tupTypmod) {
        errno_t rc = memset_s(my_extra, sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData),
                              0, sizeof(RecordIOData) - sizeof(ColumnIOData) + ncolumns * sizeof(ColumnIOData));
        securec_check(rc, "\0", "\0");
        my_extra->record_type = tupType;
        my_extra->record_typmod = tupTypmod;
        my_extra->ncolumns = ncolumns;
    }
    state = (PopulateRecordsetState *)palloc0(sizeof(PopulateRecordsetState));
    /* make these in a sufficiently long-lived memory context */
    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
    state->ret_tdesc = CreateTupleDescCopy(tupdesc);
    if (needforget) {
        DecrTupleDescRefCount(tupdesc);
    }
    BlessTupleDesc(state->ret_tdesc);
    state->tuple_store = tuplestore_begin_heap((uint32)rsi->allowedModes & SFRM_Materialize_Random,
                                               false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(old_cxt);

    state->my_extra = my_extra;
    state->rec = rec;
    state->use_json_as_text = use_json_as_text;
    state->fn_mcxt = fcinfo->flinfo->fn_mcxt;

    if (jtype == JSONOID) {
        text           *json = PG_GETARG_TEXT_P(have_record_arg ? 1 : 0);
        JsonLexContext *lex = NULL;
        JsonSemAction  *sem = NULL;

        sem = (JsonSemAction *)palloc0(sizeof(JsonSemAction));
        lex = makeJsonLexContext(json, true);
        sem->semstate = (void *) state;
        sem->array_start = populate_recordset_array_start;
        sem->array_element_start = populate_recordset_array_element_start;
        sem->scalar = populate_recordset_scalar;
        sem->object_field_start = populate_recordset_object_field_start;
        sem->object_field_end = populate_recordset_object_field_end;
        sem->object_start = populate_recordset_object_start;
        sem->object_end = populate_recordset_object_end;
        state->lex = lex;

        pg_parse_json(lex, sem);
    } else {
        Jsonb      *jb = NULL;
        JsonbIterator *it = NULL;
        JsonbValue  v;
        bool        skipNested = false;
        int         r;

        Assert(jtype == JSONBOID);
        jb = PG_GETARG_JSONB(have_record_arg ? 1 : 0);
        if (JB_ROOT_IS_SCALAR(jb) || !JB_ROOT_IS_ARRAY(jb)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call jsonb_populate_recordset on non-array")));
        }
        it = JsonbIteratorInit(VARDATA_ANY(jb));
        while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
            skipNested = true;
            if (r == WJB_ELEM) {
                Jsonb      *element = JsonbValueToJsonb(&v);
                if (!JB_ROOT_IS_OBJECT(element)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("jsonb_populate_recordset argument must be an array of objects")));
                }
                make_row_from_rec_and_jsonb(element, state);
            }
        }
    }
    rsi->setResult = state->tuple_store;
    rsi->setDesc = state->ret_tdesc;
    PG_RETURN_NULL();
}

static void populate_recordset_object_start(void *state)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
    int                     lex_level = _state->lex->lex_level;
    HASHCTL                 ctl;

    if (lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_populate_recordset on an object")));
    } else if (lex_level > 1 && !_state->use_json_as_text) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         errmsg("cannot call json_populate_recordset with nested objects")));
    }

    /* set up a new hash for this entry */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(JsonHashEntry);
    ctl.hcxt = CurrentMemoryContext;
    _state->json_hash = hash_create("json object hashtable", 100, &ctl, HASH_ELEM | HASH_CONTEXT);
}

static void populate_recordset_object_end(void *state)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
    HTAB                   *json_hash = _state->json_hash;
    Datum                  *values = NULL;
    bool                   *nulls = NULL;
    char                    fname[NAMEDATALEN];
    int                     i;
    RecordIOData           *my_extra = _state->my_extra;
    int                     ncolumns = my_extra->ncolumns;
    TupleDesc               tupdesc = _state->ret_tdesc;
    JsonHashEntry          *hashentry = NULL;
    HeapTupleHeader         rec = _state->rec;
    HeapTuple               rettuple;

    if (_state->lex->lex_level > 1) {
        return;
    }
    values = (Datum *) palloc(ncolumns * sizeof(Datum));
    nulls = (bool *) palloc(ncolumns * sizeof(bool));
    if (_state->rec) {
        HeapTupleData tuple;
        /* Build a temporary HeapTuple control structure */
        tuple.t_len = HeapTupleHeaderGetDatumLength(_state->rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid = InvalidOid;
        tuple.t_data = _state->rec;
        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);
    } else {
        for (i = 0; i < ncolumns; ++i) {
            values[i] = (Datum) 0;
            nulls[i] = true;
        }
    }

    for (i = 0; i < ncolumns; ++i) {
        ColumnIOData *column_info = &my_extra->columns[i];
        Oid           column_type = tupdesc->attrs[i].atttypid;
        char         *value = NULL;

        /* Ignore dropped columns in datatype */
        if (tupdesc->attrs[i].attisdropped) {
            nulls[i] = true;
            continue;
        }

        errno_t rc = memset_s(fname, NAMEDATALEN, 0, NAMEDATALEN);
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(fname, NAMEDATALEN, NameStr(tupdesc->attrs[i].attname), NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
        hashentry = (JsonHashEntry *)hash_search(json_hash, fname, HASH_FIND, NULL);

        /*
         * we can't just skip here if the key wasn't found since we might have
         * a domain to deal with. If we were passed in a non-null record
         * datum, we assume that the existing values are valid (if they're
         * not, then it's not our fault), but if we were passed in a null,
         * then every field which we don't populate needs to be run through
         * the input function just in case it's a domain type.
         */
        if (hashentry == NULL && rec) {
            continue;
        }

        /*
         * Prepare to convert the column value from text
         */
        if (column_info->column_type != column_type) {
            getTypeInputInfo(column_type, &column_info->typiofunc, &column_info->typioparam);
            fmgr_info_cxt(column_info->typiofunc, &column_info->proc, _state->fn_mcxt);
            column_info->column_type = column_type;
        }
        if (hashentry == NULL || hashentry->isnull) {
            /*
             * need InputFunctionCall to happen even for nulls, so that domain
             * checks are done
             */
            values[i] = InputFunctionCall(&column_info->proc, NULL, column_info->typioparam,
                                          tupdesc->attrs[i].atttypmod);
            nulls[i] = true;
        } else {
            value = hashentry->val;
            values[i] = InputFunctionCall(&column_info->proc, value, column_info->typioparam,
                                          tupdesc->attrs[i].atttypmod);
            nulls[i] = false;
        }
    }

    rettuple = heap_form_tuple(tupdesc, values, nulls);
    tuplestore_puttuple(_state->tuple_store, rettuple);
    hash_destroy(json_hash);
}

static void populate_recordset_array_element_start(void *state, bool isnull)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

    if (_state->lex->lex_level == 1 &&
        _state->lex->token_type != JSON_TOKEN_OBJECT_START) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("must call json_populate_recordset on an array of objects")));
    }
}

static void populate_recordset_array_start(void *state)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
    if (_state->lex->lex_level != 0 && !_state->use_json_as_text) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_populate_recordset with nested arrays")));
    }
}

static void populate_recordset_scalar(void *state, char *token, JsonTokenType tokentype)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

    if (_state->lex->lex_level == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call json_populate_recordset on a scalar")));
    }

    if (_state->lex->lex_level == 2) {
        _state->saved_scalar = token;
    }
}

static void populate_recordset_object_field_start(void *state, char *fname, bool isnull)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

    if (_state->lex->lex_level > 2) {
        return;
    }

    if (_state->lex->token_type == JSON_TOKEN_ARRAY_START || _state->lex->token_type == JSON_TOKEN_OBJECT_START) {
        if (!_state->use_json_as_text) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call json_populate_recordset on a nested object")));
        }
        _state->save_json_start = _state->lex->token_start;
    } else {
        _state->save_json_start = NULL;
    }
}

static void populate_recordset_object_field_end(void *state, char *fname, bool isnull)
{
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
    JsonHashEntry          *hashentry = NULL;
    bool        found = false;
    char        name[NAMEDATALEN];

    /*
     * ignore field names >= NAMEDATALEN - they can't match a record field
     * ignore nested fields.
     */
    if (_state->lex->lex_level > 2 || strlen(fname) >= NAMEDATALEN) {
        return;
    }

    errno_t rc = memset_s(name, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(name, NAMEDATALEN, fname, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    hashentry = (JsonHashEntry *)hash_search(_state->json_hash, name, HASH_ENTER, &found);
    /*
     * found being true indicates a duplicate. We don't do anything about
     * that, a later field with the same name overrides the earlier field.
     */
    hashentry->isnull = isnull;
    if (_state->save_json_start != NULL) {
        int   len = _state->lex->prev_token_terminator - _state->save_json_start;
        char *val = (char *)palloc((len + 1) * sizeof(char));
        errno_t rc = memcpy_s(val, (len + 1) * sizeof(char), _state->save_json_start, len);
        securec_check(rc, "\0", "\0");
        val[len] = '\0';
        hashentry->val = val;
    } else {
        /* must have had a scalar instead */
        hashentry->val = _state->saved_scalar;
    }
}

/*
 * findJsonbValueFromSuperHeader() wrapper that sets up JsonbValue key string.
 */
static JsonbValue *findJsonbValueFromSuperHeaderLen(JsonbSuperHeader sheader, uint32 flags, char *key, uint32 keylen)
{
    JsonbValue  k;

    k.type = jbvString;
    k.string.val = key;
    k.string.len = keylen;

    return findJsonbValueFromSuperHeader(sheader, flags, NULL, &k);
}

static void
push_null_elements(JsonbParseState **ps, int num)
{
	JsonbValue	null;

	null.type = jbvNull;

	while (num-- > 0)
		pushJsonbValue(ps, WJB_ELEM, &null);
}

static void
addJsonbToParseState(JsonbParseState **jbps, Jsonb *jb)
{
    JsonbIterator *it;
    JsonbValue    *o = &(*jbps)->contVal;
    int           type;
    JsonbValue    v;

    it = JsonbIteratorInit(VARDATA(jb));

    Assert(o->type == jbvArray || o->type == jbvObject);

    if(JB_ROOT_IS_SCALAR(jb)) {
        (void) JsonbIteratorNext(&it, &v, false);
        (void) JsonbIteratorNext(&it, &v, false);

        switch(o->type)
        {
            case jbvArray:
                (void)pushJsonbValue(jbps, WJB_ELEM, &v);
                break;
            case jbvObject:
                (void)pushJsonbValue(jbps, WJB_VALUE, &v);
                break;
            default:
                elog(ERROR, "unexpected parent oe nested structure.");
        }
    } else {
        while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
            if (type == WJB_ELEM || type == WJB_KEY || type == WJB_VALUE) {
                (void)pushJsonbValue(jbps, type, &v);
            } else {
                (void)pushJsonbValue(jbps, type, NULL);
            }
        }
    }
}

/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 *
 * If JB_PATH_DELETE bit is set in op_type, the element is to be removed.
 *
 * If any bit mentioned in JB_PATH_CREATE_OR_INSERT is set in op_type,
 * we create the new value if the key or array index does not exist.
 *
 * Bits JB_PATH_INSERT_BEFORE and JB_PATH_INSERT_AFTER in op_type
 * behave as JB_PATH_CREATE if new value is inserted in JsonbObject.
 *
 * If JB_PATH_FILL_GAPS bit is set, this will change an assignment logic in
 * case if target is an array. The assignment index will not be restricted by
 * number of elements in the array, and if there are any empty slots between
 * last element of the array and a new one they will be filled with nulls. If
 * the index is negative, it still will be considered an index from the end
 * of the array. Of a part of the path is not present and this part is more
 * than just one last element, this flag will instruct to create the whole
 * chain of corresponding objects and insert the value.
 *
 * JB_PATH_CONSISTENT_POSITION for an array indicates that the caller wants to
 * keep values with fixed indices. Indices for existing elements could be
 * changed (shifted forward) in case if the array is prepended with a new value
 * and a negative index out of the range, so this behavior will be prevented
 * and return an error.
 *
 * All path elements before the last must already exist
 * whatever bits in op_type are set, or nothing is done.
 */
static JsonbValue* setPath(JsonbIterator **it, Datum *path_elems, bool *path_nulls, int path_len,
                            JsonbParseState **st, int level, Jsonb *newval, int op_type)
{
    JsonbValue  v;
    int r;
    JsonbValue *res;

    check_stack_depth();

    if (path_nulls[level])
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("path element at position %d is null", level + 1)));

    r = JsonbIteratorNext(it, &v, false);

    switch (r) {
        case WJB_BEGIN_ARRAY:

            /*
            * If instructed complain about attempts to replace within a raw
            * scalar value. This happens even when current level is equal to
            * path_len, because the last path key should also correspond to
            * an object or an array, not raw scalar.
            */
            if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1) && v.array.rawScalar)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("cannot replace existing key"),
                        errdetail("The path assumes key is a composite object, "
                                "but it is a scalar value.")));

            (void) pushJsonbValue(st, r, NULL);
            setPathArray(it, path_elems, path_nulls, path_len, st, level, newval, v.array.nElems, op_type);
            r = JsonbIteratorNext(it, &v, false);
            Assert(r == WJB_END_ARRAY);
            res = pushJsonbValue(st, r, NULL);
            break;
        case WJB_BEGIN_OBJECT:
            (void) pushJsonbValue(st, r, NULL);
            setPathObject(it, path_elems, path_nulls, path_len, st, level, newval, v.object.nPairs, op_type);
            r = JsonbIteratorNext(it, &v, true);
            Assert(r == WJB_END_OBJECT);
            res = pushJsonbValue(st, r, NULL);
            break;
        case WJB_ELEM:
        case WJB_VALUE:

            /*
            * If instructed complain about attempts to replace within a
            * scalar value. This happens even when current level is equal to
            * path_len, because the last path key should also correspond to
            * an object or an array, not an element or value.
            */
            if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot replace existing key"),
                        errdetail("The path assumes key is a composite object, but it is a scalar value.")));

            res = pushJsonbValue(st, r, &v);
            break;
        default:
            elog(ERROR, "unrecognized iterator result: %d", (int) r);
            res = NULL;			/* keep compiler quiet */
            break;
    }

    return res;
}

/*
 * Object walker for setPath
 */
static void setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
                            int path_len, JsonbParseState **st, int level, Jsonb *newval, uint32 npairs, int op_type)
{
    text       *pathelem = NULL;
    int	        i;
    JsonbValue  k, v;
    bool        done = false;

    if (level >= path_len || path_nulls[level]) {
        done = true;
    } else {
        /* The path Datum could be toasted, in which case we must detoast it */
        pathelem = DatumGetTextPP(path_elems[level]);
    }

    /* empty object is a special case for create */
    if ((npairs == 0) && (op_type & JB_PATH_CREATE_OR_INSERT) && (level == path_len - 1)) {
        JsonbValue	newkey;

        newkey.type = jbvString;
        newkey.string.val = VARDATA_ANY(pathelem);
        newkey.string.len = VARSIZE_ANY_EXHDR(pathelem);

        (void) pushJsonbValue(st, WJB_KEY, &newkey);
        addJsonbToParseState(st, newval);
    }

    for (i = 0; i < npairs; i++) {
        int r = JsonbIteratorNext(it, &k, true);

        Assert(r == WJB_KEY);

        if (!done && k.string.len == VARSIZE_ANY_EXHDR(pathelem) &&
            memcmp(k.string.val, VARDATA_ANY(pathelem), k.string.len) == 0) {

            if (level == path_len - 1) {
                /*
                * called from jsonb_insert(), it forbids redefining an
                * existing value
                */
                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("cannot replace existing key"),
                            errhint("Try using the function jsonb_set "
                                    "to replace key value.")));

                r = JsonbIteratorNext(it, &v, true);	/* skip value */
                if (!(op_type & JB_PATH_DELETE)) {
                    (void) pushJsonbValue(st, WJB_KEY, &k);
                    addJsonbToParseState(st, newval);
                }
                done = true;
            } else {
                (void) pushJsonbValue(st, r, &k);
                setPath(it, path_elems, path_nulls, path_len,
                        st, level + 1, newval, op_type);
            }
        } else {
            if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done && level == path_len - 1 && i == npairs - 1) {
                JsonbValue	newkey;

                newkey.type = jbvString;
                newkey.string.val = VARDATA_ANY(pathelem);
                newkey.string.len = VARSIZE_ANY_EXHDR(pathelem);

                (void) pushJsonbValue(st, WJB_KEY, &newkey);
                addJsonbToParseState(st, newval);
            }

            (void) pushJsonbValue(st, r, &k);
            r = JsonbIteratorNext(it, &v, false);
            (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
            if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT) {
                int			walking_level = 1;

                while (walking_level != 0) {
                    r = JsonbIteratorNext(it, &v, false);

                    if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
                        ++walking_level;
                    if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
                        --walking_level;

                    (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
                }
            }
        }
    }
}

/*
* Array walker for setPath
*/
static void setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
            int path_len, JsonbParseState **st, int level, Jsonb *newval, uint32 nelems, int op_type)
{
    JsonbValue  v;
    int         idx,
                i;
    bool        done = false;

    /* pick correct index */
    if (level < path_len && !path_nulls[level]) {
        char	   *c = TextDatumGetCString(path_elems[level]);
        char	   *badp;
        long	    val;

        errno = 0;
        val = strtol(c, &badp, 10);
        if (errno != 0 || badp == c || badp[0] != '\0' || val > INT_MAX ||
                val < INT_MIN) 
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("path element at position %d is not an integer: \"%s\"",
                            level + 1, c)));
        idx = val;
    } else
        idx = nelems;

    if (idx < 0) {
        if (-idx > nelems) {
            /*
            * If asked to keep elements position consistent, it's not allowed
            * to prepend the array.
            */
            if (op_type & JB_PATH_CONSISTENT_POSITION)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("path element at position %d is out of range: %d",
                                level + 1, idx)));
            else
                idx = INT_MIN;
        } else
            idx = nelems + idx;
    }

    /*
    * Filling the gaps means there are no limits on the positive index are
    * imposed, we can set any element. Otherwise limit the index by nelems.
    */
    if (!(op_type & JB_PATH_FILL_GAPS)) {
        if (idx > 0 && idx > nelems)
            idx = nelems;
    }

    /*
    * if we're creating, and idx == INT_MIN, we prepend the new value to the
    * array also if the array is empty - in which case we don't really care
    * what the idx value is
    */
    if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) && (op_type & JB_PATH_CREATE_OR_INSERT)) {
        Assert(newval != NULL);
        addJsonbToParseState(st, newval);
        done = true;
    }

    /* iterate over the array elements */
    for (i = 0; i < nelems; i++) {
        int r;

        if (i == idx && level < path_len) {

            if (level == path_len - 1) {
                r = JsonbIteratorNext(it, &v, true);	/* skip */

                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
                    addJsonbToParseState(st, newval);

                /*
                * We should keep current value only in case of
                * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because
                * otherwise it should be deleted or replaced
                */
                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
                    (void) pushJsonbValue(st, r, &v);

                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
                    addJsonbToParseState(st, newval);
                done = true;
            } else
                (void) setPath(it, path_elems, path_nulls, path_len, st, level + 1, newval, op_type);
        } else {
            r = JsonbIteratorNext(it, &v, false);

            (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

            if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT) {
                int walking_level = 1;

                while (walking_level != 0) {
                    r = JsonbIteratorNext(it, &v, false);

                    if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
                        ++walking_level;
                    if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
                        --walking_level;

                    (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
                }
            }
        }
    }

    if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done && level == path_len - 1) {
        /*
        * If asked to fill the gaps, idx could be bigger than nelems, so
        * prepend the new element with nulls if that's the case.
        */
        if (op_type & JB_PATH_FILL_GAPS && idx > nelems)
            push_null_elements(st, idx - nelems);

        addJsonbToParseState(st, newval);
        done = true;
    }
}

/*
* SQL function jsonb_insert(jsonb, text[], jsonb, boolean)
*/
Datum jsonb_insert(PG_FUNCTION_ARGS)
{
    Jsonb      *in = PG_GETARG_JSONB(0);
    ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
    Jsonb      *newjsonb = PG_GETARG_JSONB(2);
    bool        after = PG_GETARG_BOOL(3);
    JsonbValue *res = NULL;
    Datum      *path_elems;
    bool       *path_nulls;
    int	        path_len;
    JsonbIterator *it;
    JsonbParseState *st = NULL;

    if (ARR_NDIM(path) > 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("wrong number of array subscripts")));

    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot set path in scalar")));

    deconstruct_array(path, TEXTOID, -1, false, 'i', &path_elems, &path_nulls, &path_len);

    if (path_len == 0)
        PG_RETURN_JSONB(in);

    it = JsonbIteratorInit(VARDATA(in));

    res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, newjsonb,
                after ? JB_PATH_INSERT_AFTER : JB_PATH_INSERT_BEFORE);

    Assert(res != NULL);

    PG_RETURN_JSONB(JsonbValueToJsonb(res));
}

/*
* SQL function jsonb_delete (jsonb, text)
*
* return a copy of the jsonb with the indicated item
* removed.
*/
Datum jsonb_delete(PG_FUNCTION_ARGS)
{
    Jsonb      *in = PG_GETARG_JSONB(0);
    text       *key = PG_GETARG_TEXT_PP(1);
    char       *keyptr = VARDATA_ANY(key);
    int	        keylen = VARSIZE_ANY_EXHDR(key);
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    JsonbValue	v, *res = NULL;
    bool        skipNested = false;
    int         r;

    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot delete from scalar")));

    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB(in);

    it = JsonbIteratorInit(VARDATA(in));

    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if ((r == WJB_ELEM || r == WJB_KEY) && (v.type == jbvString && keylen == v.string.len &&
            memcmp(keyptr, v.string.val, keylen) == 0)) {
            /* skip corresponding value as well */
            if (r == WJB_KEY)
                (void) JsonbIteratorNext(&it, &v, true);

            continue;
        }

        res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
    }

    Assert(res != NULL);

    PG_RETURN_JSONB(JsonbValueToJsonb(res));
}

/*
* SQL function jsonb_delete (jsonb, variadic text[])
*
* return a copy of the jsonb with the indicated items
* removed.
*/
Datum jsonb_delete_array(PG_FUNCTION_ARGS)
{
    Jsonb      *in = PG_GETARG_JSONB(0);
    ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
    Datum      *keys_elems;
    bool       *keys_nulls;
    int	        keys_len;
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    JsonbValue  v, *res = NULL;
    bool        skipNested = false;
    int         r;

    if (ARR_NDIM(keys) > 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("wrong number of array subscripts")));

    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot delete from scalar")));

    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB(in);

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &keys_elems, &keys_nulls, &keys_len);

    if (keys_len == 0)
        PG_RETURN_JSONB(in);

    it = JsonbIteratorInit(VARDATA(in));

    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if ((r == WJB_ELEM || r == WJB_KEY) && v.type == jbvString) {
            int			i;
            bool		found = false;

            for (i = 0; i < keys_len; i++) {
                char	   *keyptr;
                int			keylen;

                if (keys_nulls[i])
                    continue;

                /* We rely on the array elements not being toasted */
                keyptr = VARDATA_ANY(keys_elems[i]);
                keylen = VARSIZE_ANY_EXHDR(keys_elems[i]);
                if (keylen == v.string.len &&
                    memcmp(keyptr, v.string.val, keylen) == 0)
                {
                    found = true;
                    break;
                }
            }
            if (found) {
                /* skip corresponding value as well */
                if (r == WJB_KEY)
                    (void) JsonbIteratorNext(&it, &v, true);

                continue;
            }
        }

        res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
    }

    Assert(res != NULL);

    PG_RETURN_JSONB(JsonbValueToJsonb(res));
}

/*
* SQL function jsonb_delete (jsonb, int)
*
* return a copy of the jsonb with the indicated item
* removed. Negative int means count back from the
* end of the items.
*/
Datum jsonb_delete_idx(PG_FUNCTION_ARGS)
{
    Jsonb *in = PG_GETARG_JSONB(0);
    int	idx = PG_GETARG_INT32(1);
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    uint32      i = 0,
                n;
    JsonbValue  v,
            *res = NULL;
    int         r;

    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cannot delete from scalar")));

    if (JB_ROOT_IS_OBJECT(in))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cannot delete from object using integer index")));

    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB(in);

    it = JsonbIteratorInit(VARDATA(in));

    r = JsonbIteratorNext(&it, &v, false);
    Assert(r == WJB_BEGIN_ARRAY);
    n = v.array.nElems;

    if (idx < 0) {
        if (-idx > n)
            idx = n;
        else
            idx = n + idx;
    }

    if (idx >= n)
        PG_RETURN_JSONB(in);

    pushJsonbValue(&state, r, NULL);

    while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
    {
        if (r == WJB_ELEM) {
            if (i++ == idx)
                continue;
        }

        res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
    }

    Assert(res != NULL);

    PG_RETURN_JSONB(JsonbValueToJsonb(res));
}

/*
* SQL function jsonb_set(jsonb, text[], jsonb, boolean)
*/
Datum jsonb_set(PG_FUNCTION_ARGS)
{
    Jsonb	   *in = PG_GETARG_JSONB(0);
    ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
    Jsonb	   *newjsonb = PG_GETARG_JSONB(2);
    bool		create = PG_GETARG_BOOL(3);
    JsonbValue *res = NULL;
    Datum	   *path_elems;
    bool	   *path_nulls;
    int			path_len;
    JsonbIterator *it;
    JsonbParseState *st = NULL;

    if (ARR_NDIM(path) > 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("wrong number of array subscripts")));

    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot set path in scalar")));

    if (JB_ROOT_COUNT(in) == 0 && !create)
        PG_RETURN_JSONB(in);

    deconstruct_array(path, TEXTOID, -1, false, 'i', &path_elems, &path_nulls, &path_len);

    if (path_len == 0)
        PG_RETURN_JSONB(in);

    it = JsonbIteratorInit(VARDATA(in));

    res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, newjsonb, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

    Assert(res != NULL);

    PG_RETURN_JSONB(JsonbValueToJsonb(res));
}
