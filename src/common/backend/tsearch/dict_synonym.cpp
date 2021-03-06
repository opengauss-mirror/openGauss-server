/* -------------------------------------------------------------------------
 *
 * dict_synonym.c
 *		Synonym dictionary: replace word by its synonym
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/dict_synonym.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/defrem.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"

typedef struct {
    char* in;
    char* out;
    int outlen;
    uint16 flags;
} Syn;

typedef struct {
    int len; /* length of syn array */
    Syn* syn;
    bool case_sensitive;
} DictSyn;

/*
 * Finds the next whitespace-delimited word within the 'in' string.
 * Returns a pointer to the first character of the word, and a pointer
 * to the next byte after the last character in the word (in *end).
 * Character '*' at the end of word will not be threated as word
 * charater if flags is not null.
 */
static char* findwrd(char* in, char** end, uint16* flags)
{
    char* start = NULL;
    char* lastchar = NULL;

    /* Skip leading spaces */
    while (*in && t_isspace(in)) {
        in += pg_mblen(in);
    }

    /* Return NULL on empty lines */
    if (*in == '\0') {
        *end = NULL;
        return NULL;
    }

    lastchar = start = in;

    /* Find end of word */
    while (*in && !t_isspace(in)) {
        lastchar = in;
        in += pg_mblen(in);
    }

    if (in - lastchar == 1 && t_iseq(lastchar, '*') && flags) {
        *flags = TSL_PREFIX;
        *end = lastchar;
    } else {
        if (flags != NULL) {
            *flags = 0;
        }
        *end = in;
    }

    return start;
}

static int compareSyn(const void* a, const void* b)
{
    return strcmp(((const Syn*)a)->in, ((const Syn*)b)->in);
}

/*
 * @Description: Delete internal dictionary files of multiple dictionaries.
 * @in filename: dictionary file name
 * @in d: DictSyn
 * @out: void
 */
static void loadSyn(const char* filename, DictSyn* d)
{
    tsearch_readline_state trst;
    char *starti = NULL;
    char *starto = NULL;
    char *end = NULL;
    int cur = 0;
    char* line = NULL;
    uint16 flags = 0;

    if (!tsearch_readline_begin(&trst, filename)) {
        ereport(
            ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("could not open synonym file \"%s\": %m", filename)));
    }

    while ((line = tsearch_readline(&trst)) != NULL) {
        starti = findwrd(line, &end, NULL);
        if (starti == NULL) {
            /* Empty line */
            goto skipline;
        }
        if (*end == '\0') {
            /* A line with only one word. Ignore silently. */
            goto skipline;
        }
        *end = '\0';

        starto = findwrd(end + 1, &end, &flags);
        if (starto == NULL) {
            /* A line with only one word (+whitespace). Ignore silently. */
            goto skipline;
        }
        *end = '\0';

        /*
         * starti now points to the first word, and starto to the second word
         * on the line, with a \0 terminator at the end of both words.
         */

        if (cur >= d->len) {
            if (d->len == 0) {
                d->len = 64;
                d->syn = (Syn*)palloc(sizeof(Syn) * d->len);
            } else {
                d->len *= 2;
                d->syn = (Syn*)repalloc(d->syn, sizeof(Syn) * d->len);
            }
        }

        if (d->case_sensitive) {
            d->syn[cur].in = pstrdup(starti);
            d->syn[cur].out = pstrdup(starto);
        } else {
            d->syn[cur].in = lowerstr(starti);
            d->syn[cur].out = lowerstr(starto);
        }

        d->syn[cur].outlen = strlen(starto);
        d->syn[cur].flags = flags;

        cur++;

    skipline:
        pfree_ext(line);
    }

    tsearch_readline_end(&trst);

    d->len = cur;
    qsort(d->syn, d->len, sizeof(Syn), compareSyn);
}

Datum dsynonym_init(PG_FUNCTION_ARGS)
{
    List* dictoptions = (List*)PG_GETARG_POINTER(0);
    ListCell* l = NULL;

    /* 3 params in dictoptions */
    char* synfile = NULL;
    bool synloaded = false;
    bool synchanged = false;
    bool case_sensitive = false;
    bool caseloaded = false;
    char* pathname = NULL;
    bool pathloaded = false;

    /* if we are create/alter dictionary */
    bool ddlsql = (3 == PG_NARGS());
    char* dictprefix = NULL;
    int oldnum = list_length(dictoptions);
    int optidx = 0;

    if (ddlsql) {
        dictprefix = PG_GETARG_CSTRING(1);
        oldnum = PG_GETARG_INT32(2);
        if (dictprefix == NULL || oldnum < 0) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Init dictionary failed for invalid inputs")));
        }
    }

    foreach (l, dictoptions) {
        DefElem* defel = (DefElem*)lfirst(l);

        if (pg_strcasecmp("Synonyms", defel->defname) == 0) {
            if (synloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple Synonyms parameters")));
            }

            synfile = defGetString(defel);
            synloaded = true;

            /*
             * if create/alter dictionary, replace the user-defined
             * file name with the internal one.
             */
            if (ddlsql) {
                defel->arg = (Node*)makeString(dictprefix);
                if (optidx >= oldnum) {
                    synchanged = true;
                }
            }
        } else if (pg_strcasecmp("CaseSensitive", defel->defname) == 0) {
            if (caseloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple CaseSensitive parameters")));
            }

            case_sensitive = defGetBoolean(defel);
            caseloaded = true;
        } else if (pg_strcasecmp("FilePath", defel->defname) == 0) {
            if (pathloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple FilePath parameters")));
            }

            /* user defined filepath */
            pathname = defGetString(defel);
            pathloaded = true;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized synonym parameter: \"%s\"", defel->defname)));
        }

        optidx++;
    }

    if (!synloaded) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("missing Synonyms parameter")));
    }

    /* Not allowed filepath without dictionary file when create/alter */
    if (pathname != NULL && !synchanged) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("FilePath parameter should be with Synonyms")));
    }

    /* Get full file name with absolute path */
    List* userfiles = NIL;
    char* absfname = get_tsearch_config_filename(synfile, pathname, FILE_POSTFIX_SYN, ddlsql && synchanged);
    if (ddlsql) {
        userfiles = lappend(userfiles, absfname);
    }

    /*
     * Need to load dictionary for two cases:
     * 1.Use dictionary
     * 2.Create/alter dictionary query from user directly
     */
    DictSyn* d = NULL;
    if (!skipLoad(ddlsql)) {
        d = (DictSyn*)palloc0(sizeof(DictSyn));
        d->case_sensitive = case_sensitive;
        loadSyn(absfname, d);
    }

    if (ddlsql) {
        PG_RETURN_POINTER(userfiles);
    } else {
        PG_RETURN_POINTER(d);
    }
}

Datum dsynonym_lexize(PG_FUNCTION_ARGS)
{
    DictSyn* d = (DictSyn*)PG_GETARG_POINTER(0);
    char* in = (char*)PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);
    Syn key;
    Syn *found = NULL;
    TSLexeme* res = NULL;

    /* note: d->len test protects against Solaris bsearch-of-no-items bug */
    if (len <= 0 || d->len <= 0) {
        PG_RETURN_POINTER(NULL);
    }

    if (d->case_sensitive) {
        key.in = pnstrdup(in, len);
    } else {
        key.in = lowerstr_with_len(in, len);
    }

    key.out = NULL;

    found = (Syn*)bsearch(&key, d->syn, d->len, sizeof(Syn), compareSyn);
    pfree_ext(key.in);

    if (found == NULL) {
        PG_RETURN_POINTER(NULL);
    }

    res = (TSLexeme*)palloc0(sizeof(TSLexeme) * 2);
    res[0].lexeme = pnstrdup(found->out, found->outlen);
    res[0].flags = found->flags;

    PG_RETURN_POINTER(res);
}

