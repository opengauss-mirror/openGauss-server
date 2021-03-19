/* -------------------------------------------------------------------------
 *
 * dict_simple.c
 *		Simple dictionary: just lowercase and check for stopword
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/dict_simple.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/defrem.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"

typedef struct {
    StopList stoplist;
    bool accept;
} DictSimple;

Datum dsimple_init(PG_FUNCTION_ARGS)
{
    List* dictoptions = (List*)PG_GETARG_POINTER(0);
    ListCell* l = NULL;

    /* 3 params in dictoptions */
    char* stopfile = NULL;
    bool stoploaded = false;
    bool stopchanged = false;
    bool acceptvalue = true; /* default */
    bool acceptloaded = false;
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

        if (pg_strcasecmp("StopWords", defel->defname) == 0) {
            if (stoploaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple StopWords parameters")));
            }

            stopfile = defGetString(defel);
            stoploaded = true;

            /*
             * if create/alter dictionary, replace the user-defined
             * file name with the internal one.
             */
            if (ddlsql) {
                defel->arg = (Node*)makeString(dictprefix);
                if (optidx >= oldnum) {
                    stopchanged = true;
                }
            }
        } else if (pg_strcasecmp("Accept", defel->defname) == 0) {
            if (acceptloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple Accept parameters")));
            }

            acceptvalue = defGetBoolean(defel);
            acceptloaded = true;
        } else if (pg_strcasecmp("FilePath", defel->defname) == 0) {
            if (pathloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple FilePath parameters")));
            }

            /* user-defined filepath */
            pathname = defGetString(defel);
            pathloaded = true;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized simple dictionary parameter: \"%s\"", defel->defname)));
        }

        optidx++;
    }

    /* Not allowed filepath without dictionary file when create/alter */
    if (pathname != NULL && !stopchanged) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("FilePath parameter should be with StopWords")));
    }

    /* Get full file name with absolute path */
    List* userfiles = NIL;
    char* absfname = NULL;
    if (stoploaded) {
        absfname = get_tsearch_config_filename(stopfile, pathname, FILE_POSTFIX_STOP, ddlsql && stopchanged);
        if (ddlsql) {
            userfiles = lappend(userfiles, absfname);
        }
    } else {
        ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Missing StopWords parameter for simple dictionary")));
    }

    /*
     * Need to load dictionary for two cases:
     * 1.Use dictionary
     * 2.Create/alter dictionary query from user directly
     */
    DictSimple* d = NULL;
    if (!skipLoad(ddlsql)) {
        d = (DictSimple*)palloc0(sizeof(DictSimple));
        d->accept = acceptvalue;
        readstoplist(absfname, &d->stoplist, lowerstr);
    }

    if (ddlsql) {
        PG_RETURN_POINTER(userfiles);
    } else {
        PG_RETURN_POINTER(d);
    }
}

Datum dsimple_lexize(PG_FUNCTION_ARGS)
{
    DictSimple* d = (DictSimple*)PG_GETARG_POINTER(0);
    char* in = (char*)PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);
    char* txt = NULL;
    TSLexeme* res = NULL;

    txt = lowerstr_with_len(in, len);

    if (*txt == '\0' || searchstoplist(&(d->stoplist), txt)) {
        /* reject as stopword */
        pfree_ext(txt);
        res = (TSLexeme*)palloc0(sizeof(TSLexeme) * 2);
        PG_RETURN_POINTER(res);
    } else if (d->accept) {
        /* accept */
        res = (TSLexeme*)palloc0(sizeof(TSLexeme) * 2);
        res[0].lexeme = txt;
        PG_RETURN_POINTER(res);
    } else {
        /* report as unrecognized */
        pfree_ext(txt);
        PG_RETURN_POINTER(NULL);
    }
}

