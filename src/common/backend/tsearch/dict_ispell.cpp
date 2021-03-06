/* -------------------------------------------------------------------------
 *
 * dict_ispell.c
 *		Ispell dictionary interface
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/dict_ispell.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/defrem.h"
#include "tsearch/dicts/spell.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"

#define DISPELL_FILENUM 3
#define DISPELL_DICTFILE 0
#define DISPELL_AFFFILE 1
#define DISPELL_STOPWORDS 2

typedef struct {
    StopList stoplist;
    IspellDict obj;
} DictISpell;

Datum dispell_init(PG_FUNCTION_ARGS)
{
    List* dictoptions = (List*)PG_GETARG_POINTER(0);
    ListCell* l = NULL;

    /* 4 params in dictoptions */
    char* filenames[DISPELL_FILENUM] = {NULL};
    bool fileloaded[DISPELL_FILENUM] = {false};
    bool filechanged[DISPELL_FILENUM] = {false};
    char* pathname = NULL;
    bool pathloaded = false;

    /* if we are create/alter dictionary */
    const bool ddlsql = (3 == PG_NARGS());
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

        if (pg_strcasecmp(defel->defname, "DictFile") == 0) {
            if (fileloaded[DISPELL_DICTFILE]) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple DictFile parameters")));
            }

            filenames[DISPELL_DICTFILE] = defGetString(defel);
            fileloaded[DISPELL_DICTFILE] = true;

            /*
             * if create/alter dictionary, replace the user-defined
             * file name with the internal one.
             */
            if (ddlsql) {
                defel->arg = (Node*)makeString(dictprefix);
                if (optidx >= oldnum) {
                    filechanged[DISPELL_DICTFILE] = true;
                }
            }
        } else if (pg_strcasecmp(defel->defname, "AffFile") == 0) {
            if (fileloaded[DISPELL_AFFFILE]) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple AffFile parameters")));
            }

            filenames[DISPELL_AFFFILE] = defGetString(defel);
            fileloaded[DISPELL_AFFFILE] = true;

            /*
             * if create/alter dictionary, replace the user-defined
             * file name with the internal one.
             */
            if (ddlsql) {
                defel->arg = (Node*)makeString(dictprefix);
                if (optidx >= oldnum) {
                    filechanged[DISPELL_AFFFILE] = true;
                }
            }
        } else if (pg_strcasecmp(defel->defname, "StopWords") == 0) {
            if (fileloaded[DISPELL_STOPWORDS]) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple StopWords parameters")));
            }

            filenames[DISPELL_STOPWORDS] = defGetString(defel);
            fileloaded[DISPELL_STOPWORDS] = true;

            /*
             * if create/alter dictionary, replace the user-defined
             * file name with the internal one.
             */
            if (ddlsql) {
                defel->arg = (Node*)makeString(dictprefix);
                if (optidx >= oldnum) {
                    filechanged[DISPELL_STOPWORDS] = true;
                }
            }
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
                    errmsg("unrecognized Ispell parameter: \"%s\"", defel->defname)));
        }

        optidx++;
    }

    List* userfiles = NIL;
    DictISpell* d = NULL;
    if (fileloaded[DISPELL_DICTFILE] && fileloaded[DISPELL_AFFFILE]) {
        /* Not allowed only filepath without dictionary file updated when alter */
        if (pathname != NULL && !filechanged[DISPELL_DICTFILE] && !filechanged[DISPELL_AFFFILE] &&
            !filechanged[DISPELL_STOPWORDS]) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("FilePath parameter should be with DictFile/AffFile/StopWords")));
        }

        /* mark all file updated for they are copied from remote in cluster */
        if (skipLoad(ddlsql) && inCluster()) {
            filechanged[DISPELL_DICTFILE] = true;
            filechanged[DISPELL_AFFFILE] = true;
            filechanged[DISPELL_STOPWORDS] = true;
        }

        /* Get full file names with absolute path */
        char* absfname[DISPELL_FILENUM] = {NULL};
        /* DictFile */
        absfname[DISPELL_DICTFILE] = get_tsearch_config_filename(
            filenames[DISPELL_DICTFILE], pathname, FILE_POSTFIX_DICT, ddlsql && filechanged[DISPELL_DICTFILE]);
        if (ddlsql) {
            userfiles = lappend(userfiles, absfname[DISPELL_DICTFILE]);
        }
        /* AffFile */
        absfname[DISPELL_AFFFILE] = get_tsearch_config_filename(
            filenames[DISPELL_AFFFILE], pathname, FILE_POSTFIX_AFFIX, ddlsql && filechanged[DISPELL_AFFFILE]);
        if (ddlsql) {
            userfiles = lappend(userfiles, absfname[DISPELL_AFFFILE]);
        }
        /* StopWords */
        if (fileloaded[DISPELL_STOPWORDS]) {
            absfname[DISPELL_STOPWORDS] = get_tsearch_config_filename(
                filenames[DISPELL_STOPWORDS], pathname, FILE_POSTFIX_STOP, ddlsql && filechanged[DISPELL_STOPWORDS]);
            if (ddlsql) {
                userfiles = lappend(userfiles, absfname[DISPELL_STOPWORDS]);
            }
        } else {
            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Missing StopWords parameter for ispell dictionary")));
        }

        /*
         * Need to load dictionary for two cases:
         * 1.Use dictionary
         * 2.Create/alter dictionary query from user directly
         */
        if (!skipLoad(ddlsql)) {
            d = (DictISpell*)palloc0(sizeof(DictISpell));
            NIStartBuild(&(d->obj));

            NIImportDictionary(&(d->obj), absfname[DISPELL_DICTFILE]);
            NIImportAffixes(&(d->obj), absfname[DISPELL_AFFFILE]);

            readstoplist(absfname[DISPELL_STOPWORDS], &(d->stoplist), lowerstr);

            NISortDictionary(&(d->obj));
            NISortAffixes(&(d->obj));

            NIFinishBuild(&(d->obj));
        }
    } else if (!fileloaded[DISPELL_AFFFILE]) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("missing AffFile parameter")));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("missing DictFile parameter")));
    }

    if (ddlsql) {
        PG_RETURN_POINTER(userfiles);
    } else {
        PG_RETURN_POINTER(d);
    }
}

Datum dispell_lexize(PG_FUNCTION_ARGS)
{
    DictISpell* d = (DictISpell*)PG_GETARG_POINTER(0);
    char* in = (char*)PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);
    char* txt = NULL;
    TSLexeme *res = NULL;
    TSLexeme *ptr = NULL;
    TSLexeme *cptr = NULL;
    errno_t errorno = EOK;

    if (len <= 0) {
        PG_RETURN_POINTER(NULL);
    }

    txt = lowerstr_with_len(in, len);
    res = NINormalizeWord(&(d->obj), txt);
    if (res == NULL) {
        PG_RETURN_POINTER(NULL);
    }

    cptr = res;
    for (ptr = cptr; ptr->lexeme; ptr++) {
        if (searchstoplist(&(d->stoplist), ptr->lexeme)) {
            pfree_ext(ptr->lexeme);
            ptr->lexeme = NULL;
        } else {
            if (cptr != ptr) {
                errorno = memcpy_s(cptr, sizeof(TSLexeme), ptr, sizeof(TSLexeme));
                securec_check(errorno, "\0", "\0");
            }
            cptr++;
        }
    }
    cptr->lexeme = NULL;

    PG_RETURN_POINTER(res);
}

