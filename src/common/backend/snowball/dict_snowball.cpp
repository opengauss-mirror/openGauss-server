/* -------------------------------------------------------------------------
 *
 * dict_snowball.c
 *		Snowball dictionary
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/snowball/dict_snowball.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/defrem.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"

/* Some platforms define MAXINT and/or MININT, causing conflicts */
#ifdef MAXINT
#undef MAXINT
#endif
#ifdef MININT
#undef MININT
#endif

/* Now we can include the original Snowball header.h */
#include "snowball/libstemmer/header.h"
#include "snowball/libstemmer/stem_ISO_8859_1_danish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_dutch.h"
#include "snowball/libstemmer/stem_ISO_8859_1_english.h"
#include "snowball/libstemmer/stem_ISO_8859_1_finnish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_french.h"
#include "snowball/libstemmer/stem_ISO_8859_1_german.h"
#include "snowball/libstemmer/stem_ISO_8859_1_hungarian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_italian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_norwegian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_porter.h"
#include "snowball/libstemmer/stem_ISO_8859_1_portuguese.h"
#include "snowball/libstemmer/stem_ISO_8859_1_spanish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_swedish.h"
#include "snowball/libstemmer/stem_ISO_8859_2_romanian.h"
#include "snowball/libstemmer/stem_KOI8_R_russian.h"
#include "snowball/libstemmer/stem_UTF_8_danish.h"
#include "snowball/libstemmer/stem_UTF_8_dutch.h"
#include "snowball/libstemmer/stem_UTF_8_english.h"
#include "snowball/libstemmer/stem_UTF_8_finnish.h"
#include "snowball/libstemmer/stem_UTF_8_french.h"
#include "snowball/libstemmer/stem_UTF_8_german.h"
#include "snowball/libstemmer/stem_UTF_8_hungarian.h"
#include "snowball/libstemmer/stem_UTF_8_italian.h"
#include "snowball/libstemmer/stem_UTF_8_norwegian.h"
#include "snowball/libstemmer/stem_UTF_8_porter.h"
#include "snowball/libstemmer/stem_UTF_8_portuguese.h"
#include "snowball/libstemmer/stem_UTF_8_romanian.h"
#include "snowball/libstemmer/stem_UTF_8_russian.h"
#include "snowball/libstemmer/stem_UTF_8_spanish.h"
#include "snowball/libstemmer/stem_UTF_8_swedish.h"
#include "snowball/libstemmer/stem_UTF_8_turkish.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(dsnowball_init);
extern "C" Datum dsnowball_init(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dsnowball_lexize);
extern "C" Datum dsnowball_lexize(PG_FUNCTION_ARGS);

/* List of supported modules */
typedef struct stemmer_module {
    const char* name;
    pg_enc enc;
    struct SN_env* (*create)(void);
    void (*close)(struct SN_env*);
    int (*stem)(struct SN_env*);
} stemmer_module;

static const stemmer_module stemmer_modules[] = {
    /*
     * Stemmers list from Snowball distribution
     */
    {"danish", PG_LATIN1, danish_ISO_8859_1_create_env, danish_ISO_8859_1_close_env, danish_ISO_8859_1_stem},
    {"dutch", PG_LATIN1, dutch_ISO_8859_1_create_env, dutch_ISO_8859_1_close_env, dutch_ISO_8859_1_stem},
    {"english", PG_LATIN1, english_ISO_8859_1_create_env, english_ISO_8859_1_close_env, english_ISO_8859_1_stem},
    {"finnish", PG_LATIN1, finnish_ISO_8859_1_create_env, finnish_ISO_8859_1_close_env, finnish_ISO_8859_1_stem},
    {"french", PG_LATIN1, french_ISO_8859_1_create_env, french_ISO_8859_1_close_env, french_ISO_8859_1_stem},
    {"german", PG_LATIN1, german_ISO_8859_1_create_env, german_ISO_8859_1_close_env, german_ISO_8859_1_stem},
    {"hungarian",
        PG_LATIN1,
        hungarian_ISO_8859_1_create_env,
        hungarian_ISO_8859_1_close_env,
        hungarian_ISO_8859_1_stem},
    {"italian", PG_LATIN1, italian_ISO_8859_1_create_env, italian_ISO_8859_1_close_env, italian_ISO_8859_1_stem},
    {"norwegian",
        PG_LATIN1,
        norwegian_ISO_8859_1_create_env,
        norwegian_ISO_8859_1_close_env,
        norwegian_ISO_8859_1_stem},
    {"porter", PG_LATIN1, porter_ISO_8859_1_create_env, porter_ISO_8859_1_close_env, porter_ISO_8859_1_stem},
    {"portuguese",
        PG_LATIN1,
        portuguese_ISO_8859_1_create_env,
        portuguese_ISO_8859_1_close_env,
        portuguese_ISO_8859_1_stem},
    {"spanish", PG_LATIN1, spanish_ISO_8859_1_create_env, spanish_ISO_8859_1_close_env, spanish_ISO_8859_1_stem},
    {"swedish", PG_LATIN1, swedish_ISO_8859_1_create_env, swedish_ISO_8859_1_close_env, swedish_ISO_8859_1_stem},
    {"romanian", PG_LATIN2, romanian_ISO_8859_2_create_env, romanian_ISO_8859_2_close_env, romanian_ISO_8859_2_stem},
    {"russian", PG_KOI8R, russian_KOI8_R_create_env, russian_KOI8_R_close_env, russian_KOI8_R_stem},
    {"danish", PG_UTF8, danish_UTF_8_create_env, danish_UTF_8_close_env, danish_UTF_8_stem},
    {"dutch", PG_UTF8, dutch_UTF_8_create_env, dutch_UTF_8_close_env, dutch_UTF_8_stem},
    {"english", PG_UTF8, english_UTF_8_create_env, english_UTF_8_close_env, english_UTF_8_stem},
    {"finnish", PG_UTF8, finnish_UTF_8_create_env, finnish_UTF_8_close_env, finnish_UTF_8_stem},
    {"french", PG_UTF8, french_UTF_8_create_env, french_UTF_8_close_env, french_UTF_8_stem},
    {"german", PG_UTF8, german_UTF_8_create_env, german_UTF_8_close_env, german_UTF_8_stem},
    {"hungarian", PG_UTF8, hungarian_UTF_8_create_env, hungarian_UTF_8_close_env, hungarian_UTF_8_stem},
    {"italian", PG_UTF8, italian_UTF_8_create_env, italian_UTF_8_close_env, italian_UTF_8_stem},
    {"norwegian", PG_UTF8, norwegian_UTF_8_create_env, norwegian_UTF_8_close_env, norwegian_UTF_8_stem},
    {"porter", PG_UTF8, porter_UTF_8_create_env, porter_UTF_8_close_env, porter_UTF_8_stem},
    {"portuguese", PG_UTF8, portuguese_UTF_8_create_env, portuguese_UTF_8_close_env, portuguese_UTF_8_stem},
    {"romanian", PG_UTF8, romanian_UTF_8_create_env, romanian_UTF_8_close_env, romanian_UTF_8_stem},
    {"russian", PG_UTF8, russian_UTF_8_create_env, russian_UTF_8_close_env, russian_UTF_8_stem},
    {"spanish", PG_UTF8, spanish_UTF_8_create_env, spanish_UTF_8_close_env, spanish_UTF_8_stem},
    {"swedish", PG_UTF8, swedish_UTF_8_create_env, swedish_UTF_8_close_env, swedish_UTF_8_stem},
    {"turkish", PG_UTF8, turkish_UTF_8_create_env, turkish_UTF_8_close_env, turkish_UTF_8_stem},

    /*
     * Stemmer with PG_SQL_ASCII encoding should be valid for any server
     * encoding
     */
    {"english", PG_SQL_ASCII, english_ISO_8859_1_create_env, english_ISO_8859_1_close_env, english_ISO_8859_1_stem},

    {NULL, (pg_enc)0, NULL, NULL, NULL} /* list end marker */
};

typedef struct DictSnowball {
    struct SN_env* z;
    StopList stoplist;
    bool needrecode; /* needs recoding before/after call stem */
    int (*stem)(struct SN_env* z);

    /*
     * snowball saves alloced memory between calls, so we should run it in our
     * private memory context. Note, init function is executed in long lived
     * context, so we just remember CurrentMemoryContext
     */
    MemoryContext dictCtx;
} DictSnowball;

static void locate_stem_module(DictSnowball* d, char* lang)
{
    const stemmer_module* m = NULL;

    /*
     * First, try to find exact match of stemmer module. Stemmer with
     * PG_SQL_ASCII encoding is treated as working with any server encoding
     */
    for (m = stemmer_modules; m->name; m++) {
        if ((m->enc == PG_SQL_ASCII || m->enc == GetDatabaseEncoding()) && pg_strcasecmp(m->name, lang) == 0) {
            d->stem = m->stem;
            d->z = m->create();
            d->needrecode = false;
            return;
        }
    }

    /*
     * Second, try to find stemmer for needed language for UTF8 encoding.
     */
    for (m = stemmer_modules; m->name; m++) {
        if (m->enc == PG_UTF8 && pg_strcasecmp(m->name, lang) == 0) {
            d->stem = m->stem;
            d->z = m->create();
            d->needrecode = true;
            return;
        }
    }

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("no Snowball stemmer available for language \"%s\" and encoding \"%s\"",
                lang,
                GetDatabaseEncodingName())));
}

Datum dsnowball_init(PG_FUNCTION_ARGS)
{
    List* dictoptions = (List*)PG_GETARG_POINTER(0);
    ListCell* l = NULL;

    /* 3 params in dictoptions */
    char* stopfile = NULL;
    bool stoploaded = false;
    bool stopchanged = false;
    char* stemlang = NULL;
    bool stemloaded = false;
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
        } else if (pg_strcasecmp("Language", defel->defname) == 0) {
            if (stemloaded) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("multiple Language parameters")));
            }

            stemlang = defGetString(defel);
            stemloaded = true;
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
                    errmsg("unrecognized Snowball parameter: \"%s\"", defel->defname)));
        }

        optidx++;
    }

    if (!stemloaded) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("missing Language parameter")));
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
        ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Missing StopWords parameter for snowball dictionary")));
    }

    /*
     * Need to load dictionary for two cases:
     * 1.Use dictionary
     * 2.Create/alter dictionary query from user directly
     */
    DictSnowball* d = NULL;
    if (!skipLoad(ddlsql)) {
        d = (DictSnowball*)palloc0(sizeof(DictSnowball));
        readstoplist(absfname, &d->stoplist, lowerstr);
        locate_stem_module(d, stemlang);
        d->dictCtx = CurrentMemoryContext;
    }

    if (ddlsql) {
        PG_RETURN_POINTER(userfiles);
    } else {
        PG_RETURN_POINTER(d);
    }
}

Datum dsnowball_lexize(PG_FUNCTION_ARGS)
{
    DictSnowball* d = (DictSnowball*)PG_GETARG_POINTER(0);
    char* in = (char*)PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);
    char* txt = lowerstr_with_len(in, len);
    TSLexeme* res = (TSLexeme*)palloc0(sizeof(TSLexeme) * 2);

    if (*txt == '\0' || searchstoplist(&(d->stoplist), txt)) {
        pfree_ext(txt);
    } else {
        MemoryContext saveCtx;

        /*
         * recode to utf8 if stemmer is utf8 and doesn't match server encoding
         */
        if (d->needrecode) {
            char* recoded = NULL;

            recoded =
                (char*)pg_do_encoding_conversion((unsigned char*)txt, strlen(txt), GetDatabaseEncoding(), PG_UTF8);
            if (recoded != txt) {
                pfree_ext(txt);
                txt = recoded;
            }
        }

        /* see comment about d->dictCtx */
        saveCtx = MemoryContextSwitchTo(d->dictCtx);
        SN_set_current(d->z, strlen(txt), (symbol*)txt);
        d->stem(d->z);
        MemoryContextSwitchTo(saveCtx);

        if (d->z->p && d->z->l) {
            txt = (char*)repalloc(txt, d->z->l + 1);
            errno_t errorno = EOK;
            errorno = memcpy_s(txt, d->z->l + 1, d->z->p, d->z->l);
            securec_check_c(errorno, "\0", "\0");
            txt[d->z->l] = '\0';
        }

        /* back recode if needed */
        if (d->needrecode) {
            char* recoded = NULL;

            recoded =
                (char*)pg_do_encoding_conversion((unsigned char*)txt, strlen(txt), PG_UTF8, GetDatabaseEncoding());
            if (recoded != txt) {
                pfree_ext(txt);
                txt = recoded;
            }
        }

        res->lexeme = txt;
    }

    PG_RETURN_POINTER(res);
}

