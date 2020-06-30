/* -------------------------------------------------------------------------
 *
 * ts_public.h
 *	  Public interface to various tsearch modules, such as
 *	  parsers and dictionaries.
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 *
 * src/include/tsearch/ts_public.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _PG_TS_PUBLIC_H_
#define _PG_TS_PUBLIC_H_

#include "nodes/pg_list.h"
#include "pgxc/pgxc.h"
#include "tsearch/ts_type.h"

#define DICT_SEPARATOR ('_')

#define FILE_POSTFIX_DICT ("dict")
#define FILE_POSTFIX_AFFIX ("affix")
#define FILE_POSTFIX_STOP ("stop")
#define FILE_POSTFIX_SYN ("syn")
#define FILE_POSTFIX_THS ("ths")

#define skipLoad(ddlsql) ((ddlsql) && IsConnFromCoord())

/*
 * Parser's framework
 */

/*
 * returning type for prslextype method of parser
 */
typedef struct {
    int lexid;
    char* alias;
    char* descr;
} LexDescr;

/*
 * Interface to headline generator
 */
typedef struct {
    uint32 selected : 1, in : 1, replace : 1, repeated : 1, skip : 1, unused : 3, type : 8, len : 16;
    char* word;
    QueryOperand* item;
} HeadlineWordEntry;

typedef struct {
    HeadlineWordEntry* words;
    int4 lenwords;
    int4 curwords;
    char* startsel;
    char* stopsel;
    char* fragdelim;
    int2 startsellen;
    int2 stopsellen;
    int2 fragdelimlen;
} HeadlineParsedText;

/*
 * Common useful things for tsearch subsystem
 */
extern char* get_tsearch_config_filename(const char* basename, char* pathname, const char* extension, bool newfile);
extern char* get_tsfile_prefix_internal();
extern List* get_tsfile_postfix(List* filenames, char sep);
extern List* copy_tsfile_to_local(List* filenames, List* postfixes, const char* dictprefix);
extern void copy_tsfile_to_remote(List* filenames, List* postfixes);
extern void copy_tsfile_to_backup(List* filenames);
extern void delete_tsfile_internal(const char* dictprefix, Oid tmplId);
extern bool inCluster();

/*
 * Often useful stopword list management
 */
typedef struct {
    int len;
    char** stop;
} StopList;

extern void readstoplist(const char* fname, StopList* s, char* (*wordop)(const char*));
extern bool searchstoplist(StopList* s, const char* key);
extern void ts_check_feature_disable();

/*
 * Interface with dictionaries
 */

/* return struct for any lexize function */
typedef struct {
    /* ----------
     * Number of current variant of split word.  For example the Norwegian
     * word 'fotballklubber' has two variants to split: ( fotball, klubb )
     * and ( fot, ball, klubb ). So, dictionary should return:
     *
     * nvariant    lexeme
     *	   1	   fotball
     *	   1	   klubb
     *	   2	   fot
     *	   2	   ball
     *	   2	   klubb
     *
     * In general, a TSLexeme will be considered to belong to the same split
     * variant as the previous one if they have the same nvariant value.
     * The exact values don't matter, only changes from one lexeme to next.
     * ----------
     */
    uint16 nvariant;

    uint16 flags; /* See flag bits below */

    char* lexeme; /* C string */
} TSLexeme;

/* Flag bits that can appear in TSLexeme.flags */
#define TSL_ADDPOS 0x01
#define TSL_PREFIX 0x02
#define TSL_FILTER 0x04

/*
 * Struct for supporting complex dictionaries like thesaurus.
 * 4th argument for dictlexize method is a pointer to this
 */
typedef struct {
    bool isend;            /* in: marks for lexize_info about text end is
                            * reached */
    bool getnext;          /* out: dict wants next lexeme */
    void* private_state;   /* internal dict state between calls with
                            * getnext == true */
} DictSubState;

/*
 * generic superclass for configuration options nodes, and the only one filed
 * meas the length of the configuration option struncate
 */
typedef struct ParserCfOpts {
    int32 vl_len_; /* varlena header (do not touch directly!) */
} ParserCfOpts;

/*
 * configuration option struncate specail for ngram parser
 * see congruent relationship in function tsearch_config_reloptions
 */
typedef struct NgramCfOpts {
    int32 vl_len_;
    int gram_size;
    bool punctuation_ignore;
    bool grapsymbol_ignore;
} NgramCfOpts;

/*
 * configuration option struncate specail for ngram parser
 * see congruent relationship in function tsearch_config_reloptions
 */
typedef struct PoundCfOpts {
    int32 vl_len_;
    char* split_flag;
} PoundCfOpts;

/*
 * configuration option struncate specail for Zhparser parser
 * see congruent relationship in function tsearch_config_reloptions
 */
typedef struct ZhparserCfOpts {
    int32 vl_len_;
    bool punctuation_ignore;
    bool seg_with_duality;
    bool multi_short;
    bool multi_duality;
    bool multi_zmain;
    bool multi_zall;
} ZhparserCfOpts;

#endif /* _PG_TS_PUBLIC_H_ */
