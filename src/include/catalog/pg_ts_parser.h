/* -------------------------------------------------------------------------
 *
 * pg_ts_parser.h
 *    definition of parsers for tsearch
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_parser.h
 *
 * NOTES
 *        the genbki.pl script reads this file and generates .bki
 *        information from the DATA() statements.
 *
 *        XXX do NOT break up DATA() statements into multiple lines!
 *            the scripts are not as smart as you might think...
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_TS_PARSER_H
#define PG_TS_PARSER_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_ts_parser definition.  cpp turns this into
 *        typedef struct FormData_pg_ts_parser
 * ----------------
 */
#define TSParserRelationId    3601
#define TSParserRelation_Rowtype_Id 11640

CATALOG(pg_ts_parser,3601) BKI_SCHEMA_MACRO
{
    NameData       prsname;         /* parser's name */
    Oid            prsnamespace;    /* name space */
    regproc        prsstart;        /* init parsing session */
    regproc        prstoken;        /* return next token */
    regproc        prsend;          /* finalize parsing session */
    regproc        prsheadline;     /* return data for headline creation */
    regproc        prslextype;      /* return descriptions of lexeme's types */
} FormData_pg_ts_parser;

typedef FormData_pg_ts_parser *Form_pg_ts_parser;

/* ----------------
 *        compiler constants for pg_ts_parser
 * ----------------
 */
#define Natts_pg_ts_parser                    7
#define Anum_pg_ts_parser_prsname             1
#define Anum_pg_ts_parser_prsnamespace        2
#define Anum_pg_ts_parser_prsstart            3
#define Anum_pg_ts_parser_prstoken            4
#define Anum_pg_ts_parser_prsend              5
#define Anum_pg_ts_parser_prsheadline         6
#define Anum_pg_ts_parser_prslextype          7

/* ----------------
 * initial contents of pg_ts_parser
 * ----------------
 */

DATA(insert OID = 3722 ( "default" PGNSP prsd_start prsd_nexttoken prsd_end prsd_headline prsd_lextype ));
DESCR("default word parser");

DATA(insert OID = 3789 ( "ngram" PGNSP ngram_start ngram_nexttoken ngram_end prsd_headline ngram_lextype ));
DESCR("ngram chinese word parser");
#define NGRAM_PARSER 3789

DATA(insert OID = 3790 ( "zhparser" PGNSP zhprs_start zhprs_getlexeme zhprs_end prsd_headline zhprs_lextype ));
DESCR("zhparser chinese word parser");
#define ZHPARSER_PARSER 3790

DATA(insert OID = 3801 ( "pound" PGNSP pound_start pound_nexttoken pound_end prsd_headline pound_lextype ));
DESCR(" pound split parser");
#define POUND_PARSER 3801

#endif   /* PG_TS_PARSER_H */

