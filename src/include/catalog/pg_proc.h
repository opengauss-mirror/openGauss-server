/*
------------------------------------------------------------------------
 *
 * pg_proc.h
 *      definition of the system "procedure" relation (pg_proc)
 *      along with the relation's initial contents.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc.h
 *
 * NOTES
 *      The script catalog/genbki.pl reads this file and generates .bki
 *      information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *      generates fmgroids.h and fmgrtab.c the same way.
 *
 *      XXX do NOT break up DATA() statements into multiple lines!
 *          the scripts are not as smart as you might think...
 *      XXX (eg. #if 0 #endif won't do what you think)
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_PROC_H
#define PG_PROC_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_proc definition.  cpp turns this into
 *        typedef struct FormData_pg_proc
 * ----------------
 */
#define ProcedureRelationId  1255
#define ProcedureRelation_Rowtype_Id  81

#define IsProcRelation(rel) (RelationGetRelid(rel) == ProcedureRelationId)
#define IsProcCache(cache) ((cache)->cc_reloid == ProcedureRelationId)

CATALOG(pg_proc,1255) BKI_BOOTSTRAP BKI_ROWTYPE_OID(81) BKI_SCHEMA_MACRO
{
    NameData    proname;         /* procedure name */
    Oid         pronamespace;    /* OID of namespace containing this proc */
    Oid         proowner;        /* procedure owner */
    Oid         prolang;         /* OID of pg_language entry */
    float4      procost;         /* estimated execution cost */
    float4      prorows;         /* estimated # of rows out (if proretset) */
    Oid         provariadic;     /* element type of variadic array, or 0 */
    regproc     protransform;    /* transforms calls to it during planning */
	bool		proisagg;		/* is it an aggregate? */
	bool		proiswindow;	/* is it a window function? */
    bool        prosecdef;       /* security definer */
    bool        proleakproof;    /* is it a leak-proof function? */
    bool        proisstrict;     /* strict with respect to NULLs? */
    bool        proretset;       /* returns a set? */
    char        provolatile;     /* see PROVOLATILE_ categories below */
    int2        pronargs;        /* number of arguments */
    int2        pronargdefaults; /* number of arguments with defaults */
    Oid         prorettype;      /* OID of result type */

    /*
     * variable-length fields start here, but we allow direct access to
     * proargtypes
     */
    oidvector    proargtypes;    /* parameter types (excludes OUT params) */

#ifdef CATALOG_VARLEN
    Oid         proallargtypes[1];        /* all param types (NULL if IN only) */
    char        proargmodes[1];           /* parameter modes (NULL if IN only) */
    text        proargnames[1];           /* parameter names (NULL if no names) */
    pg_node_tree proargdefaults;          /* list of expression trees for argument
                                           * defaults (NULL if none) */
    text        prosrc;                   /* procedure source text */
    text        probin;                   /* secondary procedure info (can be NULL) */
    text        proconfig[1];             /* procedure-local GUC settings */
    aclitem     proacl[1];                /* access permissions */
    int2vector  prodefaultargpos;
    bool        fencedmode;
    bool        proshippable;    /* if provolatile is not 'i', proshippable will determine if the func can be shipped */
    bool        propackage;
    char        prokind;         /* see PROKIND_ categories below */
#endif
} FormData_pg_proc;

/* ----------------
 *        Form_pg_proc corresponds to a pointer to a tuple with
 *        the format of pg_proc relation.
 * ----------------
 */
typedef FormData_pg_proc *Form_pg_proc;

/* ----------------
 *        compiler constants for pg_proc
 * ----------------
 */
#define Natts_pg_proc 32
#define Anum_pg_proc_proname 1
#define Anum_pg_proc_pronamespace 2
#define Anum_pg_proc_proowner 3
#define Anum_pg_proc_prolang 4
#define Anum_pg_proc_procost 5
#define Anum_pg_proc_prorows 6
#define Anum_pg_proc_provariadic 7
#define Anum_pg_proc_protransform 8
#define Anum_pg_proc_proisagg 9
#define Anum_pg_proc_proiswindow 10
#define Anum_pg_proc_prosecdef 11
#define Anum_pg_proc_proleakproof 12
#define Anum_pg_proc_proisstrict 13
#define Anum_pg_proc_proretset 14
#define Anum_pg_proc_provolatile 15
#define Anum_pg_proc_pronargs 16
#define Anum_pg_proc_pronargdefaults 17
#define Anum_pg_proc_prorettype 18
#define Anum_pg_proc_proargtypes 19
#define Anum_pg_proc_proallargtypes 20
#define Anum_pg_proc_proargmodes 21
#define Anum_pg_proc_proargnames 22
#define Anum_pg_proc_proargdefaults 23
#define Anum_pg_proc_prosrc 24
#define Anum_pg_proc_probin 25
#define Anum_pg_proc_proconfig 26
#define Anum_pg_proc_proacl 27
#define Anum_pg_proc_prodefaultargpos 28
#define Anum_pg_proc_fenced 29
#define Anum_pg_proc_shippable 30
#define Anum_pg_proc_package 31
#define Anum_pg_proc_prokind 32

/* proc_oid is only for builitin
 * func view shouldn't be included in Natts_pg_proc
 */
#define Anum_pg_proc_oid 33

/* ----------------
 *        initial contents of pg_proc
 * ----------------
 */

/*
 * Note: every entry in pg_proc.h is expected to have a DESCR() comment,
 * except for functions that implement pg_operator.h operators and don't
 * have a good reason to be called directly rather than via the operator.
 * (If you do expect such a function to be used directly, you should
 * duplicate the operator's comment.)  initdb will supply suitable default
 * comments for functions referenced by pg_operator.
 *
 * Try to follow the style of existing functions' comments.
 * Some recommended conventions:
 *        "I/O" for typinput, typoutput, typreceive, typsend functions
 *        "I/O typmod" for typmodin, typmodout functions
 *        "aggregate transition function" for aggtransfn functions, unless
 *         they are reasonably useful in their own right
 *        "aggregate final function" for aggfinalfn functions (likewise)
 *        "convert srctypename to desttypename" for cast functions
 *        "less-equal-greater" for B-tree comparison functions
 */

/* keep the following ordered by OID so that later changes can be made easier */
     /* OIDS 1 - 99 */
#define TEXTLENOID 1257
#define EQSELRETURNOID 101
#define NEQSELRETURNOID 102
#define ABSTIMEINFUNCOID 240
#define ABSTIMEOUTFUNCOID 241
#define RELTIMEINFUNCOID 242
#define RELTIMEOUTFUNCOID 243
#define TINTERVALINFUNCOID 246
#define TINTERVALOUTFUNCOID 247
#define TIMENOWFUNCOID 250
#define INT4TOFLOAT8FUNCOID 316
#define BTINT4CMP_OID 351
#define RTRIM1FUNCOID 401
#define NAME2TEXTFUNCOID 406
#define HASHINT4OID 450
#define HASHINT8OID 949
#define HASHTEXTOID 400
#define GETPGUSERNAMEFUNCOID 710
#define CURRENTUSERFUNCOID 745
#define SESSIONUSERFUNCOID 746
#define ARRAYTOSTRINGFUNCOID 395
#define ARRAYTOSTRINGNULLFUNCOID 384
#define ARRAYAGGFUNCOID 2335
#define POSITIONFUNCOID 849
#define CURRENTDATABASEFUNCOID 861
#define CASHINFUNCOID 886
#define CASHOUTFUNCOID 887
#define CASH2NUMERICFUNCOID 3823
#define NUMERIC2CASHFUNCOID 3824
#define INTEGER2CASHFUNCOID 3811
#define BIGINT2CASHFUNCOID 3812
#define HASHBPCHAROID 1080
#define DATEINFUNCOID 1084
#define DATEOUTFUNCOID 1085
#define TIMEINFUNCOID 1143
#define TIMEOUTFUNCOID 1144
#define TIMESTAMPTZINFUNCOID 1150
#define TIMESTAMPTZOUTFUNCOID 1151
#define INTERVALINFUNCOID 1160
#define TIMESTAMPTZPARTFUNCOID 1171
#define INTERVALPARTFUNCOID 1172
#define DATETIMESTAMPTZFUNCOID 1174
#define DTAETIME2TIMESTAMPTZFUNCOID 1176
#define TIMESTAMPTZ2DATEFUNCOID 1178
#define ABSTIME2DATEFUNCOID 1179
#define TIMESTAMPTZPLINTERVALFUNCOID 1189
#define TIMESTAMPTZMIINTERVALFUNCOID 1190
#define TIMESTAMPTZTRUNCFUNCOID 1217
#define INTERVALTRUNCFUNCOID 1218
#define TIMEZPARTFUNCOID 1273
#define NOWFUNCOID 1299
#define TRANSACTIONTIMESTAMPFUNCOID 2647
#define STATEMENTTIMESTAMPFUNCOID 2648
#define PGSYSTIMESTAMPFUNCOID 3951
#define OVERLAPSV1FUNCOID 1305
#define OVERLAPSV2FUNCOID 1306
#define OVERLAPSV3FUNCOID 1307
#define TIMESTAMPINFUNCOID 1312
#define TIMESTAMPOUTFUNCOID 1313
#define LENGTHFUNCOID 1317
#define BPLENFUNCOID 1318
#define TEXTOCTLENFUNCOID 3175
#define BPOCTLENFUNCOID 3176
#define SQRTFUNCOID 1344
#define POWFUNCOID 1346
#define POWERFUNCOID 1368
#define TIMETZINFUNCOID 1350
#define ABSTIME2TIMEFUNCOID 1364
#define CHARLENFUNCOID 1381
#define DATEPARTFROMABSTIMEFUNCOID 1382
#define DATEPARTFROMRELTIMEFUNCOID 1383
#define DATEPARTFUNCOID 1384
#define TIMEPARTFUNCOID 1385
#define AGEDATETOTIMETZFUNCOID 1386
#define TIMESTAMPTZ2TIMETZFUNCOID 1388
#define CURRENTSCHEMAFUNCOID 1402
#define NEXTVALFUNCOID 1574
#define CURRVALFUNCOID 1575
#define SETVAL1FUNCOID 1576
#define SETVAL3FUNCOID 1765
#define INT4NUMERICFUNCOID 1740
#define INT8NUMERICFUNCOID 1781
#define VARCHARINT8FUNCOID 4176
#define RANDOMFUNCOID 1598
#define ECEXTENSIONFUNCOID 4244
#define ECHADOOPFUNCOID 4255
#define ARCSINEFUNCOID 1600
#define SINEFUNCOID 1604
#define COSINEFUNCOID 1605
#define STRPOSFUNCOID 868
#define LPADFUNCOID 873
#define RPADFUNCOID 874
#define LTRIMFUNCOID 875
#define RTRIMFUNCOID 876
#define SUBSTRFUNCOID 877
#define SUBSTRBWITHLENFUNCOID 3205
#define SUBSTRBNOLENFUNCOID 3206
#define LPADPARAFUNCOID 879
#define RPADPARAFUNCOID 880
#define LTRIMPARAFUNCOID 881
#define RTRIMPARAFUNCOID 882
#define SUBSTRNOLENFUNCOID 883
#define BTRIMPARAFUNCOID 884
#define BTRIMFUNCOID 885
#define SUBSTRINNFUNCOID 3182
#define SUBSTRINNNOLENFUNCOID 3183
#define TEXTSUBSTRINGFUNCOID 936
#define TEXTSUBSTRINGNOLENFUNCOID 937
#define PGCLIENTENCODINGFUNCOID 810
#define CONVERTFROMFUNCOID 1714
#define CONVERTTOFUNCOID 1717
#define CONVERTFUNCOID 1813
#define PGCHARTOENCODINGFUNCOID 1264
#define PGENCODINGTOCHARFUNCOID 1597
#define PGTYPEOFFUNCOID 1619
#define BITSUBSTRINGFUNOID 1680
#define BITPOSITIONFUNCOID 1698
#define BITSUBSTRINGNOLENFUNCOID 1699
#define NUMTOFLOAT8FUNCOID 1746
#define TIMESTAMPTZ2CHARFUNCOID 1770
#define NUMERIC2CHARFUNCOID 1772
#define INTEGER2CHARFUNCOID 1773
#define BIGINT2CHARFUNCOID 1774
#define FLOAT4TOCHARFUNCOID 1775
#define FLOAT8TOCHARFUNCOID 1776
#define TONUMBERFUNCOID 1777
#define TOTIMESTAMPFUNCOID 1778
#define TOTIMESTAMPDEFAULTFUNCOID 3207
#define TODATEFUNCOID 1780
#define INTERVALTOCHARFUNCOID 1768
#define CONCATFUNCOID 3058
#define CONCATWSFUNCOID 3059
#define ANYTOTEXTFORMATFUNCOID 3539
#define DEFAULTFORMATFUNCOID 3540
#define INTERVALACCUMFUNCOID 1843
#define INTERVALAVGFUNCOID 1844
#define STRINGAGGTRANSFNFUNCOID 3535
#define STRINGAGGFUNCOID 3538
#define BYTEASTRINGAGGFUNCOID 3545
#define LISTAGGFUNCOID 3552
#define LISTAGGNOARG2FUNCOID 3554
#define INT2LISTAGGFUNCOID 3556
#define INT2LISTAGGNOARG2FUNCOID 3558
#define INT4LISTAGGFUNCOID 3560
#define INT4LISTAGGNOARG2FUNCOID 3562
#define INT8LISTAGGFUNCOID 3564
#define INT8LISTAGGNOARG2FUNCOID 3566
#define FLOAT4LISTAGGFUNCOID 3568
#define FLOAT4LISTAGGNOARG2FUNCOID 3570
#define FLOAT8LISTAGGFUNCOID 3572
#define FLOAT8LISTAGGNOARG2FUNCOID 3574
#define NUMERICLISTAGGFUNCOID 3576
#define NUMERICLISTAGGNOARG2FUNCOID 3578
#define DATELISTAGGFUNCOID 3580
#define DATELISTAGGNOARG2FUNCOID 3582
#define TIMESTAMPLISTAGGFUNCOID 3584
#define TIMESTAMPLISTAGGNOARG2FUNCOID 3586
#define TIMESTAMPTZLISTAGGFUNCOID 3588
#define TIMESTAMPTZLISTAGGNOARG2FUNCOID 3590
#define INTERVALLISTAGGFUNCOID 4506
#define INTERVALLISTAGGNOARG2FUNCOID 4508
#define PGBACKENDPIDFUNCOID 2026
#define PGSTATGETBACKENDPIDFUNCOID 1937
#define TEXTANYCATFUNCOID 2003
#define ANYTEXTCATFUNCOID 2004
#define BYTEASUBSTRINGFUNCOID 2012
#define BYTEASUBSTRINGNOLENOID 2013
#define BYTEASUBSTRFUNCOID 2085
#define BYTEASUBSTRNOLENFUNCOID 2086
#define BYTEAPOSFUNCOID 2014
#define TIMESTAMPTZ2TIMEFUNCOID 2019
#define TIMESTAMPTRUNCFUNCOID 2020
#define TIMESTAMPPARTFUNCOID 2021
#define ABSTIMETIMESTAMPFUNCOID 2023
#define TIMESTAMPTZ2TIMESTAMPFUNCOID 2027
#define TIMESTAMP2TIMESTAMPTZFUNCOID 2028
#define TIMESTAMP2DATEOID 2029
#define TIMESTAMP2ABSTIMEFUNCOID 2030
#define TIMEZONETZFUNCOID 2037
#define TIMESTAMPHASHOID 2039
#define TIME2TIMETZFUNCOID 2047
#define TIMESTAMP2CHARFUNCOID 2049
#define DEFAULTFORMATTIMESTAMP2CHARFUNCOID 3808
#define DEFAULTFORMATTIMESTAMPTZ2CHARFUNCOID 3813
#define AGEDATETOTIMEFUNCOID 2059
#define TEXTREGEXSUBSTRINGOID 2073
#define SUBSTRINGESCAPEFUNCOID 2074
#define ANYCOUNTOID 2147
#define COUNTOID 2803
#define PGCOLUMNSIZEFUNCOID 1269
#define ANYARRAYINFUNCOID 2296
#define DATELTTIMESTAMPTZFUNCOID 2351
#define DATELETIMESTAMPTZFUNCOID 2352
#define DATEEQTIMESTAMPTZFUNCOID 2353
#define DATEGTTIMESTAMPTZFUNCOID 2354
#define DATEGETIMESTAMPTZFUNCOID 2355
#define DATENETIMESTAMPTZFUNCOID 2356
#define DATECMPTIMESTAMPTZFUNCOID 2357
#define TIMESTAMPTZLTDATEFUNCOID 2377
#define TIMESTAMPTZLEDATEFUNCOID 2378
#define TIMESTAMPTZEQDATEFUNCOID 2379
#define TIMESTAMPTZGTDATEFUNCOID 2380
#define TIMESTAMPTZGEDATEFUNCOID 2381
#define TIMESTAMPTZNEDATEFUNCOID 2382
#define TIMESTAMPTZCMPDATEFUNCOID 2383
#define TIMESTAMPLTTIMESTAMPTZFUNCOID 2520
#define TIMESTAMPLETIMESTAMPTZFUNCOID 2521
#define TIMESTAMPEQTIMESTAMPTZFUNCOID 2522
#define TIMESTAMPGTTIMESTAMPTZFUNCOID 2523
#define TIMESTAMPGETIMESTAMPTZFUNCOID 2524
#define TIMESTAMPNETIMESTAMPTZFUNCOID 2525
#define TIMESTAMPCMPTIMESTAMPTZFUNCOID 2526
#define TIMESTAMPTZLTTIMESTAMPFUNCOID 2527
#define TIMESTAMPTZLETIMESTAMPFUNCOID 2528
#define TIMESTAMPTZEQTIMESTAMPFUNCOID 2529
#define TIMESTAMPTZGTTIMESTAMPFUNCOID 2530
#define TIMESTAMPTZGETIMESTAMPFUNCOID 2531
#define TIMESTAMPTZNETIMESTAMPFUNCOID 2532
#define TIMESTAMPTZCMPTIMESTAMPFUNCOID 2533
#define GENERATESERIESFUNCOID 939
#define EVERYFUNCOID 2519
#define INTERVALPLTIMESTAMPTZFUNCOID 2549
#define LASTVALFUNCOID 2559
#define XMLAGGFUNCOID 2901
#define ROWNUMBERFUNCOID 3100
#define RANKFUNCOID 3101
#define DENSERANKFUNCOID 3102
#define INSTR2FUNCOID 3167
#define INSTR3FUNCOID 3168
#define INSTR4FUNCOID 3169
#define SMALLDATETIMEINFUNCOID 9004
#define SMALLDATETIMEOUTFUNCOID 9006
#define TODATEDEFAULTFUNCOID 5560
#define GSENCRYPTAES128FUNCOID 3464
#define TESTSKEWNESSRETURNTYPE 4048
#define PERCENTILECONTAGGFUNCOID 4452
#define MODEAGGFUNCOID 4461
#define PGCHECKAUTHIDFUNCOID 3228

/*
 * Symbolic values for prokind column
 */
#define PROKIND_FUNCTION    'f'
#define PROKIND_AGGREGATE   'a'
#define PROKIND_WINDOW      'w'
#define PROKIND_PROCEDURE   'p'

#define PROC_IS_FUNC(prokind)   ((prokind) == PROKIND_FUNCTION)
#define PROC_IS_AGG(prokind)    ((prokind) == PROKIND_AGGREGATE)
#define PROC_IS_WIN(prokind)    ((prokind) == PROKIND_WINDOW)
#define PROC_IS_PRO(prokind)    ((prokind) == PROKIND_PROCEDURE)

/*
 * Symbolic values for provolatile column: these indicate whether the result
 * of a function is dependent *only* on the values of its explicit arguments,
 * or can change due to outside factors (such as parameter variables or
 * table contents).  NOTE: functions having side-effects, such as setval(),
 * must be labeled volatile to ensure they will not get optimized away,
 * even if the actual return value is not changeable.
 */
#define PROVOLATILE_IMMUTABLE   'i'        /* never changes for given input */
#define PROVOLATILE_STABLE      's'        /* does not change within a scan */
#define PROVOLATILE_VOLATILE    'v'        /* can change even within a scan */

/*
 * Symbolic values for proargmodes column.    Note that these must agree with
 * the FunctionParameterMode enum in parsenodes.h; we declare them here to
 * be accessible from either header.
 */
#define PROARGMODE_IN       'i'
#define PROARGMODE_OUT      'o'
#define PROARGMODE_INOUT    'b'
#define PROARGMODE_VARIADIC 'v'
#define PROARGMODE_TABLE    't'

#define PROC_LIB_PATH           "$libdir/"
#define PORC_PLUGIN_LIB_PATH    "$libdir/pg_plugin/"
#define PORC_SRC_LIB_PATH       "$libdir/proc_srclib/"

#define OID_REGEXP_SPLIT_TO_TABLE 2765
#define OID_REGEXP_SPLIT_TO_TABLE_NO_FLAG 2766

#endif   /* PG_PROC_H */

