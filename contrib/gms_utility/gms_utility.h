/*---------------------------------------------------------------------------------------*
 * gms_utility.h
 *
 *  Definition about gms_utility package.
 *
 * IDENTIFICATION
 *        contrib/gms_utility/gms_utility.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_UTILITY_H
#define GMS_UTILITY_H

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/pg_object.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_synonym.h"
#include "parser/parse_relation.h"

typedef enum {
    METHOD_OPT_TABLE,
    METHOD_OPT_ALL_COLUMN,
    METHOD_OPT_ALL_INDEX
} AnalyzeMethodOpt;

typedef struct AnalyzeVar {
    bool    isEstimate;
    bool    validRows;
    int64   estimateRows;
    bool    validPercent;
    int64   estimatePercent;
} AnalyzeVar;

typedef struct TokenizeVar {
    List*   list;
    char*   dblink;
    int     nextpos;
} TokenizeVar;

typedef struct NameResolveVar {
    char* schema;
    char* part1;
    char* part2;
    int part1Type;
    Oid objectId;
    int len;
    bool synonym;
} NameResolveVar;

/*
 * name resolve function input param context valid value: 0~9
 */
typedef enum {
    NR_CONTEXT_TABLE,
    NR_CONTEXT_PLSQL,
    NR_CONTEXT_SEQUENCES,
    NR_CONTEXT_TRIGGER,
    NR_CONTEXT_JAVA_SOURCE,
    NR_CONTEXT_JAVA_RESOURCE,
    NR_CONTEXT_JAVA_CLASS,
    NR_CONTEXT_TYPE,
    NR_CONTEXT_JAVA_SHARED_DATA,
    NR_CONTEXT_INDEX,
    NR_CONTEXT_UNKNOWN
} NameResolveContext;

/*
 * name resolve function output param part1_type valid value: 
 *      0、1、2、5、6、7、8、9、12、13
 */
#define NAME_RESOLVE_TYPE_NONE 0
#define NAME_RESOLVE_TYPE_INDEX 1
#define NAME_RESOLVE_TYPE_TABLE 2
#define NAME_RESOLVE_TYPE_SYNONYM 5
#define NAME_RESOLVE_TYPE_SEQUENCE 6
#define NAME_RESOLVE_TYPE_PROCEDURE 7
#define NAME_RESOLVE_TYPE_FUNCTION 8
#define NAME_RESOLVE_TYPE_PACKAGE 9
#define NAME_RESOLVE_TYPE_TRIGGER 12
#define NAME_RESOLVE_TYPE_TYPE 13

/*
 * name parse double quote marks
 */
#define QUOTE_NONE         0x01
#define QUOTE_STARTED      0x02
#define QUOTE_ENDED        0x04

#define QUOTE_STATE(val, bits) (((val) & (bits)) == (val))
#define IS_QUOTE_STARTED(val) QUOTE_STATE(val, QUOTE_STARTED)
#define BEFORE_QUOTE_STARTED(val) QUOTE_STATE(val, QUOTE_NONE)
#define IS_QUOTE_END(val) QUOTE_STATE(val, QUOTE_ENDED)

#define NAME_TOKENIZE_MAX_ITEM_COUNT 3

extern "C" Datum gms_analyze_schema(PG_FUNCTION_ARGS);
extern "C" Datum gms_canonicalize(PG_FUNCTION_ARGS);
extern "C" Datum gms_compile_schema(PG_FUNCTION_ARGS);
extern "C" Datum gms_expand_sql_text(PG_FUNCTION_ARGS);
extern "C" Datum gms_get_cpu_time(PG_FUNCTION_ARGS);
extern "C" Datum gms_get_endianness(PG_FUNCTION_ARGS);
extern "C" Datum gms_get_sql_hash(PG_FUNCTION_ARGS);
extern "C" Datum gms_name_tokenize(PG_FUNCTION_ARGS);
extern "C" Datum gms_name_resolve(PG_FUNCTION_ARGS);
extern "C" Datum gms_is_bit_set(PG_FUNCTION_ARGS);
extern "C" Datum gms_old_current_schema(PG_FUNCTION_ARGS);

extern void RecompileSingleFunction(Oid func_oid, bool is_procedure);
extern void RecompileSinglePackage(Oid package_oid, bool is_spec);

#endif /* GMS_UTILITY_H */