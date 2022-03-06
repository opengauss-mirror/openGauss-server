/* -------------------------------------------------------------------------
 *
 * pg_proc_fn.h
 *     prototypes for functions in catalog/pg_proc.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc_fn.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_PROC_FN_H
#define PG_PROC_FN_H

#include "nodes/pg_list.h"

extern Oid ProcedureCreate(const char *procedureName,
                Oid procNamespace,
                Oid propackageid,
                bool isOraStyle,
                bool replace,
                bool returnsSet,
                Oid returnType,
                Oid proowner,
                Oid languageObjectId,
                Oid languageValidator,
                const char *prosrc,
                const char *probin,
				bool isAgg,
				bool isWindowFunc,
                bool security_definer,
                bool isLeakProof,
                bool isStrict,
                char volatility,
                oidvector *parameterTypes,
                Datum allParameterTypes,
                Datum parameterModes,
                Datum parameterNames,
                List *parameterDefaults,
                Datum proconfig,
                float4 procost,
                float4 prorows,
                int2vector *prodefaultargpos,
                bool  fenced,
                bool  shippable,
                bool  package,
                bool  proIsProcedure,
                const char *proargsrc,
                bool  isPrivate = false);

extern bool function_parse_error_transpose(const char *prosrc);

extern bool isSameParameterList(List* parameterList1, List* parameterList2);
extern char* getFuncName(List* funcNameList);

#ifndef ENABLE_MULTIPLE_NODES
extern char* ConvertArgModesToString(Datum proArgModes);
extern bool IsProArgModesEqual(Datum argModes1, Datum argModes2);
extern bool IsProArgModesEqualByTuple(HeapTuple tup, TupleDesc desc, oidvector* argModes);
extern oidvector* ConvertArgModesToMd5Vector(Datum proArgModes);
extern oidvector* MergeOidVector(oidvector* allArgTypes, oidvector* argModes);
#endif

extern oidvector* MakeMd5HashOids(oidvector* paramterTypes);

extern oidvector* ProcedureGetArgTypes(HeapTuple tuple);

extern Datum ProcedureGetAllArgTypes(HeapTuple tuple, bool* isNull);
#endif   /* PG_PROC_FN_H */

