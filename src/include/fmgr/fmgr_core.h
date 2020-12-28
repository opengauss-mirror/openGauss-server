/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2002-2007, PostgreSQL Global Development Group
 *
 * fmgr_core.h
 * core definition of fmgr
 *
 * src/include/fmgr/fmgr_core.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FMGRCORE_H
#define FMGRCORE_H

#if defined(FRONTEND_PARSER)
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif // FRONTEND_PARSER
#include "lib/stringinfo.h"

/* We don't want to include primnodes.h here, so make a stub reference */
typedef struct Node* fmNodePtr;
class ScalarVector;

/* Likewise, avoid including stringinfo.h here */
typedef struct StringInfoData* fmStringInfo;

struct FunctionCallInfoData;

struct Cursor_Data;

typedef Datum (*UDFArgsFuncType)(FunctionCallInfoData* fcinfo, int idx, Datum val);

/*
 * All functions that can be called directly by fmgr must have this signature.
 * (Other functions can be called by using a handler that does have this
 * signature.)
 */

typedef struct FunctionCallInfoData* FunctionCallInfo;

#ifndef FRONTEND_PARSER
typedef Datum (*PGFunction)(FunctionCallInfo fcinfo);
#endif /* !FRONTEND_PARSER */

#endif /* FMGRCORE_H */
