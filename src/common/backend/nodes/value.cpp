/* -------------------------------------------------------------------------
 *
 * value.cpp
 *	  implementation of Value nodes
 *
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/value.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef FRONTEND_PARSER
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/parsenodes.h"
#else
#include "postgres_fe.h"
#include "datatypes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "catalog/pg_attribute.h"
#include "access/tupdesc.h"
#include "nodes/parsenodes_common.h"
#endif // FRONTEND_PARSER
/*
 *	makeInteger
 */
Value* makeInteger(long i)
{
    Value* v = makeNode(Value);

    v->type = T_Integer;
    v->val.ival = i;
    return v;
}

/*
 *	makeFloat
 *
 * Caller is responsible for passing a palloc'd string.
 */
Value* makeFloat(char* numericStr)
{
    Value* v = makeNode(Value);

    v->type = T_Float;
    v->val.str = numericStr;
    return v;
}

/*
 *	makeString
 *
 * Caller is responsible for passing a palloc'd string.
 */
Value* makeString(char* str)
{
    Value* v = makeNode(Value);

    v->type = T_String;
    v->val.str = str;
    return v;
}

/*
 *	makeBitString
 *
 * Caller is responsible for passing a palloc'd string.
 */
Value* makeBitString(char* str)
{
    Value* v = makeNode(Value);

    v->type = T_BitString;
    v->val.str = str;
    return v;
}
