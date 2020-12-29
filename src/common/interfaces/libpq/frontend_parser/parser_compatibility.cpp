/* -------------------------------------------------------------------------
 *
 * parse_compatibility.cpp
 * handle parsing syntax compatible module
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/backend/parser/parse_compatibility.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <string.h>

THR_LOCAL bool stmt_contains_operator_plus = false;

bool getOperatorPlusFlag()
{
    return stmt_contains_operator_plus;
}

void resetOperatorPlusFlag()
{
    stmt_contains_operator_plus = false;
    return;
}
