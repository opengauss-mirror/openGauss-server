/* ---------------------------------------------------------------------------------------
 * 
 * metainformation.h
 *        support interface for pushdown metainformation.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/foreign/metainformation.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GAUSSDB_FOREIGN_METAINFORMATION
#define GAUSSDB_FOREIGN_METAINFORMATION

#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "foreign/foreign.h"
#include "utils/hsearch.h"

ForeignOptions* setForeignOptions(Oid relid);
#endif