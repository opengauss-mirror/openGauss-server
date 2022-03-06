/* -------------------------------------------------------------------------
 *
 * spiDbesql.h
 *				Server Programming Interface public declarations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/spiDbesql.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SPIDBESQL_H
#define SPIDBESQL_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/resowner.h"
#include "utils/portal.h"

typedef struct SPIDescColumns {
    AttrNumber resno;      /* attribute number */
    char* resname;         /* name of the column */
    Oid resorigtbl;        /* OID of column's source table */
} SPIDescColumns;

extern void SpiDescribeColumnsCallback(CommandDest dest, const char *src, ArrayType** resDescribe,
    MemoryContext memctx, ParserSetupHook parserSetup = NULL, void *parserSetupArg = NULL);
extern void spi_exec_bind_with_callback(CommandDest dest, const char *src, bool read_only, long tcount,
    bool direct_call, void (*callbackFn)(void *), void *clientData, int nargs, Oid *argtypes, Datum *Values,
    ParserSetupHook parserSetup, void *parserSetupArg, const char *nulls = NULL);
#endif
