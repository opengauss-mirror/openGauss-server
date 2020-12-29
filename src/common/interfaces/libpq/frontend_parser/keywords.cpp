/* -------------------------------------------------------------------------
 *
 * keywords.c
 * 	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * 	  src/backend/parser/keywords.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "nodes/pg_list.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "datatypes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "catalog/pg_attribute.h"
#include "access/tupdesc.h"
#include "nodes/parsenodes_common.h"
#include "nodes/primnodes.h"
#include "gram.hpp"
#include "parser/keywords.h"

#define PG_KEYWORD(a, b, c) { a, b, c },


const ScanKeyword ScanKeywords[] = {
#include "parser/kwlist.h"
};

const int NumScanKeywords = lengthof(ScanKeywords);

bool check_length() 
{
    return NumScanKeywords >= 0;
}
