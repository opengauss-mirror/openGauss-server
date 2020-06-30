/* ---------------------------------------------------------------------------------------
 * 
 * regioninfo.h
 *        support the obs foreign table, we get the region info from
 *        the region_map file, and clean the region info in database.
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * 
 * IDENTIFICATION
 *        src/include/foreign/regioninfo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GAUSSDB_FOREIGN_REGIION_INFO
#define GAUSSDB_FOREIGN_REGIION_INFO

#include "postgres.h"
#include "knl/knl_variable.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"

char* readDataFromJsonFile(char* region);

#endif
