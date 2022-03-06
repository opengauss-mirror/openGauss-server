/* ---------------------------------------------------------------------------------------
 * 
 * logicalfuncs.h
 *         openGauss WAL to logical transformation support functions
 * 
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/logicalfuncs.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LOGICALFUNCS_H
#define LOGICALFUNCS_H

#include "replication/logical.h"
#include "libgen.h"


extern int logical_read_local_xlog_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
    XLogRecPtr targetRecPtr, char* cur_page, TimeLineID* pageTLI, char* xlog_path = NULL);

extern bool AssignLsn(XLogRecPtr* lsn_ptr, const char* input);
extern Datum pg_logical_slot_get_changes(PG_FUNCTION_ARGS);
extern Datum pg_logical_slot_get_binary_changes(PG_FUNCTION_ARGS);
extern Datum pg_logical_slot_peek_changes(PG_FUNCTION_ARGS);
extern Datum pg_logical_slot_peek_binary_changes(PG_FUNCTION_ARGS);
extern Datum pg_logical_peek_changes(PG_FUNCTION_ARGS);
extern Datum gs_write_term_log(PG_FUNCTION_ARGS);
extern void write_term_log(uint32 term);
extern void check_permissions(bool for_backup = false);
extern void CheckLogicalPremissions(Oid userId);
extern Datum pg_logical_get_area_changes(PG_FUNCTION_ARGS);


#endif
