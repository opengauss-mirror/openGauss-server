/* ---------------------------------------------------------------------------------------
 * 
 * pl_debug.h
 *      Declarations for operations on log sequence numbers (LSNs) of
 *		PostgreSQL.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/utils/pl_debug.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PL_DEBUG_H
#define PL_DEBUG_H

#include "utils/plpgsql.h"

#define DEBUG_REASON_NONE 0 
#define DEBUG_REASON_INTERPRETER_STARTING 1 
#define DEBUG_REASON_BREAKPOINT 2 
#define DEBUG_REASON_ENTER 3 
#define DEBUG_REASON_RETURN 4 
#define DEBUG_REASON_FINISH 5 
#define DEBUG_REASON_LINE 6 
#define DEBUG_REASON_INTERRUPT 7 
#define DEBUG_REASON_EXCEPTION 8 
#define DEBUG_REASON_EXIT 9 
#define DEBUG_REASON_KNL_EXIT 10 
#define DEBUG_REASON_HANDLER 11 
#define DEBUG_REASON_TIMEOUT 12 
#define DEBUG_REASON_INSTANTIATE 13 
#define DEBUG_REASON_ABORT 14 

typedef struct {
    char* nodename;
    int port;
    Oid funcoid;
} DebuggerServerInfo;

extern void debug_server_rec_msg(DebugInfo* debug, char* firstChar);
extern void debug_server_send_msg(DebugInfo* debug, const char* msg, int msg_len);
extern void debug_client_rec_msg(DebugClientInfo* client);
extern void debug_client_send_msg(DebugClientInfo* client, char first_char, char* msg, int msg_len);
extern void init_pldebug_htcl();
extern bool AcquireDebugCommIdx(int commIdx);
extern void check_debugger_valid(int commidx);
extern void attach_session(int commidx, uint64 sid);
extern bool CheckPlpgsqlFunc(Oid funcoid, bool report_error = true);
extern DebugClientInfo* InitDebugClient(int comm_idx);
#endif /* PL_DEBUG_H */
