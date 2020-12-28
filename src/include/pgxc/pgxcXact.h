/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * pgxcXact.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/pgxcXact.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef PGXC_TRANS_H
#define PGXC_TRANS_H

#include "c.h"
#include "pgxc/pgxcnode.h"

extern int pgxc_node_begin(int conn_count, PGXCNodeHandle** connections,
                        GlobalTransactionId gxid, bool need_tran_block,
                        bool readOnly, char node_type, bool need_send_queryid = false);
extern bool pgxc_node_remote_prepare(const char* prepareGID, bool WriteCnLocalNode);
extern void pgxc_node_remote_commit(bool barrierLockHeld);
extern int pgxc_node_remote_abort(void);
extern int pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle** connections,
                    struct timeval* timeout, RemoteQueryState* combiner, bool checkerror = true);
extern List* GetWriterHandles();
extern List* GetReaderHandles();
extern void PreCommit_Remote(char* prepareGID, bool barrierLockHeld);
extern void SubXactCancel_Remote(void);
extern char* PrePrepare_Remote(const char* prepareGID, bool implicit, bool WriteCnLocalNode);
extern void PostPrepare_Remote(char* prepareGID, char* nodestring, bool implicit);
extern bool PreAbort_Remote(bool PerfectRollback);
extern void AtEOXact_Remote(void);
extern bool IsTwoPhaseCommitRequired(bool localWrite);
extern void reset_remote_handle_xid(void);
#endif