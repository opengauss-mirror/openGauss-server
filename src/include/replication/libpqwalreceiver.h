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
 * ---------------------------------------------------------------------------------------
 * 
 * libpqwalreceiver.h
 *        libpqwalreceiver init for WalreceiverMain.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/libpqwalreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LIBPQWALRECEIVER_H
#define LIBPQWALRECEIVER_H

#include "utils/tuplestore.h"

typedef struct LibpqrcvConnectParam {
    char* conninfo;
    XLogRecPtr startpoint;
    char* slotname;
    int channel_identifier;

    /* for logical replication slot */
    bool logical;
    uint32 protoVersion;    /* Logical protocol version */
    List *publicationNames; /* String list of publications */
    bool binary;            /* Ask publisher to use binary */
    bool useSnapshot;       /* Use snapshot or not */
}LibpqrcvConnectParam;

/*
 * Status of walreceiver query execution.
 *
 * We only define statuses that are currently used.
 */
typedef enum {
    WALRCV_ERROR,        /* There was error when executing the query. */
    WALRCV_OK_COMMAND,   /* Query executed utility or replication command. */
    WALRCV_OK_TUPLES,    /* Query returned tuples. */
    WALRCV_OK_COPY_IN,   /* Query started COPY FROM. */
    WALRCV_OK_COPY_OUT,  /* Query started COPY TO. */
    WALRCV_OK_COPY_BOTH, /* Query started COPY BOTH replication protocol. */
} WalRcvExecStatus;

/*
 * Return value for walrcv_query, returns the status of the execution and
 * tuples if any.
 */
typedef struct WalRcvExecResult {
    WalRcvExecStatus status;
    int sqlstate;
    char *err;
    Tuplestorestate *tuplestore;
    TupleDesc tupledesc;
} WalRcvExecResult;

extern int32 pg_atoi(char* s, int size, int c, bool can_ignore = false);
extern int32 pg_strtoint32(const char* s, bool can_ignore = false);
/* Prototypes for interface functions */
extern bool libpqrcv_connect_for_TLI(TimeLineID* timeLineID, char* conninfo);
extern bool libpqrcv_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier);
extern bool libpqrcv_receive(int timeout, unsigned char* type, char** buffer, int* len);
extern void libpqrcv_send(const char* buffer, int nbytes);
extern void libpqrcv_disconnect(void);
extern void HaSetRebuildRepInfoError(HaRebuildReason reason);
extern void SetObsRebuildReason(HaRebuildReason reason);
extern void libpqrcv_check_conninfo(const char *conninfo);
extern WalRcvExecResult* libpqrcv_exec(const char *cmd, const int nRetTypes, const Oid *retTypes);

extern void IdentifyRemoteSystem(bool checkRemote);
extern void CreateRemoteReplicationSlot(XLogRecPtr startpoint, const char* slotname, bool isLogical, XLogRecPtr *lsn,
                                        bool useSnapshot = false, CommitSeqNo *csn = NULL);
extern void StartRemoteStreaming(const LibpqrcvConnectParam *options);
extern ServerMode IdentifyRemoteMode();

#endif
