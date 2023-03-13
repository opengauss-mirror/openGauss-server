/* -------------------------------------------------------------------------
 *
 * replnodes.h
 *	  definitions for replication grammar parse nodes
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/replnodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef REPLNODES_H
#define REPLNODES_H

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"

typedef enum ReplicationKind { REPLICATION_KIND_PHYSICAL, REPLICATION_KIND_LOGICAL } ReplicationKind;

/* ----------------------
 *		IDENTIFY_SYSTEM command
 * ----------------------
 */
typedef struct IdentifySystemCmd {
    NodeTag type;
} IdentifySystemCmd;

typedef struct IdentifyVersionCmd {
    NodeTag type;
} IdentifyVersionCmd;

typedef struct IdentifyModeCmd {
    NodeTag type;
} IdentifyModeCmd;

typedef struct IdentifyMaxLsnCmd {
    NodeTag type;
} IdentifyMaxLsnCmd;

typedef struct IdentifyConsistenceCmd {
    NodeTag type;
    XLogRecPtr recordptr;
} IdentifyConsistenceCmd;

typedef struct IdentifyChannelCmd {
    NodeTag type;
    int channel_identifier;
} IdentifyChannelCmd;

typedef struct IdentifyAZCmd {
    NodeTag type;
} IdentifyAZCmd;

/* ----------------------
 *		BASE_BACKUP command
 * ----------------------
 */
typedef struct BaseBackupCmd {
    NodeTag type;
    List* options;
} BaseBackupCmd;

/* ----------------------
 *		CREATE_REPLICATION_SLOT command
 * ----------------------
 */
typedef struct CreateReplicationSlotCmd {
    NodeTag type;
    char* slotname;
    ReplicationKind kind;
    XLogRecPtr init_slot_lsn;
    char* plugin;
    bool useSnapshot;
} CreateReplicationSlotCmd;

/* ----------------------
 *		DROP_REPLICATION_SLOT command
 * ----------------------
 */
typedef struct DropReplicationSlotCmd {
    NodeTag type;
    char* slotname;
    bool wait;
} DropReplicationSlotCmd;

/* ----------------------
 *		START_REPLICATION command
 * ----------------------
 */
typedef struct StartReplicationCmd {
    NodeTag type;
    ReplicationKind kind;
    char* slotname;
    XLogRecPtr startpoint;
    List* options;
} StartReplicationCmd;

/* ----------------------
 *		ADVANCE_REPLICATION command
 * ----------------------
 */
typedef struct AdvanceReplicationCmd {
    NodeTag type;
    ReplicationKind kind;
    char* slotname;
    XLogRecPtr restart_lsn;
    XLogRecPtr confirmed_flush;
} AdvanceReplicationCmd;

/* ----------------------
 *		START_REPLICATION(DATA) command
 * ----------------------
 */
typedef struct StartDataReplicationCmd {
    NodeTag type;
} StartDataReplicationCmd;

/* ----------------------
 *		FETCH_MOT_CHECKPOINT command
 * ----------------------
 */
typedef struct FetchMotCheckpointCmd {
    NodeTag type;
} FetchMotCheckpointCmd;

/* ----------------------
 *		SQL commands
 * ----------------------
 */
typedef struct SQLCmd
{
    NodeTag type;
} SQLCmd;
/* ----------------------
 *		ADVANCE_CATALOG_XMIN command
 * ----------------------
 */
typedef struct AdvanceCatalogXminCmd {
    NodeTag type;
    char* slotname;
    TransactionId catalogXmin;
} AdvanceCatalogXminCmd;
#endif /* REPLNODES_H */
