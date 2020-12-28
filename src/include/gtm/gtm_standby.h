/* -------------------------------------------------------------------------
 *
 * gtm_standby.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_standby.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GTM_STANDBY_H
#define GTM_STANDBY_H

#include "c.h"
#include "gtm/gtm_c.h"
#include "gtm/utils/libpq-fe.h"
#include "gtm/utils/register.h"
#include "alarm/alarm.h"

/*
 * Variables to interact with GTM active under GTM standby mode.
 */
bool gtm_is_standby(void);
void gtm_set_standby(bool standby);
void gtm_set_active_conninfo(const char* addr, int port);

int gtm_standby_start_startup(void);
int gtm_standby_finish_startup(void);

int gtm_standby_restore_next_gxid(void);
int gtm_standby_restore_timeline(void);
int gtm_standby_restore_next_sequuid(void);
int gtm_standby_restore_sequence(void);
int gtm_standby_restore_node(void);

int gtm_standby_activate_self(void);

GTM_Conn* gtm_standby_connect_to_standby(void);
void gtm_standby_disconnect_from_standby(GTM_Conn* conn);
GTM_Conn* gtm_standby_reconnect_to_standby(GTM_Conn* old_conn, int retry_max);
bool gtm_standby_check_communication_error(int* retry_count, GTM_Conn* oldconn);

GTM_PGXCNodeInfo* find_standby_node_info(void);

int gtm_standby_begin_backup(void);
int gtm_standby_end_backup(void);
void gtm_standby_closeActiveConn(void);
void gtm_standby_finishActiveConn(void);

/* Functions to process backup */
void ProcessGTMBeginBackup(Port* myport, StringInfo message);
void ProcessGTMEndBackup(Port* myport, StringInfo message);
int gtm_standby_begin_switchover(void);
int gtm_standby_end_switchover(void);
void ProcessGTMBeginSwitchover(Port* myport, StringInfo message, bool noblock);
void ProcessGTMEndSwitchover(Port* myport, StringInfo message, bool noblock);
void RemoveGTMXids(void);
GTMConnStatusType GetConnectionStatus(void);
bool SendBackupFileToStandby(void);
void WriteTempFile(const char* buf, char* temppath, int size);

#define invalid_file -1
#define register_node_file 0
#define gtm_control_file 1
#define gtm_sequence_file 2

extern AlarmCheckResult GTMStandbyNotAliveChecker(Alarm* alarm, char* buffer);
extern AlarmCheckResult GTMNotInSyncStatusChecker(Alarm* alarm, char* buffer);

/*
 * Startup mode
 */
#define GTM_ACT_MODE 0
#define GTM_STANDBY_MODE 1

typedef enum CheckStandbyRoleResult {
    SRR_Unknown = 0,
    SRR_Success,
    SRR_InetError,
    SRR_NotAStandby,
    SRR_ConfigureError,
} CheckStandbyRoleResult;

void QuerySyncStatus(long* sendCount, long* receiveCount);
void PrimaryASyncStandby(void);
void AdvanceStandbyASyncCounter(void);
void PrimarySyncStandby(GTM_Conn* conn, bool autosync);

void gtm_primary_sync_standby(GTM_Conn* conn);
bool gtm_sync_primary_info_to_etcd(const char* host, int port);
char* gtm_get_primary_info_from_etcd(bool force);

#endif /* GTM_STANDBY_H */
