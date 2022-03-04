/* -------------------------------------------------------------------------
 *
 * txninfo.h
 *	  Prepared transaction info
 *
 * Portions Copyright (c) 2012 Postgres-XC Development Group
 *
 * $Postgres-XC$
 *
 * -------------------------------------------------------------------------
 */

#ifndef TXNINFO_H
#define TXNINFO_H

#include "gtm/gtm_c.h"

#define atoxid(x) ((GlobalTransactionId)strtoul((x), NULL, 10))

typedef enum TXN_STATUS {
    TXN_STATUS_INITIAL = 0, /* Initial */
    TXN_STATUS_UNKNOWN,     /* Unknown: Frozen, running, or not started */
    TXN_STATUS_PREPARED,
    TXN_STATUS_COMMITTED,
    TXN_STATUS_ABORTED,
    TXN_STATUS_UNCONNECT,
    TXN_STATUS_RUNNING,
    TXN_STATUS_FAILED /* Error detected while interacting with the node */
} TXN_STATUS;

typedef enum NODE_TYPE { NODE_TYPE_COORD = 1, NODE_TYPE_DATANODE } NODE_TYPE;

typedef struct node_info {
    char* node_name;
    int port;
    char* host;
    NODE_TYPE type;
} node_info;

typedef struct gid_info {
    char* my_gid;
    int next_node_idx;
    TransactionId next_node_xid;
} gid_info;

extern node_info* pgxc_clean_node_info;
extern int pgxc_clean_node_count;

typedef struct txn_info {
    struct txn_info* next;
    TransactionId localxid;
    TransactionId cn_xid;
    char cn_nodename[64];
    char* gid; /* GID used in prepare */
    char* owner;
    TXN_STATUS* txn_stat; /* Array for each nodes */
    gid_info* txn_gid_info;
    bool new_version;
    bool is_exec_cn_prepared;
    bool new_ddl_version;
    char* msg;            /* Notice message for this txn. */
} txn_info;

typedef struct database_info {
    struct database_info* next;
    char* database_name;
    txn_info** all_head_txn_info;
    txn_info** all_last_txn_info;
    int index_count;
} database_info;

extern database_info* head_database_info;
extern database_info* last_database_info;
extern char* rollback_cn_name;
extern int g_gs_clean_worker_num;
/* Functions */

extern txn_info* init_txn_info(char* database_name, TransactionId gxid);
extern void add_txn_info(
    const char* database, const char* node, TransactionId gxid, const char* xid, const char* owner, TXN_STATUS status);
extern txn_info* find_txn_info(TransactionId gxid);
extern database_info* find_database_info(const char* database_name);
extern database_info* add_database_info(const char* database_name);
extern int find_node_index(const char* node_name);
extern int set_node_info(const char* node_name, int port, const char* host, NODE_TYPE type, int index);
extern TXN_STATUS check_txn_global_status(txn_info* txn, bool commit_all_prepared, bool rollback_all_prepared);
extern bool check2PCExists(void);
extern char* str_txn_stat(TXN_STATUS status);

#endif /* TXNINFO_H */
