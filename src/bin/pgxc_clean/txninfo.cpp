#include <assert.h>
#include "txninfo.h"
#include "bin/elog.h"
#include "securec.h"
#include "securec_check.h"

static int check_xid_is_implicit(char* xid);
static txn_info* find_txn(const char* gid);
static txn_info* make_txn_info(const char* dbname, TransactionId localxid, const char* gid, const char* owner);

database_info* find_database_info(const char* database_name)
{
    database_info* cur_database_info = head_database_info;

    for (; cur_database_info != NULL; cur_database_info = cur_database_info->next) {
        if (strcmp(cur_database_info->database_name, database_name) == 0)
            return (cur_database_info);
    }
    return (NULL);
}

database_info* add_database_info(const char* database_name)
{
    database_info* rv = NULL;

    if ((rv = find_database_info(database_name)) != NULL)
        return rv; /* Already in the list */
    rv = (database_info*)malloc(sizeof(database_info));
    if (rv == NULL)
        return NULL;
    rv->next = NULL;
    rv->database_name = strdup(database_name);
    if (rv->database_name == NULL) {
        free(rv);
        return NULL;
    }
    if (g_gs_clean_worker_num < 1) {
        free(rv->database_name);
        free(rv);
        return NULL; /* lenth error */
    }
    rv->all_head_txn_info = (txn_info**)calloc(g_gs_clean_worker_num, sizeof(txn_info*));
    if (rv->all_head_txn_info == NULL) {
        free(rv->database_name);
        free(rv);
        return NULL; /* malloc failed */
    }
    rv->all_last_txn_info = (txn_info**)calloc(g_gs_clean_worker_num, sizeof(txn_info*));
    if (rv->all_last_txn_info == NULL) {
        free(rv->database_name);
        free(rv->all_head_txn_info);
        free(rv);
        return NULL; /* malloc failed */
    }
    rv->index_count = 0;
    if (head_database_info == NULL) {
        head_database_info = last_database_info = rv;
        return rv;
    } else {
        last_database_info->next = rv;
        last_database_info = rv;
        return rv;
    }
}

int set_node_info(const char* node_name, int port, const char* host, NODE_TYPE type, int index)
{
    node_info* cur_node_info = NULL;

    if (index >= pgxc_clean_node_count) {
        return -1;
    }
    cur_node_info = &pgxc_clean_node_info[index];
    if (cur_node_info->node_name != NULL) {
        free(cur_node_info->node_name);
        cur_node_info->node_name = NULL;
    }
    if (cur_node_info->host != NULL) {
        free(cur_node_info->host);
        cur_node_info->host = NULL;
    }
    cur_node_info->node_name = strdup(node_name);
    if (cur_node_info->node_name == NULL)
        return -1;
    cur_node_info->port = port;
    cur_node_info->host = strdup(host);
    if (cur_node_info->host == NULL) {
        free(cur_node_info->node_name);
        return -1;
    }
    cur_node_info->type = type;
    return 0;
}

int find_node_index(const char* node_name)
{
    int i;
    for (i = 0; i < pgxc_clean_node_count; i++) {
        if (pgxc_clean_node_info[i].node_name == NULL)
            continue;
        if (strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0)
            return i;
    }
    return -1;
}

static void add_txn_gid_info(txn_info* txn, gid_info* txn_gid_info, const char* gid,
                                        bool is_exec_cn, const char* node_name)
{
    /* old version just return. */
    if (gid[0] != 'N') {
        return;
    }
    int len = strlen(gid);
    int loc[2] = {0};
    int idx = 0;
    char xidstr[64];
    char next_node_name[64];
    int i;
    errno_t errorno = EOK;

    txn_gid_info->my_gid = strdup(gid);
    if (txn_gid_info->my_gid == NULL) {
        write_stderr("No more memory.\n");
        exit(1);
    }
    if (is_exec_cn) {
        return;
    }

    for (i = 1; i < len; i++) {
        if (idx == 0 && (gid[i] == 'T' || gid[i] == 'N')) {
            txn->new_ddl_version = (gid[i] == 'N');
            loc[idx] = i;
            idx++;
        } else if (gid[i] == '_' && idx == 1) {
            loc[idx] = i;
            break;
        }
    }
    /*
     * CN's in-doubt transactions may be built by other CN,
     * so gid information doesn't include next node info, return.
     */
    if (idx == 0) {
        write_stderr("gid info was built by other cn, local cn name: \"%s\", "
            " gid: \"%s\" .\n", node_name, gid);
        return;
    }

    errorno = strncpy_s(xidstr, sizeof(xidstr), gid + 1 + loc[0], loc[1] - loc[0] - 1);
    securec_check_c(errorno, "\0", "\0");
    txn_gid_info->next_node_xid = atoxid(xidstr);

    errorno = strncpy_s(next_node_name, sizeof(next_node_name), gid + loc[1] + 1, len - loc[1] - 1);
    securec_check_c(errorno, "\0", "\0");

    int next_node_idx = find_node_index(next_node_name);
    if (next_node_idx != -1) {
        txn_gid_info->next_node_idx = next_node_idx;
    } else {
        write_stderr("can not found datanode name in node list: \"%s\", "
            " gid: \"%s\".\n", next_node_name, gid);
        /* reset my node info */
        txn_gid_info->next_node_idx = 0;
        txn_gid_info->next_node_xid = 0;
    }

}

void add_txn_info(const char* dbname, const char* node_name, TransactionId localxid, const char* gid, const char* owner,
    TXN_STATUS status)
{
    txn_info* txn = NULL;
    int nodeidx;
    bool is_exec_cn = false;

    if ((txn = find_txn(gid)) == NULL) {
        txn = make_txn_info(dbname, localxid, gid, owner);
        if (txn == NULL) {
            write_stderr("No more memory.\n");
            exit(1);
        }
    }
    nodeidx = find_node_index(node_name);
    if (nodeidx != -1) {
        is_exec_cn = (strcmp(node_name, txn->cn_nodename) == 0);
        txn->txn_stat[nodeidx] = status;
        add_txn_gid_info(txn, &txn->txn_gid_info[nodeidx], gid, is_exec_cn, node_name);
    } else {
        write_stderr("invalid node_name or not defined node_name: \"%s\".\n", node_name);
    }
    return;
}

static char* parse_gid_info(const char* gid, txn_info* txn)
{
    /* parse cn xid and nodename from gid */
        int len = strlen(gid);
        int loc = 0;
        int end_loc = len;
        char xidstr[64];
        int idx = 0;
        errno_t errorno = EOK;

        txn->cn_xid = 0;
        txn->cn_nodename[0] = '\0';

        if (gid[0] == 'T' || gid[0] == 'N') {
            for (int i = 1; i < len; i++) {
                if (gid[i] == '_' && idx == 0) {
                    loc = i;
                    if (gid[0] == 'T') {
                       break;
                    } else {
                       idx++;
                    }
                } else if ((gid[i] == 'T' || gid[i] == 'N') && idx == 1) {
                    end_loc = i - 1;
                    break;
                }
            }

            if (loc - 1 >= 1 && end_loc - loc - 1 > 0) {
                errorno = strncpy_s(xidstr, sizeof(xidstr), gid + 1, loc - 1);
                securec_check_c(errorno, "\0", "\0");

                {
                    txn->cn_xid = atoxid(xidstr);
                    errorno = strncpy_s(txn->cn_nodename, sizeof(txn->cn_nodename), gid + loc + 1, end_loc - loc - 1);
                    securec_check_c(errorno, "\0", "\0");
                }
            } else {
                write_stderr("client defined gid: \"%s\".", gid);
            }
        } else {
            write_stderr("client defined gid: \"%s\".", gid);
        }

    txn->gid = (char*)malloc(sizeof(char) * (end_loc + 1));
    if (txn->gid == NULL) {
        /* caller will free the memory of txn */
        return NULL;
    }
    errorno = strncpy_s(txn->gid, (end_loc + 1), gid, end_loc);
    securec_check_c(errorno, "\0", "\0");
    txn->gid[end_loc] = '\0';
    txn->new_version = (gid[0] == 'N');
    return txn->gid;
}

static txn_info* make_txn_info(const char* dbname, TransactionId localxid, const char* gid, const char* owner)
{
    database_info* dbinfo = NULL;
    txn_info* txn = NULL;
    errno_t errorno = EOK;

    if ((dbinfo = find_database_info(dbname)) == NULL)
        dbinfo = add_database_info(dbname);
    if (dbinfo == NULL)
        return NULL;
    txn = (txn_info*)malloc(sizeof(txn_info));
    if (txn == NULL)
        return NULL;

    errorno = memset_s(txn, sizeof(txn_info), 0, sizeof(txn_info));
    securec_check_c(errorno, "\0", "\0");

    txn->localxid = localxid;
    if (parse_gid_info(gid, txn) == NULL) {
        free(txn);
        return NULL;
    }    
    txn->owner = strdup(owner);
    if (txn->owner == NULL) {
        free(txn->gid);
        txn->gid = NULL;
        free(txn);
        return NULL;
    }
    /* assign jobs */
    int work_index = dbinfo->index_count;
    if (dbinfo->all_head_txn_info[work_index] == NULL) {
        dbinfo->all_head_txn_info[work_index] = dbinfo->all_last_txn_info[work_index] = txn;
    } else {
        dbinfo->all_last_txn_info[work_index]->next = txn;
        dbinfo->all_last_txn_info[work_index] = txn;
    }
    /* update count */
    dbinfo->index_count = (dbinfo->index_count + 1) % g_gs_clean_worker_num;
    txn->txn_stat = (TXN_STATUS*)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count);
    if (txn->txn_stat == NULL) {
        free(txn->gid);
        free(txn->owner);
        free(txn);
        return (NULL);
    }
    errorno = memset_s(
        txn->txn_stat, sizeof(TXN_STATUS) * pgxc_clean_node_count, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);
    securec_check_c(errorno, "\0", "\0");

    txn->txn_gid_info = (gid_info*)malloc(sizeof(gid_info) * pgxc_clean_node_count);
    if (txn->txn_gid_info == NULL) {
        free(txn->gid);
        free(txn->owner);
        free(txn->txn_stat);
        free(txn);
        return (NULL);
    }
    errorno = memset_s(
        txn->txn_gid_info, sizeof(gid_info) * pgxc_clean_node_count, 0, sizeof(gid_info) * pgxc_clean_node_count);
    securec_check_c(errorno, "\0", "\0");

    write_stderr("make TXN: localxid: %lu, gid: \"%s\", cn_xid: \"%lu\", cn_nodename: \"%s\", version: \" %s \" \n",
        txn->localxid,
        txn->gid,
        txn->cn_xid,
        txn->cn_nodename,
        txn->new_version ? "new" : "old or gtm");

    return txn;
}

static txn_info* find_txn(const char* gid)
{
    database_info* cur_db = NULL;
    txn_info* cur_txn = NULL;
    int work_index = 0;

    for (cur_db = head_database_info; cur_db != NULL; cur_db = cur_db->next) {
        /* search all workers */
        for (work_index = 0; work_index < g_gs_clean_worker_num; work_index++) {
            for (cur_txn = cur_db->all_head_txn_info[work_index]; cur_txn != NULL; cur_txn = cur_txn->next) {
                if (strncmp(cur_txn->gid, gid, strlen(gid)) == 0 || strstr(gid, cur_txn->gid) != NULL)
                    return cur_txn;
            }
        }
    }
    return NULL;
}

#define TXN_PREPARED 0x0001
#define TXN_COMMITTED 0x0002
#define TXN_ABORTED 0x0004
#define TXN_UNCONNECT 0x0008

/* Return txn status according to the check_flag, called by check_txn_global_status() */
static TXN_STATUS get_txn_global_status(txn_info* txn, uint32 check_flag)
{
    if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
        /* Mix of committed and aborted. This should not happen. */
        return TXN_STATUS_FAILED;
    if (check_flag & TXN_COMMITTED)
        /* Some 2PC transactions are committed.  Need to commit others. */
        return TXN_STATUS_COMMITTED;
    if (check_flag & TXN_ABORTED)
        /* Some 2PC transactions are aborted.  Need to abort others. */
        return TXN_STATUS_ABORTED;
    if (check_flag & TXN_UNCONNECT) {
        if ((rollback_cn_name != NULL) && strncmp(txn->cn_nodename, rollback_cn_name, 64) == 0)
            return TXN_STATUS_ABORTED;
        else
            /* Some node connect bad.  No need to recover. */
            return TXN_STATUS_UNCONNECT;
    }
    /* All the transactions remain prepared.   No need to recover. */
    if (check_xid_is_implicit(txn->gid)) {
        return TXN_STATUS_COMMITTED;
    }
    return TXN_STATUS_PREPARED;
}

TXN_STATUS check_txn_global_status(txn_info* txn, bool commit_all_prepared, bool rollback_all_prepared)
{
    int ii;
    uint32 check_flag = 0;

    if (txn == NULL)
        return TXN_STATUS_INITIAL;
    /*
     * if exec cn is prepared but not running,
     * all participater should be prepared, because
     * always finish local prepared transaction firstly.
     * add quick notification.
     */
    if (txn->is_exec_cn_prepared == true) {
        if (commit_all_prepared) {
            check_flag |= TXN_COMMITTED;
        } else if (rollback_all_prepared) {
            check_flag |= TXN_ABORTED;
        }
    }
    for (ii = 0; ii < pgxc_clean_node_count; ii++) {
        if (txn->txn_stat[ii] == TXN_STATUS_RUNNING) {
            return TXN_STATUS_RUNNING;
        }
        if (txn->txn_stat[ii] == TXN_STATUS_INITIAL || txn->txn_stat[ii] == TXN_STATUS_UNKNOWN) {
            continue;
        } else if (txn->txn_stat[ii] == TXN_STATUS_PREPARED) {
            check_flag |= TXN_PREPARED;
        } else if (txn->txn_stat[ii] == TXN_STATUS_COMMITTED) {
            check_flag |= TXN_COMMITTED;
        } else if (txn->txn_stat[ii] == TXN_STATUS_ABORTED) {
            check_flag |= TXN_ABORTED;
        } else if (txn->txn_stat[ii] == TXN_STATUS_UNCONNECT) {
            check_flag |= TXN_UNCONNECT;
        } else {
            return TXN_STATUS_FAILED;
        }
    }
    return get_txn_global_status(txn, check_flag);
}

/*
 * Returns 1 if implicit, 0 otherwise.
 *
 * Should this be replaced with regexp calls?
 */
static int check_xid_is_implicit(char* xid)
{
#define XIDPREFIX "_$XC$"

    if (strncmp(xid, XIDPREFIX, strlen(XIDPREFIX)) != 0)
        return 0;
    for (xid += strlen(XIDPREFIX); *xid; xid++) {
        if (*xid < '0' || *xid > '9') {
            return 0;
        }
    }
    return 1;
}

bool check2PCExists(void)
{
    database_info* cur_db = NULL;
    int work_index = 0;
    for (cur_db = head_database_info; cur_db != NULL; cur_db = cur_db->next) {
        /* search all workers */
        for (work_index = 0; work_index < g_gs_clean_worker_num; work_index++) {
            if (cur_db->all_head_txn_info[work_index] != NULL)
                return true;
        }
    }
    return (false);
}

char* str_txn_stat(TXN_STATUS status)
{
    switch (status) {
        case TXN_STATUS_INITIAL:
            return ("initial");
        case TXN_STATUS_UNKNOWN:
            return ("unknown");
        case TXN_STATUS_PREPARED:
            return ("prepared");
        case TXN_STATUS_COMMITTED:
            return ("committed");
        case TXN_STATUS_ABORTED:
            return ("aborted");
        case TXN_STATUS_UNCONNECT:
            return ("unconnect");
        case TXN_STATUS_FAILED:
            return ("failed");
        case TXN_STATUS_RUNNING:
            return ("running");
        default:
            return ("undefined status");
    }
}

