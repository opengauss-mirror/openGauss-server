#include "utils/atomic.h"
#include "access/tableam.h"
#include "nodes/plannodes.h"
#include "utils/oidrbtree.h"
#include "utils/snapmgr.h"
#include "catalog/gs_policy_label.h"
#include "query_anomaly/query_anomaly_labels.h"

static OidRBTree *g_labels = NULL;
static volatile long g_labels_init = 0;
static MemoryContext g_query_anomaly_labels_ctx = NULL;
static pthread_mutex_t query_anomaly_labels_lock_write = PTHREAD_MUTEX_INITIALIZER;

/*
 * @brief load_labels
 * load the labels data structure from the catalog tables
 * should have the query_anomaly_labels_lock_write locked before calling it
 */
static void load_labels()
{
    MemoryContext old_cxt = MemoryContextSwitchTo(g_query_anomaly_labels_ctx);
    Relation labels_relation = heap_open(GsPolicyLabelRelationId, AccessShareLock);
    if (labels_relation == NULL) {
        MemoryContextSwitchTo(old_cxt);
        return;
    }
    HeapTuple rtup;
    Form_gs_policy_label rel_data;

    TableScanDesc scan = heap_beginscan(labels_relation, SnapshotNow, 0, NULL);
    if (g_labels) {
        DestroyOidRBTree(&g_labels);
        g_labels = NULL;
    }
    g_labels = CreateOidRBTree();
    while (scan && (rtup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_policy_label)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        OidRBTreeInsertOid(g_labels, rel_data->fqdnid);
    }
    heap_endscan(scan);
    heap_close(labels_relation, AccessShareLock);
    MemoryContextSwitchTo(old_cxt);
}

/*
 * @brief init_anomaly_labels
 * init the labels data structure
 */
static void init_anomaly_labels()
{
    if (likely(g_labels_init == 1)) {
        return; // Implement double lock checking to speed up the likely case
    }
    pthread_mutex_lock(&query_anomaly_labels_lock_write);
    if (g_labels_init == 1) {
        pthread_mutex_unlock(&query_anomaly_labels_lock_write);
        return;
    }
    if (!g_query_anomaly_labels_ctx) {
        g_query_anomaly_labels_ctx = AllocSetContextCreate(
            g_instance.instance_context, "AnomalyDetectinLabelsCTX", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT, false);
    }
    load_labels();
    g_labels_init = 1;
    pthread_mutex_unlock(&query_anomaly_labels_lock_write);
}

/*
 * @brief load_query_anomaly_labels
 * load all resource labels for use as a rule of query anomaly
 * @param reload boolean indicates if this is the init phase or reloading phase
 */
void load_query_anomaly_labels(const bool reload)
{
    init_anomaly_labels();
    if (reload) {
        pthread_mutex_lock(&query_anomaly_labels_lock_write);
        load_labels();
        pthread_mutex_unlock(&query_anomaly_labels_lock_write);
    }
}

/*
 * @brief get_query_anomaly_labels
 * get all resource labels used by statement
 * @param relationOids List of Oids to check
 */
const bool get_query_anomaly_labels(const List *relationOids)
{
    pthread_mutex_lock(&query_anomaly_labels_lock_write);
    if (!g_labels) {
        pthread_mutex_unlock(&query_anomaly_labels_lock_write);
        return false;
    }
    ListCell *lc = NULL;
    foreach(lc, relationOids)
    {
        if (OidRBTreeMemberOid(g_labels, lfirst_oid(lc))) {
            pthread_mutex_unlock(&query_anomaly_labels_lock_write);
            return true;
        }
    }
    pthread_mutex_unlock(&query_anomaly_labels_lock_write);
    return false;
}

void finish_query_anomaly_labels()
{
    if (g_query_anomaly_labels_ctx == NULL) {
        return;
    }
    MemoryContext old_cxt = MemoryContextSwitchTo(g_query_anomaly_labels_ctx);

    if (g_labels && g_labels->root) {
        g_labels_init = 0;
        DestroyOidRBTree(&g_labels);
        g_labels = NULL;
    }
    MemoryContextSwitchTo(old_cxt);
    MemoryContextDelete(g_query_anomaly_labels_ctx);
    g_query_anomaly_labels_ctx = NULL;
}
