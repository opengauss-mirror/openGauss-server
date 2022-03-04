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
 * gs_policy_labels.cpp
 *    operation functions related to labes, such as update, scan, check elements.
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_labels.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/heapam.h"
#include <ctype.h>
#include "commands/dbcommands.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "gs_policy_object_types.h"
#include "gs_policy_labels.h"
#include "storage/lock/lock.h"
#include "storage/spin.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "gs_mask_policy.h"
#include "gs_policy_plugin.h"

static THR_LOCAL loaded_labels *all_labels = NULL;

static pg_atomic_uint64           label_global_version = 1;
static THR_LOCAL pg_atomic_uint64 label_local_version = 0;

/*
 * update_label_value
 *    update related label row while data resource is altered
 *
 * If update target is view/table/function/schema will not update, they recorded using oid. So no need update.
 * For column type, update gs_policy_label when table column altered.
 */
bool update_label_value(const gs_stl::gs_string object_name, const gs_stl::gs_string new_object_name, 
                        int object_type)
{
    bool updated = false;
    bool nulls[Natts_gs_policy_label] = {false};
    bool replaces[Natts_gs_policy_label] = {false};
    Datum values[Natts_gs_policy_label] = {0};
    HeapTuple rtup = NULL;
    Relation rel = NULL;
    TableScanDesc scan = NULL;

    /* schema and table are recorded in Oid, so they are no need to update when renamed */
    if (object_type != O_COLUMN) {
        return updated;
    }

    rel = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);
    scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    if (scan != NULL) {
        while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
            Form_gs_policy_label rel_data = (Form_gs_policy_label)GETSTRUCT(rtup);
            if (rel_data == NULL) {
                continue;
            }
            if (!strcasecmp(rel_data->relcolumn.data, object_name.c_str())) {
                replaces[Anum_gs_policy_label_relcolumn - 1] = true;
                values[Anum_gs_policy_label_relcolumn - 1] = 
                    DirectFunctionCall1(namein, CStringGetDatum(new_object_name.c_str()));
            }

            HeapTuple new_tuple = heap_modify_tuple(rtup, RelationGetDescr(rel), values, nulls, replaces);
            simple_heap_update(rel, &new_tuple->t_self, new_tuple);
            CatalogUpdateIndexes(rel, new_tuple);
            updated = true;
        }
        heap_endscan(scan);
    }
    heap_close(rel, RowExclusiveLock);
    return updated;
}

/*
 * scan_policy_labels
 *    scan existing labels from gs_policy_label to tmp_labels
 */
bool scan_policy_labels(loaded_labels *tmp_labels)
{
    Assert(tmp_labels != NULL);
    tmp_labels->clear();

    TableScanDesc         scan = NULL;
    HeapTuple            rtup = NULL;
    Form_gs_policy_label rel_data = NULL;
    bool                 label_type_found = false;
    Relation             rel = NULL;

    rel = heap_open(GsPolicyLabelRelationId, AccessShareLock);
    scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    /* scan each row from gs_policy_label into tmp_labels */
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_policy_label)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }

        PolicyLabelItem item(rel_data->fqdnnamespace,
                             rel_data->fqdnid,
                             get_privilege_object_type(rel_data->fqdntype.data),
                             rel_data->relcolumn.data);
        const char *label_name = rel_data->labelname.data;

        if (item.m_obj_type == O_UNKNOWN) {
            if (strlen(label_name) > 0) {
                (*tmp_labels)[label_name];
            }
            continue;   /* empty label */
        }

        (*tmp_labels)[label_name][item.m_obj_type].insert(item);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return label_type_found;
}

bool is_label_exist(const char *name)
{
    if (!strcasecmp(name, "all")) { /* any label */
        return true;
    }
    loaded_labels *tmp = get_policy_labels();
    if (tmp == NULL) {
        return false;
    }
    return (tmp->find(name) != tmp->end());
}

loaded_labels *get_policy_labels()
{
    load_policy_labels(true);
    return all_labels;
}

bool load_policy_labels(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }
    
    if (!reload) {
        pg_atomic_add_fetch_u64(&label_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&label_global_version, (uint64*)&label_local_version, label_global_version)) {
        /* Latest label, changes nothing */
        return false;
    }

    /* scan to load all labels */
    if (all_labels == NULL) {
        all_labels = new loaded_labels;
    }
    scan_policy_labels(all_labels);

    return true;
}

/*
 * check_label_has_object
 *    check object is bound to labels and used in other policies.
 *    param 'labels' means labels used in other features.
 *    CheckLabelBoundPolicy is the check function provided by other features that label is of policies
 */
bool check_label_has_object(const PolicyLabelItem *object, 
                            bool (*CheckLabelBoundPolicy)(bool, const gs_stl::gs_string),
                            bool column_type_is_changed,
                            const policy_default_str_set *labels)
{
    /* Ignore checking when upgrade */
    if (u_sess->attr.attr_common.IsInplaceUpgrade) {
        return false;
    }
    Assert(CheckLabelBoundPolicy != NULL);
    loaded_labels *cur_all_labels = get_policy_labels();
    if (cur_all_labels == NULL) {
        return false;
    }

    loaded_labels::const_iterator it = cur_all_labels->begin();
    loaded_labels::const_iterator eit = cur_all_labels->end();
    for (; it != eit; ++it) {
        /* for each item of loaded existing labels, and match labels */
        if (labels != NULL && labels->find(*(it->first)) == labels->end()) {
            continue;
        }
        /* find wether object(label row) is in matched label */
        typed_labels::const_iterator fit = it->second->find(object->m_obj_type);
        if (fit != it->second->end()) {
            if (fit->second->find(*object) != fit->second->end()) {
                /* check label is bound to Policy */
                if (CheckLabelBoundPolicy(column_type_is_changed, *(it->first))) {
                    return true;
                }
            } else if (object->m_obj_type == O_COLUMN) { /* verify table of column */
                gs_policy_label_set::const_iterator tit, teit;
                tit = fit->second->begin();
                teit = fit->second->end();
                for (; tit != teit; ++tit) {
                    if (tit->m_schema == object->m_schema && tit->m_object == object->m_object) {
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

void reset_policy_labels()
{
    pg_atomic_exchange_u64(&label_local_version, 0);
}

void clear_thread_local_label() 
{
    if (all_labels != NULL) {
        delete all_labels;
        all_labels = NULL;
    }
}

void verify_drop_column(AlterTableStmt *stmt)
{
    ListCell *lcmd = NULL;
    foreach (lcmd, stmt->cmds) {
        AlterTableCmd *cmd = (AlterTableCmd *)lfirst(lcmd);
        switch (cmd->subtype) {
            case AT_DropColumn: {
                /* check by column */
                PolicyLabelItem find_obj(stmt->relation->schemaname, stmt->relation->relname, cmd->name, O_COLUMN);
                if (check_label_has_object(&find_obj, is_masking_has_object)) {
                    char buff[512] = {0};
                    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "Column: %s is part of some resource label, can not be renamed.", find_obj.m_column);
                    securec_check_ss(rc, "\0", "\0");
                    gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
                }
                break;
            }
            case AT_AlterColumnType: {
                PolicyLabelItem find_obj(stmt->relation->schemaname, stmt->relation->relname, cmd->name, O_COLUMN);
                if (check_label_has_object(&find_obj, is_masking_has_object, true)) {
                    char buff[512] = {0};
                    int ret = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "Column: %s is part of some masking policy, can not be changed.", find_obj.m_column);
                    securec_check_ss(ret, "\0", "\0");
                    gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
                }
                break;
            }
            default:
                break;
        }
    }
}