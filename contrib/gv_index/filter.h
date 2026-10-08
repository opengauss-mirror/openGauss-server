/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * filter.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/filter.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GV_GRAPH_FILTER_H
#define GV_GRAPH_FILTER_H

#include "graph_values.h"
#include "utils.h"
#include "lite/subindex/details/pair.h"

/* Forward declaration */
struct IndexTupleData;
class IndexScanDescData;

/*
 * VacuumFilter: used during VACUUM / bulk delete.
 * Checks each value by reading xmin/xmax from the raw page.
 */
template <typename Value>
struct VacuumFilter : public annlite::Filter<Value> {
    VacuumFilter(Relation index, bool is_ustore, IndexBulkDeleteCallback callback, const void* callback_state)
        : m_rel(index), m_is_ustore(is_ustore), m_callback(callback), m_callback_state(callback_state)
    {}
    bool operator()(const Value& value) override
    {
        Assert(false);
        return true;
    }
    bool operator()(const Value& value, size_t offset) override
    {
        Assert(false);
        return true;
    }
    bool operator()(const Value& value, const char* page, size_t offset) override
    {
        if (!m_is_ustore) {
            Assert(m_callback != NULL);
            if (m_callback == NULL) {
                return false;
            }
            return m_callback(
                const_cast<ItemPointer>(&(*reinterpret_cast<const GraphValueTypeV3*>(&value)).index_tuple.t_tid),
                (void *)m_callback_state, InvalidOid, InvalidBktId);
        } else {
            VectorIndexXidData uxid;
            bool xmin_committed = false;
            bool xmax_committed = false;
            return gv_graph_get_tuple_xmin_xmax(
                const_cast<char*>(page), FirstOffsetNumber + offset,
                InvalidTransactionId, &(uxid.xmin), &(uxid.xmax),
                &xmin_committed, &xmax_committed, RelationGetNamespace(m_rel) == PG_TOAST_NAMESPACE);
        }
        return false;
    }
private:
    Relation m_rel;
    bool m_is_ustore;
    IndexBulkDeleteCallback m_callback;
    const void* m_callback_state;
};

/*
 * SelectFilter: used during index scan to filter out invisible tuples.
 */
template <typename Value>
struct SelectFilter : public annlite::Filter<Value> {
    SelectFilter(IndexScanDescData* scan, bool is_ustore) : m_scan(scan), m_is_ustore(is_ustore) {}
    bool operator()(const Value& value) override
    {
        if (!m_is_ustore) {
            return true;
        }
        bool need_recheck = false;
        IndexTupleData* result = nullptr;
        result = vector_check_keys_simple(m_scan, *reinterpret_cast<const GraphValueTypeV3*>(&value),
                ForwardScanDirection, &need_recheck, true);
        (void)need_recheck;
        return result != nullptr;
    }
    bool operator()(const Value& value, size_t offset) override
    {
        Assert(false);
        return true;
    }
    bool operator()(const Value& value, const char* page, size_t offset) override
    {
        Assert(false);
        return true;
    }
private:
    IndexScanDescData* m_scan;
    bool m_is_ustore;
};

/*
 * DeleteFilter: used during index delete to match and mark-delete a specific tuple.
 */
template <typename Value>
struct DeleteFilter : public annlite::Filter<Value> {
    DeleteFilter(Relation rel, IndexTuple itup, TransactionId build_index_delete_xid)
        : m_rel(rel), m_itup(itup), m_build_index_delete_xid(build_index_delete_xid)
    {}
    bool operator()(const Value& value) override
    {
        Assert(false);
        return true;
    }
    bool operator()(const Value& value, size_t offset) override
    {
        Assert(false);
        return true;
    }
    bool operator()(const Value& value, const char* page, size_t offset) override
    {
        const GraphValueTypeV3* real_value = reinterpret_cast<const GraphValueTypeV3*>(&value);
        if (!ItemPointerEquals(&(m_itup->t_tid), const_cast<ItemPointer>(&real_value->index_tuple.t_tid))) {
            return false;
        }
        if (!gv_graph_tuple_equal(const_cast<IndexTuple>(&real_value->index_tuple), m_itup)) {
            return false;
        }
        TransactionId xmin;
        TransactionId xmax;
        bool xmin_committed = false;
        bool xmax_committed = false;
        bool is_dead = gv_graph_get_tuple_xmin_xmax(const_cast<char *>(page), FirstOffsetNumber + offset,
            InvalidTransactionId, &xmin, &xmax, &xmin_committed, &xmax_committed,
            RelationGetNamespace(m_rel) == PG_TOAST_NAMESPACE);
        if (!is_dead && !TransactionIdIsValid(xmax)) {
            modify_xid(real_value);
            return true;
        }
        return false;
    }
    void modify_xid(const GraphValueTypeV3* value)
    {
        // the xid to be modified doesn't need set null
        vector_set_xmin_xmax(const_cast<VectorIndexXid>(&value->trans_info), VECTOR_DML_DELETE,
                             m_build_index_delete_xid);
    }
private:
    Relation m_rel;
    IndexTuple m_itup;
    TransactionId m_build_index_delete_xid;
};

#endif /* GV_GRAPH_FILTER_H */
