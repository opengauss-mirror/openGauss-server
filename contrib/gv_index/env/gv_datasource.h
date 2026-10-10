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
 * gv_datasource.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_datasource.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_DATASOURCE_H
#define GVGRAPH_DATASOURCE_H

#include "c.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_partition.h"
#include "cm/cm_msg.h"
#include "nodes/relation.h"
#include "storage/lmgr.h"
#include "storage/buf/bufmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "../utils.h"
#include "../graph_values.h"
#include "lite/index/value64.h"
#include "lite/index/light_env/data_source.h"
#include "lite/subindex/details/pair.h"
#include "lite/subindex/vector_format/original/new_vector.h"

namespace gs_vector {

template <typename Key, typename Value>
struct GVDataSourceImpl : public annlite::light_env::DataSource<Key, Value> {
    using ValueType = Value;
    using HeapType = GVDataSourceImpl<Key, Value>;
    using ScanOperator = void (*)(void *, const Key *, size_t, const Value &);
    using ScanOperatorWithPosition = typename annlite::light_env::DataSource<Key, Value>::ScanOperatorWithPosition;
    using ScanOperatorWithScalar = typename annlite::light_env::DataSource<Key, Value>::ScanOperatorWithScalar;

private:
    static size_t estimate_row_count(Relation heap)
    {
        if (heap == nullptr) {
            return 0;
        }

        double reltuples = heap->rd_rel->reltuples;
        if (reltuples > 0) {
            return static_cast<size_t>(reltuples * 1.1) + 1;
        }

        BlockNumber pages = RelationGetNumberOfBlocks(heap);
        if (pages > 0) {
            static constexpr size_t default_tuples_per_page = 100;
            size_t page_tuples = static_cast<size_t>(pages) * default_tuples_per_page;
            return page_tuples + page_tuples / 5;
        }
        return 0;
    }

    struct ScanOperatorAndContext {
        ScanOperatorAndContext(GVDataSourceImpl *source, ScanOperator scan_opr_no_pos,
                               ScanOperatorWithPosition scan_opr_with_pos,
                               ScanOperatorWithScalar scan_opr_with_scalar, void *opr_context)
            : m_source(source), m_scan_opr_no_pos(scan_opr_no_pos),
              m_scan_opr_with_pos(scan_opr_with_pos), m_scan_opr_with_scalar(scan_opr_with_scalar),
              m_opr_context(opr_context)
        {}
        GVDataSourceImpl* get_source() const { return m_source; }
        ScanOperator get_scanop() const { return m_scan_opr_no_pos; }
        ScanOperatorWithPosition get_scanop_with_pos() const { return m_scan_opr_with_pos; }
        ScanOperatorWithScalar get_scanop_with_scalar() const { return m_scan_opr_with_scalar; }
        bool has_position_callback() const { return m_scan_opr_with_pos != nullptr; }
        bool has_scalar_callback() const { return m_scan_opr_with_scalar != nullptr; }
        void* get_context() const {return m_opr_context; }

    private:
        GVDataSourceImpl *m_source;
        ScanOperator m_scan_opr_no_pos;
        ScanOperatorWithPosition m_scan_opr_with_pos;
        ScanOperatorWithScalar m_scan_opr_with_scalar;
        void *m_opr_context;
    };

public:

    GVDataSourceImpl(Relation heap, Relation index, IndexInfo *index_info, bool is_ustore)
        : m_heap(heap), m_index(index), m_ii(index_info),
        m_index_tuples(0), m_heap_tuples(0), m_is_ustore(is_ustore),
        m_estimated_rows(estimate_row_count(heap))
    {}

    bool is_open() const { return m_heap != nullptr; }
    RelationData* heap_relation() const { return m_heap; }
    RelationData* index_relation() const { return m_index; }
    bool is_ustore() const { return m_is_ustore; }

    void set_rows(size_t rows) override
    {
        if (rows > 0) {
            m_estimated_rows = rows;
        }
    }

    size_t get_rows() const override
    {
        return m_estimated_rows;
    }

    bool rows_counted() const override
    {
        return true;
    }

    double heap_tuples() const { return m_heap_tuples; }
    double index_tuples() const { return (double)m_index_tuples; }

    void open()
    {
        if (is_open()) {
            return;
        }

        if (m_heap != nullptr) {
            return;
        }

        Oid heap_oid = m_index->rd_index->indrelid;
        m_heap = heap_open(heap_oid, AccessShareLock);
    }

    void close()
    {
        if (!is_open()) {
            return;
        }
        heap_close(m_heap, AccessShareLock);
        m_heap = nullptr;
    }

    void scan(void *opr_context, ScanOperator scan_opr, annlite::light_env::ComputeTaskRunner *pool) override
    {
        ScanOperatorAndContext opr_cxt(this, scan_opr, nullptr, nullptr, opr_context);
        do_scan(&opr_cxt);
    }

    void scan(void *opr_context, ScanOperatorWithPosition scan_opr,
              annlite::light_env::ComputeTaskRunner *pool) override
    {
        ScanOperatorAndContext opr_cxt(this, nullptr, scan_opr, nullptr, opr_context);
        do_scan(&opr_cxt);
    }

    void scan(void *opr_context, ScanOperatorWithScalar scan_opr,
              annlite::light_env::ComputeTaskRunner *pool) override
    {
        ScanOperatorAndContext opr_cxt(this, nullptr, nullptr, scan_opr, opr_context);
        do_scan(&opr_cxt);
    }

    void do_scan(ScanOperatorAndContext *opr_cxt)
    {
        bool need_open = !is_open();
        if (need_open) {
            open();
        }
        if (m_heap == NULL) {
            return;
        }
        m_index_tuples = 0;
        m_heap_tuples = tableam_index_build_scan(m_heap, m_index, m_ii, true,
            ScanOperatorCallBack, opr_cxt, NULL);
        if (need_open) {
            close();
        }
    }

    struct ReadIterator : public annlite::light_env::DataSourceIterator<Key, Value> {
        using VertexType = uint32_t;
        using VectorType = annlite::NewVector;
        
        using PairType = annlite::Pair<VertexType, ValueType>;
        ReadIterator(RelationData* heap, RelationData* index, bool is_ustore)
            : heap_rel(heap), index_rel(index),
            m_is_valid(false), m_is_ustore(is_ustore), m_value(nullptr),
            m_detoasted(nullptr), scan(nullptr), index_info(nullptr), slot(nullptr), estate(nullptr)
        {}
        ReadIterator(const HeapType& heap)
            : ReadIterator(heap.heap_relation(), heap.index_relation(), heap.is_ustore())
        {}
        bool is_valid() const override { return m_is_valid; }

        void seek(const ValueType& value) override
        {
            seek_internal(((*reinterpret_cast<const GraphValueTypeV3*>(&value)).index_tuple.t_tid));
        }
        void seek_internal(const ItemPointerData& ctid)
        {
            close();
            seek_tuple(ctid);
        }
        const Key* vector() override
        {
            Assert(m_value != nullptr);
            return &m_value->x[0];
        }
        void close()
        {
            if (!m_is_valid) {
                return;
            }
            EndSeek();
            m_is_valid = false;
        }
    private:
        void seek_tuple(const ItemPointerData& ctid)
        {
            Tuple tuple = NULL;
            TupleDesc heap_tup_desc = NULL;

            Assert(heap_rel != nullptr);
            scan = RelationGetIndexScan(index_rel, 0, 0);
            scan->isUpsert = false;
            scan->heapRelation = heap_rel;
            scan->xs_ctup.t_self = ctid;
            scan->xs_snapshot = GetActiveSnapshot();
            
            tuple = IndexFetchTuple(scan);
            if (tuple == NULL) {
                // got nothing
                EndScan();
                return;
            }

            heap_tup_desc = RelationGetDescr(heap_rel);
            index_info = BuildIndexInfo(index_rel);

            estate = CreateExecutorState();
            slot = MakeSingleTupleTableSlot(heap_tup_desc, false, heap_tup_desc->td_tam_ops);
            ExprContext *econtext = GetPerTupleExprContext(estate);
            econtext->ecxt_scantuple = slot;
            tableam_tslot_store_tuple(tuple, slot, InvalidBuffer, false, false);

            /* Prepare the scan for evaluating exprs (if exist) */
            Datum values[INDEX_MAX_KEYS];
            bool isnull[INDEX_MAX_KEYS];

            /* form index datum with correct tuple descriptor */
            FormIndexDatum(index_info, slot, estate, &values[0], &isnull[0]);

            if (isnull[0]) {
                EndSeek();
                return;
            }

            Datum detoasted = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
            m_value = (VectorType*) DatumGetPointer(detoasted);
            if (DatumGetPointer(detoasted) != DatumGetPointer(values[0])) {
                m_detoasted = DatumGetPointer(detoasted);
            }
            m_is_valid = true;
        }
        void EndSeek()
        {
            if (m_detoasted != nullptr) {
                pfree(m_detoasted);
                m_detoasted = nullptr;
            }
            m_value = nullptr;
            if (slot != nullptr) {
                ExecDropSingleTupleTableSlot(slot);
                slot = nullptr;
            }
            if (estate != nullptr) {
                FreeExecutorState(estate);
                estate = nullptr;
            }
            /* These may have been pointing to the now-gone estate */
            index_info->ii_ExpressionsState = NIL;
            pfree_ext(index_info);
            index_info = nullptr;
            EndScan();
        }
        void EndScan()
        {
            if (BufferIsValid(scan->xs_cbuf)) {
                ReleaseBuffer(scan->xs_cbuf);
                scan->xs_cbuf = InvalidBuffer;
            }
            IndexScanEnd(scan);
            scan = nullptr;
        }

        RelationData* heap_rel;
        RelationData* index_rel;

        bool m_is_valid;
        bool m_is_ustore;
        VectorType* m_value;
        void* m_detoasted;

        IndexScanDesc scan;
        IndexInfo* index_info;
        TupleTableSlot* slot;
        EState* estate;
    };

private:
    Relation m_heap;
    Relation m_index;
    IndexInfo* m_ii;

    // For fast counting
    size_t m_index_tuples;
    double m_heap_tuples;
    bool m_is_ustore;
    size_t m_estimated_rows;

    static void ScanOperatorCallBack(Relation index, HeapTuple hup, Datum *values,
        const bool *isnull, bool tupleIsAlive, void *state)
    {
        if (isnull[0]) {
            return;
        }
        ScanOperatorAndContext* opr_cxt = static_cast<ScanOperatorAndContext*>(state);
        opr_cxt->get_source()->m_index_tuples++;
        void *opr_context = opr_cxt->get_context();

        // Detoast the vector value
        Datum detoasted = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
        annlite::NewVector& vector = *(annlite::NewVector*) DatumGetPointer(detoasted);
        size_t dim = vector.dim;
        size_t position = opr_cxt->get_source()->m_index_tuples - 1;

        // Build IndexTuple from the index relation's tuple descriptor
        // The index tuple stores actual vector value; t_tid points to heap tuple
        bool fake_isnull[1] = {true};
        TupleDesc tupDesc = RelationGetDescr(index);
        TupleDesc itupDesc = CreateTemplateTupleDesc(tupDesc->natts, tupDesc->tdhasoid, tupDesc->td_tam_ops);
        TupleDescInitEntry(itupDesc, (AttrNumber)1, NULL,
                           TupleDescAttr(tupDesc, 0)->atttypid,
                           TupleDescAttr(tupDesc, 0)->atttypmod, 0);
        TupleDescInitEntryCollation(itupDesc, (AttrNumber)1,
                                    TupleDescAttr(tupDesc, 0)->attcollation);
        
        IndexTuple itup = index_form_tuple(itupDesc, values, &fake_isnull[0]);
        itup->t_tid = hup->t_self;

        // Set transaction info: build insert uses FROZEN_XACT_ID (frozen = always committed)
        VectorIndexXidData vxid;
        vector_set_xmin_xmax(&vxid, VECTOR_BUILD_INDEX_INSERT, 0);

        // Wrap IndexTuple + transaction info into GraphValueTypeV3 (binary-compatible with Value64)
        GraphValueTypeV3 value(itup, vxid);

        // Call the scan operator with the vector data and the trans_tid
        if (opr_cxt->has_position_callback()) {
            ScanOperatorWithPosition opr = opr_cxt->get_scanop_with_pos();
            opr(opr_context, &vector.x[0], dim, *(annlite::Value64*)&value, position);
        } else if (opr_cxt->has_scalar_callback()) {
            ScanOperatorWithScalar opr = opr_cxt->get_scanop_with_scalar();
            opr(opr_context, &vector.x[0], dim, *(annlite::Value64*)&value, nullptr, 0);
        } else {
            ScanOperator opr = opr_cxt->get_scanop();
            opr(opr_context, &vector.x[0], dim, *(annlite::Value64*)&value);
        }

        // Cleanup
        pfree(itup);
        pfree(itupDesc);

        if (DatumGetPointer(detoasted) != DatumGetPointer(values[0])) {
            pfree(DatumGetPointer(detoasted));
        }
    }
};

}  // namespace gs_vector

#endif
