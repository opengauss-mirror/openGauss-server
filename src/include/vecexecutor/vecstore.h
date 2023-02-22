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
 * vecstore.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecstore.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECSTORE_H
#define VECSTORE_H

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "storage/buf/buffile.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "utils/datum.h"
#include "vecexecutor/vectorbatch.h"
#include "workload/workload.h"

/* BatchStore is an opaque type whose details are not known outside
 * batchstore.cpp.
 * If the first sort column is text or numeric type, allocate one
 * more Datum to store the prefixal data for fast compare.
 */

typedef struct MultiColumns {
    Datum* m_values;
    uint8* m_nulls;

    int idx;
    int size;

    int GetLen(int colNum);

} MultiColumns;

/*
 * The data of those column to be sorted or unsorted
 */
typedef struct MultiColumnsData {

    MultiColumns* m_memValues;

    /*
     * The number of columns
     */
    int m_colNum;

    /*
     * The capacity of storing row in memory
     */
    int m_capacity;

    /*
     * The current row number in memory
     */
    int m_memRowNum;

    /*
     * Initialize variables
     */
    void Init(int capacity);

    bool HasFreeSlot();

    void PutValue(MultiColumns val);

    inline MultiColumns* GetValue(int row);

} MultiColumnsData;

class VecStore : public BaseObject {
public:
    char* GetData(int colNum, MultiColumns* multiColumn);
    void SetData(char* dataPtr, int size, MultiColumns* multiColumn);
    void InitColInfo(VectorBatch* batch);

    bool HasFreeSlot();

    void PutValue(MultiColumns multiColumn);

    void UseMem(int64 size);

    void FreeMem(int64 size);

    bool LackMem();

    bool GrowMemValueSlots(char* opname, int planid, MemoryContext context);

    void AppendBatch(VectorBatch* batch, MultiColumns& multiColumn, int curRow, bool isfree = true);

    /*
     * abbreSortOptimize used to mark whether allocate one more Datum for
     * fast compare of two data(text or numeric type)
     */
    template <bool abbreSortOptimize>
    MultiColumns CopyMultiColumn(VectorBatch* batch, int row)
    {
        MultiColumns multiColValue;
        m_colNum = batch->m_cols;
        if (abbreSortOptimize)
            multiColValue.m_values = (ScalarValue*)palloc((batch->m_cols + 1) * sizeof(Datum));
        else
            multiColValue.m_values = (ScalarValue*)palloc(batch->m_cols * sizeof(Datum));
        UseMem(GetMemoryChunkSpace(multiColValue.m_values));

        multiColValue.m_nulls = (uint8*)palloc(batch->m_cols * sizeof(uint8));
        UseMem(GetMemoryChunkSpace(multiColValue.m_nulls));
        if (m_addWidth)
            m_colWidth += batch->m_cols * sizeof(Datum) + batch->m_cols * sizeof(uint8);
        multiColValue.size = 0;
        uint8 flag;

        /*
         * When faced with NULL, we just skip, in the following produce, we do the same thing.
         */
        for (int col = 0; col < batch->m_cols; ++col) {
            Form_pg_attribute attr = &tupDesc->attrs[col];

            flag = batch->m_arr[col].m_flag[row];
            multiColValue.m_nulls[col] = flag;
            multiColValue.size += sizeof(uint8);

            if (!IS_NULL(flag)) {
                if (NeedDecode(col)) {
                    ScalarValue val = batch->m_arr[col].m_vals[row];
                    Datum v = ScalarVector::Decode(val);
                    int typlen = 0;
                    if (batch->m_arr[col].m_desc.typeId == NAMEOID) {
                        typlen = datumGetSize(v, false, -2);
                    } else {
                        typlen = VARSIZE_ANY(v);
                    }
                    multiColValue.m_values[col] = datumCopy(v, attr->attbyval, typlen);
                    if (m_addWidth)
                        m_colWidth += typlen;
                    char* p = DatumGetPointer(multiColValue.m_values[col]);
                    UseMem(GetMemoryChunkSpace(p));
                    multiColValue.size += datumGetSize(v, attr->attbyval, typlen);
                } else {
                    multiColValue.m_values[col] = PointerGetDatum(batch->m_arr[col].m_vals[row]);
                    multiColValue.size += sizeof(Datum);
                }
            }
        }
        return multiColValue;
    }

    FORCE_INLINE bool NeedDecode(int col)
    {
        if (m_colInfo[col].m_desc.encoded)
            return true;
        return false;
    }
    void FreeMultiColumn(MultiColumns* pMultiColumn);

public:
    MultiColumnsData m_storeColumns;

    /*
     * remaining memory allowed, in bytes
     */
    int64 m_availMem;
    /*
     * total memory allowed, in bytes
     */
    int64 m_allowedMem;
    ScalarVector* m_colInfo;
    int m_colNum;
    TupleDesc tupDesc;
    int64 m_lastFetchCursor;
    int64 m_curFetchCursor;

    int64 m_colWidth;
    bool m_addWidth;
    bool m_sysBusy;
    int64 m_maxMem;
    int m_spreadNum;
    int m_planId;
    int m_dop;
};

inline bool MultiColumnsData::HasFreeSlot()
{
    return m_memRowNum < m_capacity - 1;
}

inline void MultiColumnsData::PutValue(MultiColumns val)
{
    m_memValues[m_memRowNum] = val;
    ++m_memRowNum;
}

inline int MultiColumns::GetLen(int colNum)
{
    return size;
}

inline MultiColumns* MultiColumnsData::GetValue(int row)
{
    return m_memValues + row;
}

inline void VecStore::UseMem(int64 size)
{
    m_availMem -= size;
}

inline void VecStore::FreeMem(int64 size)
{
    m_availMem += size;
}

inline bool VecStore::LackMem()
{
    int64 usedMem = m_allowedMem - m_availMem;

    if (m_availMem < 0 || gs_sysmemory_busy(usedMem * m_dop, true))
        return true;

    return false;
}

inline bool VecStore::HasFreeSlot()
{
    return m_storeColumns.HasFreeSlot();
}

inline bool VecStore::GrowMemValueSlots(char* opname, int planid, MemoryContext context)
{
    double grow_ratio =
        Min(DEFAULT_GROW_RATIO, ((double)(MaxAllocSize / sizeof(MultiColumns))) / (double)m_storeColumns.m_capacity);
    double unspread_ratio = grow_ratio;
    bool need_spread = false;
    int64 mem_used = m_allowedMem - m_availMem;

    if (m_availMem < mem_used)
        unspread_ratio = Min(unspread_ratio, (double)m_allowedMem / mem_used);

    /* No grow of rows, so return */
    if (m_storeColumns.m_capacity * unspread_ratio <= m_storeColumns.m_capacity) {
        if (m_maxMem < m_allowedMem)
            return false;
        else
            need_spread = true;
    } else
        grow_ratio = unspread_ratio;

    if (gs_sysmemory_busy((m_allowedMem - m_availMem) * m_dop, true)) {
        m_sysBusy = true;
        AllocSet sortContext = (AllocSet)context;
        sortContext->maxSpaceSize = m_allowedMem - m_availMem;
        m_allowedMem = m_allowedMem - m_availMem;
        MEMCTL_LOG(LOG,
            "%s(%d) early spilled, workmem: %ldKB, freemem: %ldKB.",
            opname,
            planid,
            m_allowedMem / 1024L,
            m_availMem / 1024L);
        pgstat_add_warning_early_spill();
        return false;
    }

    /* For concurrent case, we don't allow sort/materialize to spread a lot */
    if (need_spread && (m_spreadNum < 2 || g_instance.wlm_cxt->stat_manager.comp_count == 1)) {
        if (m_availMem < 0)
            m_allowedMem -= m_availMem;
        int64 spreadMem = Min(Min(dywlm_client_get_memory() * 1024L, m_allowedMem), m_maxMem - m_allowedMem);
        if (spreadMem <= m_allowedMem * MEM_AUTO_SPREAD_MIN_RATIO) {
            MEMCTL_LOG(LOG,
                "%s(%d) auto mem spread %ldKB failed, and work mem is %ldKB.",
                opname,
                planid,
                spreadMem / 1024L,
                m_allowedMem / 1024L);
            if (m_spreadNum > 0) {
                pgstat_add_warning_spill_on_memory_spread();
            }
            return false;
        }
        grow_ratio = Min(grow_ratio, 1 + (double)spreadMem / m_allowedMem);
        m_allowedMem += spreadMem;
        m_availMem = spreadMem;
        AllocSet sortContext = (AllocSet)context;
        sortContext->maxSpaceSize += spreadMem;
        m_spreadNum++;
        MEMCTL_LOG(DEBUG2,
            "%s(%d) auto mem spread %ldKB succeed, and work mem is %ldKB.",
            opname,
            planid,
            spreadMem / 1024L,
            m_allowedMem / 1024L);
    }

    /* if there's no more space after grow, then fail */
    if (m_storeColumns.m_capacity * grow_ratio <= m_storeColumns.m_capacity) {
        MEMCTL_LOG(LOG, "%s(%d) mem limit reached.", opname, planid);
        return false;
    }

    if (!gs_sysmemory_avail(mem_used * (grow_ratio - 1))) {
        MEMCTL_LOG(LOG,
            "%s(%d) mem lack, workmem: %ldKB, freemem: %ldKB,"
            "usedmem: %ldKB, grow ratio: %.2f.",
            opname,
            planid,
            m_allowedMem / 1024L,
            m_availMem / 1024L,
            mem_used / 1024L,
            grow_ratio);
        return false;
    }

    FreeMem(GetMemoryChunkSpace(m_storeColumns.m_memValues));

    m_storeColumns.m_capacity *= grow_ratio;
    m_storeColumns.m_memValues =
        (MultiColumns*)repalloc(m_storeColumns.m_memValues, m_storeColumns.m_capacity * sizeof(MultiColumns));

    UseMem(GetMemoryChunkSpace(m_storeColumns.m_memValues));

    if (m_availMem < 0)
        return false;

    return true;
}

inline void VecStore::PutValue(MultiColumns multiColValue)
{
    m_storeColumns.PutValue(multiColValue);
}

#endif /* VECSTORE_H */
