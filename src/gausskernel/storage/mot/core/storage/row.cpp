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
 * row.cpp
 *    The Row class holds all that is required to manage an in-memory row in a table.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/row.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <limits>

#include "global.h"
#include "row.h"
#include "table.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(Row, Storage);

Row::Row(Table* hostTable)
    : m_rowCSN(INVALID_CSN),
      m_next(nullptr),
      m_table(hostTable),
      m_rowId(INVALID_ROW_ID),
      m_keyType(KeyType::EMPTY_KEY),
      m_rowType(RowType::ROW)
{}

Row::Row(const Row& src)
    : m_rowCSN(src.m_rowCSN),
      m_table(src.m_table),
      m_pSentinel(src.m_pSentinel),
      m_rowId(src.m_rowId),
      m_keyType(src.m_keyType),
      m_rowType(src.m_rowType)

{
    errno_t erc = memcpy_s(this->m_data, this->GetTupleSize(), src.m_data, src.GetTupleSize());
    securec_check(erc, "\0", "\0");
}

Row::Row(const Row& src, Table* tab)
    : m_rowCSN(src.m_rowCSN),
      m_table(tab),
      m_pSentinel(src.m_pSentinel),
      m_rowId(src.m_rowId),
      m_keyType(src.m_keyType),
      m_rowType(src.m_rowType)
{
    if (likely(tab == src.m_table)) {
        errno_t erc = memcpy_s(this->m_data, this->GetTupleSize(), src.m_data, src.GetTupleSize());
        securec_check(erc, "\0", "\0");
    } else {
        CopyVersion(src, tab->m_columns, tab->m_fieldCnt, src.m_table->m_columns, src.m_table->m_fieldCnt);
    }
}

void Row::CopyRowZero(const Row* src, Table* txnTable)
{
    CopyHeader(*src);
    SetTable(txnTable);
    CopyVersion(
        *src, GetTable()->m_columns, GetTable()->m_fieldCnt, src->GetTable()->m_columns, src->GetTable()->m_fieldCnt);
}

void Row::CopyHeader(const Row& src, RowType rowType)
{
    m_rowCSN = src.m_rowCSN;
    m_table = src.m_table;
    m_pSentinel = src.m_pSentinel;
    m_rowId = src.m_rowId;
    m_keyType = src.m_keyType;
    m_rowType = rowType;
}
Row::Row(const Row& src, Column** newCols, uint32_t newColCnt, Column** oldCols, uint32_t oldColCnt)
    : m_rowCSN(src.m_rowCSN),
      m_next(src.m_next),
      m_table(src.m_table),
      m_pSentinel(src.m_pSentinel),
      m_rowId(src.m_rowId),
      m_keyType(src.m_keyType),
      m_rowType(src.m_rowType)
{
    CopyVersion(src, newCols, newColCnt, oldCols, oldColCnt);
}

Row::Row(const Row& src, bool nullBitsChanged, size_t srcBitSize, size_t newBitSize, size_t dataSize, Column* col)
    : m_rowCSN(src.m_rowCSN),
      m_next(src.m_next),
      m_table(src.m_table),
      m_pSentinel(src.m_pSentinel),
      m_rowId(src.m_rowId),
      m_keyType(src.m_keyType),
      m_rowType(src.m_rowType)
{
    if (!nullBitsChanged) {
        errno_t erc = memcpy_s(this->m_data, dataSize, src.m_data, dataSize);
        securec_check(erc, "\0", "\0");
    } else {
        errno_t erc = memcpy_s(m_data, newBitSize, src.m_data, srcBitSize);
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(m_data + newBitSize, dataSize - srcBitSize, src.m_data + srcBitSize, dataSize - srcBitSize);
        securec_check(erc, "\0", "\0");
    }
    if (col != nullptr) {
        if (col->m_hasDefault) {
            // pack from default value cannot fail
            (void)col->Pack(m_data, col->m_defValue, col->m_defSize);
            BITMAP_SET(m_data, col->m_id - 1);
        } else {
            BITMAP_CLEAR(m_data, col->m_id - 1);
        }
    }
}

void Row::CopyVersion(const Row& src, Column** newCols, uint32_t newColCnt, Column** oldCols, uint32_t oldColCnt)
{
    const uint8_t* oldData = src.GetData();
    uint8_t* newBits = m_data + newCols[0]->m_offset;
    uint32_t i = 0;
    errno_t erc;
    // copy old data
    for (; i < oldColCnt; i++) {
        if (!newCols[i]->m_isDropped) {
            erc = memcpy_s(
                m_data + newCols[i]->m_offset, newCols[i]->m_size, oldData + oldCols[i]->m_offset, oldCols[i]->m_size);
            securec_check(erc, "\0", "\0");
        } else {
            BITMAP_CLEAR(newBits, newCols[i]->m_id - 1);
        }
    }
    // set new columns data
    for (; i < newColCnt; i++) {
        if (newCols[i]->m_hasDefault) {
            // pack from default value cannot fail
            (void)newCols[i]->Pack(m_data, newCols[i]->m_defValue, newCols[i]->m_defSize);
            BITMAP_SET(newBits, newCols[i]->m_id - 1);
        } else {
            BITMAP_CLEAR(newBits, newCols[i]->m_id - 1);
        }
    }
}

void Row::SetValueVariable(int id, const void* ptr, uint32_t size)
{
    const uint64_t fieldSize = m_table->GetFieldSize(id);
    MOT_ASSERT(size < fieldSize);
    errno_t erc = memcpy_s(&m_data[fieldSize], fieldSize, ptr, size);
    securec_check(erc, "\0", "\0");
}

void Row::CopyOpt(const Row* src)
{
    CopyData(src->GetData(), src->GetTupleSize());
}

Row* Row::CreateCopy()
{
    Row* row = m_table->CreateNewRow();
    if (row) {
        row->DeepCopy(this);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "N/A", "Failed to create row copy");
    }
    return row;
}

void Row::Print()
{
    Row* row = this;
    while (row) {
        MOT_LOG_INFO("%s: CSN = %lu rowID = %lu",
            row->IsRowDeleted() ? "TOMBSTONE" : "ROW",
            row->GetCommitSequenceNumber(),
            row->GetRowId());
        row = row->GetNextVersion();
        MOT_LOG_INFO("|");
        MOT_LOG_INFO("V");
    }
    MOT_LOG_INFO("NULL");
}
}  // namespace MOT
