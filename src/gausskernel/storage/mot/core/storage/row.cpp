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

Row::Row(Table* hostTable) : m_rowHeader(), m_table(hostTable), m_rowId(INVALID_ROW_ID), m_keyType(KeyType::EMPTY_KEY)
{}

Row::Row(const Row& src)
    : m_rowHeader(src.m_rowHeader),
      m_table(src.m_table),
      m_surrogateKey(src.m_surrogateKey),
      m_pSentinel(src.m_pSentinel),
      m_rowId(src.m_rowId),
      m_keyType(src.m_keyType),
      m_twoPhaseRecoverMode(src.m_twoPhaseRecoverMode)
{
    errno_t erc = memcpy_s(this->m_data, this->GetTupleSize(), src.m_data, src.GetTupleSize());
    securec_check(erc, "\0", "\0");
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

RC Row::GetRow(AccessType type, TxnAccess* txn, Row* row, TransactionId& lastTid) const
{
    MOT_ASSERT(type != INS);
    row->m_table = GetTable();
    return this->m_rowHeader.GetLocalCopy(txn, type, row, this, lastTid);
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
}  // namespace MOT
