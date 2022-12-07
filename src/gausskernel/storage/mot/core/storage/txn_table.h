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
 * txn_table.h
 *    Wrapper class around Table class to handle DDL operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/txn_table.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TXN_TABLE_H
#define TXN_TABLE_H

#include "table.h"

namespace MOT {
class DDLAlterTableAddDropColumn {
public:
    DDLAlterTableAddDropColumn() : m_table(nullptr), m_column(nullptr)
    {}
    ~DDLAlterTableAddDropColumn()
    {
        m_table = nullptr;
        m_column = nullptr;
    }

    TxnTable* m_table;
    Column* m_column;
};

class DDLAlterTableRenameColumn {
public:
    DDLAlterTableRenameColumn() : m_table(nullptr)
    {}

    ~DDLAlterTableRenameColumn()
    {
        m_table = nullptr;
    }

    TxnTable* m_table;
    char m_oldname[Column::MAX_COLUMN_NAME_LEN];
    char m_newname[Column::MAX_COLUMN_NAME_LEN];
};

class alignas(CL_SIZE) TxnTable : public Table {
    friend Row;

public:
    explicit TxnTable(Table* table)
        : m_origTab(table),
          m_origMetadataVer(table->m_metadataVer),
          m_tooManyIndexes(false),
          m_hasColumnChanges(false),
          m_hasColumnRename(false),
          m_hasIndexChanges(false),
          m_hasOnlyAddIndex(true),  // the default is true to differentiate first drop index
          m_hasTruncate(false),
          m_isDropped(false),
          m_isNew(false),
          m_isLocked(false),
          m_reservedChunks(0)
    {
        m_deserialized = table->m_deserialized;
        m_fieldCnt = table->m_fieldCnt;
        m_fixedLengthRows = table->m_fixedLengthRows;
        m_longTableName = table->m_longTableName;
        m_maxFields = table->m_maxFields;
        m_numIndexes = table->m_numIndexes;
        m_primaryIndex = table->m_primaryIndex;
        m_rowCount = table->m_rowCount;
        m_rowPool = table->m_rowPool;
        m_tombStonePool = table->m_tombStonePool;
        m_tableExId = table->m_tableExId;
        m_tableId = table->m_tableId;
        m_tableName = table->m_tableName;
        m_tupleSize = table->m_tupleSize;
    }

    ~TxnTable() override;
    bool IsTxnTable() override
    {
        return true;
    }
    Table* GetOrigTable() override
    {
        return m_origTab;
    }
    void SetHasColumnChanges()
    {
        m_hasColumnChanges = true;
        m_hasOnlyAddIndex = false;
    }
    void SetHasColumnRename()
    {
        m_hasColumnRename = true;
        m_hasOnlyAddIndex = false;
    }
    void SetHasIndexChanges(bool isDrop)
    {
        if (isDrop && m_hasOnlyAddIndex) {
            m_hasOnlyAddIndex = false;
        }
        m_hasIndexChanges = true;
    }
    void SetHasTruncate()
    {
        m_hasTruncate = true;
        m_hasIndexChanges = true;
        m_hasOnlyAddIndex = false;
        (void)MemBufferUnreserveGlobal(m_reservedChunks);
        m_reservedChunks = 0;
    }
    bool IsHasTruncate() const
    {
        return m_hasTruncate;
    }
    bool GetHasColumnChanges() const override
    {
        return m_hasColumnChanges;
    }
    bool IsDropped() const
    {
        return m_isDropped;
    }
    void SetIsDropped()
    {
        m_isDropped = true;
    }
    void SetIsNew()
    {
        m_isNew = true;
    }
    bool GetIsNew() const
    {
        return m_isNew;
    }

    /**
     * @brief Remove Index from table meta data.
     * @param index The index to use.
     * @return void.
     */
    void RemoveSecondaryIndexFromMetaData(MOT::Index* index)
    {
        if (!index->IsPrimaryKey()) {
            uint16_t rmIx = 0;
            for (uint16_t i = 1; i < m_numIndexes; i++) {
                if (m_indexes[i] == index) {
                    rmIx = i;
                    break;
                }
            }

            // prevent removing primary by mistake
            if (rmIx > 0) {
                DecIndexColumnUsage(index);
                m_numIndexes--;
                for (uint16_t i = rmIx; i < m_numIndexes; i++) {
                    m_indexes[i] = m_indexes[i + 1];
                }

                (void)m_secondaryIndexes.erase(index->GetName());
                m_indexes[m_numIndexes] = nullptr;
            }
        }
    }

    bool InitTxnTable();
    bool InitRowPool(bool local) override;
    bool InitTombStonePool(bool local) override;
    void ApplyDDLChanges(TxnManager* txn);
    void RollbackDDLChanges(TxnManager* txn);
    RC ValidateDDLChanges(TxnManager* txn);
    RC AlterAddColumn(TxnManager* txn, Column* newColumn);
    RC AlterDropColumn(TxnManager* txn, Column* col);
    RC AlterRenameColumn(TxnManager* txn, Column* col, char* newname, DDLAlterTableRenameColumn* alter);
    void ApplyAddIndexFromOtherTxn();

private:
    RC AlterAddConvertRow(Row*& oldRow, Column** newColumns, MOT::ObjAllocInterface* newRowPool, bool nullBitsChanged,
        uint64_t oldNullBytesSize, rowAddrMap_t& oldToNewMap);
    RC AlterDropConvertRow(Row*& oldRow, MOT::ObjAllocInterface* newRowPool, Column* col, size_t offset1,
        size_t offset2, size_t len2, rowAddrMap_t& oldToNewMap);
    void AlterConvertSecondaryTxnData(rowAddrMap_t& oldToNewMap, TxnAccess* txnAc);
    RC AlterAddConvertTxnData(TxnManager* txn, Column** newColumns, MOT::ObjAllocInterface* newRowPool,
        uint32_t newTupleSize, bool nullBitsChanged);
    RC AlterDropConvertTxnData(TxnManager* txn, Column* col, MOT::ObjAllocInterface* newRowPool, uint32_t newTupleSize);
    void ApplyDDLIndexChanges(TxnManager* txn);
    void AlterConvertGlobalData(TxnManager* txn);
    RC AlterConvertGlobalRow(Row* oldRow, acRowAddrMap_t& oldToNewMap);
    RC AlterReserveMem(TxnManager* txn, uint32_t newTupleSize, Column* newColumn);

    Table* m_origTab;
    uint64_t m_origMetadataVer;
    bool m_tooManyIndexes;
    bool m_hasColumnChanges;
    bool m_hasColumnRename;
    bool m_hasIndexChanges;
    bool m_hasOnlyAddIndex;
    bool m_hasTruncate;
    bool m_isDropped;
    bool m_isNew;
    bool m_isLocked;
    size_t m_reservedChunks;
};
}  // namespace MOT

#endif /* TXN_TABLE_H */
