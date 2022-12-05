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
 * table.h
 *    The Table class holds all that is required to manage an in-memory table in the database.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/table.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_TABLE_H
#define MOT_TABLE_H

#include <atomic>
#include <map>
#include <string>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <unordered_map>
#include "global.h"
#include "surrogate_key_generator.h"
#include "utilities.h"
#include "index.h"
#include "index_factory.h"
#include "column.h"
#include "serializable.h"
#include "object_pool.h"
#include "mm_gc_manager.h"
#include "txn_ddl_access.h"

namespace MOT {
class Row;
class TxnManager;
class TxnInsertAction;
class RecoveryManager;
class Access;
class TxnTable;

struct rowhashing_func {
    uint64_t operator()(const Row* key) const
    {
        return std::hash<uint64_t>()((uint64_t)key);
    }
};

struct rowkey_equal_fn {
    bool operator()(const Row* t1, const Row* t2) const
    {
        return ((uint64_t)t1 == (uint64_t)t2);
    }
};

using rowAddrMap_t = std::unordered_map<Row*, Row*, struct rowhashing_func, struct rowkey_equal_fn>;
using acRowAddrMap_t = std::unordered_multimap<Row*, Access*, struct rowhashing_func, struct rowkey_equal_fn>;

enum MetadataProtoVersion : uint32_t {
    METADATA_VER_INITIAL = 1,
    METADATA_VER_ALTER_COLUMN = 2,  // Add/Drop column changes
    METADATA_VER_LOW_RTO = 3,
    METADATA_VER_IDX_KEY_LEN = 4,
    METADATA_VER_IDX_COL_UPD = 5,
    METADATA_VER_MVCC = 6,
    METADATA_VER_CURR = METADATA_VER_MVCC
};

/**
 * @class Table
 * @brief The Table class holds all that is required to manage an in-memory table in the database.
 *
 * A table consists of the following components:
 *
 * - Columns
 * - Primary index
 * - Optional secondary indices
 */
class alignas(CL_SIZE) Table : public Serializable {
    // allow privileged access
    friend TxnManager;
    friend TxnInsertAction;
    friend MOT::Index;
    friend RecoveryManager;
    friend TxnDDLAccess;
    friend TxnTable;
    friend Row;

public:
    static void deleteTablePtr(Table* t)
    {
        delete t;
    }

    /** @brief Default constructor. */
    Table()
        : m_rowPool(nullptr),
          m_tombStonePool(nullptr),
          m_primaryIndex(nullptr),
          m_fieldCnt(0),
          m_tupleSize(0),
          m_maxFields(0),
          m_filler1(0),
          m_filler2(0),
          m_tableExId(0),
          m_deserialized(false),
          m_filler4(0),
          m_rowCount(0)
    {}

    /** @brief Destructor. */
    ~Table() override;

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    Table(const Table&) = delete;

    Table(Table&&) = delete;

    Table& operator=(const Table&) = delete;

    Table& operator=(Table&&) = delete;
    /** @endcond */

    /** @typedef internal table identifier. */
    typedef uint32_t InternalTableId;

    /** @typedef external table identifier. */
    typedef uint64_t ExternalTableId;

    /**
     * @brief Initializes the table.
     * @param tableName The name of the table.
     * @param longName The long representation of the table's name.
     * @param fieldCnt The number of columns in the table (must be declared in advance).
     * @param tableExtId Optional paramter specifies external table id
     * @return Boolean value denoting success or failure.
     */
    bool Init(const char* tableName, const char* longName, unsigned int fieldCnt, uint64_t tableExId = 0);

    /**
     * @brief Initializes row_pool object pool
     * @return True if initialization succeeded, otherwise false.
     */
    virtual bool InitRowPool(bool local = false);

    virtual bool InitTombStonePool(bool local = false);

    inline uint32_t GetRowSizeFromPool() const
    {
        return m_rowPool->m_size;
    }

    inline uint32_t GetTombStoneSizeFromPool() const
    {
        return m_tombStonePool->m_size;
    }
    /**
     * @brief Clears object pool thread level cache
     */
    void ClearThreadMemoryCache();

    /**
     * @brief Retrieves the name of the table.
     * @return The name of the table.
     */
    inline const string& GetTableName() const
    {
        return m_tableName;
    };

    inline const string& GetLongTableName() const
    {
        return m_longTableName;
    };

    /**
     * @brief Retrieves the primary index of the table.
     * @return The index object.
     */
    inline MOT::Index* GetPrimaryIndex()
    {
        return m_primaryIndex;
    }

    /**
     * @brief Retrieves a secondary index of the table.
     * @param indexName The name of the index to retrieve.
     * @return The secondary index with indicating whether it has unique keys or not.
     */
    inline MOT::Index* GetSecondaryIndex(const string& indexName)
    {
        MOT::Index* result = nullptr;
        SecondaryIndexMap::iterator itr = m_secondaryIndexes.find(indexName);
        if (MOT_EXPECT_TRUE(itr != m_secondaryIndexes.end())) {
            result = itr->second;
        }
        return result;
    }

    inline MOT::Index* GetSecondaryIndex(uint16_t ix)
    {
        return (MOT::Index*)m_indexes[ix];
    }

    inline MOT::Index* GetIndex(uint16_t ix)
    {
        return m_indexes[ix];
    }

    /**
     * @brief Sets the primary index for the table.
     * @param index The index to set.
     */
    void SetPrimaryIndex(MOT::Index* index);

    /**
     * @brief Sets the primary index for the table (replaces previously created fake primary.
     * @param index The index to set.
     * @param txn The current transaction.
     * @param tid Current thread id
     */
    bool UpdatePrimaryIndex(MOT::Index* index, TxnManager* txn, uint32_t tid);

    /**
     * @brief Adds a secondary index to the table.
     * @param indexName The name of the index to add.
     * @param index The secondary index.
     * @param txn The txn manager object.
     * @param tid The identifier of the requesting process/thread.
     * @return Boolean value denoting success or failure.
     */
    bool AddSecondaryIndex(const string& indexName, MOT::Index* index, TxnManager* txn, uint32_t tid);

    /**
     * @brief Index a table using a secondary index.
     * @param index The index to use.
     * @param txn The txn manager object.
     * @return Boolean value denoting success or failure.
     */
    bool CreateSecondaryIndexData(Index* index, TxnManager* txn);

    /**
     * @brief Removes a secondary index from the table.
     * @param name The name of the index to remove.
     * @param txn The txn manager object.
     * @return RC value denoting the operation's completion status.
     */
    RC RemoveSecondaryIndex(MOT::Index* index, TxnManager* txn);

    /**
     * @brief Add Index to table meta data.
     * @param index The index to use.
     * @return void.
     */
    void AddSecondaryIndexToMetaData(MOT::Index* index)
    {
        if (!index->IsPrimaryKey()) {
            IncIndexColumnUsage(index);
            m_secondaryIndexes[index->GetName()] = index;
            m_indexes[m_numIndexes] = index;
            ++m_numIndexes;
        }
    }

    /**
     * @brief Deletes an index.
     * @param the index to remove.
     * @return void.
     */
    void DeleteIndex(MOT::Index* index);

    /**
     * @brief Checks if table contains data.
     * @param thread id.
     * @return true if table is empty, false otherwise.
     */
    bool IsTableEmpty(uint32_t tid);

    /**
     * @brief Truncates the table.
     * @param txn The txn manager object.
     */
    void Truncate(TxnManager* txn);

    /**
     * @brief Performs a compact operation on the table.
     * @param txn The txn manager object.
     */
    void Compact(TxnManager* txn);

    /**
     * @brief Count number of absent sentinels in a table
     * @param none
     */
#ifdef MOT_DEBUG
    void CountAbsents()
    {
        uint64_t absentCounter = 0;
        MOT_LOG_INFO("Testing table %s", GetTableName().c_str());
        for (uint16_t i = 0; i < m_numIndexes; i++) {
            Index* index = GetIndex(i);
            IndexIterator* it = index->Begin(0);
            while (it->IsValid()) {
                Sentinel* Sentinel = it->GetPrimarySentinel();
                if (Sentinel->IsDirty()) {
                    absentCounter++;
                }
                it->Next();
            }
            MOT_LOG_INFO("Found %lu Absents in index %s", absentCounter, index->GetName().c_str());
            absentCounter = 0;
        }
    }

#endif

    bool IsTableSingleVersion();

    void ClearRowCache()
    {
        m_rowPool->ClearFreeCache();
        m_tombStonePool->ClearFreeCache();
        for (int i = 0; i < m_numIndexes; i++) {
            if (m_indexes[i] != nullptr) {
                m_indexes[i]->ClearFreeCache();
            }
        }
    }

    /**
     * @brief table drop hanldler.
     * @return void.
     */
    void DropImpl();

    /**
     * @brief Increases the column usage on an index.
     * @param index The index to perform on.
     */
    void IncIndexColumnUsage(MOT::Index* index);

    /**
     * @brief Decreases the column usage on an index.
     * @param index The index to perform on.
     */
    void DecIndexColumnUsage(MOT::Index* index);

    /**
     * @brief Retrieves an iterator to the first row in the primary index.
     * @param pid The identifier of the requesting process/thread.
     * @return The requested iterator.
     */
    inline IndexIterator* Begin(const uint32_t& pid, bool passive = false)
    {
        MOT_LOG_DEBUG("begin@table");
        return m_primaryIndex->Begin(pid, passive);
    }

    /**
     * @brief Finds a row by a key using the primary index.
     * @param key The key by which to search the row.
     * @param result The resulting row.
     * @param pid The logical identifier of the requesting thread.
     * @return Return code denoting success or failure reason.
     */
    inline RC FindRowByIndexId(MOT::Index* index, Key const* const& key, Sentinel*& result, const uint32_t& pid)
    {
        RC rc = RC_ERROR;
        result = nullptr;
        // read from index
        Sentinel* sent = index->IndexReadImpl(key, pid);

        // We report a row iff is non-absent
        if (sent) {
            result = sent;
            rc = RC_OK;
        }

        return rc;
    }

    /**
     * @brief Get the Sentinel from the index
     * @param key The key by which to search the row.
     * @param result The resulting row.
     * @param pid The logical identifier of the requesting thread.
     * @return Return code denoting success or failure reason.
     */
    inline RC FindRow(Key const* const& key, Sentinel*& result, const uint32_t& pid)
    {
        RC rc = RC_ERROR;
        result = nullptr;
        // read from index
        Sentinel* sent = m_primaryIndex->IndexReadImpl(key, pid);

        // We report a row iff is non-absent

        if (sent) {
            result = sent;
            rc = RC_OK;
        }

        return rc;
    }

    /**
     * @brief Queries whether the rows of the table have a fixed length.
     * @return Boolean value denoting fixed length rows or not.
     */
    inline bool GetFixedLengthRow(void) const
    {
        return m_fixedLengthRows;
    }

    /**
     * @brief Sets the fixed length row property for the table.
     * @param fixedLength Boolean value denoting whether the rows of the table have a fixed length
     * or not.
     */
    inline void SetFixedLengthRow(bool fixedLength)
    {
        m_fixedLengthRows = fixedLength;
    }

    /**
     * @brief returns unique ID of the table object.
     */
    inline uint32_t GetTableId() const
    {
        return m_tableId;
    }

    inline uint64_t GetTableExId() const
    {
        return m_tableExId;
    }

    /**
     * @brief Retrieves the length of the key in the primary index.
     * @return The primary index key length.
     */
    uint16_t GetLengthPrimaryKey() const
    {
        return m_primaryIndex->GetKeyLength();
    }

    /**
     * @brief Retrieves the length of the key in a secondary index.
     * @param index The secondary index.
     * @return The secondary index key length.
     */
    uint16_t GetLengthSecondaryKey(MOT::Index* index) const
    {
        return index->GetKeyLength();
    }

    /**
     * @brief Retrieves all previously stored length of the fields in an index.
     * @param key The ordinal number of the index.
     * @return The array of all field lengths.
     */
    uint16_t const* GetLengthKeyFields(const uint16_t& key) const
    {
        return m_indexes[key]->GetLengthKeyFields();
    }

    /**
     * @brief Queries the class of a secondary index.
     * @param idx The secondary index ordinal position.
     * @return The index class.
     */
    IndexingMethod GetIndexingMethod(const uint32_t& idx) const
    {
        return m_indexes[idx]->GetIndexingMethod();
    }

    uint16_t GetNumIndexes() const
    {
        return m_numIndexes;
    }

    /**
     * @brief Inserts a new row into transactional storage without validation.
     * Creating all secondary indexes that should point to it.
     * @param row. New row to be inserted
     * @param tid The logical identifier of the requesting thread.
     * @param k row's primary ket
     * @param skipSecIndex determines if secondaries should be added as well
     * @return Status of the operation.
     */
    RC InsertRowNonTransactional(Row* row, uint64_t tid, Key* k = NULL, bool skipSecIndex = false);

    /**
     * @brief Inserts a row into a newly created secondary index storage without validation.
     * @param tid The logical identifier of the requesting thread.
     * @return Status of the operation.
     */
    bool CreateSecondaryIndexDataNonTransactional(MOT::Index* index, uint32_t tid);

    /**
     * @brief Inserts a new row into transactional storage.
     * @param row. New row to be inserted
     * @param txn The txn manager object.
     * @return Status of the operation.
     */
    RC InsertRow(Row* row, TxnManager* txn);

    /**
     * @brief Create new row placeholder
     * @return The newly created row.
     */
    Row* CreateNewRow();

    Row* CreateTombStone();

    Row* CreateNewRowCopy(const Row* r, AccessType type);

    /**
     * @brief Releases a row's memory.
     * @param row. row to be deleted
     */
    void DestroyRow(Row* row);

    /**
     * @brief Create array of new row placeholders
     * @param numRows. Size of array to be filled
     * @param rows. Array to hold rows
     * @return The true or false
     */
    bool CreateMultipleRows(size_t numRows, Row* rows[]);

    /**
     * @brief Adds a column to the Table.
     *
     * The maximum number of columns that can be added to the
     * Table is restricted by the field count specified during Table initialization. @see
     * Table::init.
     *
     * @param col_name The name of the column to add.
     * @param size The size of the column in bytes.
     * @param type The type name of the column.
     * @return RC error code.
     */
    RC AddColumn(const char* col_name, uint64_t size, MOT_CATALOG_FIELD_TYPES type, bool isNotNull = false,
        unsigned int envelopeType = 0);

    /**
     * @brief Modifies the size of a column. This may be required for supporting ALTER TABLE.
     * @param id The id (ordinal number) of the column to be modified.
     * @param size The new size of the column to set.
     * @return Boolean value denoting success or failure.
     */
    bool ModifyColumnSize(const uint32_t& id, const uint64_t& size);

    /**
     * @brief Adds a column to the Table during alter table.
     *
     * The maximum number of columns that can be added to the
     * Table is restricted by the field count specified during Table initialization. @see
     * Table::init.
     *
     * @param col_name The name of the column to add.
     * @param size The size of the column in bytes.
     * @param type The type name of the column.
     * @return RC error code.
     */
    RC CreateColumn(Column*& newColumn, const char* col_name, uint64_t size, MOT_CATALOG_FIELD_TYPES type,
        bool isNotNull, unsigned int envelopeType, bool hasDefault, uintptr_t defValue, size_t defLen);
    /**
     * @brief Retrieves the size of a row in the table in bytes.
     * @return Row size in bytes.
     */
    inline uint32_t GetTupleSize() const
    {
        return m_tupleSize;
    };

    /**
     * @brief Retrieves the number of fields (columns) in each row in the table.
     * @return Number of fields in a row.
     */
    inline uint32_t GetFieldCount() const
    {
        return m_fieldCnt;
    }

    /**
     * @brief Retrieves the size in bytes of a field (column) specified by its id (ordinal number).
     * @param id The field (column) id
     * @return The field size in bytes.
     */
    inline uint64_t GetFieldSize(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->m_size;
    }

    inline uint64_t GetFieldKeySize(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->m_keySize;
    }

    /**
     * @brief Retrieves the offset in bytes of a field (column) specified by its id (ordinal number).
     * @param id The field (column) id
     * @return The field offset in bytes.
     */
    inline uint64_t GetFieldOffset(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->m_offset;
    }

    /**
     * @brief Retrieves the type name of a field (column) specified by its id (ordinal number).
     * @param id The field (column) id
     * @return The type name of the field.
     */
    inline MOT_CATALOG_FIELD_TYPES GetFieldType(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->m_type;
    }

    inline const char* GetFieldTypeStr(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->GetTypeStr();
    }

    /**
     * @brief Retrieves the name of a field (column) specified by its id (ordinal number).
     * @param id The field (column) id
     * @return The name of the field.
     */
    inline const char* GetFieldName(uint64_t id) const
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id]->m_name;
    }

    /**
     * @brief Retrieves the type name of a field (column) by its name.
     * @param name The name of the field.
     * @return The type name of the field.
     */
    inline MOT_CATALOG_FIELD_TYPES GetFieldType(const char* name) const
    {
        return GetFieldType(GetFieldId(name));
    };

    /**
     * @brief Retrieves the offset in bytes of a field (column) by its name.
     * @param name The name of the field.
     * @return The field offset in bytes.
     */
    inline uint64_t GetFieldOffset(const char* name) const
    {
        return GetFieldOffset(GetFieldId(name));
    };

    /**
     * @brief Retrieves the id (ordinal number) of a field (column) by its name.
     * @param name The name of the field.
     * @return The id of the field.
     */
    uint64_t GetFieldId(const char* name) const;

    Column* GetField(uint64_t id)
    {
        MOT_ASSERT(id < m_fieldCnt);
        return m_columns[id];
    }

    /**
     * @brief Prints the Table
     */
    void PrintSchema() const;

    /**
     * @brief Updates table row count
     * @param diff The number to change the row count (maybe negative)
     * @return void.
     */
    inline void UpdateRowCount(int32_t diff)
    {
        m_rowCount += diff;
    }

    /**
     * @brief Returns table row count
     */
    inline uint32_t GetRowCount() const
    {
        return m_rowCount;
    }

    /**
     * @brief Returns table size in memory
     */
    uint64_t GetTableSize(uint64_t& netTotal);

    /**
     * @brief Returns index size in memory
     */
    inline Index* GetIndexByExtId(uint64_t extId)
    {
        for (int i = 0; i < m_numIndexes; i++) {
            if (m_indexes[i]->GetExtId() == extId) {
                return m_indexes[i];
            }
        }

        return NULL;
    }

    inline Index* GetIndexByExtIdWithPos(uint64_t extId, int& pos)
    {
        pos = -1;
        for (int i = 0; i < m_numIndexes; i++) {
            if (m_indexes[i]->GetExtId() == extId) {
                pos = i;
                return m_indexes[i];
            }
        }

        return NULL;
    }

    /**
     * @brief takes a read lock on the table.
     */
    void RdLock()
    {
        (void)pthread_rwlock_rdlock(&m_rwLock);
    }

    /**
     * @brief tries to takes a write lock on the table.
     * @return True on success, False if the lock could not be acquired.
     */
    bool WrTryLock()
    {
        if (pthread_rwlock_trywrlock(&m_rwLock) != 0) {
            return false;
        }
        return true;
    }

    /**
     * @brief takes a write lock on the table.
     */
    void WrLock()
    {
        (void)pthread_rwlock_wrlock(&m_rwLock);
    }

    /**
     * @brief releases the table lock.
     */
    void Unlock()
    {
        (void)pthread_rwlock_unlock(&m_rwLock);
    }

    bool IsDeserialized() const
    {
        return m_deserialized;
    }

    void SetDeserialized(bool val)
    {
        m_deserialized = val;
    }

    PrimarySentinel* GCRemoveRow(GcQueue::DeleteVector* deletes, Row* tombstone, GC_OPERATION_TYPE gcOper);

    /**
     * @brief Creates dummy table with MAX_ROW_SIZE.
     * @return The new dummy table.
     */
    static Table* CreateDummyTable();

    /**
     * @struct CommonColumnMeta
     * @brief holds column metadata
     */
    struct CommonColumnMeta {
        char m_name[Column::MAX_COLUMN_NAME_LEN];

        uint64_t m_size;

        MOT_CATALOG_FIELD_TYPES m_type;

        bool m_isNotNull;

        unsigned int m_envelopeType;  // required for MOT JIT
        bool m_isDropped;
        bool m_hasDefault;
    };

    /**
     * @struct CommonIndexMeta
     * @brief holds index metadata
     */
    struct CommonIndexMeta {
        string m_name;

        bool m_fake;

        bool m_unique;

        IndexOrder m_indexOrder;

        IndexingMethod m_indexingMethod;

        uint64_t m_indexExtId;

        int16_t m_numKeyFields;

        uint32_t m_numTableFields;

        uint32_t m_keyLength;

        uint16_t m_lengthKeyFields[MAX_KEY_COLUMNS];

        int16_t m_columnKeyFields[MAX_KEY_COLUMNS];
    };

    /**
     * @brief returns the serialized size of a column
     * @param column the column to work on
     * @return Size_t the size
     */
    size_t SerializeItemSize(Column* column) const;

    /**
     * @brief serializes a column into a buffer
     * @param dataOut the output buffer
     * @param column the column to work on
     * @return Char* the buffer pointer.
     */
    char* SerializeItem(char* dataOut, Column* column) const;

    /**
     * @brief deserializes a column from a buffer
     * @param dataIn the input buffer
     * @param meta the metadata struct to fill.
     * @return Char* the buffer pointer.
     */
    char* DeserializeMeta(char* dataIn, CommonColumnMeta& meta, uint32_t metaVersion) const;

    /**
     * @brief returns the serialized size of an index
     * @param index the index to work on
     * @return Size_t the size
     */
    size_t SerializeItemSize(Index* index) const;

    /**
     * @brief serializes an index into a buffer
     * @param dataOut the output buffer
     * @param index the index to work on
     * @return Char* the buffer pointer.
     */
    char* SerializeItem(char* dataOut, Index* index) const;

    /**
     * @brief deserializes an index from a buffer
     * @param dataIn the input buffer
     * @param meta the metadata struct to fill.
     * @return Char* the buffer pointer.
     */
    char* DeserializeMeta(char* dataIn, CommonIndexMeta& meta, uint32_t metaVersion) const;

    /**
     * @brief creates and index from a metadata struct
     * @param meta the metadata struct to create from.
     * @param primary whether this is a primary index or not.
     * @param tid the thread identifier
     * @return RC error code.
     */
    RC CreateIndexFromMeta(CommonIndexMeta& meta, bool primary, uint32_t tid, uint32_t metaVersion,
        bool addToTable = true, Index** outIndex = nullptr);

    /**
     * @brief returns the serialized size of a table
     * @param Size_t the size
     */
    virtual size_t SerializeSize();

    /**
     * @brief serializes a table into a buffer
     * @param dataOut the output buffer
     */
    virtual void Serialize(char* dataOut);

    /**
     * @brief deserializes a table from a buffer
     * @param dataIn the input buffer
     */
    virtual void Deserialize(const char* dataIn);

    /**
     * @brief helper method to fetch names and ids from serialized data
     * @param dataIn the input buffer
     * @param intId the returned internal id.
     * @param extId  the returned external id.
     * @param name the returned table name.
     * @param longName the returned long table name.
     */
    static void DeserializeNameAndIds(
        const char* dataIn, uint32_t& metaVersion, uint32_t& intId, uint64_t& extId, string& name, string& longName);

    /**
     * @brief Gets the serialized size of a table excluding the indexes.
     * @return Serialized size.
     */
    size_t SerializeRedoSize();

    /**
     * @brief Serializes a table into a buffer for redo.
     *        Indexes are excluded, i.e., m_numIndexes will be 0.
     * @param dataOut The output buffer
     */
    void SerializeRedo(char* dataOut);

    virtual bool IsTxnTable()
    {
        return false;
    }
    virtual Table* GetOrigTable()
    {
        return this;
    }
    virtual bool GetHasColumnChanges() const
    {
        return false;
    }

private:
    inline MOT::ObjAllocInterface* GetRowPool()
    {
        return m_rowPool;
    }

    inline void ReplaceRowPool(MOT::ObjAllocInterface* rowPool)
    {
        ObjAllocInterface::FreeObjPool(&m_rowPool);
        m_rowPool = rowPool;
    }

    inline void FreeObjectPool(MOT::ObjAllocInterface* rowPool)
    {
        ObjAllocInterface::FreeObjPool(&rowPool);
    }

    void GCRemoveSecondaryUnique(GcQueue::DeleteVector* deletes, Row* tombstone, GC_OPERATION_TYPE gcOper,
        MOT::Index* ix, Sentinel* currSentinel, const Key* key);

    /** @var Global atomic table identifier. */
    static std::atomic<uint32_t> tableCounter;

    /** @var row_pool personal row allocator object pool */
    ObjAllocInterface* m_rowPool;

    /** @var row_pool personal row allocator object pool */
    ObjAllocInterface* m_tombStonePool;

    // we have only index-organized-tables (IOT) so this is the pointer to the index
    // representing the table
    /** @var The primary index holding all rows. */
    MOT::Index* m_primaryIndex;

    /** @var Number of fields in the table schema. */
    uint32_t m_fieldCnt;

    /** @var Size of raw tuple in bytes. */
    uint32_t m_tupleSize;

    /** @var Maximum number of fields in tuple. */
    uint32_t m_maxFields;

    /** @var The number of secondary indices in use. */
    uint16_t m_numIndexes = 0;

    uint16_t m_filler1;

    /** @var All columns. */
    Column** m_columns = NULL;

    /** @var Secondary index array. */
    MOT::Index** m_indexes = NULL;

    /** @var Current table unique identifier. */
    uint32_t m_tableId = tableCounter++;

    uint32_t m_filler2;

    uint64_t m_tableExId;

    /** @typedef Secondary index map (indexed by index name). */
    typedef std::map<string, MOT::Index*> SecondaryIndexMap;

    /** @var Secondary index map accessed by name. */
    SecondaryIndexMap m_secondaryIndexes;

    /** @var RW Lock that guards against deletion during checkpoint/vacuum. */
    pthread_rwlock_t m_rwLock;

    /** @var Lock that guards against concurrent DDLs. */
    pthread_mutex_t m_metaLock;

    /** @var metadta version. */
    uint64_t m_metadataVer = 0;

    string m_tableName;

    string m_longTableName;

    /** @var Specifies whether rows have fixed length. */
    bool m_fixedLengthRows = true;

    bool m_deserialized;

    /** @var Holds number of rows in the table. The information may not be accurate.
     * Used for execution planning. */
    uint8_t m_filler[6];

    uint64_t m_filler4;

    uint32_t m_rowCount = 0;

    DECLARE_CLASS_LOGGER();
};

/** @typedef internal table identifier. */
typedef Table::InternalTableId InternalTableId;

/** @typedef external table identifier. */
typedef Table::ExternalTableId ExternalTableId;
}  // namespace MOT

#endif  // MOT_TABLE_H
