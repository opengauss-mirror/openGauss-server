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
 * txn_ddl_access.h
 *    Implements TxnDDLAccess which is used to cache and access transactional DDL changes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_ddl_access.h
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"

#ifndef TXN_DDL_ACCESS_H
#define TXN_DDL_ACCESS_H

#define MAX_DDL_ACCESS_SIZE 100

namespace MOT {
enum DDLAccessType : uint8_t {
    /** @var unknown. */
    DDL_ACCESS_UNKNOWN,

    /** @var create table. */
    DDL_ACCESS_CREATE_TABLE,

    /** @var drop table. */
    DDL_ACCESS_DROP_TABLE,

    /** @var truncate table. */
    DDL_ACCESS_TRUNCATE_TABLE,

    /** @var create index. */
    DDL_ACCESS_CREATE_INDEX,

    /** @var drop index. */
    DDL_ACCESS_DROP_INDEX,

};

// forward declaration
class OccTransactionManager;
class TxnManager;

/**
 * @class TxnDDLAccess
 * @brief TxnDDLAccess used to cache and access transactional DDL changes. All
 * DDLs executed in a transaction are stored in the TxnDDLAccess and are applied
 * rolled back in the transaction commit/rollback.
 * Assumption is that the envelope takes care of DDL concurrency and ensures that
 * concurrent DDL changes are not executed in parallel.
 */
class TxnDDLAccess {
public:
    friend class OccTransactionManager;
    /**
     * @class DDLAccess
     * @brief DDLAccess represents a single DDL change.
     */
    class DDLAccess {
    public:
        /** @brief Default constructor. */
        DDLAccess();
        /* @brief Constructor. */
        inline DDLAccess(uint64_t oid, DDLAccessType accessType, void* ddlEntry)
        {
            Set(oid, accessType, ddlEntry);
        }

        ~DDLAccess()
        {}

        /**
         * @brief GetOid, returns the ddl change object id.
         * @return uint64_t object id.
         */
        uint64_t GetOid();

        /**
         * @brief GetDDLAccessType, returns the ddl change type.
         * @return DDLAccessType.
         */
        DDLAccessType GetDDLAccessType();

        /**
         * @brief GetEntry, returns the object entry.
         */
        void* GetEntry();

        /**
         * @brief Set, Sets a DDLAccess entry
         */
        void Set(uint64_t oid, DDLAccessType accessType, void* ddlEntry);

    private:
        /** @var DDL entry object id */
        uint64_t m_oid;
        /** @var DDL entry change type */
        DDLAccessType m_type;
        /** @var DDL entry on which the DDL command was applied on */
        void* m_entry;
    };

    /* @brief Constructor. */
    explicit TxnDDLAccess(TxnManager* txn);
    TxnDDLAccess(const TxnDDLAccess& orig) = delete;
    TxnDDLAccess& operator=(const TxnDDLAccess& orig) = delete;

    /* @brief Destructor. */
    ~TxnDDLAccess();

    /**
     * @brief Init. Initialize DDLTxnAccess class, currently not doing too much
     * as there is nothing to allocate or initialize.
     */
    RC Init();

    /**
     * @brief Adds a new ddlAccess to the list of transaction DDL changes.
     */
    RC Add(TxnDDLAccess::DDLAccess* ddlAccess);

    /**
     * @brief Returns the number of "in-flight" transaction DDL changes.
     */
    uint32_t Size();

    /**
     * @brief Returns DDLAccess in the provided index.
     */
    DDLAccess* Get(uint16_t index);

    /**
     * @brief Returns DDLAccess by its oid.
     */
    DDLAccess* GetByOid(uint64_t oid);

    /**
     * @brief clears and de-allocate buffered DDL changes.
     */
    void Reset();

    /**
     * @brief Deletes DDLAccess in the provided index.
     */
    void EraseAt(uint16_t index);

    /**
     * @brief Deletes DDLAccess by its oid.
     */
    void EraseByOid(uint64_t oid);

private:
    /** @var TxnManager the Transaction owning the ddl changes */
    TxnManager* m_txn;

    /** @var Indicates whether the TxnDDLAccess was already initialized or not */
    bool m_initialized;

    /** @var Denotes the number of DDLAccess entries in this TxnDDLAccess */
    uint16_t m_size;

    /** @var Denotes an array of DDLAccess entries */
    TxnDDLAccess::DDLAccess* m_accessList[MAX_DDL_ACCESS_SIZE];
};
}  // namespace MOT

#endif /* TXN_DDL_ACCESS_H */
