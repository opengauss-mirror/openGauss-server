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
 * masstree_index.h
 *    Primary index implementation using Masstree.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree_index.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MASSTREE_PRIMARY_INDEX_H
#define MASSTREE_PRIMARY_INDEX_H

#include "index.h"
#include "index_base.h"
#include "utilities.h"
#include "masstree_config.h"
#include "masstree/mot_masstree.hpp"
#include "masstree/mot_masstree_insert.hpp"
#include "masstree/mot_masstree_remove.hpp"
#include "masstree/mot_masstree_get.hpp"
#include "masstree/mot_masstree_iterator.hpp"
#include "masstree/mot_masstree_struct.hpp"
#include "masstree/mot_masstree_iterator.hpp"
#include "mot_engine.h"

#include <cmath>

static_assert(MASSTREE_MAXKEYLEN == MAX_KEY_SIZE, "MASSTREE_MAXKEYLEN must be equal to MAX_KEY_SIZE");

namespace MOT {
/**
 * @class MasstreePrimaryIndex.
 * @brief Primary index implementation using Masstree.
 */
class MasstreePrimaryIndex : public Index {
public:
    struct alignas(32) default_table_params : public Masstree::nodeparams<BTREE_ORDER, BTREE_ORDER> {
        typedef uint64_t value_type;
        typedef ::threadinfo threadinfo_type;
        typedef uint64_t value_print_type;
    };

    /** @typedef The primary index type for Masstree. */
    typedef Masstree::basic_table<default_table_params> IndexImpl;

    /** @typedef The primary index iterator type for Masstree. */
    typedef typename IndexImpl::ForwardIterator ForwardIterator;

    /** @typedef The primary index reverse iterator type for Masstree. */
    typedef typename IndexImpl::ReverseIterator ReverseIterator;

private:
    /**
     * @class MTIterator<ItrImpl>
     * @brief An index iterator implementation for a primary Masstree index.
     * @tparam IteratorType (Direction).
     */
    template <IteratorType IT>
    class MTIterator : public IndexIterator {
    private:
        /** @typedef The Masstree iterator type for primary index. */
        typedef typename MasstreePrimaryIndex::ForwardIterator ForwardIterator;

        /** @typedef The Masstree reverse iterator type for primary index. */
        typedef typename MasstreePrimaryIndex::ReverseIterator ReverseIterator;

        /** @typedef The underlying iterator type. */
        typedef
            typename std::conditional<IT == IteratorType::ITERATOR_TYPE_FORWARD, ForwardIterator, ReverseIterator>::type
                ItrType;

        /** @var The underlying iterator. */
        ItrType* m_itr;

    public:
        /**
         * @brief Constructor.
         * @param itr The underlying index iterator.
         */
        explicit MTIterator(ItrType* itr)
            : IndexIterator(IT, true),  // always bidirectional
              m_itr(itr)
        {}

        /**
         * @brief Destructor.
         */
        ~MTIterator() override
        {
            if (m_itr) {
                delete m_itr;
                m_itr = nullptr;
            }
        }

        /**
         * @brief Queries whether this iterator is valid. Iterator is said to be valid if it still
         * points to a valid index items and it has not been invalidated due to concurrent modification.
         * @return True if the iterator is valid.
         */
        bool IsValid() const override
        {
            return !m_itr->Exhausted();
        }

        /**
         * @brief Invalidates the iterator such that subsequent calls to isValid() return false.
         */
        void Invalidate() override
        {
            m_itr->Invalidate();
        }

        /**
         * @brief Performs any implementation-specific cleanup.
         */
        void Destroy() override
        {}

        /**
         * @brief Retrieves the key of the currently iterated item.
         * @return A pointer to the key of the currently iterated item.
         */
        const void* GetKey() const override
        {
            return m_itr->GetSearchKey();
        }

        /**
         * @brief Retrieves the row of the currently iterated item.
         * @return A pointer to the row of the currently iterated item.
         */
        Row* GetRow() const override
        {
            return GetSentinel()->GetData();
        }

        /**
         * @brief Retrieves the currently iterated primary sentinel.
         * @return The primary sentinel.
         */
        Sentinel* GetPrimarySentinel() const override
        {
            return const_cast<Sentinel*>(GetSentinel());
        }

        /**
         * @brief Moves forwards the iterator to the next item.
         */
        void Next() override
        {
            (void)++(*m_itr);
        }

        /**
         * @brief Moves backwards the iterator to the previous item.
         * @detail Does not supported yet.
         */
        void Prev() override
        {
            MOT_ASSERT(false);
        }

        /**
         * @brief Queries whether this index iterator equals to another index iterator.
         * @param rhs The index iterator with which to compare this iterator.
         * @return True if iterators point to the same index item, otherwise false.
         */
        bool Equals(const IndexIterator* rhs) const override
        {
            return **m_itr == **(static_cast<const MTIterator*>(rhs)->m_itr);
        }

        /**
         * Serializes the iterator into a buffer.
         * @detail Not implemented
         * @param serializeFunc The serialization function.
         * @param buff The buffer into which the iterator is to be serialized.
         */
        void Serialize(serialize_func_t serializeFunc, unsigned char* buff) const override
        {}

        /**
         * Deserializes the iterator from a buffer.
         * @detail Not implemented
         * @param deserializeFunc The deserialization function.
         * @param buff The buffer from which the iterator is to be deserialized.
         */
        void Deserialize(deserialize_func_t deserializeFunc, unsigned char* buff) override
        {}

    private:
        /**
         * @brief In-lined helper method for retrieving the currently iterated primary sentinel.
         */
        inline const Sentinel* GetSentinel() const
        {
            return reinterpret_cast<const Sentinel*>(**m_itr);
        }
    };

public:
    /**
     * @brief Default constructor.
     */
    MasstreePrimaryIndex()
        : Index(MOT::IndexOrder::INDEX_ORDER_PRIMARY, IndexingMethod::INDEXING_METHOD_TREE),
          m_leafsPool(nullptr),
          m_internodesPool(nullptr),
          m_ksuffixSlab(nullptr),
          m_initialized(false)
    {}

    /**
     * @brief Destructor.
     */
    ~MasstreePrimaryIndex() override
    {
        if (m_initialized) {
            m_initialized = false;
            DestroyPools();
        }

        m_leafsPool = nullptr;
        m_internodesPool = nullptr;
        m_ksuffixSlab = nullptr;
    }

    /**
     * @brief Calculate the Index memory consumption.
     * @return The amount of memory the Index consumes.
     */
    uint64_t GetIndexSize(uint64_t& netTotal) override;

    /**
     * @brief Retrieves the number of rows stored in the index. This may be an estimation.
     * @detail Not implemented.
     * @return The number of rows stored in the index.
     */
    uint64_t GetSize() const override
    {
        return 0;
    }

    /**
     * @brief Clears object pool thread level cache
     */
    void ClearThreadMemoryCache() override
    {
        Index::ClearThreadMemoryCache();
        if (m_leafsPool != nullptr) {
            m_leafsPool->ClearThreadCache();
        }
        if (m_internodesPool != nullptr) {
            m_internodesPool->ClearThreadCache();
        }
        if (m_ksuffixSlab != nullptr) {
            m_ksuffixSlab->ClearThreadCache();
        }
    }

    /**
     * @brief Clears object pool level cache
     */
    void ClearFreeCache() override
    {
        Index::ClearFreeCache();
        if (m_leafsPool != nullptr) {
            m_leafsPool->ClearFreeCache();
        }
        if (m_internodesPool != nullptr) {
            m_internodesPool->ClearFreeCache();
        }
        if (m_ksuffixSlab != nullptr) {
            m_ksuffixSlab->ClearFreeCache();
        }
    }

    void Compact(Table* table, uint32_t pid) override;

    /**
     * @brief Print Masstree pools memory consumption details to log.
     */
    void PrintPoolsStats(LogLevel level)
    {
        m_leafsPool->Print("Leafs pool", level);
        m_internodesPool->Print("Internode pool", level);
        m_ksuffixSlab->Print("Ksuffix slab", level);
    }

    virtual void GetLeafsPoolStats(uint64_t& objSize, uint64_t& numUsedObj, uint64_t& totalSize, uint64_t& netto)
    {
        PoolStatsSt stats = {};
        m_leafsPool->GetStats(stats);

        objSize = stats.m_objSize;
        numUsedObj = stats.m_totalObjCount - stats.m_freeObjCount;
        totalSize = stats.m_poolCount * stats.m_poolGrossSize;
        netto = numUsedObj * objSize;
    }

    virtual void GetInternodesPoolStats(uint64_t& objSize, uint64_t& numUsedObj, uint64_t& totalSize, uint64_t& netto)
    {
        PoolStatsSt stats = {};
        m_internodesPool->GetStats(stats);

        objSize = stats.m_objSize;
        numUsedObj = stats.m_totalObjCount - stats.m_freeObjCount;
        totalSize = stats.m_poolCount * stats.m_poolGrossSize;
        netto = numUsedObj * objSize;
    }

    virtual PoolStatsSt* GetKsuffixSlabStats()
    {
        return m_ksuffixSlab->GetStats();
    }

    /**
     * @brief Destroy all memory pools and init index again.
     */
    RC ReInitIndex(bool isDrop) override
    {
        m_initialized = false;
        DestroyPools();

        // remove masstree's root pointer (not valid anymore)
        *(m_index.root_ref()) = nullptr;

        if (isDrop) {
            return RC_OK;
        } else {
            return IndexInitImpl(NULL);
        }
    }

    // Iterator API
    IndexIterator* Begin(uint32_t pid, bool passive) const override;

    IndexIterator* Search(
        const Key* key, bool matchKey, bool forward, uint32_t pid, bool& found, bool passive) const override;

    /**
     * @brief Allocate memory from pools.
     * @param size How many bytes are required.
     * @param tag Hint to determine which pool to use.
     * @return Pointer to allocated memory.
     */
    virtual void* AllocateMem(int& size, enum memtag tag)
    {
        switch (tag) {
            case memtag_masstree_leaf:
                return m_leafsPool->Alloc();

            case memtag_masstree_internode:
                return m_internodesPool->Alloc();

            case memtag_masstree_ksuffixes:
            case memtag_masstree_gc:  // Using ksuffixes pool for GC requests
                return m_ksuffixSlab->Alloc(size);

            default:
                MOT_LOG_ERROR("Try to allocating size %d with unknown memtag %d", size, tag);
                return NULL;
        }
    }

    /**
     * @brief Deallocate memory from pools.
     * @param size Allocation size.
     * @param tag Hint to determine which pool to use.
     * @param Pointer to allocated memory.
     * @return True if deallocation succeeded.
     */
    virtual bool DeallocateMem(void* ptr, int size, enum memtag tag)
    {
        switch (tag) {
            case memtag_masstree_leaf:
                m_leafsPool->Release(ptr);
                return true;

            case memtag_masstree_internode:
                m_internodesPool->Release(ptr);
                return true;

            case memtag_masstree_ksuffixes:
            case memtag_masstree_gc:  // Using ksuffixes pool for GC requests
                m_ksuffixSlab->Release(ptr, size);
                return true;

            default:
                MOT_LOG_ERROR("Try to deallocating ptr %p size %d with unknown memtag %d", ptr, size, tag);
                return false;
        }
    }

    /**
     * @brief Static callback function for deallocate memory from pools.
     * @param pool Pool to deallocate from.
     * @param ptr Pointer to allocated memory.
     * @param dropIndex Indicates if this callback is part of drop index process.
     * @return Size of memory that was deallocated.
     */
    static uint32_t DeallocateFromPoolCallBack(void* gcElement, void* oper, void* aux)
    {
        LimboElement* elem = reinterpret_cast<LimboElement*>(gcElement);
        GC_OPERATION_TYPE gcOperType = (*(GC_OPERATION_TYPE*)oper);
        // If dropIndex == true, all index's pools are going to be cleaned, so we skip the release here
        ObjAllocInterface* localPoolPtr = (ObjAllocInterface*)elem->m_objectPtr;

        if (gcOperType != GC_OPERATION_TYPE::GC_OPER_DROP_INDEX) {
            localPoolPtr->Release(elem->m_objectPool);
        }
        return localPoolPtr->m_size;
    }

    /**
     * @brief Static callback function for deallocate memory from slabs.
     * @param slab Slab to deallocate from.
     * @param ptr Pointer to allocated memory (48 bits) and the size of allocation (16 bits)
     * @param dropIndex Indicates if this callback is part of drop index process.
     * @return Size of memory that was deallocated.
     */
    static uint32_t DeallocateFromSlabCallBack(void* gcElement, void* oper, void* aux)
    {
        LimboElement* elem = reinterpret_cast<LimboElement*>(gcElement);
        GC_OPERATION_TYPE gcOperType = (*(GC_OPERATION_TYPE*)oper);
        // If dropIndex == true, all index's pools are going to be cleaned, so we skip the release here
        void* ptrToFree = (void*)(((uint64_t)elem->m_objectPool) & (uint64_t)0x0000FFFFFFFFFFFF);
        int size = (((uint64_t)elem->m_objectPool) & 0xFFFF000000000000) >> 48;
        if (gcOperType != GC_OPERATION_TYPE::GC_OPER_DROP_INDEX) {
            ((SlabAllocator*)elem->m_objectPtr)->Release(ptrToFree, size);
        }
        return size;
    }

    /**
     * @brief Static callback function for execute layer removal and deallocate memory from slabs.
     * @param slab Slab to deallocate from.
     * @param gcRemoveLayerFuncObjPtr Pointer to gc_layer_rcu_callback_ng struct (derives from mrcu_callback)
     * @param dropIndex Indicates if this callback is part of drop index process.
     * @return Size of memory that was deallocated.
     */
    static uint32_t DeallocateFromSlabGcCallBack(void* gcElement, void* oper, void* aux)
    {
        LimboElement* elem = reinterpret_cast<LimboElement*>(gcElement);
        GC_OPERATION_TYPE gcOperType = (*(GC_OPERATION_TYPE*)oper);
        // If dropIndex == true, all index's pools are going to be cleaned, so we skip the release here
        mtSessionThreadInfo->set_gc_session(GetCurrentGcSession());
        bool dropIndex = (gcOperType == GC_OPERATION_TYPE::GC_OPER_DROP_INDEX);

        size_t allocationSize = (*static_cast<mrcu_callback*>(elem->m_objectPool))(dropIndex);
        if (gcOperType != GC_OPERATION_TYPE::GC_OPER_DROP_INDEX) {
            ((SlabAllocator*)elem->m_objectPtr)->Release(elem->m_objectPool, allocationSize);
        }

        mtSessionThreadInfo->set_gc_session(NULL);
        return allocationSize;
    }

    /**
     * @brief Add memory for delayed deallocation by the GC (rcu).
     * @param ptr Pointer to memory for deallocation.
     * @param size size of memory for deallocatation.
     * @param tag Hint to determine which pool to use.
     * @return True if adding to GC succeeded.
     */
    bool RecordMemRcu(void* ptr, int size, enum memtag tag)
    {
        void* ptrToFree = nullptr;
        GcManager* gc_session = (GcManager*)mtSessionThreadInfo->get_gc_session();
        MOT_ASSERT(gc_session);
        switch (tag) {
            case memtag_masstree_leaf:
                gc_session->GcRecordObject(GC_QUEUE_TYPE::GENERIC_QUEUE,
                    GetIndexId(),
                    (void*)m_leafsPool,
                    ptr,
                    DeallocateFromPoolCallBack,
                    m_leafsPool->m_size);
                return true;

            case memtag_masstree_internode:
                gc_session->GcRecordObject(GC_QUEUE_TYPE::GENERIC_QUEUE,
                    GetIndexId(),
                    (void*)m_internodesPool,
                    ptr,
                    DeallocateFromPoolCallBack,
                    m_internodesPool->m_size);
                return true;

            case memtag_masstree_ksuffixes:
                MOT_ASSERT((size >> 16) == 0);  // validate that size using 2 bytes or less
                ptrToFree = (void*)((uint64_t)ptr | ((uint64_t)size << 48));
                gc_session->GcRecordObject(GC_QUEUE_TYPE::GENERIC_QUEUE,
                    GetIndexId(),
                    (void*)m_ksuffixSlab,
                    ptrToFree,
                    DeallocateFromSlabCallBack,
                    size);
                return true;

            case memtag_masstree_gc:
                gc_session->GcRecordObject(GC_QUEUE_TYPE::GENERIC_QUEUE,
                    GetIndexId(),
                    (void*)m_ksuffixSlab,
                    ptr,
                    DeallocateFromSlabGcCallBack,
                    size);
                return true;

            default:
                MOT_LOG_ERROR("Try to record rcu ptr %p size %d with unknown memtag %d", ptr, size, tag);
                return false;
        }
    }

    /**
     * @brief Static helper for getting current GC Session.
     */
    static GcManager* GetCurrentGcSession();

protected:
    /**
     * @brief Implements index initialization.
     * @param args Null-terminated list of any additional arguments.
     * @return Return code denoting success or error.
     */
    virtual RC IndexInitImpl(void** args);

    virtual Sentinel* IndexInsertImpl(const Key* key, Sentinel* sentinel, bool& inserted, uint32_t pid);

    virtual Sentinel* IndexReadImpl(const Key* key, uint32_t pid) const;

    virtual Sentinel* IndexRemoveImpl(const Key* key, uint32_t pid);

    /** @var The underlying Masstree instance. */
    IndexImpl m_index;

private:
    static constexpr int SUFFIX_SLAB_MIN_BIN = 6;  // 2^6 = 64Bytes

    /** @var Memory pool for leafs. */
    ObjAllocInterface* m_leafsPool;

    /** @var Memory pool for internodes. */
    ObjAllocInterface* m_internodesPool;

    /** @var Memory pool for ksuffixes and gc_layer_rcu_callback. */
    SlabAllocator* m_ksuffixSlab;  // used also for gc_layer_rcu_callback

    /** @var Determine if object is initialized or not. */
    bool m_initialized;

    /**
     * @brief Init Masstree memory pools.
     * @return True if succeeded otherwise false.
     * @note In case of failure it is the responsibility of the caller to call @ref DestroyPools().
     */
    bool InitPools()
    {
        m_leafsPool =
            ObjAllocInterface::GetObjPool(m_index.getMemtagMaxSize(memtag_masstree_leaf), false, CACHE_LINE_SIZE);
        if (!m_leafsPool) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Index", "Failed to create leaf pool");
            return false;  // safe cleanup in DestroyPools()
        }

        m_internodesPool =
            ObjAllocInterface::GetObjPool(m_index.getMemtagMaxSize(memtag_masstree_internode), false, CACHE_LINE_SIZE);
        if (!m_internodesPool) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Index", "Failed to create inter-node pool");
            return false;  // safe cleanup in DestroyPools()
        }

        int memtagMaxSize = m_index.getMemtagMaxSize(memtag_masstree_ksuffixes);
        m_ksuffixSlab = new (std::nothrow)
            SlabAllocator(SUFFIX_SLAB_MIN_BIN, static_cast<int>(std::ceil(std::log2(memtagMaxSize))), false);
        if (!m_ksuffixSlab) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Index", "Failed to create suffix allocator");
            return false;  // safe cleanup in DestroyPools()
        }

        if (!m_ksuffixSlab->IsSlabInitialized()) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Index", "Failed to create suffix allocator");
            return false;  // safe cleanup in DestroyPools()
        }

        return true;
    }

    /**
     * @brief Destroy Masstree memory pools.
     */
    void DestroyPools()
    {
        if (m_leafsPool) {
            ObjAllocInterface::FreeObjPool(&m_leafsPool);
            m_leafsPool = NULL;
        }
        if (m_internodesPool) {
            ObjAllocInterface::FreeObjPool(&m_internodesPool);
            m_internodesPool = NULL;
        }

        if (m_ksuffixSlab) {
            delete m_ksuffixSlab;
            m_ksuffixSlab = NULL;
        }
    }

    /**
     * @brief Searches for a key in the index, returning an iterator to the closest matching key
     * according to the search criteria.
     * @param keybuf Key's buffer to search. May be a partial key.
     * @param keylen Buffer's size.
     * @param matchKey Specifies whether to include in the result an exact match of the key. Specify
     * true for point queries and range queries that should include the range boundary.
     * @param forward Specifies the search direction and the direction of the resulting iterator.
     * @param pid The logical identifier of the requesting thread.
     * @param[out] found Returns information whether an exact match was found. Check this output
     * parameter when issuing point queries.
     * @return The resulting iterator. The caller is responsible whether to use the iterator or not
     * in case the requested key was not found exactly.
     */
    IndexIterator* Search(char const* keybuf, uint32_t keylen, bool matchKey, bool forward, uint32_t pid, bool& found,
        bool passive = false) const;

    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT

#endif /* MASSTREE_PRIMARY_INDEX_H */
