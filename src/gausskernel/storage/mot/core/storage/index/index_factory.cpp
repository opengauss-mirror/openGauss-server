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
 * index_factory.cpp
 *    Factory for all index types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/index_factory.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "index_factory.h"
#include "masstree_index.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(IndexFactory, Storage)

IMPLEMENT_CLASS_LOGGER(IndexFactory, Storage);

Index* IndexFactory::CreateIndex(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor)
{
    Index* newIx = nullptr;

    newIx = CreatePrimaryIndex(indexingMethod, flavor);
    if (newIx != nullptr) {
        newIx->SetOrder(indexOrder);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to create primary index");
    }

    return newIx;
}

Index* IndexFactory::CreateIndexEx(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor,
    bool unique, uint32_t keyLength, const std::string& name, RC& rc, void** args)
{
    Index* index = IndexFactory::CreateIndex(indexOrder, indexingMethod, flavor);
    if (index == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to create index");
        rc = RC_ABORT;
    } else {
        rc = index->IndexInit(keyLength, unique, name, args);
        if (rc != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index", "Failed to initialize index: %s ", RcToString(rc));
            delete index;
            index = nullptr;
        }
    }
    return index;
}

Index* IndexFactory::CreatePrimaryIndex(IndexingMethod indexingMethod, IndexTreeFlavor flavor)
{
    Index* result = nullptr;

    switch (indexingMethod) {
        case IndexingMethod::INDEXING_METHOD_TREE:
            result = CreatePrimaryTreeIndex(flavor);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Create Primary Index",
                "Cannot create primary index: invalid indexing method %d",
                (int)indexingMethod);
            break;
    }

    return result;
}

Index* IndexFactory::CreatePrimaryIndexEx(IndexingMethod indexingMethod, IndexTreeFlavor flavor, uint32_t keyLength,
    const std::string& name, RC& rc, void** args)
{
    Index* index = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY, indexingMethod, flavor, true, keyLength, name, rc, args);
    return index;
}

Index* IndexFactory::CreatePrimaryTreeIndex(IndexTreeFlavor flavor)
{
    Index* result = nullptr;

    switch (flavor) {
        case IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE:
            MOT_LOG_DEBUG("Creating Masstree index.");
            result = new (std::nothrow) MasstreePrimaryIndex();
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Create Primary Tree Index",
                "Cannot create primary tree index: invalid flavor %u",
                flavor);
            return nullptr;
    }

    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Create Primary Tree Index",
            "Failed to allocate primary index of flavor %u (%s): out of memory",
            flavor,
            IndexTreeFlavorToString(flavor));
    }

    return result;
}
}  // namespace MOT
