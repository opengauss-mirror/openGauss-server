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

#ifndef INDEX_FACTORY_H
#define INDEX_FACTORY_H

#include "index.h"

namespace MOT {
/**
 * @class IndexFactory
 * @brief Factory for all index types.
 */
class IndexFactory {
public:
    /**
     * @brief Factory function for creating an index.
     * @param IndexOrder Specifies whether to create a primary or a secondary index.
     * @param IndexingMethod Specifies whether to create a hash or a tree index.
     * @param flavor Specifies the sub-type of index.
     * @return The created index.
     */
    static Index* CreateIndex(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor);

    /**
     * @brief Factory function for creating an index and initializing it.
     * @param indexOrder Specifies whether to create a primary or a secondary index.
     * @param indexingMethod Specifies whether to create a hash or a tree index.
     * @param flavor Specifies the sub-type of index.
     * @param unique Specifies whether duplicate keys are allowed or not.
     * @param keyLength The length in bytes of index keys.
     * @param name The name of the index.
     * @param[out] rc The index initialization result.
     * @param args Null-terminated list of any additional arguments.
     * @return The created index.
     */
    static Index* CreateIndexEx(IndexOrder indexOrder, IndexingMethod indexingMethod, IndexTreeFlavor flavor,
        bool unique, uint32_t keyLength, const std::string& name, RC& rc, void** args);

    /**
     * @brief Factory function for creating a primary index.
     * @param indexingMethod Specifies whether to create a hash or a tree index.
     * @param flavor Specifies the sub-type of index.
     * @return The created index.
     */
    static Index* CreatePrimaryIndex(IndexingMethod indexingMethod, IndexTreeFlavor flavor);

    /**
     * @brief Factory function for creating a primary index and initializing it.
     * @param indexingMethod Specifies whether to create a hash or a tree index.
     * @param flavor Specifies the sub-type of index.
     * @param keyLength The length in bytes of index keys.
     * @param name The name of the index.
     * @param[out] rc The index initialization result.
     * @param args Null-terminated list of any additional arguments.
     * @return The created index.
     */
    static Index* CreatePrimaryIndexEx(IndexingMethod indexingMethod, IndexTreeFlavor flavor, uint32_t keyLength,
        const std::string& name, RC& rc, void** args);

    /**
     * @brief Factory function for creating a primary tree index.
     * @param flavor Specifies the sub-type of index.
     * @return The created tree index.
     */
    static Index* CreatePrimaryTreeIndex(IndexTreeFlavor flavor);

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* INDEX_FACTORY_H */
