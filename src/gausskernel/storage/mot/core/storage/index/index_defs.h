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
 * index_defs.h
 *    Base index definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/index_defs.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INDEX_DEFS_H
#define INDEX_DEFS_H

#include <cstdint>
#include "type_formatter.h"

namespace MOT {
/**
 * @typedef Allows serializing iterators into external serialization infrastructure.
 */
typedef unsigned long long (*deserialize_func_t)(unsigned char* buff, size_t pack_length);

/**
 * @typedef Allows serializing iterators into external serialization infrastructure.
 */
typedef void (*serialize_func_t)(unsigned char* buff, size_t pack_length, unsigned long long data);

/**
 * @enum IndexTreeFlavor
 * @brief Defines index tree's type.
 */
enum class IndexTreeFlavor : uint8_t {
    /**
     * @var Denotes a Masstree tree index flavor.
     */
    INDEX_TREE_FLAVOR_MASSTREE,

    /**
     * @var Denotes a Invalid tree index flavor.
     */
    INDEX_TREE_FLAVOR_INVALID
};

#define DEFAULT_TREE_FLAVOR MOT::IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE

/**
 * @brief Convert index tree flavor in string representation to enum representation.
 * @return enum IndexTreeFlavor which representing the given char *.
 */
inline IndexTreeFlavor IndexTreeFlavorFromString(const char* flavor)
{
    if (strcmp(flavor, "MasstreeFlavor") == 0) {
        return IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE;
    }

    return IndexTreeFlavor::INDEX_TREE_FLAVOR_INVALID;
}

/**
 * @brief Convert index tree flavor in enum representation to string representation.
 * @return Char * which representing the given enum IndexTreeFlavor.
 */
const char* IndexTreeFlavorToString(const IndexTreeFlavor& indexTreeFlavor);

/**
 * @class TypeFormatter<IndexTreeFlavor>
 * @brief Specialization of TypeFormatter<T> with [ T = IndexTreeFlavor ].
 */
template <>
class TypeFormatter<IndexTreeFlavor> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const IndexTreeFlavor& value, mot_string& stringValue)
    {
        stringValue = IndexTreeFlavorToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, IndexTreeFlavor& value)
    {
        value = IndexTreeFlavorFromString(stringValue);
        return value != IndexTreeFlavor::INDEX_TREE_FLAVOR_INVALID;
    }
};

/**
 * @enum IndexOrder
 * @brief Defines constants for index order (primary or secondary).
 */
enum class IndexOrder : uint32_t {
    /**
     * @var Denotes a primary index.
     */
    INDEX_ORDER_PRIMARY,

    /**
     * @var Denotes a secondary index.
     */
    INDEX_ORDER_SECONDARY
};

/**
 * @enum InedxingMethod
 * @brief Defines constants for indexing methods (hashing, tree).
 */
enum class IndexingMethod : uint32_t {
    /**
     * @var Denotes tree-based indexing.
     */
    INDEXING_METHOD_TREE
};

/**
 * @enum ScanDirection
 * @brief Defines constants describing how an index scan progresses.
 */
enum class ScanDirection : uint32_t {
    /**
     * @var Denotes the index scan progresses forward in ascending order.
     */
    SCAN_DIRECTION_ASCENDING,

    /**
     * @var Denotes the index scan progresses backwards in descending order.
     */
    SCAN_DIRECTION_DESCENDING
};

/**
 * @enum Iterator type.
 */
enum class IteratorType : uint32_t {
    /**
     * @var Designates a forward iterator. A forward iterator moves forward when invoking
     * IndexIterator::next(), and if it is a bidirectional iterator (i.e. it supports
     * IndexIterator::prev() as well), then it moves backwards when invoking
     * IndexIterator::prev().
     */
    ITERATOR_TYPE_FORWARD,

    /**
     * @var Designates a reverse iterator. A reverse iterator moves backwards when invoking
     * IndexIterator::next(), and if it is a bidirectional iterator (i.e. it supports
     * IndexIterator::prev() as well), then it moves forward when invoking
     * IndexIterator::prev().
     */
    ITERATOR_TYPE_REVERSE
};
}  // namespace MOT

#endif /* INDEX_DEFS_H */
