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
 * dfs_query_check.h
 *
 * IDENTIFICATION
 *    src/include/access/dfs/dfs_query_check.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_QUERY_CHECK_H
#define DFS_QUERY_CHECK_H

#include "access/dfs/dfs_wrapper.h"
/*
 * Function to check if a value of basic type can match the clauses list pushed down. Here we do not
 * check the length of scanClauses, and the caller need ensure it.
 *
 * @_in param value: The value to be checked, can not be NULL (it will handled by HdfsPredicateCheckNull).
 * @_in param scanClauses: Clauses which can be pushed down to orc reader.
 *
 * @return True: means the value match the predicate pushed down, so we can not prunning it,
 *     False: means the value does not match the predicate pushed down, so skip it.
 */
template <typename wrapper, typename baseType>
bool HdfsPredicateCheckValue(baseType &value, List *&scanClauses)
{
    ListCell *lc = NULL;
    HdfsScanPredicate<wrapper, baseType> *predicate = NULL;

    foreach (lc, scanClauses) {
        predicate = (HdfsScanPredicate<wrapper, baseType> *)lfirst(lc);

        if (true == predicate->m_keepFalse) {
            return false;
        } else if (HDFS_QUERY_ISNULL == predicate->m_strategy) {
            return false;
        } else if (HDFS_QUERY_ISNOTNULL == predicate->m_strategy) {
            continue;
        } else if (!predicate->HdfsPredicateCheckOne(value)) {
            return false;
        }
    }

    return true;
}

/*
 * Function to check if a null value can match the clauses list pushed down.
 *
 * @_in param scanClauses: Clauses which can be pushed down for the partition and orc reader.
 *
 * @return True: means the value match the predicate pushed down, so we can not prunning it,
 *     False: means the value does not match the predicate pushed down, so skip it.
 */
template <typename T>
bool HdfsPredicateCheckNull(List *scanClauses)
{
    ListCell *lc = NULL;
    HdfsScanPredicate<T, void *> *predicate = NULL;

    if (0 == list_length(scanClauses))
        return true;

    foreach (lc, scanClauses) {
        predicate = (HdfsScanPredicate<T, void *> *)lfirst(lc);
        if (true == predicate->m_keepFalse) {
            return false;
        } else if (HDFS_QUERY_ISNULL == predicate->m_strategy) {
            continue;
        } else {
            return false;
        }
    }

    return true;
}

/*
 * @Description: Check the column value by predicates.
 * @in isNull, Whether the column is null value.
 * @in value, If the column is not null, it presents the column value.
 * @in predicateList, The pushdown predicates on this column.
 * @return If the column value satisfies the predicates, return true, otherwise
 * return false.
 */
template <typename wrapper, typename baseType>
bool HdfsPredicateCheck(bool isNull, baseType value, List *predicateList)
{
    bool filtered = false;
    if (isNull) {
        filtered = HdfsPredicateCheckNull<NullWrapper>(predicateList);
    } else {
        filtered = HdfsPredicateCheckValue<wrapper, baseType>(value, predicateList);
    }

    return filtered;
}

#endif
