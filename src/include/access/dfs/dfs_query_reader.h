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
 * dfs_query_reader.h
 *
 * IDENTIFICATION
 *    src/include/access/dfs/dfs_query_reader.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_QUERY_READER_H
#define DFS_QUERY_READER_H
extern HdfsQueryOperator HdfsGetQueryOperator(const char *operatorName);
/*
 * Fill up the structure: readerState which can not be NULL.
 */
extern void FillReaderState(dfs::reader::ReaderState *readerState, ScanState *ss,
                            DfsPrivateItem *item, Snapshot snapshot = NULL);
#endif
