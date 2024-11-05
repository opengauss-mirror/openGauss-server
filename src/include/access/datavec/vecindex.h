/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * vecindex.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/vecindex.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VECINDEX_H
#define VECINDEX_H

#define MIN(A, B) ((B) < (A) ? (B) : (A))
#define MAX(A, B) ((B) > (A) ? (B) : (A))

#define VecIndexTupleGetXid(itup) (((char *)(itup)) + HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(&(itup)->data)))

struct VectorScanData {
    /*
     * used in ustore only, indicate the last returned index tuple which is modified
     * by current transaction. see VecVisibilityCheckCid() for more information.
     */
    char *lastSelfModifiedItup;
    uint16 lastSelfModifiedItupBufferSize;
    Buffer buf;
};

bool VecItupGetXminXmax(Page page, OffsetNumber offnum, TransactionId oldest_xmin, TransactionId *xmin,
                        TransactionId *xmax, bool *xminCommitted, bool *xmaxCommitted, bool isToast);
bool VecVisibilityCheck(IndexScanDesc scan, Page page, OffsetNumber offnum, bool *needRecheck);

#endif  // VECINDEX_H
