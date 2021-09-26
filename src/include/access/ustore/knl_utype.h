/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_utype.h
 * the xact access interfaces of inplace update engine.
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_utype.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UTYPE_H
#define KNL_UTYPE_H

#include "access/ustore/undo/knl_uundotype.h"

typedef struct TD {
    /* 64 bit identifier for, either the transaction ID
     * or the commit sequence number.
     * The most significant bit (unused) will be used
     * to distinguish between XID and CSN
     */
    TransactionId xactid = (TransactionId)0;
    /* 64 bit identifier for undo record.
     * 16 bit seg_id, 32 bit block_id, 16 bit offset
     */
    UndoRecPtr undo_record_ptr = INVALID_UNDO_REC_PTR;
} TD;

#endif
