/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* knl_uverify.h
*  Ustore verification interface.
*
*
* IDENTIFICATION
*        src/include/access/ustore/knl_uverify.h
*
* ---------------------------------------------------------------------------------------
*/

#ifndef KNL_UVERIFY_H
#define KNL_UVERIFY_H

#include "storage/buf/bufpage.h"
#include "access/genam.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "utils/rel.h"

/* Ustore verfication module list. */
#define USTORE_VERIFY_MOD_INVALID 0x00000000
#define USTORE_VERIFY_MOD_UPAGE 0x00010000
#define USTORE_VERIFY_MOD_UBTREE 0x00020000
#define USTORE_VERIFY_MOD_UNDO 0x00040000
#define USTORE_VERIFY_MOD_REDO 0x00080000
#define USTORE_VERIFY_MOD_MASK (USTORE_VERIFY_MOD_UPAGE | USTORE_VERIFY_MOD_UBTREE | USTORE_VERIFY_MOD_UNDO | USTORE_VERIFY_MOD_REDO)

/* Ustore verification submodule list for a specific module. */
#define USTORE_VERIFY_UNDO_SUB_UNDOZONE 0x0001
#define USTORE_VERIFY_UNDO_SUB_TRANSLOT 0x0002
#define USTORE_VERIFY_UNDO_SUB_TRANSLOT_BUFFER 0x0004
#define USTORE_VERIFY_SUB_MOD_MASK 0x0000ffff

/* Ustore verification level of each modules. */
typedef enum VerifyLevel {
    USTORE_VERIFY_NONE = 0,
    USTORE_VERIFY_DEFAULT = 1,
    USTORE_VERIFY_FAST = 2,
    USTORE_VERIFY_COMPLETE = 3,
    USTORE_VERIFY_WHITEBOX = 4
} VerifyLevel;

/* Base verify info struct for each type. */
typedef struct baseVerifyInfo {
    bool analyzeVerify;
    VerifyLevel vLevel;
    Relation rel;
} baseVerifyInfo;

/* Input params struct for upage verification. */
typedef struct UPageVerifyParams {
    baseVerifyInfo bvInfo;
    Page page;
    BlockNumber blk;
    TupleDesc tupDesc;
} UPageVerifyParams;

/* Input params struct for upage redo verification. */
typedef struct URedoVerifyParams {
    UPageVerifyParams pageVerifyParams;
    XLogRecPtr latestRedo;
} URedoVerifyParams;

/* Input params struct for ubtree page verification. */
typedef struct UBtreePageVerifyParams {
    baseVerifyInfo bvInfo;
    Page page;
    GPIScanDesc gpiScan;
} UBtreePageVerifyParams;

/* Sub-module for undo verification */
typedef enum VerifySubModule {
    UNDO_VERIFY_UNDOZONE = 0,
    UNDO_VERIFY_TRANS_SLOT,
    UNDO_VERIFY_TRANS_SLOT_BUFFER,
    /* Add other types before the last element if needed*/
    UNDO_VERIFY_SUB_MODULE_BUTT
} VerifySubModule;

/* Input params struct for undo verification. */
typedef struct UndoVerifyParams {
    baseVerifyInfo bvInfo;
    VerifySubModule subModule;
    union {
        Page page;
        undo::UndoZone *undoZone;
        undo::TransactionSlot *slot;
    } paramVal;
} UndoVerifyParams;

extern bool ConstructUstoreVerifyParam(uint32 module, VerifyLevel vLevl, char *paramSt, Relation rel, Page page, BlockNumber blk,
    TupleDesc tupDesc = NULL, GPIScanDesc gpiScan = NULL, XLogRecPtr lastestRedo = InvalidXLogRecPtr,
    undo::UndoZone *uZone = NULL, undo::TransactionSlot *slot = NULL, bool analyzeVerify = false);
extern bool ExecuteUstoreVerify(uint32 module, char* verifyParam);
#endif
