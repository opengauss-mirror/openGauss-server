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
* knl_uverify.cpp
*  Implementation of ustore verification
*
*
* IDENTIFICATION
*        src/gausskernel/storage/access/ustore/knl_uverify.cpp
*
* ---------------------------------------------------------------------------------------
*/


#include "postgres.h"
 
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "access/transam.h"
#include "access/ubtree.h"
#include "access/ustore/knl_upage.h"
#include "storage/freespace.h"

 bool PrecheckUstoreVerifyParams(uint32 module, Relation rel, bool analyzeVerify)
 {
    uint32 mainModule = module & USTORE_VERIFY_MOD_MASK;

    /* Check whether the module verification function is enabled. */
    if ((analyzeVerify && (u_sess->attr.attr_storage.ustore_verify_module & mainModule) == 0) ||
    (!analyzeVerify && ((u_sess->attr.attr_storage.ustore_verify_module & mainModule) == 0 ||
    u_sess->attr.attr_storage.ustore_verify_level <= USTORE_VERIFY_DEFAULT))) {
        return false;
    }
    if ((mainModule <= USTORE_VERIFY_MOD_INVALID) || (mainModule > USTORE_VERIFY_MOD_MASK)) {
        return false;
    }

    /*
     * Precheck relation type. In non-analyze mode, only the ustore non-toast table, ubtree, and undo are allowed.
     * In analyze mode, only ustore non-toast tables are supported.
     */
    if (rel != NULL && RelationIsUstoreFormat(rel) && RelationIsToast(rel)) {
        return false;
    }
    if (!analyzeVerify && (rel != NULL && !RelationIsUstoreFormat(rel) && !RelationIsUstoreIndex(rel))) {
        return false;
    }
    if (analyzeVerify && (rel != NULL && !RelationIsUstoreFormat(rel))) {
        return false;
    }
    return true;
}

 /* Construct the ustore verify parameter structure. */
bool ConstructUstoreVerifyParam(uint32 module, VerifyLevel vLevel, char *paramSt, Relation rel, Page page,
    BlockNumber blk, TupleDesc tupDesc, GPIScanDesc gpiScan, XLogRecPtr lastestRedo, undo::UndoZone *uZone,
    undo::TransactionSlot *slot, bool analyzeVerify)
{
    errno_t rc = EOK;
    bool finishSetParams = false;
    uint32 mainModule = module & USTORE_VERIFY_MOD_MASK;
    uint32 subModule = module & USTORE_VERIFY_SUB_MOD_MASK;

    /* Precheck verify parameters. */
    if (!PrecheckUstoreVerifyParams(module, rel, analyzeVerify)) {
        return false;
    }

    switch (mainModule) {
        case USTORE_VERIFY_MOD_UPAGE: {
            rc = memset_s(paramSt, sizeof(UPageVerifyParams), 0, sizeof(UPageVerifyParams));
            securec_check(rc, "\0", "\0");
            UPageVerifyParams *params = (UPageVerifyParams *) paramSt;
            params->bvInfo.analyzeVerify = analyzeVerify;
            params->bvInfo.vLevel = vLevel;
            params->bvInfo.rel = rel;
            params->page = page;
            params->blk = blk;
            params->tupDesc = tupDesc;
            finishSetParams = true;
        }
            break;
        case USTORE_VERIFY_MOD_UBTREE: {
            rc = memset_s(paramSt, sizeof(UBtreePageVerifyParams), 0, sizeof(UBtreePageVerifyParams));
            securec_check(rc, "\0", "\0");
            UBtreePageVerifyParams *params = (UBtreePageVerifyParams *) paramSt;
            params->bvInfo.analyzeVerify = analyzeVerify;
            params->bvInfo.vLevel = vLevel;
            params->bvInfo.rel = rel;
            params->page = page;
            params->gpiScan = gpiScan;
            finishSetParams = true;
        }
            break;
        case USTORE_VERIFY_MOD_UNDO: {
            rc = memset_s(paramSt, sizeof(UndoVerifyParams), 0, sizeof(UndoVerifyParams));
            securec_check(rc, "\0", "\0");
            UndoVerifyParams *params = (UndoVerifyParams *) paramSt;
            params->bvInfo.analyzeVerify = analyzeVerify;
            params->bvInfo.vLevel = vLevel;
            if (subModule == USTORE_VERIFY_UNDO_SUB_UNDOZONE) {
                params->subModule = UNDO_VERIFY_UNDOZONE;
                params->paramVal.undoZone = uZone;
            } else if (subModule == USTORE_VERIFY_UNDO_SUB_TRANSLOT) {
                params->subModule = UNDO_VERIFY_TRANS_SLOT;
                params->paramVal.slot= slot;
            } else if (subModule == USTORE_VERIFY_UNDO_SUB_TRANSLOT_BUFFER) {
                params->subModule = UNDO_VERIFY_TRANS_SLOT_BUFFER;
                params->paramVal.page= page;
            }
            finishSetParams = true;
        }
            break;
        case USTORE_VERIFY_MOD_REDO: {
            rc = memset_s(paramSt, sizeof(URedoVerifyParams), 0, sizeof(URedoVerifyParams));
            securec_check(rc, "\0", "\0");
            URedoVerifyParams *params = (URedoVerifyParams *) paramSt;
            params->pageVerifyParams.bvInfo.analyzeVerify = analyzeVerify;
            params->pageVerifyParams.bvInfo.vLevel = vLevel;
            params->pageVerifyParams.bvInfo.rel = rel;
            params->pageVerifyParams.page = page;
            params->pageVerifyParams.blk = blk;
            params->pageVerifyParams.tupDesc = tupDesc;
            params->latestRedo = lastestRedo;
            finishSetParams = true;
        }
            break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_USTORE),
                    errmsg("Verify module(%u) is invalid.", module),
                    errdetail("N/A"),
                    errcause("Invalid verify module."),
                    erraction("Check the input paramter.")));
            return false;
        }
    }

    return (finishSetParams) ? true : false;
}

/* Verification entry of each ustore module. */
bool ExecuteUstoreVerify(uint32 module, char* verifyParam)
{
    if (verifyParam == NULL) {
        return false;
    }

    bool analyzeVerify = ((baseVerifyInfo *) verifyParam)->analyzeVerify;
    uint32 mainModule = module & USTORE_VERIFY_MOD_MASK;
    VerifyLevel vLevel = ((baseVerifyInfo *) verifyParam)->vLevel;

    /* Adjust the verification level. The value cannot be higher than the value of ustore_verify_level guc. */
    if (!analyzeVerify && vLevel >= u_sess->attr.attr_storage.ustore_verify_level) {
        ((baseVerifyInfo *) verifyParam)->vLevel = (VerifyLevel) u_sess->attr.attr_storage.ustore_verify_level;
    }

    switch (mainModule) {
        case USTORE_VERIFY_MOD_UPAGE: {
            UPageVerifyParams *params = (UPageVerifyParams *) verifyParam;
            VerifyUPageValid(params);
        }
            break;
        case USTORE_VERIFY_MOD_UBTREE: {
            UBtreePageVerifyParams *params = (UBtreePageVerifyParams *) verifyParam;
            UBTreePageVerify(params);
        }
            break;
        case USTORE_VERIFY_MOD_UNDO: {
            UndoVerifyParams *params = (UndoVerifyParams *) verifyParam;
            if (params->subModule == UNDO_VERIFY_UNDOZONE) {
                undo::VerifyUndoZone(params->paramVal.undoZone);
            } else if (params->subModule == UNDO_VERIFY_TRANS_SLOT) {
                undo::VerifyTransactionSlotValid(params->paramVal.slot);
            } else if (params->subModule == UNDO_VERIFY_TRANS_SLOT_BUFFER) {
                undo::VerifyTransactionSlotBuffer(params->paramVal.page);
            }
        }
            break;
        case USTORE_VERIFY_MOD_REDO: {
            URedoVerifyParams *params = (URedoVerifyParams *) verifyParam;
            VerifyRedoUPageValid(params);
        }
            break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_USTORE),
                    errmsg("Verify module(%u) is invalid.", module),
                    errdetail("N/A"),
                    errcause("Invalid verify module."),
                    erraction("Check the input paramter.")));
            return false;
        }
    }

    return true;
}
