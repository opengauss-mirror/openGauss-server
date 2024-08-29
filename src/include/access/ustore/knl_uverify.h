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

/* Ustore verification level of each modules. */
typedef enum VerifyLevel {
    USTORE_VERIFY_NONE = 0,
    USTORE_VERIFY_DEFAULT = 1,
    USTORE_VERIFY_FAST = 2,
    USTORE_VERIFY_COMPLETE = 3,
} VerifyLevel;

#define CHECK_VERIFY_LEVEL(level)\
{ \
    if (u_sess->attr.attr_storage.ustore_verify_level < level) { \
        return; \
    } \
}

#define BYPASS_VERIFY(module, rel) \
do { \
    if ((u_sess->attr.attr_storage.ustore_verify_module & module) == 0) { \
        return; \
    } \
    if (rel != NULL && !RelationIsUstoreFormat(rel) && !RelationIsUstoreIndex(rel)) { \
        return; \
    } \
} while(0)

#define UNDO_BYPASS_VERIFY \
do { \
    if ((u_sess->attr.attr_storage.ustore_verify_module & USTORE_VERIFY_MOD_UNDO) == 0) { \
        return; \
    } \
} while(0)

#endif
