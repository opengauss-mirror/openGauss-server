/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * oidrbtree.h
 *	  interface for openGauss generic Red-Black binary tree package of oid.
 *
 *
 * IDENTIFICATION
 *	  ./src/include/utils/oidrbtree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef OID_RBTREE_H
#define OID_RBTREE_H

#include "postgres.h"
#include "utils/rbtree.h"

/*
 * the node of oid rbtree
 */
typedef struct OidRBNode {
    RBNode rbnode;
    Oid oid;
} OidRBNode;

typedef RBTree OidRBTree;

extern OidRBTree* CreateOidRBTree();
extern void DestroyOidRBTree(OidRBTree** tree);
extern bool OidRBTreeMemberOid(OidRBTree* tree, Oid oid);
extern bool OidRBTreeInsertOid(OidRBTree* tree, Oid oid);
extern void OidRBTreeUnionOids(OidRBTree* tree1, OidRBTree* tree2);
extern bool OidRBTreeHasIntersection(OidRBTree* tree1, OidRBTree* tree2);

#endif /* RBTREE_H */
