/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * oidrbtree.cpp
 *	  The Red-Black binary tree of oid.
 *
 *
 * IDENTIFICATION
 *	  ./src/common/backend/utils/misc/oidrbtree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "utils/oidrbtree.h"
#include "utils/memutils.h"

void DeleteOidRBTreeMemory()
{
    if (t_thrd.security_policy_cxt.OidRBTreeMemoryContext != NULL) {
        MemoryContextDelete(t_thrd.security_policy_cxt.OidRBTreeMemoryContext);
        t_thrd.security_policy_cxt.OidRBTreeMemoryContext = NULL;
    }
}

/* Comparator function for oid rbtree */
static int OidRBComparator(const RBNode* rbNodeA, const RBNode* rbNodeB, void* arg)
{
    const OidRBNode* oidRBNodeA = (const OidRBNode*)rbNodeA;
    const OidRBNode* oidRBNodeB = (const OidRBNode*)rbNodeB;

    if (oidRBNodeA->oid == oidRBNodeB->oid) {
        return 0;
    }

    if (oidRBNodeA->oid > oidRBNodeB->oid) {
        return 1;
    }

    return -1;
}

/*
 * Combiner function for oid rbtree
 * An equivalent key (already existed) is not allowed to add
 */
static void OidRBCombine(RBNode* existing, const RBNode* newdata, void* arg)
{
    /* Nothing to do */
}

/* Allocator function for oid rbtree */
static RBNode* OidRBAlloc(void* arg)
{
    OidRBNode* oidRBNode = static_cast<OidRBNode*>(palloc(sizeof(OidRBNode)));
    return (RBNode*)oidRBNode;
}

/* Deallocator function for oid rbtree */
static void OidRBDealloc(RBNode* rbNode, void* arg)
{
    OidRBNode* oidRBNode = (OidRBNode*)rbNode;
    pfree_ext(oidRBNode);
}

OidRBTree* CreateOidRBTree()
{
    OidRBTree* oidRBTree = rb_create(sizeof(OidRBNode), OidRBComparator, OidRBCombine, OidRBAlloc, OidRBDealloc, NULL);
    return oidRBTree;
}

static List* OidRBTreeGetNodeList(OidRBTree& oidRBtree)
{
    List* nodeList = NIL;
    OidRBTree* tree = &oidRBtree;
    rb_begin_iterate(tree, InvertedWalk);
    RBNode *node = rb_iterate(tree);
    while (node != NULL) {
        nodeList = lappend(nodeList, node);
        node = rb_iterate(tree);
    }
    return nodeList;
}

void DestroyOidRBTree(OidRBTree** tree)
{
    if (tree == NULL || *tree == NULL) {
        return;
    }

    List* nodeList = OidRBTreeGetNodeList(**tree);
    if (nodeList != NIL) {
        list_free_deep(nodeList);
    }
    pfree_ext(*tree);
}

bool OidRBTreeMemberOid(OidRBTree* tree, Oid oid)
{
    OidRBNode oidRBNode;
    oidRBNode.oid = oid;
    return (rb_find(tree, (const RBNode*)&oidRBNode) != NULL);
}

bool OidRBTreeInsertOid(OidRBTree* tree, Oid oid)
{
    bool isNewOidRBNode = true;
    OidRBNode oidRBNode;
    oidRBNode.oid = oid;
    (void)rb_insert(tree, (const RBNode*)&oidRBNode, &isNewOidRBNode);
    return isNewOidRBNode;
}

void OidRBTreeUnionOids(OidRBTree* tree1, OidRBTree* tree2)
{
    if (tree2 == NULL) {
        return;
    }

    rb_begin_iterate(tree2, InvertedWalk);
    RBNode *node = rb_iterate(tree2);
    while (node != NULL) {
        OidRBNode* oidRBNode = (OidRBNode*)node;
        (void)OidRBTreeInsertOid(tree1, oidRBNode->oid);
        node = rb_iterate(tree2);
    }
}

bool OidRBTreeHasIntersection(OidRBTree* tree1, OidRBTree* tree2)
{
    if (tree2 == NULL) {
        return false;
    }

    rb_begin_iterate(tree2, InvertedWalk);
    RBNode *node = rb_iterate(tree2);
    while (node != NULL) {
        OidRBNode* oidRBNode = (OidRBNode*)node;
        if (OidRBTreeMemberOid(tree1, oidRBNode->oid)) {
            return true;
        }
        node = rb_iterate(tree2);
    }
    return false;
}

