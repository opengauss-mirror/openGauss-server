/* -------------------------------------------------------------------------
 *
 * rbtree.h
 *	  interface for openGauss generic Red-Black binary tree package
 *
 * Copyright (c) 2009-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/utils/rbtree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RBTREE_H
#define RBTREE_H

/*
 * RBNode is intended to be used as the first field of a larger struct,
 * whose additional fields carry whatever payload data the caller needs
 * for a tree entry.  (The total size of that larger struct is passed to
 * rb_create.)	RBNode is declared here to support this usage, but
 * callers must treat it as an opaque struct.
 */
typedef struct RBNode {
    char iteratorState;    /* workspace for iterating through tree */
    char color;            /* node's current color, red or black */
    struct RBNode* left;   /* left child, or RBNIL if none */
    struct RBNode* right;  /* right child, or RBNIL if none */
    struct RBNode* parent; /* parent, or NULL (not RBNIL!) if none */
} RBNode;

/* Opaque struct representing a whole tree */
typedef struct RBTree RBTree;

/* Available tree iteration orderings */
typedef enum RBOrderControl {
    LeftRightWalk, /* inorder: left child, node, right child */
    RightLeftWalk, /* reverse inorder: right, node, left */
    DirectWalk,    /* preorder: node, left child, right child */
    InvertedWalk   /* postorder: left child, right child, node */
} RBOrderControl;

/* Support functions to be provided by caller */
typedef int (*rb_comparator)(const RBNode* a, const RBNode* b, void* arg);
typedef void (*rb_combiner)(RBNode* existing, const RBNode* newdata, void* arg);
typedef RBNode* (*rb_allocfunc)(void* arg);
typedef void (*rb_freefunc)(RBNode* x, void* arg);
typedef void (*rb_copyfunc)(RBTree* rb, RBNode* dest, const RBNode* src);

extern RBTree* rb_create(Size node_size, rb_comparator comparator, rb_combiner combiner, rb_allocfunc allocfunc,
    rb_freefunc freefunc, void* arg, rb_copyfunc = NULL);

extern RBNode* rb_find(RBTree* rb, const RBNode* data);
extern RBNode* rb_leftmost(RBTree* rb);

extern RBNode* rb_insert(RBTree* rb, const RBNode* data, bool* isNew);
extern void rb_delete(RBTree* rb, RBNode* node);

extern void rb_begin_iterate(RBTree* rb, RBOrderControl ctrl);
extern RBNode* rb_iterate(RBTree* rb);

/*
 * RBTree control structure
 */
struct RBTree {
    RBNode* root; /* root node, or RBNIL if tree is empty */

    /* Iteration state */
    RBNode* cur; /* current iteration node */
    RBNode* (*iterate)(RBTree* rb);

    /* Remaining fields are constant after rb_create */

    Size node_size; /* actual size of tree nodes */
    /* The caller-supplied manipulation functions */
    rb_comparator comparator;
    rb_combiner combiner;
    rb_allocfunc allocfunc;
    rb_freefunc freefunc;
    rb_copyfunc copyfunc;
    /* Passthrough arg passed to all manipulation functions */
    void* arg;
};


#endif /* RBTREE_H */
