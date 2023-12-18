/* -------------------------------------------------------------------------
 *
 * nodeSortGroup.cpp
 *	  Routines to handle sorting of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeSortGroup.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecInitSortGroup	- Creates the run-time state information for the SortGroupState node
 *		ExecSortGroup	    - Groups and sorts tuples from the outer subtree of the node
 *		ExecEndSortGroup	- shutdown node and subnodes
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/node/nodeSortGroup.h"
#include "executor/tuptable.h"
#include "utils/sortsupport.h"
#include "utils/logtape.h"

#define SKIPLIST_MAXLEVEL 32  /*max level of the skiplist, should be enough for 2^64 elements*/
#define INITIAL_GROUP_SIZE 64 /*Initial size of memTuples array*/

/* ----------------
 *    GroupNode
 *
 * save the tuples and grouping keys
 * ----------------
 */
typedef struct GroupNode {
    MinimalTuple tuple; /* the tuple itself, note that the first tuple is hold in SortGroupStatePriv.maincontext */
    Datum datum1;       /* value of first key column */
    bool isnull1;       /* is first key column NULL? */
    
    int memTuplesSize;    /*current size of memTuples*/
    int memTuplesCap;     /*max capacity size of memTuples*/
    MinimalTuple *memTuples;  /*tuples belong to this group, when holding in memory*/

    /* 
     * when the used memory is greater than work_mem, 
     * tuples need to be stored on LogicalTapeSet
     */
    int tapeNum;    /* which tapenum holds the tuple of this group */
    int tapeOffset; /*tapeBlocknum and tapeOffset save the beginning position of ths group*/
    long tapeBlocknum;
} GroupNode;

/* ----------------
 *    SkiplistNode
 * ----------------
 */
typedef struct SkiplistNode {
    GroupNode *group;   /*group holds all tuples in memory or LogicalTapeSet*/
    int height;         /*height of this node*/
    struct SkiplistNode *backward;
    struct SkiplistNode *forward[FLEXIBLE_ARRAY_MEMBER];
} SkiplistNode;

/* ----------------
 *    Skiplist
 * 
 * We use skiplist to perform group sort, each skiplist node holds a group
 * and each group holds all tuples in memory or LogicalTapeSet
 * 
 * The order of the grouping key in skiplist:
 *     header->forward[0] > header->forward[0]->forward[0] > ...  > tail
 *     top-N              > top-(N-1)                      > ...  > top-1
 * when we insert a new group into the skiplist, and then the length of the skiplist exceeds max_groups, 
 * we will discatd one group that is not in the top-N groups. Discatds the head node is much easier than
 * discatds the tail node in skiplist, and that is whay we put the top-N groups into skiplist in such order.
 * ----------------
 */
typedef struct Skiplist {
    SkiplistNode *header;
    SkiplistNode *tail;
    int64 length;         /*length of the Skiplist*/
    int height;           /*max height of the Skiplist*/
} Skiplist;

typedef struct TupleIter {
    SkiplistNode *currentNode;  /* current skiplist node, which holds the tuples and grouping keys*/
    int64 memPos;               /* current position in GroupNode.memTuples*/

    /* current position in LogicalTapeSet */
    long tapeBlocknum;
    int tapeOffset;
} TupleIter;

/*
 * Private state of a SortGroupState operation.
 */
typedef struct SortGroupStatePriv {
    MemoryContext maincontext;  /* memory context for group metadata*/
    MemoryContext tuplecontext; /* memory context for tuples in memory*/
    int nKeys;                  /* number of columns in sort key */

    bool holdTupleInMem;        /*is tuples are saved in memory, otherwise saved in LogicalTapeSet */
    int64 allowedMem;           /* total memory allowed, in bytes*/
    int64 max_groups;           /* max number groups in Skiplist*/

    bool *new_group_trigger;    /* set to be TRUE when We have a new group */

    /*
     * tupDesc is only used by the MinimalTuple and CLUSTER routines.
     */
    TupleDesc tupDesc;
    SortSupport sortKeys;   /* array of length nKeys */
    Skiplist skiplist;      /* skiplist holds top-N groups*/

    LogicalTapeSet *tapeset; /* logtape.c object for tapes in a temp file */
    int *freeTape;           /* array of free tapenum in LogicalTapeSet*/     
    int freeTapeSize;        /*current size of freeTape*/
    int freeTapeCap;         /*max capacity size of freeTape*/

    TupleIter iter;          /* iterator of reanding tuples, it saves the current reading position*/
} SortGroupStatePriv;

static void initSkiplist(Skiplist *skiplist);
static SkiplistNode *skiplistCreateNode(int height);
static inline int randomLevel(void);
static SkiplistNode *skiplistInsertGroupNode(Skiplist *skiplist, const TupleTableSlot *slot, SortGroupStatePriv *state);

static SortGroupStatePriv *groupSortBegin(TupleDesc tupDesc, int nkeys, AttrNumber *attNums, Oid *sortOperators,
                                          Oid *sortCollations, bool *nullsFirstFlags, int64 max_groups, int workMem);
static void groupSortPutTupleslot(SortGroupStatePriv *state, TupleTableSlot *slot);
static void groupSortEnd(SortGroupStatePriv *state);
static void groupSortFinished(SortGroupStatePriv *state);
static void groupSortDiscardLastGroup(SortGroupStatePriv *state);
static void groupSortInittapes(SortGroupStatePriv *state);
static void groupSortSeekPos(SortGroupStatePriv *state);

static void initGroupIter(SortGroupStatePriv *state);
static int groupNodeSlotCmp(const GroupNode *a, const TupleTableSlot *slot, SortGroupStatePriv *state);
static bool groupSortGetTupleSlot(SortGroupStatePriv *state, TupleTableSlot *slot);
static GroupNode *createGroupNode(SortGroupStatePriv *state, TupleTableSlot *slot);
static void writeMinimalTuple(LogicalTapeSet *lts, int tapenum, MinimalTuple tuple);
static MinimalTuple readMinimalTuple(SortGroupStatePriv *state, TupleIter *iter);
static void deleteGroupNode(SortGroupStatePriv *state, GroupNode *group);
static void groupNodePutTupleSlot(SortGroupStatePriv *state, GroupNode *group, TupleTableSlot *slot);
static void groupNodeInitTape(SortGroupStatePriv *state, GroupNode *group);
static void groupNodeAssignTape(SortGroupStatePriv *state, GroupNode *group);
static void groupNodeReleaseTape(SortGroupStatePriv *state, GroupNode *group);

static TupleTableSlot *ExecSortGroup(PlanState *pstate);
static void dumpSortGroupState(SortGroupState *statee);

static inline int64 getMemoryContextUsedSize(MemoryContext cxt) {
     int64 bytes;
#ifndef ENABLE_MEMORY_CHECK
    Assert(cxt->type == T_AllocSetContext);
    AllocSetContext* aset = (AllocSetContext*) cxt;
    bytes = aset->totalSpace - aset->freeSpace;
#else
    Assert(cxt->type == T_AsanSetContext);
    AsanSetContext* aset = (AsanSetContext*) cxt;
    bytes = aset->totalSpace - aset->freeSpace;
#endif
    return bytes;
}

static inline int64 groupSortUsedMemory(SortGroupStatePriv *state) {
    int64 bytes = getMemoryContextUsedSize(state->maincontext);
    if (state->tuplecontext) {
        bytes += getMemoryContextUsedSize(state->tuplecontext);
    }
    return bytes;
}

/* ----------------
 *    initSkiplist
 *
 * init a new skiplist
 * ----------------
 */
void initSkiplist(Skiplist *skiplist)
{
    memset(skiplist, 0, sizeof(Skiplist));
    skiplist->height = 1;
    skiplist->length = 0;
    skiplist->header = skiplistCreateNode(SKIPLIST_MAXLEVEL);
    skiplist->tail = NULL;
}

/* ----------------
 *    skiplistCreateNode
 *
 * create a new node for SkiplistNode
 * ----------------
 */
SkiplistNode *skiplistCreateNode(int height)
{
    size_t bytes = offsetof(SkiplistNode, forward) + height * sizeof(SkiplistNode *);
    SkiplistNode *node = (SkiplistNode *)palloc0(bytes);
    node->height = height;
    return node;
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and SKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
static int randomLevel(void)
{
    /* Increase height with probability 1 in kBranching, P = 1/4 */
    static const unsigned int kBranching = 4;
    long level = 1;
    while (level < SKIPLIST_MAXLEVEL && ((random() % kBranching) == 0))
        level += 1;
    Assert(level <= SKIPLIST_MAXLEVEL);
    return level;
}

/* ----------------
 *    skiplistInsertGroupNode
 *
 * Insert a new node into the skiplist
 * ----------------
 */
static SkiplistNode *skiplistInsertGroupNode(Skiplist *skiplist, const TupleTableSlot *slot, SortGroupStatePriv *state)
{
    SkiplistNode *update[SKIPLIST_MAXLEVEL];
    SkiplistNode *x;
    SkiplistNode *forward;
    int i;
    int height;
    int result;

    x = skiplist->header;
    for (i = skiplist->height - 1; i >= 0; i--) {
        while (x->forward[i]) {
            forward = x->forward[i];
            result = groupNodeSlotCmp(forward->group, slot, state);
            if (result == 0) {
                /* forward = tuple, not need to create new node */
                return forward;
            } else if (result > 0) {
                /* tuple < forward, keep looking forward */
                x = forward;
            } else {
                /* tuple > forward, end up looking forward*/
                break;
            }
        }
        update[i] = x; /*record this level*/
    }

    height = randomLevel();
    x = skiplistCreateNode(height);

    if (height > skiplist->height) {
        for (i = skiplist->height; i < height; i++) {
            update[i] = skiplist->header;
        }
        skiplist->height = height;
    }

    for (i = 0; i < height; i++) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
    }

    x->backward = (update[0] == skiplist->header) ? NULL : update[0];
    if (x->forward[0]) {
        x->forward[0]->backward = x;
    } else {
        skiplist->tail = x;
    }
    skiplist->length++;
    return x;
}

static SortGroupStatePriv *groupSortBegin(TupleDesc tupDesc, int nkeys, AttrNumber *attNums, Oid *sortOperators,
                                          Oid *sortCollations, bool *nullsFirstFlags, int64 max_groups, int workMem)
{
    SortGroupStatePriv *state;
    MemoryContext maincontext;
    MemoryContext oldcontext;

    /*
     * This memory context holds the group metadata and group keys
     */
    maincontext = AllocSetContextCreate(CurrentMemoryContext, "GroupSort main", ALLOCSET_DEFAULT_SIZES);

    /*
     * Make the SortGroupStatePriv within the maincontext.  This way, we
     * don't need a separate pfree() operation for it at shutdown.
     */
    oldcontext = MemoryContextSwitchTo(maincontext);

    state = (SortGroupStatePriv *)palloc0(sizeof(SortGroupStatePriv));
    state->maincontext = maincontext;

    /*
     * This memory context holds tuples in memory
     */
    state->tuplecontext = AllocSetContextCreate(maincontext, "GroupSort tuple", ALLOCSET_DEFAULT_SIZES);
    state->nKeys = nkeys;
    state->tupDesc = tupDesc; /* assume we need not copy tupDesc */
    state->max_groups = max_groups;
    state->holdTupleInMem = true;

    /* Prepare SortSupport data for each column */
    state->sortKeys = (SortSupport)palloc0(nkeys * sizeof(SortSupportData));

    for (int i = 0; i < nkeys; i++) {
        SortSupport sortKey = &state->sortKeys[i];

        AssertArg(attNums[i] != 0);
        AssertArg(sortOperators[i] != 0);

        sortKey->ssup_cxt = CurrentMemoryContext;
        sortKey->ssup_collation = sortCollations[i];
        sortKey->ssup_nulls_first = nullsFirstFlags[i];
        sortKey->ssup_attno = attNums[i];
        sortKey->abbreviate = false; /*TODO: Convey if abbreviation optimization is applicable in principle*/

        PrepareSortSupportFromOrderingOp(sortOperators[i], sortKey);
    }

    state->allowedMem = Max(workMem, 64) * (int64)1024; /*work_mem is forced to be at least 64KB*/

    initSkiplist(&state->skiplist);

    MemoryContextSwitchTo(oldcontext);
    return state;
}

static void groupSortFinished(SortGroupStatePriv *state)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->maincontext);

    if (!state->holdTupleInMem) {
        SkiplistNode *node = state->skiplist.header->forward[0];

        /*prepare each group node to read*/
        while (node) {
            GroupNode *group = node->group;
            Assert(group->tapeNum >= 0);
            LogicalTapeFreeze(state->tapeset, group->tapeNum, NULL);

            /*Rewind logical tape and switch from writing to reading*/
            LogicalTapeRewindForRead(state->tapeset, group->tapeNum, BLCKSZ);

            /*obtain beginning position of each group*/
            LogicalTapeTell(state->tapeset, group->tapeNum, &group->tapeBlocknum, &group->tapeOffset);
            Assert(group->tapeOffset == 0);
            node = node->forward[0];
        }
    }

    initGroupIter(state);   /*initialize the tuples iterator*/
    MemoryContextSwitchTo(oldcontext);
}

static void initGroupIter(SortGroupStatePriv *state)
{
    TupleIter *iter = &state->iter;

    iter->memPos = 0;

    if (state->skiplist.tail) {
        iter->currentNode = state->skiplist.tail;
        iter->tapeBlocknum = iter->currentNode->group->tapeBlocknum;
        iter->tapeOffset = iter->currentNode->group->tapeOffset;
        groupSortSeekPos(state);
    }
}

/*
 * Accept one tuple while collecting input data for group sort.
 */
static void groupSortPutTupleslot(SortGroupStatePriv *state, TupleTableSlot *slot)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->maincontext);
    Skiplist *skiplist = &state->skiplist;
    SkiplistNode *groupNode;

    if (unlikely(slot->tts_nvalid < state->nKeys)) {
        tableam_tslot_getallattrs(slot);
    }

    if (skiplist->length >= state->max_groups) {
        SkiplistNode *top = skiplist->header->forward[0];
        int result;
        Assert(skiplist->length == state->max_groups);
        Assert(top);

        result = groupNodeSlotCmp(top->group, slot, state);
        if (result < 0) {
            /* tuple > top-N , new tuple is not in top-N groups, discard it*/
            return;
        } else if (result == 0) {
            groupNode = top;
        } else {
            /* find the suitable node to insert*/
            groupNode = skiplistInsertGroupNode(skiplist, slot, state);
        }
    } 
    else {
        groupNode = skiplistInsertGroupNode(skiplist, slot, state);
    }

    if (!groupNode->group) {
        /*new group, create and init the groupNode->grou*/
        groupNode->group = createGroupNode(state, slot);
    } 
    else {
        groupNodePutTupleSlot(state, groupNode->group, slot);
    }

    if (state->max_groups < skiplist->length) {
        /*when the length of the skiplist exceeds max_groups, we discard the last group*/
        groupSortDiscardLastGroup(state);
    }

    if (state->holdTupleInMem && groupSortUsedMemory(state) > state->allowedMem) {
        /* memory exceeds allowedMem, switch to tape-based operation */
        groupSortInittapes(state);

        state->holdTupleInMem = false;
        MemoryContextDelete(state->tuplecontext);
        state->tuplecontext = NULL;
    }
    MemoryContextSwitchTo(oldcontext);
}

/*
 * groupSortDiscardLastGroup - discatd one group that is not in the top-N groups.
 */
static void groupSortDiscardLastGroup(SortGroupStatePriv *state)
{
    Skiplist *skiplist = &state->skiplist;
    SkiplistNode *header = skiplist->header;
    int i;

    SkiplistNode *old_top;
    SkiplistNode *new_top;

    Assert(skiplist->length >= 1);
    Assert(state->max_groups + 1 == skiplist->length);

    old_top = header->forward[0];
    new_top = old_top->forward[0];

    for (i = 0; i < old_top->height; i++) {
        header->forward[i] = old_top->forward[i];
    }
    for (i = old_top->height; i < new_top->height; i++) {
        header->forward[i] = new_top;
    }

    deleteGroupNode(state, old_top->group);
    pfree(old_top);

    new_top->backward = NULL;
    skiplist->length--;
}

/*
 * deleteGroupNode - delete GroupNode and release its resources
 */
static void deleteGroupNode(SortGroupStatePriv *state, GroupNode *group)
{
    pfree(group->tuple);

    if (group->tapeNum >= 0) {
        groupNodeReleaseTape(state, group);
    }

    for (int i = 1; i < group->memTuplesSize; i++) {
        pfree(group->memTuples[i]);
    }
    if (group->memTuples)
        pfree(group->memTuples);

    pfree(group);
}

/*
 * createGroupNode - create and initialize a GroupNode.
 *
 * Note that GroupNode->tuple is under maincontext
 */
static GroupNode *createGroupNode(SortGroupStatePriv *state, TupleTableSlot *slot)
{
    GroupNode *group;
    MinimalTuple tuple;
    HeapTupleData htup;
    Datum original;

    Assert(CurrentMemoryContext == state->maincontext);

    tuple = ExecCopySlotMinimalTuple(slot); /* copy the tuple into sort storage */

    group = (GroupNode *)palloc0(sizeof(GroupNode));
    group->tuple = tuple;

    /* set up first-column key value */
    htup.t_len = tuple->t_len + MINIMAL_TUPLE_OFFSET;
    htup.t_data = (HeapTupleHeader)((char *)tuple - MINIMAL_TUPLE_OFFSET);
    original = heap_getattr(&htup, state->sortKeys[0].ssup_attno, state->tupDesc, &group->isnull1);

    group->datum1 = original;
    group->tapeNum = -1;

    if (state->holdTupleInMem) {
        group->memTuples = (MinimalTuple *)palloc(INITIAL_GROUP_SIZE * sizeof(MinimalTuple));
        group->memTuplesCap = INITIAL_GROUP_SIZE;
        group->memTuples[0] = tuple;
        group->memTuplesSize = 1;
    } else {
        groupNodeAssignTape(state, group);
        writeMinimalTuple(state->tapeset, group->tapeNum, tuple);
    }
    return group;
}

/*
 * groupSortInittapes - initialize for tape.
 */
static void groupSortInittapes(SortGroupStatePriv *state)
{
    SkiplistNode *node;
    state->freeTapeCap = state->freeTapeSize = Max(state->skiplist.length, 6);
    state->tapeset = LogicalTapeSetCreate(state->freeTapeSize, NULL, NULL, -1);
    state->freeTape = (int *)palloc(state->freeTapeSize * sizeof(int));
    for (int i = 0; i < state->freeTapeSize; i++) {
        state->freeTape[i] = i;
    }

    node = state->skiplist.header->forward[0];
    while (node) {
        groupNodeInitTape(state, node->group);
        node = node->forward[0];
    }
}


/* ----------------------------------------------------------------
 *  groupSortSeekPos
 *
 *  saves current position.
 * ----------------------------------------------------------------
 */
static void groupSortSeekPos(SortGroupStatePriv *state)
{
    TupleIter *iter = &state->iter;
    if (iter->currentNode && !state->holdTupleInMem) {
        LogicalTapeSeek(state->tapeset, iter->currentNode->group->tapeNum, iter->tapeBlocknum, iter->tapeOffset);
    }
}

static void groupNodeInitTape(SortGroupStatePriv *state, GroupNode *group)
{
    Assert(CurrentMemoryContext == state->maincontext);
    groupNodeAssignTape(state, group);

    for (int i = 0; i < group->memTuplesSize; i++) {
        /*
         * Dump all tuples.
         */
        writeMinimalTuple(state->tapeset, group->tapeNum, group->memTuples[i]);
    }

    /* discard all tuples in memory */
    group->memTuples = NULL;
    group->memTuplesCap = 0;
    group->memTuplesSize = 0;
}


/*
 * Assign unused tapes to specified group, extending the tape set if
 * necessary.
 */
static void groupNodeAssignTape(SortGroupStatePriv *state, GroupNode *group)
{
    int tapeNum;
    Assert(group->tapeNum == -1);

    if (state->freeTapeSize == 0) {
        int nAdditional = state->freeTapeCap;
        LogicalTapeSetExtend(state->tapeset, nAdditional);
        state->freeTapeSize = nAdditional;
        state->freeTapeCap *= 2;
        state->freeTape = (int *)repalloc(state->freeTape, state->freeTapeCap * sizeof(int));
        for (int i = 0; i < nAdditional; i++) {
            state->freeTape[i] = nAdditional + i;
        }
    }

    tapeNum = state->freeTape[--state->freeTapeSize];

    group->tapeNum = tapeNum;
}

/*
 * This function rewinds the logical tape to be free, adds its 'tapeNum' to the free list.
 */
static void groupNodeReleaseTape(SortGroupStatePriv *state, GroupNode *group)
{
    /* rewinding frees the buffer while not in use */
    LogicalTapeRewindForRead(state->tapeset, group->tapeNum, BLCKSZ);
    LogicalTapeRewindForWrite(state->tapeset, group->tapeNum);

    Assert(state->freeTapeSize < state->freeTapeCap);
    state->freeTape[state->freeTapeSize++] = group->tapeNum;
}

/*
 * Wirte a MinimalTuple into specified logical tape
 */
static void writeMinimalTuple(LogicalTapeSet *lts, int tapenum, MinimalTuple tuple)
{
    char *tupbody;
    unsigned int tupbodylen;
    unsigned int tuplen;

    /* the part of the MinimalTuple we'll write: */
    tupbody = (char *)tuple + MINIMAL_TUPLE_DATA_OFFSET;
    tupbodylen = tuple->t_len - MINIMAL_TUPLE_DATA_OFFSET;

    /* total on-disk footprint: */
    tuplen = tupbodylen + sizeof(int);

    LogicalTapeWrite(lts, tapenum, (void *)&tuplen, sizeof(tuplen));
    LogicalTapeWrite(lts, tapenum, (void *)tupbody, tupbodylen);
}

/*
 * Wirte a MinimalTuple from current position
 * 
 * Returns NULL if no more tuples
 */
static MinimalTuple readMinimalTuple(SortGroupStatePriv *state, TupleIter *iter)
{
    GroupNode *group;
    MinimalTuple tuple;
    unsigned int len;
    unsigned int tupbodylen;
    unsigned int tuplen;
    char *tupbody;
    size_t bytes;
    MemoryContext oldcontext = MemoryContextSwitchTo(state->maincontext);

    group = iter->currentNode->group;
    bytes = LogicalTapeRead(state->tapeset, group->tapeNum, &len, sizeof(len));
    if (bytes == 0) {
        /*next group*/
        iter->currentNode = iter->currentNode->backward;
        if (!iter->currentNode) {
            tuple = NULL;
            goto finished;
        }
        group = iter->currentNode->group;
        iter->tapeBlocknum = group->tapeBlocknum;
        iter->tapeOffset = group->tapeOffset;
        iter->memPos = 0;
        groupSortSeekPos(state);

        if (state->new_group_trigger)
            *state->new_group_trigger = true;
        bytes = LogicalTapeRead(state->tapeset, group->tapeNum, &len, sizeof(len));
    }

    if (bytes != sizeof(len)) {
        elog(ERROR, "unexpected end of tape");
    }

    tupbodylen = len - sizeof(int);
    tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET;

    tuple = (MinimalTuple)palloc0(tuplen);
    tuple->t_len = tuplen;
    tupbody = (char *)tuple + MINIMAL_TUPLE_DATA_OFFSET;

    if (LogicalTapeRead(state->tapeset, group->tapeNum, tupbody, tupbodylen) != tupbodylen) {
        elog(ERROR, "unexpected end of tape");
    }

finished:
    MemoryContextSwitchTo(oldcontext);
    return tuple;
}

/*
 * Wirte a MinimalTuple into specified group
 */
static void groupNodePutTupleSlot(SortGroupStatePriv *state, GroupNode *group, TupleTableSlot *slot)
{
    MinimalTuple tuple;

    if (state->holdTupleInMem) {
        MemoryContext oldcontext = MemoryContextSwitchTo(state->tuplecontext);
        tuple = ExecCopySlotMinimalTuple(slot);

        if (group->memTuplesSize >= group->memTuplesCap) {
            int newCap = group->memTuplesCap * 2;
            group->memTuples = (MinimalTuple *)repalloc(group->memTuples, newCap * sizeof(MinimalTuple));
            group->memTuplesCap = newCap;
        }
        group->memTuples[group->memTuplesSize++] = tuple;
        MemoryContextSwitchTo(oldcontext);
    } else {
        Assert(group->tapeNum >= 0);
        tuple = ExecCopySlotMinimalTuple(slot);
        writeMinimalTuple(state->tapeset, group->tapeNum, tuple);
        pfree(tuple);
    }
}

/*
 * Read a MinimalTuple from SortGroupStatePriv
 *
 * Returns flase if no more tuples
 */
static bool groupSortGetTupleSlot(SortGroupStatePriv *state, TupleTableSlot *slot)
{
    TupleIter *iter = &state->iter;
    MinimalTuple tuple;

    if (!iter->currentNode) {
        ExecClearTuple(slot);
        return false;
    }

    if (state->holdTupleInMem) {
        if (iter->memPos >= iter->currentNode->group->memTuplesSize) {
            /* next group*/
            iter->currentNode = iter->currentNode->backward;
            if (!iter->currentNode) {
                ExecClearTuple(slot);
                return false;
            }
            if (state->new_group_trigger)
                *state->new_group_trigger = true;
            iter->memPos = 0;
        }

        Assert(iter->currentNode);
        Assert(iter->currentNode->group->memTuplesSize > iter->memPos);

        tuple = iter->currentNode->group->memTuples[iter->memPos];
        ExecStoreMinimalTuple(tuple, slot, false);
        iter->memPos++;
        return true;
    }

    /*read MinimalTuple from tape*/
    Assert(iter->currentNode->group->tapeNum >= 0);
    tuple = readMinimalTuple(state, iter);
    if (!tuple) {
        ExecClearTuple(slot);
        return false;
    }

    ExecStoreMinimalTuple(tuple, slot, true);
    return true;
}

/*
 * Function to compare two tuples; result is per qsort() convention, ie:
 * <0, 0, >0 according as a<b, a=b, a>b.
 */
static int groupNodeSlotCmp(const GroupNode *a, const TupleTableSlot *b, SortGroupStatePriv *state)
{
    int nkey;
    SortSupport sortKey = state->sortKeys;
    AttrNumber attno;
    int32 compare;
    HeapTupleData ltup;
    TupleDesc tupDesc;
    Datum datum1;
    bool isnull1;
    Datum datum2;
    bool isnull2;

    Assert(b->tts_nvalid >= state->nKeys);
    tupDesc = state->tupDesc;
    attno = sortKey->ssup_attno;

    /* Compare the leading sort key */
    compare = ApplySortComparator(a->datum1, a->isnull1, b->tts_values[attno - 1], b->tts_isnull[attno - 1], sortKey);
    if (compare != 0)
        return compare;

    /* Compare additional sort keys */
    ltup.t_len = ((MinimalTuple)a->tuple)->t_len + MINIMAL_TUPLE_OFFSET;
    ltup.t_data = (HeapTupleHeader)((char *)a->tuple - MINIMAL_TUPLE_OFFSET);

    sortKey++;
    for (nkey = 1; nkey < state->nKeys; nkey++, sortKey++) {
        attno = sortKey->ssup_attno;

        datum1 = heap_getattr(&ltup, attno, tupDesc, &isnull1);
        datum2 = b->tts_values[attno - 1];
        isnull2 = b->tts_isnull[attno - 1];

        compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if (compare != 0)
            return compare;
    }

    return 0;
}

/* ----------------------------------------------------------------
 *        ExecInitSortGroup
 *
 *        Creates the run-time state information for the SortGroupState node
 *        produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
SortGroupState *ExecInitSortGroup(SortGroup *node, EState *estate, int eflags)
{
    SortGroupState *sortGroupState;

    /*
     * create state structure
     */
    sortGroupState = makeNode(SortGroupState);
    sortGroupState->ss.ps.plan = (Plan *)node;
    sortGroupState->ss.ps.state = estate;
    sortGroupState->ss.ps.ExecProcNode = ExecSortGroup;

    sortGroupState->bound = LONG_MAX; /*unlimited*/
    sortGroupState->sort_Done = false;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));

    outerPlanState(sortGroupState) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * Miscellaneous initialization
     *
     * SortGroupState nodes don't initialize their ExprContexts because they never call
     * ExecQual or ExecProject.
     */

    /*
     * Initialize return slot and type. No need to initialize projection info
     * because this node doesn't do projections.
     */
    ExecInitResultTupleSlot(estate, &sortGroupState->ss.ps);
    ExecInitScanTupleSlot(estate, &sortGroupState->ss);
    sortGroupState->ss.ps.ps_ProjInfo = NULL;

    /*
     * Initialize scan slot and type.
     */
    ExecAssignScanTypeFromOuterPlan(&sortGroupState->ss);
    ExecAssignResultTypeFromTL(&sortGroupState->ss.ps,
                               sortGroupState->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    Assert(sortGroupState->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);
    return sortGroupState;
}


/* ----------------------------------------------------------------
 *  ExecSortGroup
 *
 *  Groups and sorts tuples from the outer subtree of the node using SortGroupStatePriv,
 *  which saves the results in a temporary file or memory. After the
 *  initial call, returns a tuple from the file with each call.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *ExecSortGroup(PlanState *pstate)
{
    SortGroupState *node = castNode(SortGroupState, pstate);
    EState *estate;
    TupleTableSlot *slot;
    SortGroupStatePriv *state = node->state;

    estate = node->ss.ps.state;

    CHECK_FOR_INTERRUPTS();

    /*
     * If first time through, read all tuples from outer plan and pass them to
     * skiplist
     */
    if (unlikely(!node->sort_Done)) {
        PlanState *outerNode;
        TupleDesc tupDesc;
        SortGroup *plannode = (SortGroup *)node->ss.ps.plan;

        /*
         * Want to scan subplan in the forward direction while creating the
         * sorted data.
         */
        estate->es_direction = ForwardScanDirection;

        outerNode = outerPlanState(node);
        tupDesc = ExecGetResultType(outerNode);

        state = groupSortBegin(tupDesc, 
                               plannode->numCols, 
                               plannode->sortColIdx, 
                               plannode->sortOperators,
                               plannode->collations, 
                               plannode->nullsFirst, 
                               node->bound, 
                               u_sess->attr.attr_memory.work_mem);
        state->new_group_trigger = node->new_group_trigger;
        /*
         * Scan the subplan and feed all the tuples to SortGroupStatePriv.
         */
        for (;;) {
            slot = ExecProcNode(outerNode);

            if (TupIsNull(slot))
                break;

            groupSortPutTupleslot(state, slot);
        }

        /*
         * finally set the sorted flag to true
         */
        groupSortFinished(state);
        node->sort_Done = true;
        node->state = state;
        dumpSortGroupState(node);   /* dump memory or tape states info */
    }

    /*
     * Get the first or next tuple from SortGroupStatePriv. Returns NULL if no more
     * tuples.  Note that we only rely on slot tuple remaining valid until the
     * next fetch from the SortGroupStatePriv.
     */
    slot = node->ss.ps.ps_ResultTupleSlot;
    groupSortGetTupleSlot(state, slot);
    return slot;
}

/*
 * groupSortEnd
 *
 *    Release resources and clean up.
 */
static void groupSortEnd(SortGroupStatePriv *state)
{
    if (state->tapeset)
        LogicalTapeSetClose(state->tapeset);
    /*
     * Free the main memory context, including the SortGroupStatePriv struct
     * itself.
     */
    MemoryContextDelete(state->maincontext);
}


/* ----------------------------------------------------------------
 * ExecEndSortGroup(node)
 * ----------------------------------------------------------------
 */
void ExecEndSortGroup(SortGroupState *node)
{
    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    /* must drop pointer to sort result tuple */
    ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    /*
     * Release SortGroupStatePriv resources
     */
    if (node->state)
        groupSortEnd(node->state);
    node->state = NULL;

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *    dumpSortGroupState
 *
 * save the space used information into SortGroupState, 
 * This information is used when EXPLAN ANALYZE.
 * ----------------------------------------------------------------
 */
static void dumpSortGroupState(SortGroupState *state)
{
    SortGroupStatePriv *priv = state->state;
    if (priv->holdTupleInMem) {
        state->spaceType = "Memory";
        state->spaceUsed = groupSortUsedMemory(priv);
    } else {
        state->spaceType = "Disk";
        state->spaceUsed = LogicalTapeSetBlocks(priv->tapeset) * BLCKSZ;
    }
}

void ExecReScanSortGroup(SortGroupState *node)
{
    PlanState *outerPlan = outerPlanState(node);

    /*
     * If we haven't sorted yet, just return. If outerplan's chgParam is not
     * NULL then it will be re-scanned by ExecProcNode, else no reason to
     * re-scan it at all.
     */
    if (!node->sort_Done)
        return;

    /* must drop pointer to sort result tuple */
    ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    /*
     * If subnode is to be rescanned then we forget previous sort results; we
     * have to re-read the subplan and re-sort.  Also must re-sort if the
     * bounded-sort parameters changed.
     *
     * Otherwise we can just rewind and rescan the sorted output.
     */
    if (outerPlan->chgParam != NULL || node->bound != node->state->max_groups) {
        node->sort_Done = false;
        groupSortEnd(node->state);
        node->state = NULL;

        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (outerPlan->chgParam == NULL)
            ExecReScan(outerPlan);
    } else {
        initGroupIter(node->state);
        dumpSortGroupState(node);
    }
}