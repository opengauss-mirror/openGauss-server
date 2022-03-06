/* -------------------------------------------------------------------------
 *
 * ubtsearch.cpp
 *	  Search code for openGauss btrees.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/ubtree/ubtsearch.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/relscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "catalog/pg_proc.h"

static bool UBTreeReadPage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum);
static void UBTreeSaveItem(BTScanOpaque so, int itemIndex, OffsetNumber offnum, const IndexTuple itup,
    Oid partOid, bool wantItup, bool needRecheck);
static bool UBTreeStepPage(IndexScanDesc scan, ScanDirection dir);
static bool UBTreeEndPoint(IndexScanDesc scan, ScanDirection dir);

const uint16 INVALID_TUPLE_OFFSET = (uint16)0xa5a5;

/*
 *	UBTreeSearch() -- Search the tree for a particular scankey,
 *		or more precisely for the first leaf page it could be on.
 *
 * The passed scankey must be an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * When nextkey is false (the usual case), we are looking for the first
 * item >= scankey.  When nextkey is true, we are looking for the first
 * item strictly greater than scankey.
 *
 * Return value is a stack of parent-page pointers.  *bufP is set to the
 * address of the leaf-page buffer, which is read-locked and pinned.
 * No locks are held on the parent pages, however!
 *
 * The returned buffer is locked according to access parameter.  Additionally,
 * access = BT_WRITE will allow an empty root page to be created and returned.
 * When access = BT_READ, an empty index will result in *bufP being set to
 * InvalidBuffer.  Also, in BT_WRITE mode, any incomplete splits encountered
 * during the search will be finished
 */
BTStack UBTreeSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack)
{
    BTStack stack_in = NULL;
    int pageAccess = BT_READ;

    /* Get the root page to start with */
    *bufP = UBTreeGetRoot(rel, access);

    /* If index is empty and access = BT_READ, no root page is created. */
    if (SECUREC_UNLIKELY(!BufferIsValid(*bufP)))
        return (BTStack)NULL;

    /* Loop iterates once per level descended in the tree */
    for (;;) {
        Page page;
        UBTPageOpaqueInternal opaque;
        OffsetNumber offnum;
        ItemId itemid;
        IndexTuple itup;
        BlockNumber blkno;
        BlockNumber par_blkno;
        BTStack new_stack = NULL;

        /*
         * Race -- the page we just grabbed may have split since we read its
         * pointer in the parent (or metapage).  If it has, we may need to
         * move right to its new sibling.  Do that.
         * In write-mode, allow _bt_moveright to finish any incomplete splits
         * along the way.  Strictly speaking, we'd only need to finish an
         * incomplete split on the leaf page we're about to insert to, not on
         * any of the upper levels (they are taken care of in _bt_getstackbuf,
         * if the leaf page is split and we insert to the parent page).  But
         * this is a good opportunity to finish splits of internal pages too.
         */
        *bufP = UBTreeMoveRight(rel, key, *bufP, (access == BT_WRITE), stack_in, pageAccess);

        /* if this is a leaf page, we're done */
        page = BufferGetPage(*bufP);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (P_ISLEAF(opaque))
            break;

        /*
         * Find the appropriate item on the internal page, and get the child
         * page that it points to.
         */
        offnum = UBTreeBinarySearch(rel, key, *bufP, false);
        itemid = PageGetItemId(page, offnum);
        itup = (IndexTuple) PageGetItem(page, itemid);
        Assert(UBTreeTupleIsPivot(itup) || !key->heapkeyspace);
        blkno = UBTreeTupleGetDownLink(itup);
        par_blkno = BufferGetBlockNumber(*bufP);

        /*
         * We need to save the location of the index entry we chose in the
         * parent page on a stack. In case we split the tree, we'll use the
         * stack to work back up to the parent page.  We also save the actual
         * downlink (TID) to uniquely identify the index entry, in case it
         * moves right while we're working lower in the tree.  See the paper
         * by Lehman and Yao for how this is detected and handled. (We use the
         * child link during the second half of a page split -- if caller ends
         * up splitting the child it usually ends up inserting a new pivot
         * tuple for child's new right sibling immediately after the original
         * bts_offset offset recorded here.  The downlink block will be needed
         * to check if bts_offset remains the position of this same pivot
         * tuple.)
         */
        if (needStack) {
            new_stack = (BTStack)palloc(sizeof(BTStackData));
            new_stack->bts_blkno = par_blkno;
            new_stack->bts_offset = offnum;
            new_stack->bts_btentry = blkno;
            new_stack->bts_parent = stack_in;
        }

        /*
         * Page level 1 is lowest non-leaf page level prior to leaves.  So,
         * if we're on the level 1 and asked to lock leaf page in write mode,
         * then lock next page in write mode, because it must be a leaf.
         */
        if (opaque->btpo.level == 1 && access == BT_WRITE)
            pageAccess = BT_WRITE;

        /* drop the read lock on the parent page, acquire one on the child */
        *bufP = _bt_relandgetbuf(rel, *bufP, blkno, pageAccess);

        /* okay, all set to move down a level */
        stack_in = new_stack;
    }

    /*
     * If we're asked to lock leaf in write mode, but didn't manage to, then
     * relock.  This should only happen when the root page is a leaf page (and
     * the only page in the index other than the metapage).
     */
    if (access == BT_WRITE && pageAccess == BT_READ) {
        /* trade in our read lock for a write lock */
        LockBuffer(*bufP, BUFFER_LOCK_UNLOCK);
        LockBuffer(*bufP, BT_WRITE);

        /*
         * If the page was split between the time that we surrendered our read
         * lock and acquired our write lock, then this page may no longer be
         * the right place for the key we want to insert.  In this case, we
         * need to move right in the tree.  See Lehman and Yao for an
         * excruciatingly precise description.
         */
        *bufP = UBTreeMoveRight(rel, key, *bufP, true, stack_in, BT_WRITE);
    }

    return stack_in;
}

/*
 *	UBTreeMoveRight() -- move right in the btree if necessary.
 *
 * When we follow a pointer to reach a page, it is possible that
 * the page has changed in the meanwhile.  If this happens, we're
 * guaranteed that the page has "split right" -- that is, that any
 * data that appeared on the page originally is either on the page
 * or strictly to the right of it.
 *
 * This routine decides whether or not we need to move right in the
 * tree by examining the high key entry on the page.  If that entry
 * is strictly less than the scankey, or <= the scankey in the nextkey=true
 * case, then we followed the wrong link and we need to move right.
 *
 * The passed scankey must be an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * When nextkey is false (the usual case), we are looking for the first
 * item >= scankey.  When nextkey is true, we are looking for the first
 * item strictly greater than scankey.
 *
 * If forupdate is true, we will attempt to finish any incomplete splits
 * that we encounter.  This is required when locking a target page for an
 * insertion, because we don't allow inserting on a page before the split
 * is completed.  'stack' is only used if forupdate is true.
 *
 * On entry, we have the buffer pinned and a lock of the type specified by
 * 'access'.  If we move right, we release the buffer and lock and acquire
 * the same on the right sibling.  Return value is the buffer we stop at.
 */
Buffer UBTreeMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate, BTStack stack, int access)
{
    Page page;
    UBTPageOpaqueInternal opaque;
    int32 cmpval;

    /*
     * When nextkey = false (normal case): if the scan key that brought us to
     * this page is > the high key stored on the page, then the page has split
     * and we need to move right.  (pg_upgrade'd !heapkeyspace indexes could
     * have some duplicates to the right as well as the left, but that's
     * something that's only ever dealt with on the leaf level, after
     * _bt_search has found an initial leaf page.)
     *
     * When nextkey = true: move right if the scan key is >= page's high key.
     * (Note that key.scantid cannot be set in this case.)
     *
     * The page could even have split more than once, so scan as far as
     * needed.
     *
     * We also have to move right if we followed a link that brought us to a
     * dead page.
     */
    cmpval = itup_key->nextkey ? 0 : 1;

    for (;;) {
        page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (P_RIGHTMOST(opaque)) {
            break;
        }

        /*
         * Finish any incomplete splits we encounter along the way.
         */
        if (forupdate && P_INCOMPLETE_SPLIT(opaque)) {
            BlockNumber blkno = BufferGetBlockNumber(buf);

            /* upgrade our lock if necessary */
            if (access == BT_READ) {
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                LockBuffer(buf, BT_WRITE);
            }

            if (P_INCOMPLETE_SPLIT(opaque)) {
                UBTreeFinishSplit(rel, buf, stack);
            } else {
                _bt_relbuf(rel, buf);
            }

            /* re-acquire the lock in the right mode, and re-check */
            buf = _bt_getbuf(rel, blkno, access);
            continue;
        }

        if (P_IGNORE(opaque) || UBTreeCompare(rel, itup_key, page, P_HIKEY, InvalidBuffer) >= cmpval) {
            /* step right one page */
            buf = _bt_relandgetbuf(rel, buf, opaque->btpo_next, access);
            continue;
        } else {
            break;
        }
    }

    if (P_IGNORE(opaque))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("fell off the end of index \"%s\"", RelationGetRelationName(rel))));

    return buf;
}

/*
 *	UBTreeBinarySearch() -- Do a binary search for a key on a particular page.
 *
 * The passed scankey must be an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * When nextkey is false (the usual case), we are looking for the first
 * item >= scankey.  When nextkey is true, we are looking for the first
 * item strictly greater than scankey.
 *
 * On a leaf page, _bt_binsrch() returns the OffsetNumber of the first
 * key >= given scankey, or > scankey if nextkey is true.  (NOTE: in
 * particular, this means it is possible to return a value 1 greater than the
 * number of keys on the page, if the scankey is > all keys on the page.)
 *
 * On an internal (non-leaf) page, _bt_binsrch() returns the OffsetNumber
 * of the last key < given scankey, or last key <= given scankey if nextkey
 * is true.  (Since _bt_compare treats the first data key of such a page as
 * minus infinity, there will be at least one key < scankey, so the result
 * always points at one of the keys on the page.)  This key indicates the
 * right place to descend to be sure we find all leaf keys >= given scankey
 * (or leaf keys > given scankey when nextkey is true).
 *
 * This procedure is not responsible for walking right, it just examines
 * the given page.	_bt_binsrch() has no lock or refcount side effects
 * on the buffer.
 */
OffsetNumber UBTreeBinarySearch(Relation rel, BTScanInsert key, Buffer buf, bool fixActiveCount)
{
    Page page;
    UBTPageOpaqueInternal opaque;
    OffsetNumber low, high;
    int32 result, cmpval;

    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* Requesting nextkey semantics while using scantid seems nonsensical */
    Assert(!key->nextkey || key->scantid == NULL);

    low = P_FIRSTDATAKEY(opaque);
    high = PageGetMaxOffsetNumber(page);
    /*
     * If there are no keys on the page, return the first available slot. Note
     * this covers two cases: the page is really empty (no keys), or it
     * contains only a high key.  The latter case is possible after vacuuming.
     * This can never happen on an internal page, however, since they are
     * never empty (an internal page must have children).
     */
    if (unlikely(high < low))
        return low;

    /*
     * Binary search to find the first key on the page >= scan key, or first
     * key > scankey when nextkey is true.
     *
     * For nextkey=false (cmpval=1), the loop invariant is: all slots before
     * 'low' are < scan key, all slots at or after 'high' are >= scan key.
     *
     * For nextkey=true (cmpval=0), the loop invariant is: all slots before
     * 'low' are <= scan key, all slots at or after 'high' are > scan key.
     *
     * We can fall out when high == low.
     */
    high++; /* establish the loop invariant for high */

    cmpval = key->nextkey ? 0 : 1; /* select comparison value */

    /* if we want to fix active tuple count, must pass current buffer to UBTreeCompare */
    Buffer argBuf = (fixActiveCount ? buf : InvalidBuffer);
    while (high > low) {
        OffsetNumber mid = (uint16)((low + high) >> 1);

        /* We have low <= mid < high, so mid points at a real slot */

        result = UBTreeCompare(rel, key, page, mid, argBuf);
        if (result >= cmpval)
            low = mid + 1;
        else
            high = mid;
    }

    /*
     * At this point we have high == low, but be careful: they could point
     * past the last slot on the page.
     *
     * On a leaf page, we always return the first key >= scan key (resp. >
     * scan key), which could be the last slot + 1.
     */
    if (P_ISLEAF(opaque))
        return low;

    /*
     * On a non-leaf page, return the last key < scan key (resp. <= scan key).
     * There must be one if _bt_compare() is playing by the rules.
     */
    Assert(low > P_FIRSTDATAKEY(opaque));

    return OffsetNumberPrev(low);
}

static void UBTreeFixActiveTupleCount(Relation rel, Buffer buf, UBTPageOpaqueInternal opaque,
    IndexTuple itup, ItemId iid)
{
    UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
    TransactionId xmin = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmin);
    TransactionId oldestXmin = InvalidTransactionId;
    GetOldestXminForUndo(&oldestXmin);
    if (TransactionIdPrecedes(xmin, oldestXmin)) {
        if (UBTreeCheckXid(xmin) == XID_ABORTED) {
            ItemIdMarkDead(iid);
            opaque->activeTupleCount--;
            if (opaque->activeTupleCount == 0) {
                UBTreeRecordEmptyPage(rel, BufferGetBlockNumber(buf), opaque->last_delete_xid);
            }
        } else {
            /* freeze this index tuple */
            IndexItemIdSetFrozen(iid);
        }
    }
}

/* ----------
 *	UBTreeCompare() -- Compare scankey to a particular tuple on the page.
 *
 * The passed scankey must be an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 *	keysz: number of key conditions to be checked (might be less than the
 *		number of index columns!)
 *	page/offnum: location of btree item to be compared to.
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *		NULLs in the keys are treated as sortable values.  Therefore
 *		"equality" does not necessarily mean that the item should be
 *		returned to the caller as a matching key!
 *	buf: only passed if we want to fix active tuple count when necessary
 *
 * CRUCIAL NOTE: on a non-leaf page, the first data key is assumed to be
 * "minus infinity": this routine will always claim it is less than the
 * scankey.  The actual key value stored (if any, which there probably isn't)
 * does not matter.  This convention allows us to implement the Lehman and
 * Yao convention that the first down-link pointer is before the first key.
 * See backend/access/nbtree/README for details.
 * ----------
 */
int32 UBTreeCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum, Buffer buf)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /*
     * Check tuple has correct number of attributes.
     */
    Assert(UBTreeCheckNatts(rel, key->heapkeyspace, page, offnum));

    /*
     * Force result ">" if target item is first data item on an internal page
     * --- see NOTE above.
     */
    if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque))
        return 1;

    ItemId iid = PageGetItemId(page, offnum);
    IndexTuple itup = (IndexTuple)PageGetItem(page, iid);
    int ntupatts = UBTreeTupleGetNAtts(itup, rel);

    /*
     * we must hold write lock when passed buf is valid. This means we need to check whether xmin is
     * valid, and fix activeTupleCount if necessary.
     */
    if (BufferIsValid(buf) && P_ISLEAF(opaque) && !UBTreeTupleIsPivot(itup) &&
        !IndexItemIdIsFrozen(iid) && !ItemIdIsDead(iid)) {
        UBTreeFixActiveTupleCount(rel, buf, opaque, itup, iid);
    }

    /*
     * The scan key is set up with the attribute number associated with each
     * term in the key.  It is important that, if the index is multi-key, the
     * scan contain the first k key attributes, and that they be in order.	If
     * you think about how multi-key ordering works, you'll understand why
     * this is.
     *
     * We don't test for violation of this condition here, however.  The
     * initial setup for the index scan had better have gotten it right (see
     * _bt_first).
     */
    int ncmpkey = Min(ntupatts, key->keysz);
    Assert(key->heapkeyspace || ncmpkey == key->keysz);
    ScanKey scankey = key->scankeys;
    for (int i = 1; i <= ncmpkey; i++, scankey++) {
        Datum datum;
        bool isNull = false;
        int32 result;

        Assert(scankey->sk_attno == i);
        datum = index_getattr(itup, scankey->sk_attno, itupdesc, &isNull);

        if (likely((!(scankey->sk_flags & SK_ISNULL)) && !isNull)) {
            /* btint4cmp */
            if (scankey->sk_func.fn_oid == BTINT4CMP_OID) {
                if ((int32)datum != (int32)scankey->sk_argument) {
                    result = ((int32)datum > (int32)scankey->sk_argument) ? 1 : -1;
                } else {
                    continue;
                }
            } else {
                result = DatumGetInt32(
                    FunctionCall2Coll(&scankey->sk_func, scankey->sk_collation, datum, scankey->sk_argument));
                if (result == 0)
                    continue;
            }

            if (!(scankey->sk_flags & SK_BT_DESC))
                result = -result;
        } else {
            if (scankey->sk_flags & SK_ISNULL) { /* key is NULL */
                if (isNull) {
                    continue; /* result = 0 since NULL "=" NULL */
                } else if (scankey->sk_flags & SK_BT_NULLS_FIRST) {
                    result = -1; /* NULL "<" NOT_NULL */
                } else
                    result = 1; /* NULL ">" NOT_NULL */
            } else if (isNull) { /* key is NOT_NULL and item is NULL */
                result = (scankey->sk_flags & SK_BT_NULLS_FIRST) ? 1 : -1;
            }
        }

        /* keys are unequal, return the difference */
        return result;
    }

    /*
     * All non-truncated attributes (other than heap TID) were found to be
     * equal.  Treat truncated attributes as minus infinity when scankey has a
     * key attribute value that would otherwise be compared directly.
     *
     * Note: it doesn't matter if ntupatts includes non-key attributes;
     * scankey won't, so explicitly excluding non-key attributes isn't
     * necessary.
     */
    if (key->keysz > ntupatts)
        return 1;

    /*
     * Use the heap TID attribute and scantid to try to break the tie.  The
     * rules are the same as any other key attribute -- only the
     * representation differs.
     */
    ItemPointer heapTid = UBTreeTupleGetHeapTID(itup);
    if (key->scantid == NULL) {
        /*
         * Most searches have a scankey that is considered greater than a
         * truncated pivot tuple if and when the scankey has equal values for
         * attributes up to and including the least significant untruncated
         * attribute in tuple.
         *
         * For example, if an index has the minimum two attributes (single
         * user key attribute, plus heap TID attribute), and a page's high key
         * is ('foo', -inf), and scankey is ('foo', <omitted>), the search
         * will not descend to the page to the left.  The search will descend
         * right instead.  The truncated attribute in pivot tuple means that
         * all non-pivot tuples on the page to the left are strictly < 'foo',
         * so it isn't necessary to descend left.  In other words, search
         * doesn't have to descend left because it isn't interested in a match
         * that has a heap TID value of -inf.
         *
         * However, some searches (pivotsearch searches) actually require that
         * we descend left when this happens.  -inf is treated as a possible
         * match for omitted scankey attribute(s).  This is needed by page
         * deletion, which must re-find leaf pages that are targets for
         * deletion using their high keys.
         *
         * Note: the heap TID part of the test ensures that scankey is being
         * compared to a pivot tuple with one or more truncated key
         * attributes.
         *
         * Note: pg_upgrade'd !heapkeyspace indexes must always descend to the
         * left here, since they have no heap TID attribute (and cannot have
         * any -inf key values in any case, since truncation can only remove
         * non-key attributes).  !heapkeyspace searches must always be
         * prepared to deal with matches on both sides of the pivot once the
         * leaf level is reached.
         */
        if (key->heapkeyspace && !key->pivotsearch &&
            key->keysz == ntupatts && heapTid == NULL)
            return 1;

        /* All provided scankey arguments found to be equal */
        return 0;
    }

    /*
     * Treat truncated heap TID as minus infinity, since scankey has a key
     * attribute value (scantid) that would otherwise be compared directly
     */
    Assert(key->keysz == IndexRelationGetNumberOfKeyAttributes(rel));
    if (heapTid == NULL)
        return 1;

    Assert(ntupatts >= IndexRelationGetNumberOfKeyAttributes(rel));
    return ItemPointerCompare(key->scantid, heapTid);
}

/*
 *	UBTreeFirst() -- Find the first item in a scan.
 *
 *		We need to be clever about the direction of scan, the search
 *		conditions, and the tree ordering.	We find the first item (or,
 *		if backwards scan, the last item) in the tree that satisfies the
 *		qualifications in the scan key.  On success exit, the page containing
 *		the current index tuple is pinned but not locked, and data about
 *		the matching tuple(s) on the page has been loaded into so->currPos.
 *		scan->xs_ctup.t_self is set to the heap TID of the current tuple,
 *		and if requested, scan->xs_itup points to a copy of the index tuple.
 *
 * If there are no matching items in the index, we return FALSE, with no
 * pins or locks held.
 *
 * Note that scan->keyData[], and the so->keyData[] scankey built from it,
 * are both search-type scankeys (see nbtree/README for more about this).
 * Within this routine, we build a temporary insertion-type scankey to use
 * in locating the scan start position.
 */
bool UBTreeFirst(IndexScanDesc scan, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Buffer buf;
    OffsetNumber offnum;
    StrategyNumber strat;
    bool nextkey = false;
    bool goback = false;
    BTScanInsertData inskey;
    ScanKey startKeys[INDEX_MAX_KEYS];
    ScanKeyData notnullkeys[INDEX_MAX_KEYS];
    int keysCount = 0;
    int i;
    StrategyNumber strat_total;
    BTScanPosItem *currItem = NULL;

    pgstat_count_index_scan(rel);

    /*
     * Examine the scan keys and eliminate any redundant keys; also mark the
     * keys that must be matched to continue the scan.
     */
    _bt_preprocess_keys(scan);

    /*
     * Quit now if _bt_preprocess_keys() discovered that the scan keys can
     * never be satisfied (eg, x == 1 AND x > 2).
     */
    if (!so->qual_ok)
        return false;

    /* ----------
     * Examine the scan keys to discover where we need to start the scan.
     *
     * We want to identify the keys that can be used as starting boundaries;
     * these are =, >, or >= keys for a forward scan or =, <, <= keys for
     * a backwards scan.  We can use keys for multiple attributes so long as
     * the prior attributes had only =, >= (resp. =, <=) keys.	Once we accept
     * a > or < boundary or find an attribute with no boundary (which can be
     * thought of as the same as "> -infinity"), we can't use keys for any
     * attributes to its right, because it would break our simplistic notion
     * of what initial positioning strategy to use.
     *
     * When the scan keys include cross-type operators, _bt_preprocess_keys
     * may not be able to eliminate redundant keys; in such cases we will
     * arbitrarily pick a usable one for each attribute.  This is correct
     * but possibly not optimal behavior.  (For example, with keys like
     * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
     * x=5 would be more efficient.)  Since the situation only arises given
     * a poorly-worded query plus an incomplete opfamily, live with it.
     *
     * When both equality and inequality keys appear for a single attribute
     * (again, only possible when cross-type operators appear), we *must*
     * select one of the equality keys for the starting point, because
     * _bt_checkkeys() will stop the scan as soon as an equality qual fails.
     * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
     * start at x=4, we will fail and stop before reaching x=10.  If multiple
     * equality quals survive preprocessing, however, it doesn't matter which
     * one we use --- by definition, they are either redundant or
     * contradictory.
     *
     * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
     * If the index stores nulls at the end of the index we'll be starting
     * from, and we have no boundary key for the column (which means the key
     * we deduced NOT NULL from is an inequality key that constrains the other
     * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
     * use as a boundary key.  If we didn't do this, we might find ourselves
     * traversing a lot of null entries at the start of the scan.
     *
     * In this loop, row-comparison keys are treated the same as keys on their
     * first (leftmost) columns.  We'll add on lower-order columns of the row
     * comparison below, if possible.
     *
     * The selected scan keys (at most one per index column) are remembered by
     * storing their addresses into the local startKeys[] array.
     * ----------
     */
    strat_total = BTEqualStrategyNumber;
    if (so->numberOfKeys > 0) {
        AttrNumber curattr;
        ScanKey chosen;
        ScanKey impliesNN;
        ScanKey cur;

        /*
         * chosen is the so-far-chosen key for the current attribute, if any.
         * We don't cast the decision in stone until we reach keys for the
         * next attribute.
         */
        curattr = 1;
        chosen = NULL;
        /* Also remember any scankey that implies a NOT NULL constraint */
        impliesNN = NULL;

        /*
         * Loop iterates from 0 to numberOfKeys inclusive; we use the last
         * pass to handle after-last-key processing.  Actual exit from the
         * loop is at one of the "break" statements below.
         */
        for (cur = so->keyData, i = 0;; cur++, i++) {
            if (i >= so->numberOfKeys || cur->sk_attno != curattr) {
                /*
                 * Done looking at keys for curattr.  If we didn't find a
                 * usable boundary key, see if we can deduce a NOT NULL key.
                 */
                if (chosen == NULL && impliesNN != NULL &&
                    ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ? ScanDirectionIsForward(dir) :
                    ScanDirectionIsBackward(dir))) {
                    /* Yes, so build the key in notnullkeys[keysCount] */
                    chosen = &notnullkeys[keysCount];
                    ScanKeyEntryInitialize(chosen,
                                           (SK_SEARCHNOTNULL | SK_ISNULL |
                                            (impliesNN->sk_flags & (SK_BT_DESC | SK_BT_NULLS_FIRST))),
                                           curattr,
                                           ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ? BTGreaterStrategyNumber
                                                                                      : BTLessStrategyNumber),
                                           InvalidOid, InvalidOid, InvalidOid, (Datum)0);
                }

                /*
                 * If we still didn't find a usable boundary key, quit; else
                 * save the boundary key pointer in startKeys.
                 */
                if (chosen == NULL)
                    break;
                startKeys[keysCount++] = chosen;

                /*
                 * Adjust strat_total, and quit if we have stored a > or <
                 * key.
                 */
                strat = chosen->sk_strategy;
                if (strat != BTEqualStrategyNumber) {
                    strat_total = strat;
                    if (strat == BTGreaterStrategyNumber || strat == BTLessStrategyNumber)
                        break;
                }

                /*
                 * Done if that was the last attribute, or if next key is not
                 * in sequence (implying no boundary key is available for the
                 * next attribute).
                 */
                if (i >= so->numberOfKeys || cur->sk_attno != curattr + 1)
                    break;

                /*
                 * Reset for next attr.
                 */
                curattr = cur->sk_attno;
                chosen = NULL;
                impliesNN = NULL;
            }

            /*
             * Can we use this key as a starting boundary for this attr?
             *
             * If not, does it imply a NOT NULL constraint?  (Because
             * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
             * *any* inequality key works for that; we need not test.)
             */
            switch (cur->sk_strategy) {
                case BTLessStrategyNumber:
                case BTLessEqualStrategyNumber:
                    if (chosen == NULL) {
                        if (ScanDirectionIsBackward(dir))
                            chosen = cur;
                        else
                            impliesNN = cur;
                    }
                    break;
                case BTEqualStrategyNumber:
                    /* override any non-equality choice */
                    chosen = cur;
                    break;
                case BTGreaterEqualStrategyNumber:
                case BTGreaterStrategyNumber:
                    if (chosen == NULL) {
                        if (ScanDirectionIsForward(dir))
                            chosen = cur;
                        else
                            impliesNN = cur;
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /*
     * If we found no usable boundary keys, we have to start from one end of
     * the tree.  Walk down that edge to the first or last key, and scan from
     * there.
     */
    if (keysCount == 0)
        return UBTreeEndPoint(scan, dir);

    /*
     * We want to start the scan somewhere within the index.  Set up an
     * insertion scankey we can use to search for the boundary point we
     * identified above.  The insertion scankey is built in the local
     * scankeys[] array, using the keys identified by startKeys[].
     */
    Assert(keysCount <= INDEX_MAX_KEYS);
    for (i = 0; i < keysCount; i++) {
        ScanKey cur = startKeys[i];
        errno_t rc;

        Assert(cur->sk_attno == i + 1);

        if (cur->sk_flags & SK_ROW_HEADER) {
            /*
             * Row comparison header: look to the first row member instead.
             *
             * The member scankeys are already in insertion format (ie, they
             * have sk_func = 3-way-comparison function), but we have to watch
             * out for nulls, which _bt_preprocess_keys didn't check. A null
             * in the first row member makes the condition unmatchable, just
             * like qual_ok = false.
             */
            ScanKey subkey = (ScanKey)DatumGetPointer(cur->sk_argument);
            Assert(subkey->sk_flags & SK_ROW_MEMBER);
            if (subkey->sk_flags & SK_ISNULL)
                return false;
            rc = memcpy_s(inskey.scankeys + i, sizeof(ScanKeyData), subkey, sizeof(ScanKeyData));
            securec_check(rc, "\0", "\0");

            /*
             * If the row comparison is the last positioning key we accepted,
             * try to add additional keys from the lower-order row members.
             * (If we accepted independent conditions on additional index
             * columns, we use those instead --- doesn't seem worth trying to
             * determine which is more restrictive.)  Note that this is OK
             * even if the row comparison is of ">" or "<" type, because the
             * condition applied to all but the last row member is effectively
             * ">=" or "<=", and so the extra keys don't break the positioning
             * scheme.	But, by the same token, if we aren't able to use all
             * the row members, then the part of the row comparison that we
             * did use has to be treated as just a ">=" or "<=" condition, and
             * so we'd better adjust strat_total accordingly.
             */
            if (i == keysCount - 1) {
                bool used_all_subkeys = false;

                Assert(!(subkey->sk_flags & SK_ROW_END));
                for (;;) {
                    subkey++;
                    Assert(subkey->sk_flags & SK_ROW_MEMBER);
                    if (subkey->sk_attno != keysCount + 1)
                        break; /* out-of-sequence, can't use it */
                    if (subkey->sk_strategy != cur->sk_strategy)
                        break; /* wrong direction, can't use it */
                    if (subkey->sk_flags & SK_ISNULL)
                        break; /* can't use null keys */
                    Assert(keysCount < INDEX_MAX_KEYS);
                    rc = memcpy_s(inskey.scankeys + keysCount, sizeof(ScanKeyData), subkey, sizeof(ScanKeyData));
                    securec_check(rc, "\0", "\0");
                    keysCount++;
                    if (subkey->sk_flags & SK_ROW_END) {
                        used_all_subkeys = true;
                        break;
                    }
                }
                if (!used_all_subkeys) {
                    switch (strat_total) {
                        case BTLessStrategyNumber:
                            strat_total = BTLessEqualStrategyNumber;
                            break;
                        case BTGreaterStrategyNumber:
                            strat_total = BTGreaterEqualStrategyNumber;
                            break;
                        default:
                            break;
                    }
                }
                break; /* done with outer loop */
            }
        } else {
            /*
             * Ordinary comparison key.  Transform the search-style scan key
             * to an insertion scan key by replacing the sk_func with the
             * appropriate btree comparison function.
             *
             * If scankey operator is not a cross-type comparison, we can use
             * the cached comparison function; otherwise gotta look it up in
             * the catalogs.  (That can't lead to infinite recursion, since no
             * indexscan initiated by syscache lookup will use cross-data-type
             * operators.)
             *
             * We support the convention that sk_subtype == InvalidOid means
             * the opclass input type; this is a hack to simplify life for ScanKeyInit().
             */
            if (cur->sk_subtype == rel->rd_opcintype[i] || cur->sk_subtype == InvalidOid) {
                FmgrInfo *procinfo = NULL;

                procinfo = index_getprocinfo(rel, cur->sk_attno, BTORDER_PROC);
                ScanKeyEntryInitializeWithInfo(inskey.scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy,
                                               cur->sk_subtype, cur->sk_collation, procinfo, cur->sk_argument);
            } else {
                RegProcedure cmp_proc;

                cmp_proc = get_opfamily_proc(rel->rd_opfamily[i], rel->rd_opcintype[i], cur->sk_subtype, BTORDER_PROC);
                if (SECUREC_UNLIKELY(!RegProcedureIsValid(cmp_proc)))
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("missing support function %d(%u,%u) for attribute %d of index \"%s\"",
                                   BTORDER_PROC, rel->rd_opcintype[i], cur->sk_subtype, cur->sk_attno,
                                   RelationGetRelationName(rel))));
                ScanKeyEntryInitialize(inskey.scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy,
                                       cur->sk_subtype, cur->sk_collation, cmp_proc, cur->sk_argument);
            }
        }
    }

    /* ----------
     * Examine the selected initial-positioning strategy to determine exactly
     * where we need to start the scan, and set flag variables to control the
     * code below.
     *
     * If nextkey = false, _bt_search and _bt_binsrch will locate the first
     * item >= scan key.  If nextkey = true, they will locate the first
     * item > scan key.
     *
     * If goback = true, we will then step back one item, while if
     * goback = false, we will start the scan on the located item.
     * ----------
     */
    switch (strat_total) {
        case BTLessStrategyNumber:

            /*
             * Find first item >= scankey, then back up one to arrive at last
             * item < scankey.	(Note: this positioning strategy is only used
             * for a backward scan, so that is always the correct starting
             * position.)
             */
            nextkey = false;
            goback = true;
            break;

        case BTLessEqualStrategyNumber:

            /*
             * Find first item > scankey, then back up one to arrive at last
             * item <= scankey.  (Note: this positioning strategy is only used
             * for a backward scan, so that is always the correct starting
             * position.)
             */
            nextkey = true;
            goback = true;
            break;

        case BTEqualStrategyNumber:

            /*
             * If a backward scan was specified, need to start with last equal
             * item not first one.
             */
            if (ScanDirectionIsBackward(dir)) {
                /*
                 * This is the same as the <= strategy.  We will check at the
                 * end whether the found item is actually =.
                 */
                nextkey = true;
                goback = true;
            } else {
                /*
                 * This is the same as the >= strategy.  We will check at the
                 * end whether the found item is actually =.
                 */
                nextkey = false;
                goback = false;
            }
            break;

        case BTGreaterEqualStrategyNumber:

            /*
             * Find first item >= scankey.	(This is only used for forward
             * scans.)
             */
            nextkey = false;
            goback = false;
            break;

        case BTGreaterStrategyNumber:

            /*
             * Find first item > scankey.  (This is only used for forward
             * scans.)
             */
            nextkey = true;
            goback = false;
            break;

        default:
            /* can't get here, but keep compiler quiet */
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("unrecognized strat_total: %d", (int)strat_total)));
            return false;
    }

    /* Initialize remaining insertion scan key fields */
    inskey.heapkeyspace = true; /* ubtree always take TID as tie-breaker */

    inskey.anynullkeys = false; /* unused */
    inskey.nextkey = nextkey;
    inskey.pivotsearch = false;
    inskey.scantid = NULL;
    inskey.keysz = keysCount;

    /*
     * Use the manufactured insertion scan key to descend the tree and
     * position ourselves on the target leaf page.
     */
    (void)UBTreeSearch(rel, &inskey, &buf, BT_READ, false);

    /*
     * remember which buffer we have pinned, if any
     */
    so->currPos.buf = buf;

    if (!BufferIsValid(buf)) {
        /*
         * We only get here if the index is completely empty. Lock relation
         * because nothing finer to lock exists.
         */
        PredicateLockRelation(rel, scan->xs_snapshot);
        return false;
    } else
        PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);

    /* initialize moreLeft/moreRight appropriately for scan direction */
    if (ScanDirectionIsForward(dir)) {
        so->currPos.moreLeft = false;
        so->currPos.moreRight = true;
    } else {
        so->currPos.moreLeft = true;
        so->currPos.moreRight = false;
    }
    so->numKilled = 0;      /* just paranoia */
    so->markItemIndex = -1; /* ditto */

    /* position to the precise item on the page */
    offnum = UBTreeBinarySearch(rel, &inskey, buf, false);

    /*
     * If nextkey = false, we are positioned at the first item >= scan key, or
     * possibly at the end of a page on which all the existing items are less
     * than the scan key and we know that everything on later pages is greater
     * than or equal to scan key.
     *
     * If nextkey = true, we are positioned at the first item > scan key, or
     * possibly at the end of a page on which all the existing items are less
     * than or equal to the scan key and we know that everything on later
     * pages is greater than scan key.
     *
     * The actually desired starting point is either this item or the prior
     * one, or in the end-of-page case it's the first item on the next page or
     * the last item on this page.	Adjust the starting offset if needed. (If
     * this results in an offset before the first item or after the last one,
     * _bt_readpage will report no items found, and then we'll step to the
     * next page as needed.)
     */
    if (goback)
        offnum = OffsetNumberPrev(offnum);

    /*
     * Now load data from the first page of the scan.
     */
    if (!UBTreeReadPage(scan, dir, offnum)) {
        /*
         * There's no actually-matching data on this page.  Try to advance to
         * the next page.  Return false if there's no matching data at all.
         */
        if (!UBTreeStepPage(scan, dir))
            return false;
    }

    /* Drop the lock, but not pin, on the current page */
    LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);

    /* OK, itemIndex says what to return */
    currItem = &so->currPos.items[so->currPos.itemIndex];

    scan->xs_ctup.t_self = currItem->heapTid;
    scan->xs_recheck_itup = false;
    if (scan->xs_want_itup || currItem->needRecheck) {
        /* in this case, currTuples and tupleOffset must be valid. */
        Assert(so->currTuples != NULL && currItem->tupleOffset != INVALID_TUPLE_OFFSET);
        scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);
        /* if we can't tell whether this tuple is visible with out CID, we must fetch UHeapTuple to recheck. */
        scan->xs_recheck_itup = currItem->needRecheck;
    }

    if (scan->xs_want_ext_oid && GPIScanCheckPartOid(scan->xs_gpi_scan, currItem->partitionOid)) {
        GPISetCurrPartOid(scan->xs_gpi_scan, currItem->partitionOid);
    }

    return true;
}

/*
 *	UBTreeNext() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which is pinned
 *		but not locked, and so->currPos.itemIndex identifies which item was
 *		previously returned.
 *
 *		On successful exit, scan->xs_ctup.t_self is set to the TID of the
 *		next heap tuple, and if requested, scan->xs_itup points to a copy of
 *		the index tuple.  so->currPos is updated as needed.
 *
 *		On failure exit (no more tuples), we release pin and set
 *		so->currPos.buf to InvalidBuffer.
 */
bool UBTreeNext(IndexScanDesc scan, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    BTScanPosItem *currItem = NULL;

    /*
     * Advance to next tuple on current page; or if there's no more, try to
     * step to the next page with data.
     */
    if (ScanDirectionIsForward(dir)) {
        if (++so->currPos.itemIndex > so->currPos.lastItem) {
            /* We must acquire lock before applying _bt_steppage */
            Assert(BufferIsValid(so->currPos.buf));
            LockBuffer(so->currPos.buf, BT_READ);
            if (!UBTreeStepPage(scan, dir))
                return false;
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    } else {
        if (--so->currPos.itemIndex < so->currPos.firstItem) {
            /* We must acquire lock before applying _bt_steppage */
            Assert(BufferIsValid(so->currPos.buf));
            LockBuffer(so->currPos.buf, BT_READ);
            if (!UBTreeStepPage(scan, dir))
                return false;
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    }

    /* OK, itemIndex says what to return */
    currItem = &so->currPos.items[so->currPos.itemIndex];

    scan->xs_ctup.t_self = currItem->heapTid;
    scan->xs_recheck_itup = false;
    if (scan->xs_want_itup || currItem->needRecheck) {
        /* in this case, currTuples and tupleOffset must be valid. */
        Assert(so->currTuples != NULL && currItem->tupleOffset != INVALID_TUPLE_OFFSET);
        scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);
        /* if we can't tell whether this tuple is visible with out CID, we must fetch UHeapTuple to recheck. */
        scan->xs_recheck_itup = currItem->needRecheck;
    }

    if (scan->xs_want_ext_oid && GPIScanCheckPartOid(scan->xs_gpi_scan, currItem->partitionOid)) {
        GPISetCurrPartOid(scan->xs_gpi_scan, currItem->partitionOid);
    }

    return true;
}

/*
 *	UBTreeReadPage() -- Load data from current index page into so->currPos
 *
 * Caller must have pinned and read-locked so->currPos.buf; the buffer's state
 * is not changed here.  Also, currPos.moreLeft and moreRight must be valid;
 * they are updated as appropriate.  All other fields of so->currPos are
 * initialized from scratch here.
 *
 * We scan the current page starting at offnum and moving in the indicated
 * direction.  All items matching the scan keys are loaded into currPos.items.
 * moreLeft or moreRight (as appropriate) is cleared if _bt_checkkeys reports
 * that there can be no more matching tuples in the current scan direction.
 *
 * Returns true if any matching items found on the page, false if none.
 */
static bool UBTreeReadPage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Page page;
    UBTPageOpaqueInternal opaque;
    OffsetNumber minoff;
    OffsetNumber maxoff;
    int itemIndex;
    IndexTuple itup;
    bool continuescan = true;
    TupleDesc tupdesc;
    AttrNumber PartitionOidAttr;
    Oid partOid = InvalidOid;
    Oid heapOid = IndexScanGetPartHeapOid(scan);
    TransactionId xidBase;
    bool isnull = false;

    tupdesc = RelationGetDescr(scan->indexRelation);
    PartitionOidAttr = IndexRelationGetNumberOfAttributes(scan->indexRelation);

    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(so->currPos.buf));

    page = BufferGetPage(so->currPos.buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);
    xidBase = opaque->xid_base;

    /*
     * we must save the page's right-link while scanning it; this tells us
     * where to step right to after we're done with these items.  There is no
     * corresponding need for the left-link, since splits always go right.
     */
    so->currPos.nextPage = opaque->btpo_next;

    /* initialize tuple workspace to empty */
    so->currPos.nextTupleOffset = 0;

    so->currPos.xid_base = xidBase;

    if (ScanDirectionIsForward(dir)) {
        /* load items[] in ascending order */
        itemIndex = 0;

        offnum = Max(offnum, minoff);

        while (offnum <= maxoff) {
            bool needRecheck = false;
            itup = UBTreeCheckKeys(scan, page, offnum, dir, &continuescan, &needRecheck);
            if (itup != NULL) {
                /* Get partition oid for global partition index */
                isnull = false;
                partOid = scan->xs_want_ext_oid
                          ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                          : heapOid;
                Assert(!isnull);
                /* tuple passes all scan key conditions, so remember it */
                UBTreeSaveItem(so, itemIndex, offnum, itup, partOid, scan->xs_want_itup, needRecheck);
                itemIndex++;
            }
            if (!continuescan) {
                /* there can't be any more matches, so stop */
                so->currPos.moreRight = false;
                break;
            }

            offnum = OffsetNumberNext(offnum);
        }

        Assert(itemIndex <= MaxIndexTuplesPerPage);
        so->currPos.firstItem = 0;
        so->currPos.lastItem = itemIndex - 1;
        so->currPos.itemIndex = 0;
    } else {
        /* load items[] in descending order */
        itemIndex = MaxIndexTuplesPerPage;

        offnum = Min(offnum, maxoff);

        while (offnum >= minoff) {
            bool needRecheck = false;
            itup = UBTreeCheckKeys(scan, page, offnum, dir, &continuescan, &needRecheck);
            if (itup != NULL) {
                isnull = false;
                partOid = scan->xs_want_ext_oid
                          ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                          : heapOid;
                Assert(!isnull);
                /* tuple passes all scan key conditions, so remember it */
                itemIndex--;
                UBTreeSaveItem(so, itemIndex, offnum, itup, partOid, scan->xs_want_itup, needRecheck);
            }
            if (!continuescan) {
                /* there can't be any more matches, so stop */
                so->currPos.moreLeft = false;
                break;
            }

            offnum = OffsetNumberPrev(offnum);
        }

        Assert(itemIndex >= 0);
        so->currPos.firstItem = itemIndex;
        so->currPos.lastItem = MaxIndexTuplesPerPage - 1;
        so->currPos.itemIndex = MaxIndexTuplesPerPage - 1;
    }

    return (so->currPos.firstItem <= so->currPos.lastItem);
}

/* UBTreeSaveItem() -- Save an index item into so->currPos.items[itemIndex] */
static void UBTreeSaveItem(BTScanOpaque so, int itemIndex, OffsetNumber offnum, const IndexTuple itup,
    Oid partOid, bool wantItup, bool needRecheck)
{
    BTScanPosItem *currItem = &so->currPos.items[itemIndex];

    currItem->heapTid = itup->t_tid;
    currItem->indexOffset = offnum;
    currItem->partitionOid = partOid;
    currItem->tupleOffset = INVALID_TUPLE_OFFSET;
    currItem->needRecheck = needRecheck;

    if (needRecheck) {
        if (!so->currTuples) {
            so->currTuples = (char*)palloc(BLCKSZ);
        }
        Size itupsz = IndexTupleSize(itup);
        currItem->tupleOffset = (uint16)so->currPos.nextTupleOffset;
        errno_t rc = memcpy_s(so->currTuples + so->currPos.nextTupleOffset, itupsz, itup, itupsz);
        securec_check(rc, "\0", "\0");
        so->currPos.nextTupleOffset += MAXALIGN(itupsz);
    }

    /* save index tuple for index only scan and other similar cases */
    if (wantItup) {
        IndexTuple output_itup = itup;
        Size itupsz = IndexTupleSize(output_itup);

        /* we may require xid included in output for UStore's multi-version index */
        if (so->xs_want_xid) {
            /* copy and reserve addition 16B for xmin/xmax */
            output_itup = CopyIndexTupleAndReserveSpace(output_itup, sizeof(TransactionId) * 2);
            /* transInfo points to the xmin/xmax field */
            IndexTransInfo *transInfo = (IndexTransInfo *)(((char *)output_itup) + itupsz);
            /* uxid points to the ShortTransactionId xmin/xmax of the original index tuple */
            UstoreIndexXid uxid = (UstoreIndexXid) UstoreIndexTupleGetXid(itup);

            /* set up xid for out put */
            transInfo->xmin = ShortTransactionIdToNormal(so->currPos.xid_base, uxid->xmin);
            transInfo->xmax = ShortTransactionIdToNormal(so->currPos.xid_base, uxid->xmax);

            itupsz = IndexTupleSize(output_itup);
        }

        currItem->tupleOffset = (uint16)so->currPos.nextTupleOffset;
        errno_t rc = memcpy_s(so->currTuples + so->currPos.nextTupleOffset, itupsz, output_itup, itupsz);
        securec_check(rc, "", "");
        so->currPos.nextTupleOffset += MAXALIGN(itupsz);

        if (output_itup != itup) {
            pfree(output_itup);
        }
    }
}

/*
 *	UBTreeStepPage() -- Step to next page containing valid data for scan
 *
 * On entry, so->currPos.buf must be pinned and read-locked.  We'll drop
 * the lock and pin before moving to next page.
 *
 * On success exit, we hold pin and read-lock on the next interesting page,
 * and so->currPos is updated to contain data from that page.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return FALSE.
 */
static bool UBTreeStepPage(IndexScanDesc scan, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Relation rel;
    Page page;
    UBTPageOpaqueInternal opaque;

    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(so->currPos.buf));

    /* Before leaving current page, deal with any killed items */
    if (so->numKilled > 0)
        _bt_killitems(scan, true);

    /*
     * Before we modify currPos, make a copy of the page data if there was a
     * mark position that needs it.
     */
    if (so->markItemIndex >= 0) {
        /* bump pin on current buffer for assignment to mark buffer */
        IncrBufferRefCount(so->currPos.buf);
        errno_t rc =
            memcpy_s(&so->markPos, offsetof(BTScanPosData, items[1]) + so->currPos.lastItem * sizeof(BTScanPosItem),
                &so->currPos, offsetof(BTScanPosData, items[1]) + so->currPos.lastItem * sizeof(BTScanPosItem));
        securec_check(rc, "", "");
        if (so->markTuples) {
            rc = memcpy_s(so->markTuples, (size_t)so->currPos.nextTupleOffset, so->currTuples,
                          (size_t)so->currPos.nextTupleOffset);
            securec_check(rc, "", "");
        }
        so->markPos.itemIndex = so->markItemIndex;
        so->markItemIndex = -1;
    }

    rel = scan->indexRelation;

    if (ScanDirectionIsForward(dir)) {
        /* Walk right to the next page with data */
        /* We must rely on the previously saved nextPage link! */
        BlockNumber blkno = so->currPos.nextPage;

        /* Remember we left a page with data */
        so->currPos.moreLeft = true;

        for (;;) {
            /* release the previous buffer */
            _bt_relbuf(rel, so->currPos.buf);
            so->currPos.buf = InvalidBuffer;
            /* if we're at end of scan, give up */
            if (blkno == P_NONE || !so->currPos.moreRight)
                return false;
            /* check for interrupts while we're not holding any buffer lock */
            CHECK_FOR_INTERRUPTS();
            /* step right one page */
            so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
            /* check for deleted page */
            page = BufferGetPage(so->currPos.buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, blkno, scan->xs_snapshot);
                /* see if there are any matches on this page */
                /* note that this will clear moreRight if we can stop */
                if (UBTreeReadPage(scan, dir, P_FIRSTDATAKEY(opaque)))
                    break;
            }
            /* nope, keep going */
            blkno = opaque->btpo_next;
        }
    } else {
        /* Remember we left a page with data */
        so->currPos.moreRight = true;

        /*
         * Walk left to the next page with data.  This is much more complex
         * than the walk-right case because of the possibility that the page
         * to our left splits while we are in flight to it, plus the
         * possibility that the page we were on gets deleted after we leave
         * it.	See nbtree/README for details.
         */
        for (;;) {
            /* Done if we know there are no matching keys to the left */
            if (!so->currPos.moreLeft) {
                _bt_relbuf(rel, so->currPos.buf);
                so->currPos.buf = InvalidBuffer;
                return false;
            }

            /* Step to next physical page */
            so->currPos.buf = _bt_walk_left(rel, so->currPos.buf);

            /* if we're physically at end of index, return failure */
            if (so->currPos.buf == InvalidBuffer)
                return false;

            /*
             * Okay, we managed to move left to a non-deleted page. Done if
             * it's not half-dead and contains matching tuples. Else loop back
             * and do it all again.
             */
            page = BufferGetPage(so->currPos.buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
                /* see if there are any matches on this page */
                /* note that this will clear moreLeft if we can stop */
                if (UBTreeReadPage(scan, dir, PageGetMaxOffsetNumber(page)))
                    break;
            }
        }
    }

    return true;
}

/*
 * UBTreeGetEndPoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().	We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
Buffer UBTreeGetEndPoint(Relation rel, uint32 level, bool rightmost)
{
    Buffer buf;
    Page page;
    UBTPageOpaqueInternal opaque;
    OffsetNumber offnum;
    BlockNumber blkno;
    IndexTuple itup;

    /*
     * If we are looking for a leaf page, okay to descend from fast root;
     * otherwise better descend from true root.  (There is no point in being
     * smarter about intermediate levels.)
     */
    if (level == 0)
        buf = UBTreeGetRoot(rel, BT_READ);
    else
        buf = _bt_gettrueroot(rel);

    if (!BufferIsValid(buf))
        return InvalidBuffer;

    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    for (;;) {
        /*
         * If we landed on a deleted page, step right to find a live page
         * (there must be one).  Also, if we want the rightmost page, step
         * right if needed to get to it (this could happen if the page split
         * since we obtained a pointer to it).
         */
        while (P_IGNORE(opaque) || (rightmost && !P_RIGHTMOST(opaque))) {
            blkno = opaque->btpo_next;
            if (blkno == P_NONE)
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("fell off the end of index \"%s\"", RelationGetRelationName(rel))));
            buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
            page = BufferGetPage(buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        }

        /* Done? */
        if (opaque->btpo.level == level)
            break;
        if (opaque->btpo.level < level)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("btree level %u not found in index \"%s\"", level, RelationGetRelationName(rel))));

        /* Descend to leftmost or rightmost child page */
        if (rightmost)
            offnum = PageGetMaxOffsetNumber(page);
        else
            offnum = P_FIRSTDATAKEY(opaque);

        itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
        blkno = UBTreeTupleGetDownLink(itup);
        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    }

    return buf;
}

/*
 *	UBTreeEndPoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _bt_first().
 */
static bool UBTreeEndPoint(IndexScanDesc scan, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Buffer buf;
    Page page;
    UBTPageOpaqueInternal opaque;
    OffsetNumber start;
    BTScanPosItem *currItem = NULL;

    /*
     * Scan down to the leftmost or rightmost leaf page.  This is a simplified
     * version of _bt_search().  We don't maintain a stack since we know we
     * won't need it.
     */
    buf = UBTreeGetEndPoint(rel, 0, ScanDirectionIsBackward(dir));
    if (!BufferIsValid(buf)) {
        /*
         * Empty index. Lock the whole relation, as nothing finer to lock
         * exists.
         */
        PredicateLockRelation(rel, scan->xs_snapshot);
        so->currPos.buf = InvalidBuffer;
        return false;
    }

    PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);
    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    Assert(P_ISLEAF(opaque));

    if (ScanDirectionIsForward(dir)) {
        /* There could be dead pages to the left, so not this: */
        start = P_FIRSTDATAKEY(opaque);
    } else if (ScanDirectionIsBackward(dir)) {
        Assert(P_RIGHTMOST(opaque));

        start = PageGetMaxOffsetNumber(page);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid scan direction: %d", (int)dir)));
        start = 0; /* keep compiler quiet */
    }

    /* remember which buffer we have pinned */
    so->currPos.buf = buf;

    /* initialize moreLeft/moreRight appropriately for scan direction */
    if (ScanDirectionIsForward(dir)) {
        so->currPos.moreLeft = false;
        so->currPos.moreRight = true;
    } else {
        so->currPos.moreLeft = true;
        so->currPos.moreRight = false;
    }
    so->numKilled = 0;      /* just paranoia */
    so->markItemIndex = -1; /* ditto */

    /*
     * Now load data from the first page of the scan.
     */
    if (!UBTreeReadPage(scan, dir, start)) {
        /*
         * There's no actually-matching data on this page.  Try to advance to
         * the next page.  Return false if there's no matching data at all.
         */
        if (!UBTreeStepPage(scan, dir))
            return false;
    }

    /* Drop the lock, but not pin, on the current page */
    LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);

    /* OK, itemIndex says what to return */
    currItem = &so->currPos.items[so->currPos.itemIndex];

    scan->xs_ctup.t_self = currItem->heapTid;
    scan->xs_recheck_itup = false;
    if (scan->xs_want_itup || currItem->needRecheck) {
        /* in this case, currTuples and tupleOffset must be valid. */
        Assert(so->currTuples != NULL && currItem->tupleOffset != INVALID_TUPLE_OFFSET);
        scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);
        /* if we can't tell whether this tuple is visible with out CID, we must fetch UHeapTuple to recheck. */
        scan->xs_recheck_itup = currItem->needRecheck;
    }

    if (scan->xs_want_ext_oid && GPIScanCheckPartOid(scan->xs_gpi_scan, currItem->partitionOid)) {
        GPISetCurrPartOid(scan->xs_gpi_scan, currItem->partitionOid);
    }

    return true;
}

bool UBTreeGetTupleInternal(IndexScanDesc scan, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    bool res = false;

    /* btree indexes are never lossy */
    scan->xs_recheck = false;

    /*
     * If we have any array keys, initialize them during first call for a
     * scan.  We can't do this in btrescan because we don't know the scan
     * direction at that time.
     */
    if (so->numArrayKeys && !BTScanPosIsValid(so->currPos)) {
        /* punt if we have any unsatisfiable array keys */
        if (so->numArrayKeys < 0) {
            return false;
        }

        _bt_start_array_keys(scan, dir);
    }

    /* This loop handles advancing to the next array elements, if any */
    do {
        /*
         * If we've already initialized this scan, we can just advance it in
         * the appropriate direction.  If we haven't done so yet, we call
         * _bt_first() to get the first item in the scan.
         */
        if (!BTScanPosIsValid(so->currPos))
            res = UBTreeFirst(scan, dir);
        else {
            /*
             * Check to see if we should kill the previously-fetched tuple.
             */
            if (scan->kill_prior_tuple) {
                /*
                 * Yes, remember it for later. (We'll deal with all such
                 * tuples at once right before leaving the index page.)  The
                 * test for numKilled overrun is not just paranoia: if the
                 * caller reverses direction in the indexscan then the same
                 * item might get entered multiple times. It's not worth
                 * trying to optimize that, so we don't detect it, but instead
                 * just forget any excess entries.
                 */
                if (so->killedItems == NULL)
                    so->killedItems = (int *)palloc(MaxIndexTuplesPerPage * sizeof(int));
                if (so->numKilled < MaxIndexTuplesPerPage)
                    so->killedItems[so->numKilled++] = so->currPos.itemIndex;
            }

            /*
             * Now continue the scan.
             */
            res = UBTreeNext(scan, dir);
        }

        /* If we have a tuple, return it ... */
        if (res)
            break;
        /* ... otherwise see if we have more array keys to deal with */
    } while (so->numArrayKeys && _bt_advance_array_keys(scan, dir));

    return res;
}