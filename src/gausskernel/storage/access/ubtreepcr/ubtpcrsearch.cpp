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
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrinsert.cpp
 *  DML functions for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrinsert.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/relscan.h"
#include "access/ustore/knl_uverify.h"
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

#include "access/ubtreepcr.h"
#include "lib/binaryheap.h"
#include "storage/buf/crbuf.h"
#include "storage/checksum_impl.h"

static bool UBTreePCRReadPage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum);
static void UBTreePCRSaveItem(IndexScanDesc scan, int itemIndex, Page page, OffsetNumber offnum,
    const IndexTuple itup, Oid partOid);
static bool UBTreePCRStepPage(IndexScanDesc scan, ScanDirection dir);
static bool UBTreePCREndPoint(IndexScanDesc scan, ScanDirection dir);
static void BuildCRPage(IndexScanDesc scan, Page crPage, Buffer baseBuffer, CommandId *page_cid);

const uint16 INVALID_TUPLE_OFFSET = (uint16)0xa5a5;

/* thrshold switch scan mode from pbrcr to pcr */
const int SCAN_MODE_SWITCH_THRESHOLD = 10;

/*
 *	UBTreePCRSearch() -- Search the tree for a particular scankey,
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
BTStack UBTreePCRSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack)
{
    BTStack stack_in = NULL;
    int pageAccess = BT_READ;

    /* Get the root page to start with */
    *bufP = UBTreePCRGetRoot(rel, access);

    /* If index is empty and access = BT_READ, no root page is created. */
    if (SECUREC_UNLIKELY(!BufferIsValid(*bufP)))
        return (BTStack)NULL;

    /* Loop iterates once per level descended in the tree */
    for (;;) {
        Page page;
        UBTPCRPageOpaque opaque;
        OffsetNumber offnum;
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
        *bufP = UBTreePCRMoveRight(rel, key, *bufP, (access == BT_WRITE), stack_in, pageAccess);

        /* if this is a leaf page, we're done */
        page = BufferGetPage(*bufP);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (P_ISLEAF(opaque))
            break;

        /*
         * Find the appropriate item on the internal page, and get the child
         * page that it points to.
         */
        offnum = UBTreePCRBinarySearch(rel, key, page);
        itup = UBTreePCRGetIndexTuple(page, offnum);
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
        *bufP = _bt_relandgetbuf(rel, *bufP, blkno, pageAccess, par_blkno);

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
        *bufP = UBTreePCRMoveRight(rel, key, *bufP, true, stack_in, BT_WRITE);
    }

    return stack_in;
}

/*
 *	UBTreePCRMoveRight() -- move right in the btree if necessary.
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
Buffer UBTreePCRMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate, BTStack stack, int access)
{
    Page page;
    UBTPCRPageOpaque opaque;
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
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
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
                UBTreePCRFinishSplit(rel, buf, stack);
            } else {
                _bt_relbuf(rel, buf);
            }

            /* re-acquire the lock in the right mode, and re-check */
            buf = _bt_getbuf(rel, blkno, access);
            continue;
        }

        if (P_IGNORE(opaque) || UBTreePCRCompare(rel, itup_key, page, P_HIKEY) >= cmpval) {
            /* step right one page */
            buf = _bt_relandgetbuf(rel, buf, opaque->btpo_next, access);
            continue;
        } else {
            break;
        }
    }

    if (P_IGNORE(opaque))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("fell off the end of index \"%s\" at blkno %u",
                       RelationGetRelationName(rel), BufferGetBlockNumber(buf))));

    return buf;
}

/*
 *	UBTreePCRBinarySearch() -- Do a binary search for a key on a particular page.
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
OffsetNumber UBTreePCRBinarySearch(Relation rel, BTScanInsert key, Page page)
{
    UBTPCRPageOpaque opaque;
    OffsetNumber low, high;
    int32 result, cmpval;

    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    low = P_FIRSTDATAKEY(opaque);
    high = UBTreePCRPageGetMaxOffsetNumber(page);
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

    while (high > low) {
        OffsetNumber mid = (uint16)((low + high) >> 1);

        /* We have low <= mid < high, so mid points at a real slot */
        result = UBTreePCRCompare(rel, key, page, mid);
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

/* ----------
 *	UBTreePCRCompare() -- Compare scankey to a particular tuple on the page.
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
int32 UBTreePCRCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    /*
     * Check tuple has correct number of attributes.
     */
    Assert(UBTreePCRCheckNatts(rel, key->heapkeyspace, page, offnum));

    /*
     * Force result ">" if target item is first data item on an internal page
     * --- see NOTE above.
     */
    if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque))
        return 1;

    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
    int ntupatts = UBTreeTupleGetNAtts(itup, rel);

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
 *	UBTreePCRFirst() -- Find the first item in a scan.
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
bool UBTreePCRFirst(IndexScanDesc scan, ScanDirection dir)
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
        return UBTreePCREndPoint(scan, dir);

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
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("unrecognized strat_total: %d in index \"%s\"",
                           (int)strat_total, RelationGetRelationName(scan->indexRelation))));
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
    (void)UBTreePCRSearch(rel, &inskey, &buf, BT_READ, false);

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
    offnum = UBTreePCRBinarySearch(rel, &inskey, BufferGetPage(buf));

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
    if (!UBTreePCRReadPage(scan, dir, offnum)) {
        /*
         * There's no actually-matching data on this page.  Try to advance to
         * the next page.  Return false if there's no matching data at all.
         */
        if (!UBTreePCRStepPage(scan, dir))
            return false;
    }

    /* Drop the lock, but not pin, on the current page */
    LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);

    /* OK, itemIndex says what to return */
    currItem = &so->currPos.items[so->currPos.itemIndex];

    scan->xs_ctup.t_self = currItem->heapTid;
    scan->xs_recheck_itup = false;
    if (scan->xs_want_itup) {
        scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);
    }

    if (scan->xs_want_ext_oid && GPIScanCheckPartOid(scan->xs_gpi_scan, currItem->partitionOid)) {
        GPISetCurrPartOid(scan->xs_gpi_scan, currItem->partitionOid);
    }

    return true;
}

/*
 *	UBTreePCRNext() -- Get the next item in a scan.
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
bool UBTreePCRNext(IndexScanDesc scan, ScanDirection dir)
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
            if (!UBTreePCRStepPage(scan, dir))
                return false;
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    } else {
        if (--so->currPos.itemIndex < so->currPos.firstItem) {
            /* We must acquire lock before applying _bt_steppage */
            Assert(BufferIsValid(so->currPos.buf));
            LockBuffer(so->currPos.buf, BT_READ);
            if (!UBTreePCRStepPage(scan, dir))
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

IndexTuple UBTreePCRCheckKeys(IndexScanDesc scan, Page page, OffsetNumber offnum, ScanDirection dir,
    bool *continuescan)
{
    UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
    bool tupleAlive = false;
    bool tupleVisible = true;
    IndexTuple tuple;
    TupleDesc tupdesc;
    BTScanOpaque so = NULL;
    int keysz;
    int ikey;
    ScanKey key;
    bool showAnyTupleMode = u_sess->attr.attr_common.XactReadOnly &&
            u_sess->attr.attr_storage.enable_show_any_tuples;
    
    *continuescan = true; /* default assumption */

    /*
     * If the scan specifies not to return killed tuples, then we treat a
     * killed tuple as not passing the qual.  Most of the time, it's a win to
     * not bother examining the tuple's index keys, but just return
     * immediately with continuescan = true to proceed to the next tuple.
     * However, if this is the last tuple on the page, we should check the
     * index keys to prevent uselessly advancing to the next page.
     */
    if (scan->ignore_killed_tuples && ItemIdIsDead(iid) &&
        (!showAnyTupleMode || UBTreeItemIdHasStorage(iid))) {
        /* return immediately if there are more tuples on the page */
        if (ScanDirectionIsForward(dir)) {
            if (offnum < UBTreePCRPageGetMaxOffsetNumber(page)) {
                return NULL;
            }
        } else {
            UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            if (offnum > P_FIRSTDATAKEY(opaque)) {
                return NULL;
            }
        }

        /*
         * OK, we want to check the keys so we can set continuescan correctly,
         * but we'll return NULL even if the tuple passes the key tests.
         */
        tupleAlive = false;
    } else {
        tupleAlive = true;
    }

    so = (BTScanOpaque)scan->opaque;

    if (ItemIdIsDead(iid)) {
        tupleAlive = false;
    } else if (IsUBTreePCRItemDeleted(iid)) {
        if (so->scanMode == PCR_SCAN_MODE) {
            tupleVisible = showAnyTupleMode;
        } else {
            if (IsUBTreePCRTDReused(iid)) {
                tupleVisible = showAnyTupleMode;
            } else {
                UBTreeTD td = UBTreePCRGetTD(page, iid->lp_td_id);
                if (UBTreePCRTDIsCommited(td) || UBTreePCRTDIsFrozen(td)) {
                    tupleVisible = showAnyTupleMode;
                }
            }
        }
    }

    if (!tupleAlive || !tupleVisible) {
        return NULL;
    }

    tuple = UBTreePCRGetIndexTuple(page, offnum);
    tupdesc = RelationGetDescr(scan->indexRelation);
    keysz = so->numberOfKeys;

    for (key = so->keyData, ikey = 0; ikey < keysz; key++, ikey++) {
        Datum datum;
        bool isNull = false;
        Datum test;

        /* row-comparison keys need special processing */
        if (key->sk_flags & SK_ROW_HEADER) {
            if (_bt_check_rowcompare(key, tuple, tupdesc, dir, continuescan)) {
                continue;
            }
            return NULL;
        }

        datum = index_getattr(tuple, key->sk_attno, tupdesc, &isNull);

        if (key->sk_flags & SK_ISNULL) {
            /* Handle IS NULL/NOT NULL tests */
            if (key->sk_flags & SK_SEARCHNULL) {
                if (isNull) {
                    continue; /* tuple satisfies this qual */
                }
            } else {
                Assert(key->sk_flags & SK_SEARCHNOTNULL);
                if (!isNull) {
                    continue; /* tuple satisfies this qual */
                }
            }

            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             */
            if ((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                *continuescan = false;
            } else if ((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                *continuescan = false;
            }

            return NULL;
        }

        if (isNull) {
            if (key->sk_flags & SK_BT_NULLS_FIRST) {
                /*
                 * Since NULLs are sorted before non-NULLs, we know we have
                 * reached the lower limit of the range of values for this
                 * index attr.	On a backward scan, we can stop if this qual
                 * is one of the "must match" subset.  We can stop regardless
                 * of whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a forward scan, however, we must keep going, because we may
                 * have initially positioned to the start of the index.
                 */
                if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsBackward(dir)) {
                    *continuescan = false;
                }
            } else {
                /*
                 * Since NULLs are sorted after non-NULLs, we know we have
                 * reached the upper limit of the range of values for this
                 * index attr.	On a forward scan, we can stop if this qual is
                 * one of the "must match" subset.	We can stop regardless of
                 * whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a backward scan, however, we must keep going, because we
                 * may have initially positioned to the end of the index.
                 */
                if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsForward(dir)) {
                    *continuescan = false;
                }
            }

            return NULL;
        }

        test = FunctionCall2Coll(&key->sk_func, key->sk_collation, datum, key->sk_argument);
        if (!DatumGetBool(test)) {
            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             *
             * Note: because we stop the scan as soon as any required equality
             * qual fails, it is critical that equality quals be used for the
             * initial positioning in _bt_first() when they are available. See
             * comments in _bt_first().
             */
            if ((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                *continuescan = false;
            } else if ((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                *continuescan = false;
            }

            return NULL;
        }
    }

    if (!tupleAlive || !tupleVisible) {
        return NULL;
    }

    return tuple;
}

static bool IsXminXmaxEqual(IndexScanDesc scan, Page page, OffsetNumber offnum,
    UndoRecPtr urecptr, TransactionId xmax)
{
    Snapshot snapshot = scan->xs_snapshot;
    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
    IndexTuple undoItup;
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetMemoryContext(CurrentMemoryContext);
    urec->SetUrp(urecptr);
    bool xminXmaxEqual = false;

    while (true) {
        UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId, false, NULL);
        if (state == UNDO_TRAVERSAL_ABORT) {
            int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urec->Urp());
            undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: PCR index IsXminXmaxEqual. "
                "LogInfo: undo state %d. "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urec->Urp(), zoneId, PtrGetVal(uzone, GetInsertURecPtr()),
                PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        } else if (state != UNDO_TRAVERSAL_COMPLETE) {
            break;
        }
        if (urec->Xid() != xmax) {
            break;
        }
        undoItup = FetchTupleFromUndoRecord(urec);
        if (UBTreeItupEquals(itup, undoItup)) {
            if (urec->Utype() == UNDO_UBT_INSERT) {
                xminXmaxEqual = true;
            } else if (urec->Utype() == UNDO_UBT_DELETE) {
                xminXmaxEqual = false;
            }
        }
        urec->Reset2Blkprev();
    }

    DELETE_EX(urec);
    return xminXmaxEqual;
}

static bool IndexTupleSatisfiesMvcc(IndexScanDesc scan, Page page, OffsetNumber offnum)
{
    Snapshot snapshot = scan->xs_snapshot;
    UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
    Assert(!ItemIdIsDead(iid));
    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
    uint8 tdid = iid->lp_td_id;
    bool tupleDeleted = IsUBTreePCRItemDeleted(iid);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTuple undoItup;

check_frozen:
    /* tdid is frozen */
    if (tdid == UBTreeFrozenTDSlotId) {
        return !tupleDeleted;
    }
    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    TransactionId xid = td->xactid;

    /* td is frozen */
    if (UBTreePCRTDIsFrozen(td)) {
        return !tupleDeleted;
    }

    /* current xid, check cid */
    if (TransactionIdIsCurrentTransactionId(xid) && !IsUBTreePCRTDReused(iid)) {
        bool cidVisible = false;
        UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
        urec->SetMemoryContext(CurrentMemoryContext);
        urec->SetUrp(td->undoRecPtr);

        while (true) {
            UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
                InvalidTransactionId, false, NULL);
            Assert(state != UNDO_TRAVERSAL_ABORT);
            if (state != UNDO_TRAVERSAL_COMPLETE || td->xactid != urec->Xid()) {
                cidVisible = true;
                break;
            }
            if (urec->Cid() < snapshot->curcid) {
                cidVisible = true;
                break;
            }
            undoItup = FetchTupleFromUndoRecord(urec);
            if (UBTreeItupEquals(itup, undoItup)) {
                if (!tupleDeleted) {
                    /* undorec cid > snapshotcid xmin invisible */
                    Assert(!cidVisible);
                    break;
                } else {
                    /* undorec cid > snapshotcid xmax invisible, continue to check if xmin visible */
                    tupleDeleted = false;
                }
            }
            urec->Reset2Blkprev();
        }
        DELETE_EX(urec);
        return cidVisible != tupleDeleted;
    }

    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
    if (TransactionIdPrecedes(xid, globalRecycleXid) ||
        (TransactionIdPrecedes(xid, globalFrozenXid) && TransactionIdPrecedes(xid, snapshot->xmin)) ||
        (IsUBTreePCRTDReused(iid) && TransactionIdPrecedes(opaque->last_commit_xid, snapshot->xmin))) {
        return !tupleDeleted;
    }

    bool xidVisible;
    volatile CommitSeqNo csn;
    bool looped = false;

loop:
    csn = UBTreePCRTDHasCsn(td) ?
        td->combine.csn : TransactionIdGetCommitSeqNo(xid, false, true, false, snapshot);
    if (COMMITSEQNO_IS_COMMITTED(csn)) {
        xidVisible = csn < snapshot->snapshotcsn;
    } else if (COMMITSEQNO_IS_COMMITTING(csn)) {
        Assert(!looped);
        CommitSeqNo latestCsn = GET_COMMITSEQNO(csn);
        if (latestCsn > snapshot->snapshotcsn) {
            xidVisible = false;
        } else {
            SyncWaitXidEnd(xid, InvalidBuffer, snapshot);
            looped = true;
            goto loop;
        }
    } else {
        /* xid is in-progress or aborted */
        Assert(!looped);
        xidVisible = false;
    }

    if (xidVisible) {
        return !tupleDeleted;
    }

    /* tdxid invisible, check tuple visibility through undo chain */
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetMemoryContext(CurrentMemoryContext);
    urec->SetUrp(td->undoRecPtr);
    bool changeXid = false;
    while (true) {
        UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId, false, NULL);
        if (state == UNDO_TRAVERSAL_ABORT) {
            int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urec->Urp());
            undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: PCR index IndexTupleSatisfiesMvcc. "
                "LogInfo: undo state %d. "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urec->Urp(), zoneId, PtrGetVal(uzone, GetInsertURecPtr()),
                PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        } else if (state != UNDO_TRAVERSAL_COMPLETE) {
            xidVisible = true;
            break;
        }
        if (urec->Xid() != td->xactid) {
            changeXid = true;
        }
        if (changeXid) {
            if (urec->Xid() < snapshot->xmin) {
                xidVisible = true;
                break;
            }
            csn = TransactionIdGetCommitSeqNo(urec->Xid(), true, true, false, scan->xs_snapshot);
            Assert(COMMITSEQNO_IS_COMMITTED(csn));
            if (csn < snapshot->snapshotcsn) {
                xidVisible = true;
                break;
            }
        }
        undoItup = FetchTupleFromUndoRecord(urec);
        if (UBTreeItupEquals(itup, undoItup)) {
            if (!tupleDeleted) {
                Assert(!xidVisible);
                break;
            } else {
                tupleDeleted = false;
                UBTreeUndoInfo undoInfo = FetchUndoInfoFromUndoRecord(urec);
                if (undoInfo->prev_td_id != tdid) {
                    tdid = undoInfo->prev_td_id;
                    DELETE_EX(urec);
                    goto check_frozen;
                }
            }
        }
        urec->Reset2Blkprev();
    }
    DELETE_EX(urec);
    return xidVisible != tupleDeleted;
}

static bool IndexTupleSatisfiesDirty(IndexScanDesc scan, Page page, OffsetNumber offnum)
{
    UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
    Assert(!ItemIdIsDead(iid));
    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
    uint8 tdid = iid->lp_td_id;
    bool tupleDeleted = IsUBTreePCRItemDeleted(iid);

    /* items td id is frozen or reused */
    if (tdid == UBTreeFrozenTDSlotId || IsUBTreePCRTDReused(iid)) {
        return !tupleDeleted;
    }

    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    /* td is frozen */
    if (UBTreePCRTDIsFrozen(td)) {
        return !tupleDeleted;
    }

    TransactionId xid = td->xactid;
    if (TransactionIdIsCurrentTransactionId(xid)) {
        return !tupleDeleted;
    }

    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
    if (TransactionIdPrecedes(xid, globalRecycleXid) ||
        (TransactionIdPrecedes(xid, globalFrozenXid) && TransactionIdPrecedes(xid, scan->xs_snapshot->xmin))) {
        return !tupleDeleted;
    }

    TransactionIdStatus ts = UBTreeCheckXid(xid);
    if (ts == XID_ABORTED) {
        /* xmin aborted, not visible */
        if (!tupleDeleted) {
            return false;
        }
        /* xmax aborted, visible if xmax != xmin */
        return !IsXminXmaxEqual(scan, page, offnum, td->undoRecPtr, xid);
    } else if (ts == XID_INPROGRESS) {
        return true;
    }

    return !tupleDeleted;
}

static bool IndexTupleSatisfiesAnyAndToast(IndexScanDesc scan, Page page, OffsetNumber offnum)
{
    UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
    uint8 tdid = iid->lp_td_id;
    bool tupleDeleted = IsUBTreePCRItemDeleted(iid);
    /* items td id is frozen */
    if (tdid == UBTreeFrozenTDSlotId) {
        return !tupleDeleted;
    }
    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    /* td is frozen */
    if (UBTreePCRTDIsFrozen(td)) {
        return !tupleDeleted;
    }
    /* for inprogress and committed xid, tuple is always visible */
    return true;
}

static bool IndexTupleSatisfiesVisibility(IndexScanDesc scan, Page page, OffsetNumber offnum)
{
    Snapshot snapshot = scan->xs_snapshot;
    switch (snapshot->satisfies) {
        case SNAPSHOT_MVCC:
        case SNAPSHOT_VERSION_MVCC:
            return IndexTupleSatisfiesMvcc(scan, page, offnum);
        case SNAPSHOT_TOAST:
        case SNAPSHOT_ANY:
            return IndexTupleSatisfiesAnyAndToast(scan, page, offnum);
        case SNAPSHOT_DIRTY:
            return IndexTupleSatisfiesDirty(scan, page, offnum);
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported snapshot type %u for UBTreePCR index.", snapshot->satisfies),
                errhint("This kind of operation may not supported.")));
    }
}

/*
 * PCR_SCAN_MODE only supported in SNAPSHOT_MVCC & SNAPSHOT_VERSION_MVCC & SNAPSHOT_NOW
 */
static bool IsPCRScanModeSupported(Snapshot snapshot)
{
    return snapshot->satisfies == SNAPSHOT_MVCC || snapshot->satisfies == SNAPSHOT_VERSION_MVCC ||
        snapshot->satisfies == SNAPSHOT_NOW;
}

/*
 *	UBTreePCRReadPage() -- Load data from current index page into so->currPos
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
static bool UBTreePCRReadPage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Page page;
    UBTPCRPageOpaque opaque;
    OffsetNumber minoff;
    OffsetNumber maxoff;
    int itemIndex;
    IndexTuple itup;
    bool continuescan = true;
    TupleDesc tupdesc;
    AttrNumber PartitionOidAttr;
    Oid partOid = InvalidOid;
    Oid heapOid = IndexScanGetPartHeapOid(scan);
    bool isnull = false;

    tupdesc = RelationGetDescr(scan->indexRelation);
    PartitionOidAttr = IndexRelationGetNumberOfAttributes(scan->indexRelation);

    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(so->currPos.buf));

    page = BufferGetPage(so->currPos.buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    /*
     * we must save the page's right-link while scanning it; this tells us
     * where to step right to after we're done with these items.  There is no
     * corresponding need for the left-link, since splits always go right.
     */
    so->currPos.nextPage = opaque->btpo_next;

    /* initialize tuple workspace to empty */
    so->currPos.nextTupleOffset = 0;

    so->isPageInfoLogged = false;

    Snapshot snapshot = scan->xs_snapshot;
    Page crPage = NULL;
    CRBufferDesc* desc = NULL;

    bool canUsePCRScanMode = IsPCRScanModeSupported(snapshot);
    /*
     * In two cases, must use PCR_SCAN_MODE
     * a) snapshot type is SNAPSHOT_NOW
     * b) In an MVCC snapshot and delete operation occurs after the current snapshot is taken
     */
    bool mustUsePCRScanMode = (snapshot->satisfies == SNAPSHOT_NOW) ||
        ((snapshot->satisfies == SNAPSHOT_MVCC || snapshot->satisfies == SNAPSHOT_VERSION_MVCC)
        && TransactionIdFollowsOrEquals(opaque->last_delete_xid, snapshot->xmin));

    Page localPage = NULL;
    uint32 checkNum = 0;
    OffsetNumber checkVisibleOffs[MaxIndexTuplesPerPage] = {0};
    OffsetNumber originOffnum = offnum;
    BlockNumber blockNum = BufferGetBlockNumber(so->currPos.buf);

choose_scan_mode:
    if (canUsePCRScanMode) {
        /*
         * If snapshot supports PCR_SCAN_MODE, first try to find cr page from the cr pool
         * a) If found, use PCR_SCAN_MODE directly
         * b) If not find but mustUsePCRScanMode is true, build a new cr page
         * c) If neither condition above is met, fallback to use PBRCR_SCAN_MODE
         */
        ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("ReadCRBuffer begin (%u, %d), "
            "querycsn %ld, querycid %d", scan->indexRelation->rd_node.relNode,
            blockNum, snapshot->snapshotcsn, snapshot->curcid))));
        desc = ReadCRBuffer(scan->indexRelation, BufferGetBlockNumber(so->currPos.buf),
            snapshot->snapshotcsn, snapshot->curcid);
        if (mustUsePCRScanMode) {
            localPage = (Page)palloc(BLCKSZ);
            ereport(DEBUG5, (errmodule(MOD_PCR), (errmsg("PCR read page from cr pool notfound"))));
            CommandId page_cid;
            BuildCRPage(scan, localPage, so->currPos.buf, &page_cid);
            page_cid = page_cid == InvalidCommandId ? snapshot->curcid : page_cid;
            UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            if (desc == NULL && !((opaque->btpo_flags & BTP_DELETED) || (opaque->btpo_flags & BTP_HALF_DEAD) ||
                (opaque->btpo_flags & BTP_VACUUM_DELETING) || (opaque->btpo_flags & BTP_INCOMPLETE_SPLIT) ||
                (opaque->btpo_flags & BTP_SPLIT_END))) {
                ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("AllocCRBuffer (%u, %d) page_csn %ld, page_cid %d",
                                                             scan->indexRelation->rd_node.relNode, blockNum,
                                                             snapshot->snapshotcsn, page_cid))));
                desc = AllocCRBuffer(scan->indexRelation, MAIN_FORKNUM, BufferGetBlockNumber(so->currPos.buf),
                                     snapshot->snapshotcsn, page_cid);
                crPage = CRBufferGetPage(desc->buf_id);
                errno_t rc = memcpy_sp(crPage, BLCKSZ, (Page)localPage, BLCKSZ);
                uint64 buf_state = pg_atomic_read_u64(&desc->state);
                UnlockCRBufHdr(desc, buf_state);
            }
            so->scanMode = PCR_SCAN_MODE;
            page = localPage;
            maxoff = UBTreePCRPageGetMaxOffsetNumber(page);
        } else {
            so->scanMode = PBRCR_SCAN_MODE;
        }
        if (localPage != NULL) {
            so->scanMode = PCR_SCAN_MODE;
            page = localPage;
            maxoff = UBTreePCRPageGetMaxOffsetNumber(page);
        }
    } else {
        so->scanMode = PBRCR_SCAN_MODE;
    }
    ereport(DEBUG5, (errmsg("pcr readpage relfilenode:%u, blkno:%u, snapshot:%u, scanMode:%d (0:PCR 1:PBRCR) ",
        scan->indexRelation->rd_node.relNode, BufferGetBlockNumber(so->currPos.buf),
        snapshot->satisfies, so->scanMode)));
    
    if (ScanDirectionIsForward(dir)) {
        /* load items[] in ascending order */
        itemIndex = 0;

        offnum = Max(offnum, minoff);

        while (offnum <= maxoff) {
            itup = UBTreePCRCheckKeys(scan, page, offnum, dir, &continuescan);
            if (itup != NULL) {
                if (so->scanMode == PCR_SCAN_MODE) {
                    /* Get partition oid for global partition index */
                    isnull = false;
                    partOid = scan->xs_want_ext_oid
                        ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                        : heapOid;
                    Assert(!isnull);
                    /* tuple passes all scan key conditions, so remember it */
                    UBTreePCRSaveItem(scan, itemIndex, page, offnum, itup, partOid);
                    itemIndex++;
                } else {
                    checkVisibleOffs[checkNum++] = offnum;
                    if (checkNum >= SCAN_MODE_SWITCH_THRESHOLD && canUsePCRScanMode) {
                        mustUsePCRScanMode = true;
                        offnum = originOffnum;
                        ereport(DEBUG5, (errmsg("pcr readpage switch scan mode, relfilenode:%u, blkno:%u, snapshot:%u",
                            scan->indexRelation->rd_node.relNode, BufferGetBlockNumber(so->currPos.buf),
                            snapshot->satisfies)));
                        goto choose_scan_mode;
                    }
                }
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
            itup = UBTreePCRCheckKeys(scan, page, offnum, dir, &continuescan);
            if (itup != NULL) {
                if (so->scanMode == PCR_SCAN_MODE) {
                    isnull = false;
                    partOid = scan->xs_want_ext_oid
                        ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                        : heapOid;
                    Assert(!isnull);
                    itemIndex--;
                    UBTreePCRSaveItem(scan, itemIndex, page, offnum, itup, partOid);
                } else {
                    checkVisibleOffs[checkNum++] = offnum;
                    if (checkNum >= SCAN_MODE_SWITCH_THRESHOLD && canUsePCRScanMode) {
                        mustUsePCRScanMode = true;
                        offnum = originOffnum;
                        ereport(DEBUG5, (errmsg("pcr readpage switch scan mode, relfilenode:%u, blkno:%u, snapshot:%u",
                            scan->indexRelation->rd_node.relNode, BufferGetBlockNumber(so->currPos.buf),
                            snapshot->satisfies)));
                        goto choose_scan_mode;
                    }
                }
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
    
    if (so->scanMode == PBRCR_SCAN_MODE) {
        if (ScanDirectionIsForward(dir)) {
            int itemIndex = 0;
            for (uint32 i = 0; i < checkNum; i++) {
                offnum = checkVisibleOffs[i];
                if (IndexTupleSatisfiesVisibility(scan, page, offnum)) {
                    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
                    isnull = false;
                    partOid = scan->xs_want_ext_oid
                        ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                        : heapOid;
                    Assert(!isnull);
                    UBTreePCRSaveItem(scan, itemIndex, page, offnum, itup, partOid);
                    itemIndex++;
                }
            }
            Assert(itemIndex <= MaxIndexTuplesPerPage);
            so->currPos.lastItem = itemIndex - 1;
        } else {
            int itemIndex = MaxIndexTuplesPerPage;
            for (uint32 i = 0; i < checkNum; i++) {
                offnum = checkVisibleOffs[i];
                if (IndexTupleSatisfiesVisibility(scan, page, offnum)) {
                    IndexTuple itup = UBTreePCRGetIndexTuple(page, offnum);
                    isnull = false;
                    partOid = scan->xs_want_ext_oid
                        ? DatumGetUInt32(index_getattr(itup, PartitionOidAttr, tupdesc, &isnull))
                        : heapOid;
                    Assert(!isnull);
                    itemIndex--;
                    UBTreePCRSaveItem(scan, itemIndex, page, offnum, itup, partOid);
                }
            }
            Assert(itemIndex >= 0);
            so->currPos.firstItem = itemIndex;
        }
    }
    if (desc != NULL) {
        ReleaseCRBuffer(desc->buf_id);
        ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("release cr buffer (%u, %d), buf_id %d, csn %ld, cid %d, "
            "rsid %ld", scan->indexRelation->rd_node.relNode, BufferGetBlockNumber(so->currPos.buf),
            desc->buf_id, desc->csn, desc->cid, desc->rsid))));
    }
    if (localPage != NULL) {
        pfree(localPage);
    }
    return (so->currPos.firstItem <= so->currPos.lastItem);
}

static UndoRecPtr GetItupXidFromUndo(IndexTuple itup, UndoRecPtr urecptr, TransactionId *tdXid, uint8 *tdid,
    bool *deleted)
{
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetMemoryContext(CurrentMemoryContext);
    urec->SetUrp(urecptr);

    while (true) {
        UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId, false, NULL);
        if (state != UNDO_TRAVERSAL_COMPLETE) {
            *tdXid = InvalidTransactionId;
            break;
        }
        *tdXid = urec->Xid();
        *deleted = urec->Utype() == UNDO_UBT_DELETE;
        IndexTuple undoItup = FetchTupleFromUndoRecord(urec);
        if (UBTreeItupEquals(itup, undoItup)) {
            UBTreeUndoInfo undoInfo = FetchUndoInfoFromUndoRecord(urec);
            *tdid = undoInfo->prev_td_id;
            break;
        }
        /* td reused */
        urec->Reset2Blkprev();
    }

    UndoRecPtr prev = urec->Blkprev();
    DELETE_EX(urec);
    return prev;
}

static void GetItupTransInfo(IndexScanDesc scan, Page page, IndexTuple itup, OffsetNumber offnum,
    IndexTransInfo *transInfo)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
    Assert(!ItemIdIsDead(iid));

    uint8 tdid = iid->lp_td_id;
    Assert(tdid != UBTreeInvalidTDSlotId);
    bool deleted = IsUBTreePCRItemDeleted(iid);

    if (tdid == UBTreeFrozenTDSlotId) {
        transInfo->xmin = FrozenTransactionId;
        transInfo->xmax = (deleted ? FrozenTransactionId : InvalidTransactionId);
        return;
    }
    
    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    if (UBTreePCRTDIsFrozen(td) || (IsUBTreePCRTDReused(iid) &&
        TransactionIdPrecedes(opaque->last_commit_xid, pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid)))) {
        transInfo->xmin = FrozenTransactionId;
        transInfo->xmax = (deleted ? FrozenTransactionId : InvalidTransactionId);
        return;
    }

    TransactionId tdXid;
    uint8 prevTdid;
    UndoRecPtr blkprev = GetItupXidFromUndo(itup, td->undoRecPtr, &tdXid, &prevTdid, &deleted);
    if (!TransactionIdIsValid(tdXid)) {
        tdXid = IsUBTreePCRTDReused(iid) ? tdXid : td->xactid;
        if (deleted) {
            transInfo->xmin = FrozenTransactionId;
            transInfo->xmax = tdXid;
            return;
        }
    }

    if (!deleted) {
        transInfo->xmin = tdXid;
        transInfo->xmax = InvalidTransactionId;
        return;
    }

    transInfo->xmax = tdXid;
    if (prevTdid != tdid) {
        td = UBTreePCRGetTD(page, prevTdid);
        blkprev = td->undoRecPtr;
    }
    (void)GetItupXidFromUndo(itup, blkprev, &tdXid, &prevTdid, &deleted);

    transInfo->xmin = TransactionIdIsValid(tdXid) ? tdXid : FrozenTransactionId;
}

/* UBTreePCRSaveItem() -- Save an index item into so->currPos.items[itemIndex] */
static void UBTreePCRSaveItem(IndexScanDesc scan, int itemIndex, Page page, OffsetNumber offnum,
    const IndexTuple itup, Oid partOid)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    BTScanPosItem *currItem = &so->currPos.items[itemIndex];

    currItem->heapTid = itup->t_tid;
    currItem->indexOffset = offnum;
    currItem->partitionOid = partOid;
    currItem->tupleOffset = INVALID_TUPLE_OFFSET;
    currItem->needRecheck = false;

    /* save index tuple for index only scan and other similar cases */
    if (scan->xs_want_itup) {
        IndexTuple output_itup = itup;
        Size itupsz = IndexTupleSize(output_itup);

        /* we may require xid included in output for UStore's multi-version index */
        if (so->xs_want_xid ||
            (u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.enable_show_any_tuples)) {
            /* copy and reserve addition 16B for xmin/xmax */
            output_itup = CopyIndexTupleAndReserveSpace(output_itup, sizeof(TransactionId) * 2);
            /* transInfo points to the xmin/xmax field */
            IndexTransInfo *transInfo = (IndexTransInfo *)(((char *)output_itup) + itupsz);
            GetItupTransInfo(scan, page, itup, offnum, transInfo);
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
 * UBTreePCRKillItems - set LP_DEAD state for items an indexscan caller has
 * told us were killed
 *
 * scan->so contains information about the current page and killed tuples
 * thereon (generally, this should only be called if so->numKilled > 0).
 *
 * The caller must have pin on so->currPos.buf, but may or may not have
 * read-lock, as indicated by haveLock.  Note that we assume read-lock
 * is sufficient for setting LP_DEAD status (which is only a hint).
 *
 * We match items by heap TID before assuming they are the right ones to
 * delete.	We cope with cases where items have moved right due to insertions.
 * If an item has moved off the current page due to a split, we'll fail to
 * find it and do nothing (this is not an error case --- we assume the item
 * will eventually get marked in a future indexscan).  Note that because we
 * hold pin on the target page continuously from initially reading the items
 * until applying this function, VACUUM cannot have deleted any items from
 * the page, and so there is no need to search left from the recorded offset.
 * (This observation also guarantees that the item is still the right one
 * to delete, which might otherwise be questionable since heap TIDs can get
 * recycled.)
 */
static void UBTreePCRKillItems(IndexScanDesc scan)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Page page;
    UBTPCRPageOpaque opaque;
    OffsetNumber minoff;
    OffsetNumber maxoff;
    int i;
    bool killedsomething = false;
    Oid heapOid = IndexScanGetPartHeapOid(scan);

    Assert(BufferIsValid(so->currPos.buf));

    page = BufferGetPage(so->currPos.buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    for (i = 0; i < so->numKilled; i++) {
        int itemIndex = so->killedItems[i];
        BTScanPosItem *kitem = &so->currPos.items[itemIndex];
        OffsetNumber offnum = kitem->indexOffset;
        Oid partOid = kitem->partitionOid;
        int2 bucketid = kitem->bucketid;

        Assert(itemIndex >= so->currPos.firstItem && itemIndex <= so->currPos.lastItem);
        if (offnum < minoff) {
            continue; /* pure paranoia */
        }
        while (offnum <= maxoff) {
            UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
            IndexTuple ituple = UBTreePCRGetIndexTuple(page, offnum);
            bool killtuple = false;

            if (btree_tuple_is_posting(ituple)) {
                int posting_idx = i + 1;
                int nposting = btree_tuple_get_nposting(ituple);
                int j;
                for (j = 0; j < nposting; j++) {
                    ItemPointer item = btree_tuple_get_posting_n(ituple, j);
                    if (!ItemPointerEquals(item, &kitem->heapTid))
                        break;
                    if (posting_idx < so->numKilled)
                        kitem = &so->currPos.items[so->killedItems[posting_idx++]];
                }
                if (j == nposting)
                    killtuple = true;
            } else if (ItemPointerEquals(&ituple->t_tid, &kitem->heapTid)) {
                Oid currPartOid = scan->xs_want_ext_oid ? index_getattr_tableoid(scan->indexRelation, ituple) : heapOid;
                int2 currbktid =
                    scan->xs_want_bucketid ? index_getattr_bucketid(scan->indexRelation, ituple) : bucketid;
                if (currPartOid == partOid && currbktid == bucketid) {
                    killtuple = true;
                }
            }

            if (killtuple && !ItemIdIsDead(iid)) {
                /* found the item */
                ItemIdMarkDead(iid);
                killedsomething = true;
                break; /* out of inner search loop */
            }
            offnum = OffsetNumberNext(offnum);
        }
    }

    /*
     * Since this can be redone later if needed, it's treated the same as a
     * commit-hint-bit status update for heap tuples: we mark the buffer dirty
     * but don't make a WAL log entry.
     *
     * Whenever we mark anything LP_DEAD, we also set the page's
     * BTP_HAS_GARBAGE flag, which is likewise just a hint.
     */
    if (killedsomething) {
        opaque->btpo_flags |= BTP_HAS_GARBAGE;
        MarkBufferDirtyHint(so->currPos.buf, true);
    }

    /*
     * Always reset the scan state, so we don't look for same items on other
     * pages.
     */
    so->numKilled = 0;
}

/*
 *	UBTreePCRStepPage() -- Step to next page containing valid data for scan
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
static bool UBTreePCRStepPage(IndexScanDesc scan, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Relation rel;
    Page page;
    UBTPCRPageOpaque opaque;

    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(so->currPos.buf));

    /* Before leaving current page, deal with any killed items */
    if (so->numKilled > 0) {
        UBTreePCRKillItems(scan);
    }
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
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, blkno, scan->xs_snapshot);
                /* see if there are any matches on this page */
                /* note that this will clear moreRight if we can stop */
                if (UBTreePCRReadPage(scan, dir, P_FIRSTDATAKEY(opaque)))
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
            Buffer temp = so->currPos.buf;
            so->currPos.buf = InvalidBuffer;
            so->currPos.buf = _bt_walk_left(rel, temp);

            /* if we're physically at end of index, return failure */
            if (so->currPos.buf == InvalidBuffer)
                return false;

            /*
             * Okay, we managed to move left to a non-deleted page. Done if
             * it's not half-dead and contains matching tuples. Else loop back
             * and do it all again.
             */
            page = BufferGetPage(so->currPos.buf);
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
                /* see if there are any matches on this page */
                /* note that this will clear moreLeft if we can stop */
                if (UBTreePCRReadPage(scan, dir, UBTreePCRPageGetMaxOffsetNumber(page)))
                    break;
            }
        }
    }

    return true;
}

/*
 * UBTreePCRGetEndPoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().	We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
Buffer UBTreePCRGetEndPoint(Relation rel, uint32 level, bool rightmost)
{
    Buffer buf;
    Page page;
    UBTPCRPageOpaque opaque;
    OffsetNumber offnum;
    BlockNumber blkno;
    IndexTuple itup;

    /*
     * If we are looking for a leaf page, okay to descend from fast root;
     * otherwise better descend from true root.  (There is no point in being
     * smarter about intermediate levels.)
     */
    if (level == 0)
        buf = UBTreePCRGetRoot(rel, BT_READ);
    else
        buf = _bt_gettrueroot(rel);

    if (!BufferIsValid(buf))
        return InvalidBuffer;

    page = BufferGetPage(buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

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
                        errmsg("fell off the end of index \"%s\" at blkno %u",
                               RelationGetRelationName(rel), BufferGetBlockNumber(buf))));
            buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
            page = BufferGetPage(buf);
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        }

        /* Done? */
        if (opaque->btpo.level == level)
            break;
        if (opaque->btpo.level < level)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("btree level %u not found in index \"%s\", max level is %u",
                           level, RelationGetRelationName(rel), opaque->btpo.level)));

        /* Descend to leftmost or rightmost child page */
        if (rightmost) {
            offnum = UBTreePCRPageGetMaxOffsetNumber(page);
        } else {
            offnum = P_FIRSTDATAKEY(opaque);
        }
        itup = UBTreePCRGetIndexTuple(page, offnum);
        blkno = UBTreeTupleGetDownLink(itup);
        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    }

    return buf;
}

/*
 *	UBTreePCREndPoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _bt_first().
 */
static bool UBTreePCREndPoint(IndexScanDesc scan, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    Buffer buf;
    Page page;
    UBTPCRPageOpaque opaque;
    OffsetNumber start;
    BTScanPosItem *currItem = NULL;

    /*
     * Scan down to the leftmost or rightmost leaf page.  This is a simplified
     * version of _bt_search().  We don't maintain a stack since we know we
     * won't need it.
     */
    buf = UBTreePCRGetEndPoint(rel, 0, ScanDirectionIsBackward(dir));
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
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    Assert(P_ISLEAF(opaque));

    if (ScanDirectionIsForward(dir)) {
        /* There could be dead pages to the left, so not this: */
        start = P_FIRSTDATAKEY(opaque);
    } else if (ScanDirectionIsBackward(dir)) {
        Assert(P_RIGHTMOST(opaque));

        start = UBTreePCRPageGetMaxOffsetNumber(page);
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
    if (!UBTreePCRReadPage(scan, dir, start)) {
        /*
         * There's no actually-matching data on this page.  Try to advance to
         * the next page.  Return false if there's no matching data at all.
         */
        if (!UBTreePCRStepPage(scan, dir))
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

bool UBTreePCRGetTupleInternal(IndexScanDesc scan, ScanDirection dir)
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
            res = UBTreePCRFirst(scan, dir);
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
            res = UBTreePCRNext(scan, dir);
        }

        /* If we have a tuple, return it ... */
        if (res)
            break;
        /* ... otherwise see if we have more array keys to deal with */
    } while (so->numArrayKeys && _bt_advance_array_keys(scan, dir));

    return res;
}

int TDCompare(Datum a, Datum b, void *arg)
{
    Page crPage = (Page)arg;
    Assert(crPage != nullptr);
    UBTreeTD tdA = (UBTreeTD)UBTreePCRGetTD(crPage, DatumGetUInt8(a));
    UBTreeTD tdB = (UBTreeTD)UBTreePCRGetTD(crPage, DatumGetUInt8(b));

    Assert(UBTreePCRTDHasCsn(tdA) && UBTreePCRTDHasCsn(tdB));

    if (tdA->combine.csn > tdB->combine.csn) {
        return 1;
    } else if (tdA->combine.csn == tdB->combine.csn) {
        return 0;
    } else {
        return -1;
    }
}

static void BuildCRPage(IndexScanDesc scan, Page crPage, Buffer baseBuffer, CommandId *page_cid)
{
    /* copy base page to cr page and unlock base page */
    Page basePage = BufferGetPage(baseBuffer);
    errno_t rc = memcpy_sp(crPage, BLCKSZ, basePage, BLCKSZ);
    securec_check(rc, "\0", "\0");

    BlockNumber blkno = BufferGetBlockNumber(baseBuffer);

    Relation rel = scan->indexRelation;
    Snapshot snapshot = scan->xs_snapshot;
    *page_cid = InvalidCommandId;

    if (!IsPCRScanModeSupported(snapshot)) {
        ereport(ERROR, (errmsg("Unsupported snapshot type %u when contruct cr page rnode[%u,%u,%u], blkno:%u",
            snapshot, rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno)));
    }

    ereport(DEBUG5, (errmsg("build cr page rnode[%u,%u,%u], blkno:%u, snapshot:%u",
        rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno,
        snapshot->satisfies)));

    bool needMaxHeap = false;
    bool fillCsnOnBasePage = false;
    binaryheap *maxHeap = NULL;
    volatile CommitSeqNo csn;
    int tdCount = UBTreePageGetTDSlotCount(crPage);
    UBTreeTD td = NULL;
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);

    for (uint8 tdid = 1; tdid <= tdCount; tdid++) {
        bool prevInprogress = false;
        bool prevCommitting = false;
        td = UBTreePCRGetTD(crPage, tdid);
loop:
        TransactionId xid = td->xactid;
        if (!TransactionIdIsValid(xid)) {
            /* td unused or frozen */
            Assert(!IS_VALID_UNDO_REC_PTR(td->undoRecPtr));
            continue;
        }
        if (TransactionIdPrecedes(xid, globalRecycleXid) ||
            (TransactionIdPrecedes(xid, globalFrozenXid) && TransactionIdPrecedes(xid, snapshot->xmin))) {
            continue;
        }
        csn = UBTreePCRTDHasCsn(td) ?
            td->combine.csn : TransactionIdGetCommitSeqNo(xid, false, true, false, snapshot);
        if (COMMITSEQNO_IS_COMMITTED(csn)) {
            if (!UBTreePCRTDHasCsn(td) && csn != COMMITSEQNO_FROZEN) {
                fillCsnOnBasePage = true;
                td->combine.csn = csn;
                UBTreePCRTDSetStatus(td, TD_CSN);
            }
            if (csn < snapshot->snapshotcsn || snapshot->satisfies == SNAPSHOT_NOW) {
                /* xid is visible */
                continue;
            }
            if (!needMaxHeap) {
                maxHeap = binaryheap_allocate(tdCount, TDCompare, crPage);
                needMaxHeap = true;
            }
            binaryheap_add_unordered(maxHeap, UInt8GetDatum(tdid));
        } else if (COMMITSEQNO_IS_COMMITTING(csn)) {
            CommitSeqNo latestCsn = GET_COMMITSEQNO(csn);
            /* prevCommitting is true indicate xid is aborted */
            if (prevCommitting || (latestCsn >= snapshot->snapshotcsn && snapshot->satisfies != SNAPSHOT_NOW)) {
                /* xid is invisible, rollback it */
                RollbackCRPage(scan, crPage, tdid, NULL);
                if (!TransactionIdIsValid(xid)) {
                    continue;
                }
            } else {
                SyncWaitXidEnd(td->xactid, InvalidBuffer, snapshot);
                prevCommitting = true;
            }
            goto loop;
        } else {
            /* xid in progress or aborted */
            Assert(!prevInprogress);
            bool isCurrentXid = TransactionIdIsCurrentTransactionId(td->xactid);
            CommandId cid = isCurrentXid ? snapshot->curcid : InvalidCommandId;
            CommandId cur_page_cid;
            RollbackCRPage(scan, crPage, tdid, &cur_page_cid, cid);
            if (xid == td->xactid) {
                Assert(cid > 0);
                *page_cid = cur_page_cid;
                continue;
            }
            prevInprogress = true;
            goto loop;
        }
    }

    UBTreeTD cr_td;
    /* fill csn on base page */
    if (fillCsnOnBasePage) {
        basePage = BufferGetPage(baseBuffer);
        for (uint8 tdid = 1; tdid <= tdCount; tdid++) {
            td = UBTreePCRGetTD(basePage, tdid);
            cr_td = UBTreePCRGetTD(crPage, tdid);
            if (UBTreePCRTDHasCsn(td) || !UBTreePCRTDHasCsn(cr_td) || td->xactid != cr_td->xactid) {
                continue;
            }
            td->combine.csn = cr_td->combine.csn;
            UBTreePCRTDSetStatus(td, TD_CSN);
        }
    }

    if (!needMaxHeap) {
        /* no committed transaction need to rollback */
        return;
    }

    binaryheap_build(maxHeap);
    
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(crPage);
    IndexTuple firstTuple = UBTreePCRGetIndexTuple(crPage, P_FIRSTDATAKEY(opaque));
    Size itupsz = IndexTupleSize(firstTuple);
    IndexTuple firstTupleCopy = (IndexTuple)palloc0(UBTreePCRMaxItemSize(crPage));
    rc = memcpy_s(firstTupleCopy, itupsz, firstTuple, itupsz);
    securec_check(rc, "\0", "\0");

    uint rollbackCount = 0;
    uint8 tdid;
    /**
     * For committed transactions, if csn is bigger than snapshot csn,
     * the xid need to be rollback in csn descending order.
     */
    while (!binaryheap_empty(maxHeap)) {
        tdid = DatumGetUInt8(binaryheap_first(maxHeap));
        td = UBTreePCRGetTD(crPage, tdid);
        Assert(UBTreePCRTDHasCsn(td));
        CommitSeqNo csn = td->combine.csn;
        if (csn < snapshot->snapshotcsn) {
            break;
        }
        RollbackCRPage(scan, crPage, tdid, NULL, InvalidCommandId, firstTupleCopy);
        Assert(!UBTreePCRTDHasCsn(td));
        rollbackCount++;
        if (rollbackCount % CR_ROLLBACL_COUNT_THRESHOLD == 0) {
            CHECK_FOR_INTERRUPTS();
        }
        if (!TransactionIdIsValid(td->xactid) || TransactionIdPrecedes(td->xactid, snapshot->xmin)) {
            binaryheap_remove_first(maxHeap);
            continue;
        }

        csn = TransactionIdGetCommitSeqNo(td->xactid, true, true, false, snapshot);
        if (!COMMITSEQNO_IS_COMMITTED(csn)) {
            if (TransactionIdDidCommit(td->xactid)) {
                csn = GET_COMMITSEQNO(csn);
            } else {
                ereport(ERROR, (errmsg("build cr page error, td_id %d, xid %lu commit status error. "
                    "rnode[%u,%u,%u], blkno:%u, snapshot:%u", tdid, td->xactid,
                    rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno,
                    snapshot->satisfies)));
            }
        }
        td->combine.csn = csn;
        UBTreePCRTDSetStatus(td, TD_CSN);
        binaryheap_replace_first(maxHeap, UInt8GetDatum(tdid));
    }
    pfree_ext(firstTupleCopy);
    binaryheap_free(maxHeap);
}
