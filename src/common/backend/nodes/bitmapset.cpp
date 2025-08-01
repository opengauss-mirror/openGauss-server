/* -------------------------------------------------------------------------
 *
 * bitmapset.cpp
 *	  openGauss generic bitmap set package
 *
 * A bitmap set can represent any set of nonnegative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, a NULL pointer is always
 * accepted by all operations to represent the empty set.  (But beware
 * that this is not the only representation of the empty set.  Use
 * bms_is_empty() in preference to testing for NULL.)
 *
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/bitmapset.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "port/pg_bitutils.h"

#include "access/hash.h"

#define WORDNUM(x) ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x) ((x) % BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords) (offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

/* ----------
 * This is a well-known cute trick for isolating the rightmost one-bit
 * in a word.  It assumes two's complement arithmetic.  Consider any
 * nonzero value, and focus attention on the rightmost one.  The value is
 * then something like
 *				xxxxxx10000
 * where x's are unspecified bits.  The two's complement negative is formed
 * by inverting all the bits and adding one.  Inversion gives
 *				yyyyyy01111
 * where each y is the inverse of the corresponding x.	Incrementing gives
 *				yyyyyy10000
 * and then ANDing with the original value gives
 *				00000010000
 * This works for all cases except original value = zero, where of course
 * we get zero.
 * ----------
 */
#define RIGHTMOST_ONE(x) ((signedbitmapword)(x) & -((signedbitmapword)(x)))

#define HAS_MULTIPLE_ONES(x) ((bitmapword)RIGHTMOST_ONE(x) != (x))

/* Select appropriate bit-twiddling functions for bitmap word size */
#if BITS_PER_BITMAPWORD == 32
#define bmw_leftmost_one_pos(w) pg_leftmost_one_pos32(w)
#define bmw_rightmost_one_pos(w) pg_rightmost_one_pos32(w)
#define bmw_popcount(w) pg_popcount32(w)
#elif BITS_PER_BITMAPWORD == 64
#define bmw_leftmost_one_pos(w) pg_leftmost_one_pos64(w)
#define bmw_rightmost_one_pos(w) pg_rightmost_one_pos64(w)
#define bmw_popcount(w) pg_popcount64(w)
#else
#error "invalid BITS_PER_BITMAPWORD"
#endif

/*
 * bms_copy - make a palloc'd copy of a bitmapset
 */
Bitmapset* bms_copy(const Bitmapset* a)
{
    Bitmapset* result = NULL;
    size_t size;

    if (a == NULL) {
        return NULL;
    }
    size = BITMAPSET_SIZE(a->nwords);
    result = (Bitmapset*)palloc(size);
    errno_t rc = memcpy_s(result, size, a, size);
    securec_check(rc, "\0", "\0");
    return result;
}

/*
 * bms_equal - are two bitmapsets equal?
 *
 * This is logical not physical equality; in particular, a NULL pointer will
 * be reported as equal to a palloc'd value containing no members.
 */
bool bms_equal(const Bitmapset* a, const Bitmapset* b)
{
    const Bitmapset* shorter = NULL;
    const Bitmapset* longer = NULL;
    int shortlen;
    int longlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        if (b == NULL) {
            return true;
        }
        return bms_is_empty(b);
    } else if (b == NULL) {
        return bms_is_empty(a);
    }
    /* Identify shorter and longer input */
    if (a->nwords <= b->nwords) {
        shorter = a;
        longer = b;
    } else {
        shorter = b;
        longer = a;
    }
    /* And process */
    shortlen = shorter->nwords;
    for (i = 0; i < shortlen; i++) {
        if (shorter->words[i] != longer->words[i]) {
            return false;
        }
    }
    longlen = longer->nwords;
    for (; i < longlen; i++) {
        if (longer->words[i] != 0) {
            return false;
        }
    }
    return true;
}

/*
 * bms_make_singleton - build a bitmapset containing a single member
 */
Bitmapset* bms_make_singleton(int x)
{
    Bitmapset* result = NULL;
    int wordnum, bitnum;

    if (x < 0) {
        ereport(ERROR,
            (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION), errmsg("negative bitmapset member not allowed")));
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    result = (Bitmapset*)palloc0(BITMAPSET_SIZE(wordnum + 1));
    result->nwords = wordnum + 1;
    result->words[wordnum] = ((bitmapword)1 << (unsigned int)bitnum);
    return result;
}

/*
 * bms_build_with_length_noexcept - build a bitmapset containing a length
 */
Bitmapset* bms_build_with_length_noexcept(int x)
{
    Bitmapset* result = NULL;
    int wordnum;

    if (x < 0) {
        ereport(ERROR,
            (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("negative bitmapset member not allowed")));
    }
    wordnum = WORDNUM(x);
    result = (Bitmapset*)palloc0(BITMAPSET_SIZE(wordnum + 1));
    if (result == NULL) {
        return NULL;
    }
    result->nwords = wordnum + 1;
    return result;
}

/*
 * bms_free - free a bitmapset
 *
 * Same as pfree except for allowing NULL input
 */
void bms_free(Bitmapset* a)
{
    if (a != NULL) {
        pfree_ext(a);
    }
}

/*
 * These operations all make a freshly palloc'd result,
 * leaving their inputs untouched
 */

/*
 * bms_union - set union
 */
Bitmapset* bms_union(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = NULL;
    const Bitmapset* other = NULL;
    int otherlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return bms_copy(b);
    }
    if (b == NULL) {
        return bms_copy(a);
    }
    /* Identify shorter and longer input; copy the longer one */
    if (a->nwords <= b->nwords) {
        result = bms_copy(b);
        other = a;
    } else {
        result = bms_copy(a);
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = other->nwords;
    for (i = 0; i < otherlen; i++)
        result->words[i] |= other->words[i];
    return result;
}

/*
 * bms_intersect - set intersection
 */
Bitmapset* bms_intersect(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = NULL;
    const Bitmapset* other = NULL;
    int resultlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL || b == NULL) {
        return NULL;
    }
    /* Identify shorter and longer input; copy the shorter one */
    if (a->nwords <= b->nwords) {
        result = bms_copy(a);
        other = b;
    } else {
        result = bms_copy(b);
        other = a;
    }
    /* And intersect the longer input with the result */
    resultlen = result->nwords;
    for (i = 0; i < resultlen; i++) {
        result->words[i] &= other->words[i];
    }
    return result;
}

/*
 * bms_difference - set difference (ie, A without members of B)
 */
Bitmapset* bms_difference(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = NULL;
    int shortlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return NULL;
    }
    if (b == NULL) {
        return bms_copy(a);
    }
    /* Copy the left input */
    result = bms_copy(a);
    /* And remove b's bits from result */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        result->words[i] &= ~b->words[i];
    }
    return result;
}

/*
 * bms_is_subset - is A a subset of B?
 */
bool bms_is_subset(const Bitmapset* a, const Bitmapset* b)
{
    int shortlen;
    int longlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return true; /* empty set is a subset of anything */
    }
    if (b == NULL) {
        return bms_is_empty(a);
    }
    /* Check common words */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        if ((a->words[i] & ~b->words[i]) != 0) {
            return false;
        }
    }
    /* Check extra words */
    if (a->nwords > b->nwords) {
        longlen = a->nwords;
        for (; i < longlen; i++) {
            if (a->words[i] != 0) {
                return false;
            }
        }
    }
    return true;
}

/*
 * bms_subset_compare - compare A and B for equality/subset relationships
 *
 * This is more efficient than testing bms_is_subset in both directions.
 */
BMS_Comparison bms_subset_compare(const Bitmapset* a, const Bitmapset* b)
{
    BMS_Comparison result;
    int shortlen;
    int longlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        if (b == NULL) {
            return BMS_EQUAL;
        }
        return bms_is_empty(b) ? BMS_EQUAL : BMS_SUBSET1;
    }
    if (b == NULL) {
        return bms_is_empty(a) ? BMS_EQUAL : BMS_SUBSET2;
    }
    /* Check common words */
    result = BMS_EQUAL; /* status so far */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        bitmapword aword = a->words[i];
        bitmapword bword = b->words[i];

        if ((aword & ~bword) != 0) {
            /* a is not a subset of b */
            if (result == BMS_SUBSET1) {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET2;
        }
        if ((bword & ~aword) != 0) {
            /* b is not a subset of a */
            if (result == BMS_SUBSET2) {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET1;
        }
    }
    /* Check extra words */
    if (a->nwords > b->nwords) {
        longlen = a->nwords;
        for (; i < longlen; i++) {
            if (a->words[i] != 0) {
                /* a is not a subset of b */
                if (result == BMS_SUBSET1) {
                    return BMS_DIFFERENT;
                }
                result = BMS_SUBSET2;
            }
        }
    } else if (a->nwords < b->nwords) {
        longlen = b->nwords;
        for (; i < longlen; i++) {
            if (b->words[i] != 0) {
                /* b is not a subset of a */
                if (result == BMS_SUBSET2) {
                    return BMS_DIFFERENT;
                }
                result = BMS_SUBSET1;
            }
        }
    }
    return result;
}

/*
 * bms_is_member - is X a member of A?
 */
bool bms_is_member(int x, const Bitmapset* a)
{
    int wordnum, bitnum;

    /* XXX better to just return false for x<0 ? */
    if (x < 0) {
        ereport(ERROR,
            (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION), errmsg("negative bitmapset member not allowed")));
    }
    if (a == NULL) {
        return false;
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum >= a->nwords)  {
        return false;
    }
    if ((a->words[wordnum] & ((bitmapword)1 << (unsigned int)bitnum)) != 0) {
        return true;
    }
    return false;
}

/*
 * bms_overlap - do sets overlap (ie, have a nonempty intersection)?
 */
bool bms_overlap(const Bitmapset* a, const Bitmapset* b)
{
    int shortlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL || b == NULL) {
        return false;
    }
    /* Check words in common */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        if ((a->words[i] & b->words[i]) != 0) {
            return true;
        }
    }
    return false;
}

/*
 * bms_nonempty_difference - do sets have a nonempty difference?
 */
bool bms_nonempty_difference(const Bitmapset* a, const Bitmapset* b)
{
    int shortlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return false;
    }
    if (b == NULL) {
        return !bms_is_empty(a);
    }
    /* Check words in common */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        if ((a->words[i] & ~b->words[i]) != 0) {
            return true;
        }
    }
    /* Check extra words in a */
    for (; i < a->nwords; i++) {
        if (a->words[i] != 0) {
            return true;
        }
    }
    return false;
}

/*
 * bms_singleton_member - return the sole integer member of set
 *
 * Raises error if |a| is not 1.
 */
int bms_singleton_member(const Bitmapset* a)
{
    int result = -1;
    int nwords;
    int wordnum;

    if (a == NULL) {
        ereport(ERROR, (errmodule(MOD_CACHE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("bitmapset is empty")));
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            if (result >= 0 || HAS_MULTIPLE_ONES(w)) {
                ereport(ERROR,
                    (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION), errmsg("bitmapset has multiple members")));
            }
            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
        }
    }
    if (result < 0) {
        ereport(ERROR, (errmodule(MOD_CACHE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("bitmapset is empty")));
    }
    return result;
}

/*
 * bms_num_members - count members of set
 */
int bms_num_members(const Bitmapset* a)
{
    int result = 0;
    int nwords;
    int wordnum;

    if (a == NULL) {
        return 0;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

		/* No need to count the bits in a zero word */
        if (w != 0)
            result += bmw_popcount(w);
    }
    return result;
}

/*
 * bms_membership - does a set have zero, one, or multiple members?
 *
 * This is faster than making an exact count with bms_num_members().
 */
BMS_Membership bms_membership(const Bitmapset* a)
{
    BMS_Membership result = BMS_EMPTY_SET;
    int nwords;
    int wordnum;

    if (a == NULL) {
        return BMS_EMPTY_SET;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            if (result != BMS_EMPTY_SET || HAS_MULTIPLE_ONES(w)) {
                return BMS_MULTIPLE;
            }
            result = BMS_SINGLETON;
        }
    }
    return result;
}

/*
 * bms_is_empty - is a set empty?
 *
 * This is even faster than bms_membership().
 */
bool bms_is_empty(const Bitmapset* a)
{
    int nwords;
    int wordnum;

    if (a == NULL) {
        return true;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            return false;
        }
    }
    return true;
}

/*
 * These operations all "recycle" their non-const inputs, ie, either
 * return the modified input or pfree it if it can't hold the result.
 *
 * These should generally be used in the style
 *
 *		foo = bms_add_member(foo, x);
 */

/*
 * bms_add_member - add a specified member to set
 *
 * Input set is modified or recycled!
 */
Bitmapset* bms_add_member(Bitmapset* a, int x)
{
    int wordnum, bitnum;

    if (x < 0) {
        ereport(ERROR,
            (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION), errmsg("negative bitmapset member not allowed")));
    }
    if (a == NULL) {
        return bms_make_singleton(x);
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum >= a->nwords) {
        /* Slow path: make a larger set and union the input set into it */
        Bitmapset* result = NULL;
        int nwords;
        int i;

        result = bms_make_singleton(x);
        nwords = a->nwords;
        for (i = 0; i < nwords; i++) {
            result->words[i] |= a->words[i];
        }
        pfree_ext(a);
        return result;
    }
    /* Fast path: x fits in existing set */
    a->words[wordnum] |= ((bitmapword)1 << (unsigned int)bitnum);
    return a;
}

/*
 * bms_del_member - remove a specified member from set
 *
 * No error if x is not currently a member of set
 *
 * Input set is modified in-place!
 */
Bitmapset* bms_del_member(Bitmapset* a, int x)
{
    int wordnum, bitnum;

    if (x < 0) {
        ereport(ERROR,
            (errmodule(MOD_CACHE), errcode(ERRCODE_DATA_EXCEPTION), errmsg("negative bitmapset member not allowed")));
    }
    if (a == NULL) {
        return NULL;
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum < a->nwords) {
        a->words[wordnum] &= ~((bitmapword)1 << bitnum);
    }
    return a;
}

/*
 * bms_add_members - like bms_union, but left input is recycled
 */
Bitmapset* bms_add_members(Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = NULL;
    const Bitmapset* other = NULL;
    int otherlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return bms_copy(b);
    }
    if (b == NULL) {
        return a;
    }
    /* Identify shorter and longer input; copy the longer one if needed */
    if (a->nwords < b->nwords) {
        result = bms_copy(b);
        other = a;
    } else {
        result = a;
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = other->nwords;
    for (i = 0; i < otherlen; i++) {
        result->words[i] |= other->words[i];
    }
    if (result != a) {
        pfree_ext(a);
    }
    return result;
}

/*
 * bms_int_members - like bms_intersect, but left input is recycled
 */
Bitmapset* bms_int_members(Bitmapset* a, const Bitmapset* b)
{
    int shortlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return NULL;
    }
    if (b == NULL) {
        pfree_ext(a);
        return NULL;
    }
    /* Intersect b into a; we need never copy */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        a->words[i] &= b->words[i];
    }
    for (; i < a->nwords; i++) {
        a->words[i] = 0;
    }
    return a;
}

/*
 * bms_del_members - like bms_difference, but left input is recycled
 */
Bitmapset* bms_del_members(Bitmapset* a, const Bitmapset* b)
{
    int shortlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return NULL;
    }
    if (b == NULL) {
        return a;
    }
    /* Remove b's bits from a; we need never copy */
    shortlen = Min(a->nwords, b->nwords);
    for (i = 0; i < shortlen; i++) {
        a->words[i] &= ~b->words[i];
    }
    return a;
}

/*
 * bms_join - like bms_union, but *both* inputs are recycled
 */
Bitmapset* bms_join(Bitmapset* a, Bitmapset* b)
{
    Bitmapset* result = NULL;
    Bitmapset* other = NULL;
    int otherlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == NULL) {
        return b;
    }
    if (b == NULL) {
        return a;
    }
    /* Identify shorter and longer input; use longer one as result */
    if (a->nwords < b->nwords) {
        result = b;
        other = a;
    } else {
        result = a;
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = other->nwords;
    for (i = 0; i < otherlen; i++) {
        result->words[i] |= other->words[i];
    }
    if (other != result) {  /* pure paranoia */
        pfree_ext(other);
    }
    return result;
}

/* ----------
 * bms_first_member - find and remove first member of a set
 *
 * Returns -1 if set is empty.	NB: set is destructively modified!
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *			tmpset = bms_copy(inputset);
 *			while ((x = bms_first_member(tmpset)) >= 0)
 *				process member x;
 *			bms_free_ext(tmpset);
 * ----------
 */
int bms_first_member(Bitmapset* a)
{
    int nwords;
    int wordnum;

    if (a == NULL) {
        return -1;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            int result;

            w = RIGHTMOST_ONE(w);
            a->words[wordnum] &= ~w;

            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
            return result;
        }
    }
    return -1;
}

/*
 * bms_hash_value - compute a hash key for a Bitmapset
 *
 * Note: we must ensure that any two bitmapsets that are bms_equal() will
 * hash to the same value; in practice this means that trailing all-zero
 * words must not affect the result.  Hence we strip those before applying
 * hash_any().
 */
uint32 bms_hash_value(const Bitmapset* a)
{
    int lastword;

    if (a == NULL) {
        return 0; /* All empty sets hash to 0 */
    }
    for (lastword = a->nwords; --lastword >= 0;) {
        if (a->words[lastword] != 0) {
            break;
        }
    }
    if (lastword < 0) {
        return 0; /* All empty sets hash to 0 */
    }
    return DatumGetUInt32(hash_any((const unsigned char*)a->words, (lastword + 1) * sizeof(bitmapword)));
}

/*
 * bms_next_member - find next member of a set
 *
 * Returns smallest member greater than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be less than -1, or the behavior is unpredictable.
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_next_member(inputset, x)) >= 0)
 *				process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you
 * might expect.  The rationale for that is to allow distinguishing the
 * loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic
 * might need such an ability.
 */
int bms_next_member(const Bitmapset* a, int prevbit)
{
    int nwords;
    int wordnum;
    bitmapword mask;

    if (prevbit < -1) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The parameter can not smaller than -1.")));
    }

    if (a == NULL) {
        return -2;
    }
    nwords = a->nwords;
    prevbit++;
    mask = (~(bitmapword)0) << BITNUM((unsigned int)prevbit);
    for (wordnum = WORDNUM(prevbit); wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        /* ignore bits before prevbit */
        w &= mask;

        if (w != 0) {
            int result;

            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
            return result;
        }

        /* in subsequent words, consider all bits */
        mask = (~(bitmapword)0);
    }
    return -2;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * bms_get_singleton_member
 *
 * Test whether the given set is a singleton.
 * If so, set *member to the value of its sole member, and return true.
 * If not, return false, without changing *member.
 *
 * This is more convenient and faster than calling bms_membership() and then
 * bms_singleton_member(), if we don't care about distinguishing empty sets
 * from multiple-member sets.
 */
bool bms_get_singleton_member(const Bitmapset *a, int *member)
{
    int result = -1;
    int nwords;
    int wordnum;

    if (a == NULL)
        return false;
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            if (result >= 0 || HAS_MULTIPLE_ONES(w))
                return false;
            result = wordnum * BITS_PER_BITMAPWORD;
            while ((w & BYTE_VALUE) == 0) {
                w >>= BYTE_NUMBER;
                result += BYTE_NUMBER;
            }
            result += bmw_rightmost_one_pos(w);
        }
    }
    if (result < 0) {
        return false;
    }
    *member = result;
    return true;
}

/*
 * bms_member_index
 *		determine 0-based index of member x in the bitmap
 *
 * Returns (-1) when x is not a member.
 */
int bms_member_index(const Bitmapset *a, int x)
{
    int bitnum;
    int wordnum;
    int result = 0;
    bitmapword mask;

    /* return -1 if not a member of the bitmap */
    if (!bms_is_member(x, a))
        return -1;

    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);

    /* count bits in preceding words */
    for (int i = 0; i < wordnum; i++) {
        bitmapword w = a->words[i];

        /* No need to count the bits in a zero word */
        if (w != 0) {
            result += bmw_popcount(w);
            w >>= BYTE_NUMBER;
        }
    }

    /*
     * Now add bits of the last word, but only those before the item. We can
     * do that by applying a mask and then using popcount again. To get
     * 0-based index, we want to count only preceding bits, not the item
     * itself, so we subtract 1.
     */
    mask = ((bitmapword)1 << bitnum) - 1;
    bitmapword tmp = a->words[wordnum] & mask;
    result += bmw_popcount(tmp);
    tmp >>= BYTE_NUMBER;

    return result;
}
#endif