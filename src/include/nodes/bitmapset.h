/* -------------------------------------------------------------------------
 *
 * bitmapset.h
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
 * src/include/nodes/bitmapset.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BITMAPSET_H
#define BITMAPSET_H

/*
 * Data representation
 */

/* The unit size can be adjusted by changing these three declarations: */
#if SIZEOF_VOID_P >= 8

#define BITS_PER_BITMAPWORD 64
typedef uint64 bitmapword;      /* must be an unsigned type */
typedef int64 signedbitmapword; /* must be the matching signed type */
#define bmw_leftmost_one_pos(w)     pg_leftmost_one_pos64(w)
#define bmw_rightmost_one_pos(w)    pg_rightmost_one_pos64(w)
#define bmw_popcount(w)             pg_popcount64(w)

#else

#define BITS_PER_BITMAPWORD 32
typedef uint32 bitmapword;      /* must be an unsigned type */
typedef int32 signedbitmapword; /* must be the matching signed type */
#define bmw_leftmost_one_pos(w)     pg_leftmost_one_pos32(w)
#define bmw_rightmost_one_pos(w)    pg_rightmost_one_pos32(w)
#define bmw_popcount(w)             pg_popcount32(w)

#endif

typedef struct Bitmapset {
    int nwords;                              /* number of words in array */
    bitmapword words[FLEXIBLE_ARRAY_MEMBER]; /* really [nwords] */
} Bitmapset;

/* result of bms_subset_compare */
typedef enum {
    BMS_EQUAL,    /* sets are equal */
    BMS_SUBSET1,  /* first set is a subset of the second */
    BMS_SUBSET2,  /* second set is a subset of the first */
    BMS_DIFFERENT /* neither set is a subset of the other */
} BMS_Comparison;

/* result of bms_membership */
typedef enum {
    BMS_EMPTY_SET, /* 0 members */
    BMS_SINGLETON, /* 1 member */
    BMS_MULTIPLE   /* >1 member */
} BMS_Membership;

/*
 * function prototypes in nodes/bitmapset.c
 */
extern Bitmapset* bms_copy(const Bitmapset* a);
extern bool bms_equal(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_make_singleton(int x);
extern Bitmapset* bms_build_with_length_noexcept(int x);
extern void bms_free(Bitmapset* a);

#define bms_free_ext(bms)    \
    do {                     \
        if ((bms) != NULL) { \
            bms_free(bms);   \
            bms = NULL;      \
        }                    \
    } while (0)

extern Bitmapset* bms_union(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_intersect(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_difference(const Bitmapset* a, const Bitmapset* b);
extern bool bms_is_subset(const Bitmapset* a, const Bitmapset* b);
extern BMS_Comparison bms_subset_compare(const Bitmapset* a, const Bitmapset* b);
extern bool bms_is_member(int x, const Bitmapset* a);
extern bool bms_overlap(const Bitmapset* a, const Bitmapset* b);
extern bool bms_nonempty_difference(const Bitmapset* a, const Bitmapset* b);
extern int bms_singleton_member(const Bitmapset* a);
extern int bms_num_members(const Bitmapset* a);

/* These two macros are used to describe 1 byte. */
#define BYTE_NUMBER 8
#define BYTE_VALUE 255

#ifndef ENABLE_MULTIPLE_NODES
extern int bms_member_index(const Bitmapset *a, int x);
extern bool bms_get_singleton_member(const Bitmapset *a, int *member);
#endif

/* optimized tests when we don't need to know exact membership count: */
extern BMS_Membership bms_membership(const Bitmapset* a);
extern bool bms_is_empty(const Bitmapset* a);

/* these routines recycle (modify or free) their non-const inputs: */
extern Bitmapset* bms_add_member(Bitmapset* a, int x);
extern Bitmapset* bms_del_member(Bitmapset* a, int x);
extern Bitmapset* bms_add_members(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_int_members(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_del_members(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* bms_join(Bitmapset* a, Bitmapset* b);

/* support for iterating through the integer elements of a set: */
extern int bms_first_member(Bitmapset* a);
extern int bms_next_member(const Bitmapset* a, int prevbit);

/* support for hashtables using Bitmapsets as keys: */
extern uint32 bms_hash_value(const Bitmapset* a);

#endif /* BITMAPSET_H */

