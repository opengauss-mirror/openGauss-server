/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* -------------------------------------------------------------------------
 *
 * hashfn.c
 *		Hash functions for use in dynahash.c hashtables
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/hash/hashfn.c
 *
 * NOTES
 *	  It is expected that every bit of a hash function's 32-bit result is
 *	  as random as every other; failure to ensure this is likely to lead
 *	  to poor performance of hash tables.  In most cases a hash
 *      function should use hash_bytes() or its variant hash_bytes_uint32(),
 *      or the wrappers hash_any() and hash_uint32 defined in hashfn.h.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"

#ifdef ENABLE_NEON
/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

/* Rotate a uint32 value left by k bits - note multiple evaluation! */
#define rot(x, k) (((x) << (k)) | ((x)>>(32 - (k))))

/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *----------
 */
#define mix(a, b, c) \
{ \
    a -= c;  a ^= rot(c, 4);    c += b; \
    b -= a;  b ^= rot(a, 6);    a += c; \
    c -= b;  c ^= rot(b, 8);    b += a; \
    a -= c;  a ^= rot(c, 16);    c += b; \
    b -= a;  b ^= rot(a, 19);    a += c; \
    c -= b;  c ^= rot(b, 4);    b += a; \
}

/*----------
 * final -- final mixing of 3 32-bit values (a,b,c) into c
 *----------
 */
#define final(a, b, c) \
{ \
    c ^= b; c -= rot(b, 14); \
    a ^= c; a -= rot(c, 11); \
    b ^= a; b -= rot(a, 25); \
    c ^= b; c -= rot(b, 16); \
    a ^= c; a -= rot(c, 4); \
    b ^= a; b -= rot(a, 14); \
    c ^= b; c -= rot(b, 24); \
}

/*
 * hash_bytes() -- hash a variable-length key into a 32-bit value
 *        k        : the key (the unaligned variable-length array of bytes)
 *        len        : the length of the key, counting by bytes
 *
 * Returns a uint32 value.  Every bit of the key affects every bit of
 * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
 *
 * This procedure must never throw elog(ERROR); the ResourceOwner code
 * relies on this not to fail.
 */
uint32 hash_bytes(const unsigned char *k, int keylen)
{
    uint32 a;
    uint32 b;
    uint32 c;
    uint32 len;

    /* Set up the internal state */
    len = keylen;
    a = b = c = 0x9e3779b9 + len + 3923095;

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0) {
        /* Code path for aligned source data */
        const uint32 *ka = (const uint32 *) k;

        /* handle most of the key */
        while (len >= 12) {
            a += ka[0];
            b += ka[1];
            c += ka[2];
            mix(a, b, c);
            ka += 3;
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 8);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 24);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 8);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 16);
                /* fall through */
            case 5:
                b += ((uint32) k[4] << 24);
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 8);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 16);
                /* fall through */
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                            /* !WORDS_BIGENDIAN */
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 8);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif                            /* WORDS_BIGENDIAN */
    } else {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while (len >= 12) {
#ifdef WORDS_BIGENDIAN
            a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
            b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
            c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else                            /* !WORDS_BIGENDIAN */
            a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
            b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
            c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif                            /* WORDS_BIGENDIAN */
            mix(a, b, c);
            k += 12;
            len -= 12;
        }

        /* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 8);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 24);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += k[7];
                /* fall through */
            case 7:
                b += ((uint32) k[6] << 8);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 16);
                /* fall through */
            case 5:
                b += ((uint32) k[4] << 24);
                /* fall through */
            case 4:
                a += k[3];
                /* fall through */
            case 3:
                a += ((uint32) k[2] << 8);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 16);
                /* fall through */
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                            /* !WORDS_BIGENDIAN */
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 8);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ((uint32) k[7] << 24);
                /* fall through */
            case 7:
                b += ((uint32) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ((uint32) k[3] << 24);
                /* fall through */
            case 3:
                a += ((uint32) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif                            /* WORDS_BIGENDIAN */
    }

    final(a, b, c);

    /* report the result */
    return c;
}

/*
 * hash_bytes_extended() -- hash into a 64-bit value, using an optional seed
 *        k        : the key (the unaligned variable-length array of bytes)
 *        len        : the length of the key, counting by bytes
 *        seed    : a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to hash_bytes.
 */
uint64 hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
{
    uint32 a;
    uint32 b;
    uint32 c;
    uint32 len;

    /* Set up the internal state */
    len = keylen;
    a = b = c = 0x9e3779b9 + len + 3923095;

    /* If the seed is non-zero, use it to perturb the internal state. */
    if (seed != 0) {
        /*
         * In essence, the seed is treated as part of the data being hashed,
         * but for simplicity, we pretend that it's padded with four bytes of
         * zeroes so that the seed constitutes a 12-byte chunk.
         */
        a += (uint32) (seed >> 32);
        b += (uint32) seed;
        mix(a, b, c);
    }

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0) {
        /* Code path for aligned source data */
        const uint32 *ka = (const uint32 *) k;

        /* handle most of the key */
        while (len >= 12) {
            a += ka[0];
            b += ka[1];
            c += ka[2];
            mix(a, b, c);
            ka += 3;
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 8);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 24);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 8);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 16);
                /* fall through */
            case 5:
                b += ((uint32) k[4] << 24);
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 8);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 16);
                /* fall through */
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                            /* !WORDS_BIGENDIAN */
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 8);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif                            /* WORDS_BIGENDIAN */
    } else {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while (len >= 12) {
#ifdef WORDS_BIGENDIAN
            a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
            b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
            c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else                            /* !WORDS_BIGENDIAN */
            a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
            b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
            c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif                            /* WORDS_BIGENDIAN */
            mix(a, b, c);
            k += 12;
            len -= 12;
        }

        /* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 8);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 24);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += k[7];
                /* fall through */
            case 7:
                b += ((uint32) k[6] << 8);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 16);
                /* fall through */
            case 5:
                b += ((uint32) k[4] << 24);
                /* fall through */
            case 4:
                a += k[3];
                /* fall through */
            case 3:
                a += ((uint32) k[2] << 8);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 16);
                /* fall through */
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                            /* !WORDS_BIGENDIAN */
        switch (len) {
            case 11:
                c += ((uint32) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 8);
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                b += ((uint32) k[7] << 24);
                /* fall through */
            case 7:
                b += ((uint32) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ((uint32) k[3] << 24);
                /* fall through */
            case 3:
                a += ((uint32) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif                            /* WORDS_BIGENDIAN */
    }

    final(a, b, c);

    /* report the result */
    return ((uint64) b << 32) | c;
}

/*
 * hash_bytes_uint32() -- hash a 32-bit value to a 32-bit value
 *
 * This has the same result as
 *        hash_bytes(&k, sizeof(uint32))
 * but is faster and doesn't force the caller to store k into memory.
 */
uint32 hash_bytes_uint32(uint32 k)
{
    uint32 a;
    uint32 b;
    uint32 c;

    a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
    a += k;

    final(a, b, c);

    /* report the result */
    return c;
}

/*
 * hash_bytes_uint32_extended() -- hash 32-bit value to 64-bit value, with seed
 *
 * Like hash_bytes_uint32, this is a convenience function.
 */
uint64 hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
    uint32 a;
    uint32 b;
    uint32 c;

    a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

    if (seed != 0) {
        a += (uint32) (seed >> 32);
        b += (uint32) seed;
        mix(a, b, c);
    }

    a += k;

    final(a, b, c);

    /* report the result */
    return ((uint64) b << 32) | c;
}
#endif /* ENABLE_NEON */

/*
 * string_hash: hash function for keys that are NUL-terminated strings.
 *
 * NOTE: this is the default hash function if none is specified.
 */
uint32 string_hash(const void *key, Size keysize)
{
    /*
     * If the string exceeds keysize-1 bytes, we want to hash only that many,
     * because when it is copied into the hash table it will be truncated at
     * that length.
     */
    Size        s_len = strlen((const char *) key);

    s_len = Min(s_len, keysize - 1);
#ifdef ENABLE_NEON
    return hash_bytes((const unsigned char *) key, (int) s_len);
#else
    return DatumGetUInt32(hash_any((const unsigned char*)key, (int)s_len));
#endif
}

/*
 * tag_hash: hash function for fixed-size tag values
 */
uint32 tag_hash(const void *key, Size keysize)
{
#ifdef ENABLE_NEON
    return hash_bytes((const unsigned char *) key, (int) keysize);
#else
    return DatumGetUInt32(hash_any((const unsigned char*)key, (int)keysize));
#endif
}

/*
 * uint32_hash: hash function for keys that are uint32 or int32
 *
 * (tag_hash works for this case too, but is slower)
 */
uint32 uint32_hash(const void *key, Size keysize)
{
	Assert(keysize == sizeof(uint32));
#ifdef ENABLE_NEON
    return hash_bytes_uint32(*((const uint32 *) key));
#else
	return DatumGetUInt32(hash_uint32(*((const uint32 *) key)));
#endif
}

/*
 * oid_hash: hash function for keys that are OIDs
 *
 * (tag_hash works for this case too, but is slower)
 */
uint32 oid_hash(const void* key, Size keysize)
{
    Assert(keysize == sizeof(Oid));
    return DatumGetUInt32(hash_uint32((uint32) * ((const Oid*)key)));
}

/*
 * bitmap_hash: hash function for keys that are (pointers to) Bitmapsets
 *
 * Note: don't forget to specify bitmap_match as the match function!
 */
uint32 bitmap_hash(const void* key, Size keysize)
{
    Assert(keysize == sizeof(Bitmapset*));
    return bms_hash_value(*((const Bitmapset* const*)key));
}

/*
 * bitmap_match: match function to use with bitmap_hash
 */
int bitmap_match(const void* key1, const void* key2, Size keysize)
{
    Assert(keysize == sizeof(Bitmapset*));
    return (int)(!bms_equal(*((const Bitmapset* const*)key1), *((const Bitmapset* const*)key2)));
}
