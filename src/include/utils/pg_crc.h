/*
 * pg_crc.h
 *
 * openGauss CRC support
 *
 * See Ross Williams' excellent introduction
 * A PAINLESS GUIDE TO CRC ERROR DETECTION ALGORITHMS, available from
 * http://www.ross.net/crc/ or several other net sites.
 *
 * We use a normal (not "reflected", in Williams' terms) CRC, using initial
 * all-ones register contents and a final bit inversion.
 *
 * The 64-bit variant is not used as of PostgreSQL 8.1, but we retain the
 * code for possible future use.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/pg_crc.h
 */
#ifndef PG_CRC_H
#define PG_CRC_H

/* ugly hack to let this be used in frontend and backend code on Cygwin */
#ifdef FRONTEND
#define CRCDLLIMPORT
#else
#define CRCDLLIMPORT PGDLLIMPORT
#endif

typedef uint32 pg_crc32;

/*
 * The CRC algorithm used for WAL et al in pre-9.5 versions.
 *
 * This closely resembles the normal CRC-32 algorithm, but is subtly
 * different. Using Williams' terms, we use the "normal" table, but with
 * "reflected" code. That's bogus, but it was like that for years before
 * anyone noticed. It does not correspond to any polynomial in a normal CRC
 * algorithm, so it's not clear what the error-detection properties of this
 * algorithm actually are.
 *
 * We still need to carry this around because it is used in a few on-disk
 * structures that need to be pg_upgradeable. It should not be used in new
 * code.
 *
 * Deprecated
 *
 * using CRC32C instead
 */
#define INIT_TRADITIONAL_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_TRADITIONAL_CRC32(crc)  ((crc) ^= 0xFFFFFFFF)
/* Initialize a CRC accumulator */
#define INIT_CRC32(crc) ((crc) = 0xFFFFFFFF)

/* Finish a CRC calculation */
#define FIN_CRC32(crc) ((crc) ^= 0xFFFFFFFF)

/* Check for equality of two CRCs */
#define EQ_CRC32(c1, c2) ((c1) == (c2))
#define EQ_TRADITIONAL_CRC32(c1, c2) ((c1) == (c2))
/* Accumulate some (more) bytes into a CRC */
#define COMP_CRC32(crc, data, len)                                     \
    do {                                                               \
        const unsigned char* __data = (const unsigned char*)(data);    \
        uint32 __len = (len);                                          \
                                                                       \
        while (__len-- > 0) {                                          \
            int __tab_index = ((int)((crc) >> 24) ^ *__data++) & 0xFF; \
            (crc) = pg_crc32_table[__tab_index] ^ ((crc) << 8);        \
        }                                                              \
    } while (0)

/* Constant table for CRC calculation */
extern CRCDLLIMPORT uint32 pg_crc32_table[];

#define COMP_TRADITIONAL_CRC32(crc, data, len)	\
	COMP_CRC32_NORMAL_TABLE(crc, data, len, pg_crc32_table)

/* Sarwate's algorithm, for use with a "normal" lookup table */
#define COMP_CRC32_NORMAL_TABLE(crc, data, len, table)			  \
do {															  \
	const unsigned char *__data = (const unsigned char *) (data); \
	uint32		__len = (len); \
\
	while (__len-- > 0) \
	{ \
		int		__tab_index = ((int) (crc) ^ *__data++) & 0xFF; \
		(crc) = table[__tab_index] ^ ((crc) >> 8); \
	} \
} while (0)


#ifdef PROVIDE_64BIT_CRC

/*
 * If we use a 64-bit integer type, then a 64-bit CRC looks just like the
 * usual sort of implementation.  However, we can also fake it with two
 * 32-bit registers.  Experience has shown that the two-32-bit-registers code
 * is as fast as, or even much faster than, the 64-bit code on all but true
 * 64-bit machines.  We use SIZEOF_VOID_P to check the native word width.
 */

#if SIZEOF_VOID_P < 8

/*
 * crc0 represents the LSBs of the 64-bit value, crc1 the MSBs.  Note that
 * with crc0 placed first, the output of 32-bit and 64-bit implementations
 * will be bit-compatible only on little-endian architectures.	If it were
 * important to make the two possible implementations bit-compatible on
 * all machines, we could do a configure test to decide how to order the
 * two fields, but it seems not worth the trouble.
 */
typedef struct pg_crc64 {
    uint32 crc0;
    uint32 crc1;
} pg_crc64;

/* Initialize a CRC accumulator */
#define INIT_CRC64(crc) ((crc).crc0 = 0xffffffff, (crc).crc1 = 0xffffffff)

/* Finish a CRC calculation */
#define FIN_CRC64(crc) ((crc).crc0 ^= 0xffffffff, (crc).crc1 ^= 0xffffffff)

/* Accumulate some (more) bytes into a CRC */
#define COMP_CRC64(crc, data, len)                                                    \
    do {                                                                              \
        uint32 __crc0 = (crc).crc0;                                                   \
        uint32 __crc1 = (crc).crc1;                                                   \
        unsigned char* __data = (unsigned char*)(data);                               \
        uint32 __len = (len);                                                         \
                                                                                      \
        while (__len-- > 0) {                                                         \
            int __tab_index = ((int)(__crc1 >> 24) ^ *__data++) & 0xFF;               \
            __crc1 = pg_crc64_table1[__tab_index] ^ ((__crc1 << 8) | (__crc0 >> 24)); \
            __crc0 = pg_crc64_table0[__tab_index] ^ (__crc0 << 8);                    \
        }                                                                             \
        (crc).crc0 = __crc0;                                                          \
        (crc).crc1 = __crc1;                                                          \
    } while (0)

/* Check for equality of two CRCs */
#define EQ_CRC64(c1, c2) ((c1).crc0 == (c2).crc0 && (c1).crc1 == (c2).crc1)

/* Constant table for CRC calculation */
extern CRCDLLIMPORT const uint32 pg_crc64_table0[];
extern CRCDLLIMPORT const uint32 pg_crc64_table1[];
#else /* use int64 implementation */

typedef struct pg_crc64 {
    uint64 crc0;
} pg_crc64;

/* Initialize a CRC accumulator */
#define INIT_CRC64(crc) ((crc).crc0 = UINT64CONST(0xffffffffffffffff))

/* Finish a CRC calculation */
#define FIN_CRC64(crc) ((crc).crc0 ^= UINT64CONST(0xffffffffffffffff))

/* Accumulate some (more) bytes into a CRC */
#define COMP_CRC64(crc, data, len)                                      \
    do {                                                                \
        uint64 __crc0 = (crc).crc0;                                     \
        unsigned char* __data = (unsigned char*)(data);                 \
        uint32 __len = (len);                                           \
                                                                        \
        while (__len-- > 0) {                                           \
            int __tab_index = ((int)(__crc0 >> 56) ^ *__data++) & 0xFF; \
            __crc0 = pg_crc64_table[__tab_index] ^ (__crc0 << 8);       \
        }                                                               \
        (crc).crc0 = __crc0;                                            \
    } while (0)

/* Check for equality of two CRCs */
#define EQ_CRC64(c1, c2) ((c1).crc0 == (c2).crc0)

/* Constant table for CRC calculation */
extern CRCDLLIMPORT const uint64 pg_crc64_table[];
#endif /* SIZEOF_VOID_P < 8 */
#endif /* PROVIDE_64BIT_CRC */
#endif /* PG_CRC_H */
