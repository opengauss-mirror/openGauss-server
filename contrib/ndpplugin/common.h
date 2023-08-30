/* -------------------------------------------------------------------------
 *
 * common.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  openGauss (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/common.h
 *
 * -------------------------------------------------------------------------
 */
/*
 * ----------------------------------------------------------------
 *	 TABLE OF CONTENTS
 *
 *		When adding stuff to this file, please try to put stuff
 *		into the relevant section, or add new sections as appropriate.
 *
 *	  section	description
 *	  -------	------------------------------------------------
 *		0)		pg_config.h and standard system headers
 *		1)		hacks to cope with non-ANSI C compilers
 *		2)		bool, true, false, TRUE, FALSE, NULL
 *		3)		standard system types
 *		4)		IsValid macros for system types
 *		5)		offsetof, lengthof, endof, alignment
 *		6)		widely useful macros
 *		7)		random stuff
 *		8)		system-specific hacks
 *		9)		C++-specific stuff
 *
 * NOTE: since this file is included by both frontend and backend modules, it's
 * almost certainly wrong to put an "extern" declaration here.	typedefs and
 * macros are the kind of thing that might go here.
 *
 * ----------------------------------------------------------------
 */

#ifndef COMMON_H
#define COMMON_H

#include "securec.h"

/*
 * We have to include stdlib.h here because it defines many of these macros
 * on some platforms, and we only want our definitions used if stdlib.h doesn't
 * have its own.  The same goes for stddef and stdarg if present.
 */

#ifndef NDP_CLIENT
#include "gs_config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <memory>
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

#ifdef FAULT_INJECT
#include <unistd.h>
constexpr int PERCENTAGE = 2;
constexpr int PERCENTAGE_DIV = 100;
constexpr int IOMAP_SIZE = 1024;
#endif

#ifdef NDP_CLIENT
#include "c.h"
typedef int Status;
#else

#ifndef likely
#define likely(x) __builtin_expect((x) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x) != 0, 0)
#endif

#define PG_INT8_MIN (-0x7F - 1)
#define PG_INT8_MAX 0x7F
#define PG_UINT8_MAX 0xFF
#define PG_INT16_MIN (-0x7FFF - 1)
#define PG_INT16_MAX 0x7FFF
#define PG_UINT16_MAX 0xFFFF
#define PG_INT32_MIN (-0x7FFFFFFF - 1)
#define PG_INT32_MAX (0x7FFFFFFF)
#define PG_UINT32_MAX 0xFFFFFFFFU
#define PG_INT64_MIN (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX INT64CONST(0x7FFFFFFFFFFFFFFF)
#define PG_UINT64_MAX UINT64CONST(0xFFFFFFFFFFFFFFFF)

/*
 * int128 type has 128 bits.
 * INT128_MIN is (-1 * (1 << 127))
 * INT128_MAX is ((1 << 127) - 1)
 */
#define INT128_MAX (int128)(((uint128)1 << 127) - 1)
#define INT128_MIN (-INT128_MAX - 1)

/* ----------------------------------------------------------------
 *				Section 2:	bool, true, false, TRUE, FALSE, NULL
 * ----------------------------------------------------------------
 */

/*
 * bool
 *		Boolean value, either true or false.
 *
 * XXX for C++ compilers, we assume the compiler has a compatible
 * built-in definition of bool.
 */

/*
 * NULL
 *		Null pointer.
 */
#ifndef NULL
#define NULL ((void*)0)
#endif

/* ----------------------------------------------------------------
 *				Section 3:	standard system types
 * ----------------------------------------------------------------
 */

/*
 * Pointer
 *		Variable holding address of any memory resident object.
 *
 *		XXX Pointer arithmetic is done with this, so it can't be void *
 *		under "true" ANSI compilers.
 */
typedef char* Pointer;

/*
 * intN
 *		Signed integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_INT8
typedef signed char int8;   /* == 8 bits */
typedef signed short int16; /* == 16 bits */
typedef signed int int32;   /* == 32 bits */
#endif                      /* not HAVE_INT8 */

typedef __int128 int128;
typedef unsigned __int128 uint128;

/*
 * uintN
 *		Unsigned integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_UINT8
typedef unsigned char uint8;   /* == 8 bits */
typedef unsigned short uint16; /* == 16 bits */
typedef unsigned int uint32;   /* == 32 bits */
#endif                         /* not HAVE_UINT8 */

typedef unsigned int uint; /* == 32 bits */

/*
 * bitsN
 *		Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;   /* >= 8 bits */
typedef uint16 bits16; /* >= 16 bits */
typedef uint32 bits32; /* >= 32 bits */

/*
 * 64-bit integers
 */
typedef long long int int64;
typedef unsigned long long int uint64;

/* Decide if we need to decorate 64-bit constants */
#define INT64CONST(x) ((int64)(x))
#define UINT64CONST(x) ((uint64)(x))

/*
 * Size
 *		Size of any memory resident object, as returned by sizeof.
 */
typedef size_t Size;

/*
 * Offset
 *		Offset into any memory resident array.
 *
 * Note:
 *		This differs from an Index in that an Index is always
 *		non negative, whereas Offset may be negative.
 */
typedef signed int Offset;

/*
 * Index
 *		Index into any memory resident array.
 *
 * Note:
 *		Indices are non negative.
 */
typedef unsigned int Index;

/*
 * Common openGauss datatype names (as used in the catalogs)
 */

typedef int8 int1;
typedef int16 int2;
typedef int32 int4;
typedef float float4;
typedef double float8;

typedef uint8 uint1;
typedef uint16 uint2;
typedef uint32 uint4;

typedef int64 pg_time_t;

typedef uintptr_t Datum;

/*
 * Oid, RegProcedure, TransactionId, SubTransactionId, MultiXactId, CommandId
 */

typedef unsigned int Oid;

#define InvalidOid 0
#define InvalidBktId (-1)
#define ExrtoReadStartLSNBktId (-5)
#define ExrtoReadEndLSNBktId (-6)

typedef Oid regproc;
typedef regproc RegProcedure;

typedef uint64 TransactionId;

#define TransactionIdPrecedes(id1, id2) ((id1) < (id2))
#define TransactionIdPrecedesOrEquals(id1, id2) ((id1) <= (id2))
#define TransactionIdFollows(id1, id2) ((id1) > (id2))
#define TransactionIdFollowsOrEquals(id1, id2) ((id1) >= (id2))

#define StartTransactionIdIsValid(start_xid) ((start_xid) <= MAX_START_XID)

typedef uint32 ShortTransactionId;

typedef uint64 LocalTransactionId;

typedef uint64 SubTransactionId;


/* Define to nothing if C supports flexible array members, and to 1 if it does
   not. That way, with a declaration like `struct s { int n; double
   d[FLEXIBLE_ARRAY_MEMBER]; };', the struct hack can be used with pre-C99
   compilers. When computing the size of such an object, don't use 'sizeof
   (struct s)' as it overestimates the size. Use 'offsetof (struct s, d)'
   instead. Don't use 'offsetof (struct s, d[0])', as this doesn't work with
   MSVC and with C++ compilers. */
#define FLEXIBLE_ARRAY_MEMBER /**/

struct varlena {
    char vl_len_[4]; /* Do not touch this field directly! */
    char vl_dat[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct varlena bytea;
typedef struct varlena byteawithoutorderwithequalcol;
typedef struct varlena text;
typedef struct varlena BpChar;    /* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar;   /* var-length char, ie SQL varchar(n) */
typedef struct varlena NVarChar2; /* var-length char, ie SQL nvarchar2(n) */

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union {
    struct /* Normal varlena (4-byte length) */
    {
        uint32 va_header;
        char va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct /* Compressed-in-line format */
    {
        uint32 va_header;
        uint32 va_rawsize;                   /* Original data size (excludes header) */
        char va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} varattrib_4b;

typedef struct {
    uint8 va_header;
    char va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* inline portion of a short varlena pointing to an external resource */
typedef struct {
    uint8 va_header;                     /* Always 0x80 or 0x01 */
    uint8 va_tag;                        /* Type of datum */
    char va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

typedef union {
    struct {                /* Normal varlena (4-byte length) */
        uint32 va_header;
        char va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct {                /* Compressed-in-line format */
        uint32 va_header;
        uint32 va_rawsize;  /* Original data size (excludes header) */
        char va_data[FLEXIBLE_ARRAY_MEMBER];    /* Compressed data */
    } va_compressed;
} varattrib_4b_fe;

#define VARATT_NOT_PAD_BYTE(PTR) (*((uint8*)(PTR)) != 0)

/*
 * Endian-dependent macros.  These are considered internal --- use the
 * external macros below instead of using these directly.
 *
 * Note: IS_1B is true for external toast records but VARSIZE_1B will return 0
 * for such records. Hence you should usually check for IS_EXTERNAL before
 * checking for IS_1B.
 */

#ifdef WORDS_BIGENDIAN

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) (((varattrib_4b*)(PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) (((varattrib_1b*)(PTR))->va_header & 0x7F)
#define VARTAG_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_tag)
#define VARATT_IS_1B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x80)
#define VARATT_IS_4B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_C(PTR) ((((varattrib_1b*)(PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_4B_U(PTR) ((((varattrib_1b*)(PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_HUGE_TOAST_POINTER(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x80 && \
    ((((varattrib_1b_e*)(PTR))->va_tag) & 0x01) == 0x01)
#define VARSIZE_4B(PTR) (((varattrib_4b_fe *)(PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define SET_VARSIZE_1B(PTR, len) (((varattrib_1b*)(PTR))->va_header = (len) | 0x80)

#else /* !WORDS_BIGENDIAN */

#define VARSIZE_4B(PTR) ((((varattrib_4b*)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) ((((varattrib_1b*)(PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_tag)
#define VARATT_IS_1B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x01)
#define VARATT_IS_4B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_C(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_4B_U(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_HUGE_TOAST_POINTER(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x01 && \
    ((((varattrib_1b_e*)(PTR))->va_tag) >> 7) == 0x01)
#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b_fe*)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2))
#define SET_VARSIZE_1B(PTR, len) (((varattrib_1b*)(PTR))->va_header = (((uint8)(len)) << 1) | 0x01)

#endif /* WORDS_BIGENDIAN */

#define VARATT_IS_EXTENDED(PTR) (!VARATT_IS_4B_U(PTR))

#define VARHDRSZ_EXTERNAL offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_SHORT offsetof(varattrib_1b, va_data)
#define VARHDRSZ ((int32)sizeof(int32))
#define VARTAG_SIZE(tag) ((tag & 0x80) == 0x00 ? \
((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) :       \
((tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
((tag) == VARTAG_BUCKET ? sizeof(varatt_external) + sizeof(int2) : \
((tag) == VARTAG_LOB ? sizeof(varatt_lob_pointer) : \
TrapMacro(true, "unknown vartag"))))) : \
((tag & 0x7f) == VARTAG_INDIRECT ? sizeof(varatt_indirect) :       \
((tag & 0x7f) == VARTAG_ONDISK ? sizeof(varatt_lob_external) : \
((tag & 0x7f) == VARTAG_BUCKET ? sizeof(varatt_lob_external) + sizeof(int2) : \
((tag & 0x7f) == VARTAG_LOB ? sizeof(varatt_lob_pointer) : \
TrapMacro(true, "unknown vartag"))))))

#define VARDATA_4B(PTR) (((varattrib_4b*)(PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR) (((varattrib_4b*)(PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR) (((varattrib_1b*)(PTR))->va_data)
#define VARDATA_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_data)
#define VARDATA(PTR) VARDATA_4B(PTR)
#define VARSIZE(PTR) VARSIZE_4B(PTR)
#define VARDATA_SHORT(PTR) VARDATA_1B(PTR)
#define VARSIZE_SHORT(PTR) VARSIZE_1B(PTR)

#define VARTAG_EXTERNAL(PTR) VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR) VARDATA_1B_E(PTR)

#define VARATT_IS_SHORT(PTR) VARATT_IS_1B(PTR)
#define VARATT_IS_COMPRESSED(PTR) VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR) VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_BUCKET(PTR) \
    (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_BUCKET)
#define VARATT_IS_EXTERNAL_LOB(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_LOB)
#define VARATT_IS_EXTERNAL_ONDISK_B(PTR) \
    (VARATT_IS_EXTERNAL_ONDISK(PTR) || VARATT_IS_EXTERNAL_BUCKET(PTR))

#define VARSIZE_ANY(PTR) \
    (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : VARSIZE_4B(PTR)))

#define VARSIZE_ANY_EXHDR(PTR)                                             \
        (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) - VARHDRSZ_EXTERNAL : \
                               (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) - VARHDRSZ_SHORT : VARSIZE_4B(PTR) - VARHDRSZ))

#define VARDATA_ANY(PTR) (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len) SET_VARSIZE_1B(PTR, len)

#define VARATT_CONVERTED_SHORT_SIZE(PTR) (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)
#define VARATT_SHORT_MAX 0x7F
#define VARATT_CAN_MAKE_SHORT(PTR) \
    (VARATT_IS_4B_U(PTR) && (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)

/* ----------------
 *		Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */

#define InvalidTransactionId ((TransactionId)0)
#define BootstrapTransactionId ((TransactionId)1)
#define FrozenTransactionId ((TransactionId)2)
#define FirstNormalTransactionId ((TransactionId)3)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */

#define TransactionIdIsValid(xid) ((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2) ((id1) == (id2))

#define ShortTransactionIdToNormal(base, xid) \
    (TransactionIdIsNormal(xid) ? (TransactionId)(xid) + (base) : (TransactionId)(xid))


typedef uint32 CommandId;

#define NameStr(name) ((name).data)
#define NAMEDATALEN 64
typedef struct nameData {
    char data[NAMEDATALEN];
} NameData;
typedef NameData* Name;

#define FLOAT4OID 700
#define FLOAT8OID 701
#define INTERVALOID 1186
#define BOOLOID 16
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define TEXTOID 25
#define VARCHAROID 1043
#define NUMERICOID 1700
#define INT1OID 5545
#define CSTRINGOID 2275
#define FLOAT8ARRAYOID 1022
#define BOOLARRAYOID 1000
#define TEXTARRAYOID 1009
#define INT4ARRAYOID 1007
#define TIMESTAMPOID 1114
#define BPCHAROID 1042

#define FirstCommandId ((CommandId)0)
#define InvalidCommandId (~(CommandId)0)

/*
 * CommitSeqNo is currently an LSN, but keep use a separate datatype for clarity.
 */
typedef uint64 CommitSeqNo;

#define InvalidCommitSeqNo ((CommitSeqNo)0)

/* ----------------------------------------------------------------
 *				Section 4:	IsValid macros for system types
 * ----------------------------------------------------------------
 */

/*
 * PointerIsValid
 *		True iff pointer is valid.
 */
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

/* ----------------------------------------------------------------
 *				Section 5:	offsetof, lengthof, endof, alignment
 * ----------------------------------------------------------------
 */
/*
 * offsetof
 *		Offset of a structure/union field within that structure/union.
 *
 *		XXX This is supposed to be part of stddef.h, but isn't on
 *		some systems (like SunOS 4).
 */
#ifndef offsetof
#define offsetof(type, field) ((long)&((type*)0)->field)
#endif /* offsetof */

/*
 * lengthof
 *		Number of elements in an array.
 */
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

/* ----------------------------------------------------------------
 *				Section 6:	widely useful macros
 * ----------------------------------------------------------------
 */

/* ----------------
 * Alignment macros: align a length or address appropriately for a given type.
 * The fooALIGN() macros round up to a multiple of the required alignment,
 * while the fooALIGN_DOWN() macros round down.  The latter are more useful
 * for problems like "how many X-sized structures will fit in a page?".
 *
 * NOTE: TYPEALIGN[_DOWN] will not work if ALIGNVAL is not a power of 2.
 * That case seems extremely unlikely to be needed in practice, however.
 * ----------------
 */

#define TYPEALIGN(ALIGNVAL, LEN) (((uintptr_t)(LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t)((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN) TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN) TYPEALIGN(ALIGNOF_INT, (LEN))
#define DOUBLEALIGN(LEN) TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* ----------------------------------------------------------------
 *				Section 7:	random stuff
 * ----------------------------------------------------------------
 */

typedef enum Status {
    STATUS_ERROR = -1,
    STATUS_OK = 0
} Status;
#endif

#define CHECK_STATUS(condition) do {  \
        if (!condition) return STATUS_ERROR;  \
    }while (0)

/* ----------------------------------------------------------------
 *				Section 8: system-specific hacks
 *
 *		This should be limited to things that absolutely have to be
 *		included in every source file.	The port-specific header file
 *		is usually a better place for this sort of thing.
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *				Section 9: C++-specific stuff
 *
 *		This should be limited to stuff that are C++ language specific.
 * ----------------------------------------------------------------
 */

#define DatumGetPointer(X) ((Pointer)(X))
#define DatumGetCString(X) ((char*)DatumGetPointer(X))

typedef int64 Timestamp;
typedef int32 DateADT;

typedef struct {
    int64 count;
    int64 sum;
} Int8TransTypeData;

typedef double Cost;        /* execution cost (in page-access units) */

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8

#define SIZEOF_DATUM SIZEOF_VOID_P

#ifndef AssertMacro
#define AssertMacro(condition) ((void)true)
#endif /* AssertMacro */

#define GET_1_BYTE(datum) (((Datum)(datum)) & 0x000000ff)
#define GET_2_BYTES(datum) (((Datum)(datum)) & 0x0000ffff)
#define GET_4_BYTES(datum) (((Datum)(datum)) & 0xffffffff)
#define GET_8_BYTES(datum) ((Datum)(datum))

#define SET_1_BYTE(value) (((Datum)(value)) & 0x000000ff)
#define SET_2_BYTES(value) (((Datum)(value)) & 0x0000ffff)
#define SET_4_BYTES(value) (((Datum)(value)) & 0xffffffff)
#define SET_8_BYTES(value) ((Datum)(value))

/*
 * BoolGetDatum
 *              Returns datum representation for a boolean.
 *
 * Note: any nonzero value will be considered TRUE.
 */
#ifndef BoolGetDatum
#define BoolGetDatum(X) ((Datum)((X) ? 1 : 0))
#endif

/*
 * PointerGetDatum
 *              Returns datum representation for a pointer.
 */
#ifndef PointerGetDatum
#define PointerGetDatum(X) ((Datum)(X))
#endif

/*
 * CharGetDatum
 *              Returns datum representation for a character.
 */
#define CharGetDatum(X) ((Datum)SET_1_BYTE((unsigned char)(X)))

/*
 * Int16GetDatum
 *              Returns datum representation for a 16-bit integer.
 */
#define Int16GetDatum(X) ((Datum)SET_2_BYTES((uint16)(X)))

/*
 * Int32GetDatum
 *              Returns datum representation for a 32-bit integer.
 */
#define Int32GetDatum(X) ((Datum)SET_4_BYTES((uint32)(X)))

#define DatumGetUInt8(X) ((uint8)GET_1_BYTE(X))
#define DatumGetUInt32(X) ((uint32)GET_4_BYTES(X))
#define DatumGetInt16(X) ((int16)GET_2_BYTES(X))
#define DatumGetInt32(X) ((int32)GET_4_BYTES(X))
#define DatumGetInt64(X) ((int64)GET_8_BYTES(X))
#define DatumGetChar(X) ((char)GET_1_BYTE(X))
#define DatumGetBool(X) ((bool)(((bool)(X)) != 0))
#define DatumGetObjectId(X) ((Oid)GET_4_BYTES(X))
#define DatumGetName(X) ((Name)DatumGetPointer(X))
#define DatumGetDateADT(X) ((DateADT)DatumGetInt32(X))

#define UInt32GetDatum(X) ((Datum)SET_4_BYTES(X))
#define Int64GetDatum(X) ((Datum)SET_8_BYTES(X))
#define Int64GetDatumFast(X) Int64GetDatum(X)
#define ObjectIdGetDatum(X) ((Datum)SET_4_BYTES(X))
#define DateADTGetDatum(X) Int32GetDatum(X)

#define HIGHBIT 0x80
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch)&HIGHBIT)

#define SAMESIGN(a, b) (((a) < 0) == ((b) < 0))

typedef locale_t pg_locale_t;

#endif /* COMMON_H */
