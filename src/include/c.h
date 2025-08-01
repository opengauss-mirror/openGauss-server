/* -------------------------------------------------------------------------
 *
 * c.h
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
 * src/include/c.h
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
#ifndef C_H
#define C_H

/*
 * We have to include stdlib.h here because it defines many of these macros
 * on some platforms, and we only want our definitions used if stdlib.h doesn't
 * have its own.  The same goes for stddef and stdarg if present.
 */

#include "pg_config.h"
#include "pg_config_manual.h"               /* must be after pg_config.h */
#if !defined(WIN32) && !defined(__CYGWIN__) /* win32 will include further \
                                             * down */
#include "pg_config_os.h"                   /* must be before any system header files */
#endif
#include "postgres_ext.h"

#if _MSC_VER >= 1400 || defined(HAVE_CRTDEFS_H)
#define errcode __msvc_errcode
#include <crtdefs.h>
#undef errcode
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#ifdef HAVE_STDINT_H
/* to use INT64_MIN and INT64_MAX */
#define __STDC_LIMIT_MACROS
#include <stdint.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#if defined(WIN32) || defined(__CYGWIN__)
#include <fcntl.h> /* ensure O_BINARY is available */
#endif

#if defined(WIN32) || defined(__CYGWIN__)
/* We have to redefine some system functions after they are included above. */
#include "pg_config_os.h"
#endif

/* Must be before gettext() games below */
#include <locale.h>

/* Suppress redefined warning */
#undef _
#define _(x) gettext(x)

#ifndef likely
#define likely(x) __builtin_expect((x) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x) != 0, 0)
#endif

#ifdef ENABLE_NLS
#include <libintl.h>
#else
#define gettext(x) (x)

#define dgettext(d, x) (x)
#define ngettext(s, p, n) ((n) == 1 ? (s) : (p))
#define dngettext(d, s, p, n) ((n) == 1 ? (s) : (p))
#endif

/*
 *	Use this to mark string constants as needing translation at some later
 *	time, rather than immediately.	This is useful for cases where you need
 *	access to the original string and translated string, and for cases where
 *	immediate translation is not possible, like when initializing global
 *	variables.
 *		http://www.gnu.org/software/autoconf/manual/gettext/Special-cases.html
 */
#define gettext_noop(x) (x)

/* ----------------------------------------------------------------
 *				Section 1: hacks to cope with non-ANSI C compilers
 *
 * type prefixes (const, signed, volatile, inline) are handled in pg_config.h.
 * ----------------------------------------------------------------
 */

/*
 * CppAsString
 *		Convert the argument to a string, using the C preprocessor.
 * CppConcat
 *		Concatenate two arguments together, using the C preprocessor.
 *
 * Note: the standard Autoconf macro AC_C_STRINGIZE actually only checks
 * whether #identifier works, but if we have that we likely have ## too.
 */
#if defined(HAVE_STRINGIZE)

#define CppAsString(identifier) #identifier
#define CppConcat(x, y) x##y
#else /* !HAVE_STRINGIZE */

#define CppAsString(identifier) "identifier"

/*
 * CppIdentity -- On Reiser based cpp's this is used to concatenate
 *		two tokens.  That is
 *				CppIdentity(A)B ==> AB
 *		We renamed it to _private_CppIdentity because it should not
 *		be referenced outside this file.  On other cpp's it
 *		produces  A  B.
 */
#define _priv_CppIdentity(x) x
#define CppConcat(x, y) _priv_CppIdentity(x) y
#endif /* !HAVE_STRINGIZE */

/*
 * dummyret is used to set return values in macros that use ?: to make
 * assignments.  gcc wants these to be void, other compilers like char
 */
#ifdef __GNUC__ /* GNU cc */
#define dummyret void
#else
#define dummyret char
#endif

#ifndef __GNUC__
#define __attribute__(_arg_)
#endif

/*
 * Mark a point as unreachable in a portable fashion.  This should preferably
 * be something that the compiler understands, to aid code generation.
 * In assert-enabled builds, we prefer abort() for debugging reasons.
 */
#if defined(HAVE__BUILTIN_UNREACHABLE) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __builtin_unreachable()
#elif defined(_MSC_VER) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __assume(0)
#else
#define pg_unreachable() abort()
#endif

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

#ifndef __cplusplus

#ifndef bool
typedef char bool;
#endif

#ifndef true
#define true ((bool)1)
#endif

#ifndef false
#define false ((bool)0)
#endif
#endif /* not C++ */

typedef bool* BoolPtr;

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

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
#ifdef HAVE_LONG_INT_64
/* Plain "long int" fits, use it */

#ifndef HAVE_INT64
typedef long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int uint64;
#endif
#elif defined(HAVE_LONG_LONG_INT_64)
/* We have working support for "long long int", use that */

#ifndef HAVE_INT64
typedef long long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long long int uint64;
#endif
#else
/* neither HAVE_LONG_INT_64 nor HAVE_LONG_LONG_INT_64 */
#error must have a working 64-bit integer datatype
#endif

/* Decide if we need to decorate 64-bit constants */
#ifdef HAVE_LL_CONSTANTS
#define INT64CONST(x) ((int64)x##LL)
#define UINT64CONST(x) ((uint64)x##ULL)
#else
#define INT64CONST(x) ((int64)(x))
#define UINT64CONST(x) ((uint64)(x))
#endif

/*
 * stdint.h limits aren't guaranteed to be present and aren't guaranteed to
 * have compatible types with our fixed width types. So just define our own.
 */
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

/* Select timestamp representation (float8 or int64) */
#ifdef USE_INTEGER_DATETIMES
#define HAVE_INT64_TIMESTAMP
#endif

/* sig_atomic_t is required by ANSI C, but may be missing on old platforms */
#ifndef HAVE_SIG_ATOMIC_T
typedef int sig_atomic_t;
#endif

/*
 * Size
 *		Size of any memory resident object, as returned by sizeof.
 */
typedef size_t Size;

/*
 * Index
 *		Index into any memory resident array.
 *
 * Note:
 *		Indices are non negative.
 */
typedef unsigned int Index;

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

/*
 * 128-bit signed and unsigned integers
 *		There currently is only limited support for such types.
 *		E.g. 128bit literals and snprintf are not supported; but math is.
 *		Also, because we exclude such types when choosing MAXIMUM_ALIGNOF,
 *		it must be possible to coerce the compiler to allocate them on no
 *		more than MAXALIGN boundaries.
 */

#if defined(__GNUC__) || defined(__SUNPRO_C) || defined(__IBMC__)
#define pg_attribute_aligned(a) __attribute__((aligned(a)))
#endif

#ifndef ENABLE_DEFAULT_GCC
#if !defined(WIN32)
    #if defined(pg_attribute_aligned) || ALIGNOF_PG_INT128_TYPE <= MAXIMUM_ALIGNOF
        typedef __int128 int128  
        #if defined(pg_attribute_aligned)
                    pg_attribute_aligned(MAXIMUM_ALIGNOF)
        #endif
                    ;

        typedef unsigned __int128 uint128  
        #if defined(pg_attribute_aligned)
                    pg_attribute_aligned(MAXIMUM_ALIGNOF)
        #endif
                    ;
    #else
        ereport(ERROR, (errmsg("the compiler can't support int128 or uint128 aligned on a 8-byte boundary.")));
    #endif
#endif
#else
#ifdef __linux__
    #if __GNUC__ >= 7
        #if defined(pg_attribute_aligned) || ALIGNOF_PG_INT128_TYPE <= MAXIMUM_ALIGNOF
            typedef __int128 int128
            #if defined(pg_attribute_aligned)
                        pg_attribute_aligned(MAXIMUM_ALIGNOF)
            #endif
                        ;
            
            typedef unsigned __int128 uint128
            #if defined(pg_attribute_aligned)
                        pg_attribute_aligned(MAXIMUM_ALIGNOF)
            #endif
                        ;
        #else
            ereport(ERROR, (errmsg("the compiler can't support int128 or uint128 aligned on a 8-byte boundary.")));
        #endif
    #endif
#endif
#endif

#if !defined(WIN32)
typedef union {
    uint128   u128;
    uint64    u64[2];
    uint32    u32[4];
} uint128_u;
#endif

/*
 * int128 type has 128 bits.
 * INT128_MIN is (-1 * (1 << 127))
 * INT128_MAX is ((1 << 127) - 1)
 * UINT128_MIN is 0
 * UINT128_MAX is ((1 << 128) - 1)
 *
 */
#define INT128_MAX (int128)(((uint128)1 << 127) - 1)
#define INT128_MIN (-INT128_MAX - 1)
#define UINT128_MAX (((uint128)INT128_MAX << 1) + 1)

#define PG_INT128_MAX INT128_MAX
#define PG_INT128_MIN INT128_MIN
#define PG_UINT128_MAX UINT128_MAX

#define UINT128_IS_EQUAL(x, y) ((x).u128 == (y).u128)
#define UINT128_COPY(x, y)  (x).u128 = (y).u128


/*
 * Oid, RegProcedure, TransactionId, SubTransactionId, MultiXactId,
 * CommandId
 */

/* typedef Oid is in postgres_ext.h */

/*
 * regproc is the type name used in the include/catalog headers, but
 * RegProcedure is the preferred name in C code.
 */
typedef Oid regproc;
typedef regproc RegProcedure;

/* Macro for checking XID 64-bitness */
#define XID_IS_64BIT
#define MAX_START_XID UINT64CONST(0x3fffffffffffffff)

typedef uint64 TransactionId;

#define TransactionIdPrecedes(id1, id2) ((id1) < (id2))
#define TransactionIdPrecedesOrEquals(id1, id2) ((id1) <= (id2))
#define TransactionIdFollows(id1, id2) ((id1) > (id2))
#define TransactionIdFollowsOrEquals(id1, id2) ((id1) >= (id2))

#define StartTransactionIdIsValid(start_xid) ((start_xid) <= MAX_START_XID)

typedef uint32 ShortTransactionId;

typedef uint64 LocalTransactionId;

typedef uint64 SubTransactionId;

#define XID_FMT UINT64_FORMAT

#define CSN_FMT UINT64_FORMAT

#define InvalidSubTransactionId ((SubTransactionId)0)
#define TopSubTransactionId ((SubTransactionId)1)

typedef TransactionId MultiXactId;
#define MultiXactIdPrecedes(id1, id2) ((id1) < (id2))
#define MultiXactIdPrecedesOrEquals(id1, id2) ((id1) <= (id2))
#define MultiXactIdFollows(id1, id2) ((id1) > (id2))
#define MultiXactIdFollowsOrEquals(id1, id2) ((id1) >= (id2))

typedef uint64 MultiXactOffset;

/* MultiXactId must be equivalent to TransactionId, to fit in t_xmax */
#define StartMultiXactIdIsValid(start_mx_id) ((start_mx_id) <= MAX_START_XID)
#define StartMultiXactOffsetIsValid(start_mx_offset) ((start_mx_offset) <= MAX_START_XID)
typedef uint32 CommandId;

#define FirstCommandId ((CommandId)0)
#define InvalidCommandId (~(CommandId)0)

/*
 * CommitSeqNo is currently an LSN, but keep use a separate datatype for clarity.
 */
typedef uint64 CommitSeqNo;

#define InvalidCommitSeqNo ((CommitSeqNo)0)

/*
 * Array indexing support
 */
#define MAXDIM 6
typedef struct {
    int indx[MAXDIM];
} IntArray;

/* ----------------
 *		Variable-length datatypes all share the 'struct varlena' header.
 *
 * NOTE: for TOASTable types, this is an oversimplification, since the value
 * may be compressed or moved out-of-line.	However datatype-specific routines
 * are mostly content to deal with de-TOASTed values only, and of course
 * client-side routines should never see a TOASTed value.  But even in a
 * de-TOASTed value, beware of touching vl_len_ directly, as its representation
 * is no longer convenient.  It's recommended that code always use the VARDATA,
 * VARSIZE, and SET_VARSIZE macros instead of relying on direct mentions of
 * the struct fields.  See postgres.h for details of the TOASTed form.
 * ----------------
 */
struct varlena {
    char vl_len_[4]; /* Do not touch this field directly! */
    char vl_dat[FLEXIBLE_ARRAY_MEMBER];
};

#define VARHDRSZ ((int32)sizeof(int32))

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE(ptr) - VARHDRSZ.
 */
typedef struct varlena bytea;
typedef struct varlena byteawithoutorderwithequalcol;
typedef struct varlena text;
typedef struct varlena BpChar;    /* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar;   /* var-length char, ie SQL varchar(n) */
typedef struct varlena NVarChar2; /* var-length char, ie SQL nvarchar2(n) */
/*
 * Specialized array types.  These are physically laid out just the same
 * as regular arrays (so that the regular array subscripting code works
 * with them).	They exist as distinct types mostly for historical reasons:
 * they have nonstandard I/O behavior which we don't want to change for fear
 * of breaking applications that look at the system catalogs.  There is also
 * an implementation issue for oidvector: it's part of the primary key for
 * pg_proc, and we can't use the normal btree array support routines for that
 * without circularity.
 */
typedef struct {
    int32 vl_len_;    /* these fields must match ArrayType! */
    int ndim;         /* always 1 for int2vector */
    int32 dataoffset; /* always 0 for int2vector */
    Oid elemtype;
    int dim1;
    int lbound1;
    int2 values[FLEXIBLE_ARRAY_MEMBER];
} int2vector;

typedef int2vector int2vector_extend;

typedef struct {
    int32 vl_len_;    /* these fields must match ArrayType! */
    int ndim;         /* always 1 for oidvector */
    int32 dataoffset; /* always 0 for oidvector */
    Oid elemtype;
    int dim1;
    int lbound1;
    Oid values[FLEXIBLE_ARRAY_MEMBER];
} oidvector;

typedef oidvector oidvector_extend;

typedef struct ArrayInt4 {
    int32 count;
    int4* values;
} ArrayInt4;

typedef struct ArrayInt2 {
    int32 count;
    int2* values;
} ArrayInt2;

typedef struct ArrayOid {
    int32 count;
    Oid* values;
} ArrayOid;

typedef struct ArrayChar {
    int32 count;
    char* values;
} ArrayChar;

typedef struct ArrayCStr {
    int32 count;
    char** values;
} ArrayCStr;

/* the array type of aclitem */
typedef ArrayInt4 ArrayAcl;

#define MakeArray(_type, cnt, ...)                               \
    {                                                            \
        .count = cnt, .values = (cnt == 0) ? NULL : (_type[cnt]) \
        {                                                        \
            __VA_ARGS__                                          \
        }                                                        \
    }

#define MakeArrayInt4(cnt, ...) (ArrayInt4) MakeArray(int4, cnt, __VA_ARGS__)
#define MakeArrayInt2(cnt, ...) (ArrayInt2) MakeArray(int2, cnt, __VA_ARGS__)
#define MakeArrayOid(cnt, ...) (ArrayOid) MakeArray(Oid, cnt, __VA_ARGS__)
#define MakeArrayChar(cnt, ...) (ArrayChar) MakeArray(char, cnt, __VA_ARGS__)
#define MakeArrayCStr(cnt, ...) (ArrayCStr) MakeArray(char*, cnt, __VA_ARGS__)

#define MakeArrayOidPtr(cnt, ...)        \
    (ArrayOid[1])                        \
    {                                    \
        MakeArray(Oid, cnt, __VA_ARGS__) \
    }
#define MakeArrayCStrPtr(cnt, ...)         \
    (ArrayCStr[1])                         \
    {                                      \
        MakeArray(char*, cnt, __VA_ARGS__) \
    }
#define MakeArrayCharPtr(cnt, ...)        \
    (ArrayChar[1])                        \
    {                                     \
        MakeArray(char, cnt, __VA_ARGS__) \
    }
#define MakeArrayInt4Ptr(cnt, ...)        \
    (ArrayInt4[1])                        \
    {                                     \
        MakeArray(int4, cnt, __VA_ARGS__) \
    }
#define MakeArrayInt2Ptr(cnt, ...)        \
    (ArrayInt2[1])                        \
    {                                     \
        MakeArray(int2, cnt, __VA_ARGS__) \
    }

#define MakeSingleValuePtr(_type, val) \
    (_type[1])                         \
    {                                  \
        val                            \
    }

/*
 * Representation of a Name: effectively just a C string, but null-padded to
 * exactly NAMEDATALEN bytes.  The use of a struct is historical.
 */
typedef struct nameData {
    char data[NAMEDATALEN];
} NameData;
typedef NameData* Name;

#define NameStr(name) ((name).data)

typedef struct pathData {
    char data[MAXPGPATH];
} PathData;

/*
 * Support macros for escaping strings.  escape_backslash should be TRUE
 * if generating a non-standard-conforming string.	Prefixing a string
 * with ESCAPE_STRING_SYNTAX guarantees it is non-standard-conforming.
 * Beware of multiple evaluation of the "ch" argument!
 */
#define SQL_STR_DOUBLE(ch, escape_backslash) ((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))

#define ESCAPE_STRING_SYNTAX 'E'

/* ----------------------------------------------------------------
 *				Section 4:	IsValid macros for system types
 * ----------------------------------------------------------------
 */
/*
 * BoolIsValid
 *		True iff bool is valid.
 */
#define BoolIsValid(boolean) ((boolean) == false || (boolean) == true)

/*
 * PointerIsValid
 *		True iff pointer is valid.
 */
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

/*
 * PointerIsAligned
 *		True iff pointer is properly aligned to point to the given type.
 */
#define PointerIsAligned(pointer, type) (((intptr_t)(pointer) % (sizeof(type))) == 0)
#define OffsetToPointer(base, offset) \
        ((void *)((char *)(base) + (offset)))

#define OidIsValid(objectId) ((bool)((objectId) != InvalidOid))

#define RegProcedureIsValid(p) OidIsValid(p)

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

/*
 * endof
 *		Address of the element one past the last in an array.
 */
#define endof(array) (&(array)[lengthof(array)])

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
#define IS_TYPE_ALIGINED(ALIGNVAL, LEN) ((((uintptr_t)(LEN)) & ((uintptr_t)((ALIGNVAL) - 1))) == 0)
#define SHORTALIGN(LEN) TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN) TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN) TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN) TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
/* MAXALIGN covers only built-in types, not buffers */
#define BUFFERALIGN(LEN) TYPEALIGN(ALIGNOF_BUFFER, (LEN))
#define CACHELINEALIGN(LEN) TYPEALIGN(PG_CACHE_LINE_SIZE, (LEN))

#define TYPEALIGN_DOWN(ALIGNVAL, LEN) (((uintptr_t)(LEN)) & ~((uintptr_t)((ALIGNVAL)-1)))

#define SHORTALIGN_DOWN(LEN) TYPEALIGN_DOWN(ALIGNOF_SHORT, (LEN))
#define INTALIGN_DOWN(LEN) TYPEALIGN_DOWN(ALIGNOF_INT, (LEN))
#define LONGALIGN_DOWN(LEN) TYPEALIGN_DOWN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN_DOWN(LEN) TYPEALIGN_DOWN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN_DOWN(LEN) TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))

/* ----------------------------------------------------------------
 *				Section 6:	widely useful macros
 * ----------------------------------------------------------------
 */
/*
 * Max
 *		Return the maximum of two numbers.
 */
#define Max(x, y) ((x) > (y) ? (x) : (y))

#define MaxTriple(x, y, z) ((x) > (y) ? ((x) > (z) ? (x) : (z)) : ((y) > (z) ? (y) : (z)))

/*
 * Min
 *		Return the minimum of two numbers.
 */
#define Min(x, y) ((x) < (y) ? (x) : (y))

/*
 * Abs
 *		Return the absolute value of the argument.
 */
#define Abs(x) ((x) >= 0 ? (x) : -(x))

/*
 * MemCpy
 *		same as memcpy.
 */
#if defined(__x86_64__) && !defined(WIN32)

static inline void* MemCpy(void* dest, const void* src, Size len)
{
    if (len <= 1024) {
        if (len >= 4) {
            __asm__ __volatile__("shr $2,%2\n"
                                 "rep movsl\n"
                                 "testb $2,%b6\n"
                                 "je 1f\n"
                                 "movsw\n"
                                 "1:\n"
                                 "testb $1,%b6\n"
                                 "je 2f\n"
                                 "movsb\n"
                                 "2:"
                                 : "=&D"(dest), "=&S"(src), "=&c"(len)
                                 : "0"(dest), "1"(src), "2"(len), "q"(len)
                                 : "memory");

            return dest;
        }

        __asm__ __volatile__("rep movsb"
                             : "=&D"(dest), "=&S"(src), "=&c"(len)
                             : "0"(dest), "1"(src), "2"(len)
                             : "memory");

        return dest;
    }

    return memcpy(dest, src, len);
}

#else
static inline void* MemCpy(void* dest, const void* src, Size len)
{
    return memcpy(dest, src, len);
}
#endif

/*
 * StrNCpy
 *	Like standard library function strncpy(), except that result string
 *	is guaranteed to be null-terminated --- that is, at most N-1 bytes
 *	of the source string will be kept.
 *	Also, the macro returns no result (too hard to do that without
 *	evaluating the arguments multiple times, which seems worse).
 *
 *	BTW: when you need to copy a non-null-terminated string (like a text
 *	datum) and add a null, do not do it with StrNCpy(..., len+1).  That
 *	might seem to work, but it fetches one byte more than there is in the
 *	text object.  One fine day you'll have a SIGSEGV because there isn't
 *	another byte before the end of memory.	Don't laugh, we've had real
 *	live bug reports from real live users over exactly this mistake.
 *	Do it honestly with "memcpy(dst,src,len); dst[len] = '\0';", instead.
 */
#define StrNCpy(dst, src, len)          \
    do {                                \
        char* _dst = (dst);             \
        Size _len = (len);              \
                                        \
        if (_len > 0) {                 \
            strncpy(_dst, (src), _len); \
            _dst[_len - 1] = '\0';      \
        }                               \
    } while (0)

/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)

/*
 * MemSet
 *	Exactly the same as standard library function memset(), but considerably
 *	faster for zeroing small word-aligned structures (such as parsetree nodes).
 *	This has to be a macro because the main point is to avoid function-call
 *	overhead.	However, we have also found that the loop is faster than
 *	native libc memset() on some platforms, even those with assembler
 *	memset() functions.  More research needs to be done, perhaps with
 *	MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len)                                                                            \
    do {                                                                                                   \
        /* must be void* because we don't know if it is integer aligned yet */                             \
        void* _vstart = (void*)(start);                                                                    \
        int _val = (val);                                                                                  \
        Size _len = (len);                                                                                 \
                                                                                                           \
        if ((((uintptr_t)_vstart) & LONG_ALIGN_MASK) == 0 && (_len & LONG_ALIGN_MASK) == 0 && _val == 0 && \
            _len <= MEMSET_LOOP_LIMIT && /*                                                                \
                                          *	If MEMSET_LOOP_LIMIT == 0, optimizer should find               \
                                          *	the whole "if" false at compile time.                          \
                                          */                                                               \
            MEMSET_LOOP_LIMIT != 0) {                                                                      \
            long* _start = (long*)_vstart;                                                                 \
            long* _stop = (long*)((char*)_start + _len);                                                   \
            while (_start < _stop)                                                                         \
                *_start++ = 0;                                                                             \
        } else                                                                                             \
            memset(_vstart, _val, _len);                                                                   \
    } while (0)

/*
 * MemSetAligned is the same as MemSet except it omits the test to see if
 * "start" is word-aligned.  This is okay to use if the caller knows a-priori
 * that the pointer is suitably aligned (typically, because he just got it
 * from palloc(), which always delivers a max-aligned pointer).
 */
#define MemSetAligned(start, val, len)                                                                           \
    do {                                                                                                         \
        long* _start = (long*)(start);                                                                           \
        int _val = (val);                                                                                        \
        Size _len = (len);                                                                                       \
                                                                                                                 \
        if ((_len & LONG_ALIGN_MASK) == 0 && _val == 0 && _len <= MEMSET_LOOP_LIMIT && MEMSET_LOOP_LIMIT != 0) { \
            long* _stop = (long*)((char*)_start + _len);                                                         \
            while (_start < _stop)                                                                               \
                *_start++ = 0;                                                                                   \
        } else                                                                                                   \
            memset(_start, _val, _len);                                                                          \
    } while (0)

/*
 * MemSetTest/MemSetLoop are a variant version that allow all the tests in
 * MemSet to be done at compile time in cases where "val" and "len" are
 * constants *and* we know the "start" pointer must be word-aligned.
 * If MemSetTest succeeds, then it is okay to use MemSetLoop, otherwise use
 * MemSetAligned.  Beware of multiple evaluations of the arguments when using
 * this approach.
 */
#define MemSetTest(val, len) \
    (((len)&LONG_ALIGN_MASK) == 0 && (len) <= MEMSET_LOOP_LIMIT && MEMSET_LOOP_LIMIT != 0 && (val) == 0)

#define MemSetLoop(start, val, len)                         \
    do {                                                    \
        long* _start = (long*)(start);                      \
        long* _stop = (long*)((char*)_start + (Size)(len)); \
                                                            \
        while (_start < _stop)                              \
            *_start++ = 0;                                  \
    } while (0)

/* ----------------------------------------------------------------
 *				Section 7:	random stuff
 * ----------------------------------------------------------------
 */

/* msb for char */
#define HIGHBIT 0x80
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch)&HIGHBIT)

#define STATUS_OK 0
#define STATUS_ERROR (-1)
#define STATUS_EOF (-2)
#define STATUS_WRONG_PASSWORD (-3)
#define STATUS_EXPIRED (-4)
#define STATUS_FOUND 1
#define STATUS_WAITING 2
#define STATUS_FOUND_NEED_CANCEL 3

/*
 * Append PG_USED_FOR_ASSERTS_ONLY to definitions of variables that are only
 * used in assert-enabled builds, to avoid compiler warnings about unused
 * variables in assert-disabled builds.
 */
#ifdef USE_ASSERT_CHECKING
#define PG_USED_FOR_ASSERTS_ONLY
#else
#define PG_USED_FOR_ASSERTS_ONLY __attribute__((unused))
#endif

#if defined(__GNUC__) || defined(__SUNPRO_C) || defined(__IBMC__)
#define pg_noinline __attribute__((noinline))
#elif defined(_MSC_VER)
#define pg_noinline __declspec(noinline)
#else
#define pg_noinline
#endif

// Conditionally disable a feature
//
// To reduce the exposure surface of the engine, we disabled some seldom
// used, or not-confident features. But we still want keep them minimally
// execised to make sure later code changes won't break them totally. Thus
// we differentiate them with a configure directive, and test them only
// when it is enabled. For public release, the features are disabled.
//

#ifdef PGXC
#define FEATURE_NOT_PUBLIC_ERROR(x)                                                \
    do {                                                                           \
        /* initdb might use some of the features, hornour it */                    \
        if (!IsInitdb && !g_instance.attr.attr_common.support_extended_features && \
            !u_sess->attr.attr_common.IsInplaceUpgrade)                            \
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(x)));   \
    } while (0)
#endif /*PGXC*/

/* gettext domain name mangling */

/*
 * To better support parallel installations of major PostgeSQL
 * versions as well as parallel installations of major library soname
 * versions, we mangle the gettext domain name by appending those
 * version numbers.  The coding rule ought to be that whereever the
 * domain name is mentioned as a literal, it must be wrapped into
 * PG_TEXTDOMAIN().  The macros below do not work on non-literals; but
 * that is somewhat intentional because it avoids having to worry
 * about multiple states of premangling and postmangling as the values
 * are being passed around.
 *
 * Make sure this matches the installation rules in nls-global.mk.
 */

/* need a second indirection because we want to stringize the macro value, not the name */
#define CppAsString2(x) CppAsString(x)

#ifdef SO_MAJOR_VERSION
#define PG_TEXTDOMAIN(domain) (domain CppAsString2(SO_MAJOR_VERSION) "-" PG_MAJORVERSION)
#else
#define PG_TEXTDOMAIN(domain) (domain "-" PG_MAJORVERSION)
#endif

#ifndef WIN32
// Compiler hint to force inlining or unlining a function
//
#if defined(_MSC_VER)
#define FORCE_INLINE __forceinline
#define NO_INLINE __declspec(noinline)
#else
#if (!defined ENABLE_LLT) && (!defined ENABLE_UT)
#define FORCE_INLINE __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif
#define NO_INLINE __attribute__((noinline))
// auto-vectorization flags
// used in function declaration to enable auto-vectorization without the need to add flags to cmake
#define AUTO_VECTORIZE __attribute__((optimize( \
    "tree-vectorize", "unswitch-loops", "tree-loop-vectorize", "tree-loop-distribute-patterns", \
    "split-paths", "tree-slp-vectorize",  "vect-cost-model",  "tree-partial-pre", "peel-loops", \
    "opt-info-loop-optimized")))
#endif

// SAL annotations -- remove when compiler annotation header is fixed.
//
#define _in_     // input argument: function reads it
#define _out_    // output argument: function writes it
#define __inout  // both in and out argument: function r/w it
#endif           /* WIN32 */
/* ----------------------------------------------------------------
 *				Section 8: system-specific hacks
 *
 *		This should be limited to things that absolutely have to be
 *		included in every source file.	The port-specific header file
 *		is usually a better place for this sort of thing.
 * ----------------------------------------------------------------
 */

/*
 *	NOTE:  this is also used for opening text files.
 *	WIN32 treats Control-Z as EOF in files opened in text mode.
 *	Therefore, we open files in binary mode on Win32 so we can read
 *	literal control-Z.	The other affect is that we see CRLF, but
 *	that is OK because we can already handle those cleanly.
 */
#if defined(WIN32) || defined(__CYGWIN__)
#define PG_BINARY O_BINARY
#define PG_BINARY_A "ab"
#define PG_BINARY_R "rb"
#define PG_BINARY_W "wb"
#define PG_BINARY_RW "r+"
#else
#define PG_BINARY 0
#define PG_BINARY_A "a"
#define PG_BINARY_R "r"
#define PG_BINARY_W "w"
#define PG_BINARY_RW "r+"
#endif

/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * throw a compile error using the "errmessage" (a string literal).
 *
 * gcc 4.6 and up supports _Static_assert(), but there are bizarre syntactic
 * placement restrictions.  These macros make it safe to use as a statement
 * or in an expression, respectively.
 *
 * Otherwise we fall back on a kluge that assumes the compiler will complain
 * about a negative width for a struct bit-field.  This will not include a
 * helpful error message, but it beats not getting an error at all.
 */
#define StaticAssertStmt(condition, errmessage) ((void)(1 / (int)(!!(condition))))
#define StaticAssertExpr(condition, errmessage) StaticAssertStmt(condition, errmessage)

/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is intended for use in macros, eg
 *		#define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it provides at least some coverage.
 */

#define AssertVariableIsOfType(varname, typename) \
    StaticAssertStmt(                             \
        sizeof(varname) == sizeof(typename), CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
    (StaticAssertExpr(                           \
        sizeof(varname) == sizeof(typename), CppAsString(varname) " does not have type " CppAsString(typename)))
/*
 * Provide prototypes for routines not present in a particular machine's
 * standard C library.
 */

#if !HAVE_DECL_SNPRINTF
extern int snprintf(char* str, size_t count, const char* fmt, ...)
    /* This extension allows gcc to check the format string */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
#endif

#if !HAVE_DECL_VSNPRINTF
extern int vsnprintf(char* str, size_t count, const char* fmt, va_list args);
#endif

/* no special DLL markers on most ports */
#ifndef PGDLLIMPORT
#define PGDLLIMPORT
#endif
#ifndef PGDLLEXPORT
#define PGDLLEXPORT
#endif

/*
 * The following is used as the arg list for signal handlers.  Any ports
 * that take something other than an int argument should override this in
 * their pg_config_os.h file.  Note that variable names are required
 * because it is used in both the prototypes as well as the definitions.
 * Note also the long name.  We expect that this won't collide with
 * other names causing compiler warnings.
 */

#ifndef SIGNAL_ARGS
#define SIGNAL_ARGS int postgres_signal_arg
#endif

/*
 * When there is no sigsetjmp, its functionality is provided by plain
 * setjmp. Incidentally, nothing provides setjmp's functionality in
 * that case.
 */
#ifndef HAVE_SIGSETJMP
#define sigjmp_buf jmp_buf
#define sigsetjmp(x, y) setjmp(x)
#define siglongjmp longjmp
#endif

#if defined(HAVE_FDATASYNC) && !HAVE_DECL_FDATASYNC
extern int fdatasync(int fildes);
#endif

/* If strtoq() exists, rename it to the more standard strtoll() */
#if defined(HAVE_LONG_LONG_INT_64) && !defined(HAVE_STRTOLL) && defined(HAVE_STRTOQ)
#define strtoll strtoq
#define HAVE_STRTOLL 1
#endif

/* If strtouq() exists, rename it to the more standard strtoull() */
#if defined(HAVE_LONG_LONG_INT_64) && !defined(HAVE_STRTOULL) && defined(HAVE_STRTOUQ)
#define strtoull strtouq
#define HAVE_STRTOULL 1
#endif

/*
 * We assume if we have these two functions, we have their friends too, and
 * can use the wide-character functions.
 */
#if defined(HAVE_WCSTOMBS) && defined(HAVE_TOWLOWER)
#define USE_WIDE_UPPER_LOWER
#endif

#define EXEC_BACKEND

/* EXEC_BACKEND defines */
#ifdef EXEC_BACKEND
#define NON_EXEC_STATIC
#else
#define NON_EXEC_STATIC static
#endif

/* ----------------------------------------------------------------
 *				Section 9: C++-specific stuff
 *
 *		This should be limited to stuff that are C++ language specific.
 * ----------------------------------------------------------------
 */
#ifndef WIN32
#ifdef __cplusplus

// The rtl namespace (read as "run time library") contains some crystally clear
// stuff from STL, which we do not encourage to use to avoid potential issues.
// Fortunately there shouldn't be much to copy here.
//
namespace rtl {
template <class T>
const T& max(const T& a, const T& b)
{
    return (a >= b) ? a : b;
}

template <class T>
const T& min(const T& a, const T& b)
{
    return (a < b) ? a : b;
}
}  // namespace rtl

#endif /* __cplusplus */
#endif /* WIN32 */

/* /port compatibility functions */
#include "port.h"

#define LOG2(x) (log(x) / 0.693147180559945)

#define pg_restrict __restrict

#define INT2UINT64(val) (unsigned int64)((unsigned int)(val))

#define INT2ULONG(val) (unsigned long)((unsigned int)(val))

#define INT2SIZET(val) (Size)((unsigned int)(val))

#define isIntergratedMachine false  // not work for now, adapt it later

typedef void* Tuple;

#endif /* C_H */
