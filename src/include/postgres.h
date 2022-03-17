/* -------------------------------------------------------------------------
 *
 * postgres.h
 *	  Primary include file for PostgreSQL server .c files
 *
 * This should be the first file included by PostgreSQL backend modules.
 * Client-side code should include postgres_fe.h instead.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1995, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/postgres.h
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
 *		1)		variable-length datatypes (TOAST support)
 *		2)		datum type + support macros
 *		3)		exception handling definitions
 *
 *	 NOTES
 *
 *	In general, this file should contain declarations that are widely needed
 *	in the backend environment, but are of no interest outside the backend.
 *
 *	Simple type definitions live in c.h, where they are shared with
 *	postgres_fe.h.	We do that since those type definitions are needed by
 *	frontend modules that want to deal with binary data transmission to or
 *	from the backend.  Type definitions in this file should be for
 *	representations that never escape the backend, such as Datum or
 *	TOASTed varlena objects.
 *
 * ----------------------------------------------------------------
 */
#ifndef POSTGRES_H
#define POSTGRES_H

#ifdef PC_LINT
#define THR_LOCAL
#endif

#include "c.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "storage/spin.h"

#ifndef WIN32
#include <cxxabi.h>
#endif /* win-odbc */

#ifndef HDFS_FDW
#define HDFS_FDW "hdfs_fdw"
#endif

#ifndef HDFS
#define HDFS "hdfs"
#endif

#ifndef OBS
#define OBS "obs"
#endif

#ifndef DIST_FDW
#define DIST_FDW "dist_fdw"
#endif

#ifndef MYSQL_FDW
#define MYSQL_FDW "mysql_fdw"
#endif

#ifndef ORACLE_FDW
#define ORACLE_FDW "oracle_fdw"
#endif

#ifndef POSTGRES_FDW
#define POSTGRES_FDW "postgres_fdw"
#endif

#ifndef MOT_FDW
#define MOT_FDW "mot_fdw"
#endif

#ifndef MOT_FDW_SERVER
#define MOT_FDW_SERVER "mot_server"
#endif

#ifndef DFS_FDW
#define DFS_FDW "dfs_fdw"
#endif

#ifndef LOG_FDW
#define LOG_FDW "log_fdw"
#define LOG_SRV "log_srv"
#endif /* LOG_FDW */

#ifndef GC_FDW
#define GC_FDW "gc_fdw"
#endif

#include "securec.h"
#include "securec_check.h"

#define AUXILIARY_BACKENDS 20
/* the maximum number of autovacuum launcher thread */
#define AV_LAUNCHER_PROCS 2
#define BITS_PER_INT (BITS_PER_BYTE * sizeof(int))

#define STREAM_RESERVE_PROC_TIMES (16)

/* for CLOB/BLOB more than 1GB, the first chunk threadhold. */
#define MAX_TOAST_CHUNK_SIZE 1073741771

/* this struct is used to store connection info got from pool */
typedef struct {
    /* hostname of the connection */
    nameData host;
    /* port of the connection */
    int port;
} PoolConnInfo;

/* this enum type is used to mark om online operation. */
typedef enum { OM_ONLINE_EXPANSION, OM_ONLINE_NODE_REPLACE } OM_ONLINE_STATE;

extern void reload_configfile(void);
extern void reload_online_pooler(void);
extern OM_ONLINE_STATE get_om_online_state(void);

/* ----------------------------------------------------------------
 *				Section 1:	variable-length datatypes (TOAST support)
 * ----------------------------------------------------------------
 */

/*
 * struct varatt_external is a "TOAST pointer", that is, the information
 * needed to fetch a stored-out-of-line Datum.	The data is compressed
 * if and only if va_extsize < va_rawsize - VARHDRSZ.  This struct must not
 * contain any padding, because we sometimes compare pointers using memcmp.
 *
 * Note that this information is stored unaligned within actual tuples, so
 * you need to memcpy from the tuple into a local struct variable before
 * you can look at these fields!  (The reason we use memcmp is to avoid
 * having to do that just to detect equality of two TOAST pointers...)
 */
typedef struct varatt_external {
    int32 va_rawsize;  /* Original data size (includes header) */
    int32 va_extsize;  /* External saved size (doesn't) */
    Oid va_valueid;    /* Unique ID of value within TOAST table */
    Oid va_toastrelid; /* RelID of TOAST table containing it */
} varatt_external;

typedef struct varatt_lob_external {
    int64 va_rawsize;  /* Original data size (includes header) */
    Oid va_valueid;    /* Unique ID of value within TOAST table */
    Oid va_toastrelid; /* RelID of TOAST table containing it */
} varatt_lob_external;

typedef struct varatt_lob_pointer {
    Oid relid;
    int2 columid;
    int2 bucketid;
    uint16 bi_hi;
    uint16 bi_lo;
    uint16 ip_posid;
} varatt_lob_pointer;

/*
 * Out-of-line Datum thats stored in memory in contrast to varatt_external
 * pointers which points to data in an external toast relation.
 *
 * Note that just as varatt_external's this is stored unaligned within the
 * tuple.
 */
typedef struct varatt_indirect {
    struct varlena* pointer; /* Pointer to in-memory varlena */
} varatt_indirect;

/*
 * Type of external toast datum stored. The peculiar value for VARTAG_ONDISK
 * comes from the requirement for on-disk compatibility with the older
 * definitions of varattrib_1b_e where v_tag was named va_len_1be...
 */
typedef enum vartag_external { VARTAG_INDIRECT = 1, VARTAG_BUCKET = 8, VARTAG_ONDISK = 18, VARTAG_LOB = 28 } vartag_external;

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

#define TRY_LOCK_ACCOUNT 0
#define TRY_UNLOCK_ACCOUNT 1
#define MAX_USER_NUM 100
typedef struct accounttask {
    Oid roleid;
    int failedcount;
} AccountTask;

/* inline portion of a short varlena pointing to an external resource */
typedef struct {
    uint8 va_header;                     /* Always 0x80 or 0x01 */
    uint8 va_tag;                        /* Type of datum */
    char va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

#ifndef HAVE_DATABASE_TYPE
#define HAVE_DATABASE_TYPE
/*Type of database; increase for sql compatibility*/

typedef enum { 
    A_FORMAT = 0x0001,
    B_FORMAT = 0x0002,
    C_FORMAT = 0x0004,
    PG_FORMAT = 0x0008
} DatabaseType;

#define IS_CMPT(cmpt, flag) (((uint32)cmpt & (uint32)(flag)) != 0)
#define DB_IS_CMPT(flag) IS_CMPT(u_sess->attr.attr_sql.sql_compatibility, (flag))

#endif /* HAVE_DATABASE_TYPE */

typedef enum { EXPLAIN_NORMAL, EXPLAIN_PRETTY, EXPLAIN_SUMMARY, EXPLAIN_RUN } ExplainStyle;

typedef enum { SKEW_OPT_OFF, SKEW_OPT_NORMAL, SKEW_OPT_LAZY } SkewStrategy;

typedef enum { RESOURCE_TRACK_NONE, RESOURCE_TRACK_QUERY, RESOURCE_TRACK_OPERATOR } ResourceTrackOption;

typedef enum {
    CODEGEN_PARTIAL, /* allow to call c-function in codegen */
    CODEGEN_PURE     /* do not allow to call c-function in codegen */
} CodegenStrategy;

typedef enum {
    MEMORY_TRACKING_NONE = 0,     /* not to track the memory usage */
    MEMORY_TRACKING_PEAKMEMORY,
    MEMORY_TRACKING_NORMAL,   /* just update the peak information internal */
    MEMORY_TRACKING_EXECUTOR, /* to logging the memory information in executor */
    MEMORY_TRACKING_FULLEXEC  /* to logging the all memory context information in executor */
} MemoryTrackingStyle;

/*
 * Bit layouts for varlena headers on big-endian machines:
 *
 * 00xxxxxx 4-byte length word, aligned, uncompressed data (up to 1G)
 * 01xxxxxx 4-byte length word, aligned, *compressed* data (up to 1G)
 * 10000000 1-byte length word, unaligned, TOAST pointer
 * 1xxxxxxx 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * Bit layouts for varlena headers on little-endian machines:
 *
 * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
 * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
 * 00000001 1-byte length word, unaligned, TOAST pointer
 * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * The "xxx" bits are the length field (which includes itself in all cases).
 * In the big-endian case we mask to extract the length, in the little-endian
 * case we shift.  Note that in both cases the flag bits are in the physically
 * first byte.	Also, it is not possible for a 1-byte length word to be zero;
 * this lets us disambiguate alignment padding bytes from the start of an
 * unaligned datum.  (We now *require* pad bytes to be filled with zero!)
 */

/*
 * Endian-dependent macros.  These are considered internal --- use the
 * external macros below instead of using these directly.
 *
 * Note: IS_1B is true for external toast records but VARSIZE_1B will return 0
 * for such records. Hence you should usually check for IS_EXTERNAL before
 * checking for IS_1B.
 */

#ifdef WORDS_BIGENDIAN

#define VARATT_IS_4B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_U(PTR) ((((varattrib_1b*)(PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_4B_C(PTR) ((((varattrib_1b*)(PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_1B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x80)
#define VARATT_IS_HUGE_TOAST_POINTER(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x80 && \
    ((((varattrib_1b_e*)(PTR))->va_tag) & 0x01) == 0x01)

#define FUNC_CHECK_HUGE_POINTER(is_null, ptr, funcName)                                                      \
    do {                                                                                                     \
        if (!is_null && unlikely(VARATT_IS_HUGE_TOAST_POINTER((varlena *)ptr))) {                                      \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                        \
                errmsg("%s could not support larger than 1GB clob/blob data", funcName),                     \
                    errcause("parameter larger than 1GB"), erraction("parameter must less than 1GB")));      \
        }                                                                                                    \
    } while (0)

#define VARATT_NOT_PAD_BYTE(PTR) (*((uint8*)(PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) (((varattrib_4b*)(PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) (((varattrib_1b*)(PTR))->va_header & 0x7F)
#define VARSIZE_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_len_1be)

#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b*)(PTR))->va_4byte.va_header = (len)&0x3FFFFFFF)
#define SET_VARSIZE_4B_C(PTR, len) (((varattrib_4b*)(PTR))->va_4byte.va_header = ((len)&0x3FFFFFFF) | 0x40000000)
#define SET_VARSIZE_1B(PTR, len) (((varattrib_1b*)(PTR))->va_header = (len) | 0x80)
#define SET_VARTAG_1B_E(PTR, tag) (((varattrib_1b_e*)(PTR))->va_header = 0x80, ((varattrib_1b_e*)(PTR))->va_tag = (tag))
#define SET_HUGE_TOAST_POINTER_TAG(PTR, tag) (((varattrib_1b_e*)(PTR))->va_header = 0x80, \
    ((varattrib_1b_e*)(PTR))->va_tag = (tag) | 0x01)
#else /* !WORDS_BIGENDIAN */

#define VARATT_IS_4B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) ((((varattrib_1b*)(PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x01)
#define VARATT_IS_HUGE_TOAST_POINTER(PTR) ((((varattrib_1b*)(PTR))->va_header) == 0x01 && \
    ((((varattrib_1b_e*)(PTR))->va_tag) >> 7) == 0x01)

#define FUNC_CHECK_HUGE_POINTER(is_null, ptr, funcName)                                                      \
    do {                                                                                                     \
        if (!is_null && VARATT_IS_HUGE_TOAST_POINTER((varlena *)ptr)) {                                      \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                        \
                errmsg("%s could not support larger than 1GB clob/blob data", funcName),                     \
                    errcause("parameter larger than 1GB"), erraction("parameter must less than 1GB")));      \
        }                                                                                                    \
    } while (0)

#define VARATT_NOT_PAD_BYTE(PTR) (*((uint8*)(PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) ((((varattrib_4b*)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) ((((varattrib_1b*)(PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_tag)
#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b*)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2))
#define SET_VARSIZE_4B_C(PTR, len) (((varattrib_4b*)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR, len) (((varattrib_1b*)(PTR))->va_header = (((uint8)(len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR, tag) (((varattrib_1b_e*)(PTR))->va_header = 0x01, ((varattrib_1b_e*)(PTR))->va_tag = (tag))
#define SET_HUGE_TOAST_POINTER_TAG(PTR, tag) (((varattrib_1b_e*)(PTR))->va_header = 0x01, \
    ((varattrib_1b_e*)(PTR))->va_tag = (tag) | 0x80)
#endif /* WORDS_BIGENDIAN */

#define VARHDRSZ_SHORT offsetof(varattrib_1b, va_data)
#define VARATT_SHORT_MAX 0x7F
#define VARATT_CAN_MAKE_SHORT(PTR) \
    (VARATT_IS_4B_U(PTR) && (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
#define VARATT_CONVERTED_SHORT_SIZE(PTR) (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)

#define VARHDRSZ_EXTERNAL offsetof(varattrib_1b_e, va_data)

#define VARDATA_4B(PTR) (((varattrib_4b*)(PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR) (((varattrib_4b*)(PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR) (((varattrib_1b*)(PTR))->va_data)
#define VARDATA_1B_E(PTR) (((varattrib_1b_e*)(PTR))->va_data)

#define VARRAWSIZE_4B_C(PTR) (((varattrib_4b*)(PTR))->va_compressed.va_rawsize)

/* Externally visible macros */

/*
 * VARDATA, VARSIZE, and SET_VARSIZE are the recommended API for most code
 * for varlena datatypes.  Note that they only work on untoasted,
 * 4-byte-header Datums!
 *
 * Code that wants to use 1-byte-header values without detoasting should
 * use VARSIZE_ANY/VARSIZE_ANY_EXHDR/VARDATA_ANY.  The other macros here
 * should usually be used only by tuple assembly/disassembly code and
 * code that specifically wants to work with still-toasted Datums.
 *
 * WARNING: It is only safe to use VARDATA_ANY() -- typically with
 * PG_DETOAST_DATUM_PACKED() -- if you really don't care about the alignment.
 * Either because you're working with something like text where the alignment
 * doesn't matter or because you're not going to access its constituent parts
 * and just use things like memcpy on it anyways.
 */
#define VARDATA(PTR) VARDATA_4B(PTR)
#define VARSIZE(PTR) VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR) VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR) VARDATA_1B(PTR)

#define VARTAG_EXTERNAL(PTR) VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR) VARDATA_1B_E(PTR)

#define VARATT_IS_COMPRESSED(PTR) VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR) VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_BUCKET(PTR) \
    (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_BUCKET)
#define VARATT_IS_EXTERNAL_LOB(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_LOB)
#define VARATT_IS_EXTERNAL_ONDISK_B(PTR) \
    (VARATT_IS_EXTERNAL_ONDISK(PTR) || VARATT_IS_EXTERNAL_BUCKET(PTR))

#define VARATT_IS_SHORT(PTR) VARATT_IS_1B(PTR)
#define VARATT_IS_EXTENDED(PTR) (!VARATT_IS_4B_U(PTR))

#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len) SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len) SET_VARSIZE_4B_C(PTR, len)
#define SET_VARTAG_EXTERNAL(PTR, tag) SET_VARTAG_1B_E(PTR, tag)

#define VARSIZE_ANY(PTR) \
    (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : VARSIZE_4B(PTR)))

#define VARSIZE_ANY_EXHDR(PTR)                                             \
        (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) - VARHDRSZ_EXTERNAL : \
                               (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) - VARHDRSZ_SHORT : VARSIZE_4B(PTR) - VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
/* caution: this will return a possibly unaligned pointer */
#define VARDATA_ANY(PTR) (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

/* ----------------------------------------------------------------
 *				Section 2:	datum type + support macros
 * ----------------------------------------------------------------
 */

/*
 * Port Notes:
 *	openGauss makes the following assumptions about datatype sizes:
 *
 *	sizeof(Datum) == sizeof(void *) == 4 or 8
 *	sizeof(char) == 1
 *	sizeof(short) == 2
 *
 * When a type narrower than Datum is stored in a Datum, we place it in the
 * low-order bits and are careful that the DatumGetXXX macro for it discards
 * the unused high-order bits (as opposed to, say, assuming they are zero).
 * This is needed to support old-style user-defined functions, since depending
 * on architecture and compiler, the return value of a function returning char
 * or short may contain garbage when called as if it returned Datum.
 */

typedef uintptr_t Datum;

#define SIZEOF_DATUM SIZEOF_VOID_P

typedef Datum* DatumPtr;

#define GET_1_BYTE(datum) (((Datum)(datum)) & 0x000000ff)
#define GET_2_BYTES(datum) (((Datum)(datum)) & 0x0000ffff)
#define GET_4_BYTES(datum) (((Datum)(datum)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define GET_8_BYTES(datum) ((Datum)(datum))
#endif
#define SET_1_BYTE(value) (((Datum)(value)) & 0x000000ff)
#define SET_2_BYTES(value) (((Datum)(value)) & 0x0000ffff)
#define SET_4_BYTES(value) (((Datum)(value)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define SET_8_BYTES(value) ((Datum)(value))
#endif

/*
 * DatumGetBool
 *		Returns boolean value of a datum.
 *
 * Note: any nonzero value will be considered TRUE, but we ignore bits to
 * the left of the width of bool, per comment above.
 */

#define DatumGetBool(X) ((bool)(((bool)(X)) != 0))

/*
 * BoolGetDatum
 *		Returns datum representation for a boolean.
 *
 * Note: any nonzero value will be considered TRUE.
 */
#ifndef BoolGetDatum
#define BoolGetDatum(X) ((Datum)((X) ? 1 : 0))
#endif
/*
 * DatumGetChar
 *		Returns character value of a datum.
 */

#define DatumGetChar(X) ((char)GET_1_BYTE(X))

/*
 * CharGetDatum
 *		Returns datum representation for a character.
 */

#define CharGetDatum(X) ((Datum)SET_1_BYTE((unsigned char)(X)))

/*
 * Int8GetDatum
 *		Returns datum representation for an 8-bit integer.
 */

#define Int8GetDatum(X) ((Datum)SET_1_BYTE((uint8)X))

/*
 * DatumGetInt8
 *             Returns 8-bit integer value of a datum.
 */
#define DatumGetInt8(X) ((int8)GET_1_BYTE(X))

/*
 * DatumGetUInt8
 *		Returns 8-bit unsigned integer value of a datum.
 */

#define DatumGetUInt8(X) ((uint8)GET_1_BYTE(X))

/*
 * UInt8GetDatum
 *		Returns datum representation for an 8-bit unsigned integer.
 */

#define UInt8GetDatum(X) ((Datum)SET_1_BYTE((uint8)(X)))

/*
 * DatumGetInt16
 *		Returns 16-bit integer value of a datum.
 */

#define DatumGetInt16(X) ((int16)GET_2_BYTES(X))

/*
 * Int16GetDatum
 *		Returns datum representation for a 16-bit integer.
 */

#define Int16GetDatum(X) ((Datum)SET_2_BYTES((uint16)(X)))

/*
 * DatumGetUInt16
 *		Returns 16-bit unsigned integer value of a datum.
 */

#define DatumGetUInt16(X) ((uint16)GET_2_BYTES(X))

/*
 * UInt16GetDatum
 *		Returns datum representation for a 16-bit unsigned integer.
 */

#define UInt16GetDatum(X) ((Datum)SET_2_BYTES(X))

/*
 * DatumGetInt32
 *		Returns 32-bit integer value of a datum.
 */

#define DatumGetInt32(X) ((int32)GET_4_BYTES(X))

/*
 * Int32GetDatum
 *		Returns datum representation for a 32-bit integer.
 */

#define Int32GetDatum(X) ((Datum)SET_4_BYTES((uint32)(X)))

/*
 * DatumGetUInt32
 *		Returns 32-bit unsigned integer value of a datum.
 */

#define DatumGetUInt32(X) ((uint32)GET_4_BYTES(X))

/*
 * UInt32GetDatum
 *		Returns datum representation for a 32-bit unsigned integer.
 */

#define UInt32GetDatum(X) ((Datum)SET_4_BYTES(X))

/*
 * DatumGetObjectId
 *		Returns object identifier value of a datum.
 */

#define DatumGetObjectId(X) ((Oid)GET_4_BYTES(X))

/*
 * ObjectIdGetDatum
 *		Returns datum representation for an object identifier.
 */

#define ObjectIdGetDatum(X) ((Datum)SET_4_BYTES(X))

/*
 * DatumGetTransactionId
 *		Returns transaction identifier value of a datum.
 */

#define DatumGetTransactionId(X) (DatumGetUInt64(X))
#define DatumGetShortTransactionId(X) ((ShortTransactionId)GET_4_BYTES(X))
/*
 * TransactionIdGetDatum
 *		Returns datum representation for a transaction identifier.
 */

#define TransactionIdGetDatum(X) (UInt64GetDatum(X))
#define ShortTransactionIdGetDatum(X) ((Datum)SET_4_BYTES((X)))
/*
 * DatumGetCommandId
 *		Returns command identifier value of a datum.
 */

#define DatumGetCommandId(X) ((CommandId)GET_4_BYTES(X))

/*
 * CommandIdGetDatum
 *		Returns datum representation for a command identifier.
 */

#define CommandIdGetDatum(X) ((Datum)SET_4_BYTES(X))

/*
 * DatumGetPointer
 *		Returns pointer value of a datum.
 */

#define DatumGetPointer(X) ((Pointer)(X))

/*
 * PointerGetDatum
 *		Returns datum representation for a pointer.
 */
#ifndef PointerGetDatum
#define PointerGetDatum(X) ((Datum)(X))
#endif 
/*
 * DatumGetCString
 *		Returns C string (null-terminated string) value of a datum.
 *
 * Note: C string is not a full-fledged openGauss type at present,
 * but type input functions use this conversion for their inputs.
 */

#define DatumGetCString(X) ((char*)DatumGetPointer(X))

/*
 * CStringGetDatum
 *		Returns datum representation for a C string (null-terminated string).
 *
 * Note: C string is not a full-fledged openGauss type at present,
 * but type output functions use this conversion for their outputs.
 * Note: CString is pass-by-reference; caller must ensure the pointed-to
 * value has adequate lifetime.
 */

#define CStringGetDatum(X) PointerGetDatum(X)

/*
 * DatumGetName
 *		Returns name value of a datum.
 */

#define DatumGetName(X) ((Name)DatumGetPointer(X))

/*
 * NameGetDatum
 *		Returns datum representation for a name.
 *
 * Note: Name is pass-by-reference; caller must ensure the pointed-to
 * value has adequate lifetime.
 */

#define NameGetDatum(X) PointerGetDatum(X)

/*
 * DatumGetInt64
 *		Returns 64-bit integer value of a datum.
 *
 * Note: this macro hides whether int64 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
#define DatumGetInt64(X) ((int64)GET_8_BYTES(X))
#else
#define DatumGetInt64(X) (*((int64*)DatumGetPointer(X)))
#endif

#define BatchMaxSize 1000
/*
 * Int64GetDatum
 *		Returns datum representation for a 64-bit integer.
 *
 * Note: if int64 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
#define Int64GetDatum(X) ((Datum)SET_8_BYTES(X))
#else
extern Datum Int64GetDatum(int64 X);
#endif

/*
 * DatumGetUInt64
 *		Returns 64-bit unsigned integer value of a datum.
 *
 * Note: this macro hides whether int64 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
#define DatumGetUInt64(X) ((uint64)GET_8_BYTES(X))
#else
#define DatumGetUInt64(X) (*((uint64*)DatumGetPointer(X)))
#endif

/*
 * UInt64GetDatum
 *		Returns datum representation for a 64-bit unsigned integer.
 *
 * Note: if int64 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
#define UInt64GetDatum(X) ((Datum)SET_8_BYTES(X))
#else
#define UInt64GetDatum(X) Int64GetDatum((int64)(X))
#endif

#ifndef WIN32
/* Always pass by reference for int128 type. */
extern Datum Int128GetDatum(int128 X);
#define DatumGetInt128(X) (*((int128*)DatumGetPointer(X)))
#endif

/*
 * DatumGetFloat4
 *		Returns 4-byte floating point value of a datum.
 *
 * Note: this macro hides whether float4 is pass by value or by reference.
 */

#ifdef USE_FLOAT4_BYVAL
extern float4 DatumGetFloat4(Datum X);
#else
#define DatumGetFloat4(X) (*((float4*)DatumGetPointer(X)))
#endif

/*
 * Float4GetDatum
 *		Returns datum representation for a 4-byte floating point number.
 *
 * Note: if float4 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

extern Datum Float4GetDatum(float4 X);

/*
 * DatumGetFloat8
 *		Returns 8-byte floating point value of a datum.
 *
 * Note: this macro hides whether float8 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
extern float8 DatumGetFloat8(Datum X);
#else
#define DatumGetFloat8(X) (*((float8*)DatumGetPointer(X)))
#endif

/*
 * Float8GetDatum
 *		Returns datum representation for an 8-byte floating point number.
 *
 * Note: if float8 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

extern Datum Float8GetDatum(float8 X);

/*
 * Int64GetDatumFast
 * Float8GetDatumFast
 * Float4GetDatumFast
 *
 * These macros are intended to allow writing code that does not depend on
 * whether int64, float8, float4 are pass-by-reference types, while not
 * sacrificing performance when they are.  The argument must be a variable
 * that will exist and have the same value for as long as the Datum is needed.
 * In the pass-by-ref case, the address of the variable is taken to use as
 * the Datum.  In the pass-by-val case, these will be the same as the non-Fast
 * macros.
 */

#ifdef USE_FLOAT8_BYVAL
#define Int64GetDatumFast(X) Int64GetDatum(X)
#define Float8GetDatumFast(X) Float8GetDatum(X)
#else
#define Int64GetDatumFast(X) PointerGetDatum(&(X))
#define Float8GetDatumFast(X) PointerGetDatum(&(X))
#endif

#ifdef USE_FLOAT4_BYVAL
#define Float4GetDatumFast(X) Float4GetDatum(X)
#else
#define Float4GetDatumFast(X) PointerGetDatum(&(X))
#endif

#ifdef HAVE_INT64_TIMESTAMP
#define TimeGetDatum(X) Int64GetDatum(X)
#else
#define TimeGetDatum(X) Float8GetDatum(X)
#endif

/* ----------------------------------------------------------------
 *				Section 3:	exception handling definitions
 *							Assert, Trap, etc macros
 * ----------------------------------------------------------------
 */

#ifdef WIN32
extern THR_LOCAL bool assert_enabled;
#else
extern THR_LOCAL PGDLLIMPORT bool assert_enabled;
#endif

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Trap
 *		Generates an exception if the given condition is true.
 */
#define Trap(condition, errorType)                                                         \
    do {                                                                                   \
        if ((assert_enabled) && (condition))                                               \
            ExceptionalCondition(CppAsString(condition), (errorType), __FILE__, __LINE__); \
    } while (0)

/*
 *	TrapMacro is the same as Trap but it's intended for use in macros:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 *
 *	Isn't CPP fun?
 */
#define TrapMacro(condition, errorType)          \
    ((bool)((!assert_enabled) || !(condition) || \
            (ExceptionalCondition(CppAsString(condition), (errorType), __FILE__, __LINE__), 0)))

#ifdef Assert
#undef Assert
#endif
#ifdef PC_LINT
#define Assert(condition)       \
    do {                        \
        if (!(bool)(condition)) \
            exit(1);            \
    } while (0)
#ifndef AssertMacro
#define AssertMacro Assert
#endif
#define AssertArg Assert
#define DBG_ASSERT Assert
#define AssertState Assert

#else
#ifndef USE_ASSERT_CHECKING
#define Assert(condition)
#ifndef AssertMacro
#define AssertMacro(condition) ((void)true)
#endif /* AssertMacro */
#define AssertArg(condition)
#define DBG_ASSERT(condition)
#define AssertState(condition)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#ifndef AssertMacro
#define AssertMacro(p) ((void)assert(p))
#endif
#define AssertArg(condition) assert(condition)
#define DBG_ASSERT(condition)
#define AssertState(condition) assert(condition)
#define AssertPointerAlignment(ptr, bndr) ((void)true)
#else /* USE_ASSERT_CHECKING && !FRONTEND */

#define Assert(condition) Trap(!(condition), "FailedAssertion")

#ifndef AssertMacro
#define AssertMacro(condition) ((void)TrapMacro(!(condition), "FailedAssertion"))
#endif /* AssertMacro */

#define AssertArg(condition) Trap(!(condition), "BadArgument")

#define AssertState(condition) Trap(!(condition), "BadState")
#define DBG_ASSERT Assert
#endif /* USE_ASSERT_CHECKING */
#endif /* PC_LINT */

typedef enum {
    ENV_OK,
    ENV_ERR_LENGTH,
    ENV_ERR_NULL,
    ENV_ERR_DANGER_CHARACTER
} EnvCheckResult;

extern void ExceptionalCondition(const char *conditionName, const char *errorType, const char *fileName,
    int lineNumber);

extern void ConnFree(void* conn);

extern THR_LOCAL bool IsInitdb;

extern size_t mmap_threshold;

/* Set the pooler reload flag when signaled by SIGUSR1 */
void HandlePoolerReload(void);
void HandleMemoryContextDump(void);
void HandleExecutorFlag(void);

extern void start_xact_command(void);
extern void finish_xact_command(void);
extern void exec_init_poolhandles(void);

extern void InitVecFuncMap(void);

/* load ir file count for each process */
extern long codegenIRloadProcessCount;

extern pthread_mutex_t nodeDefCopyLock;

/* Job worker Process, execute procedure */
extern void execute_simple_query(const char* query_string);

/* check the value from environment variablethe to prevent command injection. */
extern void check_backend_env(const char* input_env_value);
extern EnvCheckResult check_backend_env_sigsafe(const char* input_env_value);
extern bool backend_env_valid(const char* input_env_value, const char* stamp);

extern void CleanSystemCaches(bool is_in_read_command);

/*Audit user logout*/
extern void audit_processlogout_unified();
extern void audit_processlogout(int code, Datum arg);

/* free the pointer malloced by cJSON_internal_malloc.*/
extern void cJSON_internal_free(void* pointer);

extern void InitThreadLocalWhenSessionExit();
extern void RemoveTempNamespace();
#ifndef ENABLE_MULTIPLE_NODES
#define CacheIsProcNameArgNsp(cc_id) ((cc_id) == PROCNAMEARGSNSP || (cc_id) == PROCALLARGS)
#else 
#define CacheIsProcNameArgNsp(cc_id) ((cc_id) == PROCNAMEARGSNSP)
#endif 
#define CacheIsProcOid(cc_id) ((cc_id) == PROCOID)
#define IsBootingPgProc(rel) IsProcRelation(rel)
#define BootUsingBuiltinFunc true

extern int errdetail_abort(void);

void log_disconnections(int code, Datum arg);
void cleanGPCPlanProcExit(int code, Datum arg);

void ResetInterruptCxt();

#define MSG_A_REPEAT_NUM_MAX 1024
#define OVERRIDE_STACK_LENGTH_MAX 1024

typedef enum {
    RUN_MODE_PRIMARY,
    RUN_MODE_STANDBY,
} ClusterRunMode;

#ifdef ENABLE_UT
#include "lib/stringinfo.h"
extern void exec_describe_statement_message(const char* stmt_name);
extern void exec_get_ddl_params(StringInfo input_message);
#endif


#endif /* POSTGRES_H */
