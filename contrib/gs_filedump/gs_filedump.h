
#ifndef GS_FILEDUMP_H
#define GS_FILEDUMP_H
/*
 * gs_filedump.h - PostgreSQL file dump utility for dumping and
 *				   formatting heap (data), index and control files.
 *
 * Copyright (c) 2002-2010 Red Hat, Inc.
 * Copyright (c) 2011-2022, PostgreSQL Global Development Group
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Original Author: Patrick Macdonald <patrickm@redhat.com>
 */

#include "postgres_fe.h"
#include "postgres.h"

#include <ctime>
#include <cctype>

#include "access/gin_private.h"
#include "access/gist.h"
#include "access/hash.h"
#include "access/nbtree.h"
#include "access/spgist_private.h"

const int MAXOUTPUTLEN = 1048576;

/* access/htup.h */
inline TransactionId HeapTupleHeaderGetXmin_tuple(HeapTupleHeader tup)
{
    return HeapTupleHeaderXminFrozen(tup) ? FrozenTransactionId : (tup)->t_choice.t_heap.t_xmin;
}

#define HeapTupleHeaderXminFrozen(tup) (((tup)->t_infomask & (HEAP_XMIN_FROZEN)) == HEAP_XMIN_FROZEN)

const int HEAP_MOVED_OFF = 0x4000; /* moved to another place by pre-9.0 \
                                    * VACUUM FULL; kept for binary      \
                                    * upgrade support */
const int HEAP_MOVED_IN = 0x8000;  /* moved from another place by pre-9.0 \
                                    * VACUUM FULL; kept for binary        \
                                    * upgrade support */
#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)

/* storage/item/itemptr.h */
#define GinItemPointerSetBlockNumber(pointer, blkno) ItemPointerSetBlockNumber((pointer), (blkno))
#define GinItemPointerSetOffsetNumber(pointer, offnum) ItemPointerSetOffsetNumber((pointer), (offnum))

/* access/gist.h */
constexpr int F_HAS_GARBAGE = (1 << 4);  // some tuples on the page are dead, but not deleted yet

/*	Options for Block formatting operations */
extern unsigned int g_blockOptions;

using BlockSwitches = enum {
    BLOCK_ABSOLUTE = 0x00000001,     /* -a: Absolute(vs Relative) addressing */
    BLOCK_BINARY = 0x00000002,       /* -b: Binary dump of block */
    BLOCK_FORMAT = 0x00000004,       /* -f: Formatted dump of blocks / control file */
    BLOCK_FORCED = 0x00000008,       /* -S: Block size forced */
    BLOCK_NO_INTR = 0x00000010,      /* -d: Dump straight blocks */
    BLOCK_RANGE = 0x00000020,        /* -R: Specific block range to dump */
    BLOCK_CHECKSUMS = 0x00000040,    /* -k: verify block checksums */
    BLOCK_DECODE = 0x00000080,       /* -D: Try to decode tuples */
    BLOCK_DECODE_TOAST = 0x00000100, /* -t: Try to decode TOAST values */
    BLOCK_IGNORE_OLD = 0x00000200,   /* -o: Decode old values */
    BLOCK_USTORE = 0x00000400        /* -u: storage Engine ustore */
};

/* Segment-related options */
extern unsigned int g_segmentOptions;

using SegmentSwitches = enum {
    SEGMENT_SIZE_FORCED = 0x00000001,   /* -s: Segment size forced */
    SEGMENT_NUMBER_FORCED = 0x00000002, /* -n: Segment number forced */
};

using ReturnTag = enum {
    MEMORY_ALL_FAILED = -2,    /* Memory allocation failed */
    ATTRIBUTE_SIZE_ERROR = -1, /* attribute size error */
    RETURN_SUCCESS = 0,
    RETURN_ERROR = 1
};

/* -R[start]:Block range start */
extern int g_blockStart;

/* -R[end]:Block range end */
extern int g_blockEnd;

/* Options for Item formatting operations */
extern unsigned int g_itemOptions;

using ItemSwitches = enum {
    ITEM_DETAIL = 0x00000001,    /* -i: Display interpreted items */
    ITEM_HEAP = 0x00000002,      /* -y: Blocks contain HeapTuple items */
    ITEM_INDEX = 0x00000004,     /* -x: Blocks contain IndexTuple items */
    ITEM_SPG_INNER = 0x00000008, /* Blocks contain SpGistInnerTuple items */
    ITEM_SPG_LEAF = 0x00000010   /* Blocks contain SpGistLeafTuple items */
};

/* Options for Control File formatting operations */
extern unsigned int g_controlOptions;

using ControlSwitches = enum {
    CONTROL_DUMP = 0x00000001,     /* -c: Dump control file */
    CONTROL_FORMAT = BLOCK_FORMAT, /* -f: Formatted dump of control file */
    CONTROL_FORCED = BLOCK_FORCED  /* -S: Block size forced */
};

/* Possible value types for the Special Section */
using SpecialSectionTypes = enum {
    SPEC_SECT_NONE,          /* No special section on block */
    SPEC_SECT_SEQUENCE,      /* Sequence info in special section */
    SPEC_SECT_INDEX_BTREE,   /* BTree index info in special section */
    SPEC_SECT_INDEX_HASH,    /* Hash index info in special section */
    SPEC_SECT_INDEX_GIST,    /* GIST index info in special section */
    SPEC_SECT_INDEX_GIN,     /* GIN index info in special section */
    SPEC_SECT_INDEX_SPGIST,  /* SP - GIST index info in special section */
    SPEC_SECT_ERROR_UNKNOWN, /* Unknown error */
    SPEC_SECT_ERROR_BOUNDARY /* Boundary error */
};

extern unsigned int g_specialType;

/* Possible return codes from option validation routine.
 * gs_filedump doesn't do much with them now but maybe in
 * the future... */
using OptionReturnCodes = enum {
    OPT_RC_VALID,     /* All options are valid */
    OPT_RC_INVALID,   /* Improper option string */
    OPT_RC_FILE,      /* File problems */
    OPT_RC_DUPLICATE, /* Duplicate option encountered */
    OPT_RC_COPYRIGHT  /* Copyright should be displayed */
};

/* Simple macro to check for duplicate options and then set
 * an option flag for later consumption */
#define SET_OPTION(_x, _y, _z)  \
    if ((_x) & (_y)) {          \
        rc = OPT_RC_DUPLICATE;  \
        duplicateSwitch = (_z); \
    } else                      \
        (_x) |= (_y);

constexpr int MAX_OPTION_LINE_LENGTH = 50;

constexpr unsigned int SEQUENCE_MAGIC = 0x1717; /* PostgreSQL defined magic number */
constexpr int EOF_ENCOUNTERED = -1;             /* Indicator for partial read */
constexpr int BYTES_PER_LINE = 16;              /* Format the binary 16 bytes per line */

constexpr uint8 VARBYTE_CONTINUATION_BIT = 0x80;  /* Continuation bit. */
constexpr uint8 VARBYTE_DATA_BITS = 0x7F;         /* The least significant 7 bits are the data bits. */
constexpr int VARBYTE_SHIFT_PER_BYTE = 7;         /* Shift per byte */
constexpr int BLOCK_ID_HIGH_SHIFT = 16;           /* Move 16 bits to get the high block ID */

/* Constants for pg_relnode.map decoding */
constexpr unsigned int RELMAPPER_MAGICSIZE = 4;
constexpr unsigned int RELMAPPER_FILESIZE = 512;
/* From utils/cache/relmapper.c -- Maybe ask community to put
 * these into utils/cache/relmapper.h? */

#define RELMAPPER_FILEMAGIC 0x592717
#define MAX_MAPPINGS 62
constexpr const char *SEGTOASTTAG = "sgtt";

extern char *g_fileName;
extern bool g_isUHeap;
extern bool g_isSegment;

extern int g_tableRelfilenode;
extern int g_toastRelfilenode;

/*
 * Function Prototypes
 */
unsigned int GetBlockSize(FILE *fp);
unsigned int GetUHeapBlockSize(FILE *fp);

int DumpFileContents(unsigned int blockOptions, unsigned int controlOptions, FILE *fp, unsigned int blockSize,
                     int blockStart, int blockEnd, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                     char *toastValue);

int DumpUHeapFileContents(unsigned int blockOptions, unsigned int controlOptions, FILE *fp, unsigned int blockSize,
                          int blockStart, int blockEnd, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                          char *toastValue);

#endif