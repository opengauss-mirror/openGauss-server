/*
 * pagehack.cpp
 *
 * A simple hack program for PostgreSQL's internal files
 * Originally written by Nicole Nie (coolnyy@gmail.com).
 *
 * contrib/pagehack/pagehack.cpp
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

#ifdef WIN32
#define FD_SETSIZE 1024 /* set before winsock2.h is included */

#endif /* ! WIN32 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.    Hence this ugly hack.
 */
#define FRONTEND 1

#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <new>

#include "access/heapam.h"
#include "access/htup.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/slru.h"
#include "access/twophase_rmgr.h"
#include "access/double_write.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uundorecord.h"
#include "access/double_write_basic.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "catalog/pg_control.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_index.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_partition.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "replication/slot.h"
#include "storage/buf/bufpage.h"
#include "storage/fsm_internals.h"
#include "storage/lock/lock.h"
#include "storage/proc.h"
#include "storage/smgr/relfilenode.h"
#include "storage/sinval.h"
#include "storage/smgr/segment.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/file/fio_device.h"
#include "replication/bcm.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/timestamp.h"
#include "cstore.h"
#include "common/build_query/build_query.h"
#include <libgen.h>
#include "tool_common.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/constant_def.h"
#endif

#include "PageCompression.h"

/* Max number of pg_class oid, currently about 4000 */
#define MAX_PG_CLASS_ID 10000
/* Number of pg_class types */
#define CLASS_TYPE_NUM 512
#define TEN 10
#define BLOCK_META_INFO_NUM_PER_PAGE 127
#define BASE_PAGE_MAP_SIZE 16
#define BASE_PAGE_MAP_BIT_SIZE (BASE_PAGE_MAP_SIZE * BITS_PER_BYTE)
#define DIVIDED_BY_TWO 2
#define WAL_ID_OFFSET 32

typedef unsigned char* binary;
static const char* indents[] = {  // 10 tab is enough to used.
    "",
    "\t",
    "\t\t",
    "\t\t\t",
    "\t\t\t\t",
    "\t\t\t\t\t",
    "\t\t\t\t\t\t",
    "\t\t\t\t\t\t\t",
    "\t\t\t\t\t\t\t\t",
    "\t\t\t\t\t\t\t\t\t",
    "\t\t\t\t\t\t\t\t\t\t"};
static const int nIndents = sizeof(indents) / sizeof(indents[0]);
static int indentLevel = 0;
static uint64 g_tdCount = 0;
static uint64 g_rpCount = 0;
static uint64 g_tdMax = 0;
static uint64 g_rpMax = 0;
static uint64 g_pageCount = 0;
static uint64 g_freeSpace = 0;
static uint64 g_freeMax = 0;

static HeapTupleData dummyTuple;

// add those special tables to parse, so we can read the tuple data
//
typedef void (*ParseHeapTupleData)(binary tup, int len, binary nullBitmap, int natrrs);

static const char* PgBtreeIndexRelName[] = {"toast_index"};

static const char* PgHeapRelName[] = {"pg_class",
    "pg_index",
    "pg_partition",
    "pg_cudesc_xx",  // all the relations about pg_cudesc_xx kind, not the real pg_cudesc table
    "ts_cudesc_xx",
    "pg_database",
    "pg_tablespace",
    "pg_attribute",
    "pg_am",
    "pg_statistic",
    "pg_toast"};

typedef enum SegmentType {
    SEG_HEAP,
    SEG_FSM,
    SEG_UHEAP,
    SEG_INDEX_BTREE,
    SEG_UNDO,
    SEG_UNKNOWN
} SegmentType;

static void ParsePgClassTupleData(binary tupdata, int len, binary nullBitmap, int natrrs);
static void ParsePgIndexTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgPartitionTupleData(binary tupdata, int len, binary nullBitmap, int natrrs);
static void ParsePgCudescXXTupleData(binary tupdata, int len, binary nullBitmap, int natrrs);
static void ParseTsCudescXXTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgDatabaseTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgTablespaceTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgAttributeTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgAmTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParsePgstatisticTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static void ParseToastTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);

/* UStore */
static void ParseTDSlot(const char *page);

static void ParseToastIndexTupleData(binary tupdata, int len, binary nullBitmap, int nattrs);
static int parse_uncompressed_page_file(const char *filename, SegmentType type, const uint32 start_point,
    const uint32 number_read);

static ParseHeapTupleData PgHeapRelTupleParser[] = {
    ParsePgClassTupleData,       // pg_class
    ParsePgIndexTupleData,       // pg_index
    ParsePgPartitionTupleData,   // pg_partition
    ParsePgCudescXXTupleData,    // pg_cudesc_xx
    ParseTsCudescXXTupleData,    // ts_cudesc_xx
    ParsePgDatabaseTupleData,    // pg_database
    ParsePgTablespaceTupleData,  // pg_tablespace
    ParsePgAttributeTupleData,   // pg_attribute
    ParsePgAmTupleData           // pg_am
    ,
    ParsePgstatisticTupleData  // pg_statistic
    ,
    ParseToastTupleData  // pg_toast
};

static ParseHeapTupleData PgIndexRelTupleParser[] = {ParseToastIndexTupleData};

static int PgHeapRelTupleParserCursor = -1;
static int PgIndexRelTupleParserCursor = -1;


/* For Assert(...) macros. */
#ifdef USE_ASSERT_CHECKING
THR_LOCAL bool assert_enabled = true;
#else
THR_LOCAL bool assert_enabled = false;
#endif

/* Options */
bool only_vm = false;
bool only_bcm = false;
bool write_back = false;
bool dirty_page = false;
bool enable_dss = false;
int start_item = 1;
int num_item = 0;
int SegNo = 0;

bool vm_cache[BLCKSZ] = {false};

/*
 * MaxLevelNum can not exceed 0x0F
 */
#define MaxLevelNumInternal 5

static const int freespaceThreshold[MaxLevelNumInternal] = {
    0,                          /* used 96% ~ 100% */
    MaxHeapTupleSize * 4 / 100, /* used 81% ~ 95% */
    MaxHeapTupleSize * 1 / 5,   /* used 51% ~ 80% */
    MaxHeapTupleSize / 2,       /* used 1%  ~ 50% */
    MaxHeapTupleSize            /* not used */
};

/* table for fast counting of bcm bits */
static const uint8 number_of_bcm_bits[256] = {0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    3,
    3,
    4,
    4,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    3,
    3,
    4,
    4,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    3,
    3,
    4,
    4,
    2,
    2,
    3,
    3,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    3,
    3,
    4,
    4};

/* table for fast counting of meta bits */
static const uint8 number_of_meta_bits[256] = {0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    0,
    1,
    1,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2,
    1,
    1,
    2,
    2};

/*
 * SpaceGetBlockFreeLevel
 *        Returns the block free level according to freespace.
 */
#define BlockFreeLevelGetSpaceInternal(level) (freespaceThreshold[(level)])

void ExceptionalCondition(const char* conditionName, const char* errorType, const char* fileName, int lineNumber)
{
    fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n", errorType, conditionName, fileName, lineNumber);
    abort();
}

#define RELCACHE_INIT_FILEMAGIC 0x573266 /* version ID value */

typedef struct PgClass_table {
    const char* class_name;
    Oid ralation_id;
} PgClass_table;

typedef struct TwoPhaseRecordOnDisk {
    uint32 len;          /* length of rmgr data */
    TwoPhaseRmgrId rmid; /* resource manager for this record */
    uint16 info;         /* flag bits for use by rmgr */
} TwoPhaseRecordOnDisk;

typedef struct TwoPhaseFileHeader {
    uint32 magic;            /* format identifier */
    uint32 total_len;        /* actual file length */
    TransactionId xid;       /* original transaction XID */
    Oid database;            /* OID of database it was in */
    TimestampTz prepared_at; /* time of preparation */
    Oid owner;               /* user running the transaction */
    int32 nsubxacts;         /* number of following subxact XIDs */
    int32 ncommitrels;       /* number of delete-on-commit rels */
    int32 nabortrels;        /* number of delete-on-abort rels */
    int32 ninvalmsgs;        /* number of cache invalidation messages */
    bool initfileinval;      /* does relcache init file need invalidation? */
    char gid[200];           /* GID for transaction */
} TwoPhaseFileHeader;

#ifndef WIN32
#include <sys/time.h>
#include <unistd.h>
#endif /* ! WIN32 */

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h> /* for getrlimit */
#endif

#ifndef INT64_MAX
#define INT64_MAX INT64CONST(0x7FFFFFFFFFFFFFFF)
#endif

typedef enum HackingType {
    HACKING_HEAP,  /* heap page */
    HACKING_FSM,   /* fsm page */
    HACKING_UHEAP, /* uheap page */
    HACKING_INDEX, /* index page */
    HACKING_UNDO,  /* undo page */
    HACKING_FILENODE,
    HACKING_INTERNAL_INIT, /* pg_internal.init */
    HACKING_TWOPHASE,      /* two phase file */
    HACKING_CU,            /* cu page */
    HACKING_SLOT,
    HACKING_CONTROL,
    HACKING_CLOG,
    HACKING_CSNLOG,
    HACKING_STATE,
    HACKING_DW,
    HACKING_DW_SINGLE,
    HACKING_UNDO_ZONE_META,
    HACKING_UNDO_SPACE_META,
    HACKING_UNDO_SLOT_SPACE_META,
    HACKING_UNDO_SLOT,
    HACKING_UNDO_RECORD,
    HACKING_UNDO_FIX,
    HACKING_SEGMENT,
    NUM_HACKINGTYPE,
    HACKING_LSN_INFO_META,
    HACKING_BLOCK_INFO_META,
} HackingType;

static HackingType hackingtype = HACKING_HEAP;
static const char* HACKINGTYPE[] = {"heap",
    "fsm",
    "uheap",
    "btree_index",
    "undo",
    "filenode_map",
    "pg_internal_init",
    "twophase",
    "cu",
    "slot",
    "pg_control",
    "clog",
    "csnlog",
    "gaussdb_state",
    "double_write",
    "dw_single_flush_file",
    "undo_zone",
    "undo_space",
    "undo_slot_space",
    "undo_slot",
    "undo_record",
    "undo_fix",
    "segment",
    "lsn_info_meta",
    "block_info_meta"
};

const char* PageTypeNames[] = {"DATA", "FSM", "VM"};

#define GETHEAPSTRUCT(TUP) ((unsigned char*)(TUP) + (TUP)->t_hoff)

#define GETINDEXSTRUCT(ITUP) ((unsigned char*)(ITUP) + IndexInfoFindDataOffset((ITUP)->t_info))

extern char* optarg;
extern int optind;

char* pgdata = NULL;

static void fill_filenode_map(char** class_map);

static const char HexCharMaps[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

#define Byte2HexChar(_buff, _byte)              \
    do {                                        \
        (_buff)[0] = HexCharMaps[(_byte) >> 4]; \
        (_buff)[1] = HexCharMaps[(_byte)&0x0F]; \
    } while (0)

/* PG */
#define CLOG_BITS_PER_XACT 2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK ((1 << CLOG_BITS_PER_XACT) - 1)
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToPage(xid) ((xid) / (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid) (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_BYTE)

/*
 * The internal FSM routines work on a logical addressing scheme. Each
 * level of the tree can be thought of as a separately addressable file.
 */
typedef struct FSMAddress {
    uint level;     /* level */
    uint logpageno; /* page number within the level */
} FSMAddress;
/*
 * Depth of the on-disk tree. We need to be able to address 2^32-1 blocks,
 * and 1626 is the smallest number that satisfies X^3 >= 2^32-1. Likewise,
 * 216 is the smallest number that satisfies X^4 >= 2^32-1. In practice,
 * this means that 4096 bytes is the smallest BLCKSZ that we can get away
 * with a 3-level tree, and 512 is the smallest we support.
 */
#define FSM_TREE_DEPTH ((SlotsPerFSMPage >= 1626) ? 3 : 4)
#define FSM_ROOT_LEVEL (FSM_TREE_DEPTH - 1)
const int FSM_BOTTOM_LEVEL = 0;

using namespace undo;
using namespace extreme_rto_standby_read;

static void formatBytes(unsigned char* start, int len)
{
#ifdef DEBUG
    int cnt;
    char byteBuf[4] = {0, 0, ' ', '\0'};
    unsigned char ch;
    int bytesEachLine = 32;

    // set the indent-level first
    ++indentLevel;

    for (cnt = 0; cnt < len; cnt++) {
        ch = (unsigned char)*start;
        start++;

        // print 32bytes each line
        if (bytesEachLine == 32) {
            fprintf(stdout, "\n%s", indents[indentLevel]);
            bytesEachLine = 0;
        } else if (bytesEachLine % 8 == 0) {
            fprintf(stdout, " ");
        }
        ++bytesEachLine;

        Byte2HexChar(byteBuf, ch);
        fprintf(stdout, "%s", byteBuf);
    }

    --indentLevel;
#endif
}

static void formatBitmap(const unsigned char* start, int len, char bit1, char bit0)
{
    int bytesOfOneLine = 8;

    ++indentLevel;

    for (int i = 0; i < len; ++i) {
        unsigned char ch = start[i];
        unsigned char bitmask = 1;

        // print 8bytes each line
        if (bytesOfOneLine == 8) {
            fprintf(stdout, "\n%s", indents[indentLevel]);
            bytesOfOneLine = 0;
        }
        ++bytesOfOneLine;

        // print 8 bits within a loop
        do {
            fprintf(stdout, "%c", ((ch & bitmask) ? bit1 : bit0));
            bitmask <<= 1;
        } while (bitmask != 0);
        fprintf(stdout, " ");
    }

    --indentLevel;
}

static void usage(const char* progname)
{
    printf("%s is a hacking tool for PostgreSQL.\n"
           "\n"
           "Usage:\n"
           "  %s [OPTIONS]\n"
           "\nHacking options:\n"
           "  -f FILENAME  the database file to hack\n",
        progname,
        progname);

    // print supported type within HACKINGTYPE[]
    //
    printf("  -t {");
    int nTypes = sizeof(HACKINGTYPE) / sizeof(HACKINGTYPE[0]);
    for (int i = 0; i < nTypes; ++i) {
        printf(" %s%s", HACKINGTYPE[i], (i == nTypes - 1) ? " " : "|");
    }
    printf("}\n"
           "       the hacking type (default: heap)\n");

    // print supported table name for HEAP type
    //
    printf("  -r {");
    int nrel = (int)sizeof(PgHeapRelName) / sizeof(PgHeapRelName[0]);
    for (int i = 0; i < nrel; ++i) {
        printf(" %s%s", PgHeapRelName[i], (i == nrel - 1) ? " " : "|");
    }
    printf("} given relation name when -t is heap \n");

    // print supported table name for INDEX type
    //
    printf("  -i {");
    nrel = (int)sizeof(PgBtreeIndexRelName) / sizeof(PgBtreeIndexRelName[0]);
    for (int i = 0; i < nrel; ++i) {
        printf(" %s%s", PgBtreeIndexRelName[i], (i == nrel - 1) ? " " : "|");
    }
    printf("} given relation name when -t is btree_index \n");

    printf("  -v only show visibility map info, can only work when parsing index or heap\n"
           "  -b only show BCM map info, can only work when parsing index or heap\n"
           "  -u double write hacking\n"
           "  -s set the start point to hack when hacking heap/index/undo\n"
           "  -n set the number of blocks to hack\n"
           "  -o set the CU pointer from cudesc\n"
           "  -I set the start item slot need change in one page\n"
           "  -N set the number slots need change in one page \n"
           "  -w write the change to file\n"
           "  -d only for test, use 0xFF to fill the last half page[4k]\n"
           "  -z only for undo space/group meta, dump the specified space/group\n"
           "  -S heap file segment number\n"
           "\nDss options:\n"
           "  -D enable shared storage mode\n"
           "  -c SOCKETPATH  dss connect socket file path\n"
           "\nCommon options:\n"
           "  --help, -h       show this help, then exit\n"
           "  --version, -V    output version information, then exit\n");
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf("\nReport bugs to GaussDB support.\n");
#else
    printf("\nReport bugs to openGauss community by raising an issue.\n");
#endif
}

static bool HexStringToInt(char* hex_string, int* result)
{
    int num = 0;
    char* temp = hex_string;
    int onechar = 0;
    int index = 0;

    if (NULL == hex_string)
        return false;

    while (*temp++ != '\0')
        num++;

    while (num--) {
        if (hex_string[num] >= 'A' && hex_string[num] <= 'F')
            onechar = hex_string[num] - 55;
        else if (hex_string[num] >= '0' && hex_string[num] <= '9')
            onechar = hex_string[num] - 48;
        else
            return false;

        *result += onechar << (index * 4);
        index++;
    }

    return true;
}

static void ParsePgCudescXXTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != CUDescCUExtraAttr) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", CUDescCUExtraAttr, nattrs);
        exit(1);
    }

    int datlen = 0;
    char* dat = NULL;
    char* nextAttr = (char*)tupdata;

    bool isnulls[nattrs];
    errno_t rc = memset_s(isnulls, nattrs, false, nattrs);
    securec_check_c(rc, "\0", "\0");
    if (NULL != nullBitmap) {
        datlen = (nattrs + 7) / 8;
        int j = 0;
        for (int i = 0; i < datlen; ++i) {
            unsigned char ch = nullBitmap[i];
            unsigned char bitmask = 1;
            do {
                isnulls[j] = (ch & bitmask) ? false : true;
                bitmask <<= 1;
                ++j;
            } while (bitmask != 0 && j < nattrs);
        }
    }

    indentLevel = 4;

    if (!isnulls[0]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout,
            "\n%s"
            "ColId: %d",
            indents[indentLevel],
            *(int32*)nextAttr);
        nextAttr += sizeof(int32);
    }

    if (!isnulls[1]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout,
            "\n%s"
            "CUId: %ud",
            indents[indentLevel],
            *(uint32*)nextAttr);
        nextAttr += sizeof(uint32);
    }

    if (!isnulls[2]) {
        // rough check MIN/MAX
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        Assert(datlen >= 0 && datlen < 32);
        dat = VARDATA_ANY(nextAttr);
        Assert(dat == nextAttr + 1 || dat == nextAttr + 4);
        fprintf(stdout,
            "\n%s"
            "MIN size: %d",
            indents[indentLevel],
            datlen);
#ifdef DEBUG
        fprintf(stdout,
            "\n%s"
            "data are:",
            indents[indentLevel]);
        formatBytes((unsigned char*)dat, datlen);
#endif
        nextAttr += datlen + int(dat - nextAttr);
    }

    if (!isnulls[3]) {
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        Assert(datlen >= 0 && datlen < 32);
        dat = VARDATA_ANY(nextAttr);
        Assert(dat == nextAttr + 1 || dat == nextAttr + 4);
        fprintf(stdout,
            "\n%s"
            "MAX size: %d",
            indents[indentLevel],
            datlen);
#ifdef DEBUG
        fprintf(stdout,
            "\n%s"
            "data are:",
            indents[indentLevel]);
        formatBytes((unsigned char*)dat, datlen);
#endif
        nextAttr += datlen + int(dat - nextAttr);
    }

    if (!isnulls[4]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout,
            "\n%s"
            "rows: %d",
            indents[indentLevel],
            *(int32*)nextAttr);
        nextAttr += sizeof(int32);
    }

    if (!isnulls[5]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout,
            "\n%s"
            "mode: %x",
            indents[indentLevel],
            *(uint32*)nextAttr);
        nextAttr += sizeof(uint32);
    }

    if (!isnulls[6]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'd');
        fprintf(stdout,
            "\n%s"
            "cu size: %d",
            indents[indentLevel],
            *(int32*)nextAttr);
        nextAttr += sizeof(int64);
    }

    if (!isnulls[7]) {
        // CU Pointer
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        dat = VARDATA_ANY(nextAttr);
        if (datlen >= 0 && datlen <= (int)sizeof(uint64)) {
            fprintf(stdout,
                "\n%s"
                "cu pointer: %lu, sizeof: %d",
                indents[indentLevel],
                *(uint64*)dat,
                datlen);
        } else {
            fprintf(stdout,
                "\n%s"
                "cu pointer bitmap: %d",
                indents[indentLevel],
                datlen);
#ifdef DEBUG
            fprintf(stdout,
                "\n%s"
                "data are:",
                indents[indentLevel]);
            formatBytes((unsigned char*)dat, datlen);
#endif
        }
        nextAttr += datlen + int(dat - nextAttr);
    }

    if (!isnulls[8]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout,
            "\n%s"
            "cu magic: %u",
            indents[indentLevel],
            *(uint32*)nextAttr);
        nextAttr += sizeof(uint32);
    }

    Assert(len >= int(nextAttr - (char*)tupdata));
}

static void ParseTsCudescXXTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (nattrs != TsCudesc::MAX_ATT_NUM) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", TsCudesc::MAX_ATT_NUM, nattrs);
        exit(1);
    }
#endif

    int datlen = 0;
    int j = 0;
    int i = 0;
    char* dat = NULL;
    char* nextAttr = (char*)tupdata;
    unsigned char ch = 0;
    unsigned char bitmask = 0;
    bool isnulls[nattrs] = {0};

    if (NULL != nullBitmap) {
        datlen = (nattrs + 7) / 8;
        j = 0;
        for (i = 0; i < datlen; ++i) {
            ch = nullBitmap[i];
            bitmask = 1;
            do {
                isnulls[j] = (ch & bitmask) ? false : true;
                bitmask <<= 1;
                ++j;
            } while (bitmask != 0 && j < nattrs);
        }
    }

    indentLevel = 4;
    if (!isnulls[0]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "ColId: %d", indents[indentLevel], *(int32*)nextAttr);
        nextAttr += sizeof(int32);
    }

    if (!isnulls[1]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "TagId: %d", indents[indentLevel], *(int32*)nextAttr);
        nextAttr += sizeof(int32);
    }

    if (!isnulls[2]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "CUId: %u", indents[indentLevel], *(uint32*)nextAttr);
        nextAttr += sizeof(uint32);
    }

    if (!isnulls[3]) {
        // rough check MIN/MAX
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        Assert(datlen < 32 && datlen >= 0);
        dat = VARDATA_ANY(nextAttr);
        Assert(dat == nextAttr + 4 || dat == nextAttr + 1);
        fprintf(stdout, "\n%s" "MIN size: %d", indents[indentLevel], datlen);
#ifdef DEBUG
        fprintf(stdout, "\n%s" "data are:", indents[indentLevel]);
        formatBytes((unsigned char*)dat, datlen);
#endif
        nextAttr += datlen + int(dat - nextAttr);
    }

    if (!isnulls[4]) {
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        Assert(datlen < 32 && datlen >= 0);
        dat = VARDATA_ANY(nextAttr);
        Assert(dat == nextAttr + 4 || dat == nextAttr + 1);
        fprintf(stdout, "\n%s" "MAX size: %d", indents[indentLevel], datlen);
#ifdef DEBUG
        fprintf(stdout, "\n%s" "data are:", indents[indentLevel]);
        formatBytes((unsigned char*)dat, datlen);
#endif
        nextAttr = nextAttr + datlen + int(dat - nextAttr);
    }

    if (!isnulls[5]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "rows: %d", indents[indentLevel], *(int32*)nextAttr);
        nextAttr = nextAttr + sizeof(int32);
    }

    if (!isnulls[6]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "mode: %x", indents[indentLevel], *(uint32*)nextAttr);
        nextAttr = nextAttr + sizeof(int32);
    }

    if (!isnulls[7]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'd');
        fprintf(stdout, "\n%s" "cu size: %d", indents[indentLevel], *(int32*)nextAttr);
        nextAttr = nextAttr + sizeof(int64);
    }

    if (!isnulls[8]) {
        // CU Pointer
        nextAttr = (char*)att_align_pointer((long int)nextAttr, 'i', -1, nextAttr);
        datlen = VARSIZE_ANY_EXHDR(nextAttr);
        dat = VARDATA_ANY(nextAttr);
        if (datlen >= 0 && datlen <= (int)sizeof(uint64)) {
            fprintf(stdout, "\n%s" "cu pointer: %lu, sizeof: %d",
                indents[indentLevel], *(uint64*)dat, datlen);
        } else {
            fprintf(stdout,"\n%s" "cu pointer bitmap: %d",
                indents[indentLevel], datlen);
#ifdef DEBUG
            fprintf(stdout, "\n%s" "data are:", indents[indentLevel]);
            formatBytes((unsigned char*)dat, datlen);
#endif
        }
        nextAttr = nextAttr + datlen + int(dat - nextAttr);
    }

    if (!isnulls[9]) {
        nextAttr = (char*)att_align_nominal((long int)nextAttr, 'i');
        fprintf(stdout, "\n%s" "cu magic: %u", indents[indentLevel], *(uint32*)nextAttr);
        nextAttr = nextAttr + sizeof(uint32);
    }
    Assert(len >= int(nextAttr - (char*)tupdata));
}

// parse and print tuple data in PG_PARTITION relation
//
static void ParsePgPartitionTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_partition) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_partition, nattrs);
        exit(1);
    }

    Form_pg_partition pgPartitionTupData = (Form_pg_partition)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "name: %s",
        indents[indentLevel],
        (pgPartitionTupData->relname.data));
    fprintf(stdout,
        "\n%s"
        "part type: %c",
        indents[indentLevel],
        pgPartitionTupData->parttype);
    fprintf(stdout,
        "\n%s"
        "parent id: %u",
        indents[indentLevel],
        pgPartitionTupData->parentid);
    fprintf(stdout,
        "\n%s"
        "range num: %d",
        indents[indentLevel],
        pgPartitionTupData->rangenum);
    fprintf(stdout,
        "\n%s"
        "intervalnum: %d",
        indents[indentLevel],
        pgPartitionTupData->intervalnum);

    fprintf(stdout,
        "\n%s"
        "partstrategy: %c",
        indents[indentLevel],
        pgPartitionTupData->partstrategy);
    fprintf(stdout,
        "\n%s"
        "relfilenode: %u",
        indents[indentLevel],
        pgPartitionTupData->relfilenode);
    fprintf(stdout,
        "\n%s"
        "reltablespace: %u",
        indents[indentLevel],
        pgPartitionTupData->reltablespace);
    fprintf(stdout,
        "\n%s"
        "relpages: %f",
        indents[indentLevel],
        pgPartitionTupData->relpages);
    fprintf(stdout,
        "\n%s"
        "reltuples: %f",
        indents[indentLevel],
        pgPartitionTupData->reltuples);

    fprintf(stdout,
        "\n%s"
        "relallvisible: %d",
        indents[indentLevel],
        pgPartitionTupData->relallvisible);
    fprintf(stdout,
        "\n%s"
        "reltoastrelid: %u",
        indents[indentLevel],
        pgPartitionTupData->reltoastrelid);
    fprintf(stdout,
        "\n%s"
        "reltoastidxid: %u",
        indents[indentLevel],
        pgPartitionTupData->reltoastidxid);
    fprintf(stdout,
        "\n%s"
        "indextblid: %u",
        indents[indentLevel],
        pgPartitionTupData->indextblid);
    fprintf(stdout,
        "\n%s"
        "indisusable: %d",
        indents[indentLevel],
        pgPartitionTupData->indisusable);

    fprintf(stdout,
        "\n%s"
        "reldeltarelid: %u",
        indents[indentLevel],
        pgPartitionTupData->reldeltarelid);
    fprintf(stdout,
        "\n%s"
        "reldeltaidx: %u",
        indents[indentLevel],
        pgPartitionTupData->reldeltaidx);
    fprintf(stdout,
        "\n%s"
        "relcudescrelid: %u",
        indents[indentLevel],
        pgPartitionTupData->relcudescrelid);
    fprintf(stdout,
        "\n%s"
        "relcudescidx: %u",
        indents[indentLevel],
        pgPartitionTupData->relcudescidx);
    fprintf(stdout,
        "\n%s"
        "relfrozenxid: %u",
        indents[indentLevel],
        pgPartitionTupData->relfrozenxid);

    fprintf(stdout,
        "\n%s"
        "intspnum: %d",
        indents[indentLevel],
        pgPartitionTupData->intspnum);

#ifdef DEBUG
    int remain = len - PARTITION_TUPLE_SIZE;
    if (remain > 0) {
        if (remain - sizeof(TransactionId) > 0) {
            fprintf(stdout, "\n%sthe others:", indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain - sizeof(TransactionId));
            fprintf(stdout, "\n%srelfrozenxid64: ", indents[indentLevel]);
            formatBytes(tupdata + (len - remain + sizeof(TransactionId)), sizeof(TransactionId));
        } else if (remain - sizeof(TransactionId) == 0) {
            fprintf(stdout, "\n%srelfrozenxid64: ", indents[indentLevel]);
            formatBytes(tupdata + (len - remain), sizeof(TransactionId));
        } else {
            fprintf(stdout, "\n%sthe others:", indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain);
        }
    }
#endif
    fprintf(stdout, "\n");
}

static void ParseToastIndexTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    int* values = (int*)tupdata;
    indentLevel = 3;

    if (len > 0) {
        fprintf(stdout,
            "\n%s"
            "unique_id: %d",
            indents[indentLevel],
            *values++);
        fprintf(stdout,
            "\n%s"
            "chunk_seq: %d",
            indents[indentLevel],
            *values++);
    }
}

// parse and print tuple data in PG_TOAST relation
//
static void ParseToastTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    int* values = (int*)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "unique_id: %d",
        indents[indentLevel],
        *values++);
    fprintf(stdout,
        "\n%s"
        "chunk_seq: %d",
        indents[indentLevel],
        *values++);
}

// parse and print tuple data in PG_STATISTIC relation
//
static void ParsePgstatisticTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_statistic) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_statistic, nattrs);
        exit(1);
    }

    Form_pg_statistic pgStatTupData = (Form_pg_statistic)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "starelid: %u",
        indents[indentLevel],
        (pgStatTupData->starelid));
    fprintf(stdout,
        "\n%s"
        "starelkind: %c",
        indents[indentLevel],
        (pgStatTupData->starelkind));
    fprintf(stdout,
        "\n%s"
        "staattnum: %d",
        indents[indentLevel],
        (pgStatTupData->staattnum));
    fprintf(stdout,
        "\n%s"
        "stainherit: %d",
        indents[indentLevel],
        (pgStatTupData->stainherit));
    fprintf(stdout,
        "\n%s"
        "stanullfrac: %f",
        indents[indentLevel],
        (pgStatTupData->stanullfrac));
    fprintf(stdout,
        "\n%s"
        "stawidth: %d",
        indents[indentLevel],
        (pgStatTupData->stawidth));
    fprintf(stdout,
        "\n%s"
        "stadistinct: %f",
        indents[indentLevel],
        (pgStatTupData->stadistinct));
    fprintf(stdout,
        "\n%s"
        "stakind1: %d",
        indents[indentLevel],
        (pgStatTupData->stakind1));
    fprintf(stdout,
        "\n%s"
        "stakind2: %d",
        indents[indentLevel],
        (pgStatTupData->stakind2));
    fprintf(stdout,
        "\n%s"
        "stakind3: %d",
        indents[indentLevel],
        (pgStatTupData->stakind3));
    fprintf(stdout,
        "\n%s"
        "stakind4: %d",
        indents[indentLevel],
        (pgStatTupData->stakind4));
    fprintf(stdout,
        "\n%s"
        "stakind5: %d",
        indents[indentLevel],
        (pgStatTupData->stakind5));
    fprintf(stdout,
        "\n%s"
        "staop1: %u",
        indents[indentLevel],
        (pgStatTupData->staop1));
    fprintf(stdout,
        "\n%s"
        "staop2: %u",
        indents[indentLevel],
        (pgStatTupData->staop2));
    fprintf(stdout,
        "\n%s"
        "staop3: %u",
        indents[indentLevel],
        (pgStatTupData->staop3));
    fprintf(stdout,
        "\n%s"
        "staop4: %u",
        indents[indentLevel],
        (pgStatTupData->staop4));
    fprintf(stdout,
        "\n%s"
        "staop5: %u",
        indents[indentLevel],
        (pgStatTupData->staop5));
    fprintf(stdout, "\n");
}

// parse and print tuple data in PG_CLASS relation
//
static void ParsePgClassTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_class) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_class, nattrs);
        exit(1);
    }

    Form_pg_class pgClassTupData = (Form_pg_class)tupdata;
    indentLevel = 3;

#ifdef DEBUG
    fprintf(stdout,
        "\n%s"
        "name: %s",
        indents[indentLevel],
        (pgClassTupData->relname.data));
#endif
    fprintf(stdout,
        "\n%s"
        "relnamespace: %u",
        indents[indentLevel],
        pgClassTupData->relnamespace);
    fprintf(stdout,
        "\n%s"
        "reltype: %u",
        indents[indentLevel],
        pgClassTupData->reltype);
    fprintf(stdout,
        "\n%s"
        "reloftype: %u",
        indents[indentLevel],
        pgClassTupData->reloftype);
    fprintf(stdout,
        "\n%s"
        "relowner: %u",
        indents[indentLevel],
        pgClassTupData->relowner);

    fprintf(stdout,
        "\n%s"
        "relam: %u",
        indents[indentLevel],
        pgClassTupData->relam);
    fprintf(stdout,
        "\n%s"
        "relfilenode: %u",
        indents[indentLevel],
        pgClassTupData->relfilenode);
    fprintf(stdout,
        "\n%s"
        "reltablespace: %u",
        indents[indentLevel],
        pgClassTupData->reltablespace);
    fprintf(stdout,
        "\n%s"
        "relpages: %f",
        indents[indentLevel],
        pgClassTupData->relpages);
    fprintf(stdout,
        "\n%s"
        "reltuples: %f",
        indents[indentLevel],
        pgClassTupData->reltuples);

    fprintf(stdout,
        "\n%s"
        "relallvisible: %d",
        indents[indentLevel],
        pgClassTupData->relallvisible);
    fprintf(stdout,
        "\n%s"
        "reltoastrelid: %u",
        indents[indentLevel],
        pgClassTupData->reltoastrelid);
    fprintf(stdout,
        "\n%s"
        "reltoastidxid: %u",
        indents[indentLevel],
        pgClassTupData->reltoastidxid);
    fprintf(stdout,
        "\n%s"
        "reldeltarelid: %u",
        indents[indentLevel],
        pgClassTupData->reldeltarelid);
    fprintf(stdout,
        "\n%s"
        "reldeltaidx: %u",
        indents[indentLevel],
        pgClassTupData->reldeltaidx);

    fprintf(stdout,
        "\n%s"
        "relcudescrelid: %u",
        indents[indentLevel],
        pgClassTupData->relcudescrelid);
    fprintf(stdout,
        "\n%s"
        "relcudescidx: %u",
        indents[indentLevel],
        pgClassTupData->relcudescidx);
    fprintf(stdout,
        "\n%s"
        "relhasindex: %u",
        indents[indentLevel],
        pgClassTupData->relhasindex);
    fprintf(stdout,
        "\n%s"
        "relisshared: %d",
        indents[indentLevel],
        pgClassTupData->relisshared);
    fprintf(stdout,
        "\n%s"
        "relpersistence: %c",
        indents[indentLevel],
        pgClassTupData->relpersistence);

    fprintf(stdout,
        "\n%s"
        "relkind: %c",
        indents[indentLevel],
        pgClassTupData->relkind);
    fprintf(stdout,
        "\n%s"
        "relnatts: %d",
        indents[indentLevel],
        pgClassTupData->relnatts);
    fprintf(stdout,
        "\n%s"
        "relchecks: %d",
        indents[indentLevel],
        pgClassTupData->relchecks);
    fprintf(stdout,
        "\n%s"
        "relhasoids: %d",
        indents[indentLevel],
        pgClassTupData->relhasoids);
    fprintf(stdout,
        "\n%s"
        "relhaspkey: %d",
        indents[indentLevel],
        pgClassTupData->relhaspkey);

    fprintf(stdout,
        "\n%s"
        "relhasrules: %d",
        indents[indentLevel],
        pgClassTupData->relhasrules);
    fprintf(stdout,
        "\n%s"
        "relhastriggers: %d",
        indents[indentLevel],
        pgClassTupData->relhastriggers);
    fprintf(stdout,
        "\n%s"
        "relhassubclass: %d",
        indents[indentLevel],
        pgClassTupData->relhassubclass);
    fprintf(stdout,
        "\n%s"
        "relcmprs: %d",
        indents[indentLevel],
        pgClassTupData->relcmprs);
    fprintf(stdout,
        "\n%s"
        "relhasclusterkey: %d",
        indents[indentLevel],
        pgClassTupData->relhasclusterkey);

    fprintf(stdout,
        "\n%s"
        "relrowmovement: %d",
        indents[indentLevel],
        pgClassTupData->relrowmovement);
    fprintf(stdout,
        "\n%s"
        "parttype: %c",
        indents[indentLevel],
        pgClassTupData->parttype);
    fprintf(stdout,
        "\n%s"
        "relfrozenxid: %u",
        indents[indentLevel],
        pgClassTupData->relfrozenxid);

#ifdef DEBUG
    int remain = len - CLASS_TUPLE_SIZE;
    if (remain > 0) {
        if (remain - sizeof(TransactionId) > 0) {
            fprintf(stdout,
                "\n%s"
                "the others:",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain - sizeof(TransactionId));
            fprintf(stdout,
                "\n%s"
                "relfrozenxid64: ",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain + sizeof(TransactionId)), sizeof(TransactionId));
        } else if (remain - sizeof(TransactionId) == 0) {
            fprintf(stdout,
                "\n%s"
                "relfrozenxid64: ",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), sizeof(TransactionId));
        } else {
            fprintf(stdout,
                "\n%s"
                "the others:",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain);
        }
    }
#endif
    fprintf(stdout, "\n");
}

static void ParsePgAmTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_am) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_am, nattrs);
        exit(1);
    }

    Form_pg_am pgAmTupData = (Form_pg_am)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "amname: %s",
        indents[indentLevel],
        (pgAmTupData->amname.data));
    fprintf(stdout,
        "\n%s"
        "amstrategies: %d",
        indents[indentLevel],
        pgAmTupData->amstrategies);
    fprintf(stdout,
        "\n%s"
        "amsupport: %d",
        indents[indentLevel],
        pgAmTupData->amsupport);
    fprintf(stdout,
        "\n%s"
        "amcanorder: %d",
        indents[indentLevel],
        pgAmTupData->amcanorder);
    fprintf(stdout,
        "\n%s"
        "amcanorderbyop: %d",
        indents[indentLevel],
        pgAmTupData->amcanorderbyop);
    fprintf(stdout,
        "\n%s"
        "amcanbackward: %d",
        indents[indentLevel],
        pgAmTupData->amcanbackward);
    fprintf(stdout,
        "\n%s"
        "amcanunique: %d",
        indents[indentLevel],
        pgAmTupData->amcanunique);
    fprintf(stdout,
        "\n%s"
        "amcanmulticol: %d",
        indents[indentLevel],
        pgAmTupData->amcanmulticol);
    fprintf(stdout,
        "\n%s"
        "amoptionalkey: %d",
        indents[indentLevel],
        pgAmTupData->amoptionalkey);
    fprintf(stdout,
        "\n%s"
        "amsearcharray: %d",
        indents[indentLevel],
        pgAmTupData->amsearcharray);
    fprintf(stdout,
        "\n%s"
        "amsearchnulls: %d",
        indents[indentLevel],
        pgAmTupData->amsearchnulls);
    fprintf(stdout,
        "\n%s"
        "amstorage: %d",
        indents[indentLevel],
        pgAmTupData->amstorage);
    fprintf(stdout,
        "\n%s"
        "amclusterable: %d",
        indents[indentLevel],
        pgAmTupData->amclusterable);
    fprintf(stdout,
        "\n%s"
        "ampredlocks: %d",
        indents[indentLevel],
        pgAmTupData->ampredlocks);
    fprintf(stdout,
        "\n%s"
        "amkeytype: %u",
        indents[indentLevel],
        pgAmTupData->amkeytype);
    fprintf(stdout,
        "\n%s"
        "aminsert: %u",
        indents[indentLevel],
        pgAmTupData->aminsert);
    fprintf(stdout,
        "\n%s"
        "ambeginscan: %u",
        indents[indentLevel],
        pgAmTupData->ambeginscan);
    fprintf(stdout,
        "\n%s"
        "amgettuple: %u",
        indents[indentLevel],
        pgAmTupData->amgettuple);
    fprintf(stdout,
        "\n%s"
        "amgetbitmap: %u",
        indents[indentLevel],
        pgAmTupData->amgetbitmap);
    fprintf(stdout,
        "\n%s"
        "amrescan: %u",
        indents[indentLevel],
        pgAmTupData->amrescan);
    fprintf(stdout,
        "\n%s"
        "amendscan: %u",
        indents[indentLevel],
        pgAmTupData->amendscan);
    fprintf(stdout,
        "\n%s"
        "ammarkpos: %u",
        indents[indentLevel],
        pgAmTupData->ammarkpos);
    fprintf(stdout,
        "\n%s"
        "amrestrpos: %u",
        indents[indentLevel],
        pgAmTupData->amrestrpos);
    fprintf(stdout,
        "\n%s"
        "ammerge: %u",
        indents[indentLevel],
        pgAmTupData->ammerge);
    fprintf(stdout,
        "\n%s"
        "ambuild: %u",
        indents[indentLevel],
        pgAmTupData->ambuild);
    fprintf(stdout,
        "\n%s"
        "ambuildempty: %u",
        indents[indentLevel],
        pgAmTupData->ambuildempty);
    fprintf(stdout,
        "\n%s"
        "ambulkdelete: %u",
        indents[indentLevel],
        pgAmTupData->ambulkdelete);
    fprintf(stdout,
        "\n%s"
        "amvacuumcleanup: %u",
        indents[indentLevel],
        pgAmTupData->amvacuumcleanup);
    fprintf(stdout,
        "\n%s"
        "amcanreturn: %u",
        indents[indentLevel],
        pgAmTupData->amcanreturn);
    fprintf(stdout,
        "\n%s"
        "amcostestimate: %u",
        indents[indentLevel],
        pgAmTupData->amcostestimate);
    fprintf(stdout,
        "\n%s"
        "amoptions: %u",
        indents[indentLevel],
        pgAmTupData->amoptions);
}

// parse and print tuple data in PG_INDEX relation
//
static void ParsePgIndexTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_index) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_index, nattrs);
        exit(1);
    }

    Form_pg_index pgIndexTupData = (Form_pg_index)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "indexrelid: %u",
        indents[indentLevel],
        (pgIndexTupData->indexrelid));
    fprintf(stdout,
        "\n%s"
        "indrelid: %u",
        indents[indentLevel],
        (pgIndexTupData->indrelid));
    fprintf(stdout,
        "\n%s"
        "indnatts: %d",
        indents[indentLevel],
        (pgIndexTupData->indnatts));
    fprintf(stdout,
        "\n%s"
        "indisunique: %d",
        indents[indentLevel],
        (pgIndexTupData->indisunique));
    fprintf(stdout,
        "\n%s"
        "indisprimary: %d",
        indents[indentLevel],
        (pgIndexTupData->indisprimary));

    fprintf(stdout,
        "\n%s"
        "indisexclusion: %d",
        indents[indentLevel],
        (pgIndexTupData->indisexclusion));
    fprintf(stdout,
        "\n%s"
        "indimmediate: %d",
        indents[indentLevel],
        (pgIndexTupData->indimmediate));
    fprintf(stdout,
        "\n%s"
        "indisclustered: %d",
        indents[indentLevel],
        (pgIndexTupData->indisclustered));
    fprintf(stdout,
        "\n%s"
        "indisusable: %d",
        indents[indentLevel],
        (pgIndexTupData->indisusable));
    fprintf(stdout,
        "\n%s"
        "indisvalid: %d",
        indents[indentLevel],
        (pgIndexTupData->indisvalid));

    fprintf(stdout,
        "\n%s"
        "indcheckxmin: %d",
        indents[indentLevel],
        (pgIndexTupData->indcheckxmin));
    fprintf(stdout,
        "\n%s"
        "indisready: %d",
        indents[indentLevel],
        (pgIndexTupData->indisready));

    //  add the other fields if needed. you have to inplace following indisready with the right field
    //
#ifdef DEBUG
    int remain = len - (offsetof(FormData_pg_index, indisready) + sizeof(bool));
    if (remain > 0) {
        fprintf(stdout,
            "\n%s"
            "the others:",
            indents[indentLevel]);
        formatBytes(tupdata + (len - remain), remain);
    }
#endif
    fprintf(stdout, "\n");
}

/* parse and print tuple data in PG_DATABASE relation */
static void ParsePgDatabaseTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_database) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_database, nattrs);
        exit(1);
    }

    Form_pg_database pgDatabaseTupData = (Form_pg_database)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "datname: %s",
        indents[indentLevel],
        (pgDatabaseTupData->datname.data));
    fprintf(stdout,
        "\n%s"
        "datdba: %u",
        indents[indentLevel],
        (pgDatabaseTupData->datdba));
    fprintf(stdout,
        "\n%s"
        "encoding: %d",
        indents[indentLevel],
        (pgDatabaseTupData->encoding));
    fprintf(stdout,
        "\n%s"
        "datcollate: %s",
        indents[indentLevel],
        (pgDatabaseTupData->datcollate.data));
    fprintf(stdout,
        "\n%s"
        "datctype: %s",
        indents[indentLevel],
        (pgDatabaseTupData->datctype.data));

    fprintf(stdout,
        "\n%s"
        "datistemplate: %d",
        indents[indentLevel],
        (pgDatabaseTupData->datistemplate));
    fprintf(stdout,
        "\n%s"
        "datallowconn: %d",
        indents[indentLevel],
        (pgDatabaseTupData->datallowconn));
    fprintf(stdout,
        "\n%s"
        "datconnlimit: %d",
        indents[indentLevel],
        (pgDatabaseTupData->datconnlimit));
    fprintf(stdout,
        "\n%s"
        "datlastsysoid: %u",
        indents[indentLevel],
        (pgDatabaseTupData->datlastsysoid));
    fprintf(stdout,
        "\n%s"
        "datfrozenxid: %u",
        indents[indentLevel],
        (pgDatabaseTupData->datfrozenxid));

    fprintf(stdout,
        "\n%s"
        "dattablespace: %u",
        indents[indentLevel],
        (pgDatabaseTupData->dattablespace));
    fprintf(stdout,
        "\n%s"
        "datcompatibility: %s",
        indents[indentLevel],
        (pgDatabaseTupData->datcompatibility.data));

#ifdef DEBUG
    int remain = len - DATABASE_TUPLE_SIZE;
    if (remain > 0) {
        if (remain - sizeof(TransactionId) > 0) {
            fprintf(stdout,
                "\n%s"
                "the others:",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain - sizeof(TransactionId));
            fprintf(stdout,
                "\n%s"
                "datfrozenxid64: ",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain + sizeof(TransactionId)), sizeof(TransactionId));
        } else if (remain - sizeof(TransactionId) == 0) {
            fprintf(stdout,
                "\n%s"
                "datfrozenxid64: ",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), sizeof(TransactionId));
        } else {
            fprintf(stdout,
                "\n%s"
                "the others:",
                indents[indentLevel]);
            formatBytes(tupdata + (len - remain), remain);
        }
    }
#endif
    fprintf(stdout, "\n");
}

/* parse and print tuple data in PG_TABLESPACE relation */
static void ParsePgTablespaceTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_tablespace) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_tablespace, nattrs);
        exit(1);
    }

    Form_pg_tablespace pgTablespaceTupData = (Form_pg_tablespace)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "spcname: %s",
        indents[indentLevel],
        (pgTablespaceTupData->spcname.data));
    fprintf(stdout,
        "\n%s"
        "spcowner: %u",
        indents[indentLevel],
        (pgTablespaceTupData->spcowner));

#ifdef DEBUG
    int remain = len - (offsetof(FormData_pg_tablespace, spcowner) + sizeof(Oid));
    if (remain > 0) {
        fprintf(stdout,
            "\n%s"
            "the others:",
            indents[indentLevel]);
        formatBytes(tupdata + (len - remain), remain);
    }
#endif
    fprintf(stdout, "\n");
}

static void ParsePgAttributeTupleData(binary tupdata, int len, binary nullBitmap, int nattrs)
{
    if (nattrs != Natts_pg_attribute) {
        fprintf(stdout, "invalid attributes number, expected %d, result %d", Natts_pg_attribute, nattrs);
        exit(1);
    }

    Form_pg_attribute pgAttributeTupData = (Form_pg_attribute)tupdata;
    indentLevel = 3;

    fprintf(stdout,
        "\n%s"
        "attrelid: %u",
        indents[indentLevel],
        (pgAttributeTupData->attrelid));
    fprintf(stdout,
        "\n%s"
        "attname: %s",
        indents[indentLevel],
        (pgAttributeTupData->attname.data));
    fprintf(stdout,
        "\n%s"
        "atttypid: %u",
        indents[indentLevel],
        (pgAttributeTupData->atttypid));
    fprintf(stdout,
        "\n%s"
        "attstattarget: %d",
        indents[indentLevel],
        (pgAttributeTupData->attstattarget));
    fprintf(stdout,
        "\n%s"
        "attlen: %d",
        indents[indentLevel],
        (pgAttributeTupData->attlen));
    fprintf(stdout,
        "\n%s"
        "attnum: %d",
        indents[indentLevel],
        (pgAttributeTupData->attnum));
    fprintf(stdout,
        "\n%s"
        "attndims: %d",
        indents[indentLevel],
        (pgAttributeTupData->attndims));
    fprintf(stdout,
        "\n%s"
        "attcacheoff: %d",
        indents[indentLevel],
        (pgAttributeTupData->attcacheoff));
    fprintf(stdout,
        "\n%s"
        "atttypmod: %d",
        indents[indentLevel],
        (pgAttributeTupData->atttypmod));
    fprintf(stdout,
        "\n%s"
        "attbyval: %d",
        indents[indentLevel],
        (pgAttributeTupData->attbyval));
    fprintf(stdout,
        "\n%s"
        "attstorage: %c",
        indents[indentLevel],
        (pgAttributeTupData->attstorage));
    fprintf(stdout,
        "\n%s"
        "attalign: %c",
        indents[indentLevel],
        (pgAttributeTupData->attalign));
    fprintf(stdout,
        "\n%s"
        "attnotnull: %d",
        indents[indentLevel],
        (pgAttributeTupData->attnotnull));
    fprintf(stdout,
        "\n%s"
        "atthasdef: %d",
        indents[indentLevel],
        (pgAttributeTupData->atthasdef));
    fprintf(stdout,
        "\n%s"
        "attisdropped: %d",
        indents[indentLevel],
        (pgAttributeTupData->attisdropped));
    fprintf(stdout,
        "\n%s"
        "attislocal: %d",
        indents[indentLevel],
        (pgAttributeTupData->attislocal));
    fprintf(stdout,
        "\n%s"
        "attcmprmode: %d",
        indents[indentLevel],
        (pgAttributeTupData->attcmprmode));
    fprintf(stdout,
        "\n%s"
        "attinhcount: %d",
        indents[indentLevel],
        (pgAttributeTupData->attinhcount));
    fprintf(stdout,
        "\n%s"
        "attcollation: %d",
        indents[indentLevel],
        (pgAttributeTupData->attcollation));
    fprintf(stdout,
        "\n%s" "attkvtype: %d",
        indents[indentLevel],
        (pgAttributeTupData->attkvtype));

#ifdef DEBUG
    int remain = len - ATTRIBUTE_FIXED_PART_SIZE;
    if (remain > 0) {
        fprintf(stdout,
            "\n%s"
            "the others:",
            indents[indentLevel]);
        formatBytes(tupdata + (len - remain), remain);
    }
#endif
    fprintf(stdout, "\n");
}

static void parse_uheap_item(const Item item, unsigned len, int blkno, int lineno)
{
    binary content;
    char buffer[128];
    int savedIndentLevel = indentLevel;
    UHeapDiskTuple utuple = (UHeapDiskTuple)item;

    indentLevel = 3;

    errno_t rc = snprintf_s(buffer, 128, 127, "\t\t\txid:%u, td:%d locker_td:%d\n",
        utuple->xid, utuple->td_id, utuple->reserved);
    securec_check(rc, "\0", "\0");
    fprintf(stdout, "%s", buffer);
    fprintf(stdout, "\t\t\tNumber of columns: %d\n", UHeapTupleHeaderGetNatts(utuple));
    fprintf(stdout, "\t\t\tFlag: %d\n", utuple->flag);
    fprintf(stdout, "\t\t\tFlag2: %d\n", utuple->flag2);
    fprintf(stdout, "\t\t\tt_hoff: %d\n", utuple->t_hoff);

    if (utuple->flag & UHEAP_HAS_NULL) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_HASNULL ");
    }
    if (utuple->flag & UHEAP_DELETED) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_DELETED ");
    }
    if (utuple->flag & UHEAP_INPLACE_UPDATED) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_INPLACE_UPDATED ");
    }
    if (utuple->flag & UHEAP_UPDATED) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_UPDATED ");
    }
    if (utuple->flag & UHEAP_XID_KEYSHR_LOCK) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_XID_KEYSHR_LOCK ");
    }
    if (utuple->flag & UHEAP_XID_NOKEY_EXCL_LOCK) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_XID_NOKEY_EXCL_LOCK ");
    }
    if (utuple->flag & UHEAP_XID_EXCL_LOCK) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_XID_EXCL_LOCK ");
    }
    if (utuple->flag & UHEAP_MULTI_LOCKERS) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_MULTI_LOCKERS ");
    }
    if (utuple->flag & UHEAP_INVALID_XACT_SLOT) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "UHEAP_INVALID_XACT_SLOT ");
    }
    if (utuple->flag & SINGLE_LOCKER_XID_IS_LOCK) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "SINGLE_LOCKER_XID_IS_LOCK ");
    }
    if (utuple->flag & SINGLE_LOCKER_XID_IS_SUBXACT) {
        fprintf(stdout, "\t\t\tFlag: %s\n", "SINGLE_LOCKER_XID_IS_SUBXACT ");
    }

    fprintf(stdout, "%sdata:", indents[indentLevel]);
    content = ((unsigned char *)(utuple) + utuple->t_hoff);
    len -= utuple->t_hoff;
    formatBytes(content, len);

    indentLevel = savedIndentLevel;
}

/*
 * Returns the value of given slot on page.
 *
 * Since this is just a read-only access of a single byte, the page doesn't
 * need to be locked.
 */
uint8 fsm_get_avail(Page page, uint slot)
{
    FSMPage fsmpage = (FSMPage)PageGetContents(page);
    Assert((slot) < LeafNodesPerPage);
    return fsmpage->fp_nodes[NonLeafNodesPerPage + slot];
}

/*
 * Returns the value at the root of a page.
 *
 * Since this is just a read-only access of a single byte, the page doesn't
 * need to be locked.
 */
uint8 fsm_get_max_avail(Page page)
{
    FSMPage fsmpage = (FSMPage)PageGetContents(page);
    return fsmpage->fp_nodes[0];
}

static void parse_heap_item(const Item item, unsigned len, int blkno, int lineno)
{
    HeapTupleHeader tup = (HeapTupleHeader)item;
    binary content;
    indentLevel = 3;
    dummyTuple.t_data = tup;

    fprintf(stdout,
        "%st_xmin/t_xmax/t_cid: %lu/%lu/%u\n",
        indents[indentLevel],
        HeapTupleGetRawXmin(&dummyTuple),
        HeapTupleGetRawXmax(&dummyTuple),
        HeapTupleHeaderGetRawCommandId(tup));

    fprintf(stdout,
        "%sctid:(block %u/%u, offset %u)\n",
        indents[indentLevel],
        tup->t_ctid.ip_blkid.bi_hi,
        tup->t_ctid.ip_blkid.bi_lo,
        tup->t_ctid.ip_posid);

    fprintf(stdout, "%st_infomask: ", indents[indentLevel]);
    if (tup->t_infomask & HEAP_HASNULL)
        fprintf(stdout, "HEAP_HASNULL ");
    if (tup->t_infomask & HEAP_HASVARWIDTH)
        fprintf(stdout, "HEAP_HASVARWIDTH ");
    if (tup->t_infomask & HEAP_HASEXTERNAL)
        fprintf(stdout, "HEAP_HASEXTERNAL ");
    if (tup->t_infomask & HEAP_HASOID)
        fprintf(stdout, "HEAP_HASOID(%d) ", HeapTupleHeaderGetOid(tup));
    if (tup->t_infomask & HEAP_COMPRESSED)
        fprintf(stdout, "HEAP_COMPRESSED ");
    if (tup->t_infomask & HEAP_COMBOCID)
        fprintf(stdout, "HEAP_COMBOCID ");
    if (tup->t_infomask & HEAP_XMAX_EXCL_LOCK)
        fprintf(stdout, "HEAP_XMAX_EXCL_LOCK ");
    if (tup->t_infomask & HEAP_XMAX_SHARED_LOCK)
        fprintf(stdout, "HEAP_XMAX_SHARED_LOCK ");
    if (tup->t_infomask & HEAP_XMIN_COMMITTED)
        fprintf(stdout, "HEAP_XMIN_COMMITTED ");

    if (tup->t_infomask & HEAP_XMIN_INVALID)
        fprintf(stdout, "HEAP_XMIN_INVALID ");
    if (lineno >= start_item && lineno < start_item + num_item) {
        tup->t_infomask &= ~HEAP_XMIN_COMMITTED;
        tup->t_infomask &= ~HEAP_COMPRESSED;
        tup->t_infomask &= ~HEAP_HASEXTERNAL;
        tup->t_infomask &= ~HEAP_XMAX_COMMITTED;
        tup->t_infomask &= ~HEAP_COMBOCID;
        tup->t_infomask |= HEAP_XMIN_INVALID;
        fprintf(stdout,
            "force writer tup->t_infomask to HEAP_XMIN_INVALID and clean HEAP_XMIN_COMMITTED | HEAP_COMPRESSED | "
            "HEAP_HASEXTERNAL | HEAP_XMAX_COMMITTED | HEAP_COMBOCID");
    }
    if (tup->t_infomask & HEAP_XMAX_COMMITTED)
        fprintf(stdout, "HEAP_XMAX_COMMITTED ");
    if (tup->t_infomask & HEAP_XMAX_INVALID)
        fprintf(stdout, "HEAP_XMAX_INVALID ");
    if (tup->t_infomask & HEAP_XMAX_IS_MULTI)
        fprintf(stdout, "HEAP_XMAX_IS_MULTI ");
    if (tup->t_infomask & HEAP_UPDATED)
        fprintf(stdout, "HEAP_UPDATED ");
    if ((tup->t_infomask & HEAP_HAS_8BYTE_UID)) {
        fprintf(stdout, "HEAP_HAS_8BYTE_UID ");
    } else {
        fprintf(stdout, "HEAP_HAS_NO_UID ");
    }
    fprintf(stdout, "\n");

    fprintf(stdout, "%st_infomask2: ", indents[indentLevel]);
    if (tup->t_infomask2 & HEAP_HOT_UPDATED)
        fprintf(stdout, "HEAP_HOT_UPDATED ");
    if (tup->t_infomask2 & HEAP_ONLY_TUPLE)
        fprintf(stdout, "HEAP_ONLY_TUPLE ");
    fprintf(stdout, "Attrs Num: %d", HeapTupleHeaderGetNatts(tup, NULL));
    fprintf(stdout, "\n");

    fprintf(stdout, "%st_hoff: %u\n", indents[indentLevel], tup->t_hoff);
    len -= tup->t_hoff;

    // nulls bitmap
    //
    fprintf(stdout, "%st_bits: ", indents[indentLevel]);
    formatBitmap((const unsigned char*)tup->t_bits, BITMAPLEN(HeapTupleHeaderGetNatts(tup, NULL)), 'V', 'N');
    fprintf(stdout, "\n");

    content = GETHEAPSTRUCT(tup);
    // compressed bitmap
    //
#ifdef DEBUG
    if (HEAP_TUPLE_IS_COMPRESSED(tup)) {
        fprintf(stdout, "%scompressed bitmap: ", indents[indentLevel]);
        int nbytes = BITMAPLEN(HeapTupleHeaderGetNatts(tup, NULL));
        bits8* bitmap = (bits8*)content;
        formatBitmap((const unsigned char*)bitmap, nbytes, 'P', 'C');
        content += nbytes;
        len -= nbytes;
        fprintf(stdout, "\n");
        fprintf(stdout, "%sdata (excluding compressed bitmap): ", indents[indentLevel]);
    } else {
        fprintf(stdout, "%sdata: ", indents[indentLevel]);
    }
#endif

    if (PgHeapRelTupleParserCursor >= 0) {
        (PgHeapRelTupleParser[PgHeapRelTupleParserCursor])(content, len, tup->t_bits, HeapTupleHeaderGetNatts(tup, NULL));
    } else {
        formatBytes(content, len);
    }
    fprintf(stdout, "\n");
}

static void parse_btree_index_item(const Item item, unsigned len, int blkno, int lineno)
{
    IndexTuple itup = (IndexTuple)item;
    bool hasnull = (itup->t_info & INDEX_NULL_MASK);
    unsigned int tuplen = (itup->t_info & INDEX_SIZE_MASK);
    unsigned int offset = 0;
    unsigned char* null_map = NULL;

    indentLevel = 3;

    if (tuplen != len) {
        /* there are xmin/xmax */
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
        fprintf(stdout,
            "%s xmin:%d xmax:%d Heap Tid: block %u/%u, offset %u\n",
            indents[indentLevel],
            uxid->xmin,
            uxid->xmax,
            itup->t_tid.ip_blkid.bi_hi,
            itup->t_tid.ip_blkid.bi_lo,
            itup->t_tid.ip_posid);
    } else {
        fprintf(stdout,
            "%s Heap Tid: block %u/%u, offset %u\n",
            indents[indentLevel],
            itup->t_tid.ip_blkid.bi_hi,
            itup->t_tid.ip_blkid.bi_lo,
            itup->t_tid.ip_posid);
    }

    fprintf(stdout, "%s Length: %u", indents[indentLevel], tuplen);
    if (itup->t_info & INDEX_VAR_MASK)
        fprintf(stdout, ", has var-width attrs");
    if (hasnull) {
        fprintf(stdout, ", has nulls \n");
        formatBitmap((unsigned char*)item + sizeof(IndexTupleData), sizeof(IndexAttributeBitMapData), 'V', 'N');
        null_map = (unsigned char*)item + sizeof(IndexTupleData);
    } else
        fprintf(stdout, "\n");

    offset = IndexInfoFindDataOffset(itup->t_info);
    if (PgIndexRelTupleParserCursor >= 0) {
        (PgIndexRelTupleParser[PgIndexRelTupleParserCursor])(
            (unsigned char*)item + offset, tuplen - offset, null_map, 32 /* skip */);
    } else {
        formatBytes((unsigned char*)item + offset, tuplen - offset);
    }
}

static void parse_one_item(const Item item, unsigned len, int blkno, int lineno, SegmentType type)
{
    switch (type) {
        case SEG_HEAP:
            parse_heap_item(item, len, blkno, lineno);
            break;

        case SEG_UHEAP:
            parse_uheap_item(item, len, blkno, lineno);
            break;

        case SEG_INDEX_BTREE:
            parse_btree_index_item(item, len, blkno, lineno);
            break;

        default:
            break;
    }
}

static void ParseHeapPageHeader(const PageHeader page, int blkno, int blknum)
{
    bool checksum_matched = false;
    uint64 freeSpace = 0;
    if (CheckPageZeroCases(page)) {
        uint16 checksum = pg_checksum_page((char*)page, (BlockNumber)blkno + (SegNo * ((BlockNumber)RELSEG_SIZE)));
        checksum_matched = (checksum == page->pd_checksum);
    }
    fprintf(stdout, "page information of block %d/%d\n", blkno, blknum);
    fprintf(stdout, "\tpd_lsn: %X/%X\n", (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page));
    fprintf(stdout, "\tpd_checksum: 0x%X, verify %s\n", page->pd_checksum, checksum_matched ? "success" : "fail");

    fprintf(stdout, "\tpd_flags: ");
    if (PageHasFreeLinePointers(page))
        fprintf(stdout, "PD_HAS_FREE_LINES ");
    if (PageIsFull(page))
        fprintf(stdout, "PD_PAGE_FULL ");
    if (PageIsAllVisible(page))
        fprintf(stdout, "PD_ALL_VISIBLE ");
    if (PageIsCompressed(page))
        fprintf(stdout, "PD_COMPRESSED_PAGE ");
    if (PageIsLogical(page))
        fprintf(stdout, "PD_LOGICAL_PAGE ");
    if (PageIsEncrypt(page))
        fprintf(stdout, "PD_ENCRYPT_PAGE ");
    fprintf(stdout, "\n");
    fprintf(stdout, "\tpd_lower: %u, %s\n", page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty");
    fprintf(stdout, "\tpd_upper: %u, %s\n", page->pd_upper, PageIsNew(page) ? "new" : "old");
    fprintf(stdout, "\tpd_special: %u, size %u\n", page->pd_special, PageGetSpecialSize(page));
    fprintf(stdout,
        "\tPage size & version: %u, %u\n",
        (uint16)PageGetPageSize(page),
        (uint16)PageGetPageLayoutVersion(page));
    fprintf(stdout,
        "\tpd_xid_base: %lu, pd_multi_base: %lu\n",
        ((HeapPageHeader)(page))->pd_xid_base,
        ((HeapPageHeader)(page))->pd_multi_base);
    fprintf(stdout,
        "\tpd_prune_xid: %lu\n",
        ((HeapPageHeader)(page))->pd_prune_xid + ((HeapPageHeader)(page))->pd_xid_base);
    if (page->pd_upper < page->pd_lower) {
        fprintf(stdout, "WARNING: INVALID PAGE!");
    } else {
        freeSpace = page->pd_upper - page->pd_lower;
        g_freeMax = freeSpace > g_freeMax ? freeSpace : g_freeMax;
        g_freeSpace += freeSpace;
    }
    g_pageCount++;
    return;
}

static void ParseUHeapPageHeader(const PageHeader page, int blkno, int blknum)
{
    bool checksum_matched = false;
    uint64 tdCount = 0;
    uint64 freeSpace = 0;
    UHeapPageHeader upage = (UHeapPageHeader)page;
    if (CheckPageZeroCases(page)) {
        uint16 checksum = pg_checksum_page((char*)page, (BlockNumber)blkno + (SegNo * ((BlockNumber)RELSEG_SIZE)));
        checksum_matched = (checksum == page->pd_checksum);
    }
    fprintf(stdout, "page information of block %d/%d\n", blkno, blknum);
    fprintf(stdout, "\tpd_lsn: %X/%X\n", (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page));
    fprintf(stdout, "\tpd_checksum: 0x%X, verify %s\n", page->pd_checksum, checksum_matched ? "success" : "fail");

    fprintf(stdout, "\tpd_flags: ");
    if (UPageHasFreeLinePointers(page))
        fprintf(stdout, "PD_HAS_FREE_LINES ");
    if (UPageIsFull(page))
        fprintf(stdout, "PD_PAGE_FULL ");
    if (PageIsAllVisible(page))
        fprintf(stdout, "PD_ALL_VISIBLE ");
    if (PageIsCompressed(page))
        fprintf(stdout, "PD_COMPRESSED_PAGE ");
    if (PageIsLogical(page))
        fprintf(stdout, "PD_LOGICAL_PAGE ");
    if (PageIsEncrypt(page))
        fprintf(stdout, "PD_ENCRYPT_PAGE ");
    fprintf(stdout, "\n");
    fprintf(stdout, "\tpd_lower: %u, %s\n", page->pd_lower, UPageIsEmpty(upage) ? "empty" : "non-empty");
    fprintf(stdout, "\tpd_upper: %u, %s\n", page->pd_upper, PageIsNew(page) ? "new" : "old");
    fprintf(stdout, "\tpd_special: %u, size %u\n", page->pd_special, PageGetSpecialSize(page));
    fprintf(stdout, "\tPage size & version: %u, %u\n",
        (uint16)PageGetPageSize(page), (uint16)PageGetPageLayoutVersion(page));
    fprintf(stdout, "\tpotential_freespace: %u\n", upage->potential_freespace);
    fprintf(stdout, "\ttd_count: %u\n", upage->td_count);
    fprintf(stdout, "\tpd_prune_xid: %lu\n", upage->pd_prune_xid);
    fprintf(stdout, "\tpd_xid_base: %lu, pd_multi_base: %lu\n",
        upage->pd_xid_base, upage->pd_multi_base);
    if (upage->pd_upper < upage->pd_lower) {
        fprintf(stdout, "WARNING: INVALID PAGE!\n");
    } else {
        freeSpace = upage->pd_upper - upage->pd_lower;
        g_freeMax = freeSpace > g_freeMax ? freeSpace : g_freeMax;
        g_freeSpace += freeSpace;
    }
    g_pageCount++;
    tdCount = upage->td_count;
    g_tdCount += tdCount;
    g_tdMax = tdCount > g_tdMax ? tdCount : g_tdMax;

    return;
}

static void ParsePageHeader(const PageHeader page, int blkno, int blknum, SegmentType type = SEG_HEAP)
{

    switch (type) {
        case SEG_UHEAP:
            ParseUHeapPageHeader(page, blkno, blknum);
            break;
        default:
            ParseHeapPageHeader(page, blkno, blknum);
            break;
    }

}

static void parse_special_data(const char* buffer, SegmentType type)
{
    PageHeader page = (PageHeader)buffer;

    if (SEG_HEAP == type) {
        if (!PageIsCompressed(page)) {
            fprintf(stdout, "\nNormal Heap Page, special space is 0 \n");
            return;
        }

        int size = PageGetSpecialSize(page);
        fprintf(stdout, "\ncompress metadata: offset %u, size %d\n\t\t", page->pd_special, size);
#ifdef DEBUG
        const char* content = buffer + page->pd_special;
        formatBytes((unsigned char*)content, size);
#endif
        fprintf(stdout, "\n");
    } else if (SEG_INDEX_BTREE == type) {
        BTPageOpaqueInternal opaque;
        UBTPageOpaqueInternal uopaque = NULL;

        opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (PageGetSpecialSize(page) > MAXALIGN(sizeof(BTPageOpaqueData))) {
            uopaque = (UBTPageOpaqueInternal)opaque;
        }
        fprintf(stdout, "\nbtree index special information:\n");
        fprintf(stdout, "\tbtree left sibling: %u\n", opaque->btpo_prev);
        fprintf(stdout, "\tbtree right sibling: %u\n", opaque->btpo_next);
        if (!P_ISDELETED(opaque))
            fprintf(stdout, "\tbtree tree level: %u\n", opaque->btpo.level);
        else {
            if (uopaque)
                fprintf(stdout, "\tnext txid (deleted): %lu\n", ((UBTPageOpaque)uopaque)->xact);
            else
                fprintf(stdout, "\tnext txid (deleted): %lu\n", ((BTPageOpaque)opaque)->xact);
        }
        fprintf(stdout, "\tbtree flag: ");
        if (P_ISLEAF(opaque))
            fprintf(stdout, "BTP_LEAF ");
        else
            fprintf(stdout, "BTP_INTERNAL ");
        if (P_ISROOT(opaque))
            fprintf(stdout, "BTP_ROOT ");
        if (P_ISDELETED(opaque))
            fprintf(stdout, "BTP_DELETED ");
        if (P_ISHALFDEAD(opaque))
            fprintf(stdout, "BTP_HALF_DEAD ");
        if (P_HAS_GARBAGE(opaque))
            fprintf(stdout, "BTP_HAS_GARBAGE ");
        fprintf(stdout, "\n");

        fprintf(stdout, "\tbtree cycle ID: %u\n", opaque->btpo_cycleid);
        if (uopaque) {
            fprintf(stdout, "\tubtree active tuples: %d\n", uopaque->activeTupleCount);
        }
    } else if (SEG_UHEAP == type) {
        ParseTDSlot(buffer);
    }
}

static bool parse_bcm_page(const char* buffer, int blkno, int blknum)
{
    BlockNumber managedBlockNum = 0;
    unsigned char* bcm = NULL;
    uint32 i;
    BCMHeader* bcm_header = NULL;
    int numberOfBcmBits = 0;
    int numberOfMetaBits = 0;

    managedBlockNum = Max(0, blknum - blkno - 1);
    fprintf(stdout, "\n\tBCM information on page %d\n", blkno);

    /* 24 is page header */
    bcm = (unsigned char*)(buffer + 24);
    bcm_header = (BCMHeader*)(buffer + 24);

    if (0 == blkno) {
        /* map header */
        fprintf(stdout, "\t\tBCM page type : BCM map header\n");
        fprintf(stdout, "\t\t\tBCM file header info	:\n");
        fprintf(stdout, "\t\t\t\tStorage		: %s\n", bcm_header->type == 0 ? "ROW_STORE" : "COLUMN_STORE");
        fprintf(stdout,
            "\t\t\t\tFile info	: %d/%d/%d\n",
            bcm_header->node.spcNode,
            bcm_header->node.dbNode,
            bcm_header->node.relNode);
        fprintf(stdout, "\t\t\t\tBlock size	: %d\n", bcm_header->blockSize);
    } else if (0 == ((blkno - 1) % (META_BLOCKS_PER_PAGE + 1))) {
        /* meta page */
        fprintf(stdout, "\t\tBCM page type : BCM meta page\n");
        fprintf(stdout, "\t\t");
        for (i = 0; i < BCMMAPSIZE; i++) {
            fprintf(stdout, "%X ", bcm[i]);
            numberOfMetaBits += number_of_meta_bits[bcm[i]];

            if ((i + 1) % 80 == 0)
                fprintf(stdout, "\n\t\t");
        }
        fprintf(stdout, "\t Unsynced bcm blocks: %d/%ld", numberOfMetaBits, BCMMAPSIZE << 1);
        fprintf(stdout, "\n");
    } else {
        /* normal page */
        fprintf(stdout, "\t\tBCM page type : BCM normal page\n");
        fprintf(stdout, "\t\t");
        for (i = 0; i < BCMMAPSIZE; i++) {
            fprintf(stdout, "%X ", bcm[i]);
            numberOfBcmBits += number_of_bcm_bits[bcm[i]];

            if ((i + 1) % 80 == 0)
                fprintf(stdout, "\n\t\t");
        }
        fprintf(stdout, "\t Unsynced heap blocks: %d/%ld", numberOfBcmBits, BCMMAPSIZE << 2);
        fprintf(stdout, "\n");
    }
    return true;
}

static void ParseTDSlot(const char *page)
{
    TD *thistrans;
    unsigned short tdCount;
    UHeapPageTDData *tdPtr;
    UHeapPageHeaderData *uheapPage = (UHeapPageHeaderData *)page;

    tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    tdCount = uheapPage->td_count;
    fprintf(stdout, "\n\n\tUHeap Page TD information, nTDSlots = %hu\n", tdCount);
    for (int i = 0; i < tdCount; i++) {
        thistrans = &tdPtr->td_info[i];
        fprintf(stdout, "\n\t\t TD Slot #%d, xid:%ld, urp:%ld\n",
                i + 1, thistrans->xactid, thistrans->undo_record_ptr);
    }
}

static void parse_heap_or_index_page(const char* buffer, int blkno, SegmentType type)
{
    const PageHeader page = (const PageHeader)buffer;
    int i;
    int nline, nstorage, nunused, nnormal, ndead;
    ItemId lp;
    Item item;
    RowPtr *rowptr;

    nstorage = 0;
    nunused = nnormal = ndead = 0;
    if (type == SEG_UHEAP) {
        UHeapPageHeaderData *upghdr = (UHeapPageHeaderData *)buffer;
        if (upghdr->pd_lower <= SizeOfUHeapPageHeaderData) {
            nline = 0;
        } else {
            nline = (upghdr->pd_lower - (SizeOfUHeapPageHeaderData +
                SizeOfUHeapTDData(upghdr))) / sizeof(RowPtr);
            g_rpCount += (uint64)nline;
            g_rpMax = (uint64)nline > g_rpMax ? (uint64)nline : g_rpMax;
        }
        fprintf(stdout, "\n\tUHeap tuple information on this page\n");
        for (i = FirstOffsetNumber; i <= nline; i++) {
            rowptr = UPageGenerateRowPtr(buffer, i);
            if (RowPtrIsUsed(rowptr)) {
                if (RowPtrHasStorage(rowptr))
                    nstorage++;

                if (RowPtrIsNormal(rowptr)) {
                    fprintf(stdout, "\n\t\tTuple #%d is normal: length %u, offset %u\n", i, RowPtrGetLen(rowptr),
                        RowPtrGetOffset(rowptr));
                    nnormal++;

                    item = UPageGetRowData(page, rowptr);
                    HeapTupleCopyBaseFromPage(&dummyTuple, page);
                    parse_one_item(item, RowPtrGetLen(rowptr), blkno, i, type);
                } else if (RowPtrIsDead(rowptr)) {
                    fprintf(stdout, "\n\t\tTuple #%d is dead: length %u, offset %u", i, RowPtrGetLen(rowptr),
                        RowPtrGetOffset(rowptr));
                    ndead++;
                } else {
                    fprintf(stdout, "\n\t\tTuple #%d is redirected: length %u, offset %u", i, RowPtrGetLen(rowptr),
                        RowPtrGetOffset(rowptr));
                }
            } else {
                nunused++;
                fprintf(stdout, "\n\t\tTuple #%d is unused\n", i);
            }
        }
        fprintf(stdout, "\tSummary (%d total): %d unused, %d normal, %d dead\n", nline, nunused, nnormal, ndead);

        parse_special_data(buffer, type);
    } else if (type == SEG_HEAP || type == SEG_INDEX_BTREE) {
        nline = PageGetMaxOffsetNumber((Page)page);
        g_rpCount += (uint64)nline;
        g_rpMax = (uint64)nline > g_rpMax ? (uint64)nline : g_rpMax;
        fprintf(stdout, "\n\tHeap tuple information on this page\n");
        for (i = FirstOffsetNumber; i <= nline; i++) {
            lp = PageGetItemId(page, i);
            if (ItemIdIsUsed(lp)) {
                if (ItemIdHasStorage(lp))
                    nstorage++;

                if (ItemIdIsNormal(lp) || (type == SEG_INDEX_BTREE && blkno != 0 && IndexItemIdIsFrozen(lp))) {
                    if (ItemIdIsNormal(lp)) {
                        fprintf(stdout,
                                "\n\t\tTuple #%d is normal: length %u, offset %u\n",
                                i,
                                ItemIdGetLength(lp),
                                ItemIdGetOffset(lp));
                    } else {
                        fprintf(stdout, "\n\t\tTuple #%d is frozen: length %u, offset %u\n", i, ItemIdGetLength(lp),
                            ItemIdGetOffset(lp));
                    }
                    nnormal++;

                    item = PageGetItem(page, lp);
                    HeapTupleCopyBaseFromPage(&dummyTuple, page);
                    parse_one_item(item, ItemIdGetLength(lp), blkno, i, type);
                } else if (ItemIdIsDead(lp)) {
                    fprintf(stdout,
                        "\n\t\tTuple #%d is dead: length %u, offset %u",
                        i,
                        ItemIdGetLength(lp),
                        ItemIdGetOffset(lp));
                    ndead++;
                } else {
                    fprintf(stdout,
                        "\n\t\tTuple #%d is redirected: length %u, offset %u",
                        i,
                        ItemIdGetLength(lp),
                        ItemIdGetOffset(lp));
                }
            } else {
                nunused++;
                fprintf(stdout, "\n\t\tTuple #%d is unused\n", i);
            }
        }
        fprintf(stdout, "\tSummary (%d total): %d unused, %d normal, %d dead\n", nline, nunused, nnormal, ndead);

        /* compress meta data or index special data */
        parse_special_data(buffer, type);
    }

    fprintf(stdout, "\n");
    return;
}

const int MAX_OUTPUT_ONELINE = 40;
static void parse_fsm_page(const char *buffer, int blkno)
{
    FSMAddress addr;
    FSMPage fsmpage;
    uint cur_index = 0;
    uint i = 0;
    uint j = 0;
    uint cu = 0;
    uint slot;
    uint8 cat;
    uint8 max_cat;
    PageHeader page = (PageHeader)buffer;

    for (i = 0; i < FSM_TREE_DEPTH; i++) {
        cu = cu * SlotsPerFSMPage + 1;
    }

    for (i = FSM_ROOT_LEVEL; i > 0; i--) {
        if (blkno == 0) {
            break;
        }
        cur_index = blkno / cu;
        if (blkno % cu == 0) {
            blkno = 0;
            break;
        } else {
            blkno -= cur_index + 1;
        }
        cu = (cu - 1) / SlotsPerFSMPage;
    }
    if (i > 0) {
        addr.logpageno = cur_index;
    } else {
        addr.logpageno = blkno;
    }
    addr.level = i;
    /* Print fsm info */
    max_cat = fsm_get_max_avail((Page)buffer);
    fsmpage = (FSMPage)PageGetContents(page);

    fprintf(stdout, "fsm page [%u,%u]\n", addr.level, addr.logpageno);
    fprintf(stdout, "max avail %d fp_next_slot %d\n", max_cat, fsmpage->fp_next_slot);

    if (addr.level == FSM_BOTTOM_LEVEL) {
        fprintf(stdout, "heap page range %lu-%lu :\n", addr.logpageno * SlotsPerFSMPage,
            addr.logpageno * SlotsPerFSMPage + SlotsPerFSMPage - 1);
    } else {
        fprintf(stdout, "fsm page level %u, range %lu-%lu :\n", addr.level - 1, addr.logpageno * SlotsPerFSMPage,
            addr.logpageno * SlotsPerFSMPage + SlotsPerFSMPage - 1);
    }

    for (slot = 0; slot < SlotsPerFSMPage; slot++) {
        j++;
        cat = fsm_get_avail((Page)buffer, slot);
        fprintf(stdout, "%3d ", cat);
        if (j % MAX_OUTPUT_ONELINE == 0) {
            fprintf(stdout, "\n");
        }
    }
    fprintf(stdout, "\n");
}

static bool parse_data_page(const char *buffer, int blkno, SegmentType type)
{
    if (only_vm || only_bcm) {
        return true;
    }
    if (type == SEG_HEAP || type == SEG_UHEAP || type == SEG_INDEX_BTREE) {
        parse_heap_or_index_page(buffer, blkno, type);
    } else if (type == SEG_FSM) {
        parse_fsm_page(buffer, blkno);
    }
    return true;
}

static int parse_a_page(const char* buffer, int blkno, int blknum, SegmentType type)
{
    const PageHeader page = (const PageHeader)buffer;
    uint16 headersize;

    if (PageIsNew(page)) {
        fprintf(stdout, "Page information of block %d/%d : new page\n\n", blkno, blknum);
        ParsePageHeader(page, blkno, blknum, type);
        return true;
    }

    headersize = GetPageHeaderSize(page);
    if (page->pd_lower < headersize || page->pd_lower > page->pd_upper || page->pd_upper > page->pd_special ||
        page->pd_special > BLCKSZ || page->pd_special != MAXALIGN(page->pd_special)) {
        fprintf(stderr,
            "The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u\n",
            page->pd_lower,
            page->pd_upper,
            page->pd_special);
        return false;
    }

    if (only_bcm) {
        parse_bcm_page(buffer, blkno, blknum);
    } else if (!only_vm) {
        ParsePageHeader(page, blkno, blknum, type);
    }

    parse_data_page(buffer, blkno, type);

    return true;
}

static BlockNumber CalculateMaxBlockNumber(BlockNumber blknum, BlockNumber start, BlockNumber number)
{
    /* parse */
    if (start >= blknum) {
        (void)fprintf(stderr, "start point exceeds the total block number of relation.\n");
        return InvalidBlockNumber;
    } else if ((start + number) > blknum) {
        (void)fprintf(stderr, "don't have %u blocks from block %u in the relation, only %u blocks\n", number, start,
                (blknum - start));
        number = blknum;
    } else if (number == 0) {
        number = blknum;
    } else {
        number += start;
    }
    return number;
}

static void MarkBufferDirty(char *buffer, size_t len)
{
    size_t writeLen = len / 2;
    unsigned char fill_byte[writeLen] = {0xFF};
    for (size_t i = 0; i < writeLen; i++)
        fill_byte[i] = 0xFF;
    auto rc = memcpy_s(buffer + writeLen, len - writeLen, fill_byte, writeLen);
    securec_check(rc, "", "");
}

static int parse_page_file(const char *filename, SegmentType type, const uint32 start_point, const uint32 number_read)
{
    if (!IsCompressedFile(filename, strlen(filename))) {
        return parse_uncompressed_page_file(filename, type, start_point, number_read);
    }

    PageCompression *pageCompression = new(std::nothrow) PageCompression();
    if (pageCompression == NULL) {
        fprintf(stderr, "compression page new failed\n");
        return false;
    }
    if (pageCompression->Init(filename, (BlockNumber)SegNo) != SUCCESS) {
        delete pageCompression;
        return parse_uncompressed_page_file(filename, type, start_point, number_read);
    }

    BlockNumber start = start_point;
    BlockNumber blknum = pageCompression->GetMaxBlockNumber();
    BlockNumber number = CalculateMaxBlockNumber(blknum, start, number_read);
    if (number == InvalidBlockNumber) {
        delete pageCompression;
        return false;
    }
    char compressed[BLCKSZ];
    char decompressed[BLCKSZ];
    while (start < number) {
        size_t compressedSize = pageCompression->ReadCompressedBuffer(start, compressed, BLCKSZ);
        if (compressedSize > MIN_COMPRESS_ERROR_RT) {
            fprintf(stderr, "read block %u failed, filename: %s: code: %zu %s\n",
                    start, filename, compressedSize, strerror(errno));
            delete pageCompression;
            return false;
        }
        char *parseFile = NULL;
        if (compressedSize < BLCKSZ) {
            (void)pageCompression->DecompressedPage(compressed, decompressed);
            parseFile = decompressed;
        } else {
            parseFile = compressed;
        }
        if (!parse_a_page(parseFile, start, blknum, type)) {
            fprintf(stderr, "Error during parsing block %d/%d\n", start, blknum);
            delete pageCompression;
            return false;
        }
        if ((write_back && num_item) || dirty_page) {
            if (dirty_page) {
                MarkBufferDirty(parseFile, BLCKSZ);
            }
            if (!pageCompression->WriteBackUncompressedData(compressed, compressedSize, parseFile, BLCKSZ, start)) {
                fprintf(stderr, "write back failed, filename: %s: %s\n", filename, strerror(errno));
                delete pageCompression;
                return false;
            }
        }
        start++;
    }
    delete pageCompression;
    return true;
}

static int parse_uncompressed_page_file(const char *filename, SegmentType type, const uint32 start_point,
                                        const uint32 number_read)
{
    char buffer[BLCKSZ];
    FILE* fd = NULL;
    long size;
    BlockNumber blknum;
    BlockNumber start = start_point;
    BlockNumber number = number_read;
    size_t result;

    fd = fopen(filename, "rb+");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    size = ftell(fd);
    rewind(fd);

    if ((0 == size) || (0 != size % BLCKSZ)) {
        fprintf(stderr, "The page file size is not an exact multiple of BLCKSZ\n");
        fclose(fd);
        return false;
    }

    blknum = size / BLCKSZ;

    /* parse */
    number = CalculateMaxBlockNumber(blknum, start, number);
    if (number == InvalidBlockNumber) {
        fclose(fd);
        return false;
    } else if ((start + number) > blknum) {
        fprintf(stderr,
            "don't have %u blocks from block %u in the relation, only %u blocks\n",
            number,
            start,
            (blknum - start));
        number = blknum;
    } else if (number == 0) {
        number = blknum;
    } else {
        number += start;
    }

    Assert((start * BLCKSZ) < size);
    if (start != 0)
        fseek(fd, (start * BLCKSZ), SEEK_SET);

    while (start < number) {
        result = fread(buffer, 1, BLCKSZ, fd);
        if (BLCKSZ != result) {
            fprintf(stderr, "Reading error");
            fclose(fd);
            return false;
        }

        if (!parse_a_page(buffer, start, blknum, type)) {
            fprintf(stderr, "Error during parsing block %u/%u\n", start, blknum);
            fclose(fd);
            return false;
        }

        if ((write_back && num_item) || dirty_page) {
            if (dirty_page) {
                unsigned char fill_byte[4096] = {0xFF};
                for (int i = 0; i < 4096; i++)
                    fill_byte[i] = 0xFF;
                errno_t rc = memcpy_s(buffer + 4096, BLCKSZ - 4096, fill_byte, 4096);
                securec_check_c(rc, "\0", "\0");
            }
            fseek(fd, (start * BLCKSZ), SEEK_SET);
            fwrite(buffer, 1, BLCKSZ, fd);
            fflush(fd);
            fseek(fd, ((start + 1) * BLCKSZ), SEEK_SET);
        }

        start++;
    }

    float8 rpAvg = g_pageCount == 0 ? 0 : (float8)g_rpCount / g_pageCount;
    float8 tdAvg = g_pageCount == 0 ? 0 : (float8)g_tdCount / g_pageCount;
    float8 freeAvg = g_pageCount == 0 ? 0 : (float8)g_freeSpace / g_pageCount;
    fprintf(stdout, "Relation information : pageCount %lu.\n", g_pageCount);
    fprintf(stdout, "RP information : rpCount %lu, rpMax %lu, rpAvg %f.\n",
        g_rpCount, g_rpMax, rpAvg);
    fprintf(stdout, "TD information : tdCount %lu, tdMax %lu, tdAvg %f.\n",
        g_tdCount, g_tdMax, tdAvg);
    fprintf(stdout, "Freespace information : freeTotal %lu, freeMax %lu, freeAvg %f.\n",
        g_freeSpace, g_freeMax, freeAvg);
    fclose(fd);

    return true;
}

static void parse_relation_data_struct(Relation reldata, int pos)
{
    indentLevel = 0;
    fprintf(stdout, "\n%s RelationData Struct #%d:\n", indents[indentLevel], pos);

    ++indentLevel;
    if (reldata->rd_node.bucketNode == -1) {
        fprintf(stdout,
            "%s rd_node: %u/%u/%u\n",
            indents[indentLevel],
            reldata->rd_node.spcNode,
            reldata->rd_node.dbNode,
            reldata->rd_node.relNode);
    } else {
        fprintf(stdout,
            "%s rd_node: %u/%u/%u/%d\n",
            indents[indentLevel],
            reldata->rd_node.spcNode,
            reldata->rd_node.dbNode,
            reldata->rd_node.relNode,
            reldata->rd_node.bucketNode);
    }
    fprintf(stdout, "%s rd_refcnt: %d\n", indents[indentLevel], reldata->rd_refcnt);
    fprintf(stdout, "%s rd_backend: %d\n", indents[indentLevel], reldata->rd_backend);
    fprintf(stdout, "%s rd_isnailed: %d\n", indents[indentLevel], reldata->rd_isnailed);
    fprintf(stdout, "%s rd_isvalid: %d\n", indents[indentLevel], reldata->rd_isvalid);
    fprintf(stdout, "%s rd_indexvalid: %d\n", indents[indentLevel], reldata->rd_indexvalid);
    fprintf(stdout, "%s rd_islocaltemp: %d\n", indents[indentLevel], reldata->rd_islocaltemp);
    fprintf(stdout, "%s rd_createSubid: %lu\n", indents[indentLevel], reldata->rd_createSubid);
    fprintf(stdout, "%s rd_newRelfilenodeSubid: %lu\n", indents[indentLevel], reldata->rd_newRelfilenodeSubid);
    fprintf(stdout, "%s rd_id: %d\n", indents[indentLevel], reldata->rd_id);
    fprintf(stdout, "%s rd_oidindex: %d\n", indents[indentLevel], reldata->rd_oidindex);
    fprintf(stdout, "%s rd_toastoid: %d\n", indents[indentLevel], reldata->rd_toastoid);
    fprintf(stdout, "%s parentId: %d\n", indents[indentLevel], reldata->parentId);
    --indentLevel;
}

static void parse_relation_options_struct(char* vardata, int relkind)
{
    fprintf(stdout, "%s Relation Option Struct: [relkink %c] \n", indents[indentLevel], (char)relkind);
    if (RELKIND_RELATION == relkind) {
        ++indentLevel;
        StdRdOptions* stdopt = (StdRdOptions*)vardata;
        fprintf(stdout, "%s fillfactor: %d\n", indents[indentLevel], stdopt->fillfactor);

        fprintf(stdout, "%s autovacuum.enabled: %d\n", indents[indentLevel], stdopt->autovacuum.enabled);
        fprintf(
            stdout, "%s autovacuum.vacuum_threshold: %d\n", indents[indentLevel], stdopt->autovacuum.vacuum_threshold);
        fprintf(stdout,
            "%s autovacuum.analyze_threshold: %d\n",
            indents[indentLevel],
            stdopt->autovacuum.analyze_threshold);
        fprintf(stdout,
            "%s autovacuum.vacuum_cost_delay: %d\n",
            indents[indentLevel],
            stdopt->autovacuum.vacuum_cost_delay);
        fprintf(stdout,
            "%s autovacuum.vacuum_cost_limit: %d\n",
            indents[indentLevel],
            stdopt->autovacuum.vacuum_cost_limit);
        fprintf(stdout, "%s autovacuum.freeze_min_age: %lu\n", indents[indentLevel], stdopt->autovacuum.freeze_min_age);
        fprintf(stdout, "%s autovacuum.freeze_max_age: %lu\n", indents[indentLevel], stdopt->autovacuum.freeze_max_age);
        fprintf(
            stdout, "%s autovacuum.freeze_table_age: %lu\n", indents[indentLevel], stdopt->autovacuum.freeze_table_age);
        fprintf(stdout,
            "%s autovacuum.vacuum_scale_factor: %f\n",
            indents[indentLevel],
            stdopt->autovacuum.vacuum_scale_factor);
        fprintf(stdout,
            "%s autovacuum.analyze_scale_factor: %f\n",
            indents[indentLevel],
            stdopt->autovacuum.analyze_scale_factor);

        fprintf(stdout, "%s security_barrier: %d\n", indents[indentLevel], stdopt->security_barrier);
        fprintf(stdout, "%s max_batch_rows: %d\n", indents[indentLevel], stdopt->max_batch_rows);
        fprintf(stdout, "%s delta_rows_threshold: %d\n", indents[indentLevel], stdopt->delta_rows_threshold);
        fprintf(stdout, "%s partial_cluster_rows: %d\n", indents[indentLevel], stdopt->partial_cluster_rows);
        fprintf(stdout, "%s compresslevel: %d\n", indents[indentLevel], stdopt->compresslevel);
        fprintf(stdout, "%s internalMask: 0x%x\n", indents[indentLevel], stdopt->internalMask);
        fprintf(stdout, "%s ignore_enable_hadoop_env: %d\n", indents[indentLevel], stdopt->ignore_enable_hadoop_env);
        fprintf(stdout, "%s rel_cn_oid: %u\n", indents[indentLevel], stdopt->rel_cn_oid);
        fprintf(stdout, "%s append_mode_internal: %u\n", indents[indentLevel], stdopt->append_mode_internal);

        int optstr_offset = *(int*)&(stdopt->compression);
        fprintf(stdout, "%s compression: %s\n", indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int*)&(stdopt->orientation);
        fprintf(stdout, "%s orientation: %s\n", indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int *)&(stdopt->ttl);
        fprintf(stdout, "%s ttl: %s\n",  indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int *)&(stdopt->period);
        fprintf(stdout, "%s period: %s\n",  indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int*)&(stdopt->version);
        fprintf(stdout, "%s version: %s\n", indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int*)&(stdopt->append_mode);
        fprintf(stdout, "%s append_mode: %s\n", indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int*)&(stdopt->start_ctid_internal);
        fprintf(stdout, "%s start_ctid_internal: %s\n", indents[indentLevel], (vardata + optstr_offset));
        optstr_offset = *(int*)&(stdopt->end_ctid_internal);
        fprintf(stdout, "%s end_ctid_internal: %s\n", indents[indentLevel], (vardata + optstr_offset));

        --indentLevel;
    }
    /*  else can index relation options but won't solve it now*/

}

static void parse_oid_array(Oid* ids, int nids)
{
    indentLevel++;
    fprintf(stdout, "\n%s", indents[indentLevel]);

    int line_break = 0;
    const int oids_of_one_line = 16;
    for (int i = 0; i < nids; ++i) {
        fprintf(stdout, " %u", ids[i]);
        if ((++line_break) == oids_of_one_line) {
            /* 16 OID within each line */
            line_break = 0;
            fprintf(stdout, "\n%s", indents[indentLevel]);
        }
    }

    indentLevel--;
}

static bool parse_internal_init_file(char* filename)
{
    char* readbuf = NULL;
    bool result = true;
    FILE* fp = NULL;
    int attrs_num = 0;
    int relkind = 0;
    int magic = 0;
    Size len;
    size_t nread;

    /* 10MB size enough */
    const int readbuf_size = 1024 * 1024 * 10;
    if (NULL == (readbuf = (char*)malloc(readbuf_size))) {
        fprintf(stderr, "failed to malloc read buffer\n");
        return false;
    }

    fp = fopen(filename, "rb");
    if (fp == NULL) {
        result = false;
        fprintf(stderr, "IO error when opening %s: %s\n", filename, strerror(errno));
        goto read_failed;
    }

    /* check for correct magic number (compatible version) */
    if (fread(&magic, 1, sizeof(magic), fp) != sizeof(magic)) {
        result = false;
        fprintf(stderr, "failed to read magic number of this file\n");
        goto read_failed;
    }

    if (magic != RELCACHE_INIT_FILEMAGIC) {
        result = false;
        fprintf(stderr, "magic number not matched, expected 0x%x, result 0x%x\n", RELCACHE_INIT_FILEMAGIC, magic);
        goto read_failed;
    }

    for (int relno = 0;; relno++) {
        /* first read the relation descriptor length */
        nread = fread(&len, 1, sizeof(len), fp);
        if (nread != sizeof(len)) {
            if (nread == 0) {
                break; /* end of file */
            }
            result = false;
            goto read_failed;
        }

        /* safety check for incompatible relcache layout */
        if (len != sizeof(RelationData)) {
            result = false;
            goto read_failed;
        }

        Assert(len <= readbuf_size);

        /* then, read the Relation structure */
        if (fread(readbuf, 1, len, fp) != len) {
            result = false;
            goto read_failed;
        } else {
            /* format and output Relation structure data */
            parse_relation_data_struct((Relation)readbuf, relno);
        }

        /* next read the relation tuple form */
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
            result = false;
            goto read_failed;
        }
        Assert(len <= readbuf_size);
        if (fread(readbuf, 1, len, fp) != len) {
            result = false;
            goto read_failed;
        } else {
            /* format and output relation tuple data */
            fprintf(stdout, "%s pg_class tuple data >>:\n", indents[2]);
            ParsePgClassTupleData((binary)readbuf, len, NULL, Natts_pg_class);
            attrs_num = ((Form_pg_class)readbuf)->relnatts;
            relkind = ((Form_pg_class)readbuf)->relkind;
        }

        /* next read all the attribute tuple form data entries */
        for (int i = 0; i < attrs_num; i++) {
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
                result = false;
                goto read_failed;
            }
            if (len != ATTRIBUTE_FIXED_PART_SIZE) {
                result = false;
                goto read_failed;
            }

            Assert(len <= readbuf_size);
            if (fread(readbuf, 1, len, fp) != len) {
                result = false;
                goto read_failed;
            } else {
                /* format and output attribute data */
                fprintf(stdout, "%s pg_attribute tuple data #%d:\n", indents[2], i);
                ParsePgAttributeTupleData((binary)readbuf, len, NULL, Natts_pg_attribute);
            }
        }

        /* next read the access method specific field */
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
            result = false;
            goto read_failed;
        }
        if (len > 0) {
            Assert(len <= readbuf_size);
            if (fread(readbuf, 1, len, fp) != len) {
                result = false;
                goto read_failed;
            } else {
                /* format and output relation options */
                parse_relation_options_struct(readbuf, relkind);
            }
            if (len != VARSIZE(readbuf)) {
                result = false;
                goto read_failed;
            }
        }

        /* If it's an index, there's more to do */
        if (relkind == RELKIND_INDEX) {
            /* next, read the pg_index tuple */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
                result = false;
                goto read_failed;
            }

            Assert(len <= readbuf_size);
            if (fread(readbuf, 1, len, fp) != len) {
                result = false;
                goto read_failed;
            } else {
                HeapTuple rd_indextuple = (HeapTuple)readbuf;
                rd_indextuple->t_data = (HeapTupleHeader)((char*)rd_indextuple + HEAPTUPLESIZE);
                Form_pg_index rd_index = (Form_pg_index)GETSTRUCT(rd_indextuple);

                /* Form_pg_index struct */
                fprintf(stdout, "%s pg_index tuple data >>:\n", indents[2]);
                ParsePgIndexTupleData((binary)rd_index, len, NULL, Natts_pg_index);
            }

            /* next, read the access method tuple form */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
                result = false;
                goto read_failed;
            }

            Assert(len <= readbuf_size);
            if (fread(readbuf, 1, len, fp) != len) {
                result = false;
                goto read_failed;
            } else {
                /* Form_pg_am */
                fprintf(stdout, "%s pg_am tuple data >>:\n", indents[2]);
                ParsePgAmTupleData((binary)readbuf, len, NULL, Natts_pg_am);
            }

            /*
             * next:
             *     1. read the vector of opfamily OIDs
             *     2. read the vector of opcintype OIDs
             *     3. read the vector of support procedure OIDs
             *     4. read the vector of collation OIDs
             *     5. read the vector of indoption values
             */
            const char* oids_desc[] = {
                "Opfamily OIDs", "Opcintype OIDs", "Procedure OIDs", "Collation OIDs", "Index Option"};
            for (int kk = 0; kk < (int)(sizeof(oids_desc) / sizeof(oids_desc[0])); kk++) {
                if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
                    result = false;
                    goto read_failed;
                }
                Assert(len <= readbuf_size);
                if (fread(readbuf, 1, len, fp) != len) {
                    result = false;
                    goto read_failed;
                } else {
                    fprintf(stdout, "\n%s%s:", indents[indentLevel], oids_desc[kk]);
                    parse_oid_array((Oid*)readbuf, (len / sizeof(Oid)));
                }
            }
        }
    }

read_failed:

    if (NULL != readbuf) {
        free(readbuf);
    }
    if (NULL != fp) {
        fclose(fp);
    }

    return result;
}

static int ReadOldVersionRelmapFile(char *buffer, FILE *fd)
{
    errno_t rc;
    char oldMapCache[RELMAP_SIZE_OLD];
    int readByte = fread(oldMapCache, 1, RELMAP_SIZE_OLD, fd);
    rc = memcpy_s(buffer, sizeof(RelMapFile), oldMapCache, MAPPING_LEN_OLDMAP_HEAD);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(
        buffer + offsetof(RelMapFile, crc), MAPPING_LEN_TAIL, oldMapCache + MAPPING_LEN_OLDMAP_HEAD, MAPPING_LEN_TAIL);
    securec_check(rc, "\0", "\0");
    return readByte;
}

static void DoRelMapCrcCheck(const RelMapFile* mapfile, int32 magicNum)
{
    pg_crc32 crc;
    /* verify the CRC */
    INIT_CRC32(crc);
    if (IS_NEW_RELMAP(magicNum)) {
        COMP_CRC32(crc, (char*)mapfile, offsetof(RelMapFile, crc));
    } else {
        COMP_CRC32(crc, (char*)mapfile, MAPPING_LEN_OLDMAP_HEAD);
    }
    FIN_CRC32(crc);
    if (!EQ_CRC32(crc, (pg_crc32)(mapfile->crc))) {
        fprintf(stdout, "WARNING: relmap crc check failed!");
    }
}

static int parse_filenodemap_file(char* filename)
{
    char buffer[sizeof(RelMapFile)];
    FILE* fd = NULL;
    long size;
    size_t result;
    RelMapFile* mapfile = NULL;
    char* pg_class_map[MAX_PG_CLASS_ID];
    char* pg_class = NULL;
    int i = 0;
    int32 magicNum;
    int32 relmapSize;
    int32 relmapMaxMappings;

    for (i = 0; i < MAX_PG_CLASS_ID; i++) {
        pg_class_map[i] = NULL;
    }
    fill_filenode_map(pg_class_map);

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    size = ftell(fd);
    rewind(fd);

    result = fread(&magicNum, 1, sizeof(int32), fd);
    rewind(fd);
    if (result != sizeof(int32)) {
        fprintf(stderr, "Reading magic error");
        fclose(fd);
        return false;
    }

    if (IS_NEW_RELMAP(magicNum)) {
        result = fread(buffer, 1, sizeof(RelMapFile), fd);
        relmapSize = RELMAP_SIZE_NEW;
        relmapMaxMappings = MAX_MAPPINGS_4K;
    } else {
        result = ReadOldVersionRelmapFile(buffer, fd);
        relmapSize = RELMAP_SIZE_OLD;
        relmapMaxMappings = MAX_MAPPINGS;
    }
    if ((size_t)relmapSize != result) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    mapfile = (RelMapFile*)buffer;
    DoRelMapCrcCheck(mapfile, magicNum);
    fprintf(stdout, "Magic number: 0x%x\n", mapfile->magic);
    fprintf(stdout, "Number of mappings: %u\n", mapfile->num_mappings);
    fprintf(stdout, "Mapping pairs:\n");
    for (i = 0; i < relmapMaxMappings; i++) {
        if (0 == mapfile->mappings[i].mapoid && 0 == mapfile->mappings[i].mapfilenode)
            break;

        if (mapfile->mappings[i].mapoid > MAX_PG_CLASS_ID)
            fprintf(stderr,
                "the oid(%u) of catalog is bigger than MAX_PG_CLASS_ID(%d)\n",
                mapfile->mappings[i].mapoid,
                MAX_PG_CLASS_ID);

        pg_class = pg_class_map[mapfile->mappings[i].mapoid];
        fprintf(stdout,
            "\t[%d] relid: %u, relfilenode: %u, catalog: %s\n",
            i,
            mapfile->mappings[i].mapoid,
            mapfile->mappings[i].mapfilenode,
            pg_class);
    }
    fclose(fd);

    return true;
}

/* parse a cu data hearder for cu file */
static int parse_cu_file(char* filename, uint64 offset)
{
    char fullpath[1024];
    FILE* fd = NULL;
    int seg_num = 0;
    int seg_offset = 0;
    char buffer[BLCKSZ];
    size_t result;
    int pos = 0;

    /* cu header */
    int crc = 0;
    uint32 magic = 0;
    uint16 infoMode = 0;
    uint16 bpNullCompressedSize = 0;
    uint32 srcDataSize = 0;
    int cmprDataSize = 0;

    seg_num = offset / (RELSEG_SIZE * BLCKSZ);
    seg_offset = offset % (RELSEG_SIZE * BLCKSZ);

    errno_t rc = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, "%s.%d", filename, seg_num);
    securec_check_ss_c(rc, "\0", "\0");

    fd = fopen(fullpath, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", fullpath, strerror(errno));
        return false;
    }

    fseek(fd, seg_offset, SEEK_SET);

    result = fread(buffer, 1, BLCKSZ, fd);
    if (result != BLCKSZ) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    fprintf(stdout, "CU file name:                                      %s\n", fullpath);
    fprintf(stdout, "CU data offset:                                    %lu\n", offset);
    fprintf(stdout, "CU file offset:                                    %d\n", seg_offset);

    crc = *(int32*)(buffer + pos);
    pos += sizeof(crc);
    fprintf(stdout, "CU data crc:                                       0x%X\n", crc);

    magic = *(uint32*)(buffer + pos);
    pos += sizeof(magic);
    fprintf(stdout, "CU data magic:                                     %u\n", magic);

    infoMode = *(int16*)(buffer + pos);
    pos += sizeof(infoMode);
    fprintf(stdout, "CU data infoMode:                                  0x%X\n", infoMode);

    if (infoMode & CU_HasNULL) {
        bpNullCompressedSize = *(int16*)(buffer + pos);
        pos += sizeof(bpNullCompressedSize);
        fprintf(stdout, "CU data infoMode has NULL, NullCompressedSize:     %d\n", bpNullCompressedSize);
    } else
        fprintf(stdout, "CU data infoMode has not NULL, NullCompressedSize: %d\n", bpNullCompressedSize);

    srcDataSize = *(int32*)(buffer + pos);
    pos += sizeof(srcDataSize);
    fprintf(stdout, "CU data srcDataSize:                               %d\n", srcDataSize);

    cmprDataSize = *(int*)(buffer + pos);
    pos += sizeof(cmprDataSize);
    fprintf(stdout, "CU data cmprDataSize:                              %d\n", cmprDataSize);

    fclose(fd);

    return true;
}

/* parse a replslot file */
static int parse_slot_file(char* filename)
{
    const uint32 upperLen = 32;
    FILE* fd = NULL;
    ReplicationSlotOnDisk cp;
    size_t readBytes = 0;
    pg_crc32 checksum = 0;

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    /* read part of statefile that's guaranteed to be version independent */
    readBytes = fread(&cp, 1, ReplicationSlotOnDiskConstantSize, fd);
    if (readBytes != ReplicationSlotOnDiskConstantSize) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    /* verify magic */
    if (cp.magic != SLOT_MAGIC) {
        fprintf(stderr, "wrong magic");
        fclose(fd);
        return false;
    }

    /* verify version */
    if (cp.version != SLOT_VERSION) {
        fprintf(stderr, "unsupported version");
        fclose(fd);
        return false;
    }

    /* boundary check on length */
    if (cp.length != ReplicationSlotOnDiskDynamicSize) {
        fprintf(stderr, "corrupted length");
        fclose(fd);
        return false;
    }

    readBytes = fread((char*)&cp + ReplicationSlotOnDiskConstantSize, 1, cp.length, fd);
    if (readBytes != cp.length) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    /* now verify the CRC32C */
    /* using CRC32C since V1R8C10 */
    INIT_CRC32C(checksum);
    COMP_CRC32C(checksum, (char*)&cp + ReplicationSlotOnDiskConstantSize, ReplicationSlotOnDiskDynamicSize);
    FIN_CRC32C(cp.checksum);

    if (!EQ_CRC32C(checksum, cp.checksum)) {
        fprintf(stderr, "slot crc error");
        fclose(fd);
        return false;
    }

    /* print slot */
    fprintf(stdout, "slot name:      %s\n", cp.slotdata.name.data);

    fprintf(stdout, "magic:          %u\n", cp.magic);
    fprintf(stdout, "checksum:       %u\n", cp.checksum);
    fprintf(stdout, "version:        %u\n", cp.version);
    fprintf(stdout, "length:         %u\n", cp.length);

    fprintf(stdout, "database oid:   %u\n", cp.slotdata.database);
    fprintf(stdout, "isDummyStandby: %d\n", cp.slotdata.isDummyStandby);

    fprintf(stdout, "xmin:           %lu\n", cp.slotdata.xmin);
    fprintf(
        stdout, "restart_lsn:    %X/%X\n", (uint32)(cp.slotdata.restart_lsn >> upperLen),
        (uint32)(cp.slotdata.restart_lsn));
    fprintf(
        stdout, "confirmed_flush:    %X/%X\n", (uint32)(cp.slotdata.confirmed_flush >> upperLen),
        (uint32)(cp.slotdata.confirmed_flush));
    fclose(fd);

    return true;
}

/* parse gaussdb.state file */
static int parse_gaussdb_state_file(char* filename)
{
    FILE* fd = NULL;
    GaussState state;
    size_t readBytes = 0;
    char* ServerModeStr[] = {"unknown", "normal", "primary", "standby", "pending"};
    char* DbStateStr[] = {"unknown",
        "normal",
        "needrepair",
        "starting",
        "waiting",
        "demoting",
        "promoting",
        "building",
        "Catchup",
        "Coredump"};
    char* HaRebuildReasonStr[] = {"none", "walsegment", "connect", "timeline", "systemid", "version", "mode"};
    char* BuildModeStr[] = {"node", "auto", "full", "incremental"};
    XLogRecPtr lsn;
    uint32 term;
    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    readBytes = fread(&state, 1, sizeof(GaussState), fd);
    if (readBytes != sizeof(GaussState)) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    lsn = state.lsn;
    term = state.term;

    fprintf(stdout, "gaussdb.state info: \n");
    fprintf(stdout, "ServerMode:        %s\n", ServerModeStr[state.mode]);
    fprintf(stdout, "conn_num:          %d\n", state.conn_num);
    fprintf(stdout, "DbState:           %s\n", DbStateStr[state.state]);
    fprintf(stdout, "sync_stat:         %d\n", state.sync_stat);
    fprintf(stdout, "lsn:               %X/%X\n", (uint32)(lsn >> 32), (uint32)lsn);
    fprintf(stdout, "term:              %u\n", term);
    fprintf(stdout, "ha_rebuild_reason: %s\n", HaRebuildReasonStr[state.ha_rebuild_reason]);
    fprintf(stdout, "Build mode:        %s\n", BuildModeStr[state.build_info.build_mode]);
    fprintf(stdout, "Build total done:  %lu\n", state.build_info.total_done);
    fprintf(stdout, "Build total szie:  %lu\n", state.build_info.total_size);
    fprintf(stdout, "Build schedule:    %d%%\n", state.build_info.process_schedule);
    if (state.build_info.estimated_time == -1)
        fprintf(stdout, "Build remain time: %s\n", "--:--:--");
    else
        fprintf(stdout,
            "Build remain time: %.2d:%.2d:%.2d\n",
            state.build_info.estimated_time / S_PER_H,
            (state.build_info.estimated_time % S_PER_H) / S_PER_MIN,
            (state.build_info.estimated_time % S_PER_H) % S_PER_MIN);

    fclose(fd);

    return true;
}

/* parse pg_control file */
static int parse_pg_control_file(char* filename)
{
    FILE* fd = NULL;
    ControlFileData ControlFile;
    size_t readBytes = 0;
    char* wal_level_str[] = {"minimal", "archive", "hot_standby"};
    char* db_state_str[] = {"starting up",
        "shut down",
        "shut down in recovery",
        "shutting down",
        "in crash recovery",
        "in archive recovery",
        "in production"};
    pg_crc32c crc;
    time_t time_tmp;
    char pgctime_str[128];
    char ckpttime_str[128];
    char sysident_str[32];
    const char* strftime_fmt = "%c";

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    readBytes = fread(&ControlFile, 1, sizeof(ControlFileData), fd);
    if (readBytes != sizeof(ControlFileData)) {
        fprintf(stderr, "Reading error");
        fclose(fd);
        return false;
    }

    /* Check the CRC. */
    /* using CRC32C since V1R8C10 */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)&ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);
    if (!EQ_CRC32C(crc, ControlFile.crc)) {
        fprintf(stderr, "pg_control crc error");
        fclose(fd);
        return false;
    }

    time_tmp = (time_t)ControlFile.time;
    strftime(pgctime_str, sizeof(pgctime_str), strftime_fmt, localtime(&time_tmp));
    time_tmp = (time_t)ControlFile.checkPointCopy.time;
    strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt, localtime(&time_tmp));

    /*
     * Format system_identifier separately to keep platform-dependent format
     * code out of the translatable message string.
     */
    errno_t rc = snprintf_s(sysident_str, sizeof(sysident_str), sizeof(sysident_str) - 1, UINT64_FORMAT,
        ControlFile.system_identifier);
    securec_check_ss_c(rc, "\0", "\0");

    fprintf(stdout, "pg_control version number:            %u\n", ControlFile.pg_control_version);

    if (ControlFile.pg_control_version % 65536 == 0 && ControlFile.pg_control_version / 65536 != 0)
        fprintf(stdout,
            "WARNING: possible byte ordering mismatch\n"
            "The byte ordering used to store the pg_control file might not match the one\n"
            "used by this program.  In that case the results below would be incorrect, and\n"
            "the PostgreSQL installation would be incompatible with this data directory.\n");
    fprintf(stdout, "Catalog version number:               %u\n", ControlFile.catalog_version_no);
    fprintf(stdout, "Database system identifier:           %s\n", sysident_str);
    fprintf(stdout, "Database cluster state:               %s\n", db_state_str[ControlFile.state]);
    fprintf(stdout, "pg_control last modified:             %s\n", pgctime_str);
    fprintf(stdout,
        "Latest checkpoint location:           %X/%X\n",
        (uint32)(ControlFile.checkPoint >> 32),
        (uint32)ControlFile.checkPoint);
    fprintf(stdout,
        "Prior checkpoint location:            %X/%X\n",
        (uint32)(ControlFile.prevCheckPoint >> 32),
        (uint32)ControlFile.prevCheckPoint);
    fprintf(stdout,
        "Latest checkpoint's REDO location:    %X/%X\n",
        (uint32)(ControlFile.checkPointCopy.redo >> 32),
        (uint32)ControlFile.checkPointCopy.redo);
    fprintf(stdout, "Latest checkpoint's TimeLineID:       %u\n", ControlFile.checkPointCopy.ThisTimeLineID);
    fprintf(
        stdout, "Latest checkpoint's full_page_writes: %s\n", ControlFile.checkPointCopy.fullPageWrites ? "on" : "off");
    fprintf(stdout, "Latest checkpoint's NextXID:          %lu\n", ControlFile.checkPointCopy.nextXid);
    fprintf(stdout, "Latest checkpoint's NextOID:          %u\n", ControlFile.checkPointCopy.nextOid);
    fprintf(stdout, "Latest checkpoint's NextMultiXactId:  %lu\n", ControlFile.checkPointCopy.nextMulti);
    fprintf(stdout, "Latest checkpoint's NextMultiOffset:  %lu\n", ControlFile.checkPointCopy.nextMultiOffset);
    fprintf(stdout, "Latest checkpoint's oldestXID:        %lu\n", ControlFile.checkPointCopy.oldestXid);
    fprintf(stdout, "Latest checkpoint's oldestXID's DB:   %u\n", ControlFile.checkPointCopy.oldestXidDB);
    fprintf(stdout, "Latest checkpoint's oldestActiveXID:  %lu\n", ControlFile.checkPointCopy.oldestActiveXid);
    fprintf(stdout, "Time of latest checkpoint:            %s\n", ckpttime_str);
    fprintf(stdout,
        "Minimum recovery ending location:     %X/%X\n",
        (uint32)(ControlFile.minRecoveryPoint >> 32),
        (uint32)ControlFile.minRecoveryPoint);
    fprintf(stdout,
        "Backup start location:                %X/%X\n",
        (uint32)(ControlFile.backupStartPoint >> 32),
        (uint32)ControlFile.backupStartPoint);
    fprintf(stdout,
        "Backup end location:                  %X/%X\n",
        (uint32)(ControlFile.backupEndPoint >> 32),
        (uint32)ControlFile.backupEndPoint);
    fprintf(stdout, "End-of-backup record required:        %s\n", ControlFile.backupEndRequired ? "yes" : "no");
    fprintf(stdout, "Current wal_level setting:            %s\n", wal_level_str[(WalLevel)ControlFile.wal_level]);
    fprintf(stdout, "Current max_connections setting:      %d\n", ControlFile.MaxConnections);
    fprintf(stdout, "Current max_prepared_xacts setting:   %d\n", ControlFile.max_prepared_xacts);
    fprintf(stdout, "Current max_locks_per_xact setting:   %d\n", ControlFile.max_locks_per_xact);
    fprintf(stdout, "Maximum data alignment:               %u\n", ControlFile.maxAlign);
    /* we don't print floatFormat since can't say much useful about it */
    fprintf(stdout, "Database block size:                  %u\n", ControlFile.blcksz);
    fprintf(stdout, "Blocks per segment of large relation: %u\n", ControlFile.relseg_size);
    fprintf(stdout, "WAL block size:                       %u\n", ControlFile.xlog_blcksz);
    fprintf(stdout, "Bytes per WAL segment:                %u\n", ControlFile.xlog_seg_size);
    fprintf(stdout, "Maximum length of identifiers:        %u\n", ControlFile.nameDataLen);
    fprintf(stdout, "Maximum columns in an index:          %u\n", ControlFile.indexMaxKeys);
    fprintf(stdout, "Maximum size of a TOAST chunk:        %u\n", ControlFile.toast_max_chunk_size);
    fprintf(stdout,
        "Date/time type storage:               %s\n",
        (ControlFile.enableIntTimes ? "64-bit integers" : "floating-point numbers"));
    fprintf(
        stdout, "Float4 argument passing:              %s\n", (ControlFile.float4ByVal ? "by value" : "by reference"));
    fprintf(
        stdout, "Float8 argument passing:              %s\n", (ControlFile.float8ByVal ? "by value" : "by reference"));
    fprintf(stdout, "Database system TimeLine:             %u\n", ControlFile.timeline);

    fclose(fd);
    return true;
}

/* parse clog file */
static int parse_clog_file(char* filename)
{
    FILE* fd = NULL;
    int segnum = 0;
    /* one segment file has 8k*8bit/2*32 xids */
    uint32 segnum_xid = BLCKSZ * CLOG_XACTS_PER_BYTE * SLRU_PAGES_PER_SEGMENT;
    /* the first xid number of current segment file */
    TransactionId xid = 0;
    int nread = 0;
    int byte_index = 0;
    int bit_index = 0;
    int bshift = 0;
    char* byteptr = NULL;
    int status;
    char* xid_status_name[] = {"IN_PROGRESS", "COMMITTED", "ABORTED", "SUB_COMMITTED"};
    char buffer[8192];

    if (!HexStringToInt(filename, &segnum)) {
        fprintf(stderr, "%s input error \n", filename);
        return false;
    }

    xid = (uint64)segnum * segnum_xid;

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    while ((nread = fread(buffer, 1, BLCKSZ, fd)) != 0) {
        if (nread < 0) {
            fprintf(stderr, "read file error!\n");
            fclose(fd);
            return false;
        }

        while (byte_index < nread) {
            byteptr = buffer + byte_index;

            for (bit_index = 0; bit_index < 4; bit_index++) {
                bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
                status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;
                fprintf(stdout, "xid %lu, status: %s\n", xid, xid_status_name[status]);
                xid++;
            }

            byte_index++;
        }

        byte_index = 0;
    }

    fclose(fd);
    return true;
}

/* parse subtran file */
static int parse_csnlog_file(char* filename)
{
    FILE* fd = NULL;
    int segnum = 0;
    /* one segment file has 1k*2048 xids */
    uint32 segnum_xid = CSNLOG_XACTS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
    /* One page has 1k xids */
    int page_xid = BLCKSZ / sizeof(CommitSeqNo);
    /* the first xid number of current segment file */
    TransactionId xid = 0;
    CommitSeqNo csn = 0;
    int nread = 0;
    int entry = 0;
    char buffer[8192];

    if (!HexStringToInt(filename, &segnum)) {
        fprintf(stderr, "%s input error \n", filename);
        return false;
    }

    xid = (uint64)segnum * segnum_xid;

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", filename, gs_strerror(errno));
        return false;
    }

    while ((nread = fread(buffer, 1, BLCKSZ, fd)) != 0) {
        if (nread != BLCKSZ) {
            fprintf(stderr, "read file error!\n");
            fclose(fd);
            return false;
        }

        while (entry < page_xid) {
            csn = *(CommitSeqNo*)(buffer + (entry * sizeof(CommitSeqNo)));
            fprintf(stdout, "xid %lu, csn: %lu\n", xid, csn);
            xid++;
            entry++;
        }

        entry = 0;
    }

    fclose(fd);
    return true;
}

static bool parse_dw_file_head(char* file_head, dw_file_head_t* saved_file_head, int size = 0)
{
    uint32 i;
    uint16 id;
    dw_file_head_t *curr_head, *working_head;
    working_head = NULL;
    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t*)(file_head + (id * sizeof(dw_file_head_t)));
        if (dw_verify_file_head(curr_head)) {
            working_head = curr_head;
            fprintf(stdout, "working head id %u\n", id);
            break;
        }
    }

    if (working_head == NULL) {
        fprintf(stderr, "file head broken\n");
        return false;
    }
    fprintf(stdout, "file_head[dwn %u, start %u]\n", working_head->head.dwn, working_head->start);
    *saved_file_head = *working_head;
    return true;
}

static bool read_batch_pages(
    FILE* fd, dw_batch_t* curr_head, uint16 reading_pages, uint16* read_pages, uint16* file_page_id)
{
    size_t result;
    char* read_buf = ((char*)curr_head + *read_pages * BLCKSZ);
    if (reading_pages > 0) {
        /* skip the read pages */
        result = fread(read_buf, BLCKSZ, reading_pages, fd);
        if (result != reading_pages) {
            fprintf(stderr, "read batch failed %s\n", strerror(errno));
            return false;
        }
        *read_pages += reading_pages;
        *file_page_id += reading_pages;
    }
    Assert(curr_head->head.page_id + *read_pages == *file_page_id);
    Assert(*read_pages >= curr_head->page_num + DW_EXTRA_FOR_ONE_BATCH);
    return true;
}

static bool verify_batch_continous(dw_batch_t* curr_head, uint16 dwn)
{
    dw_batch_t* curr_tail = dw_batch_tail_page(curr_head);
    if (dw_verify_page(curr_head) && dw_verify_page(curr_tail)) {
        if (curr_head->head.dwn + 1 == curr_tail->head.dwn) {
            fprintf(stdout,
                "double write file truncated: batch_head[page_id %u, dwn %u, page_num %u], "
                "batch_tail[page_id %u, dwn %u, page_num %u]\n",
                curr_head->head.page_id,
                curr_head->head.dwn,
                curr_head->page_num,
                curr_tail->head.page_id,
                curr_tail->head.dwn,
                curr_tail->page_num);
        }
    } else if (!dw_verify_batch(curr_head, dwn)) {
        if (dw_verify_page(curr_head) && (curr_head->page_num == 0 || curr_head->head.page_id == DW_BATCH_FILE_START)) {
            fprintf(stdout,
                "no double write pages since last ckpt or file full: "
                "batch_head[page_id %u, dwn %u, page_num %u]\n",
                curr_head->head.page_id,
                curr_head->head.dwn,
                curr_head->page_num);
        } else {
            fprintf(stdout,
                "double write batch broken: batch_head[page_id %u, dwn %u, page_num %u], "
                "batch_tail[page_id %u, dwn %u, page_num %u]\n",
                curr_head->head.page_id,
                curr_head->head.dwn,
                curr_head->page_num,
                curr_tail->head.page_id,
                curr_tail->head.dwn,
                curr_tail->page_num);
        }
        return false;
    }
    return true;
}

static uint16 parse_batch_data_pages(dw_batch_t* curr_head, uint16 page_num)
{
    uint16 i;
    char* page_start;
    BufferTag* buf_tag;
    BufferTagFirstVer* bufTagOrig;
    bool isHashbucket = ((curr_head->page_num & IS_HASH_BKT_SEGPAGE_MASK) != 0);

    fprintf(stdout,
        "batch_head[page_id %u, dwn %u, page_num %u]\n",
        curr_head->head.page_id,
        curr_head->head.dwn,
        GET_REL_PGAENUM(curr_head->page_num));
    page_start = (char*)curr_head + BLCKSZ;
    page_num--;
    if (curr_head->buftag_ver == HASHBUCKET_TAG) {
        isHashbucket = true;
    }
    for (i = 0; i < GET_REL_PGAENUM(curr_head->page_num) && page_num > 0; i++, page_num--) {
        if (isHashbucket) {
            buf_tag = &curr_head->buf_tag[i];
            fprintf(stdout,
                "buf_tag[rel %u/%u/%u/%d blk %u fork %d]\n",
                buf_tag->rnode.spcNode,
                buf_tag->rnode.dbNode,
                buf_tag->rnode.relNode,
                buf_tag->rnode.bucketNode,
                buf_tag->blockNum,
                buf_tag->forkNum);
            (void)parse_a_page(page_start, buf_tag->blockNum, buf_tag->blockNum % RELSEG_SIZE, SEG_UNKNOWN);
        } else {
            bufTagOrig = &((dw_batch_first_ver *)curr_head)->buf_tag[i];
            fprintf(stdout,
                "buf_tag[rel %u/%u/%u blk %u fork %d]\n",
                bufTagOrig->rnode.spcNode,
                bufTagOrig->rnode.dbNode,
                bufTagOrig->rnode.relNode,
                bufTagOrig->blockNum,
                bufTagOrig->forkNum);
            (void)parse_a_page(page_start, bufTagOrig->blockNum, bufTagOrig->blockNum % RELSEG_SIZE, SEG_UNKNOWN);
        }
        page_start += BLCKSZ;
    }
    return page_num;
}

static uint16 calc_reading_pages(dw_batch_t** curr_head, char* start_buf, uint16 read_pages, uint16 file_page_id,
    uint16 dw_batch_page_num)
{
    uint16 buf_page_id;
    errno_t rc;
    dw_batch_t* batch_head = *curr_head;
    int readingPages = (GET_REL_PGAENUM(batch_head->page_num) + DW_EXTRA_FOR_ONE_BATCH) - read_pages;
    /* next batch is already im memory, no need read */
    if (readingPages <= 0) {
        readingPages = 0;
    } else {
        /* buffer capacity less than uint16, cast to shorter is safe */
        buf_page_id = (uint16)dw_page_distance(batch_head, start_buf);
        /* buf size not enough, move the read pages to the start_buf */
        /* all 3 variable less than (DW_BUF_MAX * 2), sum not overflow uint16 */
        if (buf_page_id + read_pages + readingPages >= DW_BUF_MAX) {
            rc = memmove_s(start_buf, (read_pages * BLCKSZ), batch_head, (read_pages * BLCKSZ));
            securec_check(rc, "\0", "\0");
            *curr_head = (dw_batch_t*)start_buf;
        }
    }

    Assert((char*)(*curr_head) + (read_pages + readingPages) * BLCKSZ <= start_buf + DW_BUF_MAX * BLCKSZ);
    Assert(file_page_id + read_pages + readingPages <= dw_batch_page_num);
    return (uint16)readingPages;
}

static void parse_dw_batch(char* buf, FILE* fd, dw_file_head_t* file_head, uint16 page_num, uint16 dw_batch_page_num)
{
    uint16 file_page_id, read_pages;
    uint16 reading_pages;
    uint32 flush_pages;
    char* start_buf = NULL;
    dw_batch_t* curr_head = NULL;

    fseek(fd, (file_head->start * BLCKSZ), SEEK_SET);

    file_page_id = file_head->start;
    start_buf = buf;
    curr_head = (dw_batch_t*)start_buf;
    read_pages = 0;
    reading_pages = Min(DW_BATCH_MAX_FOR_NOHBK, (dw_batch_page_num - file_page_id));
    flush_pages = 0;

    for (;;) {
        if (!read_batch_pages(fd, curr_head, reading_pages, &read_pages, &file_page_id)) {
            return;
        }

        /* the first read may get an empty batch */
        /* but we don't know until read it */
        if (curr_head->page_num == 0) {
            fprintf(stdout, "no double write pages since last ckpt or file full\n");
            break;
        }
        if (!verify_batch_continous(curr_head, file_head->head.dwn)) {
            break;
        }

        page_num = parse_batch_data_pages(curr_head, page_num);
        if (page_num == 0) {
            break;
        }

        /* discard the first batch. including head page and data pages */
        flush_pages += (1 + GET_REL_PGAENUM(curr_head->page_num));
        read_pages -= (1 + GET_REL_PGAENUM(curr_head->page_num));
        curr_head = dw_batch_tail_page(curr_head);
        if (GET_REL_PGAENUM(curr_head->page_num) == 0) {
            fprintf(stdout, "double write batch parsed: pages %u\n", flush_pages);
            break;
        }

        reading_pages = calc_reading_pages(&curr_head, start_buf, read_pages, file_page_id, dw_batch_page_num);
    }
}

static bool parse_dw_file(const char* file_name, uint32 start_page, uint32 page_num)
{
    errno_t rc;
    FILE* fd;
    size_t result;
    uint32 dw_batch_page_num;
    dw_file_head_t file_head;
    char* meta_name;
    char meta_full_path[PATH_MAX];
    char meta_name_tmp[PATH_MAX];
    dw_batch_meta_file* batch_meta_file;
    char* meta_buf = NULL;
    char* dw_buf = NULL;

    /* copy the full path of dw file to meta_full_path */
    if (realpath(file_name, meta_full_path) == NULL && file_name[0] == '\0') {
        fprintf(stderr, "could not get correct path or the absolute path is too long!\n");
        return false;
    }

    /* extract the path dir of dw file, which is the dir of meta file */
    (void)dirname(meta_full_path);

    /* extract the meta name from DW_META_FILE */
    rc = strcpy_s(meta_name_tmp, PATH_MAX, T_DW_META_FILE);
    securec_check(rc, "", "");
    meta_name = basename(meta_name_tmp);

    /* fetch the full meta path with above two parts */
    rc = strcat_s(meta_full_path, PATH_MAX, "/");
    securec_check(rc, "", "");
    rc = strcat_s(meta_full_path, PATH_MAX, meta_name);
    securec_check(rc, "", "");

    fd = fopen(meta_full_path, "rb+");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", meta_full_path, strerror(errno));
        return false;
    }

    meta_buf = (char*)malloc(DW_META_FILE_BLOCK_NUM * BLCKSZ);
    if (meta_buf == NULL) {
        fclose(fd);
        fprintf(stderr, "out of memory\n");
        return false;
    }

    result = fread(meta_buf, sizeof(dw_batch_meta_file), 1, fd);
    if (result != 1) {
        free(meta_buf);
        fclose(fd);
        fprintf(stderr, "read %s: %s\n", meta_full_path, strerror(errno));
        return false;
    }

    batch_meta_file = (dw_batch_meta_file *) meta_buf;
    dw_batch_page_num = (uint32) (DW_FILE_SIZE_UNIT * batch_meta_file->dw_file_size / BLCKSZ);

    free(meta_buf);
    fclose(fd);

    fd = fopen(file_name, "rb+");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", file_name, strerror(errno));
        return false;
    }

    dw_buf = (char*)malloc(BLCKSZ * DW_BUF_MAX_FOR_NOHBK);
    if (dw_buf == NULL) {
        fclose(fd);
        fprintf(stderr, "out of memory\n");
        return false;
    }

    result = fread(dw_buf, BLCKSZ, 1, fd);
    if (result != 1) {
        free(dw_buf);
        fclose(fd);
        fprintf(stderr, "read %s: %s\n", file_name, strerror(errno));
        return false;
    }
    if (!parse_dw_file_head(dw_buf, &file_head, BLCKSZ)) {
        free(dw_buf);
        fclose(fd);
        return false;
    }
    if (start_page != 0) {
        if (start_page >= dw_batch_page_num) {
            fprintf(stdout, "start_page %u exceeds the double write file upper limit offset %u\n",
                start_page, dw_batch_page_num - 1);
            free(dw_buf);
            fclose(fd);
            return false;
        }
        file_head.start = (uint16)start_page;

        if (page_num != 0) {
            fprintf(stdout, "start_page and page_num are set, hacking from %u, num %u\n", start_page, page_num);
        } else {
            fprintf(stdout, "start_page is set, page_num not set, hacking from %u, until end\n", start_page);
        }
    }
    if (page_num == 0) {
        page_num = dw_batch_page_num - start_page;
    }

    parse_dw_batch(dw_buf, fd, &file_head, (uint16)page_num, (uint16)dw_batch_page_num);

    free(dw_buf);
    fclose(fd);
    return true;
}

/* forkNames in catalog.cpp can not be linked. Thus we define a new ForkNames here */
static const char* ForkNames[] = {
    "main", /* MAIN_FORKNUM */
    "fsm",  /* FSM_FORKNUM */
    "vm",   /* VISIBILITYMAP_FORKNUM */
    "bcm",  /* BCM_FORKNUM */
    "init"  /* INIT_FORKNUM */
};
static bool parse_normal_table(SegmentHead *seg_head, BlockNumber head)
{
    fprintf(stdout,
        "Normal segment table (Head %u), nblocks: %u, nextents: %u, total "
        "blocks: %u\n",
        head, seg_head->nblocks, seg_head->nextents, seg_head->total_blocks);

    for (int i = 0; i < int(SEGMENT_MAX_FORKNUM); i++) {
        BlockNumber fhead = seg_head->fork_head[i];
        if (BlockNumberIsValid(fhead)) {
            fprintf(stdout, "\tFork (%s) segment head: %u\n", ForkNames[i], fhead);
        } else {
            fprintf(stdout, "\tFork (%s) segment head: NIL\n", ForkNames[i]);
        }
    }

    fprintf(stdout, "Extents (level 0):\n");
    for (int i = 0; i < (int)seg_head->nextents && i < BMT_HEADER_LEVEL0_SLOTS; i++) {
        BlockNumber ext_st = seg_head->level0_slots[i];
        fprintf(stdout, "\t%u-%u\n", ext_st, ext_st + ExtentSizeByCount(i));
    }

    for (uint32 i = 0; i < seg_head->nextents; i++) {
        if (RequireNewLevel0Page(i)) {
            int id = ExtentIdToLevel1Slot(i);
            fprintf(stdout, "Extents (level 1 slots): %u\n", seg_head->level1_slots[id]);
        }
    }

    return true;
}

static bool parse_bucket_table(char *head_buf, BlockNumber head, int fd, char *filename)
{
    fprintf(stdout, "Bucket segment table (Head %u), LSN (%X/%X)\n", head, (uint32)(PageGetLSN(head_buf) >> 32),
            (uint32)PageGetLSN(head_buf));
    BktMainHead *bkt_head = (BktMainHead *)PageGetContents(head_buf);
    char *buf = (char *)malloc(BLCKSZ);
    if (buf == NULL) {
        fprintf(stderr, "out of memory\n");
        return false;
    }

    uint32 total_segments = 0;
    for (uint32 i = 0; i < BktMapBlockNumber; i++) {
        BlockNumber map_blocknum = bkt_head->bkt_map[i];
        off_t offset = 1LLU * map_blocknum * BLCKSZ;
        offset = offset % (RELSEG_SIZE * BLCKSZ);
        ssize_t nread = pread(fd, buf, BLCKSZ, offset);
        if (nread != BLCKSZ) {
            fprintf(stderr, "Failed to read %s: %s\n", filename, strerror(errno));
            free(buf);
            return false;
        }

        BktHeadMapBlock *mapblock = (BktHeadMapBlock *)PageGetContents(buf);
        for (uint32 j = 0; j < BktMapEntryNumberPerBlock; j++) {
            if (mapblock->head_block[j] != InvalidBlockNumber) {
                uint32 index = total_segments + j;
                int fork = index / MAX_BUCKETMAPLEN;
                int bktNode = index % MAX_BUCKETMAPLEN;
                fprintf(stdout, "\t\t\tBucket Node (%d, %s) Segment head: %u\n", bktNode, ForkNames[fork],
                        mapblock->head_block[j]);
            }
        }

        total_segments += BktMapEntryNumberPerBlock;
    }
    free(buf);
    return true;
}

static bool parse_segment_head(char *filename, uint32 start_page)
{
    char *buf = (char*) malloc(BLCKSZ);
    if (buf == NULL) {
        fprintf(stderr, "out of memory\n");
        return false;
    }

    bool result;
    int fd = open(filename, O_RDONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", filename, strerror(errno));
        free(buf);
        return false;
    }

    off_t offset = 1LLU * start_page * BLCKSZ;
    ssize_t nread = pread(fd, buf, BLCKSZ, offset);
    if (nread != BLCKSZ) {
        fprintf(stderr, "Failed to read %s: %s\n", filename, strerror(errno));
        free(buf);
        close(fd);
        return false;
    }

    SegmentHead *seg_head = (SegmentHead *)PageGetContents(buf);
    switch (seg_head->magic) {
        case SEGMENT_HEAD_MAGIC:
            result = parse_normal_table(seg_head, start_page);
            break;
        case BUCKET_SEGMENT_MAGIC:
            result = parse_bucket_table(buf, start_page, fd, filename);
            break;
        default:
            result = false;
            break;
    }

    free(buf);
    close(fd);
    return result;
}

static bool dw_verify_item(const dw_single_flush_item* item, uint16 dwn)
{
    if (item->dwn != dwn) {
        return false;
    }

    pg_crc32c crc;
    /* Contents are protected with a CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)item, offsetof(dw_single_flush_item, crc));
    FIN_CRC32C(crc);

    if (EQ_CRC32C(crc, item->crc)) {
        return true;
    } else {
        return false;
    }
}

static bool parse_dw_single_flush_file(const char* file_name)
{
    FILE* fd;
    size_t result;
    dw_single_flush_item *item = (dw_single_flush_item*)malloc(sizeof(dw_single_flush_item) *
        DW_SECOND_DATA_PAGE_NUM);
    uint16 blk_num = DW_SECOND_BUFTAG_PAGE_NUM;
    char *unaligned_buf = (char *)malloc((blk_num + 1 + 1 + 1) * BLCKSZ);
    char *buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    char *file_head = buf;
    buf = buf + BLCKSZ;
    char *second_file_head = buf;
    buf = buf + BLCKSZ;
    char *item_buf = buf;
    char *unaligned_buf2 = (char *)malloc(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *dw_block = (char *)TYPEALIGN(BLCKSZ, unaligned_buf2);
    PageHeader pghr = NULL;

    fd = fopen(file_name, "rb");
    if (fd == NULL) {
        fprintf(stderr, "%s: %s\n", file_name, gs_strerror(errno));
        free(item);
        free(unaligned_buf);
        free(unaligned_buf2);
        return false;
    }

    /* read all buffer tag item, need skip head page */
    fseek(fd, 0, SEEK_SET);
    result = fread(file_head, 1, BLCKSZ, fd);
    if (BLCKSZ != result) {
        fprintf(stderr, "Reading error");
        free(item);
        free(unaligned_buf);
        free(unaligned_buf2);
        fclose(fd);
        return false;
    }

    fseek(fd, (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ, SEEK_SET);
    result = fread(second_file_head, 1, BLCKSZ, fd);

    fseek(fd, (1 + DW_FIRST_DATA_PAGE_NUM + 1) * BLCKSZ, SEEK_SET);
    result = fread(item_buf, 1, blk_num * BLCKSZ, fd);
    if (blk_num * BLCKSZ != result) {
        fprintf(stderr, "Reading error");
        free(item);
        free(unaligned_buf);
        free(unaligned_buf2);
        fclose(fd);
        return false;
    }

    int offset = 0;
    int num = 0;
    dw_single_flush_item *temp = NULL;
    dw_file_head_t* head = (dw_file_head_t*)file_head;
    uint16 head_dwn = head->head.dwn;
    dw_first_flush_item flush_item;
    errno_t rc = EOK;

    fprintf(stdout, "first version page: \n");
    fprintf(stdout, "file_head info: dwn is %u, start is %u \n", head->head.dwn, head->start);

    for (uint16 i = head->start; i < DW_FIRST_DATA_PAGE_NUM; i++) {
        offset = (i + 1) * BLCKSZ;  /* need skip the file head */
        fseek(fd, offset, SEEK_SET);
        result = fread(dw_block, 1, BLCKSZ, fd);
        if (BLCKSZ != result) {
            fprintf(stderr, "Reading error");
            free(item);
            free(unaligned_buf);
            free(unaligned_buf2);
            fclose(fd);
            return false;
        }
        pghr = (PageHeader)dw_block;
        rc = memcpy_s(&flush_item, sizeof(dw_first_flush_item), dw_block + pghr->pd_lower, sizeof(dw_first_flush_item));
        securec_check(rc, "\0", "\0");
        fprintf(stdout,
                "dwn is %u, buf_tag[rel %u/%u/%u/%d blk %u fork %d]\n",
                flush_item.dwn,
                flush_item.buf_tag.rnode.spcNode,
                flush_item.buf_tag.rnode.dbNode,
                flush_item.buf_tag.rnode.relNode,
                flush_item.buf_tag.rnode.bucketNode,
                flush_item.buf_tag.blockNum,
                flush_item.buf_tag.forkNum);

        if (CheckPageZeroCases(pghr)) {
            uint16 blkno = flush_item.buf_tag.blockNum;
            uint16 checksum = pg_checksum_page((char*)dw_block, blkno);
            if (checksum != pghr->pd_checksum) {
                fprintf(stdout, "page checksum failed \n");
            }
        }
        (void)parse_a_page(dw_block, flush_item.buf_tag.blockNum, flush_item.buf_tag.blockNum % RELSEG_SIZE, SEG_UNKNOWN);

    }

    head = (dw_file_head_t*)second_file_head;
    head_dwn = head->head.dwn;
    fprintf(stdout, "second version page: \n");
    fprintf(stdout, "file_head info: dwn is %u, start is %u \n", head->head.dwn, head->start);

    for (uint16 i = head->start; i < DW_SECOND_DATA_PAGE_NUM; i++) {
        offset = i * sizeof(dw_single_flush_item);
        temp = (dw_single_flush_item*)((char*)buf + offset);
        fprintf(stdout, "flush_item[data_page_idx %u, dwn %u, crc %u]\n",
            temp->data_page_idx, temp->dwn, temp->crc);
        if (dw_verify_item(temp, head_dwn)) {
            item[num].data_page_idx = temp->data_page_idx;
            item[num].dwn = temp->dwn;
            item[num].buf_tag = temp->buf_tag;
            item[num].crc = temp->crc;
            num++;
        } else {
            fprintf(stdout, "flush item check failed, not need recovery \n");
        }
    }

    for (int i = 0; i < num; i++) {
        int idx = item[i].data_page_idx;
        fseek(fd, (DW_SECOND_DATA_START_IDX + idx) * BLCKSZ, SEEK_SET);
        result = fread(dw_block, 1, BLCKSZ, fd);
        if (BLCKSZ != result) {
            fprintf(stderr, "Reading error");
            free(item);
            free(unaligned_buf);
            free(unaligned_buf2);
            fclose(fd);
            return false;
        }

        fprintf(stdout,
            "buf_tag[rel %u/%u/%u/%d blk %u fork %d]\n",
            item[i].buf_tag.rnode.spcNode,
            item[i].buf_tag.rnode.dbNode,
            item[i].buf_tag.rnode.relNode,
            item[i].buf_tag.rnode.bucketNode,
            item[i].buf_tag.blockNum,
            item[i].buf_tag.forkNum);
        (void)parse_a_page(dw_block, item[i].buf_tag.blockNum, item[i].buf_tag.blockNum % RELSEG_SIZE, SEG_UNKNOWN);
    }

    free(item);
    free(unaligned_buf);
    free(unaligned_buf2);
    fclose(fd);
    return true;
}

void CheckCRC(pg_crc32 comCrcVal, pg_crc32 pageCrcVal, const uint64 readSize, char* uspMetaBuffer)
{
    INIT_CRC32C(comCrcVal);
    COMP_CRC32C(comCrcVal, (void *) uspMetaBuffer, readSize);
    FIN_CRC32C(comCrcVal);

    if (!EQ_CRC32C(pageCrcVal, comCrcVal)) {
        fprintf(stderr,
                "Undo meta CRC calculated(%u) is different from CRC recorded(%u) in page.\n", comCrcVal, pageCrcVal);
        return;
    }
}

static int ParseUndoZoneMeta(const char *filename, int zid)
{
    errno_t rc = EOK;
    int zoneId = INVALID_ZONE_ID;
    uint32 ret = 0;
    uint64 readSize = 0;
    uint32 totalPageCnt = 0;
    pg_crc32 pageCrcVal = 0;       /* CRC store in undo meta page */
    pg_crc32 comCrcVal = 0;        /* calculating CRC current */
    UndoZoneMetaInfo *uspMetaInfo = NULL;
    char uspMetaBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    int fd = open(filename, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);

    if (fd < 0) {
        fprintf(stderr, "Open file(%s), return code desc(%s).\n", UNDO_META_FILE, strerror(errno));
        return false;
    }

    UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);

    /* Ensure read at start posistion of file. */
    lseek(fd, 0, SEEK_SET);

    for (uint32 loop = 1; loop <= totalPageCnt; loop++) {
        rc = memset_s(uspMetaBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
        securec_check(rc, "\0", "\0");

        if (loop != totalPageCnt) {
            readSize = UNDOZONE_COUNT_PER_PAGE * sizeof(UndoZoneMetaInfo);
        } else {
            readSize = (PERSIST_ZONE_COUNT - (totalPageCnt - 1) * UNDOZONE_COUNT_PER_PAGE) * sizeof(UndoZoneMetaInfo);
        }

        /* Read one page(4K), store in uspMetaBuffer. */
        ret = read(fd, (char *) uspMetaBuffer, UNDO_META_PAGE_SIZE);
        if (ret != UNDO_META_PAGE_SIZE) {
            close(fd);
            fprintf(stderr, "Read undo meta page failed, expect size(%u), real size(%u).\n", UNDO_META_PAGE_SIZE, ret);
            return false;
        }

        /* Get page CRC from uspMetaBuffer. */
        pageCrcVal = *(pg_crc32 *) (uspMetaBuffer + readSize);

        /*
         * Calculate the CRC value based on all undospace meta information stored on the page.
         * Then compare with pageCrcVal.
         */
        CheckCRC(comCrcVal, pageCrcVal, readSize, uspMetaBuffer);
        /* Set attribute values of undospace stored in shared memory. */
        for (uint32 offset = 0; offset < (readSize / sizeof(UndoZoneMetaInfo)); offset++) {
            zoneId = (loop - 1) * UNDOZONE_COUNT_PER_PAGE + offset;
            uspMetaInfo = (UndoZoneMetaInfo *) (uspMetaBuffer + offset * sizeof(UndoZoneMetaInfo));
            if ((zid == INVALID_ZONE_ID) || (zid != INVALID_ZONE_ID && zid == zoneId)) {
                fprintf(stdout,
                    "zid=%d, insertURecPtr=%lu, discardURecPtr=%lu, forcediscardURecPtr=%lu, allocateTSlotPtr=%lu, "
                    "recycleTSlotPtr=%lu, recyclexid=%lu, lsn=%lu.\n",
                    zoneId,
                    UNDO_PTR_GET_OFFSET(uspMetaInfo->insertURecPtr),
                    UNDO_PTR_GET_OFFSET(uspMetaInfo->discardURecPtr),
                    UNDO_PTR_GET_OFFSET(uspMetaInfo->forceDiscardURecPtr),
                    UNDO_PTR_GET_OFFSET(uspMetaInfo->allocateTSlotPtr),
                    UNDO_PTR_GET_OFFSET(uspMetaInfo->recycleTSlotPtr),
                    uspMetaInfo->recycleXid, uspMetaInfo->lsn);

                if (zid != INVALID_ZONE_ID) {
                    break;
                }
            }
        }
    }
    close(fd);
    return true;
}

static int ParseUndoSpaceMeta(const char *filename, int zid, UndoSpaceType type)
{
    errno_t rc = EOK;
    int zoneId = INVALID_ZONE_ID;
    uint32 ret = 0;
    uint64 readSize = 0;
    uint32 totalPageCnt = 0;
    pg_crc32 pageCrcVal = 0;       /* CRC store in undo meta page */
    pg_crc32 comCrcVal = 0;        /* calculating CRC current */
    UndoSpaceMetaInfo *uspSpaceInfo = NULL;
    char uspMetaBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    int fd = open(filename, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);

    if (fd < 0) {
        fprintf(stderr, "Open file(%s), return code desc(%s).\n", UNDO_META_FILE, strerror(errno));
        return false;
    }

    if (type == UNDO_LOG_SPACE) {
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        lseek(fd, totalPageCnt * UNDO_META_PAGE_SIZE, SEEK_SET);
    } else {
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        uint32 seek = totalPageCnt * UNDO_META_PAGE_SIZE;
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);
        seek += totalPageCnt * UNDO_META_PAGE_SIZE;
        lseek(fd, seek, SEEK_SET);
    }
    UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);

    for (uint32 loop = 1; loop <= totalPageCnt; loop++) {
        rc = memset_s(uspMetaBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
        securec_check(rc, "\0", "\0");

        if (loop != totalPageCnt) {
            readSize = UNDOSPACE_COUNT_PER_PAGE * sizeof(UndoSpaceMetaInfo);
        } else {
            readSize = (PERSIST_ZONE_COUNT - (totalPageCnt - 1) * UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
        }

        /* Read one page(512), store in uspMetaBuffer. */
        ret = read(fd, (char *) uspMetaBuffer, UNDO_META_PAGE_SIZE);
        if (ret != UNDO_META_PAGE_SIZE) {
            close(fd);
            fprintf(stderr, "Read undo meta page failed, expect size(%u), real size(%u).\n", UNDO_META_PAGE_SIZE, ret);
            return false;
        }

        /* Get page CRC from uspMetaBuffer. */
        pageCrcVal = *(pg_crc32 *) (uspMetaBuffer + readSize);

        /*
         * Calculate the CRC value based on all undospace meta information stored on the page.
         * Then compare with pageCrcVal.
         */
        CheckCRC(comCrcVal, pageCrcVal, readSize, uspMetaBuffer);
        /* Set attribute values of undospace stored in shared memory. */
        for (uint32 offset = 0; offset < (readSize / sizeof(UndoSpaceMetaInfo)); offset++) {
            zoneId = (loop - 1) * UNDOSPACE_COUNT_PER_PAGE + offset;
            uspSpaceInfo = (UndoSpaceMetaInfo *) (uspMetaBuffer + offset * sizeof(UndoSpaceMetaInfo));
            if ((zid == INVALID_ZONE_ID) || (zid != INVALID_ZONE_ID && zid == zoneId)) {
                fprintf(stdout, "zid=%d, head=%lu, tail=%lu, lsn=%lu.\n", zoneId,
                    UNDO_PTR_GET_OFFSET(uspSpaceInfo->head), UNDO_PTR_GET_OFFSET(uspSpaceInfo->tail),
                    uspSpaceInfo->lsn);

                if (zid != INVALID_ZONE_ID) {
                    break;
                }
            }
        }
    }
    close(fd);
    return true;
}

static int ParseUndoSlot(const char *filename)
{
    errno_t rc = EOK;
    off_t seekpos;
    uint32 ret = 0;
    TransactionSlot *slot = NULL;
    char buffer[BLCKSZ] = {'\0'};
    int fd = open(filename, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);

    if (fd < 0) {
        fprintf(stderr, "Open file(%s), return code desc(%s).\n", UNDO_META_FILE, strerror(errno));
        return false;
    }

    for (uint32 loop = 0; loop < UNDO_META_SEG_SIZE; loop++) {
        int flag = 0;
        seekpos = (off_t)BLCKSZ * loop;
        lseek(fd, seekpos, SEEK_SET);
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");

        ret = read(fd, (char *)buffer, BLCKSZ);
        if (ret != BLCKSZ) {
            close(fd);
            fprintf(stderr, "Read undo meta page failed, expect size(8192), real size(%u).\n", ret);
            return false;
        }
        fprintf(stdout, "Block %u, LSN (%X/%X)\n", loop, (uint32)(PageGetLSN(buffer) >> 32),
            (uint32)PageGetLSN(buffer));

        if (PageIsNew(buffer)) {
            continue;
        }
        for (uint32 offset = UNDO_LOG_BLOCK_HEADER_SIZE; offset < BLCKSZ - MAXALIGN(sizeof(TransactionSlot));
            offset += MAXALIGN(sizeof(TransactionSlot))) {
            slot = (TransactionSlot *) (buffer + offset);
            if (slot->XactId() != InvalidTransactionId || slot->StartUndoPtr() != INVALID_UNDO_REC_PTR) {
                if (flag > 0) {
                    uint32 tempOffset = offset - (uint32)flag * MAXALIGN(sizeof(TransactionSlot));
                    TransactionSlot *tempSlot = NULL;
                    fprintf(stdout, "WARNING: invalid slot num %d.\n", flag);
                    for (int i = 0; i < flag; i++) {
                        tempOffset += MAXALIGN(sizeof(TransactionSlot));
                        tempSlot = (TransactionSlot *) (buffer + tempOffset);
                        fprintf(stdout,
                            "offset=%u, xid=%lu, startptr=%lu, endptr=%lu, dbid=%u, rollback finish=%d.\n",
                            tempOffset, tempSlot->XactId(), tempSlot->StartUndoPtr(), tempSlot->EndUndoPtr(),
                            tempSlot->DbId(), !(tempSlot->NeedRollback()));
                    }
                    flag = 0;
                }
                fprintf(stdout, "offset=%u, xid=%lu, startptr=%lu, endptr=%lu, dbid=%u, rollback finish=%d.\n",
                    offset, slot->XactId(), slot->StartUndoPtr(), slot->EndUndoPtr(),
                    slot->DbId(), !(slot->NeedRollback()));
            } else {
                flag++;
            }
        }
    }
    close(fd);
    return true;
}

static void parse_map_position(uint8 map)
{
    uint8 pagemap[BITS_PER_BYTE] = { 0 };
    int pos = 0;
    pos = 0;
    while (map > 0) {
        pagemap[pos] = map % DIVIDED_BY_TWO;
        ++pos;
        map /= DIVIDED_BY_TWO;
    }
    for (int loop = BITS_PER_BYTE - 1; loop >= 0; loop--) {
        fprintf(stdout, "%u", pagemap[loop]);
    }
    fprintf(stdout, " ");
}

static void parse_lsn_info_head(LsnInfoPageHeader *header)
{
    PageXLogRecPtr lsn = header->lsn;
    fprintf(stdout, "%slsn: xlogid %u, xrecoff %u, lsn %lu\n",
            indents[indentLevel], lsn.xlogid, lsn.xrecoff, ((uint64)lsn.xlogid << WAL_ID_OFFSET) | lsn.xrecoff);
    fprintf(stdout, "%schecksum: %u, flags: %u, version: %u",
            indents[indentLevel], header->checksum, header->flags, header->version);
    fprintf(stdout, "%sbase page map: ", indents[indentLevel]);
    for (uint32 loop = 0; loop < BASE_PAGE_MAP_SIZE; loop++) {
        parse_map_position(header->base_page_map[loop]);
    }
    fprintf(stdout, "\n");
}

static void parse_lsn_info_node(LsnInfoNode *lsninfo)
{
    fprintf(stdout, "%slsn info list: prev %lu, next: %lu\n",
            indents[indentLevel], lsninfo->lsn_list.prev, lsninfo->lsn_list.next);
    fprintf(stdout, "%sflags: %u, type: %u, used: %u\n",
            indents[indentLevel], lsninfo->flags, lsninfo->type, lsninfo->used);
    fprintf(stdout, "%slsn:", indents[indentLevel]);
    for (uint loop = 0; loop < LSN_NUM_PER_NODE; loop++) {
        fprintf(stdout, " %lu", lsninfo->lsn[loop]);
    }
    fprintf(stdout, "\n");
}

static void parse_base_page_info_node(BasePageInfoNode *pageinfo)
{
    RelFileNode rnode = pageinfo->relfilenode;
    fprintf(stdout, "%slsn info:\n", indents[indentLevel]);

    ++indentLevel;
    parse_lsn_info_node(&(pageinfo->lsn_info_node));
    --indentLevel;

    fprintf(stdout, "%sbase page list: prev %lu, next: %lu\n",
            indents[indentLevel], pageinfo->base_page_list.prev, pageinfo->base_page_list.next);
    fprintf(stdout, "%scurrent page lsn: %lu\n",
            indents[indentLevel], pageinfo->cur_page_lsn);
    fprintf(stdout, "%srefile node:\n", indents[indentLevel]);
    ++indentLevel;
    fprintf(stdout, "%sspcnode: %u, dbnode: %u, relnode: %u, bucketnode: %d, opt: %u\n",
            indents[indentLevel], rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, rnode.opt);
    --indentLevel;
    fprintf(stdout, "%sfork num: %d, block num: %u\n",
            indents[indentLevel], pageinfo->fork_num, pageinfo->block_num);
    fprintf(stdout, "%snext base page lsn: %lu, base page position: %lu\n",
            indents[indentLevel], pageinfo->next_base_page_lsn, pageinfo->base_page_position);
}

static void parse_lsn_info_block(FILE* fd, uint8 isbasepage[], uint32 &handledblock, uint32 loop)
{
    char bufferlsn[sizeof(LsnInfoNode)];
    char bufferpage[sizeof(BasePageInfoNode)];
    LsnInfoNode *lsnInfo = NULL;
    BasePageInfoNode *basepageinfo = NULL;

    if (isbasepage[handledblock]) {
        fprintf(stdout, "it's a basepage.\n");
        (void)fread(bufferpage, 1, sizeof(BasePageInfoNode), fd);
        basepageinfo = (BasePageInfoNode *)bufferpage;
        if (basepageinfo->lsn_info_node.type != LSN_INFO_TYPE_BASE_PAGE) {
            fprintf(stderr, "Data at page %u, block %u must be base page, but its type is: %u.\n",
                    loop, handledblock, basepageinfo->lsn_info_node.type); // report error but continue.
        }
        parse_base_page_info_node(basepageinfo);
        handledblock += 2; // index need add by 2 for basepage takes 2 blocks.
    } else {
        (void)fread(bufferlsn, 1, sizeof(LsnInfoNode), fd);
        lsnInfo = (LsnInfoNode *)bufferlsn;
        if (!is_lsn_info_node_valid(lsnInfo->flags)) {
            fprintf(stdout, "Data at page %u, block %u is not valid.\n", loop, handledblock);
        } else {
            fprintf(stdout, "it's a lsn page.\n");
            if (lsnInfo->type != LSN_INFO_TYPE_LSNS) {
                fprintf(stderr, "Data at page %u, block %u must be lsn page, but its type is: %u.\n",
                        loop, handledblock, lsnInfo->type); // report error but continue.
            }
            parse_lsn_info_node(lsnInfo);
        }
        handledblock++;
    }
}

static bool parse_lsn_info_meta(const char *filename)
{
    char bufferhead[sizeof(LsnInfoPageHeader)];
    LsnInfoPageHeader *pageheader = NULL;
    FILE* fd = NULL;
    uint32 loop, loopmap, loopbit, handledblock;
    uint8 pagemappos;
    uint8 isbasepage[BASE_PAGE_MAP_BIT_SIZE] = { 0 };
    if (NULL == (fd = fopen(filename, "rb"))) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    long size = ftell(fd);
    rewind(fd);

    if (size % BLCKSZ != 0) {
        fprintf(stderr, "Reading lsn/page info meta file error: file size is not divisible by page size(8k).\n");
        fclose(fd);
        return false;
    }

    long pagenum = size / BLCKSZ;
    fprintf(stdout, "file length is %ld, pagenum is %ld\n", size, pagenum);

    for (loop = 1; loop <= pagenum; loop++) {
        fprintf(stdout, "Page %u information:\n", loop);
        ++indentLevel;
        if (fread(bufferhead, 1, sizeof(LsnInfoPageHeader), fd) != sizeof(LsnInfoPageHeader)) {
            fprintf(stderr, "%sReading header error", indents[indentLevel]);
            fclose(fd);
            return false;
        }

        pageheader = (LsnInfoPageHeader *)bufferhead;
        if (!is_lsn_info_page_valid(pageheader)) {
            fseek(fd, (BASE_PAGE_MAP_BIT_SIZE - 1) * BLCKSZ, SEEK_SET); // push 127 * 64 bytes
            fprintf(stdout, "%sPage %u is not valid.\n", indents[indentLevel], loop);
            --indentLevel;
            continue;
        }
        parse_lsn_info_head(pageheader);

        pagemappos = 0;
        for (loopmap = 0; loopmap < BASE_PAGE_MAP_SIZE; loopmap++) {
            for (loopbit = 0; loopbit < BITS_PER_BYTE; loopbit++) {
                isbasepage[pagemappos] = (((pageheader->base_page_map[loopmap]) & (0x1 << loopbit)) >> loopbit);
                pagemappos++;
            }
        }

        handledblock = 1; // 1st block is handled as header
        while (handledblock < BASE_PAGE_MAP_BIT_SIZE) {
            fprintf(stdout, "%sBlock %u information: ", indents[indentLevel], handledblock);
            ++indentLevel;
            parse_lsn_info_block(fd, isbasepage, handledblock, loop);
            --indentLevel;
        }
        memset_s(isbasepage, sizeof(isbasepage), 0, sizeof(isbasepage));
        --indentLevel;
    }
    fclose(fd);
    return true;
}

static void parse_block_info_head(BlockInfoPageHeader *header)
{
    PageXLogRecPtr lsn = header->lsn;
    fprintf(stdout, "%slsn: xlogid %u, xrecoff %u, lsn %lu\n",
            indents[indentLevel], lsn.xlogid, lsn.xrecoff, ((uint64)lsn.xlogid << WAL_ID_OFFSET) | lsn.xrecoff);
    fprintf(stdout, "%schecksum: %u, flags: %u\n",
            indents[indentLevel], header->checksum, header->flags);
    fprintf(stdout, "%sversion: %u, total_block_num: %lu\n",
            indents[indentLevel], header->version, header->total_block_num);
}

static void parse_block_info_content(BlockMetaInfo *blockInfo)
{
    fprintf(stdout, "%stimeline: %u, record_num: %u\n",
            indents[indentLevel], blockInfo->timeline, blockInfo->record_num);
    fprintf(stdout, "%smin_lsn: %lu, max_lsn: %lu, flags: %u\n",
            indents[indentLevel], blockInfo->min_lsn, blockInfo->max_lsn, blockInfo->flags);
    fprintf(stdout, "%slsn_info_list: prev %lu, next: %lu. base_page_info_list: prev %lu, next: %lu\n",
            indents[indentLevel], blockInfo->lsn_info_list.prev, blockInfo->lsn_info_list.next,
            blockInfo->base_page_info_list.prev, blockInfo->base_page_info_list.next);
}

static bool parse_block_info_meta(const char *filename)
{
    char bufferhead[sizeof(BlockInfoPageHeader)];
    char bufferblock[sizeof(BlockMetaInfo)];
    uint32 loop, loopinfo;
    FILE* fd = NULL;

    if (NULL == (fd = fopen(filename, "rb"))) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    long size = ftell(fd);
    rewind(fd);

    if (size % BLCKSZ != 0) {
        fprintf(stderr, "Reading block info meta file error: file size is not divisible by page size(8k).\n");
        fclose(fd);
        return false;
    }
    long pagenum = size / BLCKSZ;
    fprintf(stdout, "file length is %ld, pagenum is %ld\n", size, pagenum);

    for (loop = 0; loop < pagenum; loop++) {
        fprintf(stdout, "Page %u information:\n", loop);
        ++indentLevel;

        if (fread(bufferhead, 1, sizeof(BlockInfoPageHeader), fd) != sizeof(BlockInfoPageHeader)) {
            fprintf(stderr, "%sReading header error", indents[indentLevel]);
            fclose(fd);
            return false;
        }
        parse_block_info_head((BlockInfoPageHeader *)bufferhead);

        for (loopinfo = 0; loopinfo < BLOCK_META_INFO_NUM_PER_PAGE; loopinfo++) {
            fprintf(stdout, "%sBlock %u information:\n", indents[indentLevel], loopinfo);
            ++indentLevel;
            if (fread(bufferblock, 1, sizeof(BlockMetaInfo), fd) != sizeof(BlockMetaInfo)) {
                fprintf(stderr, "%sReading block meta file error at %u page, %u block.\n",
                        indents[indentLevel], loop, loopinfo);
                fclose(fd);
                return false;
            }
            parse_block_info_content((BlockMetaInfo *)bufferblock);
            --indentLevel;
        }
        --indentLevel;
    }

    fclose(fd);
    return true;
}

typedef struct UndoHeader {
    UndoRecordHeader        whdr_;
    UndoRecordBlock         wblk_;
    UndoRecordTransaction   wtxn_;
    UndoRecordPayload wpay_;
    UndoRecordOldTd         wtd_;
    UndoRecordPartition wpart_;
    UndoRecordTablespace wtspc_;
    StringInfoData rawdata_;
} UndoHeader;

typedef struct UHeapDiskTupleDataHeader {
    ShortTransactionId xid;
    uint16 td_id : 8, reserved : 8; /* Locker as well as the last updater, 8 bits each */
    uint16 flag;                          /* Flag for tuple attributes */
    uint16 flag2;                         /* Number of attributes for now(11 bits) */
    uint8 t_hoff;                         /*  header incl. bitmap, padding */
} UHeapDiskTupleDataHeader;

char g_dir[MAX_PATH_LEN] = {0};

static int OpenUndoBlock(int zoneId, BlockNumber blockno)
{
    char fileName[MAX_PATH_LEN] = {0};
    const int idLen = 13;
    errno_t rc = EOK;
    int segno = blockno / UNDOSEG_SIZE;

    rc = snprintf_s(fileName, sizeof(fileName), sizeof(fileName) - 1, g_dir);
    securec_check(rc, "\0", "\0");
    if (strlen(g_dir) + idLen >= MAX_PATH_LEN) {
        fprintf(stdout, "ERROR: path is too long, MAX_PATH_LEN %d, path len %zu.\n", MAX_PATH_LEN, strlen(g_dir));
    }
    rc = snprintf_s(fileName + strlen(fileName), sizeof(fileName) - strlen(fileName),
        sizeof(fileName) - strlen(fileName) - 1, "%05X.%07zX", zoneId, segno);
    securec_check(rc, "\0", "\0");

    int fd = open(fileName, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);

    if (fd < 0) {
        fprintf(stderr, "Open file(%s), return code desc(%s).\n", fileName, strerror(errno));
        return -1;
    }

    return fd;
}

bool ReadUndoBytes(char *destptr, int destlen, char **readeptr, char *endptr, int *myBytesRead, int *alreadyRead)
{
    if (*myBytesRead >= destlen) {
        *myBytesRead -= destlen;
        return true;
    }

    int remaining = destlen - *myBytesRead;
    int maxReadOnCurrPage = endptr - *readeptr;
    int canRead = Min(remaining, maxReadOnCurrPage);

    if (canRead == 0) {
        return false;
    }

    errno_t rc = memcpy_s(destptr + *myBytesRead, remaining, *readeptr, canRead);
    securec_check(rc, "\0", "\0");

    *readeptr += canRead;
    *alreadyRead += canRead;
    *myBytesRead = 0;

    return (canRead == remaining);
}

bool ReadUndoRecord(UndoHeader *urec, char *buffer, int startingByte, int *alreadyRead)
{
    char *readptr = buffer + startingByte;
    char *endptr = buffer + BLCKSZ;
    int myBytesRead = *alreadyRead;

    if (!ReadUndoBytes((char*)&(urec->whdr_), SIZE_OF_UNDO_RECORD_HEADER,
        &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }
    if (!ReadUndoBytes((char*)&(urec->wblk_), SIZE_OF_UNDO_RECORD_BLOCK,
        &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtxn_, SIZE_OF_UNDO_RECORD_TRANSACTION,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtd_, SIZE_OF_UNDO_RECORD_OLDTD,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        if (!ReadUndoBytes((char *)&urec->wpart_, SIZE_OF_UNDO_RECORD_PARTITION,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtspc_, SIZE_OF_UNDO_RECORD_TABLESPACE,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        if (!ReadUndoBytes((char *)&urec->wpay_, SIZE_OF_UNDO_RECORD_PAYLOAD,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }

        urec->rawdata_.len = urec->wpay_.payloadlen;
        if (urec->rawdata_.len > 0) {
            if (urec->rawdata_.data == NULL) {
                urec->rawdata_.data = (char *)malloc(urec->rawdata_.len);
                if (NULL == urec->rawdata_.data) {
                    fprintf(stderr, "out of memory\n");
                    return false;
                }
            }
            if (!ReadUndoBytes((char *)urec->rawdata_.data, urec->rawdata_.len,
                &readptr, endptr, &myBytesRead, alreadyRead)) {
                return false;
            }
        }
    }
    return true;
}

static bool ParseUndoRecord(UndoRecPtr urp, bool forward = false)
{
    do {
        char buffer[BLCKSZ] = {'\0'};
        BlockNumber blockno = UNDO_PTR_GET_BLOCK_NUM(urp);
        int zoneId = UNDO_PTR_GET_ZONE_ID(urp);
        int startingByte = UNDO_PTR_GET_PAGE_OFFSET(urp);
        int fd = -1;
        int alreadyRead = 0;
        off_t seekpos;
        errno_t rc = EOK;
        uint32 ret = 0;
        UndoHeader *urec = (UndoHeader *)malloc(sizeof(UndoHeader));
        UndoRecPtr blkprev = INVALID_UNDO_REC_PTR;
        const UndoRecordSize UNDO_RECORD_FIX_SIZE = SIZE_OF_UNDO_RECORD_HEADER + SIZE_OF_UNDO_RECORD_BLOCK;
        uint32 curSize = UNDO_RECORD_FIX_SIZE + sizeof(UndoRecordSize);
        PageHeader phdr;

        rc = memset_s(urec, sizeof(UndoHeader), (0), sizeof(UndoHeader));
        securec_check(rc, "\0", "\0");

        do {
            fd = OpenUndoBlock(zoneId, blockno);
            if (fd < 0) {
                free(urec);
                return false;
            }
            seekpos = (off_t)BLCKSZ * (blockno % ((BlockNumber)UNDOSEG_SIZE));
            lseek(fd, seekpos, SEEK_SET);
            rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
            securec_check(rc, "\0", "\0");

            ret = read(fd, (char *)buffer, BLCKSZ);
            if (ret != BLCKSZ) {
                close(fd);
                free(urec);
                fprintf(stderr, "Read undo meta page failed, expect size(8192), real size(%u).\n", ret);
                return false;
            }
            phdr = (PageHeader)buffer;
            if (ReadUndoRecord(urec, buffer, startingByte, &alreadyRead)) {
                break;
            }

            startingByte = UNDO_LOG_BLOCK_HEADER_SIZE;
            blockno++;
            close(fd);
        } while (true);

        if (!TransactionIdIsValid(urec->whdr_.xid)) {
            free(urec);
            close(fd);
            return true;
        }

        fprintf(stdout, "DumpPageInfo: lsn:%X/%X, pd_checksum:%u, flags:%u, lower:%u, upper:%u, special:%u, "
            "pagesize_version:%u.\n",
            phdr->pd_lsn.xlogid, phdr->pd_lsn.xrecoff, (uint16)(phdr->pd_checksum), (uint16)(phdr->pd_flags),
            (uint16)(phdr->pd_lower), (uint16)(phdr->pd_upper), (uint16)(phdr->pd_special),
            (uint16)(phdr->pd_pagesize_version));
        
        blkprev = urec->wblk_.blkprev;
        fprintf(stdout, "UndoRecPtr(%lu):\nwhdr = xid(%lu), cid(%u), reloid(%u), relfilenode(%u), utype(%u), "
            "uinfo(%u).\n", urp, urec->whdr_.xid, urec->whdr_.cid, urec->whdr_.reloid, urec->whdr_.relfilenode,
            urec->whdr_.utype, urec->whdr_.uinfo);
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
            fprintf(stdout, "flag_payload, size = %lu.\n", SIZE_OF_UNDO_RECORD_PAYLOAD);
            curSize += SIZE_OF_UNDO_RECORD_PAYLOAD;
            curSize += urec->rawdata_.len;
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
            fprintf(stdout, "flag_transac, size = %lu.\n", SIZE_OF_UNDO_RECORD_TRANSACTION);
            curSize += SIZE_OF_UNDO_RECORD_TRANSACTION;
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_BLOCK) != 0) {
            fprintf(stdout, "flag_block, size = %lu.\n", SIZE_OF_UNDO_RECORD_BLOCK);
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
            fprintf(stdout, "flag_oldtd, size = %lu.\n", SIZE_OF_UNDO_RECORD_OLDTD);
            curSize += SIZE_OF_UNDO_RECORD_OLDTD;
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_CONTAINS_SUBXACT) != 0) {
            /* subxid is at the end of rawdata */
            char *end = (char *)(urec->rawdata_.data) + (urec->rawdata_.len - sizeof(SubTransactionId));
            SubTransactionId *subxid = (SubTransactionId *)end;
            fprintf(stdout, "flag_subxact, subxid = %lu.\n", *subxid);
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
            fprintf(stdout, "flag_partoid, size = %lu.\n", SIZE_OF_UNDO_RECORD_PARTITION);
            curSize += SIZE_OF_UNDO_RECORD_PARTITION;
        }
        if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
            fprintf(stdout, "flag_tablespaceoid, size = %lu.\n", SIZE_OF_UNDO_RECORD_TABLESPACE);
            curSize += SIZE_OF_UNDO_RECORD_TABLESPACE;
        }
        fprintf(stdout, "wblk = blk_prev(%lu), blockno(%u), offset(%u).\n", urec->wblk_.blkprev, urec->wblk_.blkno,
            urec->wblk_.offset);
        fprintf(stdout, "wtxn = prevurp(%lu).\n", urec->wtxn_.prevurp);
        fprintf(stdout, "wpay = payloadlen(%u).\n", urec->wpay_.payloadlen);
        fprintf(stdout, "wtd = oldxactid(%lu).\n", urec->wtd_.oldxactid);
        fprintf(stdout, "wpart_ = partitionoid(%u).\n", urec->wpart_.partitionoid);
        fprintf(stdout, "wtspc_ = tablespace(%u).\n", urec->wtspc_.tablespace);
        fprintf(stdout, "len = alreadyRead(%d).\n", alreadyRead);
        
        char prevLen[2];
        UndoRecordSize byteToRead = sizeof(UndoRecordSize);
        char *readptr = buffer + startingByte - byteToRead;
        for (auto i = 0; i < byteToRead; i++) {
            prevLen[i] = *readptr;
            readptr++;
        }
        UndoRecordSize prevRecLen = *(UndoRecordSize *)(prevLen);
        fprintf(stdout, "prevLen = prevLen(%u).\n", prevRecLen);

        if (urec->whdr_.utype != UNDO_INSERT && urec->whdr_.utype != UNDO_MULTI_INSERT &&
            urec->rawdata_.len > 0 && urec->rawdata_.data != NULL) {
            UHeapDiskTupleDataHeader diskTuple;
            if (urec->whdr_.utype == UNDO_INPLACE_UPDATE) {
                Assert(urec->rawdata_.len >= (int)SizeOfUHeapDiskTupleData);
                uint8 *t_hoff_ptr = (uint8 *)(urec->rawdata_.data);
                uint8 t_hoff = *t_hoff_ptr;
                char *cur_undodata_ptr = NULL;
                fprintf(stdout, "t_hoff %u ", t_hoff);
                rc = memcpy_s((char *)&diskTuple + OffsetTdId, SizeOfUHeapDiskTupleHeaderExceptXid,
                    urec->rawdata_.data + sizeof(uint8), SizeOfUHeapDiskTupleHeaderExceptXid);
                securec_check(rc, "", "");
                cur_undodata_ptr = urec->rawdata_.data + sizeof(uint8) + t_hoff - OffsetTdId;
                uint8* flags_ptr = (uint8 *)cur_undodata_ptr;
                uint8 flags = *flags_ptr;
                fprintf(stdout, "flags %u ", flags);
                cur_undodata_ptr += sizeof(uint8);
                if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
                    uint16* prefixlen_ptr = (uint16 *)(cur_undodata_ptr);
                    cur_undodata_ptr += sizeof(uint16);
                    uint16 prefixlen = *prefixlen_ptr;
                    fprintf(stdout, "PREFIXLEN %u ", prefixlen);
                }

                if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
                    uint16* suffixlen_ptr = (uint16 *)(cur_undodata_ptr);
                    cur_undodata_ptr += sizeof(uint16);
                    uint16 suffixlen = *suffixlen_ptr;
                    fprintf(stdout, "SUFFIXLEN %u ", suffixlen);
                }
                diskTuple.xid = (ShortTransactionId)InvalidTransactionId;
            } else {
                Assert(urec->rawdata_.len >= (int)SizeOfUHeapDiskTupleHeaderExceptXid);
                rc = memcpy_s(((char *)&diskTuple + OffsetTdId), SizeOfUHeapDiskTupleHeaderExceptXid,
                    urec->rawdata_.data, SizeOfUHeapDiskTupleHeaderExceptXid);
                securec_check(rc, "", "");
                diskTuple.xid = (ShortTransactionId)InvalidTransactionId;
            }
            fprintf(stdout, "\ndiskTuple: td_id %u, reserved %u, flag %u, flag2 %u, t_hoff %u.\n",
                    diskTuple.td_id, diskTuple.reserved, diskTuple.flag, diskTuple.flag2, diskTuple.t_hoff);
            fprintf(stdout, "current undo record size: %u\n\n", curSize);
        }

        free(urec);
        close(fd);

        if (!forward) {
            urp = blkprev;
        } else {
            urp = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(urp, curSize);
        }
    } while (urp != INVALID_UNDO_REC_PTR);

    return true;
}

static void fill_filenode_map(char** class_map)
{
    int i = 0;
    char* name = NULL;
    const PgClass_table cmap[CLASS_TYPE_NUM] = {{"pg_default_acl", 826},
        {"pg_pltemplate", 1136},
        {"pg_tablespace", 1213},
        {"pg_shdepend", 1214},
        {"pg_type", 1247},
        {"pg_attribute", 1249},
        {"pg_proc", 1255},
        {"pg_class", 1259},
        {"pg_authid", 1260},
        {
            "pg_auth_members",
            1261,
        },
        {"pg_database", 1262},
        {"pg_foreign_server", 1417},
        {"pg_user_mapping", 1418},
        {"pg_foreign_data_wrapper", 2328},
        {"pg_shdescription", 2396},
        {"pg_aggregate", 2600},
        {
            "pg_am",
            2601,
        },
        {"pg_amop", 2602},
        {"pg_amproc", 2603},
        {"pg_attrdef", 2604},
        {"pg_cast", 2605},
        {"pg_constraint", 2606},
        {"pg_conversion", 2607},
        {"pg_depend", 2608},
        {"pg_description", 2609},
        {"pg_index", 2610},
        {"pg_inherits", 2611},
        {"pg_language", 2612},
        {"pg_largeobject", 2613},
        {"pg_namespace", 2615},
        {"pg_opclass", 2616},
        {"pg_operator", 2617},
        {"pg_rewrite", 2618},
        {"pg_statistic", 2619},
        {"pg_trigger", 2620},
        {"pg_opfamily", 2753},
        {"pg_db_role_setting", 2964},
        {"pg_largeobject_metadata", 2995},
        {"pg_extension", 3079},
        {"pg_foreign_table", 3118},
        {"pg_enum", 3501},
        {"pg_set", 3516},
        {"pg_seclabel", 3596},
        {"pg_ts_dict", 3600},
        {"pg_ts_parser", 3601},
        {"pg_ts_config", 3602},
        {"pg_ts_config_map", 3603},
        {"pg_ts_template", 3764},
        /* normal catalogs */
        {"pg_attrdef", 2830},
        {"pg_attrdef", 2831},
        {"pg_constraint", 2832},
        {"pg_constraint", 2833},
        {"pg_description", 2834},
        {"pg_description", 2835},
        {"pg_proc", 2836},
        {"pg_proc", 2837},
        {"pg_rewrite", 2838},
        {"pg_rewrite", 2839},
        {"pg_seclabel", 3598},
        {"pg_seclabel", 3599},
        {"pg_statistic", 2840},
        {"pg_statistic", 2841},
        {"pg_trigger", 2336},
        {"pg_trigger", 2337},
        /* shared catalogs */
        {"pg_database", 2844},
        {"pg_database", 2845},
        {"pg_shdescription", 2846},
        {"pg_shdescription", 2847},
        {"pg_db_role_setting", 2966},
        {"pg_db_role_setting", 2967},
        /* indexing */
        {"pg_aggregate_fnoid_index", 2650},
        {"pg_am_name_index", 2651},
        {"pg_am_oid_index", 2652},
        {"pg_amop_fam_strat_index", 2653},
        {"pg_amop_opr_fam_index", 2654},
        {"pg_amop_oid_index", 2756},
        {"pg_amproc_fam_proc_index", 2655},
        {"pg_amproc_oid_index", 2757},
        {"pg_attrdef_adrelid_adnum_index", 2656},
        {"pg_attrdef_oid_index", 2657},
        {"pg_attribute_relid_attnam_index", 2658},
        {"pg_attribute_relid_attnum_index", 2659},
        {"pg_authid_rolname_index", 2676},
        {"pg_authid_oid_index", 2677},
        {"pg_auth_members_role_member_index", 2694},
        {"pg_auth_members_member_role_index", 2695},
        {"pg_cast_oid_index", 2660},
        {"pg_cast_source_target_index", 2661},
        {"pg_class_oid_index", 2662},
        {"pg_class_relname_nsp_index", 2663},
        {"pg_collation_enc_def_index", 3147},
        {"pg_collation_name_enc_nsp_index", 3164},
        {"pg_collation_oid_index", 3085},
        {"pg_constraint_conname_nsp_index", 2664},
        {"pg_constraint_conrelid_index", 2665},
        {"pg_constraint_contypid_index", 2666},
        {"pg_constraint_oid_index", 2667},
        {"pg_conversion_default_index", 2668},
        {"pg_conversion_name_nsp_index", 2669},
        {"pg_conversion_oid_index", 2670},
        {"pg_database_datname_index", 2671},
        {"pg_database_oid_index", 2672},
        {"pg_depend_depender_index", 2673},
        {"pg_depend_reference_index", 2674},
        {"pg_description_o_c_o_index", 2675},
        {"pg_shdescription_o_c_index", 2397},
        {"pg_enum_oid_index", 3502},
        {"pg_enum_typid_label_index", 3503},
        {"pg_enum_typid_sortorder_index", 3534},
        {"pg_index_indrelid_index", 2678},
        {"pg_index_indexrelid_index", 2679},
        {"pg_inherits_relid_seqno_index", 2680},
        {"pg_inherits_parent_index", 2187},
        {"pg_language_name_index", 2681},
        {"pg_language_oid_index", 2682},
        {"pg_largeobject_loid_pn_index", 2683},
        {"pg_largeobject_metadata_oid_index", 2996},
        {"pg_namespace_nspname_index", 2684},
        {"pg_namespace_oid_index", 2685},
        {"pg_opclass_am_name_nsp_index", 2686},
        {"pg_opclass_oid_index", 2687},
        {"pg_operator_oid_index", 2688},
        {"pg_operator_oprname_l_r_n_index", 2689},
        {"pg_opfamily_am_name_nsp_index", 2754},
        {"pg_opfamily_oid_index", 2755},
        {"pg_pltemplate_name_index", 1137},
        {"pg_proc_oid_index", 2690},
        {"pg_proc_proname_args_nsp_index", 2691},
        {"pg_rewrite_oid_index", 2692},
        {"pg_rewrite_rel_rulename_index", 2693},
        {"pg_shdepend_depender_index", 1232},
        {"pg_shdepend_reference_index", 1233},
        {"pg_statistic_relid_att_inh_index", 2696},
        {"pg_tablespace_oid_index", 2697},
        {"pg_tablespace_spcname_index", 2698},
        {"pg_trigger_tgconstraint_index", 2699},
        {"pg_trigger_tgrelid_tgname_index", 2701},
        {"pg_trigger_oid_index", 2702},
        {"pg_ts_config_cfgname_index", 3608},
        {"pg_ts_config_oid_index", 3712},
        {"pg_ts_config_map_index", 3609},
        {"pg_ts_dict_dictname_index", 3604},
        {"pg_ts_dict_oid_index", 3605},
        {"pg_ts_parser_prsname_index", 3606},
        {"pg_ts_parser_oid_index", 3607},
        {"pg_ts_template_tmplname_index", 3766},
        {"pg_ts_template_oid_index", 3767},
        {"pg_type_oid_index", 2703},
        {"pg_type_typname_nsp_index", 2704},
        {"pg_foreign_data_wrapper_oid_index", 112},
        {"pg_foreign_data_wrapper_name_index", 548},
        {"pg_foreign_server_oid_index", 113},
        {"pg_foreign_server_name_index", 549},
        {"pg_user_mapping_oid_index", 174},
        {"pg_user_mapping_user_server_index", 175},
        {"pg_foreign_table_relid_index", 3119},
        {"pg_default_acl_role_nsp_obj_index", 827},
        {"pg_default_acl_oid_index", 828},
        {"pg_db_role_setting_databaseid_rol_index", 2965},
        {"pg_seclabel_object_index", 3597},
        {"pg_extension_oid_index", 3080},
        {"pg_extension_name_index", 3081},
        {"pg_auth_history", 3457},
        {"pg_user_status", 3460},
        {"pg_shseclabel", 3592},
        {"pg_shseclabel_object_index", 3593},
        {"pg_auth_history_index", 3458},
        {"pg_auth_history_oid_index", 3459},
        {"pg_user_status_index", 3461},
        {"pg_user_status_oid_index", 3462},
        {"pg_job_oid_index", 3466},
        {"pg_job_proc_oid_index", 3467},
        {"pg_job_schedule_oid_index", 3468},
        {"pg_job_jobid_index", 3469},
        {"pg_job_proc_procid_index", 3470},
        {"pg_job_schedule_schid_index", 3471},
        {"pg_directory_oid_index", 3223},
        {"pg_directory_dirname_index", 3224},
        {"pg_proc_lang_index", 3225},
        {"pg_proc_lang_namespace_index", 3226},
        {"pg_job", 9000},
        {"pg_job_schedule", 9001},
        {"pg_job_proc", 9002},
        {"pg_directory", 3222},
        {"pgxc_node", 9015 /* PgxcNodeRelationId */},
        {"pgxc_group", 9014 /* PgxcGroupRelationId */},
        {"pg_resource_pool", 3450 /*  ResourcePoolRelationId */},
        {"pg_workload_group", 3451 /* WorkloadGroupRelationId */},
        {"pg_app_workloadgroup_mapping", 3464 /* AppWorkloadGroupMappingRelationId */},
        {"pgxc_node_oid_index", 9010},
        {"pgxc_node_name_index", 9011},
        {"pg_extension_data_source_oid_index", 2717},
        {"pg_extension_data_source_name_index", 2718}};

    for (i = 0; i < CLASS_TYPE_NUM; i++) {
        name = (char*)malloc(64 * sizeof(char));
        if (NULL == name) {
            printf("error allocate space\n");
            return;
        }
        if (NULL != cmap[i].class_name) {
            errno_t rc = EOK;
            rc = memcpy_s(name, 64 * sizeof(char), cmap[i].class_name, strlen(cmap[i].class_name) + 1);
            securec_check_c(rc, "\0", "\0");
            class_map[cmap[i].ralation_id] = name;
            name = NULL;
        } else {
            free(name);
            name = NULL;
        }
    }
    return;
}

static long int strtolSafe(const char* nptr, long int default_value)
{
    char* tmp = NULL;
    long int res = strtol(nptr, &tmp, TEN);
    if (errno == ERANGE || tmp == nptr || (errno != 0 && res == 0)) {
        fprintf(stdout, "WARNING: failed to convert parameter %s to int!\n", nptr);
        res = default_value;
    }
    return res;
}

static long long int strtollSafe(const char* nptr, long long int default_value)
{
    char* tmp = NULL;
    long long int res = strtoll(nptr, &tmp, TEN);
    if (errno == ERANGE || tmp == nptr || (errno != 0 && res == 0)) {
        fprintf(stdout, "WARNING: failed to convert parameter %s to int!\n", nptr);
        res = default_value;
    }
    return res;
}

static void checkDssInput(const char* file, const char** socketpath)
{
    if (!enable_dss && file != NULL && file[0] == '+') {
        enable_dss = true;
    }

    /* set socketpath if not existed when enable dss */
    if (enable_dss && *socketpath == NULL) {
        *socketpath = getSocketpathFromEnv();
    }
}

int main(int argc, char** argv)
{
    int c;
    char* filename = NULL;
    char* env = NULL;
    const char* progname = NULL;
    const char* socketpath = NULL;
    uint32 start_point = 0;
    uint32 num_block = 0;
    uint64 cu_offset = 0;
    int zid = INVALID_ZONE_ID;
    errno_t ret;
    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0 || strcmp(argv[1], "-h") == 0) {
            usage(progname);
            exit(0);
        }

        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("pagehack (PostgreSQL) " PG_VERSION);
            exit(0);
        }
    }

#ifdef WIN32
    /* stderr is buffered on Win32. */
    setvbuf(stderr, NULL, _IONBF, 0);
#endif

    while ((c = getopt(argc, argv, "bc:Df:o:t:vs:z:n:r:i:I:N:uwdS:")) != -1) {
        switch (c) {
            case 'f':
                filename = optarg;
                break;

            case 't': {
                int i;
                for (i = 0; i < NUM_HACKINGTYPE; ++i) {
                    if (strcmp(optarg, HACKINGTYPE[i]) == 0)
                        break;
                }

                hackingtype = (HackingType)i;
                if (hackingtype >= NUM_HACKINGTYPE) {
                    fprintf(stderr, "invalid hacking type (-t): %s\n", optarg);
                    exit(1);
                }
                break;
            }

            case 'o':
                cu_offset = (uint64)strtollSafe(optarg, 0);
                break;

            case 'r':  // relation name given
            {
                int i = 0;
                int n = sizeof(PgHeapRelName) / sizeof(PgHeapRelName[0]);
                for (; i < n; ++i) {
                    if (strcmp(optarg, PgHeapRelName[i]) == 0)
                        break;
                }

                if (i == n) {
                    fprintf(stderr, "invalid heap relation name (-r): %s\n", optarg);
                    exit(1);
                }
                PgHeapRelTupleParserCursor = i;
                hackingtype = HACKING_HEAP;
                break;
            }

            case 'u': {
                hackingtype = HACKING_DW;
                break;
            }

            case 'i':  // index name given
            {
                int i = 0;
                int n = sizeof(PgBtreeIndexRelName) / sizeof(PgBtreeIndexRelName[0]);
                for (; i < n; ++i) {
                    if (strcmp(optarg, PgBtreeIndexRelName[i]) == 0)
                        break;
                }

                if (i == n) {
                    fprintf(stderr, "invalid index relation name (-i): %s\n", optarg);
                    exit(1);
                }
                PgIndexRelTupleParserCursor = i;
                hackingtype = HACKING_INDEX;
                break;
            }

            case 'v':
                only_vm = true;
                break;

            case 'b':
                only_bcm = true;
                break;

            case 's':
                start_point = (unsigned int)strtolSafe(optarg, 0);
                break;

            case 'n':
                num_block = (unsigned int)strtolSafe(optarg, 0);
                break;

            case 'I':
                start_item = (unsigned int)strtolSafe(optarg, 1);
                break;

            case 'N':
                num_item = (unsigned int)strtolSafe(optarg, 0);
                break;

            case 'w':
                write_back = true;
                break;

            case 'd':
                dirty_page = true;
                break;

            case 'z':
                zid = (int)strtolSafe(optarg, INVALID_ZONE_ID);
                break;

            case 'S':
                SegNo = (unsigned int)strtolSafe(optarg, 0);
                break;

            case 'D':
                enable_dss = true;
                break;

            case 'c':
                socketpath = optarg;
                enable_dss = true;
                break;

            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }

    checkDssInput(filename, &socketpath);

    if (NULL == filename) {
        fprintf(stderr, "must specify a file to parse.\n");
        exit(1);
    }

    if (only_vm && (hackingtype > HACKING_INDEX)) {
        fprintf(stderr, "Only heap file and index file have visibility map info.\n");
        exit(1);
    }

    if (only_bcm && (hackingtype > HACKING_INDEX)) {
        fprintf(stderr, "Only heap file and index file have bcm map info.\n");
        exit(1);
    }

    if (enable_dss && (socketpath == NULL || strlen(socketpath) == 0 || strncmp("UDS:", socketpath, 4) != 0)) {
        fprintf(stderr, "Socketpath must be specific correctly when enable dss, "
            "format is: '-c UDS:xxx'.\n");
        exit(1);
    }

    if (enable_dss && (filename[0] != '+' || strstr(filename, "/") == NULL)) {
        fprintf(stderr, "Filepath should be absolutely when enable dss.\n");
        exit(1);
    }

    if (((start_point != 0) || (num_block != 0)) &&
        /* only heap/index/undo/dw */
        (hackingtype > HACKING_UNDO && hackingtype != HACKING_DW && hackingtype != HACKING_SEGMENT)) {
        fprintf(stderr, "Start point or block number only takes effect when hacking segment/heap/undo/index.\n");
        start_point = num_block = 0;
    }

    if (((start_point != 0) || (num_block != 0)) && (only_vm == true)) {
        fprintf(stderr, "can't show only visibility map info when hack partial relation.\n");
        exit(1);
    }

    if ((zid != INVALID_ZONE_ID) && (hackingtype != HACKING_UNDO_SPACE_META) &&
        (hackingtype != HACKING_UNDO_SPACE_META)) {
        fprintf(stderr, "zid is used for undo space dump.\n");
        zid = -1;
    } else if ((zid < INVALID_ZONE_ID) || (zid >= PERSIST_ZONE_COUNT)) {
        fprintf(stderr, "zid should in -1 ~ %d.\n", PERSIST_ZONE_COUNT);
        zid = -1;
    }

    if (argc > optind) {
        pgdata = argv[optind];
    } else {
        if ((env = getenv("GAUSSDATA")) != NULL && *env != '\0')
            pgdata = env;
    }

    // dss device init
    if (dss_device_init(socketpath, enable_dss) != DSS_SUCCESS) {
        exit(1);
    }

    initDataPathStruct(false);

    // if heap relation name is given (-r), force hackingtype to be HACKING_HEAP
    if (PgHeapRelTupleParserCursor >= 0) {
        hackingtype = HACKING_HEAP;
    }

    switch (hackingtype) {
        case HACKING_HEAP:
            if (!parse_page_file(filename, SEG_HEAP, start_point, num_block)) {
                fprintf(stderr, "Error during parsing heap file %s.\n", filename);
                exit(1);
            }
            break;
        case HACKING_FSM:
            if (!parse_page_file(filename, SEG_FSM, start_point, num_block)) {
                fprintf(stderr, "Error during parsing fsm file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_UHEAP:
            if (!parse_page_file(filename, SEG_UHEAP, start_point, num_block)) {
                fprintf(stderr, "Error during parsing heap file %s.\n", filename);
                exit(1);
            }
            break;

        case HACKING_INDEX:
            if (!parse_page_file(filename, SEG_INDEX_BTREE, start_point, num_block)) {
                fprintf(stderr, "Error during parsing index file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_UNDO:
            if (!parse_page_file(filename, SEG_UNDO, start_point, num_block)) {
                fprintf(stderr, "Error during parsing heap file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_FILENODE:
            if (!parse_filenodemap_file(filename)) {
                fprintf(stderr, "Error during parsing filenode_map file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_INTERNAL_INIT:
            if (!parse_internal_init_file(filename)) {
                fprintf(stderr, "Error during parsing \"%s\" file\n", filename);
                exit(1);
            }
            break;

        case HACKING_CU:
            if (!parse_cu_file(filename, cu_offset)) {
                fprintf(stderr, "Error during parsing cu file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_SLOT:
            if (!parse_slot_file(filename)) {
                fprintf(stderr, "Error during parsing slot file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_STATE:
            if (!parse_gaussdb_state_file(filename)) {
                fprintf(stderr, "Error during parsing gaussdb.state file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_CONTROL:
            if (!parse_pg_control_file(filename)) {
                fprintf(stderr, "Error during parsing pg_control file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_CLOG:
            if (!parse_clog_file(filename)) {
                fprintf(stderr, "Error during parsing clog file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_CSNLOG:
            if (!parse_csnlog_file(filename)) {
                fprintf(stderr, "Error during parsing suntran file %s\n", filename);
                exit(1);
            }
            break;

        case HACKING_DW:
            if (!parse_dw_file(filename, start_point, num_block)) {
                fprintf(stderr, "Error during parsing double write file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_SEGMENT:
            if (!parse_segment_head(filename, start_point)) {
                fprintf(stderr, "Error during parsing segment head %s\n", filename);
            }
            break;
        case HACKING_DW_SINGLE:
            if (!parse_dw_single_flush_file(filename)) {
                fprintf(stderr, "Error during parsing dw single flush file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_ZONE_META:
            if (!ParseUndoZoneMeta(filename, zid)) {
                fprintf(stderr, "Error during parsing undo space meta file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_SPACE_META:
            if (!ParseUndoSpaceMeta(filename, zid, UNDO_LOG_SPACE)) {
                fprintf(stderr, "Error during parsing undo group meta file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_SLOT_SPACE_META:
            if (!ParseUndoSpaceMeta(filename, zid, UNDO_SLOT_SPACE)) {
                fprintf(stderr, "Error during parsing undo group meta file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_SLOT:
            if (!ParseUndoSlot(filename)) {
                fprintf(stderr, "Error during parsing undo group meta file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_RECORD:
            ret = snprintf_s(g_dir, sizeof(g_dir), sizeof(g_dir), filename);
            securec_check(ret, "\0", "\0");
            fprintf(stdout, "Parsing backward, urp %lu:\n", cu_offset);
            if (!ParseUndoRecord(cu_offset, false)) {
                fprintf(stderr, "Error during parsing undo group meta file %s backward\n", filename);
                exit(1);
            }
            fprintf(stdout, "Parsing forward, urp %lu:\n", cu_offset);
            if (!ParseUndoRecord(cu_offset, true)) {
                fprintf(stderr, "Error during parsing undo group meta file %s forward\n", filename);
                exit(1);
            }
            break;
        case HACKING_UNDO_FIX:
            break;
        case HACKING_LSN_INFO_META:
            if (!parse_lsn_info_meta(filename)) {
                fprintf(stderr, "Error during parsing lsn info meta file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_BLOCK_INFO_META:
            if (!parse_block_info_meta(filename)) {
                fprintf(stderr, "Error during parsing block info meta file %s\n", filename);
                exit(1);
            }
            break;
        default:
            /* should be impossible to be here */
            Assert(false);
    }

    return 0;
}
