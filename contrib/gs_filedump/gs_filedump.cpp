/*
 * gs_filedump.c - PostgreSQL file dump utility for dumping and
 *				   formatting heap (data), index and control files.
 *
 * Copyright (c) 2002-2010 Red Hat, Inc.
 * Copyright (c) 2011-2023, PostgreSQL Global Development Group
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

#include "gs_filedump.h"

#include <utils/pg_crc.h>

#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/smgr/segment_internal.h"
#include "storage/smgr/segment.h"
#include "decode.h"
#include "segment.h"

const static char *FD_VERSION = "6.0.0";                  /* version ID of gs_filedump */
const static char *FD_PG_VERSION = "openGauss 6.x.x ..."; /* PG version it works with */

/*
 * Global variables for ease of use mostly
 */
/*	Options for Block formatting operations */
unsigned int g_blockOptions = 0;

/* Segment-related options */
unsigned int g_segmentOptions = 0;

/* -R[start]:Block range start */
int g_blockStart = -1;

/* -R[end]:Block range end */
int g_blockEnd = -1;

/* table relfilenode -r */
int g_tableRelfilenode = -1;

/* table toast relfilenode -T */
int g_toastRelfilenode = -1;

/* Options for Item formatting operations */
unsigned int g_itemOptions = 0;

/* Options for Control File formatting operations */
unsigned int g_controlOptions = 0;

unsigned int g_specialType = SPEC_SECT_NONE;

static bool g_verbose = false;

/* File to dump or format */
FILE *fp = NULL;
FILE *fpToast = NULL;

/* File name for display */
char *g_fileName = nullptr;

/* Current block size */
static unsigned int g_blockSize = 0;

/* Segment size in bytes */
static unsigned int g_segmentSize = RELSEG_SIZE * BLCKSZ;

/* Number of current segment */
static unsigned int g_segmentNumber = 0;

/* Offset of current block */
static unsigned int g_pageOffset = 0;

/* Number of bytes to format */
static unsigned int g_bytesToFormat = 0;

/* Block version number */
static unsigned int g_blockVersion = 0;

/* Flag to indicate pg_filenode.map file */
static bool g_isRelMapFile = false;

/* Program exit code */
static int g_exitCode = 0;

bool g_isUHeap;

bool g_isSegment;

/*
 * Function Prototypes
 */

static void DisplayOptions(unsigned int validOptions);
static unsigned int ConsumeOptions(int numOptions, char **options);
static int GetOptionValue(char *optionString);
static void FormatBlock(unsigned int blockOptions, unsigned int controlOptions, char *buffer, BlockNumber currentBlock,
                        unsigned int blockSize, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                        char *toastValue, unsigned int *toastRead);
static unsigned int GetSpecialSectionType(char *buffer, Page page);
static bool IsBtreeMetaPage(Page page);
static void CreateDumpFileHeader(int numOptions, char **options);
static int FormatHeader(char *buffer, Page page, BlockNumber blkno, bool isToast);
static int FormatUHeapHeader(char *buffer, Page page, BlockNumber blkno, bool isToast);
static void FormatItemBlock(char *buffer, Page page, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                            char *toastValue, unsigned int *toastRead);
static void FormatUHeapItemBlock(char *buffer, Page page, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                                 char *toastValue, unsigned int *toastRead);
static void FormatItem(char *buffer, unsigned int numBytes, unsigned int startIndex, unsigned int formatAs);
static void FormatUHeapItem(char *buffer, unsigned int numBytes, unsigned int startIndex, unsigned int formatAs);
static void FormatSpecial(char *buffer);
static void FormatUHeapSpecial(char *buffer);
static void FormatControl(char *buffer);
static void FormatBinary(char *buffer, unsigned int numBytes, unsigned int startIndex);
static void DumpBinaryBlock(char *buffer);
static int PrintRelMappings(void);

static const char *wal_level_str(WalLevel wal_level)
{
    switch (wal_level) {
        case WAL_LEVEL_MINIMAL:
            return "minimal";
        case WAL_LEVEL_ARCHIVE:
            return "archive";
        case WAL_LEVEL_HOT_STANDBY:
            return "hot_standby";
        case WAL_LEVEL_LOGICAL:
            return "logical";
        default:
            break;
    }
    return _("unrecognized wal_level");
}

static const DbStateMapping dbStateTable[] = {
    {DB_STARTUP, "STARTUP"},
    {DB_SHUTDOWNED, "SHUTDOWNED"},
    {DB_SHUTDOWNED_IN_RECOVERY, "SHUTDOWNED_IN_RECOVERY"},
    {DB_SHUTDOWNING, "SHUTDOWNING"},
    {DB_IN_CRASH_RECOVERY, "IN CRASH RECOVERY"},
    {DB_IN_ARCHIVE_RECOVERY, "IN ARCHIVE RECOVERY"},
    {DB_IN_PRODUCTION, "IN PRODUCTION"},
    {-1, "UNKNOWN"}  // default
};

static const FlagMapping flagTable[] = {
    {UHEAP_HAS_NULL, "UHEAP_HAS_NULL|"},
    {UHEAP_HASVARWIDTH, "UHEAP_HASVARWIDTH|"},
    {UHEAP_HASEXTERNAL, "UHEAP_HASEXTERNAL|"},
    {UHEAP_DELETED, "UHEAP_DELETED|"},
    {UHEAP_INPLACE_UPDATED, "UHEAP_INPLACE_UPDATED|"},
    {UHEAP_UPDATED, "UHEAP_UPDATED|"},
    {UHEAP_XID_KEYSHR_LOCK, "UHEAP_XID_KEYSHR_LOCK|"},
    {UHEAP_XID_NOKEY_EXCL_LOCK, "UHEAP_XID_NOKEY_EXCL_LOCK|"},
    {UHEAP_XID_EXCL_LOCK, "UHEAP_XID_EXCL_LOCK|"},
    {UHEAP_MULTI_LOCKERS, "UHEAP_MULTI_LOCKERS|"},
    {UHEAP_INVALID_XACT_SLOT, "UHEAP_INVALID_XACT_SLOT|"},
    {UHEAP_XID_COMMITTED, "UHEAP_XID_COMMITTED|"},
    {UHEAP_XID_INVALID, "UHEAP_XID_INVALID|"},
    {UHEAP_XID_FROZEN, "UHEAP_XID_FROZEN|"}
};

static const FlagMapping btreeFlagTable[] = {
    {BTP_LEAF, "LEAF|"},
    {BTP_ROOT, "ROOT|"},
    {BTP_DELETED, "DELETED|"},
    {BTP_META, "META|"},
    {BTP_HALF_DEAD, "HALFDEAD|"},
    {BTP_SPLIT_END, "SPLITEND|"},
    {BTP_HAS_GARBAGE, "HASGARBAGE|"},
    {BTP_INCOMPLETE_SPLIT, "INCOMPLETESPLIT|"}
};

static const FlagMapping hashFlagTable[] = {
    {LH_UNUSED_PAGE, "UNUSED|"},
    {LH_OVERFLOW_PAGE, "OVERFLOW|"},
    {LH_BUCKET_PAGE, "BUCKET|"},
    {LH_BITMAP_PAGE, "BITMAP|"},
    {LH_META_PAGE, "META|"},
    {LH_BUCKET_BEING_POPULATED, "BUCKET_BEING_POPULATED|"},
    {LH_BUCKET_BEING_SPLIT, "BUCKET_BEING_SPLIT|"},
    {LH_BUCKET_NEEDS_SPLIT_CLEANUP, "BUCKET_NEEDS_SPLIT_CLEANUP|"},
    {LH_PAGE_HAS_DEAD_TUPLES, "PAGE_HAS_DEAD_TUPLES|"}
};

static const FlagMapping gistFlagTable[] = {
    {F_LEAF, "LEAF|"},
    {F_DELETED, "DELETED|"},
    {F_TUPLES_DELETED, "TUPLES_DELETED|"},
    {F_FOLLOW_RIGHT, "FOLLOW_RIGHT|"},
    {F_HAS_GARBAGE, "HAS_GARBAGE|"}
};

static const FlagMapping spgistFlagTable[] = {
    {SPGIST_META, "META|"},
    {SPGIST_DELETED, "DELETED|"},
    {SPGIST_LEAF, "LEAF|"},
    {SPGIST_NULLS, "NULLS|"}
};

static const FlagMapping ginFlagTable[] = {
    {GIN_DATA, "DATA|"},
    {GIN_LEAF, "LEAF|"},
    {GIN_DELETED, "DELETED|"},
    {GIN_META, "META|"},
    {GIN_LIST, "LIST|"},
    {GIN_LIST_FULLROW, "FULLROW|"},
    {GIN_INCOMPLETE_SPLIT, "INCOMPLETESPLIT|"},
    {GIN_COMPRESSED, "COMPRESSED|"}
};

static const FlagMapping pageFlagTable[] = {
    {PD_HAS_FREE_LINES, "HAS_FREE_LINES|"},
    {PD_PAGE_FULL, "PAGE_FULL|"},
    {PD_ALL_VISIBLE, "ALL_VISIBLE|"},
    {PD_COMPRESSED_PAGE, "COMPRESSED_PAGE|"},
    {PD_LOGICAL_PAGE, "LOGICAL_PAGE|"},
    {PD_ENCRYPT_PAGE, "ENCRYPT_PAGE|"},
    {UHEAP_PAGE_FULL, "UPAGE_IS_FULL|"},
    {UHEAP_HAS_FREE_LINES, "UPAGE_HAS_FREE_LINE_POINTERS|"},
    {PD_CHECKSUM_FNV1A, "CHECKSUM_FNV1A|"},
    {PD_JUST_AFTER_FPW, "JUST_AFTER_FPW|"},
    {PD_EXRTO_PAGE, "EXRTO_PAGE|"},
    {PD_TDE_PAGE, "TDE_PAGE|"}
};

static const FlagMapping maskFlags[] = {
    {HEAP_HASNULL, "HASNULL|"},
    {HEAP_HASVARWIDTH, "HASVARWIDTH|"},
    {HEAP_HASEXTERNAL, "HASEXTERNAL|"},
    {HEAP_HASOID, "HASOID|"},
    {HEAP_XMAX_KEYSHR_LOCK, "XMAX_KEYSHR_LOCK|"},
    {HEAP_COMBOCID, "COMBOCID|"},
    {HEAP_XMAX_EXCL_LOCK, "XMAX_EXCL_LOCK|"},
    {HEAP_XMAX_LOCK_ONLY, "XMAX_LOCK_ONLY|"},
    {HEAP_XMIN_COMMITTED, "XMIN_COMMITTED|"},
    {HEAP_XMIN_INVALID, "XMIN_INVALID|"},
    {HEAP_XMAX_COMMITTED, "XMAX_COMMITTED|"},
    {HEAP_XMAX_INVALID, "XMAX_INVALID|"},
    {HEAP_XMAX_IS_MULTI, "XMAX_IS_MULTI|"},
    {HEAP_UPDATED, "UPDATED|"},
    {HEAP_MOVED_OFF, "MOVED_OFF|"},
    {HEAP_MOVED_IN, "MOVED_IN|"},
};

static const FlagMapping mask2Flags[] = {
    {HEAP_KEYS_UPDATED, "KEYS_UPDATED|"},
    {HEAP_HOT_UPDATED, "HOT_UPDATED|"},
    {HEAP_ONLY_TUPLE, "HEAP_ONLY|"},
};

/* Send properly formed usage information to the user. */
static void DisplayOptions(unsigned int validOptions)
{
    if (validOptions == OPT_RC_COPYRIGHT) {
        printf("\nVersion %s (for %s)"
               "\nCopyright (c) 2002-2010 Red Hat, Inc."
               "\nCopyright (c) 2011-2023, PostgreSQL Global Development Group\n",
               FD_VERSION, FD_PG_VERSION);
    }

    printf("\nUsage: gs_filedump [-abcdfhikuxy] [-r relfilenode] [-T reltoastrelid] [-R startblock [endblock]] [-D "
           "attrlist] [-S blocksize] [-s segsize] "
           "[-n segnumber] file\n\n"
           "Display formatted contents of a PostgreSQL heap/index/control file\n"
           "Defaults are: relative addressing, range of the entire file, block\n"
           "               size as listed on block 0 in the file\n\n"
           "The following options are valid for heap and index files:\n"
           "  -a  Display absolute addresses when formatting (Block header\n"
           "      information is always block relative)\n"
           "  -b  Display binary block images within a range (Option will turn\n"
           "      off all formatting options)\n"
           "  -d  Display formatted block content dump (Option will turn off\n"
           "      all other formatting options)\n"
           "  -D  Decode tuples using given comma separated list of types\n"
           "      Supported types:\n"
           "        bigint bigserial bool char charN date float float4 float8 int\n"
           "        json macaddr name numeric oid real serial smallint smallserial text\n"
           "        time timestamp timestamptz timetz uuid varchar varcharN xid xml\n"
           "      ~ ignores all attributes left in a tuple\n"
           "  -f  Display formatted block content dump along with interpretation\n"
           "  -h  Display this information\n"
           "  -i  Display interpreted item details\n"
           "  -k  Verify block checksums\n"
           "  -o  Do not dump old values.\n"
           "  -R  Display specific block ranges within the file (Blocks are\n"
           "      indexed from 0)\n"
           "        [startblock]: block to start at\n"
           "        [endblock]: block to end at\n"
           "      A startblock without an endblock will format the single block\n"
           "  -s  Force segment size to [segsize]\n"
           "  -u  Decode block which storage type is ustore\n"
           "  -t  Dump TOAST files\n"
           "  -v  Ouput additional information about TOAST relations\n"
           "  -n  Force segment number to [segnumber]\n"
           "  -S  Force block size to [blocksize]\n"
           "  -x  Force interpreted formatting of block items as index items\n"
           "  -y  Force interpreted formatting of block items as heap items\n\n"
           "The following options are valid for segment storage table:\n"
           "      When specifying a segmented storage table, the file path must be specified as '{filedir}/1'\n"
           "  -r  Specify the relfilenode [relfilenode] of the table \n"
           "  -T  Specify the relfilenode [reltoastrelid] of the pg_toast of the table\n"
           "      Parameter '-t' will not support\n"
           "The following options are valid for control files:\n"
           "  -c  Interpret the file listed as a control file\n"
           "  -f  Display formatted content dump along with interpretation\n"
           "  -S  Force block size to [blocksize]\n"
           "Additional functions:\n"
           "  -m  Interpret file as pg_filenode.map file and print contents (all\n"
           "      other options will be ignored)\n"
           "\nReport bugs to <pgsql-bugs@postgresql.org>\n");
}

/*
 * Determine segment number by segment file name. For instance, if file
 * name is /path/to/xxxx.7 procedure returns 7. Default return value is 0.
 */
static unsigned int GetSegmentNumberFromFileName(const char *segFileName)
{
    int segnumOffset = strlen(segFileName) - 1;
    if (segnumOffset < 0) {
        return RETURN_SUCCESS;
    }
    while (isdigit(segFileName[segnumOffset])) {
        segnumOffset--;
        if (segnumOffset < 0) {
            return RETURN_SUCCESS;
        }
    }
    if (segFileName[segnumOffset] != '.') {
        return RETURN_SUCCESS;
    }
    return atoi(&segFileName[segnumOffset + 1]);
}

static char *GetFilename(char *path)
{
    char *filename = path;
    for (char *p = path; *p != '\0'; p++) {
        if IS_DIR_SEP (*p) {
            filename = p + 1;
        }
    }
    return (*filename != '\0') ? filename : nullptr;
}

/*	Iterate through the provided options and set the option flags.
 *	An error will result in a positive rc and will force a display
 *	of the usage information.  This routine returns enum
 *	optionReturnCode values. */
static unsigned int ConsumeOptions(int numOptions, char **options)
{
    unsigned int rc = OPT_RC_VALID;
    int x;
    unsigned int optionStringLength;
    char *optionString;
    char duplicateSwitch = 0x00;
    for (x = 1; x < numOptions; x++) {
        optionString = options[x];
        optionStringLength = strlen(optionString);
        /* Range is a special case where we have to consume the next 1 or 2
         * parameters to mark the range start and end */
        if ((optionStringLength == 2) && (strcmp(optionString, "-R") == 0)) {
            int range = 0;
            SET_OPTION(g_blockOptions, BLOCK_RANGE, 'R');
            /* Only accept the range option once */
            if (rc == OPT_RC_DUPLICATE) {
                break;
            }
            /* Make sure there are options after the range identifier */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing range start identifier.\n");
                g_exitCode = 1;
                break;
            }
            /*
             * Mark that we have the range and advance the option to what
             * should be the range start. Check the value of the next
             * parameter */
            optionString = options[++x];
            if ((range = GetOptionValue(optionString)) < 0) {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid range start identifier <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }

            /* The default is to dump only one block */
            g_blockStart = g_blockEnd = static_cast<unsigned int>(range);
            /* We have our range start marker, check if there is an end
             * marker on the option line.  Assume that the last option
             * is the file we are dumping, so check if there are options
             * range start marker and the file */
            if (x <= (numOptions - 3)) {
                if ((range = GetOptionValue(options[x + 1])) >= 0) {
                    /* End range must be => start range */
                    if (g_blockStart <= range) {
                        g_blockEnd = static_cast<unsigned int>(range);
                        x++;
                    } else {
                        rc = OPT_RC_INVALID;
                        printf("Error: Requested block range start <%d> is "
                               "greater than end <%d>.\n",
                               g_blockStart, range);
                        g_exitCode = 1;
                        break;
                    }
                }
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-S") == 0)) {
            int localBlockSize;
            /* Check for the special case where the user forces a block size
             * instead of having the tool determine it.  This is useful if
             * the header of block 0 is corrupt and gives a garbage block size */
            SET_OPTION(g_blockOptions, BLOCK_FORCED, 'S');
            /* Only accept the forced size option once */
            if (rc == OPT_RC_DUPLICATE) {
                break;
            }

            /* The token immediately following -S is the block size */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing block size identifier.\n");
                break;
            }
            /* Next option encountered must be forced block size */
            optionString = options[++x];
            if ((localBlockSize = GetOptionValue(optionString)) > 0) {
                g_blockSize = static_cast<unsigned int>(localBlockSize);
            } else {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid block size requested <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-s") == 0)) {
            int localSegmentSize;
            /* Check for the special case where the user forces a segment size. */
            SET_OPTION(g_segmentOptions, SEGMENT_SIZE_FORCED, 's');
            /* Only accept the forced size option once */
            if (rc == OPT_RC_DUPLICATE) {
                break;
            }
            /* The token immediately following -s is the segment size */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing segment size identifier.\n");
                g_exitCode = 1;
                break;
            }
            /* Next option encountered must be forced segment size */
            optionString = options[++x];
            if ((localSegmentSize = GetOptionValue(optionString)) > 0) {
                g_segmentSize = static_cast<unsigned int>(localSegmentSize);
            } else {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid segment size requested <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-r") == 0)) {
            int localTableRelfilenode;
            /* The token immediately following -r is attrubute types string */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing table relfilenode string.\n");
                g_exitCode = 1;
                break;
            }
            /* Next option encountered must be forced segment size */
            optionString = options[++x];
            if ((localTableRelfilenode = GetOptionValue(optionString)) > 0) {
                g_tableRelfilenode = static_cast<int>(localTableRelfilenode);
            } else {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid segment size requested <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-T") == 0)) {
            int localToastRelfilenode;
            SET_OPTION(g_blockOptions, BLOCK_DECODE_TOAST, 't');
            /* The token immediately following -r is attrubute types string */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing toast relfilenode string.\n");
                g_exitCode = 1;
                break;
            }

            /* Next option encountered must be forced segment size */
            optionString = options[++x];
            if ((localToastRelfilenode = GetOptionValue(optionString)) > 0) {
                g_toastRelfilenode = static_cast<int>(localToastRelfilenode);
            } else {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid segment size requested <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-D") == 0)) {
            /* Check for the special case where the user forces tuples decoding. */
            SET_OPTION(g_blockOptions, BLOCK_DECODE, 'D');
            /* Only accept the decode option once */
            if (rc == OPT_RC_DUPLICATE) {
                break;
            }
            /* The token immediately following -D is attrubute types string */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing attribute types string.\n");
                g_exitCode = 1;
                break;
            }
            /* Next option encountered must be attribute types string */
            optionString = options[++x];
            if (ParseAttributeTypesString(optionString) < 0) {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid attribute types string <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if ((optionStringLength == 2) && (strcmp(optionString, "-n") == 0)) {
            /* Check for the special case where the user forces a segment number
             * instead of having the tool determine it by file name. */
            int localSegmentNumber;
            SET_OPTION(g_segmentOptions, SEGMENT_NUMBER_FORCED, 'n');
            /* Only accept the forced segment number option once */
            if (rc == OPT_RC_DUPLICATE) {
                break;
            }
            /* The token immediately following -n is the segment number */
            if (x >= (numOptions - 2)) {
                rc = OPT_RC_INVALID;
                printf("Error: Missing segment number identifier.\n");
                g_exitCode = 1;
                break;
            }
            /* Next option encountered must be forced segment number */
            optionString = options[++x];
            if ((localSegmentNumber = GetOptionValue(optionString)) > 0) {
                g_segmentNumber = static_cast<unsigned int>(localSegmentNumber);
            } else {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid segment number requested <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
        } else if (x == (numOptions - 1)) {
            /* The last option MUST be the file name */
            /* Check to see if this looks like an option string before opening */
            if (optionString[0] != '-') {
                char *segMataFile = nullptr;
                char *segToastMataFile = nullptr;
                if (strcmp(GetFilename(optionString), "1") == 0) {
                    g_isSegment = true;
                    if (not(g_blockOptions & BLOCK_RANGE)) {
                        SET_OPTION(g_blockOptions, BLOCK_RANGE, 'R');
                    }
                    if (g_tableRelfilenode < 0) {
                        rc = OPT_RC_INVALID;
                        printf("Error: `-r` is requested <%s>.\n", optionString);
                        g_exitCode = 1;
                        break;
                    } else if ((g_blockOptions & BLOCK_DECODE_TOAST) && (g_toastRelfilenode < 0)) {
                        rc = OPT_RC_INVALID;
                        printf("Error: `-t` 2 `-T [g_toastRelfilenode]` <%s>.\n", optionString);
                        g_exitCode = 1;
                        break;
                    }
                    if (g_toastRelfilenode > 0) {
                        segToastMataFile = SliceFilename(optionString, (g_toastRelfilenode / DF_FILE_SLICE_BLOCKS));
                        fpToast = fopen(segToastMataFile, "rb");
                        if (not fpToast) {
                            rc = OPT_RC_FILE;
                            printf("Error: Could not open file <%s>.\n", segToastMataFile);
                            g_exitCode = 1;
                            break;
                        }
                    }
                }

                if (g_isSegment) {
                    segMataFile = SliceFilename(optionString, (g_tableRelfilenode / DF_FILE_SLICE_BLOCKS));
                    fp = fopen(segMataFile, "rb");
                } else {
                    fp = fopen(optionString, "rb");
                }
                if (fp) {
                    g_fileName = options[x];
                    if (!(g_segmentOptions & SEGMENT_NUMBER_FORCED)) {
                        g_segmentNumber = GetSegmentNumberFromFileName(g_fileName);
                    }
                } else {
                    rc = OPT_RC_FILE;
                    printf("Error: Could not open file <%s>.\n", optionString);
                    g_exitCode = 1;
                    break;
                }
            } else {
                /* Could be the case where the help flag is used without a
                 * filename. Otherwise, the last option isn't a file */
                if (strcmp(optionString, "-h") == 0) {
                    rc = OPT_RC_COPYRIGHT;
                } else {
                    rc = OPT_RC_FILE;
                    printf("Error: Missing file name to dump.\n");
                    g_exitCode = 1;
                }
                break;
            }
        } else {
            unsigned int y;

            /* Option strings must start with '-' and contain switches */
            if (optionString[0] != '-') {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid option string <%s>.\n", optionString);
                g_exitCode = 1;
                break;
            }
            /* Iterate through the singular option string, throw out
             * garbage, duplicates and set flags to be used in formatting */
            for (y = 1; y < optionStringLength; y++) {
                switch (optionString[y]) {
                        /* Use absolute addressing */
                    case 'a':
                        SET_OPTION(g_blockOptions, BLOCK_ABSOLUTE, 'a');
                        break;

                        /* Dump the binary contents of the page */
                    case 'b':
                        SET_OPTION(g_blockOptions, BLOCK_BINARY, 'b');
                        break;

                        /* Dump the listed file as a control file */
                    case 'c':
                        SET_OPTION(g_controlOptions, CONTROL_DUMP, 'c');
                        break;

                        /* Do not interpret the data. Format to hex and ascii. */
                    case 'd':
                        SET_OPTION(g_blockOptions, BLOCK_NO_INTR, 'd');
                        break;

                    case 'u':
                        SET_OPTION(g_blockOptions, BLOCK_USTORE, 'u');
                        break;

                        /*
                         * Format the contents of the block with
                         * interpretation of the headers */
                    case 'f':
                        SET_OPTION(g_blockOptions, BLOCK_FORMAT, 'f');
                        break;

                        /* Display the usage screen */
                    case 'h':
                        rc = OPT_RC_COPYRIGHT;
                        break;

                        /* Format the items in detail */
                    case 'i':
                        SET_OPTION(g_itemOptions, ITEM_DETAIL, 'i');
                        break;

                        /* Verify block checksums */
                    case 'k':
                        SET_OPTION(g_blockOptions, BLOCK_CHECKSUMS, 'k');
                        break;

                        /* Treat file as pg_filenode.map file */
                    case 'm':
                        g_isRelMapFile = true;
                        break;

                        /* Display old values. Ignore Xmax */
                    case 'o':
                        SET_OPTION(g_blockOptions, BLOCK_IGNORE_OLD, 'o');
                        break;

                    case 't':
                        SET_OPTION(g_blockOptions, BLOCK_DECODE_TOAST, 't');
                        break;

                    case 'v':
                        g_verbose = true;
                        break;

                        /* Interpret items as standard index values */
                    case 'x':
                        SET_OPTION(g_itemOptions, ITEM_INDEX, 'x');
                        if (g_itemOptions & ITEM_HEAP) {
                            rc = OPT_RC_INVALID;
                            printf("Error: Options <y> and <x> are "
                                   "mutually exclusive.\n");
                            g_exitCode = 1;
                        }
                        break;

                        /* Interpret items as heap values */
                    case 'y':
                        SET_OPTION(g_itemOptions, ITEM_HEAP, 'y');
                        if (g_itemOptions & ITEM_INDEX) {
                            rc = OPT_RC_INVALID;
                            printf("Error: Options <x> and <y> are "
                                   "mutually exclusive.\n");
                            g_exitCode = 1;
                        }
                        break;

                    default:
                        rc = OPT_RC_INVALID;
                        printf("Error: Unknown option <%c>.\n", optionString[y]);
                        g_exitCode = 1;
                        break;
                }

                if (rc) {
                    break;
                }
            }
        }
    }
    g_isUHeap = g_blockOptions & BLOCK_USTORE;
    if (g_isSegment && g_isUHeap) {
        rc = OPT_RC_INVALID;
        printf("Error: `-u` is not supported when segment is on.\n");
        g_exitCode = 1;
    }
    if (rc == OPT_RC_DUPLICATE) {
        printf("Error: Duplicate option listed <%c>.\n", duplicateSwitch);
        g_exitCode = 1;
    }

    /* If the user requested a control file dump, a pure binary
     * block dump or a non-interpreted formatted dump, mask off
     * all other block level options (with a few exceptions) */
    if (rc == OPT_RC_VALID) {
        /* The user has requested a control file dump, only -f and */
        /* -S are valid... turn off all other formatting */
        if (g_controlOptions & CONTROL_DUMP) {
            if ((g_blockOptions & ~(BLOCK_FORMAT | BLOCK_FORCED)) || (g_itemOptions)) {
                rc = OPT_RC_INVALID;
                printf("Error: Invalid options used for Control File dump.\n"
                       "       Only options <Sf> may be used with <c>.\n");
                g_exitCode = 1;
            } else {
                g_controlOptions |= (g_blockOptions & (BLOCK_FORMAT | BLOCK_FORCED));
                g_blockOptions = g_itemOptions = 0;
            }
        } else if (g_blockOptions & BLOCK_BINARY) {
            /* The user has requested a binary block dump... only -R and -f are honoured */
            g_blockOptions &= (BLOCK_BINARY | BLOCK_RANGE | BLOCK_FORCED);
            g_itemOptions = 0;
        } else if (g_blockOptions & BLOCK_NO_INTR) {
            /* The user has requested a non-interpreted dump... only -a, -R and -f are honoured */
            g_blockOptions &= (BLOCK_NO_INTR | BLOCK_ABSOLUTE | BLOCK_RANGE | BLOCK_FORCED);
            g_itemOptions = 0;
        }
    }
    return (rc);
}

/* Given the index into the parameter list, convert and return the
 * current string to a number if possible */
static int GetOptionValue(char *optionString)
{
    int x;
    int value = -1;
    int optionStringLength = strlen(optionString);

    /* Verify the next option looks like a number */
    for (x = 0; x < optionStringLength; x++) {
        if (!isdigit(static_cast<unsigned char>(optionString[x]))) {
            break;
        }
    }
    /* Convert the string to a number if it looks good */
    if (x == optionStringLength) {
        value = atoi(optionString);
    }
    return (value);
}

/* Read the page header off of block 0 to determine the block size
 * used in this file.  Can be overridden using the -S option. The
 * returned value is the block size of block 0 on disk */
unsigned int GetBlockSize(FILE *fp)
{
    unsigned int localSize = 0;
    int bytesRead = 0;
    size_t headerSize = g_isUHeap ? sizeof(UHeapPageHeaderData) : sizeof(PageHeaderData);
    char localCache[headerSize];
    /* Read the first header off of block 0 to determine the block size */
    bytesRead = fread(&localCache, 1, headerSize, fp);
    rewind(fp);

    if (static_cast<size_t>(bytesRead) == headerSize) {
        if (g_isUHeap) {
            localSize = static_cast<unsigned int>(
                (Size)(((UHeapPageHeader)(localCache))->pd_pagesize_version & (uint16)0xFF00));
        } else {
            localSize = static_cast<unsigned int>(PageGetPageSize(localCache));
        }
    } else {
        printf("Error: Unable to read full page header from block 0.\n"
               "  ===> Read %d bytes\n",
               bytesRead);
        g_exitCode = 1;
    }

    if (localSize == 0) {
        printf("Notice: Block size determined from reading block 0 is zero, using default %d instead.\n", BLCKSZ);
        printf("Hint: Use -S <size> to specify the size manually.\n");
        localSize = BLCKSZ;
    }

    return (localSize);
}

/* Determine the contents of the special section on the block and
 * return this enum value */
static unsigned int GetSpecialSectionType(char *buffer, Page page)
{
    unsigned int rc;
    unsigned int specialOffset;
    unsigned int specialSize;
    unsigned int specialValue;
    void *pageHeader = g_isUHeap ? (void *)((UHeapPageHeader)page) : (void *)((PageHeader)page);

    /* If this is not a partial header, check the validity of the
     * special section offset and contents */
    if (g_bytesToFormat > (g_isUHeap ? sizeof(UHeapPageHeaderData) : sizeof(PageHeaderData))) {
        specialOffset = static_cast<unsigned int>(g_isUHeap ? ((UHeapPageHeader)pageHeader)->pd_special
                                                            : ((PageHeader)pageHeader)->pd_special);

        /* Check that the special offset can remain on the block or
         * the partial block */
        if ((specialOffset == 0) || (specialOffset > g_blockSize) || (specialOffset > g_bytesToFormat)) {
            rc = SPEC_SECT_ERROR_BOUNDARY;
        } else {
            /* we may need to examine last 2 bytes of page to identify index */
            uint16 *ptype = reinterpret_cast<uint16 *>(const_cast<char *>(buffer) + g_blockSize - sizeof(uint16));

            specialSize = g_blockSize - specialOffset;

            /* If there is a special section, use its size to guess its
             * contents, checking the last 2 bytes of the page in cases
             * that are ambiguous.  Note we don't attempt to dereference
             * the pointers without checking g_bytesToFormat == g_blockSize. */
            if (specialSize == 0) {
                rc = SPEC_SECT_NONE;
            } else if (specialSize == MAXALIGN(sizeof(uint32))) {
                /* If MAXALIGN is 8, this could be either a sequence or
                 * SP-GiST or GIN. */
                if (g_bytesToFormat == g_blockSize) {
                    specialValue = *reinterpret_cast<int *>(buffer + specialOffset);
                    if (specialValue == SEQUENCE_MAGIC) {
                        rc = SPEC_SECT_SEQUENCE;
                    } else if (specialSize == MAXALIGN(sizeof(SpGistPageOpaqueData)) && *ptype == SPGIST_PAGE_ID) {
                        rc = SPEC_SECT_INDEX_SPGIST;
                    } else if (specialSize == MAXALIGN(sizeof(GinPageOpaqueData))) {
                        rc = SPEC_SECT_INDEX_GIN;
                    } else {
                        rc = SPEC_SECT_ERROR_UNKNOWN;
                    }
                } else {
                    rc = SPEC_SECT_ERROR_UNKNOWN;
                }
            } else if (specialSize == MAXALIGN(sizeof(SpGistPageOpaqueData)) && g_bytesToFormat == g_blockSize &&
                       *ptype == SPGIST_PAGE_ID) {
                /* SP-GiST and GIN have same size special section, so check the page ID bytes first. */
                rc = SPEC_SECT_INDEX_SPGIST;
            } else if (specialSize == MAXALIGN(sizeof(GinPageOpaqueData))) {
                rc = SPEC_SECT_INDEX_GIN;
            } else if (specialSize > 2 && g_bytesToFormat == g_blockSize) {
                /* As of 8.3, BTree, Hash, and GIST all have the same size
                 * special section, but the last two bytes of the section
                 * can be checked to determine what's what. */
                if (*ptype <= MAX_BT_CYCLE_ID && specialSize == MAXALIGN(sizeof(BTPageOpaqueData))) {
                    rc = SPEC_SECT_INDEX_BTREE;
                } else if (*ptype == HASHO_PAGE_ID && specialSize == MAXALIGN(sizeof(HashPageOpaqueData))) {
                    rc = SPEC_SECT_INDEX_HASH;
                } else if (*ptype == GIST_PAGE_ID && specialSize == MAXALIGN(sizeof(GISTPageOpaqueData))) {
                    rc = SPEC_SECT_INDEX_GIST;
                } else {
                    rc = SPEC_SECT_ERROR_UNKNOWN;
                }
            } else {
                rc = SPEC_SECT_ERROR_UNKNOWN;
            }
        }
    } else {
        rc = SPEC_SECT_ERROR_UNKNOWN;
    }

    return (rc);
}

/*	Check whether page is a btree meta page */
static bool IsBtreeMetaPage(Page page)
{
    PageHeader pageHeader = (PageHeader)page;
    if ((PageGetSpecialSize(page) == (MAXALIGN(sizeof(BTPageOpaqueData)))) && (g_bytesToFormat == g_blockSize)) {
        BTPageOpaque btpo = (BTPageOpaque)((char *)page + pageHeader->pd_special);

        /* Must check the cycleid to be sure it's really btree. */
        if ((btpo->bt_internal.btpo_cycleid <= MAX_BT_CYCLE_ID) && (btpo->bt_internal.btpo_flags & BTP_META)) {
            return true;
        }
    }
    return false;
}

/*	Check whether page is a gin meta page */
static bool IsGinMetaPage(Page page)
{
    if ((PageGetSpecialSize(page) == (MAXALIGN(sizeof(GinPageOpaqueData)))) && (g_bytesToFormat == g_blockSize)) {
        GinPageOpaque gpo = GinPageGetOpaque(page);
        if (gpo->flags & GIN_META) {
            return true;
        }
    }
    return false;
}

/*	Check whether page is a gin leaf page */
static bool IsGinLeafPage(Page page)
{
    if ((PageGetSpecialSize(page) == (MAXALIGN(sizeof(GinPageOpaqueData)))) && (g_bytesToFormat == g_blockSize)) {
        GinPageOpaque gpo = GinPageGetOpaque(page);
        if (gpo->flags & GIN_LEAF) {
            return true;
        }
    }
    return false;
}

/*	Check whether page is a gin leaf page */
static bool IsUHeapGinLeafPage(Page page)
{
    if ((PageGetSpecialSize(page) == ((sizeof(GinPageOpaqueData)))) && (g_bytesToFormat == g_blockSize)) {
        GinPageOpaque gpo = GinPageGetOpaque(page);
        if (gpo->flags & GIN_LEAF) {
            return true;
        }
    }
    return false;
}

/* Check whether page is a SpGist meta page */
static bool IsSpGistMetaPage(Page page)
{
    if ((PageGetSpecialSize(page) == ((sizeof(SpGistPageOpaqueData)))) && (g_bytesToFormat == g_blockSize)) {
        SpGistPageOpaque spgpo = SpGistPageGetOpaque(page);
        if ((spgpo->spgist_page_id == SPGIST_PAGE_ID) && (spgpo->flags & SPGIST_META)) {
            return true;
        }
    }
    return false;
}

/* Display a header for the dump so we know the file name, the options
 * used and the time the dump was taken */
static void CreateDumpFileHeader(int numOptions, char **options)
{
    char optionBuffer[52] = "\0";
    /* Iterate through the options and cache them.
     * The maximum we can display is 50 option characters + spaces. */
    for (int x = 1; x < (numOptions - 1); x++) {
        if ((strlen(optionBuffer) + strlen(options[x])) > MAX_OPTION_LINE_LENGTH) {
            break;
        }
        int len = strlen(optionBuffer);
        int res = snprintf(optionBuffer + len, sizeof(optionBuffer) - len, "%s", options[x]);
        if (res < 0 || res >= static_cast<int>(sizeof(optionBuffer) - len)) {
            printf(" Error: Dump FileHeader Failed.\n\n");
        }
        if (x < numOptions - 2) {
            errno_t rc = strcat_s(optionBuffer, sizeof(optionBuffer), " ");
            securec_check(rc, "\0", "\0");
        }
    }
    printf("\n*******************************************************************\n"
           "* PostgreSQL File/Block Formatted Dump Utility\n"
           "*\n"
           "* File: %s\n"
           "* Options used: %s\n"
           "*******************************************************************\n",
           g_fileName, (strlen(optionBuffer)) ? optionBuffer : "None");
}

/*	Dump out a formatted block header for the requested block */
static int FormatHeader(char *buffer, Page page, BlockNumber blkno, bool isToast)
{
    int rc = 0;
    unsigned int headerBytes;
    PageHeader pageHeader = (PageHeader)page;
    const char *indent = isToast ? "\t" : "";
    if (!isToast || g_verbose) {
        printf("%s<Header> -----\n", indent);
    }
    /* Only attempt to format the header if the entire header (minus the item
     * array) is available */
    if (g_bytesToFormat < offsetof(PageHeaderData, pd_linp[0])) {
        headerBytes = g_bytesToFormat;
        rc = EOF_ENCOUNTERED;
    } else {
        XLogRecPtr pageLSN = PageGetLSN(page);
        unsigned int maxOffset = PageGetMaxOffsetNumber(page);
        char flagString[100];

        headerBytes = offsetof(PageHeaderData, pd_linp[0]);
        g_blockVersion = (unsigned int)PageGetPageLayoutVersion(page);

        /* The full header exists but we have to check that the item array
         * is available or how far we can index into it */
        if (maxOffset > 0) {
            unsigned int itemsLength = maxOffset * sizeof(ItemIdData);

            if (g_bytesToFormat < (headerBytes + itemsLength)) {
                headerBytes = g_bytesToFormat;
                rc = EOF_ENCOUNTERED;
            } else {
                headerBytes += itemsLength;
            }
        }

        flagString[0] = '\0';
        errno_t rc;
        for (const auto& entry : pageFlagTable) {
            if (pageHeader->pd_flags & entry.mask) {
                rc = strcat_s(flagString, sizeof(flagString), entry.name);
                securec_check(rc, "\0", "\0");
            }
        }
        
        if (strlen(flagString)) {
            flagString[strlen(flagString) - 1] = '\0';
        }
        /* Interpret the content of the header */
        if (!isToast || g_verbose) {
            printf("%s Block Offset: 0x%08x         Offsets: Lower    %4u (0x%04hx)\n", indent, g_pageOffset,
                   pageHeader->pd_lower, pageHeader->pd_lower);
            printf("%s Block: Size %4d  Version %4u            Upper    %4u (0x%04hx)\n", indent,
                   (int)PageGetPageSize(page), g_blockVersion, pageHeader->pd_upper, pageHeader->pd_upper);
            printf("%s LSN:  logid %6d recoff 0x%08x      Special  %4u (0x%04hx)\n", indent, (uint32)(pageLSN >> 32),
                   (uint32)pageLSN, pageHeader->pd_special, pageHeader->pd_special);
            printf("%s Items: %4u                      Free Space: %4u\n", indent, maxOffset,
                   pageHeader->pd_upper - pageHeader->pd_lower);
            printf("%s Checksum: 0x%04x  Prune XID: 0x%08lx  Flags: 0x%04x (%s)\n", indent, pageHeader->pd_checksum,
                   ((HeapPageHeader)(page))->pd_prune_xid + ((HeapPageHeader)(page))->pd_xid_base, pageHeader->pd_flags,
                   flagString);
            printf("%s Length (including item array): %u\n\n", indent, headerBytes);
        }

        /* If it's a btree meta page, print the contents of the meta block. */
        if (IsBtreeMetaPage(page)) {
            BTMetaPageData *btpMeta = BTPageGetMeta(buffer);

            if (!isToast || g_verbose) {
                printf("%s BTree Meta Data:  Magic (0x%08x)   Version (%u)\n", indent, btpMeta->btm_magic,
                       btpMeta->btm_version);
                printf("%s                   Root:     Block (%u)  Level (%u)\n", indent, btpMeta->btm_root,
                       btpMeta->btm_level);
                printf("%s                   FastRoot: Block (%u)  Level (%u)\n\n", indent, btpMeta->btm_fastroot,
                       btpMeta->btm_fastlevel);
            }
            headerBytes += sizeof(BTMetaPageData);
        }

        /* Eye the contents of the header and alert the user to possible
         * problems. */
        if ((maxOffset < 0) || (maxOffset > g_blockSize) || (pageHeader->pd_upper > g_blockSize) ||
            (pageHeader->pd_upper > pageHeader->pd_special) ||
            (pageHeader->pd_lower < (sizeof(PageHeaderData) - sizeof(ItemIdData))) ||
            (pageHeader->pd_lower > g_blockSize) || (pageHeader->pd_upper < pageHeader->pd_lower) ||
            (pageHeader->pd_special > g_blockSize)) {
            printf(" Error: Invalid header information.\n\n");
            g_exitCode = 1;
        }

        if (g_blockOptions & BLOCK_CHECKSUMS) {
            uint32 delta = (g_segmentSize / g_blockSize) * g_segmentNumber;
            uint16 calc_checksum = pg_checksum_page(page, delta + blkno);
            if (calc_checksum != pageHeader->pd_checksum) {
                printf(" Error: checksum failure: calculated 0x%04x.\n\n", calc_checksum);
                g_exitCode = 1;
            }
        }
    }
    /* If we have reached the end of file while interpreting the header, let
     * the user know about it */
    if (rc == EOF_ENCOUNTERED) {
        if (!isToast || g_verbose) {
            printf("%s Error: End of block encountered within the header."
                   " Bytes read: %4u.\n\n",
                   indent, g_bytesToFormat);
        }
        g_exitCode = 1;
    }
    /* A request to dump the formatted binary of the block (header,
     * items and special section).  It's best to dump even on an error
     * so the user can see the raw image. */
    if (g_blockOptions & BLOCK_FORMAT) {
        FormatBinary(buffer, headerBytes, 0);
    }
    return (rc);
}

/*	Dump out a formatted block header for the requested block */
static int FormatUHeapHeader(char *buffer, Page page, BlockNumber blkno, bool isToast)
{
    int rc = 0;
    unsigned int headerBytes;
    UHeapPageHeader upageHeader = (UHeapPageHeader)page;
    const char *indent = isToast ? "\t" : "";
    if (!isToast || g_verbose) {
        printf("%s<Header> -----\n", indent);
    }
    /* Only attempt to format the header if the entire header (minus the item
     * array) is available */
    if (g_bytesToFormat < offsetof(UHeapPageHeaderData, td_count)) {
        headerBytes = g_bytesToFormat;
        rc = EOF_ENCOUNTERED;
    } else {
        unsigned int maxOffset = UHeapPageGetMaxOffsetNumber(page);
        char flagString[100];
        headerBytes = SizeOfUHeapPageHeaderData;
        g_blockVersion = (unsigned int)(upageHeader->pd_pagesize_version);

        /* The full header exists but we have to check that the item array
         * is available or how far we can index into it */
        if (maxOffset > 0) {
            unsigned int itemsLength = maxOffset * sizeof(ItemIdData);

            if (g_bytesToFormat < (headerBytes + itemsLength)) {
                headerBytes = g_bytesToFormat;
                rc = EOF_ENCOUNTERED;
            } else {
                headerBytes += itemsLength;
            }
        }

        flagString[0] = '\0';
        errno_t rc;
        for (const auto& entry : pageFlagTable) {
            if (upageHeader->pd_flags & entry.mask) {
                rc = strcat_s(flagString, sizeof(flagString), entry.name);
                securec_check(rc, "\0", "\0");
            }
        }
        if (strlen(flagString)) {
            flagString[strlen(flagString) - 1] = '\0';
        }

        /* Interpret the content of the header */
        if (!isToast || g_verbose) {
            printf("%s Block Offset: 0x%08x         Offsets: Lower    %4u (0x%04hx)\n", indent, g_pageOffset,
                   upageHeader->pd_lower, upageHeader->pd_lower);
            printf("%s Block: Size %4d  Version %4u            Upper    %4u (0x%04hx)\n", indent,
                   (int)PageGetPageSize(page), g_blockVersion, upageHeader->pd_upper, upageHeader->pd_upper);
            printf("%s PD_LSN: %X/0x%08lx,   Special  %4u (0x%04hx)\n", indent, upageHeader->pd_lsn.xlogid,
                   ((uint64)upageHeader->pd_lsn.xlogid << XLOG_LSN_HIGH_OFF) + upageHeader->pd_lsn.xrecoff,
                   upageHeader->pd_special, upageHeader->pd_special);
            printf("%s Items: %4u                      Free Space: %d\n", indent, maxOffset,
                   upageHeader->pd_upper - upageHeader->pd_lower);
            printf("%s Checksum: 0x%04x  Prune XID: 0x%08lx  Flags: 0x%04x (%s)\n", indent, upageHeader->pd_checksum,
                   ((HeapPageHeader)(page))->pd_prune_xid + ((HeapPageHeader)(page))->pd_xid_base,
                   upageHeader->pd_flags, flagString);
            printf("%s Length (including item array): %u\n\n", indent, headerBytes);
        }

        /* If it's a btree meta page, print the contents of the meta block. */
        if (IsBtreeMetaPage(page)) {
            BTMetaPageData *btpMeta = BTPageGetMeta(buffer);

            if (!isToast || g_verbose) {
                printf("%s BTree Meta Data:  Magic (0x%08x)   Version (%u)\n", indent, btpMeta->btm_magic,
                       btpMeta->btm_version);
                printf("%s                   Root:     Block (%u)  Level (%u)\n", indent, btpMeta->btm_root,
                       btpMeta->btm_level);
                printf("%s                   FastRoot: Block (%u)  Level (%u)\n\n", indent, btpMeta->btm_fastroot,
                       btpMeta->btm_fastlevel);
            }
            headerBytes += sizeof(BTMetaPageData);
        }

        /* Eye the contents of the header and alert the user to possible
         * problems. */
        if ((maxOffset < 0) || (maxOffset > g_blockSize) || (upageHeader->pd_upper > g_blockSize) ||
            (upageHeader->pd_upper > upageHeader->pd_special) ||
            (upageHeader->pd_lower < (sizeof(PageHeaderData) - sizeof(ItemIdData))) ||
            (upageHeader->pd_lower > g_blockSize) || (upageHeader->pd_upper < upageHeader->pd_lower) ||
            (upageHeader->pd_special > g_blockSize)) {
            printf(" Error: Invalid header information.\n\n");
            g_exitCode = 1;
        }

        if (g_blockOptions & BLOCK_CHECKSUMS) {
            uint32 delta = (g_segmentSize / g_blockSize) * g_segmentNumber;
            uint16 calc_checksum = pg_checksum_page(page, delta + blkno);
            if (calc_checksum != upageHeader->pd_checksum) {
                printf(" Error: checksum failure: calculated 0x%04x.\n\n", calc_checksum);
                g_exitCode = 1;
            }
        }
    }

    /* If we have reached the end of file while interpreting the header, let
     * the user know about it */
    if (rc == EOF_ENCOUNTERED) {
        if (!isToast || g_verbose) {
            printf("%s Error: End of block encountered within the header."
                   " Bytes read: %4u.\n\n",
                   indent, g_bytesToFormat);
        }
        g_exitCode = 1;
    }

    /* A request to dump the formatted binary of the block (header,
     * items and special section).  It's best to dump even on an error
     * so the user can see the raw image. */
    if (g_blockOptions & BLOCK_FORMAT) {
        FormatBinary(buffer, headerBytes, 0);
    }

    return (rc);
}

/* Copied from ginpostinglist.c */
constexpr int MAX_HEAP_TUPLES_PER_PAGE_BITS = 11;
static uint64 itemptr_to_uint64(const ItemPointer iptr)
{
    uint64 val = 0;
    val = GinItemPointerGetBlockNumber(iptr);
    val <<= MAX_HEAP_TUPLES_PER_PAGE_BITS;
    val |= GinItemPointerGetOffsetNumber(iptr);
    return val;
}

static void uint64_to_itemptr(uint64 val, ItemPointer iptr)
{
    GinItemPointerSetOffsetNumber(iptr, val & ((1 << MAX_HEAP_TUPLES_PER_PAGE_BITS) - 1));
    val = val >> MAX_HEAP_TUPLES_PER_PAGE_BITS;
    GinItemPointerSetBlockNumber(iptr, val);
}

/*
 * Decode varbyte-encoded integer at *ptr. *ptr is incremented to next integer.
 */
static uint64 decode_varbyte(unsigned char **ptr)
{
    uint64 val = 0;
    unsigned char *p = *ptr;
    int shift = 0;

    while (true) {
        uint64 c = *(p++);
        val |= (c & VARBYTE_DATA_BITS) << shift;
        if (!(c & VARBYTE_CONTINUATION_BIT)) {
            break;
        }
        shift += VARBYTE_SHIFT_PER_BYTE;
        if (shift > (VARBYTE_SHIFT_PER_BYTE * MAX_VARBYTE_SHIFT_BYTES)) {
            Assert((c & VARBYTE_CONTINUATION_BIT) == 0);
            break;
        }
    }
    
    *ptr = p;
    return val;
}

/*	Dump out gin-specific content of block */
static void FormatGinBlock(char *buffer, bool isToast, Oid toastOid, unsigned int toastExternalSize, char *toastValue,
                           unsigned int *toastRead)
{
    Page page = (Page)buffer;
    const char *indent = isToast ? "\t" : "";
    if (isToast && !g_verbose) {
        return;
    }
    printf("%s<Data> -----\n", indent);
    if (IsGinLeafPage(page)) {
        if (GinPageIsCompressed(page)) {
            GinPostingList *seg = GinDataLeafPageGetPostingList(page);
            int plistIdx = 1;
            Size len = GinDataLeafPageGetPostingListSize(page);
            Pointer endptr = ((Pointer)seg) + len;
            ItemPointer cur;

            while ((Pointer)seg < endptr) {
                int itemIdx = 1;
                uint64 val;
                unsigned char *endseg = seg->bytes + seg->nbytes;
                unsigned char *ptr = seg->bytes;

                cur = &seg->first;
                printf("\n%s Posting List	%3d -- Length: %4u\n", indent, plistIdx, seg->nbytes);
                printf("%s	ItemPointer %3d -- Block Id: %4u linp Index: %4u\n", indent, itemIdx,
                       ((uint32)((cur->ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)cur->ip_blkid.bi_lo)),
                       cur->ip_posid);

                val = itemptr_to_uint64(&seg->first);
                while (ptr < endseg) {
                    val += decode_varbyte(&ptr);
                    itemIdx++;

                    uint64_to_itemptr(val, cur);
                    printf("%s	ItemPointer %3d -- Block Id: %4u linp Index: %4u\n", indent, itemIdx,
                           ((uint32)((cur->ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)cur->ip_blkid.bi_lo)),
                           cur->ip_posid);
                }
                plistIdx++;
                seg = GinNextPostingListSegment(seg);
            }
        } else {
            int i = GinPageGetOpaque(page)->maxoff;
            int nitems = GinPageGetOpaque(page)->maxoff;
            ItemPointer items = (ItemPointer)GinDataPageGetData(page);
            for (i = 0; i < nitems; i++) {
                printf("%s ItemPointer %d -- Block Id: %u linp Index: %u\n", indent, i + 1,
                       ((uint32)((items[i].ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)items[i].ip_blkid.bi_lo)),
                       items[i].ip_posid);
            }
        }
    } else {
        OffsetNumber cur = GinPageGetOpaque(page)->maxoff;
        OffsetNumber high = GinPageGetOpaque(page)->maxoff;
        PostingItem *pitem = NULL;
        for (cur = FirstOffsetNumber; cur <= high; cur = OffsetNumberNext(cur)) {
            pitem = GinDataPageGetPostingItem(page, cur);
            printf("%s PostingItem %d -- child Block Id: (%u) Block Id: %u linp Index: %u\n", indent, cur,
                   ((uint32)((pitem->child_blkno.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)pitem->child_blkno.bi_lo)),
                   ((uint32)((pitem->key.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)pitem->key.ip_blkid.bi_lo)),
                   pitem->key.ip_posid);
        }
    }
    printf("\n");
}

/*	Dump out gin-specific content of block */
static void FormatUHeapGinBlock(char *buffer, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                                char *toastValue, unsigned int *toastRead)
{
    Page page = (Page)buffer;
    const char *indent = isToast ? "\t" : "";
    if (isToast && !g_verbose) {
        return;
    }
    printf("%s<Data> -----\n", indent);
    if (IsUHeapGinLeafPage(page)) {
        if (GinPageIsCompressed(page)) {
            GinPostingList *seg = GinDataLeafPageGetPostingList(page);
            int plistIdx = 1;
            Size len = GinDataLeafPageGetPostingListSize(page);
            Pointer endptr = ((Pointer)seg) + len;
            ItemPointer cur;

            while ((Pointer)seg < endptr) {
                int itemIdx = 1;
                uint64 val;
                unsigned char *endseg = seg->bytes + seg->nbytes;
                unsigned char *ptr = seg->bytes;
                cur = &seg->first;
                printf("\n%s Posting List	%3d -- Length: %4u\n", indent, plistIdx, seg->nbytes);
                printf("%s	ItemPointer %3d -- Block Id: %4u linp Index: %4u\n", indent, itemIdx,
                       ((uint32)((cur->ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)cur->ip_blkid.bi_lo)),
                       cur->ip_posid);

                val = itemptr_to_uint64(&seg->first);
                while (ptr < endseg) {
                    val += decode_varbyte(&ptr);
                    itemIdx++;

                    uint64_to_itemptr(val, cur);
                    printf("%s	ItemPointer %3d -- Block Id: %4u linp Index: %4u\n", indent, itemIdx,
                           ((uint32)((cur->ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)cur->ip_blkid.bi_lo)),
                           cur->ip_posid);
                }
                plistIdx++;
                seg = GinNextPostingListSegment(seg);
            }
        } else {
            int i = GinPageGetOpaque(page)->maxoff;
            int nitems = GinPageGetOpaque(page)->maxoff;
            ItemPointer items = (ItemPointer)GinDataPageGetData(page);
            for (i = 0; i < nitems; i++) {
                printf("%s ItemPointer %d -- Block Id: %u linp Index: %u\n", indent, i + 1,
                       ((uint32)((items[i].ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)items[i].ip_blkid.bi_lo)),
                       items[i].ip_posid);
            }
        }
    } else {
        OffsetNumber cur = GinPageGetOpaque(page)->maxoff;
        OffsetNumber high = GinPageGetOpaque(page)->maxoff;
        PostingItem *pitem = NULL;
        for (cur = FirstOffsetNumber; cur <= high; cur = OffsetNumberNext(cur)) {
            pitem = GinDataPageGetPostingItem(page, cur);
            printf("%s PostingItem %d -- child Block Id: (%u) Block Id: %u linp Index: %u\n", indent, cur,
                   ((uint32)((pitem->child_blkno.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)pitem->child_blkno.bi_lo)),
                   ((uint32)((pitem->key.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)pitem->key.ip_blkid.bi_lo)),
                   pitem->key.ip_posid);
        }
    }

    printf("\n");
}

/*	Dump out formatted items that reside on this block */
static void FormatItemBlock(char *buffer, Page page, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                            char *toastValue, unsigned int *toastRead)
{
    unsigned int x;
    unsigned int itemSize;
    unsigned int itemOffset;
    unsigned int itemFlags;
    ItemId itemId;
    unsigned int maxOffset = PageGetMaxOffsetNumber(page);
    const char *indent = isToast ? "\t" : "";
    errno_t rc;

    /* If it's a btree meta page, the meta block is where items would normally
     * be; don't print garbage. */
    if (IsBtreeMetaPage(page)) {
        return;
    }

    /* Same as above */
    if (IsSpGistMetaPage(page)) {
        return;
    }

    /* Same as above */
    if (IsGinMetaPage(page)) {
        return;
    }

    /* Leaf pages of GIN index contain posting lists
     * instead of item array.
     */
    if (g_specialType == SPEC_SECT_INDEX_GIN) {
        FormatGinBlock(buffer, isToast, toastOid, toastExternalSize, toastValue, toastRead);
        return;
    }

    if (!isToast || g_verbose) {
        printf("%s<Data> -----\n", indent);
    }

    /* Loop through the items on the block.  Check if the block is
     * empty and has a sensible item array listed before running
     * through each item */
    if (maxOffset == 0) {
        if (!isToast || g_verbose) {
            printf("%s Empty block - no items listed \n\n", indent);
        }
        return;
    }

    if ((maxOffset < 0) || (maxOffset > g_blockSize)) {
        if (!isToast || g_verbose) {
            printf("%s Error: Item index corrupt on block. Offset: <%u>.\n\n", indent, maxOffset);
        }
        g_exitCode = 1;
        return;
    }
    int formatAs;
    char textFlags[16];
    uint32 chunkId;
    unsigned int chunkSize = 0;

    /* First, honour requests to format items a special way, then
        * use the special section to determine the format style */
    if (g_itemOptions & ITEM_INDEX) {
        formatAs = ITEM_INDEX;
    } else if (g_itemOptions & ITEM_HEAP) {
        formatAs = ITEM_HEAP;
    } else {
        switch (g_specialType) {
            case SPEC_SECT_INDEX_BTREE:
            case SPEC_SECT_INDEX_HASH:
            case SPEC_SECT_INDEX_GIST:
            case SPEC_SECT_INDEX_GIN:
                formatAs = ITEM_INDEX;
                break;
            case SPEC_SECT_INDEX_SPGIST: {
                SpGistPageOpaque spgpo = (SpGistPageOpaque)((char *)page + ((PageHeader)page)->pd_special);

                if (spgpo->flags & SPGIST_LEAF) {
                    formatAs = ITEM_SPG_LEAF;
                } else {
                    formatAs = ITEM_SPG_INNER;
                }
                break;
            }
            default:
                formatAs = ITEM_HEAP;
                break;
        }
    }

    for (x = 1; x < (maxOffset + 1); x++) {
        itemId = PageGetItemId(page, x);
        itemFlags = (unsigned int)ItemIdGetFlags(itemId);
        itemSize = (unsigned int)ItemIdGetLength(itemId);
        itemOffset = (unsigned int)ItemIdGetOffset(itemId);
        switch (itemFlags) {
            case LP_UNUSED:
                rc = strcpy_s(textFlags, sizeof textFlags, "UNUSED");
                securec_check(rc, "\0", "\0");
                break;
            case LP_NORMAL:
                rc = strcpy_s(textFlags, sizeof textFlags, "NORMAL");
                securec_check(rc, "\0", "\0");
                break;
            case LP_REDIRECT:
                rc = strcpy_s(textFlags, sizeof textFlags, "REDIRECT");
                securec_check(rc, "\0", "\0");
                break;
            case LP_DEAD:
                rc = strcpy_s(textFlags, sizeof textFlags, "DEAD");
                securec_check(rc, "\0", "\0");
                break;
            default:
                /* shouldn't be possible */
                rc = sprintf_s(textFlags, strlen(textFlags) + 1, "0x%02x", itemFlags);
                securec_check(rc, "\0", "\0");
                break;
        }

        if (!isToast || g_verbose) {
            printf("%s Item %3u -- Length: %4u  Offset: %4u (0x%04x)"
                   "  Flags: %s\n",
                   indent, x, itemSize, itemOffset, itemOffset, textFlags);
        }

        /* Make sure the item can physically fit on this block before
            * formatting */
        if ((itemOffset + itemSize > g_blockSize) || (itemOffset + itemSize > g_bytesToFormat)) {
            if (!isToast || g_verbose) {
                printf("%s  Error: Item contents extend beyond block.\n"
                       "%s         BlockSize<%u> Bytes Read<%u> Item Start<%u>.\n",
                       indent, indent, g_blockSize, g_bytesToFormat, itemOffset + itemSize);
            }
            g_exitCode = 1;
            return;
        }
        HeapTupleHeader tuple_header;
        TransactionId xmax;

        /* If the user requests that the items be interpreted as
            * heap or index items... */
        if (g_itemOptions & ITEM_DETAIL) {
            FormatItem(buffer, itemSize, itemOffset, formatAs);
        }

        /* Dump the items contents in hex and ascii */
        if (g_blockOptions & BLOCK_FORMAT) {
            FormatBinary(buffer, itemSize, itemOffset);
        }

        /* Check if tuple was deleted */
        tuple_header = (HeapTupleHeader)(&buffer[itemOffset]);
        xmax = HeapTupleHeaderGetRawXmax(page, tuple_header);
        if ((g_blockOptions & BLOCK_IGNORE_OLD) && xmax != 0) {
            if (!isToast || g_verbose) {
                printf("%stuple was removed by transaction #%ld\n", indent, xmax);
            }
        } else if (isToast) {
            ToastChunkDecode(&buffer[itemOffset], itemSize, toastOid, &chunkId, toastValue + *toastRead,
                             &chunkSize);

            if (!isToast || g_verbose) {
                printf("%s  Read TOAST chunk. TOAST Oid: %d, chunk id: %d, "
                       "chunk data size: %u\n",
                       indent, toastOid, chunkId, chunkSize);
            }

            *toastRead += chunkSize;

            if (*toastRead >= toastExternalSize) {
                break;
            }
        } else if ((g_blockOptions & BLOCK_IGNORE_OLD) && !HeapTupleHeaderXminCommitted(tuple_header)) {
            if (!isToast || g_verbose) {
                printf("%stuple was not committed. \n", indent);
            }
        } else if ((g_blockOptions & BLOCK_DECODE) && (itemFlags == LP_NORMAL)) {
            /* Decode tuple data */
            FormatDecode(&buffer[itemOffset], itemSize);
        }

        if (!isToast && x == maxOffset) {
            printf("\n");
        }
    }
}

/*	Dump out formatted items that reside on this block */
static void FormatUHeapItemBlock(char *buffer, Page page, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                                 char *toastValue, unsigned int *toastRead)
{
    unsigned int x;
    unsigned int itemSize;
    unsigned int itemOffset;
    unsigned int itemFlags;
    RowPtr *itemId;
    unsigned int maxOffset = UHeapPageGetMaxOffsetNumber(page);
    const char *indent = isToast ? "\t" : "";
    errno_t rc;

    /* If it's a btree meta page, the meta block is where items would normally
     * be; don't print garbage. */
    if (IsBtreeMetaPage(page)) {
        return;
    }

    /* Same as above */
    if (IsSpGistMetaPage(page)) {
        return;
    }

    /* Same as above */
    if (IsGinMetaPage(page)) {
        return;
    }

    /* Leaf pages of GIN index contain posting lists
     * instead of item array.
     */
    if (g_specialType == SPEC_SECT_INDEX_GIN) {
        FormatUHeapGinBlock(buffer, isToast, toastOid, toastExternalSize, toastValue, toastRead);
        return;
    }

    if (!isToast || g_verbose) {
        printf("%s<Data> -----\n", indent);
    }

    /* Loop through the items on the block.  Check if the block is
     * empty and has a sensible item array listed before running
     * through each item */
    if (maxOffset == 0) {
        if (!isToast || g_verbose) {
            printf("%s Empty block - no items listed \n\n", indent);
        }
        return;
    }
    if ((maxOffset < 0) || (maxOffset > g_blockSize)) {
        if (!isToast || g_verbose) {
            printf("%s Error: Item index corrupt on block. Offset: <%u>.\n\n", indent, maxOffset);
        }
        g_exitCode = 1;
        return;
    }
    int formatAs;
    char textFlags[16];
    uint32 chunkId;
    unsigned int chunkSize = 0;

    /* First, honour requests to format items a special way, then
        * use the special section to determine the format style */
    if (g_itemOptions & ITEM_INDEX) {
        formatAs = ITEM_INDEX;
    } else if (g_itemOptions & ITEM_HEAP) {
        formatAs = ITEM_HEAP;
    } else {
        switch (g_specialType) {
            case SPEC_SECT_INDEX_BTREE:
            case SPEC_SECT_INDEX_HASH:
            case SPEC_SECT_INDEX_GIST:
            case SPEC_SECT_INDEX_GIN:
                formatAs = ITEM_INDEX;
                break;
            case SPEC_SECT_INDEX_SPGIST: {
                SpGistPageOpaque spgpo = (SpGistPageOpaque)((char *)page + ((UHeapPageHeader)page)->pd_special);

                if (spgpo->flags & SPGIST_LEAF) {
                    formatAs = ITEM_SPG_LEAF;
                } else {
                    formatAs = ITEM_SPG_INNER;
                }
                break;
            }
            default:
                formatAs = ITEM_HEAP;
                break;
        }
    }

    for (x = 1; x < (maxOffset + 1); x++) {
        itemId = UPageGetRowPtr(page, x);
        itemFlags = (unsigned int)itemId->flags;
        itemSize = (unsigned int)itemId->len;
        itemOffset = RowPtrGetOffset(itemId);

        switch (itemFlags) {
            case RP_UNUSED:
                rc = strcpy_s(textFlags, sizeof textFlags, "UNUSED");
                securec_check(rc, "\0", "\0");
                break;
            case RP_NORMAL:
                rc = strcpy_s(textFlags, sizeof textFlags, "NORMAL");
                securec_check(rc, "\0", "\0");
                break;
            case RP_REDIRECT:
                rc = strcpy_s(textFlags, sizeof textFlags, "REDIRECT");
                securec_check(rc, "\0", "\0");
                break;
            case RP_DEAD:
                rc = strcpy_s(textFlags, sizeof textFlags, "DEAD");
                securec_check(rc, "\0", "\0");
                break;
            default:
                /* shouldn't be possible */
                rc = sprintf_s(textFlags, strlen(textFlags) + 1, "0x%02x", itemFlags);
                securec_check(rc, "\0", "\0");
                break;
        }

        if (!isToast || g_verbose) {
            printf("%s Item %3u -- Length: %4u  Offset: %4u (0x%04x)"
                   "  Flags: %s\n",
                   indent, x, itemSize, itemOffset, itemOffset, textFlags);
        }

        /* Make sure the item can physically fit on this block before
            * formatting */
        if ((itemOffset + itemSize > g_blockSize) || (itemOffset + itemSize > g_bytesToFormat)) {
            if (!isToast || g_verbose) {
                printf("%s  Error: Item contents extend beyond block.\n"
                       "%s         BlockSize<%u> Bytes Read<%u> Item Start<%u>.\n",
                       indent, indent, g_blockSize, g_bytesToFormat, itemOffset + itemSize);
            }
            g_exitCode = 1;
            return;
        }
        UHeapDiskTuple utuple_header;
        TransactionId xmax;

        /* If the user requests that the items be interpreted as
            * heap or index items... */
        if (g_itemOptions & ITEM_DETAIL) {
            FormatUHeapItem(buffer, itemSize, itemOffset, formatAs);
        }

        /* Dump the items contents in hex and ascii */
        if (g_blockOptions & BLOCK_FORMAT) {
            FormatBinary(buffer, itemSize, itemOffset);
        }

        /* Check if tuple was deleted */
        utuple_header = (UHeapDiskTuple)(&buffer[itemOffset]);
        xmax = UHeapDiskTupleDeleted(utuple_header);
        if ((g_blockOptions & BLOCK_IGNORE_OLD) && (xmax != 0)) {
            if (!isToast || g_verbose) {
                printf("%stuple was removed by transaction.\n", indent);
            }
        } else if (isToast) {
            ToastChunkDecode(&buffer[itemOffset], itemSize, toastOid, &chunkId, toastValue + *toastRead,
                             &chunkSize);

            if (!isToast || g_verbose) {
                printf("%s  Read TOAST chunk. TOAST Oid: %d, chunk id: %d, "
                       "chunk data size: %u\n",
                       indent, toastOid, chunkId, chunkSize);
            }

            *toastRead += chunkSize;

            if (*toastRead >= toastExternalSize) {
                break;
            }
        } else if ((g_blockOptions & BLOCK_DECODE) && (itemFlags == LP_NORMAL)) {
            /* Decode tuple data */
            FormatDecode(&buffer[itemOffset], itemSize);
        }

        if (!isToast && x == maxOffset) {
            printf("\n");
        }
    }
}

/* Interpret the contents of the item based on whether it has a special
 * section and/or the user has hinted */
static void FormatItem(char *buffer, unsigned int numBytes, unsigned int startIndex, unsigned int formatAs)
{
    static const char *const spgistTupstates[4] = {"LIVE", "REDIRECT", "DEAD", "PLACEHOLDER"};

    if (formatAs == ITEM_INDEX) {
        /* It is an IndexTuple item, so dump the index header */
        if (numBytes < sizeof(ItemPointerData)) {
            if (numBytes) {
                printf("  Error: This item does not look like an index item.\n");
                g_exitCode = 1;
            }
        } else {
            IndexTuple itup = (IndexTuple)(&(buffer[startIndex]));

            printf("  Block Id: %u  linp Index: %u  Size: %d\n"
                   "  Has Nulls: %u  Has Varwidths: %u\n\n",
                   ((uint32)((itup->t_tid.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)itup->t_tid.ip_blkid.bi_lo)),
                   itup->t_tid.ip_posid, (int)IndexTupleSize(itup), IndexTupleHasNulls(itup) ? 1 : 0,
                   IndexTupleHasVarwidths(itup) ? 1 : 0);

            if (numBytes != IndexTupleSize(itup)) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)IndexTupleSize(itup));
                g_exitCode = 1;
            }
        }
    } else if (formatAs == ITEM_SPG_INNER) {
        /* It is an SpGistInnerTuple item, so dump the index header */
        if (numBytes < SGITHDRSZ) {
            if (numBytes) {
                printf("  Error: This item does not look like an SPGiST item.\n");
                g_exitCode = 1;
            }
        } else {
            SpGistInnerTuple itup = (SpGistInnerTuple)(&(buffer[startIndex]));

            printf("  State: %s  allTheSame: %d nNodes: %u prefixSize: %u\n\n", spgistTupstates[itup->tupstate],
                   itup->allTheSame, itup->nNodes, itup->prefixSize);

            if (numBytes != itup->size) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)itup->size);
                g_exitCode = 1;
            } else if (itup->prefixSize == MAXALIGN(itup->prefixSize)) {
                int i;
                SpGistNodeTuple node;

                /* Dump the prefix contents in hex and ascii */
                if ((g_blockOptions & BLOCK_FORMAT) && SGITHDRSZ + itup->prefixSize <= numBytes) {
                    FormatBinary(buffer, SGITHDRSZ + itup->prefixSize, startIndex);
                }

                /* Try to print the nodes, but only while pointer is sane */
                SGITITERATE(itup, i, node)
                {
                    int off = (char *)node - (char *)itup;

                    if (off + SGNTHDRSZ > numBytes) {
                        break;
                    }
                    printf("  Node %2d:  Downlink: %u/%u  Size: %d  Null: %u\n", i,
                           ((uint32)((node->t_tid.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) |
                           (uint16)node->t_tid.ip_blkid.bi_lo)),
                           node->t_tid.ip_posid, (int)IndexTupleSize(node), IndexTupleHasNulls(node) ? 1 : 0);
                    /* Dump the node's contents in hex and ascii */
                    if ((g_blockOptions & BLOCK_FORMAT) && off + IndexTupleSize(node) <= numBytes) {
                        FormatBinary(buffer, IndexTupleSize(node), startIndex + off);
                    }
                    if (IndexTupleSize(node) != MAXALIGN(IndexTupleSize(node))) {
                        break;
                    }
                }
            }
            printf("\n");
        }
    } else if (formatAs == ITEM_SPG_LEAF) {
        /* It is an SpGistLeafTuple item, so dump the index header */
        if (numBytes < SGLTHDRSZ) {
            if (numBytes) {
                printf("  Error: This item does not look like an SPGiST item.\n");
                g_exitCode = 1;
            }
        } else {
            SpGistLeafTuple itup = (SpGistLeafTuple)(&(buffer[startIndex]));
            printf("  State: %s  nextOffset: %u  Block Id: %u  linp Index: %u\n\n", spgistTupstates[itup->tupstate],
                   itup->nextOffset,
                   ((uint32)((itup->heapPtr.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) |
                   (uint16)itup->heapPtr.ip_blkid.bi_lo)),
                   itup->heapPtr.ip_posid);

            if (numBytes != itup->size) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)itup->size);
                g_exitCode = 1;
            }
        }
    } else {
        /* It is a HeapTuple item, so dump the heap header */
        unsigned int alignedSize = MAXALIGN(sizeof(HeapTupleHeaderData));
        if (numBytes < alignedSize) {
            if (numBytes) {
                printf("  Error: This item does not look like a heap item.\n");
                g_exitCode = 1;
            }
        } else {
            char flagString[256];
            unsigned int x;
            unsigned int bitmapLength = 0;
            unsigned int oidLength = 0;
            unsigned int computedLength;
            unsigned int infoMask;
            unsigned int infoMask2;
            int localNatts;
            unsigned int localHoff;
            bits8 *localBits;
            unsigned int localBitOffset;

            HeapTupleHeader htup = (HeapTupleHeader)(&buffer[startIndex]);
            TupleDesc tdup = (TupleDesc)(&buffer[startIndex]);

            infoMask = htup->t_infomask;
            infoMask2 = htup->t_infomask2;
            localBits = &(htup->t_bits[0]);
            localNatts = HeapTupleHeaderGetNatts(htup, tdup);
            localHoff = htup->t_hoff;
            localBitOffset = offsetof(HeapTupleHeaderData, t_bits);

            printf("  XMIN: %lu  XMAX: %u  CID|XVAC: %u", HeapTupleHeaderGetXmin_tuple(htup),
                   htup->t_choice.t_heap.t_xmax, HeapTupleHeaderGetRawCommandId(htup));

            if (infoMask & HEAP_HASOID) {
                printf("  OID: %u", HeapTupleHeaderGetOid(htup));
            }
            printf("\n"
                   "  Block Id: %u  linp Index: %u   Attributes: %d   Size: %d\n",
                   ((uint32)((htup->t_ctid.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) |
                   (uint16)htup->t_ctid.ip_blkid.bi_lo)),
                   htup->t_ctid.ip_posid, localNatts, htup->t_hoff);

            /* Place readable versions of the tuple info mask into a buffer.
             * Assume that the string can not expand beyond 256. */
            flagString[0] = '\0';
            errno_t rc;
            for (const FlagMapping& entry : maskFlags) {
                if ((infoMask & entry.mask)) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            for (const FlagMapping& entry : mask2Flags) {
                if ((infoMask2 & entry.mask)) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf("  infomask: 0x%04x (%s) \n", infoMask, flagString);

            /* As t_bits is a variable length array, determine the length of
             * the header proper */
            if (infoMask & HEAP_HASNULL) {
                bitmapLength = BITMAPLEN(localNatts);
            } else {
                bitmapLength = 0;
            }
            if (infoMask & HEAP_HASOID) {
                oidLength += sizeof(Oid);
            }
            computedLength = MAXALIGN(localBitOffset + bitmapLength + oidLength);
            /* Inform the user of a header size mismatch or dump the t_bits
             * array */
            if (computedLength != localHoff) {
                printf("  Error: Computed header length not equal to header size.\n"
                       "         Computed <%u>  Header: <%u>\n",
                       computedLength, localHoff);
                g_exitCode = 1;
            } else if ((infoMask & HEAP_HASNULL) && bitmapLength) {
                printf("  t_bits: ");
                for (x = 0; x < bitmapLength; x++) {
                    printf("[%u]: 0x%02x ", x, localBits[x]);
                    if (((x & 0x03) == 0x03) && (x < bitmapLength - 1)) {
                        printf("\n          ");
                    }
                }
                printf("\n");
            }
            printf("\n");
        }
    }
}

/* Interpret the contents of the item based on whether it has a special
 * section and/or the user has hinted */
static void FormatUHeapItem(char *buffer, unsigned int numBytes, unsigned int startIndex, unsigned int formatAs)
{
    static const char *const spgistTupstates[4] = {"LIVE", "REDIRECT", "DEAD", "PLACEHOLDER"};

    if (formatAs == ITEM_INDEX) {
        /* It is an IndexTuple item, so dump the index header */
        if (numBytes < sizeof(ItemPointerData)) {
            if (numBytes) {
                printf("  Error: This item does not look like an index item.\n");
                g_exitCode = 1;
            }
        } else {
            IndexTuple itup = (IndexTuple)(&(buffer[startIndex]));
            printf("  Block Id: %u  linp Index: %u  Size: %d\n"
                   "  Has Nulls: %u  Has Varwidths: %u\n\n",
                   ((uint32)((itup->t_tid.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) | (uint16)itup->t_tid.ip_blkid.bi_lo)),
                   itup->t_tid.ip_posid, (int)IndexTupleSize(itup), IndexTupleHasNulls(itup) ? 1 : 0,
                   IndexTupleHasVarwidths(itup) ? 1 : 0);

            if (numBytes != IndexTupleSize(itup)) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)IndexTupleSize(itup));
                g_exitCode = 1;
            }
        }
    } else if (formatAs == ITEM_SPG_INNER) {
        /* It is an SpGistInnerTuple item, so dump the index header */
        if (numBytes < SGITHDRSZ) {
            if (numBytes) {
                printf("  Error: This item does not look like an SPGiST item.\n");
                g_exitCode = 1;
            }
        } else {
            SpGistInnerTuple itup = (SpGistInnerTuple)(&(buffer[startIndex]));

            printf("  State: %s  allTheSame: %d nNodes: %u prefixSize: %u\n\n", spgistTupstates[itup->tupstate],
                   itup->allTheSame, itup->nNodes, itup->prefixSize);
            if (numBytes != itup->size) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)itup->size);
                g_exitCode = 1;
            } else if (itup->prefixSize == MAXALIGN(itup->prefixSize)) {
                int i;
                SpGistNodeTuple node;
                /* Dump the prefix contents in hex and ascii */
                if ((g_blockOptions & BLOCK_FORMAT) && SGITHDRSZ + itup->prefixSize <= numBytes) {
                    FormatBinary(buffer, SGITHDRSZ + itup->prefixSize, startIndex);
                }
                /* Try to print the nodes, but only while pointer is sane */
                SGITITERATE(itup, i, node)
                {
                    int off = (char *)node - (char *)itup;
                    if (off + SGNTHDRSZ > numBytes) {
                        break;
                    }
                    printf("  Node %2d:  Downlink: %u/%u  Size: %d  Null: %u\n", i,
                           ((uint32)((node->t_tid.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) |
                           (uint16)node->t_tid.ip_blkid.bi_lo)),
                           node->t_tid.ip_posid, (int)IndexTupleSize(node), IndexTupleHasNulls(node) ? 1 : 0);
                    /* Dump the node's contents in hex and ascii */
                    if ((g_blockOptions & BLOCK_FORMAT) && off + IndexTupleSize(node) <= numBytes) {
                        FormatBinary(buffer, IndexTupleSize(node), startIndex + off);
                    }
                    if (IndexTupleSize(node) != MAXALIGN(IndexTupleSize(node))) {
                        break;
                    }
                }
            }
            printf("\n");
        }
    } else if (formatAs == ITEM_SPG_LEAF) {
        /* It is an SpGistLeafTuple item, so dump the index header */
        if (numBytes < SGLTHDRSZ) {
            if (numBytes) {
                printf("  Error: This item does not look like an SPGiST item.\n");
                g_exitCode = 1;
            }
        } else {
            SpGistLeafTuple itup = (SpGistLeafTuple)(&(buffer[startIndex]));

            printf("  State: %s  nextOffset: %u  Block Id: %u  linp Index: %u\n\n", spgistTupstates[itup->tupstate],
                   itup->nextOffset,
                   ((uint32)((itup->heapPtr.ip_blkid.bi_hi << BLOCK_ID_HIGH_SHIFT) |
                   (uint16)itup->heapPtr.ip_blkid.bi_lo)),
                   itup->heapPtr.ip_posid);

            if (numBytes != itup->size) {
                printf("  Error: Item size difference. Given <%u>, "
                       "Internal <%d>.\n",
                       numBytes, (int)itup->size);
                g_exitCode = 1;
            }
        }
    } else {
        /* It is a HeapTuple item, so dump the heap header */
        unsigned int alignedSize = UHeapDiskTupleDataHeaderSize;

        if (numBytes < alignedSize) {
            if (numBytes) {
                printf("  Error: This item does not look like a heap item.\n");
                g_exitCode = 1;
            }
        } else {
            char flagString[256];
            unsigned int bitmapLength = 0;
            unsigned int oidLength = 0;
            unsigned int computedLength;
            unsigned int infoMask;
            unsigned int infoMask2;
            int localNatts;
            unsigned int localHoff;
            bits8 *localBits;
            unsigned int localBitOffset;
            UHeapDiskTuple utuple = (UHeapDiskTuple)(&buffer[startIndex]);

            infoMask = utuple->flag;
            infoMask2 = utuple->flag2;
            localBits = &(utuple->data[0]);
            localNatts = UHeapTupleHeaderGetNatts(utuple);
            localHoff = utuple->t_hoff;
            localBitOffset = offsetof(UHeapDiskTupleData, data);

            printf("  xid: %u \t td: %d \t locker_td : %d \n"
                   "  Attributes: %u, localHoff : %u \n",
                   utuple->xid, utuple->td_id, utuple->reserved, infoMask2, localHoff);

            /* Place readable versions of the tuple info mask into a buffer.
             * Assume that the string can not expand beyond 256. */
            flagString[0] = '\0';
            errno_t rc;
            for (const auto& entry : flagTable) {
                if (infoMask & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf("  infomask: 0x%04x (%s) \n", infoMask, flagString);

            /* As t_bits is a variable length array, determine the length of
             * the header proper */
            if (infoMask & UHEAP_HAS_NULL) {
                bitmapLength = BITMAPLEN(localNatts);
            } else {
                bitmapLength = 0;
            }

            computedLength = localBitOffset + bitmapLength + oidLength;

            printf("\n");
        }
    }
}

/* On blocks that have special sections, print the contents
 * according to previously determined special section type */
static void FormatSpecial(char *buffer)
{
    PageHeader pageHeader = (PageHeader)buffer;
    char flagString[100] = "\0";
    unsigned int specialOffset = pageHeader->pd_special;
    unsigned int specialSize = (g_blockSize >= specialOffset) ? (g_blockSize - specialOffset) : 0;
    errno_t rc;
    printf("<Special Section> -----\n");
    switch (g_specialType) {
        case SPEC_SECT_ERROR_UNKNOWN:
        case SPEC_SECT_ERROR_BOUNDARY:
            printf(" Error: Invalid special section encountered.\n");
            g_exitCode = 1;
            break;

        case SPEC_SECT_SEQUENCE:
            printf(" Sequence: 0x%08x\n", SEQUENCE_MAGIC);
            break;

            /* Btree index section */
        case SPEC_SECT_INDEX_BTREE: {
            BTPageOpaque btreeSection = (BTPageOpaque)(buffer + specialOffset);

            for (const auto& entry : btreeFlagTable) {
                if (btreeSection->bt_internal.btpo_flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf(" BTree Index Section:\n"
                   "  Flags: 0x%04x (%s)\n"
                   "  Blocks: Previous (%d)  Next (%d)  %s (%d)  CycleId (%d)\n\n",
                   btreeSection->bt_internal.btpo_flags, flagString, btreeSection->bt_internal.btpo_prev,
                   btreeSection->bt_internal.btpo_next,
                   (btreeSection->bt_internal.btpo_flags & BTP_DELETED) ? "Next XID" : "Level",
                   btreeSection->bt_internal.btpo.level, btreeSection->bt_internal.btpo_cycleid);
            break;
        }
        /* Hash index section */
        case SPEC_SECT_INDEX_HASH: {
            HashPageOpaque hashSection = (HashPageOpaque)(buffer + specialOffset);

            for (const auto& entry : hashFlagTable) {
                if (hashSection->hasho_flag & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf(" Hash Index Section:\n"
                   "  Flags: 0x%04x (%s)\n"
                   "  Bucket Number: 0x%04x\n"
                   "  Blocks: Previous (%d)  Next (%d)\n\n",
                   hashSection->hasho_flag, flagString, hashSection->hasho_bucket, hashSection->hasho_prevblkno,
                   hashSection->hasho_nextblkno);
            break;
        }
            /* GIST index section */
        case SPEC_SECT_INDEX_GIST: {
            GISTPageOpaque gistSection = (GISTPageOpaque)(buffer + specialOffset);

            for (const auto& entry : gistFlagTable) {
                if (gistSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" GIST Index Section:\n"
                   "  NSN: 0x%08lx\n"
                   "  RightLink: %d\n"
                   "  Flags: 0x%08x (%s)\n"
                   "  GIST_page_id: 0x%08x\n\n",
                   gistSection->nsn, gistSection->rightlink, gistSection->flags, flagString, gistSection->gist_page_id);
            break;
        }
        /* GIN index section */
        case SPEC_SECT_INDEX_GIN: {
            GinPageOpaque ginSection = (GinPageOpaque)(buffer + specialOffset);
            for (const auto& entry : ginFlagTable) {
                if (ginSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" GIN Index Section:\n"
                   "  Flags: 0x%08x (%s)  Maxoff: %d\n"
                   "  Blocks: RightLink (%d)\n\n",
                   ginSection->flags, flagString, ginSection->maxoff, ginSection->rightlink);
            break;
        }
        /* SP-GIST index section */
        case SPEC_SECT_INDEX_SPGIST: {
            SpGistPageOpaque spgistSection = (SpGistPageOpaque)(buffer + specialOffset);

            for (const auto& entry : spgistFlagTable) {
                if (spgistSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" SPGIST Index Section:\n"
                   "  Flags: 0x%08x (%s)\n"
                   "  nRedirection: %d\n"
                   "  nPlaceholder: %d\n\n",
                   spgistSection->flags, flagString, spgistSection->nRedirection, spgistSection->nPlaceholder);
            break;
        }
        /* No idea what type of special section this is */
        default:
            printf(" Unknown special section type. Type: <%u>.\n", g_specialType);
            g_exitCode = 1;
            break;
    }

    /* Dump the formatted contents of the special section */
    if (g_blockOptions & BLOCK_FORMAT) {
        if (g_specialType == SPEC_SECT_ERROR_BOUNDARY) {
            printf(" Error: Special section points off page."
                   " Unable to dump contents.\n");

            g_exitCode = 1;
        } else {
            FormatBinary(buffer, specialSize, specialOffset);
        }
    }
}

/* On blocks that have special sections, print the contents
 * according to previously determined special section type */
static void FormatUHeapSpecial(char *buffer)
{
    UHeapPageHeader upageHeader = (UHeapPageHeader)buffer;
    char flagString[100] = "\0";
    unsigned int specialOffset = upageHeader->pd_special;
    unsigned int specialSize = (g_blockSize >= specialOffset) ? (g_blockSize - specialOffset) : 0;
    errno_t rc;
    printf("<Special Section> -----\n");

    switch (g_specialType) {
        case SPEC_SECT_ERROR_UNKNOWN:
        case SPEC_SECT_ERROR_BOUNDARY:
            printf(" Error: Invalid special section encountered.\n");
            g_exitCode = 1;
            break;

        case SPEC_SECT_SEQUENCE:
            printf(" Sequence: 0x%08x\n", SEQUENCE_MAGIC);
            break;

            /* Btree index section */
        case SPEC_SECT_INDEX_BTREE: {
            BTPageOpaque btreeSection = (BTPageOpaque)(buffer + specialOffset);

            for (const auto& entry : btreeFlagTable) {
                if (btreeSection->bt_internal.btpo_flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf(" BTree Index Section:\n"
                   "  Flags: 0x%04x (%s)\n"
                   "  Blocks: Previous (%d)  Next (%d)  %s (%d)  CycleId (%d)\n\n",
                   btreeSection->bt_internal.btpo_flags, flagString, btreeSection->bt_internal.btpo_prev,
                   btreeSection->bt_internal.btpo_next,
                   (btreeSection->bt_internal.btpo_flags & BTP_DELETED) ? "Next XID" : "Level",
                   btreeSection->bt_internal.btpo.level, btreeSection->bt_internal.btpo_cycleid);
            break;
        }
            /* Hash index section */
        case SPEC_SECT_INDEX_HASH: {
            HashPageOpaque hashSection = (HashPageOpaque)(buffer + specialOffset);

            for (const auto& entry : hashFlagTable) {
                if (hashSection->hasho_flag & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" Hash Index Section:\n"
                   "  Flags: 0x%04x (%s)\n"
                   "  Bucket Number: 0x%04x\n"
                   "  Blocks: Previous (%d)  Next (%d)\n\n",
                   hashSection->hasho_flag, flagString, hashSection->hasho_bucket, hashSection->hasho_prevblkno,
                   hashSection->hasho_nextblkno);
            break;
        }
            /* GIST index section */
        case SPEC_SECT_INDEX_GIST: {
            GISTPageOpaque gistSection = (GISTPageOpaque)(buffer + specialOffset);

            for (const auto& entry : gistFlagTable) {
                if (gistSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" GIST Index Section:\n"
                   "  NSN: 0x%08lx\n"
                   "  RightLink: %d\n"
                   "  Flags: 0x%08x (%s)\n"
                   "  GIST_page_id: 0x%08x\n\n",
                   gistSection->nsn, gistSection->rightlink, gistSection->flags, flagString, gistSection->gist_page_id);
            break;
        }
            /* GIN index section */
        case SPEC_SECT_INDEX_GIN: {
            GinPageOpaque ginSection = (GinPageOpaque)(buffer + specialOffset);

            for (const auto& entry : ginFlagTable) {
                if (ginSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }

            printf(" GIN Index Section:\n"
                   "  Flags: 0x%08x (%s)  Maxoff: %d\n"
                   "  Blocks: RightLink (%d)\n\n",
                   ginSection->flags, flagString, ginSection->maxoff, ginSection->rightlink);
            break;
        }
            /* SP-GIST index section */
        case SPEC_SECT_INDEX_SPGIST: {
            SpGistPageOpaque spgistSection = (SpGistPageOpaque)(buffer + specialOffset);

            for (const auto& entry : spgistFlagTable) {
                if (spgistSection->flags & entry.mask) {
                    rc = strcat_s(flagString, sizeof(flagString), entry.name);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (strlen(flagString)) {
                flagString[strlen(flagString) - 1] = '\0';
            }
            printf(" SPGIST Index Section:\n"
                   "  Flags: 0x%08x (%s)\n"
                   "  nRedirection: %d\n"
                   "  nPlaceholder: %d\n\n",
                   spgistSection->flags, flagString, spgistSection->nRedirection, spgistSection->nPlaceholder);
            break;
        }
            /* No idea what type of special section this is */
        default:
            printf(" Unknown special section type. Type: <%u>.\n", g_specialType);
            g_exitCode = 1;
            break;
    }

    /* Dump the formatted contents of the special section */
    if (g_blockOptions & BLOCK_FORMAT) {
        if (g_specialType == SPEC_SECT_ERROR_BOUNDARY) {
            printf(" Error: Special section points off page."
                   " Unable to dump contents.\n");

            g_exitCode = 1;
        } else {
            FormatBinary(buffer, specialSize, specialOffset);
        }
    }
}

/*	For each block, dump out formatted header and content information */
static void FormatBlock(unsigned int blockOptions, unsigned int controlOptions, char *buffer, BlockNumber currentBlock,
                        unsigned int blockSize, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                        char *toastValue, unsigned int *toastRead)
{
    Page page = (Page)buffer;
    const char *indent = isToast ? "\t" : "";

    g_pageOffset = blockSize * currentBlock;
    g_specialType = GetSpecialSectionType(buffer, page);

    if (!isToast || g_verbose) {
        printf("\n%sBlock %4u **%s***************************************\n", indent, currentBlock,
               (g_bytesToFormat == blockSize) ? "***************" : " PARTIAL BLOCK ");
    }

    /* Either dump out the entire block in hex+acsii fashion or
     * interpret the data based on block structure */
    if (blockOptions & BLOCK_NO_INTR) {
        FormatBinary(buffer, g_bytesToFormat, 0);
    } else {
        int rc;

        /* Every block contains a header, items and possibly a special
         * section.  Beware of partial block reads though */
        if (g_isUHeap) {
            rc = FormatUHeapHeader(buffer, page, currentBlock, isToast);
        } else {
            rc = FormatHeader(buffer, page, currentBlock, isToast);
        }

        /* If we didn't encounter a partial read in the header, carry on... */
        if (rc != EOF_ENCOUNTERED) {
            if (g_isUHeap) {
                FormatUHeapItemBlock(buffer, page, isToast, toastOid, toastExternalSize, toastValue, toastRead);
                if (g_specialType != SPEC_SECT_NONE) {
                    FormatUHeapSpecial(buffer);
                }
            } else {
                FormatItemBlock(buffer, page, isToast, toastOid, toastExternalSize, toastValue, toastRead);
                if (g_specialType != SPEC_SECT_NONE) {
                    FormatSpecial(buffer);
                }
            }
        }
    }
}

/*	Dump out the content of the PG control file */
static void FormatControl(char *buffer)
{
    unsigned int localPgVersion = 0;
    unsigned int controlFileSize = 0;
    time_t cdTime;
    time_t cpTime;

    printf("\n<pg_control Contents> *********************************************\n\n");

    /* Check the version */
    if (g_bytesToFormat >= offsetof(ControlFileData, catalog_version_no)) {
        localPgVersion = ((ControlFileData *)buffer)->pg_control_version;
    }

    try {
        controlFileSize = sizeof(ControlFileData);
    } catch (const std::exception &e) {
        printf("gs_filedump: pg_control version %u not supported.\n", localPgVersion);
        return;
    }

    /* Interpret the control file if it's all there */
    if (g_bytesToFormat >= controlFileSize) {
        ControlFileData *controlData = (ControlFileData *)buffer;
        CheckPoint *checkPoint = &(controlData->checkPointCopy);
        pg_crc32 crcLocal;

        /* Compute a local copy of the CRC to verify the one on disk */
        INIT_CRC32C(crcLocal);
        COMP_CRC32C(crcLocal, buffer, offsetof(ControlFileData, crc));
        FIN_CRC32C(crcLocal);

        /* Grab a readable version of the database state */
        const char* dbState = "UNKNOWN";
        for (const auto& entry : dbStateTable) {
            if (controlData->state == entry.state) {
                dbState = entry.name;
                break;
            }
        }

        /* convert timestamps to system's time_t width */
        cdTime = controlData->time;
        cpTime = checkPoint->time;

        printf("                          CRC: %s\n"
               "           pg_control Version: %u%s\n"
               "              Catalog Version: %u\n"
               "   Database system Identifier: " UINT64_FORMAT "\n"
               "       Database cluster State: %s\n"
               "     pg_control last modifyed: %s"
               "       Last Checkpoint Record: Log File (%u) Offset (0x%08x)\n"
               "   Previous Checkpoint Record: Log File (%u) Offset (0x%08x)\n"
               "  Last Checkpoint Record Redo: Log File (%u) Offset (0x%08x)\n"
               "          |-       TimeLineID: %u\n"
               "          |- full_path_writes: %s\n"
               "          |-         Next XID: %lu\n"
               "          |-         Next OID: %u\n"
               "          |- Next MultiXactId: %lu\n"
               "          |- Next MultiOffset: %lu\n"
               "          |-        oldestXid: %lu\n"
               "          |-   oldestXid's DB: %u\n"
               "          |-  oldestActiveXid: %lu\n"
               "          |-       remove_seg: %X/%lu\n"
               "    Time of latest checkpoint: %s"

               "       Minimum Recovery Point: Log File (%u) Offset (0x%08x)\n"
               "        Backup start location: %X/%X\n"
               "          Backup end location: %X/%X\n"
               "End-of-backup record required: %s\n"
               " Current Setting:\n"
               "                    wal_level: %s\n"
               "              max_connections: %u\n"
               "           max_prepared_xacts: %u\n"
               "           max_locks_per_xact: %u\n"

               "       Maximum Data Alignment: %u\n"
               "        Floating-Point Sample: %.7g%s\n"
               "          Database Block Size: %u\n"
               "           Blocks Per Segment: %u\n"
               "              XLOG Block Size: %u\n"
               "            XLOG Segment Size: %u\n"
               "Maximum length of identifiers: %u\n"
               "  Maximum columns in an index: %u\n"
               "Maximum size of a TOAST chunk: %u\n"
               "       Date/time type storage: %s\n"
               "      Float4 argument passing: %s\n"
               "      Float8 argument passing: %s\n"
               "     Database system TimeLine: %u\n",
               EQ_CRC32C(crcLocal, controlData->crc) ? "Correct" : "Not Correct", controlData->pg_control_version,
               (controlData->pg_control_version == PG_CONTROL_VERSION ? "" : " (Not Correct!)"),
               controlData->catalog_version_no, controlData->system_identifier, dbState, ctime(&(cdTime)),
               (uint32)(controlData->checkPoint >> 32), (uint32)controlData->checkPoint,
               (uint32)(controlData->prevCheckPoint >> 32), (uint32)controlData->prevCheckPoint,
               (uint32)(checkPoint->redo >> 32), (uint32)checkPoint->redo, checkPoint->ThisTimeLineID,
               checkPoint->fullPageWrites ? _("on") : _("off"), checkPoint->nextXid, checkPoint->nextOid,
               checkPoint->nextMulti, checkPoint->nextMultiOffset, checkPoint->oldestXid, checkPoint->oldestXidDB,
               checkPoint->oldestActiveXid, (uint32)(checkPoint->remove_seg >> 32), checkPoint->remove_seg,
               ctime(&cpTime), (uint32)(controlData->minRecoveryPoint >> 32), (uint32)controlData->minRecoveryPoint,
               (uint32)(controlData->backupStartPoint >> 32), (uint32)controlData->backupStartPoint,
               (uint32)(controlData->backupEndPoint >> 32), (uint32)controlData->backupEndPoint,
               controlData->backupEndRequired ? _("yes") : _("no"),

               wal_level_str((WalLevel)controlData->wal_level), controlData->MaxConnections,
               controlData->max_prepared_xacts, controlData->max_locks_per_xact,

               controlData->maxAlign, controlData->floatFormat,
               (controlData->floatFormat == FLOATFORMAT_VALUE ? "" : " (Not Correct!)"), controlData->blcksz,
               controlData->relseg_size, controlData->xlog_blcksz, controlData->xlog_seg_size, controlData->nameDataLen,
               controlData->indexMaxKeys, controlData->toast_max_chunk_size,
               (controlData->enableIntTimes ? _("64-bit integers") : _("floating-point numbers")),
               (controlData->float4ByVal ? _("by value") : _("by reference")),
               (controlData->float8ByVal ? _("by value") : _("by reference")), controlData->timeline);
    } else {
        printf(" Error: pg_control file size incorrect.\n"
               "        Size: Correct <%u>  Received <%u>.\n\n",
               controlFileSize, g_bytesToFormat);

        /* If we have an error, force a formatted dump so we can see
         * where things are going wrong */
        g_controlOptions |= CONTROL_FORMAT;

        g_exitCode = 1;
    }

    /* Dump hex and ascii representation of data */
    if (g_controlOptions & CONTROL_FORMAT) {
        printf("<pg_control Formatted Dump> *****************"
               "**********************\n\n");
        FormatBinary(buffer, g_bytesToFormat, 0);
    }
}

/* Dump out the contents of the block in hex and ascii.
 * BYTES_PER_LINE bytes are formatted in each line. */
static void FormatBinary(char *buffer, unsigned int numBytes, unsigned int startIndex)
{
    unsigned int index = 0;
    unsigned int stopIndex = 0;
    unsigned int x = 0;
    unsigned int lastByte = startIndex + numBytes;

    if (numBytes) {
        /* Iterate through a printable row detailing the current
         * address, the hex and ascii values */
        for (index = startIndex; index < lastByte; index += BYTES_PER_LINE) {
            stopIndex = index + BYTES_PER_LINE;

            /* Print out the address */
            if (g_blockOptions & BLOCK_ABSOLUTE) {
                printf("  %08x: ", static_cast<unsigned int>(g_pageOffset + index));
            } else {
                printf("  %04x: ", static_cast<unsigned int>(index));
            }

            /* Print out the hex version of the data */
            for (x = index; x < stopIndex; x++) {
                if (x < lastByte) {
                    printf("%02x", 0xff & static_cast<unsigned int>(static_cast<unsigned char>(buffer[x])));
                } else {
                    printf("  ");
                }
                if ((x & 0x03) == 0x03) {
                    printf(" ");
                }
            }
            printf(" ");

            /* Print out the ascii version of the data */
            for (x = index; x < stopIndex; x++) {
                if (x < lastByte) {
                    printf("%c", isprint(buffer[x]) ? buffer[x] : '.');
                } else {
                    printf(" ");
                }
            }
            printf("\n");
        }
        printf("\n");
    }
}

/* Dump the binary image of the block */
static void DumpBinaryBlock(char *buffer)
{
    unsigned int x;

    for (x = 0; x < g_bytesToFormat; x++) {
        putchar(buffer[x]);
    }
}

/* Control the dumping of the blocks within the file */
int DumpFileContents(unsigned int blockOptions, unsigned int controlOptions, FILE *fp, unsigned int blockSize,
                     int blockStart, int blockEnd, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                     char *toastValue)
{
    unsigned int initialRead = 1;
    unsigned int contentsToDump = 1;
    unsigned int toastDataRead = 0;
    BlockNumber currentBlock = 0;
    int result = 0;
    if (g_isSegment && isToast) {
        blockStart = 0;
        blockEnd = 0;
    }
    /* On a positive block size, allocate a local buffer to store
     * the subsequent blocks */
    if (blockSize == 0 || blockSize > MAXOUTPUTLEN) {
        fprintf(stderr, "Invalid memory allocation size: <%u>\n", blockSize);
        result = 1;
    }
    char *block = static_cast<char *>(malloc(blockSize));
    if (!block) {
        printf("\nError: Unable to create buffer of size <%u>.\n", blockSize);
        result = 1;
    }

    /* If the user requested a block range, seek to the correct position
     * within the file for the start block. */
    if ((result == 0) && (blockOptions & BLOCK_RANGE)) {
        unsigned int position = blockSize * blockStart;
        if (fseek(fp, position, SEEK_SET) != 0) {
            printf("Error: Seek error encountered before requested "
                   "start block <%d>.\n",
                   blockStart);
            contentsToDump = 0;
            result = 1;
        } else {
            currentBlock = blockStart;
        }
    }

    /* Iterate through the blocks in the file until you reach the end or
     * the requested range end */
    while (contentsToDump && result == 0) {
        g_bytesToFormat = fread(block, 1, blockSize, fp);
        if (g_bytesToFormat == 0) {
            /* fseek() won't pop an error if you seek passed eof. The next
             * subsequent read gets the error. */
            if (initialRead) {
                printf("Error: Premature end of file encountered.\n");
            } else if (!(blockOptions & BLOCK_BINARY)) {
                printf("\n*** End of File Encountered. Last Block "
                       "Read: %d ***\n",
                       currentBlock - 1);
            }
            contentsToDump = 0;
        } else {
            if (blockOptions & BLOCK_BINARY) {
                DumpBinaryBlock(block);
            } else {
                if (controlOptions & CONTROL_DUMP) {
                    FormatControl(block);
                    contentsToDump = false;
                } else {
                    FormatBlock(blockOptions, controlOptions, block, currentBlock, blockSize, isToast, toastOid,
                                toastExternalSize, toastValue, &toastDataRead);
                }
            }
        }

        /* Check to see if we are at the end of the requested range. */
        if ((blockOptions & BLOCK_RANGE) && ((int)currentBlock >= blockEnd) && (contentsToDump)) {
            /* Don't print out message if we're doing a binary dump */
            if (!(blockOptions & BLOCK_BINARY)) {
                printf("\n*** End of Requested Range Encountered. "
                       "Last Block Read: %d ***\n",
                       currentBlock);
            }
            contentsToDump = 0;
        } else {
            currentBlock++;
        }
        initialRead = 0;
        /* If TOAST data is read */
        if (isToast && toastDataRead >= toastExternalSize) {
            break;
        }
    }
    free(block);
    return result;
}

/* Control the dumping of the blocks within the file */
int DumpUHeapFileContents(unsigned int blockOptions, unsigned int controlOptions, FILE *fp, unsigned int blockSize,
                          int blockStart, int blockEnd, bool isToast, Oid toastOid, unsigned int toastExternalSize,
                          char *toastValue)
{
    unsigned int initialRead = 1;
    unsigned int contentsToDump = 1;
    unsigned int toastDataRead = 0;
    BlockNumber currentBlock = 0;
    int result = 0;
    /* On a positive block size, allocate a local buffer to store
     * the subsequent blocks */
    if (blockSize == 0 || blockSize > MAXOUTPUTLEN) {
        fprintf(stderr, "Invalid memory allocation size: <%u>\n", blockSize);
        result = 1;
    }
    char *block = static_cast<char *>(malloc(blockSize));
    if (!block) {
        printf("\nError: Unable to create buffer of size <%u>.\n", blockSize);
        result = 1;
    }

    /* If the user requested a block range, seek to the correct position
     * within the file for the start block. */
    if ((result == 0) && (blockOptions & BLOCK_RANGE)) {
        unsigned int position = blockSize * blockStart;

        if (fseek(fp, position, SEEK_SET) != 0) {
            printf("Error: Seek error encountered before requested "
                   "start block <%d>.\n",
                   blockStart);
            contentsToDump = 0;
            result = 1;
        } else {
            currentBlock = blockStart;
        }
    }

    /* Iterate through the blocks in the file until you reach the end or
     * the requested range end */
    while (contentsToDump && result == 0) {
        g_bytesToFormat = fread(block, 1, blockSize, fp);
        if (g_bytesToFormat == 0) {
            /* fseek() won't pop an error if you seek passed eof. The next
             * subsequent read gets the error. */
            if (initialRead) {
                printf("Error: Premature end of file encountered.\n");
            } else if (!(blockOptions & BLOCK_BINARY)) {
                printf("\n*** End of File Encountered. Last Block "
                       "Read: %d ***\n",
                       currentBlock - 1);
            }
            contentsToDump = 0;
        } else {
            if (blockOptions & BLOCK_BINARY) {
                DumpBinaryBlock(block);
            } else {
                if (controlOptions & CONTROL_DUMP) {
                    FormatControl(block);
                    contentsToDump = false;
                } else {
                    FormatBlock(blockOptions, controlOptions, block, currentBlock, blockSize, isToast, toastOid,
                                toastExternalSize, toastValue, &toastDataRead);
                }
            }
        }

        /* Check to see if we are at the end of the requested range. */
        if ((blockOptions & BLOCK_RANGE) && ((int)currentBlock >= blockEnd) && (contentsToDump)) {
            /* Don't print out message if we're doing a binary dump */
            if (!(blockOptions & BLOCK_BINARY)) {
                printf("\n*** End of Requested Range Encountered. "
                       "Last Block Read: %d ***\n",
                       currentBlock);
            }
            contentsToDump = 0;
        } else {
            currentBlock++;
        }

        initialRead = 0;

        /* If TOAST data is read */
        if (isToast && toastDataRead >= toastExternalSize) {
            break;
        }
    }

    free(block);

    return result;
}

int PrintRelMappings(void)
{
    /* For storing ingested data */
    char charbuf[RELMAPPER_FILESIZE];
    RelMapFile *map;
    RelMapping *mappings;
    RelMapping m;
    int bytesRead;

    /* For confirming Magic Number correctness */
    char m1[RELMAPPER_MAGICSIZE];
    char m2[RELMAPPER_MAGICSIZE];
    int magicRef = RELMAPPER_FILEMAGIC_4K;
    int magicVal;
    int numLoops;
    errno_t rc;

    /* Read in the file */
    rewind(fp);  // Make sure to start from the beginning
    bytesRead = fread(charbuf, 1, RELMAPPER_FILESIZE, fp);
    if (bytesRead != RELMAPPER_FILESIZE) {
        printf("Read %d bytes, expected %d\n", bytesRead, RELMAPPER_FILESIZE);
        return RETURN_ERROR;
    }
    /* Convert to RelMapFile type for usability */
    map = (RelMapFile *)charbuf;

    /* Check and print Magic Number correctness */
    printf("Magic Number: 0x%x", map->magic);
    magicVal = map->magic;

    rc = memcpy_s(m1, sizeof(m1), &magicRef, RELMAPPER_MAGICSIZE);
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(m2, sizeof(m2), &magicVal, RELMAPPER_MAGICSIZE);
    securec_check(rc, "\0", "\0");

    if (memcmp(m1, m2, RELMAPPER_MAGICSIZE) == 0) {
        printf(" (CORRECT)\n");
    } else {
        printf(" (INCORRECT)\n");
    }

    /* Print Mappings */
    printf("Num Mappings: %d\n", map->num_mappings);
    printf("Detailed Mappings list:\n");
    mappings = map->mappings;

    /* Limit number of mappings as per MAX_MAPPINGS */
    numLoops = map->num_mappings;
    if (map->num_mappings > MAX_MAPPINGS_4K) {
        numLoops = MAX_MAPPINGS_4K;
        printf("  NOTE: listing has been limited to the first %d mappings\n", MAX_MAPPINGS_4K);
        printf("        (perhaps your file is not a valid pg_filenode.map file?)\n");
    }

    for (int i = 0; i < numLoops; i++) {
        m = mappings[i];
        printf("OID: %u\tFilenode: %u\n", m.mapoid, m.mapfilenode);
    }
    return RETURN_SUCCESS;
}

/* Consume the options and iterate through the given file, formatting as
 * requested. */
int main(int argv, char **argc)
{
    /* If there is a parameter list, validate the options */
    unsigned int validOptions = 0;
    validOptions = (argv < 2) ? OPT_RC_COPYRIGHT : ConsumeOptions(argv, argc);
    /* Display valid options if no parameters are received or invalid options
     * where encountered */
    if (validOptions != OPT_RC_VALID) {
        DisplayOptions(validOptions);
    } else if (g_isRelMapFile) {
        CreateDumpFileHeader(argv, argc);
        g_exitCode = PrintRelMappings();
    } else {
        /* Don't dump the header if we're dumping binary pages */
        if (!(g_blockOptions & BLOCK_BINARY)) {
            CreateDumpFileHeader(argv, argc);
        }
        /* If the user has not forced a block size, use the size of the
         * control file data or the information from the block 0 header */
        if (g_controlOptions) {
            if (!(g_controlOptions & CONTROL_FORCED)) {
                g_blockSize = sizeof(ControlFileData);
            }
        } else if (!(g_blockOptions & BLOCK_FORCED)) {
            g_blockSize = GetBlockSize(fp);
        }
        if (g_isSegment) {
            InitSegmentInfo(fp, fpToast);
        } else if (g_isUHeap) {
            g_exitCode = DumpUHeapFileContents(g_blockOptions, g_controlOptions, fp, g_blockSize, g_blockStart,
                                               g_blockEnd, false, /* is toast realtion */
                                               0,                 /* no toast Oid */
                                               0,                 /* no toast external size */
                                               NULL               /* no out toast value */
            );
        } else {
            g_exitCode = DumpFileContents(g_blockOptions, g_controlOptions, fp, g_blockSize, g_blockStart, g_blockEnd,
                                          false, /* is toast realtion */
                                          0,     /* no toast Oid */
                                          0,     /* no toast external size */
                                          NULL   /* no out toast value */
            );
        }
    }

    if (fp) {
        fclose(fp);
        fp = nullptr;
    }
    if (fpToast) {
        fclose(fpToast);
        fpToast = nullptr;
    }

    exit(g_exitCode);
    return RETURN_SUCCESS;
}