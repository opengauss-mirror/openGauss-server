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
            strcat_s(optionBuffer, sizeof(optionBuffer), " ");
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
        if (pageHeader->pd_flags & PD_HAS_FREE_LINES) {
            strcat_s(flagString, sizeof(flagString), "HAS_FREE_LINES|");
        }
        if (pageHeader->pd_flags & PD_PAGE_FULL) {
            strcat_s(flagString, sizeof(flagString), "PAGE_FULL|");
        }
        if (pageHeader->pd_flags & PD_ALL_VISIBLE) {
            strcat_s(flagString, sizeof(flagString), "ALL_VISIBLE|");
        }
        if (PageIsCompressed(page)) {
            strcat_s(flagString, sizeof(flagString), "COMPRESSED_PAGE|");
        }
        if (PageIsLogical(page)) {
            strcat_s(flagString, sizeof(flagString), "LOGICAL_PAGE|");
        }
        if (PageIsEncrypt(page)) {
            strcat_s(flagString, sizeof(flagString), "ENCRYPT_PAGE|");
        }
        if (pageHeader->pd_flags & PD_CHECKSUM_FNV1A) {
            strcat_s(flagString, sizeof(flagString), "CHECKSUM_FNV1A|");
        }
        if (pageHeader->pd_flags & PD_JUST_AFTER_FPW) {
            strcat_s(flagString, sizeof(flagString), "JUST_AFTER_FPW|");
        }
        if (pageHeader->pd_flags & PD_EXRTO_PAGE) {
            strcat_s(flagString, sizeof(flagString), "EXRTO_PAGE|");
        }
        if (pageHeader->pd_flags & PD_TDE_PAGE) {
            strcat_s(flagString, sizeof(flagString), "TDE_PAGE|");
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
        if (upageHeader->pd_flags & UHEAP_HAS_FREE_LINES) {
            strcat_s(flagString, sizeof(flagString), "HAS_FREE_LINES|");
        }
        if (upageHeader->pd_flags & UHEAP_PAGE_FULL) {
            strcat_s(flagString, sizeof(flagString), "PAGE_FULL|");
        }
        if (upageHeader->pd_flags & UHP_ALL_VISIBLE) {
            strcat_s(flagString, sizeof(flagString), "ALL_VISIBLE|");
        }
        if (UPageIsFull(page)) {
            strcat_s(flagString, sizeof(flagString), "UPAGE_IS_FULL|");
        }
        if (UPageHasFreeLinePointers(page)) {
            strcat_s(flagString, sizeof(flagString), "UPAGE_HAS_FREE_LINE_POINTERS|");
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