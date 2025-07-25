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