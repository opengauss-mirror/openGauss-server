/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2000-2009, PostgreSQL Global Development Group
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * pg_check_clog.cpp
 *        This tool is to parse clog file, it can work two mode,
 *            single mode and file mode.
 *
 *
 * IDENTIFICATION
 *        contrib/pg_check_clog/pg_check_clog.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/clog.h"
#include "access/slru.h"
#include "access/transam.h"

/* PG */
#define CLOG_BITS_PER_XACT 2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK ((1 << CLOG_BITS_PER_XACT) - 1)

#define TransactionIdToPage(xid) ((xid) / (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid) (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_BYTE)

char* DataDir;
char buffer[8192];

typedef enum parse_mode { file = 0, single } parse_mode;

const char* xid_status_name[] = {"IN_PROGRESS", "COMMITTED", "ABORTED", "SUB_COMMITTED"};

/*function*/
static void usage(const char* progname)
{
    printf("%s is a parse clog tool for PostgreSQL.\n\n"
           "Usage:\n"
           "  %s [OPTIONS]... [PGDATA]\n"
           "\noptions:\n"
           "  -t xid		show the state of the special transaction\n"
           "  -n filename		set the start filename you want to parse\n"
           "  example		./pg_check_clog -n 0 /home/user install/data\n"
           "\nCommon options:\n"
           "  --help       show this help, then exit\n"
           "  --version    output version information, then exit\n"
           "\n",
        progname,
        progname);
}

static void parse_clog_file(const int segnum)
{

    /* one segment file has 8k*8bit/2*2048 xids */
    uint32 segnum_xid = BLCKSZ * CLOG_XACTS_PER_BYTE * SLRU_PAGES_PER_SEGMENT;
    /* the first xid number of current segment file */
    TransactionId xid = segnum * segnum_xid;
    int nread = 0;
    int byte_index = 0;
    int bit_index = 0;
    int bshift = 0;
    char path[MAXPGPATH];
    int fd;
    char* byteptr = NULL;
    CLogXidStatus status;

    snprintf(path, MAXPGPATH, "%s/%s/%04X", DataDir, "pg_clog", segnum);

    fd = open(path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        fprintf(stderr, "%s open error!\n", path);
        exit(2);
    }

    while ((nread = read(fd, buffer, BLCKSZ)) != 0) {
        if (nread < 0) {
            fprintf(stderr, "read file error!\n");
            close(fd);
            exit(2);
        }

        while (byte_index < nread) {
            byteptr = buffer + byte_index;

            for (bit_index = 0; bit_index < 4; bit_index++) {
                bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
                status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;
                fprintf(stdout, "xid %u, status: %s\n", xid, xid_status_name[status]);
                xid++;
            }

            byte_index++;
        }

        byte_index = 0;
    }

    if (close(fd)) {
        fprintf(stderr, "file close error !\n");
        exit(2);
    }

    return;
}

static void parse_single_xid(const TransactionId xid)
{
    int pageno = TransactionIdToPage(xid);
    int byteno = TransactionIdToByte(xid);
    int bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;

    int segnum = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;

    int offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int fd;

    char* byteptr = NULL;
    CLogXidStatus status;

    snprintf(path, MAXPGPATH, "%s/%s/%04X", DataDir, "pg_clog", segnum);

    fd = open(path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        fprintf(stderr, "%s open error !\n", path);
        exit(2);
    }

    if (lseek(fd, (off_t)offset, SEEK_SET) < 0) {
        fprintf(stderr, "lseek error !\n");
        close(fd);
        exit(2);
    }

    if (read(fd, buffer, BLCKSZ) != BLCKSZ) {
        fprintf(stderr, "read file error !\n");
        close(fd);
        exit(2);
    }

    byteptr = buffer + byteno;
    status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;
    fprintf(stdout, "xid %u, status: %s\n", xid, xid_status_name[status]);

    if (close(fd)) {
        fprintf(stderr, "file close error !\n");
        exit(2);
    }

    return;
}

int main(int argc, char* argv[])
{
    int c;

    TransactionId xid = InvalidTransactionId;
    int segnum = -1;
    const char* progname = NULL;
    uint32 mode = single;

    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage(progname);
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            fprintf(stdout, "pg_check_clog V1.0 ! \n");
            exit(0);
        }
    }

    while ((c = getopt(argc, argv, "t:n:")) != -1) {
        switch (c) {
            case 't':
                xid = (unsigned)atoi(optarg);
                break;
            case 'n':
                segnum = (unsigned)atoi(optarg);
                break;
            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }

    if (argc > optind)
        DataDir = argv[optind];

    if (DataDir == NULL) {
        fprintf(stderr, "%s: no data directory specified\n", progname);
        fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
        exit(1);
    }

    if (-1 != segnum)
        mode = file;
    else if (TransactionIdIsValid(xid))
        mode = single;
    else {
        fprintf(stderr, "%s need at last one parameter.\n", progname);
        exit(1);
    }

    switch (mode) {
        case file:
            parse_clog_file(segnum);
            break;
        case single:
            parse_single_xid(xid);
            break;
        default:
            fprintf(stderr, "invalid mode.\n");
            exit(1);
            break;
    }

    return 0;
}
