/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * pg_check_replslot.cpp
 *        A simple hack program for internal files
 *
 *
 *
 * IDENTIFICATION
 *        contrib/pg_check_replslot/pg_check_replslot.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

#include "replication/slot.h"

static void usage(const char* progname);
static void parse_replslot_file(const char* datadir, char* slot);
static char* slurpFile(const char* path, size_t* filesize);
static void show_slot(char* buffer, size_t size);

int main(int argc, char* argv[])
{
    int c;
    char* DataDir = NULL;
    const char* progname = NULL;
    char* slotname = NULL;
    char* env = NULL;

    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage(progname);
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("pg_check_replslot V1.0.1");
            exit(0);
        }
    }

    while ((c = getopt(argc, argv, "D:s:")) != -1) {
        switch (c) {
            case 'D':
                DataDir = strdup(optarg);
                if (DataDir == NULL) {
                    fprintf(stderr, "out of memory\n");
                    exit(1);
                }
                break;
            case 's':
                slotname = strdup(optarg);
                if (slotname == NULL) {
                    fprintf(stderr, "out of memory\n");
                    exit(1);
                }
                break;
            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }

    if (optind < argc && DataDir == NULL)
        DataDir = argv[optind++];
    else {
        if ((env = getenv("GAUSSDATA")) != NULL && *env != '\0')
            DataDir = env;
    }

    if (DataDir == NULL) {
        fprintf(stderr, "%s: no data directory specified\n", progname);
        fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
        exit(1);
    }

    /* parser engine */
    parse_replslot_file(DataDir, slotname);

    return 0;
}

static void parse_replslot_file(const char* datadir, char* slot)
{
    bool showall = (slot == NULL) ? true : false;
    DIR* xldir = NULL;
    struct dirent* xlde;
    char replslotpath[MAXPGPATH] = {0};
    char slotpath[MAXPGPATH] = {0};

    if (datadir == NULL)
        return; /* do nothing */

    snprintf(replslotpath, MAXPGPATH, "%s/pg_replslot", datadir);
    if (!showall)
        snprintf(slotpath, MAXPGPATH, "%s/pg_replslot/%s/state", datadir, slot);

    xldir = opendir(replslotpath);
    if (xldir == NULL) {
        fprintf(stderr, _("could not open directory \"%s\": %s\n"), replslotpath, strerror(errno));
        exit(0);
    }

    while (errno = 0, (xlde = readdir(xldir)) != NULL) {
        struct stat fst;
        char path[MAXPGPATH];
        size_t size;
        char* buffer = NULL;

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
            continue;

        /* get the state file path */
        snprintf(path, MAXPGPATH, "%s/%s/state", replslotpath, xlde->d_name);

        if (!showall && strncmp(slotpath, path, strlen(path)) != 0)
            continue;

        if (lstat(path, &fst) == 0) {
            buffer = slurpFile(path, &size);
            show_slot(buffer, size);
            free(buffer);
        }
    }

    closedir(xldir);
}

static char* slurpFile(const char* path, size_t* filesize)
{
    int fd;
    char* buffer = NULL;
    struct stat statbuf;
    int len;

    if (path == NULL)
        return NULL;

    if ((fd = open(path, O_RDONLY | PG_BINARY, 0)) == -1) {
        fprintf(stderr, _("could not open file \"%s\" for reading: %s\n"), path, strerror(errno));
        exit(1);
    }

    if (fstat(fd, &statbuf) < 0) {
        fprintf(stderr, _("could not open file \"%s\" for reading: %s\n"), path, strerror(errno));
        exit(1);
    }

    len = statbuf.st_size;

    buffer = (char*)malloc(len + 1);
    if (buffer == NULL) {
        fprintf(stderr, _("Failed to alloc memory."));
        exit(1);
    }

    if (read(fd, buffer, len) != len) {
        fprintf(stderr, _("could not read file \"%s\": %s\n"), path, strerror(errno));
        exit(1);
    }
    close(fd);

    /* Zero-terminate the buffer. */
    buffer[len] = '\0';

    if (filesize)
        *filesize = len;
    return buffer;
}

static void show_slot(char* buffer, size_t size)
{
#define SLOT_MAGIC 0x1051CA1 /* format identifier */
#define SLOT_VERSION 1       /* version for new files */

    static bool firstcall = true;
    ReplicationSlotOnDisk slot;
    char lsn[64] = {0};

    if (size != sizeof(ReplicationSlotOnDisk)) {
        fprintf(stderr, _("unexpected slot file size %d, expected %d\n"), (int)size, sizeof(ReplicationSlotOnDisk));
        exit(1);
    }

    memcpy(&slot, buffer, sizeof(ReplicationSlotOnDisk));

    /* verify magic number */
    if (slot.magic != SLOT_MAGIC) {
        fprintf(stderr, _("replication slot has wrong magic %u instead of %u"), slot.magic, SLOT_MAGIC);
        exit(1);
    }

    /* verify version */
    if (slot.version != SLOT_VERSION) {
        fprintf(stderr, _("replication slot has unsupported version %u"), slot.version);
        exit(1);
    }

    /* show slot */
    if (firstcall) {
        char blank[87] = {0};

        firstcall = false;
        fprintf(stderr,
            _("%-24s| %-12s| %-8s| %-8s| %-12s| %-12s\n"),
            "slot_name",
            "slot_type",
            "datoid",
            "xmin",
            "restart_lsn",
            "dummy_standby");
        memset(blank, '-', 86);
        fprintf(stderr, _("%s\n"), blank);
    }

    snprintf(lsn, 63, "%X/%X", (uint32)(slot.slotdata.restart_lsn >> 32), (uint32)slot.slotdata.restart_lsn);

    fprintf(stderr,
        _("%-24s| %-12s| %-8u| %-8u| %-12s| %-12s\n"),
        NameStr(slot.slotdata.name),
        (slot.slotdata.database == InvalidOid) ? "physical" : "logical",
        slot.slotdata.database,
        slot.slotdata.xmin,
        lsn,
        slot.slotdata.isDummyStandby ? "t" : "f");
}

static void usage(const char* progname)
{
    printf("%s is a parse slot tool for openGauss.\n\n"
           "Usage:\n"
           "  %s [OPTIONS]... [GAUSSDATA]\n"
           "\noptions:\n"
           "  -D directory   location of the database storage area\n"
           "  -s slotname    set the slot name you want to parse\n"
           "  example        ./pg_check_replslot -D install/data -s slot\n"
           "\nCommon options:\n"
           "  --help       show this help, then exit\n"
           "  --version    output version information, then exit\n"
           "\n",
        progname,
        progname);
}
