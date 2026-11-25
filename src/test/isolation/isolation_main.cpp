/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* -------------------------------------------------------------------------
 *
 * isolation_main --- pg_regress test launcher for isolation tests
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/isolation/isolation_main.c
 *
 * -------------------------------------------------------------------------
 */

#include "pg_regress.h"

/*
 * start an isolation tester process for specified file (including
 * redirection), and return process ID
 */
static PID_TYPE isolation_start_test(
    const char* testname, _stringlist** resultfiles, _stringlist** expectfiles, _stringlist** tags)
{
    PID_TYPE pid;
    char infile[MAXPGPATH];
    char outfile[MAXPGPATH];
    char expectfile[MAXPGPATH];
    char psql_cmd[MAXPGPATH * 3];
    size_t offset = 0;

    /*
     * Look for files in the output dir first, consistent with a vpath search.
     * This is mainly to create more reasonable error messages if the file is
     * not found.  It also allows local test overrides when running pg_regress
     * outside of the source tree.
     */
    snprintf(infile, sizeof(infile), "%s/specs/%s.spec", outputdir, testname);
    if (!file_exists(infile))
        snprintf(infile, sizeof(infile), "%s/specs/%s.spec", inputdir, testname);

    snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, testname);

    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", outputdir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, testname);

    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    if (launcher)
        offset += snprintf(psql_cmd + offset, sizeof(psql_cmd) - offset, "%s ", launcher);

    /* TODO: sync to modify isolation test with regress test.
     * the following statement has some limits:
     * 1. myinfo must be set;
     * 2. make sure that 'regress' before 'isolation' in line 'SUBDIRS = ' within file Code/src/test/Makefile
     */
    snprintf(psql_cmd + offset,
        sizeof(psql_cmd) - offset,
        SYSTEMQUOTE "\"./isolationtester\" \"dbname=%s port=%d\" < \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
        dblist->str,
        myinfo.co_port[0],
        infile,
        outfile);

    pid = spawn_process(psql_cmd);

    if (pid == INVALID_PID) {
        fprintf(stderr, _("could not start process for test %s\n"), testname);
        exit(2);
    }

    return pid;
}

static void isolation_init(void)
{
    /* set default regression database name */
    add_stringlist_item(&dblist, "isolationtest");
}

int main(int argc, char* argv[])
{
    return regression_main(argc, argv, isolation_init, isolation_start_test, NULL);
}
