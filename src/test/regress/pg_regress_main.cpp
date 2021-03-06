/*-------------------------------------------------------------------------
 *
 * pg_regress_main --- regression test for the main backend
 *
 * This is a C implementation of the previous shell script for running
 * the regression tests, and should be mostly compatible with it.
 * Initial author of C translation: Magnus Hagander
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/regress/pg_regress_main.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_regress.h"

extern pgxc_node_info myinfo;
extern char* difffilename;

#define DG_LEVEL_REDO_FILE 1
#define DG_LEVEL_TRACE_FILE 2
#define DG_LEVEL_CONTROL_FILE 4
#define DG_LEVEL_LOG_FILE 8
#define DG_LEVEL_DB_FOLDER 16

extern bool test_single_node;

/*
 * start a psql test process for specified file (including redirection),
 * and return process ID
 */
static PID_TYPE psql_start_test(
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
    snprintf(infile, sizeof(infile), "%s/sql/%s.sql", outputdir, testname);
    if (!file_exists(infile))
        snprintf(infile, sizeof(infile), "%s/sql/%s.sql", inputdir, testname);

    /* If the .sql file does not exist, then record the error in diff summary
     * file and cont */
    if (!file_exists(infile)) {
        FILE* fp = fopen(difffilename, "a");

        if (fp) {
            (void)fprintf(fp, "\n[%s]: No such file or directory!!\n", infile);
            fclose(fp);
        } else
            fprintf(stderr, _("\n COULD NOT OPEN [%s]!!!!\n"), difffilename);
    }

    (void)snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, testname);

    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", outputdir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, testname);

    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    if (launcher)
        offset += snprintf(psql_cmd + offset, sizeof(psql_cmd) - offset, "%s ", launcher);

    int port = test_single_node ? myinfo.dn_port[0] : myinfo.co_port[0];
    (void)snprintf(psql_cmd + offset,
        sizeof(psql_cmd) - offset,
        SYSTEMQUOTE "\"%s%sgsql\" -X -p %d -a %s %s -q -d \"%s\" -C"
                    "< \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
        psqldir ? psqldir : "",
        psqldir ? "/" : "",
        port,
        (char*)g_stRegrConfItems.acFieldSepForAllText,
        (char*)g_stRegrConfItems.acTuplesOnly,
        dblist->str,
        infile,
        outfile);

    pid = spawn_process(psql_cmd);

    if (pid == INVALID_PID) {
        fprintf(stderr, _("could not start process for test %s\n"), testname);
        exit(2);
    }

    return pid;
}

static void psql_init(void)
{
    /* set default regression database name */
    add_stringlist_item(&dblist, "regression");
}

static PID_TYPE psql_diagnosis_collect(char* filename)
{
    char query[MAXPGPATH + 31];
    char psql_cmd[MAXPGPATH * 3];
    PID_TYPE pid;

    snprintf(query, sizeof(query), "select collect_diagnosis(\'%s\', %d)", filename, DG_LEVEL_DB_FOLDER);

    snprintf(psql_cmd,
        sizeof(psql_cmd),
        SYSTEMQUOTE "\"%s%sgsql\" -q -d \"%s\" -c \"%s\" > col_diag_temp.out 2>&1" SYSTEMQUOTE,
        psqldir ? psqldir : "",
        psqldir ? "/" : "",
        dblist->str,
        query);

    pid = spawn_process(psql_cmd);

    if (pid == INVALID_PID) {
        exit(2);
    }

    return pid;
}

int main(int argc, char* argv[])
{
    return regression_main(argc, argv, psql_init, psql_start_test, psql_diagnosis_collect);
}
