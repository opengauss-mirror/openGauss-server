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

const int SQL_CMD_LEN = (MAXPGPATH * 5);

extern bool test_single_node;

int EndsWith(const char *str, const char *suffix)
{
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}
int EndsWithBin(const char *str)
{ 
    return EndsWith(str, "_bin"); 
}
int EndsWithJavaBin(const char *str) { 
    return EndsWith(str, "Bin"); 
}
/* generate gsql/jdbc command for specific flag */
static void gen_sql_cmd(char * const psql_cmd, int psql_size, const char* testname, const char* infile, 
                        const char* outfile, bool is_binary, bool use_jdbc_client, bool is_jdbc_binary_test)
{
    errno_t rc = EOK;
    size_t offset = 0;
    if (launcher) {
        rc = snprintf_s(psql_cmd + offset, (SQL_CMD_LEN - offset), psql_size - offset, "%s ", launcher);
        securec_check_ss_c(rc, "", "");
        offset += strlen(launcher);
    }
    int port = test_single_node ? myinfo.dn_port[0] : myinfo.co_port[0];
    if (!is_binary){
        if (use_jdbc_client) {
            const char *JDBC_LOCATION_REF = "/opengauss/src/test/regress/jdbc_client/jdbc_client.jar";
            const char *JNI_LOCATION_REF = "/lib";
            char jni_location[MAXPGPATH] = {0};
            char jdbc_location[MAXPGPATH] = {0};
            rc = sprintf_s(jni_location, MAXPGPATH, "%s/%s", outputdir, JNI_LOCATION_REF);
            securec_check_ss_c(rc, "", "");
            rc = sprintf_s(jdbc_location, MAXPGPATH, "%s/%s", outputdir, JDBC_LOCATION_REF);
            securec_check_ss_c(rc, "", "");
            if (is_jdbc_binary_test) {
                (void)snprintf_s(psql_cmd, (SQL_CMD_LEN - offset), psql_size,
                    "java -Djava.library.path=%s -cp %s gauss.regress.jdbc.JdbcClient \
                    localhost %d %s jdbc_regress 1q@W3e4r %s %s",
                    jni_location, jdbc_location, port, dblist->str,
                    testname, // use the test name as parameter
                    outfile);
                securec_check_ss_c(rc, "\0", "\0");
            } else {
                (void)snprintf_s(psql_cmd, (SQL_CMD_LEN - offset), psql_size,
                    "java -Djava.library.path=%s -cp %s gauss.regress.jdbc.JdbcClient \
                    localhost %d %s jdbc_regress 1q@W3e4r %s %s",
                    jni_location, jdbc_location, port, dblist->str,
                    infile, // use the SQL file name as parameter
                    outfile);
                securec_check_ss_c(rc, "\0", "\0");
            }
        } else{
            (void)snprintf_s(psql_cmd + offset,
                             (SQL_CMD_LEN - offset),
                             psql_size - offset,
                             SYSTEMQUOTE "\"%s%sgsql\" -X -p %d -a %s %s -q -d \"%s\" -C "
                                         "< \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
                             psqldir ? psqldir : "",
                             psqldir ? "/" : "",
                             port,
                             (char*)g_stRegrConfItems.acFieldSepForAllText,
                             (char*)g_stRegrConfItems.acTuplesOnly,
                             dblist->str,
                             infile,
                             outfile);
            securec_check_ss_c(rc, "\0", "\0");
        }
    } else {
        rc = snprintf_s(psql_cmd + offset,
            (SQL_CMD_LEN - offset), psql_size - offset,
            "%s > %s 2>&1", infile, outfile);
        securec_check_ss_c(rc, "\0", "\0");
    }
}

/*
 * start a psql test process for specified file (including redirection),
 * and return process ID
 */
static PID_TYPE psql_start_test(
    const char* testname, _stringlist** resultfiles, _stringlist** expectfiles, _stringlist** tags,
    bool use_jdbc_client)
{
    PID_TYPE pid;
    errno_t rc = EOK;
    char infile[MAXPGPATH];
    char outfile[MAXPGPATH];
    char expectfile[MAXPGPATH];
    char psql_cmd[SQL_CMD_LEN];
    bool is_binary = EndsWithBin(testname);
    bool is_jdbc_binary_test = false;
    if (use_jdbc_client && EndsWithJavaBin(testname)) {
        is_jdbc_binary_test = true;
    }
    if (!is_binary && !is_jdbc_binary_test) {
        /*
         * Look for files in the output dir first, consistent with a vpath search.
         * This is mainly to create more reasonable error messages if the file is
         * not found.  It also allows local test overrides when running pg_regress
         * outside of the source tree.
         */
        snprintf(infile, sizeof(infile), "%s/sql/%s.sql", outputdir, testname);
        if (!file_exists(infile)) {
            snprintf(infile, sizeof(infile), "%s/sql/%s.sql", inputdir, testname);
        }
    } else if (is_binary) {
        rc = snprintf_s(infile, MAXPGPATH, MAXPGPATH - 1, "%s/binary/%s.out", inputdir, testname);
 
        securec_check_ss_c(rc, "", "");
    }

    /* If the .sql file does not exist, then record the error in diff summary
     * file and cont */
    if (!is_jdbc_binary_test && !file_exists(infile)) {
        FILE* fp = fopen(difffilename, "a");

        if (fp) {
            (void)fprintf(fp, "\n[%s]: No such file or directory!!\n", infile);
            fclose(fp);
        } else
            fprintf(stderr, _("\n COULD NOT OPEN [%s]!!!!\n"), difffilename);
    }

    if (!use_jdbc_client) { 
        (void)snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, testname);
    } else {
        (void)snprintf(outfile, sizeof(outfile), "%s/results_jdbc/%s.out", outputdir, testname);
    };

    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", outputdir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, testname);

    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    gen_sql_cmd(psql_cmd, sizeof(psql_cmd), testname, infile, outfile, is_binary, use_jdbc_client, is_jdbc_binary_test);

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
