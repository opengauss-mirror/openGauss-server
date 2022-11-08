/*-------------------------------------------------------------------------
 * pg_regress.h --- regression test driver
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/regress/pg_regress.h
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <unistd.h>

#ifndef WIN32
#define PID_TYPE pid_t
#define INVALID_PID (-1)
#else
#define PID_TYPE HANDLE
#define INVALID_PID INVALID_HANDLE_VALUE
#endif

#define MAX_TUPLE_ONLY_STRLEN 5
#define MAX_COLUMN_SEP_STRLEN 5

typedef struct pgxc_node_info {
    int co_num;
    int* co_port;
    int* co_ctl_port;
    int* co_sctp_port;
    int* co_pool_port;
    int co_pid[10];
    int dn_num;
    int* dn_port;
    int* dns_port;
    int dn_pid[20];
    int* dn_pool_port;
    int* dn_ctl_port;
    int* dn_sctp_port;
    int* dns_ctl_port;
    int* dns_sctp_port;
    int* dn_primary_port;
    int* dn_standby_port;
    int* dn_secondary_port;
    int gtm_port;
    int gtm_pid;
    bool keep_data;
    bool run_check;
    int shell_pid[50];
    int shell_count;
} pgxc_node_info;

extern pgxc_node_info myinfo;

/* simple list of strings */
typedef struct _stringlist {
    char* str;
    struct _stringlist* next;
} _stringlist;

/* Structure corresponding to the regression
 * configuration file: regress.conf */
typedef struct tagREGR_CONF_ITEMS_STRU {
    char acFieldSepForAllText[MAX_COLUMN_SEP_STRLEN + 5]; /* Column seperator for
                                                           * table query result
                                                           * for aligned and
                                                           * unaligned text
                                                           * +2 for -C, +2 for
                                                           * two " character and
                                                           * +1 for \0 */
    char acTuplesOnly[MAX_TUPLE_ONLY_STRLEN];             /* String indicating the value
                                                           * of "column_name_present"
                                                           * configuration item. -t
                                                           * indicates to print tuples
                                                           * only*/
} REGR_CONF_ITEMS_STRU;

/* Structure for the storage of the details of the replacement pattern strings
 * that may be provided in the regress.conf */
typedef struct tagREGR_REPLACE_PATTERNS_STRU {
    int iNumOfPatterns;                 /* Total number of patterns already
                                         * loaded into Memry */
    int iMaxNumOfPattern;               /* Max num of pattern that can be loaded
                                         * as of now */
    int iRemainingPattBuffSize;         /* Remaining space in the storage
                                         * buffer */
    unsigned int* puiPatternOffset;     /* Points to the start of the memory
                                         * block allocated for the storage of
                                         * the replacement pattern strings and
                                         * their values including the index for
                                         * them*/
    unsigned int* puiPattReplValOffset; /* Points to the index for the strings
                                         * that are to be replaced for the
                                         * replacement pattern strings */
    char* pcBuf;                        /* Points to the memory where actual
                                         * replacement pattern strings and their
                                         * corresponding values are stored */
} REGR_REPLACE_PATTERNS_STRU;

/* To store the values of the regress.conf values */
extern REGR_CONF_ITEMS_STRU g_stRegrConfItems;

typedef PID_TYPE (*test_function)(const char*, _stringlist**, _stringlist**, _stringlist**, bool use_jdbc_client);
typedef void (*init_function)(void);

typedef PID_TYPE (*diag_function)(char*);

extern char* bindir;
extern char* libdir;
extern char* datadir;
extern char* host_platform;

extern _stringlist* dblist;
extern bool debug;
extern char* inputdir;
extern char* outputdir;
extern char* launcher;
extern char* top_builddir;

/*
 * This should not be global but every module should be able to read command
 * line parameters.
 */
extern char* psqldir;

extern const char* basic_diff_opts;
extern const char* pretty_diff_opts;

int regression_main(int argc, char* argv[], init_function ifunc, test_function tfunc, diag_function dfunc);
void add_stringlist_item(_stringlist** listhead, const char* str);
PID_TYPE spawn_process(const char* cmdline);
void exit_nicely(int code);
void replace_string(char* string, char* replace, char* replacement);
bool file_exists(const char* file);

PID_TYPE psql_ss_start_test(
    const char* testname, _stringlist** resultfiles, _stringlist** expectfiles, _stringlist** tags,
    bool is_stanby);
