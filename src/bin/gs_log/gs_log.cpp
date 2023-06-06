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
 *---------------------------------------------------------------------------------------
 *
 *  gs_log.cpp
 *	        core interface and implements of log dumping control.
 *	        split control flow, data processing and output view.
 *          tools for binary logs, now including profile log.
 *
 * IDENTIFICATION
 *        src/bin/gs_log/gs_log.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <assert.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <libgen.h>

#include "gs_log_dump.h"
#include "bin/elog.h"
#include "port.h"
using namespace std;

/* append a new line */
#define PRINT_NEWLN() printf("\n")

#define THREAD_ID_FMT "[0x%lx] "
#define THREAD_ID() pthread_self()

#ifndef USE_ASSERT_CHECKING
#define ASSERT(condition)
#else
#define ASSERT(condition) assert(condition)
#endif

/*
 * callback protype for type directory.
 * the first argument: output argument specified by caller
 * the second argument: text of current parent directory.
 * the third argument: text of current child directory.
 */
typedef void (*cb_for_dir)(void*, const char*, const char*);

/*
 * callback protype for regular file type.
 * the first argument: output argument specified by caller
 * the second argument: full filename.
 */
typedef void (*cb_for_file)(void*, const char*);

typedef vector<string> job_type;

typedef struct {
    /* short option */
    char* short_opt;

    /* long option */
    char* long_opt;

    /* function description */
    char* opt_desc;

} arg_opt;

static const arg_opt dump_options[] = {
    {"-f", "", "log file name"}, {"-r", "", "remove source log file after dumping successfully"}};

static const arg_opt dumpall_options[] = {
    {"-d", "", "log directory to dump recursively"}, {"-r", "", "remove source log file after dumping successfully"}};

static const arg_opt gensql_options[] = {{"-o", "", "output sql file"}};

/* gs_log sub-commands list */
enum subcmd {
    SUBCMD_HELP = 0, /* help sub-command */
    SUBCMD_DUMP,     /* dump sub-command, input a log file */
    SUBCMD_DUMPALL,  /* dumpall sub-command, input a log directory */
    SUBCMD_GENSQL,   /* generate SQL statements about DDL */
    SUBCMD_MAX
};

static const char* log_subcommands[SUBCMD_MAX] = {
    "help" /* subcommand help */
    ,
    "dump" /* dump binary log */
    ,
    "dumpall" /* dump all binary log under a dir */
    ,
    "gensql" /* generate SQL statements about DDL */
};

typedef struct {
    /* acquire mylock before accessing the following members */
    pthread_mutex_t mylock;
    pthread_cond_t mycond;

    /* each item within thsi job list is a directory. */
    job_type* jobs;

    /*
     * set EXIT flag before worker thread exits
     * it's written by worker thread, and read by main thread.
     */
    bool exit_flag;

    /*
     * set job assign done flag for all worker thread.
     * it's written by main thread, and read by worker thread.
     */
    bool assign_done;
} worker_input;

typedef void* (*work_f)(void*);

typedef struct {
    /* thread id */
    pthread_t thread_id;

    /* main entry of worker thread */
    work_f worker_main;

    /* input argument into worker passed from main thread */
    worker_input worker_args;
} worker_data;

/* print all options/arguments for one sub-command */
#define print_subcmd_options(cmdopt)                                                                    \
    do {                                                                                                \
        int len = sizeof(cmdopt) / sizeof((cmdopt)[0]);                                                 \
        for (int i = 0; i < len; ++i) {                                                                 \
            printf("  %s, %s %s\n", (cmdopt)[i].short_opt, (cmdopt)[i].long_opt, (cmdopt)[i].opt_desc); \
        }                                                                                               \
        PRINT_NEWLN();                                                                                  \
    } while (0)

static void scan_curdir_and_do(const char*, cb_for_dir, void*, cb_for_file, void*);
static void* worker_main(void* in_args);
static worker_data* create_and_init_worker_data(int workers_num);
static void run_workers(worker_data* workers, const int workers_num);
static void split_jobs_to_workers(job_type*, worker_data*, const int);
static void wait_workers_complete(worker_data*, int);

/*
 * Global variable, whether remove source log file after dumping it.
 * it's set/written by the main thread, and read by all the worker threads.
 * so needn't any locking.
 */
static bool g_rm_logfile = false;

/* print all sub-commands excluding HELP subcommand */
static void print_valid_subcmds(void)
{
    int len = sizeof(log_subcommands) / sizeof(log_subcommands[0]);

    /* skip help subcommand */
    for (int i = 1; i < len; ++i) {
        printf("  %s\n", log_subcommands[i]);
    }
    PRINT_NEWLN();
}

/* sub-command GENSQL usage */
static void help_subcmd_gensql(const char* progname)
{
    printf("generate SQL statement for creating table.\n");
    printf("\nUsage: %s %s [OPTIONS] \n", progname, log_subcommands[SUBCMD_GENSQL]);
    printf("\nValid options:\n");
    print_subcmd_options(gensql_options);
}

/* sub-command DUMP usage */
static void help_subcmd_dump(const char* progname)
{
    printf("parse and dump one profile log to readable text.\n");
    printf("\nUsage: %s %s [OPTIONS] \n", progname, log_subcommands[SUBCMD_DUMP]);
    printf("\nValid options:\n");
    print_subcmd_options(dump_options);
}

/* sub-command DUMPALL usage */
static void help_subcmd_dumpall(const char* progname)
{
    printf("parse and dump batch profile logs to readable text.\n");
    printf("\nUsage: %s %s [OPTIONS] \n", progname, log_subcommands[SUBCMD_DUMPALL]);
    printf("\nValid options:\n");
    print_subcmd_options(dumpall_options);
}

/* top-command usage */
static void help(const char* progname)
{
    printf("%s is a tool for %s log.\n"
           "\n"
           "Usage: %s <subcommand> [OPTIONS] \n",
        progname,
        GAUSS_ID,
        progname);

    printf("Type '%s help <subcommand>' for help on a specific subcommand.\n", progname);
    printf("\nAvailable subcommands:\n");
    print_valid_subcmds();
}

/* enter pointer of HELP function */
static void enter_help_br(int argc, char* const* argv)
{
    if (2 == argc) {
        help(get_progname(argv[0]));
        exit(0);
    }

    int cmdidx = -1;
    for (int i = 1; i < SUBCMD_MAX; ++i) {
        if (strcmp(argv[2], log_subcommands[i]) == 0) {
            cmdidx = i;
            break;
        }
    }
    if (cmdidx < 0) {
        fprintf(stderr, "Invalid subcommands. Available subcommands:\n");
        print_valid_subcmds();
        exit(0);
    }

    /* subcommand help branches */
    switch (cmdidx) {
        case SUBCMD_DUMP:
            help_subcmd_dump(get_progname(argv[0]));
            break;
        case SUBCMD_DUMPALL:
            help_subcmd_dumpall(get_progname(argv[0]));
            break;
        case SUBCMD_GENSQL:
            help_subcmd_gensql(get_progname(argv[0]));
            break;
        default:
            break;
    }
    exit(0);
}

/*
 * @Description: compute the output file name given input file name.
 * @Param[IN] infile: input file name
 * @Return: output file name
 * @See also:
 */
static char* get_log_default_outfile(const char* infile)
{
    size_t infile_len = strlen(infile);
    size_t outfile_len = 0;
    char* outfile = NULL;
    int ret = 0;

    if (infile_len > 0) {
        /* find the suffix of input file name */
        char* p = (char*)memrchr(infile, '.', infile_len);

        /*
         * 5 = strlen(".prf") + tail 0
         * if infile has its suffix, replace it with 'txt'.
         * otherwise just append '.txt' with the whole infile string.
         */
        outfile_len = p != NULL ? ((p - infile) + 5) : (infile_len + 5);
        outfile = (char*)malloc(outfile_len);
        if (NULL != outfile) {
            ret = memcpy_s(outfile, outfile_len, infile, outfile_len - 5);
            secure_check_ret(ret);
            outfile[outfile_len - 5] = '.';
            outfile[outfile_len - 4] = 't';
            outfile[outfile_len - 3] = 'x';
            outfile[outfile_len - 2] = 't';
            outfile[outfile_len - 1] = '\0';
            return outfile;
        }
    }
    return NULL;
}

/*
 * according to input filename, compute its output log filename,
 * and try to open it. if open failed, return NULL.
 */
static FILE* open_default_out_logfile(const char* infile)
{
    char* log_outfile = NULL;
    FILE* log_outfd = NULL;

    log_outfile = get_log_default_outfile(infile);
    if (NULL != log_outfile) {
        canonicalize_path(log_outfile);
        log_outfd = fopen(log_outfile, "w");
        if (log_outfd == NULL) {
            fprintf(
                stderr, THREAD_ID_FMT "Failed to open FILE \"%s\": %s\n", THREAD_ID(), log_outfile, gs_strerror(errno));
            free(log_outfile);
            log_outfile = NULL;
            return NULL;
        }
        int fd = fileno(log_outfd);
        if (fd == -1) {
            fprintf(stderr,
                THREAD_ID_FMT "Failed to convert FILE \"%s\": %s\n",
                THREAD_ID(),
                log_outfile,
                gs_strerror(errno));
            fclose(log_outfd);
            free(log_outfile);
            log_outfile = NULL;
            return NULL;
        }
        int ret = fchmod(fd, S_IRUSR | S_IWUSR);
        if (ret != 0) {
            fprintf(stderr,
                THREAD_ID_FMT "Could not set permissions of file \"%s\": %s\n",
                THREAD_ID(),
                log_outfile,
                gs_strerror(errno));
            fclose(log_outfd);
            free(log_outfile);
            log_outfile = NULL;
            return NULL;
        }
        free(log_outfile);
        log_outfile = NULL;
    }
    return log_outfd;
}

/* dump one profile log file */
static int dump_single_logfile(gslog_dumper* dumper, const char* infile)
{
    int ret = dumper->set_logfile(infile);
    if (ret != 0) {
        fprintf(stderr, THREAD_ID_FMT "Failed to set logfile \"%s\", ", THREAD_ID(), infile);
        /* input log file maybe don't exist */
        dumper->print_detailed_errinfo();
        return -1;
    }

    FILE* log_outfd = open_default_out_logfile(infile);
    if (NULL == log_outfd) {
        fprintf(stderr,
            THREAD_ID_FMT "Failed to create output file for \"%s\": %s.\n",
            THREAD_ID(),
            infile,
            strerror(errno));
        return -1;
    }

    dumper->set_logtype(LOG_TYPE_PLOG);
    dumper->set_out_fd(log_outfd);

    if (0 != dumper->dump()) {
        /* skip this error and handle next file */
        ret = 1;
        fprintf(stderr, THREAD_ID_FMT, THREAD_ID());
        dumper->print_detailed_errinfo();
    } else if (g_rm_logfile) {
        /* close fd of input log file before unlink it */
        dumper->close_logfile();
        (void)unlink(infile);
        ret = 0;
    }
    (void)fclose(log_outfd);
    log_outfd = NULL;

    return ret;
}

/*
 * branch to dump a single profile log.
 * default output file is suffixed with ".txt" and under the
 * same directory where input log file lays.
 */
static void enter_dump_br(int argc, char* const* argv)
{
    char* log_infile = NULL;
    int c = 0;
    int ret = 0;

    while ((c = getopt(argc, argv, "f:r")) != -1) {
        switch (c) {
            case 'f': {
                check_env_value_c(optarg);
                log_infile = optarg;
                break;
            }
            case 'r': {
                g_rm_logfile = true;
                break;
            }
            default: {
                fprintf(stderr,
                    _("Try \"%s help %s\" for more information.\n"),
                    get_progname(argv[0]),
                    log_subcommands[SUBCMD_DUMP]);
                exit(0);
            }
        }
    }

    if (NULL == log_infile) {
        fprintf(stderr, "Must specify a file(-f) to dump.\n");
        exit(1);
    }

    ret = set_timezone_dirpath();
    if (ret != 0) {
        gslog_detailed_errinfo(ret);
        exit(1);
    }
    set_threadlocal_vars();

    gslog_dumper* dumper = new (std::nothrow) gslog_dumper();
    if (NULL == dumper) {
        fprintf(stderr, THREAD_ID_FMT "Failed to new log dump object.\n", THREAD_ID());
        exit(0);
    }
    (void)dump_single_logfile(dumper, log_infile);
    delete dumper;
}

/*
 * callback function of type scan_dir_callback.
 */
static void callback_for_subdir(void* dir_list, const char* parent_dir, const char* child_dir)
{
    char fullpath[MAXPGPATH] = {0};
    int ret = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", parent_dir, child_dir);
    secure_check_ss(ret);

    job_type* jobs = (job_type*)dir_list;
    jobs->push_back(fullpath);
}

/*
 * judge this file is of type profile log.
 * we use file suffix as its identifier flag.
 */
static inline bool type_of_profile_log(const char* filename)
{
    size_t flen = strlen(filename);
    return ((flen > 4) && (0 == strncmp(PROFILE_LOG_SUFFIX, filename + flen - 4, 4)));
}

/*
 * callback for a regular file.
 * collect all the log files.
 */
static void callback_for_reg_file(void* file_list, const char* fullname)
{
    if (type_of_profile_log(fullname)) {
        job_type* logfiles = (job_type*)file_list;
        logfiles->push_back(fullname);
    }
}

/*
 * @Description: Given input directory path, we will dump all profile
 *    log files under it.
 * @Param [IN] in_dir: input file path by user
 * @Return: void
 * @See also:
 */
static void dump_dir_r(const char* in_dir)
{
    job_type ping_dirlist;
    job_type pang_dirlist;
    job_type all_logfiles;

    job_type* ping = &ping_dirlist;
    job_type* pang = &pang_dirlist;
    job_type* logfiles = &all_logfiles;

    /* the max number of workers is an arbitrary choice */
    const int workers_num = 6;
    const int batch_jobs = 20 * workers_num;

    worker_data* workers = create_and_init_worker_data(workers_num);
    if (NULL == workers) {
        fprintf(stderr,
            THREAD_ID_FMT "Failed to preapre for starting worker thread, requred %zu bytes\n",
            THREAD_ID(),
            (sizeof(worker_data) * workers_num));
        _exit(1);
    }
    run_workers(workers, workers_num);

    ping->push_back(in_dir);

    /* step1: collect all the logfiles */
    while (ping->size() > 0) {
        job_type::iterator iter = ping->begin();
        for (; iter != ping->end(); iter++) {
            const char* cur_dir = iter->c_str();
            scan_curdir_and_do(cur_dir, callback_for_subdir, pang, callback_for_reg_file, logfiles);

            /*
             * check currnt number of log files. if need,
             * fill job list of each worker, and notify them to dump.
             */
            if (logfiles->size() >= batch_jobs) {
                split_jobs_to_workers(logfiles, workers, workers_num);
                logfiles->resize(0);
            }
        }

        /* clear up ping list */
        ping->resize(0);

        /* swap ping and pang pointer */
        job_type* tmp = ping;
        ping = pang;
        pang = tmp;
    }

    if (logfiles->size() > 0) {
        split_jobs_to_workers(logfiles, workers, workers_num);
        logfiles->resize(0);
    }
    wait_workers_complete(workers, workers_num);

    /* ok, do cleanup work */
    for (int i = 0; i < workers_num; i++) {
        delete workers[i].worker_args.jobs;
    }
    free(workers);
    workers = NULL;
}

/* create and init informatin about worker thread */
static worker_data* create_and_init_worker_data(int workers_num)
{
    worker_data* worker = (worker_data*)malloc(sizeof(worker_data) * workers_num);
    if (NULL == worker) {
        return NULL;
    }

    for (int i = 0; i < workers_num; ++i) {
        worker_data* w = worker + i;

        w->thread_id = (pthread_t)-1;
        w->worker_main = &worker_main;

        /* at default main thread will set them be flase. */
        w->worker_args.exit_flag = false;
        w->worker_args.assign_done = false;
        w->worker_args.mylock = PTHREAD_MUTEX_INITIALIZER;
        w->worker_args.mycond = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
        w->worker_args.jobs = new (std::nothrow) job_type;
        if (NULL == w->worker_args.jobs) {
            free(worker);
            worker = NULL;
            return NULL;
        }
    }
    return worker;
}

static inline void split_jobs(size_t total_jobs, size_t total_workers, size_t& wokers_part1, size_t& jobs_part1)
{
    if (total_workers == 0) {
        return;
    }
    jobs_part1 = total_jobs / total_workers;

    if (jobs_part1 * total_workers != total_jobs) {
        /*
         * PART 1: the number of workers is wokers_part1, and
         *         the number of jobs is average value plus 1.
         * PART 2: the number of workers is (total_workers - wokers_part1),
         *         the number of jobs is average value.
         */
        wokers_part1 = (total_jobs - jobs_part1 * total_workers);
        ++jobs_part1;
    } else {
        /* all workers have the same number of jobs to do */
        wokers_part1 = total_workers;
    }
}

/* split all jobs into each worker at average */
static void split_jobs_to_workers(job_type* jobs, worker_data* worker, const int workers_num)
{
    size_t batch_jobs1 = 0;
    size_t workers_num1 = 0;

#ifdef ENABLE_UT
    split_jobs(6, 6, workers_num1, batch_jobs1);
    ASSERT(workers_num1 == 6);
    ASSERT(batch_jobs1 == 1);
    split_jobs(6, 5, workers_num1, batch_jobs1);
    ASSERT(workers_num1 == 1);
    ASSERT(batch_jobs1 == 2);
    split_jobs(5, 6, workers_num1, batch_jobs1);
    ASSERT(workers_num1 == 5);
    ASSERT(batch_jobs1 == 1);
#endif /* ENABLE_UT */

    split_jobs(jobs->size(), workers_num, workers_num1, batch_jobs1);

    job_type::iterator iter = jobs->begin();

    for (size_t i = 0; i < workers_num1; ++i) {
        worker_data* w = worker + i;
        (void)pthread_mutex_lock(&(w->worker_args.mylock));
        /* assign (batch_jobs1) jobs to each worker of the first part */
        for (size_t j = 0; j < batch_jobs1 && iter != jobs->end(); iter++, j++) {
            w->worker_args.jobs->push_back(*iter);
        }
        /* wake up worker thread to handle its jobs */
        pthread_cond_signal(&(w->worker_args.mycond));
        (void)pthread_mutex_unlock(&(w->worker_args.mylock));
    }

    for (int i = workers_num1; i < workers_num; i++) {
        worker_data* w = worker + i;
        (void)pthread_mutex_lock(&(w->worker_args.mylock));
        /* assign (batch_jobs1-1) jobs to each worker of the second part */
        for (size_t j = 0; j < batch_jobs1 - 1 && iter != jobs->end(); iter++, j++) {
            w->worker_args.jobs->push_back(*iter);
        }
        /* wake up worker thread to handle its jobs */
        pthread_cond_signal(&(w->worker_args.mycond));
        (void)pthread_mutex_unlock(&(w->worker_args.mylock));
    }
}

/* start and run worker thread */
static void run_workers(worker_data* workers, const int workers_num)
{
    worker_data* w = NULL;
    int ret = 0;

    for (int i = 0; i < workers_num; ++i) {
        w = workers + i;
        ret = pthread_create(&w->thread_id, NULL, w->worker_main, (void*)w);
        if (0 != ret) {
            /* if failed to create worker thread, I guess OS resource may
             * be not enough to run these work. so terminate the whole process.
             */
            fprintf(stderr, THREAD_ID_FMT "Could not create worker thread[%d]: %s \n", THREAD_ID(), i, strerror(errno));
            exit(0);
        }
    }
}

/* wait all the worker thread until they are done */
static void wait_workers_complete(worker_data* workers, int workers_num)
{
    worker_data* w = NULL;
    int exit_thd_count = 0;

    for (int i = 0; i < workers_num; i++) {
        w = workers + i;
        (void)pthread_mutex_lock(&(w->worker_args.mylock));
        /* job assign is done */
        w->worker_args.assign_done = true;
        pthread_cond_signal(&(w->worker_args.mycond));
        (void)pthread_mutex_unlock(&(w->worker_args.mylock));
    }

    for (;;) {
        worker_data* wd = NULL;
        int thd_exit_idx = -1;

        for (int i = 0; i < workers_num; i++) {
            wd = workers + i;
            if (wd->worker_args.exit_flag) {
                thd_exit_idx = i;
                break;
            }
        }

        if (thd_exit_idx < 0) {
            /* sleep until next check */
            sleep(1);
            continue;
        }

        wd = workers + thd_exit_idx;
        pthread_join(wd->thread_id, NULL);

        /* reset its EXIT flag and count */
        wd->worker_args.exit_flag = false;
        exit_thd_count++;

        if (workers_num == exit_thd_count) {
            /* ok, all worker have done and exit */
            break;
        }
    }
}

static void enter_dumpall_br(int argc, char* const* argv)
{
    char* in_dir = NULL;
    int c = 0;
    int ret = 0;

    while ((c = getopt(argc, argv, "d:r")) != -1) {
        switch (c) {
            case 'd': {
                /* check input directory existing and access permissions */
                check_env_value_c(optarg);
                in_dir = optarg;
                ret = access(in_dir, R_OK | X_OK);
                if (ret != 0) {
                    fprintf(stderr, "Access directory \"%s\" failed: %s.\n", in_dir, strerror(errno));
                    exit(0);
                }
                break;
            }
            case 'r': {
                g_rm_logfile = true;
                break;
            }
            default: {
                fprintf(stderr,
                    _("Try \"%s help %s\" for more information.\n"),
                    get_progname(argv[0]),
                    log_subcommands[SUBCMD_DUMPALL]);
                exit(0);
            }
        }
    }

    if (NULL == in_dir) {
        fprintf(stderr, "Must specify a file(-d) to dump recursively.\n");
        exit(1);
    } else {
        /* the main thread will do some testing work, includng
         * timezone info setting. if it fails, report these errors
         * earlier.
         */
        ret = set_timezone_dirpath();
        if (ret != 0) {
            gslog_detailed_errinfo(ret);
            exit(1);
        }
    }

    dump_dir_r(in_dir);
}

static void gensql_main(FILE* outfd)
{
    fprintf(outfd, "CREATE TABLE profile_log ( \n");
    fprintf(outfd, "  hostname text, \n");
    fprintf(outfd, "  reqtime timestamp with time zone, \n");
    fprintf(outfd, "  nodename text, \n");
    fprintf(outfd, "  thread bigint, \n");
    fprintf(outfd, "  xid bigint, \n");
    fprintf(outfd, "  qid bigint, \n");
    fprintf(outfd, "  reqsrc text, \n");
    fprintf(outfd, "  reqtype text, \n");
    fprintf(outfd, "  reqok    int, \n");
    fprintf(outfd, "  reqcount bigint, \n");
    fprintf(outfd, "  reqsize  bigint, \n");
    fprintf(outfd, "  requsec  bigint \n");
    fprintf(outfd, ") with (ORIENTATION = COLUMN) \n");
    fprintf(outfd, "  DISTRIBUTE BY HASH (hostname, nodename) \n");
    fprintf(outfd, " ; \n");
    fprintf(outfd, "\n");
}

static void enter_gensql_br(int argc, char* const* argv)
{
    char* outfile = NULL;
    int c = 0;
    int ret = 0;

    while ((c = getopt(argc, argv, "o:")) != -1) {
        switch (c) {
            case 'o': {
                check_env_value_c(optarg);
                outfile = optarg;
                ret = access(outfile, R_OK | W_OK);
                if (ret == 0) {
                    fprintf(stderr, "File \"%s\" exists.\n", outfile);
                    exit(0);
                }
                break;
            }
            default: {
                fprintf(stderr,
                    _("Try \"%s help %s\" for more information.\n"),
                    get_progname(argv[0]),
                    log_subcommands[SUBCMD_GENSQL]);
                exit(0);
            }
        }
    }

    if (NULL != outfile) {
        /* create output file */
        canonicalize_path(outfile);
        FILE* outfd = fopen(outfile, "w");
        if (NULL == outfd) {
            fprintf(stderr, "Create file \"%s\" failed: %s\n", outfile, strerror(errno));
            exit(0);
        }
        ret = fchmod(outfd->_fileno, S_IRUSR | S_IWUSR);
        if (ret != 0) {
            fprintf(stderr, "fchmod failed: %s\n", strerror(errno));
            (void)fclose(outfd);
            exit(0);
        }
        gensql_main(outfd);
        (void)fclose(outfd);
        outfd = NULL;
    } else {
        /* standard out will be used */
        gensql_main(stdout);
    }
}

int main(int argc, char** argv)
{
    const char* progname = get_progname(argv[0]);
    int cmdidx = -1;

    if (argc > 1) {
        /* help branch */
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0 || strcmp(argv[1], "-h") == 0) {
            help(progname);
            exit(0);
        }

        for (int i = 0; i < SUBCMD_MAX; ++i) {
            if (strcmp(argv[1], log_subcommands[i]) == 0) {
                cmdidx = i;
                break;
            }
        }
    }

    switch (cmdidx) {
        case SUBCMD_HELP: {
            /* help <subcommand> branch */
            enter_help_br(argc, argv);
            break;
        }
        case SUBCMD_DUMP: {
            /* dump single log file branch */
            enter_dump_br(argc, argv);
            break;
        }
        case SUBCMD_DUMPALL: {
            /* dump batch log files branch */
            enter_dumpall_br(argc, argv);
            break;
        }
        case SUBCMD_GENSQL: {
            /* generating SQL statement branch */
            enter_gensql_br(argc, argv);
            break;
        }
        default: {
            if (argc > 1) {
                fprintf(stderr, "%s is not a valid subcommand.\n\n", argv[1]);
            }
            help(progname);
            exit(0);
        }
    }
    if (progname) {
        free((void*)progname);
    }
    return 0;
}

/*
 * @Description: scan current directory and handle its children directory by
 *    calling callback(). '.' and '..' will be ignored.
 * @Param [IN] path: input current directory
 * @Param [IN] dir_cb: callback function for directory type.
 * @Param [IN] dir_cb_args: input arguments of dir_cb callback function
 * @Param [IN] fl_cb: callback function for regular type.
 * @Param [IN] fl_cb_args: input arguments of fl_cb callback function
 * @See also:
 */
static void scan_curdir_and_do(
    const char* cur_dir, cb_for_dir dir_cb, void* dir_cb_args, cb_for_file fl_cb, void* fl_cb_args)
{
    char fullpath[MAXPGPATH] = {0};
    DIR* pdir = NULL;
    struct dirent* ent = NULL;
    struct stat st;
    int ret = 0;

    pdir = opendir(cur_dir);
    if (NULL != pdir) {
        while (NULL != (ent = readdir(pdir))) {
            /* skip . and .. */
            if (0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, "..")) {
                continue;
            }

            ret = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", cur_dir, ent->d_name);
            securec_check_ss_c(ret, "\0", "\0");

            /*
             * don't use ent->d_type to judge whether it's a direcotry.
             * Only the fields d_name and d_ino are specified in POSIX.1-2001.
             * The remaining fields are available on many, but  not  all  systems.
             * the d_type field is available mainly only on BSD systems.
             */
            if (0 == lstat(fullpath, &st) && !S_ISLNK(st.st_mode)) {
                if (S_ISDIR(st.st_mode) && (dir_cb != NULL)) {
                    dir_cb(dir_cb_args, cur_dir, ent->d_name);
                } else if (S_ISREG(st.st_mode) && (fl_cb != NULL)) {
                    fl_cb(fl_cb_args, fullpath);
                }
                /* else: nothing to do */
            }
            /* else: file doesn't exist, nothing to do */
        }
        (void)closedir(pdir);
        pdir = NULL;
    }
}

/* main entry for worker thread */
static void* worker_main(void* in_args)
{
    worker_data* w = (worker_data*)in_args;
    job_type* jobs = w->worker_args.jobs;

    gslog_dumper* dumper = new (std::nothrow) gslog_dumper();
    const char* logfile = NULL;
    int ret = 0;
    bool job_done = false;

    if (NULL == dumper) {
        fprintf(stderr, THREAD_ID_FMT "Failed to create log dumper.\n", THREAD_ID());
        goto error_happened;
    }

    /* set important thread env before starting to work */
    set_threadlocal_vars();

    while (!job_done) {
        (void)pthread_mutex_lock(&(w->worker_args.mylock));
        if (jobs->size() > 0) {
            job_type::iterator iter = jobs->begin();

            /* handle each log file ... */
            for (; iter != jobs->end(); iter++) {
                logfile = iter->c_str();

                ret = dump_single_logfile(dumper, logfile);
                if (ret < 0) {
                    (void)pthread_mutex_unlock(&(w->worker_args.mylock));
                    goto error_happened;
                }
                /* continue to handle the next log file */
            }

            /* clean up after dumping all log files */
            jobs->resize(0);
        }
        (void)pthread_mutex_unlock(&(w->worker_args.mylock));

        (void)pthread_mutex_lock(&(w->worker_args.mylock));
        if (!w->worker_args.assign_done) {
            /*
             * wait until some jobs been assigned, or no job will arrive.
             * we must check job list again, so set job_done be false.
             */
            pthread_cond_wait(&(w->worker_args.mycond), &(w->worker_args.mylock));
            job_done = false;
        } else {
            /* check my job list the last time after no job will arrive */
            job_done = (0 == jobs->size());
        }
        (void)pthread_mutex_unlock(&(w->worker_args.mylock));
    }

error_happened:

    /* release all using resource */

    if (NULL != dumper) {
        delete dumper;
        dumper = NULL;
    }

    /*
     * Done, start to exit thread.
     * it doesn't matter that lock is hold or not, becuase this
     * thread will exit immediately.
     */
    w->worker_args.exit_flag = true;

    return NULL;
}
