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
 *-------------------------------------------------------------------------
 *
 * main.cpp
 * main function file for gs_cgroup utility
 *
 * IDENTIFICATION
 * src/bin/gs_cgroup/main.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <libcgroup.h>

#include "securec.h"
#include "cgutil.h"
#include "pg_config.h"
#include "getopt_long.h"

extern int CheckBackendEnv(const char* input_env_value);

/* global variable to describe Cgroup configuration file */
gscgroup_grp_t* cgutil_vaddr[GSCGROUP_ALLNUM] = {NULL};

/* global variable of gs_cgroup options */
cgutil_opt_t cgutil_opt = {0};

/* the cpu count */
int cgutil_cpucnt = 0;

/* global variable to indicate the user of Cgroup configuration file */
struct passwd* cgutil_passwd_user = NULL;

/* global variable for version info */
static char* cgutil_version = NULL;
/* all cores for OS */
char cgutil_allset[CPUSET_LEN];
/* memory set for OS */
char cgutil_mems[CPUSET_LEN];

char* current_nodegroup = NULL;

#define MAX_PATH_LEN 1024                          /* the max length of the file path */
#define MAX_BUF_SIZE 2048                          /* the max size of the buffer */
#define STATIC_CONFIG_FILE "cluster_static_config" /* the name of cluster static config file */
#define PROG_NAME "gs_cgroup"
/*
 * function name: usage
 * description  : gs_cgroup usage function
 *
 */
static void usage(void)
{
    fprintf(stdout,
        "\ngs_cgroup is used to manage the Gauss Cgroups on each node.\n"
        "Usage:\n gs_cgroup [OPTION]...\n\n"
        "OPTIONS:\n"
        " -a [--abort]      : the abort exception flag, should be used with '-E data'.\n"
        " -b pct            : backend group percentage\n"
        " -B name           : specify the group name together with '-u'\n"
        " -c                : create default control groups, \n"
        "                     with '-S' and '-G' to create specified class groups and workload groups;\n"
        "                     with '-N' to create control group of the specified logical cluster.\n"
        " -d                : drop all control groups, with '-S' and '-G' to drop specified groups\n"
        "                     with '-N' to drop control group of the specified logical cluster.\n"
        " -D mpoint         : specify a mount point instead of default point: \"/dev/cgroup/subsystem\"\n"
        " -E data           : Exception data with the following format string: \n"
        "                     blocktime=value (unit is second) \n"
        "                     elapsedtime=value (unit is second) \n"
        "                     allcputime=value (unit is second) \n"
        "                     qualificationtime=value (unit is second) \n"
        "                     cpuskewpercent=value (0 ~ 100) \n"
        "                     spillsize=value (unit is MB) \n"
        "                     broadcastsize=value (unit is MB) \n"
        "                     these strings can be joined with ',' sperator.\n"
        " -h [--help]       : help information\n"
        " -H                : GAUSSHOME PATH for the specified user\n"
        " -f                : to specify cpu cores to use like this: a or a-b.\n"
        "                     the argument is only used on Gaussdb:user group.\n"
        " --fixed           : allocate cpu cores by percentage for different groups.\n"
        " -g pct            : workload group percentage\n"
        " -G name           : specify the group name together with '-c', '-d', '-u' or '-E' \n"
        "                     and '-S' option; ',' operator is used to join multiple groups.\n"
        " -m                : mount cgroups\n"
        " -M                : umount cgroups\n"
        " -N [--group] name : specify the name of logical cluster.\n"
        " -p                : display the default cgroups configuration information.\n"
        "                     with '-N' to display the control group configuration of the specified logical cluster.\n"
        " -P                : display all cgroups tree information of whole cluster.\n"
        " --penalty         : the penalty exception flag, should be used with '-E data'.\n"
        " --recover         : recover the group configure to last change by normal user.\n"
        "                     with '-N' to recover control group of the specified logical cluster.\n"
        " --refresh         : refresh the cgroup group based on the configuration file.\n"
        "                     with '-N' to refresh control group of the specified logical cluster.\n"
        " --revert          : revert the group to default.\n"
        " -s pct            : class group percentage\n"
        " -S name           : specify the Class name together with '-c', '-d', '-u' or '-E' option\n"
        "                     if the class name is \"default\", it will be treated as \"DefaultClass\". \n"
        "                     if this option is not set, class name will be 'DefaultClass' while with '-E'.\n"
        " -t pct            : top group percentage\n"
        " -T name           : specify the Top group name together with '-u' option\n"
        " -u                : modify the information of a specified class group with '-S', \n"
        "                     or a specified top group with '-T', \n"
        "                     or a specified workload group with '-G'.\n"
        "                     with '-N' to update control group of the specified logical cluster.\n"
        " -U name           : the user name of database\n"
        " -V [--version]    : show the version.\n"
        "\n"
        "Examples:\n"
        "Root user can execute:\n"
        "gs_cgroup -U name -H path -c : create the default control groups.\n"
        "gs_cgroup -U name -d : drop all control groups.\n"
        "gs_cgroup -m: mount cgroup\n"
        "gs_cgroup -M: umount cgroup\n"
        "\n"
        "Non-root user can execute:\n"
        "gs_cgroup -p: display the control groups configuration information.\n"
        "gs_cgroup -c -S class: create the default control groups for class\n"
        "gs_cgroup -d -S class: drop all control groups of class\n"
        "gs_cgroup -c -S class -G wg1: create wg1 groups for class.\n"
        "gs_cgroup -d -S class -G wg2 : \n"
        "  drop wg2 groups of class, its child group is moved to its level.\n"
        "gs_cgroup -u -T Gaussdb -t 70: \n"
        "  update CPU percentage of Gaussdb cgroup as 70%%.\n"
        "gs_cgroup -u -f 2-8 -T Gaussdb: \n"
        "  update the CPU cores of Gaussdb:user cgroup as 2~8.\n"
        "gs_cgroup -u --fixed -S class1 -s 40: \n"
        "  update the CPU cores percentage of class1 group as 40%% of the Top group: Class\n"
        "gs_cgroup -S class -G wg -E \"blocktime=5,elapsedtime=5\" -a\n"
        "gs_cgroup -S class -G wg -E \"spillsize=256,broadcastsize=100\" -a\n"
        "gs_cgroup -c -N ngname: create control groups for the logical cluster ngname.\n"
        "gs_cgroup -c -N ngname -S class -G wg1: create wg1 groups for class in the logical cluster ngname.\n"
        "gs_cgroup -d -N ngname -S class: drop class control groups in the logical cluster ngname.\n"
        "gs_cgroup -p -N ngname: display the control groups configuration information of the logical cluster ngname.\n"
        "\n");

    (void)fflush(stdout);
}
/*
 * @Description: if more than one level of cgroup is specified, the reduntant groups are set to NULL
 * @IN bkd: check if backend group is being updated
 * @IN grp: check if workload group is being updated
 * @IN cls: check if class group is being updated
 * @IN top: check if top group is being updated
 * @See also:
 */
void check_group_name_redundant(int bkd, int grp, int cls, int top)
{
    if (bkd) {
        cgutil_opt.wdname[0] = '\0';
        cgutil_opt.clsname[0] = '\0';
        cgutil_opt.topname[0] = '\0';
    }
    /* clsname must be left */
    else if (grp) {
        cgutil_opt.bkdname[0] = '\0';
        cgutil_opt.topname[0] = '\0';
    } else if (cls) {
        cgutil_opt.bkdname[0] = '\0';
        if (cgutil_opt.uflag)
            cgutil_opt.wdname[0] = '\0';
        cgutil_opt.topname[0] = '\0';
    } else if (top) {
        cgutil_opt.bkdname[0] = '\0';
        cgutil_opt.wdname[0] = '\0';
        cgutil_opt.clsname[0] = '\0';
    }
}
/*
 * @Description: check percentage for different groups and cpusets.
 * @IN bkd: check if backend group is being updated
 * @IN grp: check if workload group is being updated
 * @IN cls: check if class group is being updated
 * @IN top: check if top group is being updated
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int check_percentage_value(int bkd, int grp, int cls, int top)
{
    /* fixed mode */
    if (cgutil_opt.fixed) {
        /*
         * it is not allowed if more than one group percentage is specified when updating
         * cpuset by percentage
         */
        if (bkd + cls + top + grp > 1) {
            fprintf(stderr, "ERROR: redundant options of cpu core percentage. \n");
            return -1;
        } else if (bkd + cls + top + grp == 0) {
            return 0;
        }

        check_group_name_redundant(bkd, grp, cls, top);

        /* check backend percentage, cpuset percentage range is 1-100 */
        if (cgutil_opt.uflag && bkd) {
            if (cgutil_opt.bkdpct > 100 || cgutil_opt.bkdpct < 0) {
                fprintf(stderr,
                    "ERROR: invalid value for cpu core percentage. "
                    "its range should be 0-100. \n");
                return -1;
            }
            cgutil_opt.setspct = cgutil_opt.bkdpct;
            cgutil_opt.bkdpct = 0;
        }

        /* check group percentage */
        if (cgutil_opt.uflag && grp) {
            if (cgutil_opt.grppct > 100 || cgutil_opt.grppct < 0) {
                fprintf(stderr,
                    "ERROR: invalid value for cpu core percentage. "
                    "its range should be 0-100. \n");
                return -1;
            }
            cgutil_opt.setspct = cgutil_opt.grppct;
            cgutil_opt.grppct = 0;
        }

        /* check class percentage */
        if (cgutil_opt.uflag && cls) {
            if (cgutil_opt.clspct > 100 || cgutil_opt.clspct < 0) {
                fprintf(stderr,
                    "ERROR: invalid value for cpu core percentage. "
                    "its range should be 0-100. \n");
                return -1;
            }
            cgutil_opt.setspct = cgutil_opt.clspct;
            cgutil_opt.clspct = 0;
            cgutil_opt.clssetpct = 1;
        }

        /* check top group percentage */
        if (cgutil_opt.uflag && top) {
            if (cgutil_opt.toppct > 100 || cgutil_opt.toppct < 0) {
                fprintf(stderr,
                    "ERROR: invalid value for cpu core percentage. "
                    "its range should be 0-100. \n");
                return -1;
            }
            cgutil_opt.setspct = cgutil_opt.toppct;
            cgutil_opt.toppct = 0;
        }

        // if user set core percentage is 0, set a flag to show that user set
        if (cgutil_opt.setspct == 0)
            cgutil_opt.setfixed = 1;
    } else {
        if ((cgutil_opt.cflag || cgutil_opt.uflag) && bkd && (cgutil_opt.bkdpct >= 100 || cgutil_opt.bkdpct < 1)) {
            fprintf(stderr,
                "ERROR: invalid value for backend group dynamic percentage. "
                "its range should be 1 ~ 99!\n");
            return -1;
        }

        /* check backend percentage */
        if ((cgutil_opt.cflag || cgutil_opt.uflag) && grp && (cgutil_opt.grppct >= 100 || cgutil_opt.grppct < 1)) {
            fprintf(stderr,
                "ERROR: invalid value for workload group dynamic percentage. "
                "its range should be 1 ~ 99!\n");
            return -1;
        }

        /* check group percentage */
        if ((cgutil_opt.cflag || cgutil_opt.uflag) && cls && (cgutil_opt.clspct >= 100 || (cgutil_opt.clspct < 1))) {
            fprintf(stderr,
                "ERROR: invalid value for class group dynamic percentage. "
                "its range should be 1 ~ 99!\n");
            return -1;
        }

        /* check class percentage */
        if ((cgutil_opt.cflag || cgutil_opt.uflag) && top && (cgutil_opt.toppct >= 100 || cgutil_opt.toppct < 1)) {
            fprintf(stderr,
                "ERROR: invalid value for top group dynamic percentage. "
                "its range should be 1 ~ 99!\n");
            return -1;
        }
    }

    return 0;
}

/*
 * @Description: check if the node group is valid.
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_node_group_name()
{
    char path[MAX_PATH_LEN];
    struct stat stat_buf;

    /* get the static configuration file */
    errno_t sret;
    sret = memset_s(&stat_buf, sizeof(stat_buf), 0, sizeof(stat_buf));
    securec_check_errno(sret, , -1);

    char* exec_path = gs_getenv_r("GAUSSHOME");
    if (NULL == exec_path) {
        fprintf(stderr, "ERROR: Get GAUSSHOME failed, please check.\n");
        return -1;
    }
    if (CheckBackendEnv(exec_path) != 0) {
        return -1;
    }
    sret = snprintf_s(path,
        MAX_PATH_LEN,
        MAX_PATH_LEN - 1,
        "%s/%s/%s_%s%s",
        exec_path,
        GSCGROUP_CONF_DIR,
        GSCFG_PREFIX,
        cgutil_passwd_user->pw_name,
        GSCFG_SUFFIX);
    securec_check_intval(sret, , -1);

    /* check if the file access */
    if (stat(path, &stat_buf) != 0) {
        fprintf(stderr, "ERROR: the file %s doesn't exist.\n", path);
        return -1;
    }

    return 0;
}

/*
 * @Description: check input for security
 * @IN input: input string
 * @Return:  void
 * @See also:
 */
static void check_input_for_security(char* input)
{
    char* danger_token[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "!", "\n", NULL};

    for (int i = 0; danger_token[i] != NULL; ++i) {
        if (strstr(input, danger_token[i]) != NULL) {
            printf("invalid token \"%s\"\n", danger_token[i]);
            exit(1);
        }
    }
}

/*
 * @Description: Check whether the name of class,
 *               Class group and Workload group is valid.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_name_valid(void)
{
    int namelen = GPNAME_LEN / 2 - 1; /* max name length */
    errno_t sret;

    /* check class name and class exception data */
    if (*cgutil_opt.clsname == '\0' && *cgutil_opt.edata) {
        fprintf(stdout,
            "NOTICE: if not specify class name but exceptional data is valid, "
            "class name will be \"%s\"!\n",
            GSCGROUP_DEFAULT_CLASS);
        sret = snprintf_s(cgutil_opt.clsname, GPNAME_LEN, GPNAME_LEN - 1, "%s", GSCGROUP_DEFAULT_CLASS);
        securec_check_intval(sret, , -1);
    }

    /* check class name length */
    if (strlen(cgutil_opt.clsname) > (size_t)namelen) {
        *cgutil_opt.clsname = '\0';
        fprintf(stderr,
            "ERROR: The name of Class group is beyond "
            "its dedicated size which is %d bytes.\n",
            namelen);

        return -1;
    }

    /* check workload group name length */
    if (strlen(cgutil_opt.wdname) > (size_t)namelen - 3) {
        *cgutil_opt.wdname = '\0';
        fprintf(stderr,
            "ERROR: The name of Workload group is beyond "
            "its dedicated size which is %d bytes.\n",
            namelen - 3);

        return -1;
    }

    return 0;
}

/*
 * @Description: check input values.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_input_valid(void)
{
    /* check group name with flag '--fixed' */
    if (*cgutil_opt.clsname == '\0' && *cgutil_opt.wdname == '\0' && *cgutil_opt.bkdname == '\0' &&
        *cgutil_opt.topname == '\0' && cgutil_opt.fixed) {
        fprintf(stderr, "ERROR: Please specify a group name with flag \"--fixed\"\n");
        return -1;
    }

    /* check flag '--fixed' and '-u' */
    if (cgutil_opt.fixed && 0 == cgutil_opt.uflag) {
        fprintf(stderr, "ERROR: Please specify \'--fixed\' flag together with \'-u\' flag.\n");
        return -1;
    }

    /* check group name with flag '-f' */
    if ((*cgutil_opt.clsname || *cgutil_opt.wdname || *cgutil_opt.bkdname ||
            (*cgutil_opt.topname &&
                (0 != strncmp(cgutil_opt.topname, GSCGROUP_TOP_DATABASE, sizeof(GSCGROUP_TOP_DATABASE))))) &&
        *cgutil_opt.sets) {
        fprintf(stderr, "ERROR: Only specify \'-f\' option on Gaussdb Group.\n");
        return -1;
    }

    /* users cannot use -f and --fixed at the same time */
    if (cgutil_opt.fixed && *cgutil_opt.sets) {
        fprintf(stderr, "ERROR: Please specify one option from \'-f\',\'--fixed\'.\n");
        return -1;
    }

    /* get current mount points */
    if (cgexec_get_mount_points() < 0) {
        return -1;
    }

    /* check '-c', '-d', '-u' flag */
    if ((cgutil_opt.cflag && cgutil_opt.dflag) || (cgutil_opt.cflag && cgutil_opt.uflag) ||
        (cgutil_opt.uflag && cgutil_opt.dflag)) {
        fprintf(stderr, "ERROR: please only specify one option from '-c', '-d' and '-u'.\n");
        return -1;
    }

    /* check '-e' flag */
    if (IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_ERROR)) {
        fprintf(stderr, "ERROR: abort and penalty cannot be specified together!\n");
        return -1;
    }

    /* check exception data from '-e' flag */
    if (cgutil_opt.clsname[0] == '\0' && IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_PENALTY)) {
        fprintf(stderr, "ERROR: you must specify a class name with penalty!\n");
        return -1;
    }

    /* set default exception data without '--penalty', '--abort' and '-a' flag */
    if (*cgutil_opt.edata && IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_NONE)) {
        cgutil_opt.eflag = EXCEPT_FLAG(EXCEPT_PENALTY);
        fprintf(stdout, "NOTICE: if do not specify exceptional action, default is penalty!\n");
    }

    /* check '--refresh', '--revert' and '--recover' flag */
    if ((cgutil_opt.cflag || cgutil_opt.dflag || cgutil_opt.uflag) &&
        (cgutil_opt.refresh || cgutil_opt.revert || cgutil_opt.recover)) {
        fprintf(stderr,
            "ERROR: you cannot specify option '-c', '-u' or '-d' with "
            "'--refresh' or '--revert' or '--recover'!\n");
        return -1;
    }

    /* check '--recover' flag */
    if ((geteuid() == 0) && cgutil_opt.recover) {
        fprintf(stderr, "ERROR: you cannpt specify option '--recover' by root user!\n");
        return -1;
    }

    return 0;
}

/*
 * @Description: check user info with flags.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_user_process(void)
{
    /* check root user process */
    if ((geteuid() == 0) && ((cgutil_opt.cflag || cgutil_opt.display || cgutil_opt.uflag || cgutil_opt.dflag) &&
                                cgutil_opt.user[0] == '\0')) {
        fprintf(stderr,
            "ERROR: you must specify the user name with '-c', '-d', '-p' or '-u' "
            "while running as root user.\n");
        return -1;
    }

    /* check non-root user process */
    if (geteuid() && cgutil_opt.user[0] != '\0') {
        fprintf(stderr, "ERROR: you can't specify the user name while running as non-root user.\n");
        return -1;
    }

    /* check user info for '-P' flag */
    if (0 == geteuid() && cgutil_opt.ptree && '\0' == *cgutil_opt.user) {
        fprintf(stderr,
            "ERROR: you must specify the user name when running as root user "
            "to display the cgroup tree.\n");
        return -1;
    }

    /* check non-root user info for '-M' flag */
    if ((cgutil_opt.mflag || cgutil_opt.umflag) && geteuid()) {
        fprintf(stderr, "ERROR: you must run mount or umount cgroup by root user!\n");
        return -1;
    }

    return 0;
}

/*
 * @Description: check all flags.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_flag_process(void)
{
    /* create flag process */
    if (cgutil_opt.cflag) {
        /* check top and backend group name */
        if (cgutil_opt.topname[0] != '\0' || cgutil_opt.bkdname[0] != '\0') {
            fprintf(stderr,
                "ERROR: you can't specify the Top group or backend group"
                " during creating cgroup!\n");
            return -1;
        }

        /* check workload group name and class name */
        if (cgutil_opt.wdname[0] != '\0' && cgutil_opt.clsname[0] == '\0') {
            fprintf(stderr,
                "ERROR: You can' specify the group name without"
                " specifying the class name during creating cgroup!\n");
            return -1;
        }

        if (*cgutil_opt.wdname && NULL != strchr(cgutil_opt.wdname, ':')) {
            fprintf(stderr, "ERROR, workload group cannot be named with ':'. \n");
            return -1;
        }
    }

    /* delete flag process */
    if (cgutil_opt.dflag) {
        /* check top and backend group name */
        if (cgutil_opt.topname[0] != '\0' || cgutil_opt.bkdname[0] != '\0') {
            fprintf(stderr,
                "ERROR: you can't specify the Top group or backend group"
                " during dropping cgroup!\n");
            return -1;
        }
    }

    /* update flag process */
    if (cgutil_opt.uflag &&
        ('\0' == cgutil_opt.topname[0] && '\0' == cgutil_opt.bkdname[0] && '\0' == cgutil_opt.clsname[0])) {
        fprintf(stderr, "ERROR: please specify the Group name when updating!\n");
        return -1;
    }
    return 0;
}

/*
 * @Description: check group names.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_group_name_process(int top, int bkd)
{
    /* check class name and percentage */
    if (cgutil_opt.clspct && '\0' == cgutil_opt.clsname[0]) {
        fprintf(stderr,
            "ERROR: please specify the Class name "
            "together with Class percent!\n");
        return -1;
    }

    /* check workload group name and percentage */
    if (cgutil_opt.grppct && '\0' == cgutil_opt.wdname[0]) {
        fprintf(stderr,
            "ERROR: please specify the Workload name "
            "together with Workload percent!\n");
        return -1;
    }

    /* workload group name special process */
    if (cgutil_opt.wdname[0] != '\0') {
        if ((NULL == strchr(cgutil_opt.wdname, ':') && 0 == strcmp(cgutil_opt.wdname, GSCGROUP_TOP_WORKLOAD)) ||
            (NULL != strchr(cgutil_opt.wdname, ':') &&
                0 == strncmp(cgutil_opt.wdname, GSCGROUP_TOP_WORKLOAD, sizeof(GSCGROUP_TOP_WORKLOAD) - 1))) {
            fprintf(stderr, "ERROR: can't do any operation on %s group!\n", GSCGROUP_TOP_WORKLOAD);
            return -1;
        }
    }

    /* check timeshare group name */
    if (cgutil_opt.wdname[0] && (0 == strcmp(cgutil_opt.wdname, GSCGROUP_RUSH_TIMESHARE) ||
                                    0 == strcmp(cgutil_opt.wdname, GSCGROUP_HIGH_TIMESHARE) ||
                                    0 == strcmp(cgutil_opt.wdname, GSCGROUP_MEDIUM_TIMESHARE) ||
                                    0 == strcmp(cgutil_opt.wdname, GSCGROUP_LOW_TIMESHARE))) {
        fprintf(stderr,
            "ERROR: can't specify the name of Workload group the same as "
            "the name of default Timeshare Group!\n");
        return -1;
    }

    /* top group name special process */
    if (cgutil_opt.topname[0] != '\0') {
        if (!cgutil_opt.uflag) {
            fprintf(stderr, "ERROR: please specify the option '-u' when using top name!\n");
            return -1;
        }

        /* check top percentage with '-f' flag */
        if (!cgutil_opt.fixed && !*cgutil_opt.sets && !cgutil_opt.toppct) {
            fprintf(stderr, "ERROR: please specify the top dynamic percent when using top name!\n");
            return -1;
        } else if (cgutil_opt.fixed &&
                   !(cgutil_opt.setspct || cgutil_opt.toppct || top)) {
            fprintf(stderr,
                "ERROR: please specify the cpu core percent or IO values "
                "when updating fixed values!\n");
            return -1;
        }
    }

    /* backend group name special process */
    if (cgutil_opt.bkdname[0] != '\0') {
        if (!cgutil_opt.uflag) {
            fprintf(stderr,
                "ERROR: please specify the option '-u' "
                "when using backend name!\n");
            return -1;
        }

        /* check backend percent with '--fixed' flag */
        if (cgutil_opt.fixed && !(cgutil_opt.setspct || cgutil_opt.bkdpct || bkd)) {
            fprintf(stderr,
                "ERROR: please specified the cpu core percent or IO values "
                "when updating fixed values!\n");
            return -1;
        }
    }

    /* check backend name and percentage */
    if (cgutil_opt.bkdpct && '\0' == cgutil_opt.bkdname[0]) {
        fprintf(stderr,
            "ERROR: please specify the backend name "
            "together with backend percent!\n");
        return -1;
    }
    /* check backend name and percentage */
    if (cgutil_opt.toppct && '\0' == cgutil_opt.topname[0]) {
        fprintf(stderr,
            "ERROR: please specify the top cgroup name "
            "together with top percent!\n");
        return -1;
    }

    /* Check if the node group has been created or will be created */
    if ('\0' != cgutil_opt.nodegroup[0]) {
        if ((cgutil_opt.clsname[0] != '\0' || cgutil_opt.wdname[0] != '\0' || cgutil_opt.refresh) &&
            -1 == check_node_group_name()) {
            fprintf(stderr, "ERROR: please check if the node group exists!\n");
            return -1;
        }

        /* can't run command by root user */
        if (geteuid() == 0) {
            fprintf(stderr,
                "ERROR: please execute command by non-root user "
                "when the node group is specified!\n");
            return -1;
        }

        if (0 == strcmp(cgutil_opt.nodegroup, GSCGROUP_TOP_CLASS) ||
            0 == strcmp(cgutil_opt.nodegroup, GSCGROUP_TOP_BACKEND)) {
            fprintf(stderr, "ERROR: the name of logical cluster can't be 'Class' or 'Backend'.\n");
            return -1;
        }

        if (!cgutil_opt.cflag && !cgutil_opt.dflag && !cgutil_opt.uflag && !cgutil_opt.display && !cgutil_opt.recover &&
            !cgutil_opt.refresh && ('\0' == *cgutil_opt.edata)) {
            fprintf(stderr, "ERROR: please specify logical cluster with -c/-d/-u/--recover/--refresh option!\n");
            return -1;
        }
    }

    /* Check if the rename flag is set together with nodegroup */
    if (cgutil_opt.rename && '\0' == cgutil_opt.nodegroup[0]) {
        fprintf(stderr, "ERROR: please specify the rename flag together with nodegroup name!\n");
        return -1;
    }

    /* set the name when there is no rename flag */
    if (!cgutil_opt.rename && '\0' != cgutil_opt.nodegroup[0])
        current_nodegroup = cgutil_opt.nodegroup;

    return 0;
}

/*
 * @Description: check user name.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_user_name(void)
{
    /* user name is valid */
    if (cgutil_opt.user[0]) {
        /* check it's root user */
        if (0 == strcmp(cgutil_opt.user, "root")) {
            fprintf(stderr, "ERROR: can't specify the user name as root.\n");
            return -1;
        } else /* get the user id and group id */
        {
            cgutil_passwd_user = getpwnam(cgutil_opt.user);
            if (NULL == cgutil_passwd_user) {
                fprintf(stderr,
                    "ERROR: can't get the uid and gid of %s.\n"
                    "HINT: please check the specified user name.\n",
                    cgutil_opt.user);
                return -1;
            }
        }
    } else {
        /* save current user info */
        cgutil_passwd_user = getpwuid(geteuid());
        if (NULL == cgutil_passwd_user) {
            fprintf(stderr,
                "ERROR: can't get the cgutil_passwd_user.\n"
                "HINT: please check the running user!\n");
            return -1;
        }
    }

    return 0;
}

/*
 * @Description: check all input whether is valid.
 * @IN bkd: backend group id
 * @IN grp: group id
 * @IN cls: class id
 * @IN top: top group id
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_input_isvalid(int bkd, int grp, int cls, int top)
{
    /* check list */
    if (check_name_valid() == -1 || check_input_valid() == -1 || check_user_name() == -1 ||
        check_percentage_value(bkd, grp, cls, top) == -1 || check_group_name_process(top, bkd) == -1 ||
        check_user_process() == -1 || check_flag_process() == -1) {
            return -1;
        }        

    return 0;
}

/*
 * @Description: check cpuset value is valid.
 * @IN cpuset: cpuset value
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_cpuset_value_valid(char* cpuset)
{
    char* bad = NULL;
    int a = -1;
    int b = -1;
    char* p = NULL;

    if (*cpuset == '-') {
        fprintf(stderr, "ERROR: please specify the cpuset with a valid value.\n");
        return -1;
    }

    p = strchr(cpuset, '-');

    /* check "cpuset" value is like this: a-b */
    if (p == NULL) {
        a = (int)strtol(cpuset, &bad, 10);
        if ((bad != NULL) && *bad) {
            fprintf(stderr, "ERROR: please specify the cpuset with \"a-b\" or \"a\"!\n");
            return -1;
        }

        b = a;
    } else {
        *p++ = '\0';

        a = (int)strtol(cpuset, &bad, 10);
        if ((bad != NULL) && *bad) {
            fprintf(stderr, "ERROR: please specify the cpuset with a valid value.\n");
            return -1;
        }

        b = (int)strtol(p, &bad, 10);
        if ((bad != NULL) && *bad) {
            fprintf(stderr, "ERROR: please specify the cpuset with a valid value.\n");
            return -1;
        }
    }

    if ((a < 0) || (b < 0) || (a > b) || (b >= cgutil_cpucnt)) {
        fprintf(stderr, "ERROR: please specify the cpuset with a valid value.\n");
        return -1;
    }

    int rcs = sprintf_s(cgutil_opt.sets, CPUSET_LEN, "%d-%d", a, b);
    securec_check_intval(rcs, , -1);

    return 0;
}

/*
 * @Description: check config flag.
 * @IN void
 * @Return:  1: OK 0: Not OK
 * @See also:
 */
static int check_config_flag(void)
{
    if (cgutil_opt.cflag || (cgutil_opt.dflag && (*cgutil_opt.clsname || *cgutil_opt.nodegroup)) || cgutil_opt.uflag ||
        cgutil_opt.display || (*cgutil_opt.edata && *cgutil_opt.clsname) || cgutil_opt.refresh || cgutil_opt.upgrade ||
        cgutil_opt.revert || cgutil_opt.recover)
        return 1;

    return 0;
}

/*
 * @Description: initialize cgroup config.
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int initialize_cgroup_config(void)
{
    char* hpath = NULL;
    errno_t sret;

    /* retrieve the information of configure file; if it doesn't, create one */
    if (check_config_flag() > 0) {
        if (geteuid() == 0) {
            if ('\0' == cgutil_opt.hpath[0]) {
                fprintf(stderr,
                    "ERROR: you need specify the GAUSSHOME path "
                    "when runing as root user!\n");
                return -1;
            }
        } else {
            if (NULL == (hpath = gs_getenv_r("GAUSSHOME"))) {
                fprintf(stderr, "ERROR: environment variable $GAUSSHOME is not set!\n");
                return -1;
            }
            if (CheckBackendEnv(hpath) != 0) {
                return -1;
            }
            sret = snprintf_s(cgutil_opt.hpath, sizeof(cgutil_opt.hpath), sizeof(cgutil_opt.hpath) - 1, "%s", hpath);
            securec_check_intval(sret, , -1);
        }

        if (-1 == cgconf_parse_config_file()) {
            if (cgutil_opt.dflag && '\0' != cgutil_opt.nodegroup[0]) {
                fprintf(stderr, "WARNING: failed to parse the node group configure file!\n");
            } else {
                fprintf(stderr, "FATAL: failed to parse the configure file!\n");
                return -1;
            }
        }
    }

    return 0;
}

/*
 * @Description: check and get group percent.
 * @IN percent: input percent
 * @IN gtype: group type: class, workload or top
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
static int check_and_get_group_percent(char* percent, char* gtype)
{
    char* bad = NULL;

    if (strcmp(gtype, "top") == 0) {
        cgutil_opt.toppct = (int)strtol(percent, &bad, 10);
    }        
    else if (strcmp(gtype, "class") == 0) {
        cgutil_opt.clspct = (int)strtol(percent, &bad, 10);
    }        
    else if (strcmp(gtype, "workload") == 0) {
        cgutil_opt.grppct = (int)strtol(percent, &bad, 10);
    }        
    else if (strcmp(gtype, "backend") == 0) {
        cgutil_opt.bkdpct = (int)strtol(percent, &bad, 10);
    }        
    else {
        return -1;
    }        

    if ((bad != NULL) && *bad) {
        fprintf(stderr, "ERROR: incorrect %s percent %s!\n", gtype, percent);
        return -1;
    }

    return 0;
}

/*
 * function name: parse_options
 * description  : parse the option of gs_cgroup utility
 * arguments    : as main function arguments
 * return value :
 *            -1: abnormal
 *             0: normal
 */
static struct option long_options[] = {{"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"abort", no_argument, NULL, 'a'},
    {"group", required_argument, NULL, 'N'},
    {"penalty", no_argument, NULL, 1},
    {"upgrade", no_argument, NULL, 2},
    {"refresh", no_argument, NULL, 3},
    {"revert", no_argument, NULL, 4},
    {"fixed", no_argument, NULL, 5},
    {"recover", no_argument, NULL, 6},
    {"rename", no_argument, NULL, 7},
    {NULL, 0, NULL, 0}};

static int parse_options(int argc, char** argv)
{
    int c;
    int option_index;
    int bkd = 0;
    int grp = 0;
    int cls = 0;
    int top = 0;
    int last = 0;
    errno_t sret;

    sret = memset_s(&cgutil_opt, sizeof(cgutil_opt_t), 0, sizeof(cgutil_opt_t));
    securec_check_errno(sret, , -1);

    /* option parse */
    while ((c = getopt_long(
                argc, argv, "ab:B:cdD:E:f:hH:g:G:mMN:pPr:R:s:S:t:T:uU:Vw:W:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'a': /* abort */
                if (IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_NONE))
                    cgutil_opt.eflag = EXCEPT_FLAG(EXCEPT_ABORT);
                else
                    cgutil_opt.eflag = EXCEPT_FLAG(EXCEPT_ERROR);
                break;
            case 'b': /* backend group percentage */
                if (check_and_get_group_percent(optarg, "backend") == -1)
                    return -1;

                bkd = 1;
                break;
            case 'B': /* backend group name */
                sret = strncpy_s(cgutil_opt.bkdname, GPNAME_LEN, optarg, GPNAME_LEN - 1);
                securec_check_errno(sret, , -1);

                check_input_for_security(cgutil_opt.bkdname);

                break;
            case 'c': /* create group */
                cgutil_opt.cflag = 1;
                break;
            case 'd': /* drop group */
                cgutil_opt.dflag = 1;
                break;
            case 'D': /* mount point */
                cgutil_opt.mpflag = 1;
                sret = strncpy_s(cgutil_opt.mpoint, MAXPGPATH, optarg, MAXPGPATH - 1);
                securec_check_errno(sret, , -1);

                check_input_for_security(cgutil_opt.mpoint);

                last = strlen(cgutil_opt.mpoint) - 1;
                if ('/' == cgutil_opt.mpoint[last])
                    cgutil_opt.mpoint[last] = '\0';
                break;
            case 'E': /* Exceptional data */
                sret = strncpy_s(cgutil_opt.edata, EXCEPT_LEN, optarg, EXCEPT_LEN - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.edata);
                break;
            case 'h': /* help */
                usage();
                exit(0);
            case 'H': /* GAUSSHOME path */
                sret = strncpy_s(cgutil_opt.hpath, MAXPGPATH, optarg, MAXPGPATH - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.hpath);
                break;
            case 'f': /* core numbers */
                if (check_cpuset_value_valid(optarg) == -1)
                    return -1;
                break;
            case 'g': /* workload group percentage */
                if (check_and_get_group_percent(optarg, "workload") == -1)
                    return -1;

                grp = 1;
                break;
            case 'G': /* workload group name for "gpname:gplevel" */

                sret = strncpy_s(cgutil_opt.wdname, GPNAME_LEN, optarg, GPNAME_LEN - 1 - 2);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.wdname);
                break;
            case 'm': /* mount cgroup */
                cgutil_opt.mflag = 1;
                break;
            case 'M': /* umount cgroup */
                cgutil_opt.umflag = 1;
                break;
            case 'N': /* Nodegroup information */
                sret = strncpy_s(cgutil_opt.nodegroup, GPNAME_LEN, optarg, GPNAME_LEN - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.nodegroup);
                break;
            case 'p': /* display gscgroup.cfg information */
                cgutil_opt.display = 1;
                break;
            case 'P': /* display Cgroup tree information */
                cgutil_opt.ptree = 1;
                break;
            case 's': /* Class group percentage */
                if (check_and_get_group_percent(optarg, "class") == -1)
                    return -1;

                cls = 1;
                break;
            case 'S': /* Class group name */
                if (strchr(optarg, ':') != NULL) {
                    fprintf(stderr, "ERROR, class cannot be named with ':'. \n");
                    return -1;
                }
                sret = strncpy_s(cgutil_opt.clsname, GPNAME_LEN, optarg, GPNAME_LEN - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.clsname);
                break;
            case 't': /* Top group percentage */
                if (check_and_get_group_percent(optarg, "top") == -1)
                    return -1;

                top = 1;
                break;
            case 'T': /* Top group name */
                sret = strncpy_s(cgutil_opt.topname, GPNAME_LEN, optarg, GPNAME_LEN - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.topname);
                break;
            case 'u': /* update flag */
                cgutil_opt.uflag = 1;
                break;
            case 'U': /* user name */
                sret = strncpy_s(cgutil_opt.user, USERNAME_LEN, optarg, USERNAME_LEN - 1);
                securec_check_errno(sret, , -1);
                check_input_for_security(cgutil_opt.user);
                break;
            case 'V': /* version */
                cgutil_version = DEF_GS_VERSION;
                return 0;
            case 1:
                if (IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_NONE))
                    cgutil_opt.eflag = EXCEPT_FLAG(EXCEPT_PENALTY);
                else
                    cgutil_opt.eflag = EXCEPT_FLAG(EXCEPT_ERROR);
                break;
            case 2:
                cgutil_opt.upgrade = 1;
                break;
            case 3:
                cgutil_opt.refresh = 1;
                break;
            case 4:
                cgutil_opt.revert = 1;
                break;
            case 5:
                cgutil_opt.fixed = 1;
                break;
            case 6:
                cgutil_opt.recover = 1;
                break;
            case 7:
                cgutil_opt.rename = 1;
                break;
            default:
                fprintf(stderr, "ERROR: incorrect option: %s\n.", optarg);
                usage();
                return -1;
        }
    }

    return check_input_isvalid(bkd, grp, cls, top);
}

/*
 * function name: main
 * description  : main entry of gs_cgroup utility
 * arguments    : main function default arguments
 */
int main(int argc, char** argv)
{
    char* cpuset = NULL;
    int ret = 0;

    if (argc < 2) {
        usage();
        exit(-1);
    }

    // log output redirect
    init_log(PROG_NAME);

    /* print the log about arguments of gs_cgroup */
    char arguments[MAX_BUF_SIZE] = {0x00};
    for (int i = 0; i < argc; i++) {
        errno_t rc = strcat_s(arguments, MAX_BUF_SIZE, argv[i]);
        size_t len = strlen(arguments);
        if (rc != EOK || len >= (MAX_BUF_SIZE - 2))
            break;
        arguments[len] = ' ';
        arguments[len + 1] = '\0';
    }
    write_log("The gs_cgroup run with the following arguments: [%s].\n", arguments);

    /* get the cpu count value */
    cgutil_cpucnt = gsutil_get_cpu_count();

    if (cgutil_cpucnt == -1) {
        fprintf(stderr,
            "get cpu core range failed, please check if \"/proc/cpuinfo\""
            " or \"/sys/devices/system\" is acceptable. \n");
        exit(-1);
    }

    int rc = sprintf_s(cgutil_allset, sizeof(cgutil_allset), "%d-%d", 0, cgutil_cpucnt - 1);
    securec_check_intval(rc, , -1);

    /* parse the options */
    ret = parse_options(argc, argv);
    if (-1 == ret) {
        fprintf(stderr, "HINT: please run 'gs_cgroup -h' to display the usage!\n");
        exit(-1);
    }

    if (cgutil_version != NULL) {
        fprintf(stdout, "gs_cgroup %s\n", cgutil_version);
        return 0;
    }

    if (geteuid() == 0 && cgutil_opt.mflag) {
        cgexec_mount_cgroups();
    }        

    if (geteuid() == 0 && cgutil_opt.umflag && !cgutil_opt.dflag) {
        cgexec_umount_cgroups();
        exit(0);
    }

    /* retrieve the information of configure file; if it doesn't, create one */
    if (initialize_cgroup_config() == -1)
        return -1;

    /* check upgrade flag */
    if (cgutil_opt.upgrade) {
        cgutil_opt.refresh = 1;

        /* maybe we need not do upgrade */
        if (cgexec_check_mount_for_upgrade() == -1) {
            goto error;
        }
    }

    if (cgutil_opt.cflag) {
        /* run as root user */
        if (geteuid() == 0 && cgutil_opt.upgrade == 0) {
            /* check if cgroups have been mounted if it doesn't specify mflag */
            if (-1 == (ret = cgexec_mount_root_cgroup())) {
                goto error;
            }
        }
    }

    /* initialize libcgroup */
    ret = cgroup_init();
    if (ret) {
        fprintf(stderr,
            "FATAL: libcgroup initialization failed: %s\n"
            "please run 'gs_cgroup -m' to "
            "mount cgroup by root user!\n",
            cgroup_strerror(ret));
        goto error;
    }

    /* get memory set */
    if (-1 == cgexec_get_cgroup_cpuset_info(TOPCG_ROOT, &cpuset)) {
        fprintf(stderr, "ERROR: failed to get cpusets and mems during initialization.\n");
        goto error;
    }

    rc = snprintf_s(cgutil_allset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpuset);
    securec_check_intval(rc, , -1);
    free(cpuset);
    cpuset = NULL;

    /* create/delete/update operation */
    if (cgutil_opt.cflag) {
        if (cgexec_create_groups() == -1) {
            goto error;
        }
    } else if (cgutil_opt.dflag) {
        if (cgexec_drop_groups() == -1) {
            goto error;
        }
    } else if (cgutil_opt.uflag) {
        if (cgexec_update_groups() == -1) {
            goto error;
        }
    } else if (cgutil_opt.revert) {
        if (cgexec_revert_groups() == -1) {
            goto error;
        }
    }

    /* refresh current groups */
    if (cgutil_opt.refresh) {
        if (cgexec_refresh_groups() == -1) {
            goto error;
        }
    }

    /* recover the last changes of groups */
    if (cgutil_opt.recover) {
        if (cgexec_recover_groups() == -1) {
            goto error;
        }
    }

    /* process the exceptional data */
    if (*cgutil_opt.edata && *cgutil_opt.clsname && -1 == cgexcp_class_exception())
        goto error;

    /* display the cgroup configuration file information */
    if (cgutil_opt.display)
        cgconf_display_groups();

    /* display the cgroup tree information */
    if (cgutil_opt.ptree) {
        if (cgptree_display_cgroups() == -1) {
            goto error;
        }
    }

    if (cgutil_vaddr[0] != NULL)
        (void)munmap(cgutil_vaddr[0], GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));

    return 0;
error:
    if (cgutil_vaddr[0] != NULL)
        (void)munmap(cgutil_vaddr[0], GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));
    write_log("gs_cgroup execution error.\n");
    exit(-1);
}

#ifdef ENABLE_UT
void cgroup_set_default_group()
{
    errno_t sret;
    char tmpstr[GPNAME_LEN];

    cgutil_vaddr[TOPCG_ROOT]->used = 1;
    cgutil_vaddr[TOPCG_ROOT]->gid = TOPCG_ROOT;
    cgutil_vaddr[TOPCG_ROOT]->gtype = GROUP_TOP;
    sret = strncpy_s(cgutil_vaddr[TOPCG_ROOT]->grpname, GPNAME_LEN, GSCGROUP_ROOT, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TOPCG_ROOT]->ginfo.top.percent = 100 * DEFAULT_IO_WEIGHT / MAX_IO_WEIGHT;
    cgutil_vaddr[TOPCG_ROOT]->ainfo.weight = DEFAULT_IO_WEIGHT;
    cgutil_vaddr[TOPCG_ROOT]->percent = 1000;

    /* set root group as default cpu set */
    (void)sprintf_s(cgutil_vaddr[TOPCG_ROOT]->cpuset, CPUSET_LEN, "%s", cgutil_allset);

    cgutil_vaddr[TOPCG_BACKEND]->used = 1;
    cgutil_vaddr[TOPCG_BACKEND]->gid = TOPCG_BACKEND;
    cgutil_vaddr[TOPCG_BACKEND]->gtype = GROUP_TOP;
    sret = strncpy_s(cgutil_vaddr[TOPCG_BACKEND]->grpname, GPNAME_LEN, GSCGROUP_TOP_BACKEND, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TOPCG_BACKEND]->ginfo.top.percent = TOP_BACKEND_PERCENT;
    cgutil_vaddr[TOPCG_BACKEND]->ainfo.shares = DEFAULT_CPU_SHARES * TOP_BACKEND_PERCENT / 10;
    cgutil_vaddr[TOPCG_BACKEND]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, TOP_BACKEND_PERCENT);
    cgutil_vaddr[TOPCG_BACKEND]->percent = cgutil_vaddr[TOPCG_GAUSSDB]->percent * TOP_BACKEND_PERCENT / 100;

    /* set root group as gaussdb group cpu set */
    if (*cgutil_vaddr[TOPCG_BACKEND]->cpuset == '\0')
        (void)sprintf_s(cgutil_vaddr[TOPCG_BACKEND]->cpuset, CPUSET_LEN, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);

    cgutil_vaddr[TOPCG_CLASS]->used = 1;
    cgutil_vaddr[TOPCG_CLASS]->gid = TOPCG_CLASS;
    cgutil_vaddr[TOPCG_CLASS]->gtype = GROUP_TOP;
    sret = strncpy_s(cgutil_vaddr[TOPCG_CLASS]->grpname, GPNAME_LEN, GSCGROUP_TOP_CLASS, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent = TOP_CLASS_PERCENT;
    cgutil_vaddr[TOPCG_CLASS]->ainfo.shares = DEFAULT_CPU_SHARES * TOP_CLASS_PERCENT / 10;
    cgutil_vaddr[TOPCG_CLASS]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, TOP_CLASS_PERCENT);
    cgutil_vaddr[TOPCG_CLASS]->percent = cgutil_vaddr[TOPCG_GAUSSDB]->percent * TOP_CLASS_PERCENT / 100;
    /* set root group as gaussdb group cpu set */
    if (*cgutil_vaddr[TOPCG_CLASS]->cpuset == '\0')
        (void)sprintf_s(cgutil_vaddr[TOPCG_CLASS]->cpuset, CPUSET_LEN, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);

    cgutil_vaddr[BACKENDCG_START_ID]->used = 1;
    cgutil_vaddr[BACKENDCG_START_ID]->gid = BACKENDCG_START_ID;
    cgutil_vaddr[BACKENDCG_START_ID]->gtype = GROUP_BAKWD;
    cgutil_vaddr[BACKENDCG_START_ID]->ginfo.cls.tgid = TOPCG_BACKEND;
    cgutil_vaddr[BACKENDCG_START_ID]->ginfo.cls.percent = DEFAULT_BACKEND_PERCENT;
    sret = strncpy_s(cgutil_vaddr[BACKENDCG_START_ID]->grpname, GPNAME_LEN, GSCGROUP_DEFAULT_BACKEND, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[BACKENDCG_START_ID]->ainfo.shares = DEFAULT_CPU_SHARES * DEFAULT_BACKEND_PERCENT / 10;
    cgutil_vaddr[BACKENDCG_START_ID]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, DEFAULT_BACKEND_PERCENT);
    cgutil_vaddr[BACKENDCG_START_ID]->percent = cgutil_vaddr[TOPCG_BACKEND]->percent * DEFAULT_BACKEND_PERCENT / 100;

    /* set root group as backend group cpu set */
    if (*cgutil_vaddr[BACKENDCG_START_ID]->cpuset == '\0')
        (void)sprintf_s(
            cgutil_vaddr[BACKENDCG_START_ID]->cpuset, CPUSET_LEN, "%s", cgutil_vaddr[TOPCG_BACKEND]->cpuset);

    cgutil_vaddr[BACKENDCG_START_ID + 1]->used = 1;
    cgutil_vaddr[BACKENDCG_START_ID + 1]->gid = BACKENDCG_START_ID + 1;
    cgutil_vaddr[BACKENDCG_START_ID + 1]->gtype = GROUP_BAKWD;
    cgutil_vaddr[BACKENDCG_START_ID + 1]->ginfo.cls.tgid = TOPCG_BACKEND;
    cgutil_vaddr[BACKENDCG_START_ID + 1]->ginfo.cls.percent = VACUUM_PERCENT;
    sret = strncpy_s(cgutil_vaddr[BACKENDCG_START_ID + 1]->grpname, GPNAME_LEN, GSCGROUP_VACUUM, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[BACKENDCG_START_ID + 1]->ainfo.shares = DEFAULT_CPU_SHARES * VACUUM_PERCENT / 10;
    cgutil_vaddr[BACKENDCG_START_ID + 1]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, VACUUM_PERCENT);
    cgutil_vaddr[BACKENDCG_START_ID + 1]->percent = cgutil_vaddr[TOPCG_BACKEND]->percent * VACUUM_PERCENT / 100;
    /* set root group as backend group cpu set */
    if (*cgutil_vaddr[BACKENDCG_START_ID + 1]->cpuset == '\0')
        (void)sprintf_s(
            cgutil_vaddr[BACKENDCG_START_ID + 1]->cpuset, CPUSET_LEN, "%s", cgutil_vaddr[TOPCG_BACKEND]->cpuset);

    cgutil_vaddr[CLASSCG_START_ID]->used = 1;
    cgutil_vaddr[CLASSCG_START_ID]->gid = CLASSCG_START_ID;
    cgutil_vaddr[CLASSCG_START_ID]->gtype = GROUP_CLASS;
    cgutil_vaddr[CLASSCG_START_ID]->ginfo.cls.tgid = TOPCG_CLASS;
    cgutil_vaddr[CLASSCG_START_ID]->ginfo.cls.maxlevel = 1; /* initialized value */
    cgutil_vaddr[CLASSCG_START_ID]->ginfo.cls.percent = DEFAULT_CLASS_PERCENT;
    cgutil_vaddr[CLASSCG_START_ID]->ginfo.cls.rempct = 100; /* initialized value */
    sret = strncpy_s(cgutil_vaddr[CLASSCG_START_ID]->grpname, GPNAME_LEN, GSCGROUP_DEFAULT_CLASS, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[CLASSCG_START_ID]->ainfo.shares = DEFAULT_CPU_SHARES * DEFAULT_CLASS_PERCENT / 10;
    cgutil_vaddr[CLASSCG_START_ID]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, DEFAULT_CLASS_PERCENT);
    /* it has only this class, so it has all resource */
    cgutil_vaddr[CLASSCG_START_ID]->percent = cgutil_vaddr[TOPCG_CLASS]->percent;

    cgutil_vaddr[CLASSCG_START_ID]->except[EXCEPT_PENALTY].skewpercent = DEFAULT_CPUSKEWPCT;
    cgutil_vaddr[CLASSCG_START_ID]->except[EXCEPT_PENALTY].qualitime = DEFAULT_QUALITIME;

    cgutil_vaddr[WDCG_START_ID]->used = 1;
    cgutil_vaddr[WDCG_START_ID]->gid = WDCG_START_ID;
    cgutil_vaddr[WDCG_START_ID]->gtype = GROUP_DEFWD;
    cgutil_vaddr[WDCG_START_ID]->ginfo.wd.cgid = CLASSCG_START_ID;
    cgutil_vaddr[WDCG_START_ID]->ginfo.wd.wdlevel = 1;
    sret = snprintf_s(tmpstr, sizeof(tmpstr), sizeof(tmpstr) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(sret, , );

    sret = strncpy_s(cgutil_vaddr[WDCG_START_ID]->grpname, GPNAME_LEN, tmpstr, GPNAME_LEN - 1);
    securec_check_errno(sret, , );

    cgutil_vaddr[WDCG_START_ID]->ainfo.shares = MAX_CLASS_CPUSHARES * TOPWD_PERCENT / 100;
    cgutil_vaddr[WDCG_START_ID]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, TOPWD_PERCENT);

    cgutil_vaddr[TSCG_START_ID]->used = 1;
    cgutil_vaddr[TSCG_START_ID]->gid = TSCG_START_ID;
    cgutil_vaddr[TSCG_START_ID]->gtype = GROUP_TSWD;
    cgutil_vaddr[TSCG_START_ID]->ginfo.ts.cgid = CLASSCG_START_ID;
    cgutil_vaddr[TSCG_START_ID]->ginfo.ts.rate = TS_LOW_RATE;
    sret = strncpy_s(cgutil_vaddr[TSCG_START_ID]->grpname, GPNAME_LEN, GSCGROUP_LOW_TIMESHARE, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TSCG_START_ID]->ainfo.shares = DEFAULT_CPU_SHARES * TS_LOW_RATE;
    cgutil_vaddr[TSCG_START_ID]->ainfo.weight = MIN_IO_WEIGHT * TS_LOW_RATE;

    /* medium group of default group */
    cgutil_vaddr[TSCG_START_ID + 1]->used = 1;
    cgutil_vaddr[TSCG_START_ID + 1]->gid = TSCG_START_ID + 1;
    cgutil_vaddr[TSCG_START_ID + 1]->gtype = GROUP_TSWD;
    cgutil_vaddr[TSCG_START_ID + 1]->ginfo.ts.cgid = CLASSCG_START_ID;
    cgutil_vaddr[TSCG_START_ID + 1]->ginfo.ts.rate = TS_MEDIUM_RATE;
    sret = strncpy_s(cgutil_vaddr[TSCG_START_ID + 1]->grpname, GPNAME_LEN, GSCGROUP_MEDIUM_TIMESHARE, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TSCG_START_ID + 1]->ainfo.shares = DEFAULT_CPU_SHARES * TS_MEDIUM_RATE;
    cgutil_vaddr[TSCG_START_ID + 1]->ainfo.weight = MIN_IO_WEIGHT * TS_MEDIUM_RATE;

    /* high group of default group */
    cgutil_vaddr[TSCG_START_ID + 2]->used = 1;
    cgutil_vaddr[TSCG_START_ID + 2]->gid = TSCG_START_ID + 2;
    cgutil_vaddr[TSCG_START_ID + 2]->gtype = GROUP_TSWD;
    cgutil_vaddr[TSCG_START_ID + 2]->ginfo.ts.cgid = CLASSCG_START_ID;
    cgutil_vaddr[TSCG_START_ID + 2]->ginfo.ts.rate = TS_HIGH_RATE;
    sret = strncpy_s(cgutil_vaddr[TSCG_START_ID + 2]->grpname, GPNAME_LEN, GSCGROUP_HIGH_TIMESHARE, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TSCG_START_ID + 2]->ainfo.shares = DEFAULT_CPU_SHARES * TS_HIGH_RATE;
    cgutil_vaddr[TSCG_START_ID + 2]->ainfo.weight = MIN_IO_WEIGHT * TS_HIGH_RATE;

    /* rush group of default group */
    cgutil_vaddr[TSCG_START_ID + 3]->used = 1;
    cgutil_vaddr[TSCG_START_ID + 3]->gid = TSCG_START_ID + 3;
    cgutil_vaddr[TSCG_START_ID + 3]->gtype = GROUP_TSWD;
    cgutil_vaddr[TSCG_START_ID + 3]->ginfo.ts.cgid = CLASSCG_START_ID;
    cgutil_vaddr[TSCG_START_ID + 3]->ginfo.ts.rate = TS_RUSH_RATE;
    sret = strncpy_s(cgutil_vaddr[TSCG_START_ID + 3]->grpname, GPNAME_LEN, GSCGROUP_RUSH_TIMESHARE, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TSCG_START_ID + 3]->ainfo.shares = DEFAULT_CPU_SHARES * TS_RUSH_RATE;
    cgutil_vaddr[TSCG_START_ID + 3]->ainfo.weight = MIN_IO_WEIGHT * TS_RUSH_RATE;
}

extern void cgconf_generate_default_config_file(void* vaddr);
extern int cgexec_update_remain_cgroup_cpuset(int cls, char* cpuset, unsigned char update);
extern int cgexec_check_cpuset_value(const char* clsset, const char* grpset);
extern int cgexec_update_class_cpuset(int cls, char* cpuset);
extern int cgexec_update_top_group_cpuset(int top, char* cpuset);
extern void cgexec_update_fixed_config(int high, int extended);

void cgroup_unit_test_case()
{
    char* argv[] = {"gs_cgroup", "-D", "/dev/cgroups/test", "--upgrade"};
    int argc = sizeof(argv) / sizeof(*argv);
    int sret = 0;

    cgutil_opt.mpflag = 1;
    (void)cgexec_mount_root_cgroup();
    (void)cgexec_umount_root_cgroup();

    (void)parse_options(argc, argv);

    for (int i = 0; i < GSCGROUP_ALLNUM; ++i) {
        if (NULL == (cgutil_vaddr[i] = (gscgroup_grp_t*)malloc(sizeof(gscgroup_grp_t)))) {
            fprintf(stderr, "ERROR: failed to allocate memory for gsgroup!\n");
            for (int index = 0; index < i; ++index) {
                free(cgutil_vaddr[index]);
                cgutil_vaddr[index] = NULL;
            }
            return;
        }
    }

    cgroup_set_default_group();

    for (int i = 0; i < GSCGROUP_ALLNUM; ++i) {
        free(cgutil_vaddr[i]);
        cgutil_vaddr[i] = NULL;
    }
    gscgroup_grp_t vaddr[GSCGROUP_ALLNUM];

    cgconf_generate_default_config_file(vaddr);

    sret = memset_s(&cgutil_opt, sizeof(cgutil_opt), 0, sizeof(cgutil_opt));
    securec_check_c(sret, "\0", "\0");

    sret = snprintf_s(cgutil_opt.sets, sizeof(cgutil_opt.sets), sizeof(cgutil_opt.sets) - 1, "%s", "2-8");
    securec_check_ss_c(sret, "\0", "\0");

    cgconf_set_class_group(1);

    sret = snprintf_s(cgutil_opt.wdname, sizeof(cgutil_opt.wdname), sizeof(cgutil_opt.wdname) - 1, "%s", "wg1");
    securec_check_ss_c(sret, "\0", "\0");

    cgconf_set_class_group(1);

    cgconf_set_workload_group(1, 2);

    cgutil_opt.fixed = 1;
    cgutil_is_sles11_sp2 = 0;
    check_percentage_value(1, 1, 1, 1);

    cgutil_opt.display = 1;
    cgutil_opt.user[0] = '\0';
    check_user_process();

    cgutil_opt.display = 0;
    cgutil_opt.ptree = 1;
    check_user_process();

    cgutil_opt.ptree = 0;
    cgutil_opt.cflag = 1;
    cgutil_opt.fixed = 0;

    sret = snprintf_s(cgutil_opt.topname, sizeof(cgutil_opt.topname), sizeof(cgutil_opt.topname) - 1, "%s", "Gaussdb");
    securec_check_ss_c(sret, "\0", "\0");

    check_flag_process();

    cgutil_opt.topname[0] = '\0';
    sret = snprintf_s(cgutil_opt.wdname, sizeof(cgutil_opt.wdname), sizeof(cgutil_opt.wdname) - 1, "%s", "wg1");
    securec_check_ss_c(sret, "\0", "\0");

    check_flag_process();

    sret = snprintf_s(cgutil_opt.wdname, sizeof(cgutil_opt.wdname), sizeof(cgutil_opt.wdname) - 1, "%s", "class1:wg1");
    securec_check_ss_c(sret, "\0", "\0");

    check_flag_process();

    cgutil_opt.cflag = 0;
    cgutil_opt.dflag = 1;
    sret = snprintf_s(cgutil_opt.topname, sizeof(cgutil_opt.topname), sizeof(cgutil_opt.topname) - 1, "%s", "Gaussdb");
    securec_check_ss_c(sret, "\0", "\0");
    check_flag_process();

    cgutil_opt.topname[0] = '\0';
    sret = snprintf_s(cgutil_opt.user, sizeof(cgutil_opt.user), sizeof(cgutil_opt.user) - 1, "%s", "root");
    securec_check_ss_c(sret, "\0", "\0");
    check_user_name();

    sret = snprintf_s(cgutil_opt.user, sizeof(cgutil_opt.user), sizeof(cgutil_opt.user) - 1, "%s", "xxx");
    securec_check_ss_c(sret, "\0", "\0");
    check_user_name();

    cgutil_opt.user[0] = '\0';
    check_and_get_group_percent(NULL, "abc");

    cgexec_check_cpuset_value("1-2", "3-4");

    cgexec_update_remain_cgroup_cpuset(1, "3-4", 1);
    cgexec_update_remain_cgroup_cpuset(1, "3-4", 0);
    cgexec_update_class_cpuset(1, "3-4");

    cgexec_update_top_group_cpuset(TOPCG_ROOT, "3-4");
    cgexec_update_top_group_cpuset(TOPCG_GAUSSDB, "3-4");
    cgexec_update_top_group_cpuset(TOPCG_BACKEND, "3-4");
    cgexec_update_top_group_cpuset(TOPCG_CLASS, "3-4");
    cgexec_update_top_group_cpuset(-1, "3-4");

    cgutil_opt.mpflag = 1;
    cgexec_check_mount_for_upgrade();

    cgutil_opt.mpflag = 0;

    cgutil_opt.cflag = 0;

    cgutil_opt.cflag = 1;
    sret = snprintf_s(
        cgutil_opt.mpoints[0], sizeof(cgutil_opt.mpoints[0]), sizeof(cgutil_opt.mpoints[0]) - 1, "%s", "/dev/abc");
    securec_check_ss_c(sret, "\0", "\0");

    cgexec_umount_root_cgroup();

    sret = memset_s(cgutil_vaddr[CLASSCG_START_ID]->except,
        EXCEPT_ALL_KINDS * sizeof(except_data_t),
        0,
        EXCEPT_ALL_KINDS * sizeof(except_data_t));
    securec_check_c(sret, "\0", "\0");

    cgexec_create_groups();

    sret = snprintf_s(cgutil_opt.clsname, sizeof(cgutil_opt.clsname), sizeof(cgutil_opt.clsname) - 1, "%s", "class2");
    securec_check_ss_c(sret, "\0", "\0");
    cgconf_set_class_group(31);

    sret = snprintf_s(cgutil_opt.clsname, sizeof(cgutil_opt.clsname), sizeof(cgutil_opt.clsname) - 1, "%s", "class3");
    securec_check_ss_c(sret, "\0", "\0");
    cgconf_set_class_group(32);

    sret = snprintf_s(cgutil_opt.clsname, sizeof(cgutil_opt.clsname), sizeof(cgutil_opt.clsname) - 1, "%s", "class4");
    securec_check_ss_c(sret, "\0", "\0");
    cgconf_set_class_group(33);

    sret = snprintf_s(cgutil_vaddr[TOPCG_CLASS]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", "0-47");
    securec_check_ss_c(sret, "\0", "\0");

    sret = snprintf_s(cgutil_vaddr[31]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", "0-46");
    securec_check_ss_c(sret, "\0", "\0");
    sret = snprintf_s(cgutil_vaddr[32]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", "0-30");
    securec_check_ss_c(sret, "\0", "\0");
    sret = snprintf_s(cgutil_vaddr[33]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", "0-0");
    securec_check_ss_c(sret, "\0", "\0");

    cgexec_update_fixed_config(TOPCG_CLASS, 0);

    cgconf_reset_class_group(31);
    cgconf_reset_class_group(32);
    cgconf_reset_class_group(33);
}
#endif
