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
 * cgexec.cpp
 *    Cgroup configration file process functions
 *
 * IDENTIFICATION
 *    src/bin/gs_cgroup/cgconf.cpp
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <alloca.h>
#include <sys/mount.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <mntent.h>
#include <libcgroup.h>
#include <linux/version.h>
#include <sys/utsname.h>
#include <sys/mman.h>

#include "workload/gscgroup.h"

#include "cgutil.h"
#include "securec.h"
#include "bin/elog.h"

#ifdef ENABLE_UT
#define static
#endif

#define MAX_COMMAND_LENGTH 128
#define MOUNT_POINT_LENGTH (MAXPGPATH + 16)

int cgutil_is_sles11_sp2 = 0; /* to indicate if the current OS is SLES SP2 version */

char* cgutil_subsys_table[] = {
    MOUNT_CPU_NAME, MOUNT_CPUACCT_NAME, MOUNT_BLKIO_NAME, MOUNT_CPUSET_NAME, MOUNT_MEMORY_NAME};

static gscgroup_grp_t* cgutil_vaddr_back[GSCGROUP_ALLNUM] = {NULL}; /* for recovering */

/*
 *****************  STATIC FUNCTIONS ************************
 */
/*
 * static functions for updating cpuset of different level of groups,
 * declare here for use of functions that reset cpu cores of different level of groups.
 */
/*the core function of updating cpu cores. */
static int cgexec_update_cgroup_cpuset_value(char* relpath, char* cpuset);
/* update class cpu cores and the belonging workload groups. */
static int cgexec_update_class_cpuset(int cls, char* cpuset);
/* update top groups cpu cores and their all the belonging groups*/
static int cgexec_update_top_group_cpuset(int top, char* cpuset);
/* update one group cpu cores */
static int cgexec_update_cgroup_cpuset(gscgroup_grp_t* grp, char* cpuset);

int CheckBackendEnv(const char* input_env_value)
{
    const int max_env_len = 1024;
    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    int i = 0;

    if (input_env_value == nullptr || strlen(input_env_value) >= max_env_len) {
        fprintf(stderr, "ERROR: wrong environment variable \"%s\"\n", input_env_value);
        return -1;
    }
    
    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i])) {
            fprintf(stderr, "ERROR: environment variable \"%s\" contain invaild symbol \"%s\".\n",
                input_env_value, danger_character_list[i]);
            return -1;
        }
    }
    return 0;
}

inline int CheckSystemSucess(pid_t status)
{
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return 0;
    } else {
        fprintf(stderr, "command execute failed for: %d!\n", WEXITSTATUS(status));
        return -1;
    }
}

/*
 * function name: cgexec_get_cgroup_number
 * description  : get the Cgroup numbers
 * return value :
 *            -1: abnormal
 *         other: normal
 */
static int cgexec_get_cgroup_number(void)
{
    char buf[PROCLINE_LEN];
    FILE* f = NULL;
    char *p = NULL, *q = NULL;
    int hierarchy;
    int cgcnt = -1;

    f = fopen("/proc/cgroups", "r");
    if (f == NULL)
        return -1;

    while (NULL != fgets(buf, PROCLINE_LEN, f)) {
        /* example from proc:
         * #subsys_name hierarchy   num_cgroups enabled
         * cpu  0   1   1
         *
         * get the first column, such as cpu
         */
        p = buf;
        q = strchr(p, '\t');
        if (q == NULL)
            continue;

        *q = '\0';
        if (0 == strcmp(MOUNT_CPU_NAME, p)) {
            while (*(q++) == ' ')
                continue;

            /* get the second column */
            p = strchr(q, '\t');
            if (p == NULL)
                break;

            *p = '\0';

            hierarchy = (int)strtol(q, NULL, 10);
            if (hierarchy == 0) {
                fprintf(stderr, "cgroup is not mounted!\n");
                break;
            }

            while (*(p++) == ' ')
                continue;

            /* get the third column */
            q = strchr(p, '\t');
            if (q == NULL) {
                fclose(f);
                return -1;
            }
            *q = '\0';

            cgcnt = (int)strtol(p, NULL, 10);
        }
    }

    fclose(f);
    return cgcnt;
}

/*
 * @Description: check cpuset value.
 * @IN clsset: class cpuset
 * @IN grpset: group cpuset
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
int cgexec_check_cpuset_value(const char* clsset, const char* grpset)
{
    int clsstart, clsend;
    int grpstart, grpend;

    errno_t ret = sscanf_s(clsset, "%d-%d", &clsstart, &clsend);
    if (ret != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return -1;
    }
    ret = sscanf_s(grpset, "%d-%d", &grpstart, &grpend);
    if (ret != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return -1;
    }

    /* group cpuset value must be in class cpuset range */
    if (grpstart >= clsstart && grpend <= clsend)
        return 0;

    return -1;
}

/*
 * @Description: get cpuset length.
 * @IN cpuset: cpuset to be parsed
 * @OUT start: start value of the cpuset
 * @OUT end: end value of the cpuset
 * @Return: length of the cpuset
 * @See also:
 */
static int cgexec_get_cpuset_length(const char* cpuset, int* start, int* end)
{
    errno_t ret = sscanf_s(cpuset, "%d-%d", start, end);
    if (ret != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return -1;
    }

    return *end - *start + 1;
}

/*
 * @Description: copy the start value and the end value into cpuset.
 * @OUT cpuset: cpuset set well
 * @IN start: start value of the cpuset
 * @IN end: end value of the cpuset
 * @See also:
 */
static void cgexec_get_cpu_core_range(char* cpuset, int start, int end)
{
    errno_t ret = sprintf_s(cpuset, CPUSET_LEN, "%d-%d", start, end);
    securec_check_intval(ret, , );
}
/*
 * @Description     : transfer from percentage to length of cpuset.
 * @IN whole        : the length of cpuset of the upper level group
 * @IN wdpct        : the percentage value of user set("--fixed").
 * @Return          : -1: abnormal
 * @Return          : cpusetlength: the length of the cpuset to be updated.
 * @See also:
 */
static int cgexec_trans_percent_to_cpusets(int whole, int wdpct)
{
    int cpusetlength = 0;
    char tempvalue[CPUSET_LEN] = {0};
    char* temp = NULL;
    errno_t ret;

    ret = sprintf_s(tempvalue, CPUSET_LEN, "%.1f", (float)whole * wdpct / GROUP_ALL_PERCENT);
    securec_check_intval(ret, , -1);

    temp = strchr(tempvalue, '.');
    cpusetlength = atoi(tempvalue);

    if ((temp != NULL) && (*(++temp) > '5' || cpusetlength == 0)) {
        cpusetlength++;
    }

    return cpusetlength;
}

/*
 * @Description     : transfer from length of cpuset to percentage.
 * @IN highlen      : the length of cpuset of the upper level group
 * @IN lowlen       : the percentage value of user set("--fixed").
 * @Return          : cpusetlength: the length of the cpuset to be updated.
 * @See also:
 */
static int cgexec_trans_cpusets_to_percent(int highlen, int lowlen)
{
    int pct = 0;

    if (highlen == 0) {
        fprintf(stderr, "ERROR: %s:%d, Division by zero!\n", __FILE__, __LINE__);
        return -1;
    }
    pct = lowlen * GROUP_ALL_PERCENT / highlen;

    /*
     * if the number of cores of the system is lower than 100,
     * we prefer the smaller percentage, to transfer the cpuset
     * to quota as much as possible.
     *
     * make sure that the length got from the percentage
     * will be the same with the low length.
     */
    while (cgexec_trans_percent_to_cpusets(highlen, pct) < lowlen || !pct)
        pct++;

    return pct;
}

/*
 * @Description     : get cgroup id range
 * @IN high         : the id of the group id
 * @OUT forstart    : start group id of the range
 * @OUT forend      : end group id of the range
 * @Return          : -1: abnormal
 *                     0: the cpuset has been set well
 * @See also        :
 */
int cgexec_get_cgroup_id_range(int high, int* forstart, int* forend)
{
    if (high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) {
        *forstart = WDCG_START_ID;
        *forend = WDCG_END_ID;
    } else if (high == TOPCG_CLASS) {
        *forstart = CLASSCG_START_ID;
        *forend = CLASSCG_END_ID;
    } else if (high == TOPCG_BACKEND) {
        *forstart = BACKENDCG_START_ID;
        *forend = BACKENDCG_END_ID;
    } else if (high == TOPCG_GAUSSDB) {
        *forstart = TOPCG_BACKEND;
        *forend = TOPCG_CLASS;
    } else
        return -1;

    return 0;
}
/*
 * @Description     : check whether the total percentage of the low groups
 *                    are beyond the upper limit
 * @IN high         : the id of the high group that the low group belongs to.
 * @IN low          : the id of the low group to be updated.
 * @OUT cpuset      : if succeed, the calculated cpuset will be stored in it.
 * @Return          : -1: abnormal
 *                     0: the cpuset has been set well
 *                     1: need reset.
 * @See also        :
 */
static int cgexec_check_cpuset_percent(int high, int low, char* cpuset)
{
    /* start and end value of the loop */
    int forstart = 0, forend = 0;
    /* start value and end values of the low and the high levels cpuset */
    int i, highstart = 0, highend = 0, lowstart = 0, lowend = 0;
    /* sum of cpu cores and quota discarding the low groups which is to be updated */
    int sum_cpusets = 0, sum_quota = 0;
    /* cpuset length of the groups and max value of the current low level groups */
    int lowlen = 0, highlen = 0, lowmax = 0;
    /* return values which are to be restored in  cpuset */
    int ret_start, ret_end;

    if (cgexec_get_cgroup_id_range(high, &forstart, &forend) == -1)
        return -1;

    /* the cpuset length of the high level group */
    highlen = cgexec_get_cpuset_length(cgutil_vaddr[high]->cpuset, &highstart, &highend);

    /* the cpuset length to be updated. */
    lowlen = cgexec_trans_percent_to_cpusets(highlen, cgutil_opt.setspct);

    for (i = forstart; i <= forend; i++) {
        /* the low level group is ignored currently */
        if (cgutil_vaddr[i]->used == 0 || i == low)
            continue;

        /* only the workload groups with the same class "high" are considered. */
        if ((high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) && high != cgutil_vaddr[i]->ginfo.wd.cgid)
            continue;

        /*
         * in order to check whether the newly set setspct makes
         * the total cpu cores out of range, count the sum of
         * cpu cores, max core value, and the sum of quota value
         * of the groups, ignoring the low group.
         *
         * quota is the percentage of cpu cores,
         * if quota is 0, then the cpu cores would be set by default.
         */
        if (cgutil_vaddr[i]->ainfo.quota) {
            sum_cpusets += cgexec_get_cpuset_length(cgutil_vaddr[i]->cpuset, &lowstart, &lowend);
            sum_quota += cgutil_vaddr[i]->ainfo.quota;
            lowmax = (lowend > lowmax) ? lowend : lowmax;
        }
    }
    /*
     * if sum of quota values and the newly set setspct out of range,
     * an error is thrown out. However, there are some cases, that the
     * quota is not out of range, but sum of cpu cores are, since the
     * calculated decimals (such as 1.6 is rounded to 2, and 0.1 is
     * rounded to 1, 1.5 is rounded to 1) are rounded up or down.
     * these cases will be handled in macro GET_CPUSET_START_VALUE.
     * For example, if there are 2 cores left for the newly set group,
     * but it need 3 cores after calculation from setspct,
     * then it will be set the last three cores.
     */
    if (sum_quota + cgutil_opt.setspct > GROUP_ALL_PERCENT) {
        if (*cgutil_vaddr[low]->grpname)
            fprintf(stderr,
                "ERROR: the total percentage of cpu cores are larger than 100, "
                "you cannot set %d%% for group \"%s\"\n",
                cgutil_opt.setspct,
                cgutil_vaddr[low]->grpname);

        return -1;
    }

    ret_start = GET_CPUSET_START_VALUE(highstart, highend, sum_cpusets, lowmax, lowlen);
    ret_end = ret_start + lowlen - 1;
    cgexec_get_cpu_core_range(cpuset, ret_start, ret_end);

    /* return value indicates need reset or not */
    return (ret_start > lowmax) ? 0 : 1;
}
/*
 * @Description     : reset group cpuset values.
 * @IN high         : high level group id that low group belongs to
 * @IN low          : low level group to be updated, which is not included in the reseting list.
 * @Return -1       : abnormal
 * @Return 0        : normal.
 * @See also:
 */
static int cgexec_reset_cpuset_cgroups(int high, int low)
{
    int forstart = 0, forend = 0; /* start and end value of the loop */
    int i = 0;
    int lowlen = 0, highlen = 0;    /* low and high level cpuset length */
    int lowstart = 0, lowend = 0;   /* low group cpuset start and end value */
    int highstart = 0, highend = 0; /* high group cpuset start and end value */
    char sets[CPUSET_LEN];          /* the calculated cpuset to be updated */
    bool flag = false;              /*flag to indicate first time enter the loop */

    if (cgexec_get_cgroup_id_range(high, &forstart, &forend) == -1)
        return -1;

    /* the cpuset length of the high level group */
    highlen = cgexec_get_cpuset_length(cgutil_vaddr[high]->cpuset, &highstart, &highend);

    for (i = forstart; i <= forend; i++) {
        /* the low level group is ignored in the reseting list */
        if (cgutil_vaddr[i]->used == 0 || (low != 0 && i == low))
            continue;

        /* only the workload groups belonging to high class is considered */
        if ((high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) && high != cgutil_vaddr[i]->ginfo.wd.cgid)
            continue;

        /* only groups with quota values are considered */
        if (cgutil_vaddr[i]->ainfo.quota) {
            /* the low level groups (same level groups with "low") cpu core length */
            lowlen = cgexec_trans_percent_to_cpusets(highlen, cgutil_vaddr[i]->ainfo.quota);

            /*
             * only the first time enter the loop, flag is false
             * the cpu cores are allocated sequentially from high group cpu core range.
             * the first group to be reset is allocated from "highstart",
             * the next are allocated following the previous group "lowend" + 1
             */
            lowstart = flag ? (lowend + 1) : highstart;
            lowend = lowstart + lowlen - 1;

            /*
             * callers of this function can guarantee the total quota not out of range,
             * so here we only need check whether the left cpu cores are enough or not,
             * and and the not enough cases will be handled in the same way with
             * cgexec_check_cpuset_percent.
             */
            if (lowend > highend) {
                lowstart = highend - lowlen + 1;
                lowend = highend;
            }
            /* "sets" restore the cpuset to be reset*/
            cgexec_get_cpu_core_range(sets, lowstart, lowend);

            /* reset the group cpuset with "sets" */
            if ((high == TOPCG_CLASS && cgexec_update_class_cpuset(i, sets) == -1) ||
                (high == TOPCG_GAUSSDB && cgexec_update_top_group_cpuset(i, sets) == -1) ||
                (((high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) || high == TOPCG_BACKEND) &&
                    (cgexec_update_cgroup_cpuset(cgutil_vaddr[i], sets) == -1))) {
                fprintf(stderr, "ERROR: reset cpu cores for \"%s\" failed\n", cgutil_vaddr[i]->grpname);
                return -1;
            }

            /* next time enter the loop, flag will be true*/
            if (!flag)
                flag = true;
        }
        /*
         * for the case of reseting backend groups, or workload groups, since they don't have
         * low level groups, no reset is needed.
         * in other cases, reseting the low level groups recursively is needed.
         */
        if ((high == TOPCG_CLASS || high == TOPCG_GAUSSDB) && cgexec_reset_cpuset_cgroups(i, 0) == -1) {
            fprintf(
                stderr, "ERROR: reset group failed when reseting cpuset for group \"%s\".\n", cgutil_vaddr[i]->grpname);
            return -1;
        }
    }
    return 0;
}

/*
 * @Description     : get the total cpu core percentage of the groups,
 *                    with "high" as their higher level group id
 * @IN high         : high level group id.
 * @Return          : total quota value
 * @See also:
 */
static int cgexec_check_fixed_percent(int high)
{
    int forstart = 0, forend = 0; /* start and end value of the loop */
    int sets_total_pct = 0;       /* total percentage of the low groups */
    int i = 0;

    (void)cgexec_get_cgroup_id_range(high, &forstart, &forend);

    for (i = forstart; i <= forend; i++) {
        if (cgutil_vaddr[i]->used == 0 || !cgutil_vaddr[i]->ainfo.quota)
            continue;

        /* only the workload groups belonging to high class is considered */
        if ((high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) && high != cgutil_vaddr[i]->ginfo.wd.cgid)
            continue;

        sets_total_pct += cgutil_vaddr[i]->ainfo.quota;
    }

    return sets_total_pct;
}

/*
 * @Description: get large cpuset value.
 * @IN clsset: class cpuset
 * @IN grpset: group cpuset
 * @OUT result: large cpuset
 * @Return: large cpuset value
 * @See also:
 */
char* cgexec_get_large_cupset(const char* clsset, const char* grpset, char* result)
{
    int clsstart, clsend;
    int grpstart, grpend;
    int resstart, resend;

    int rc = sscanf_s(clsset, "%d-%d", &clsstart, &clsend);
    if (rc != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return NULL;
    }

    rc = sscanf_s(grpset, "%d-%d", &grpstart, &grpend);
    if (rc != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return NULL;
    }

    /* get large start value */
    resstart = (clsstart < grpstart) ? clsstart : grpstart;
    /* get large end value */
    resend = (clsend > grpend) ? clsend : grpend;

    /* get large cpuset */
    rc = sprintf_s(result, CPUSET_LEN, "%d-%d", resstart, resend);
    /* check the return value of security function */
    securec_check_ss_c(rc, "\0", "\0");

    return result;
}

/*
 * @Description: get cgroup info with relpath.
 * @IN relpath: relpath of the cgroup
 * @Return: cgroup info
 * @See also:
 */
struct cgroup* cgexec_get_cgroup(const char* relpath)
{
    struct cgroup* cg = NULL;
    int ret;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", relpath);
        return NULL;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get cgroup information for %s(%d)\n", cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return NULL;
    }

    return cg;
}

/*
 * function name: cgexec_update_remain_value
 * description  : update the dynamic value of Remain Cgroup
 * arguments    :
 *       relpath: the relative path of Remain Cgroup
 *     cpushares: the value of cpu.shares
 *      ioweight: the value of blkio.weight
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function only updates the cpu.shares and blkio.weight.
 */
int cgexec_update_remain_value(char* relpath, u_int64_t cpushares, u_int64_t ioweight)
{
    struct cgroup* cg = NULL;
    struct cgroup_controller* cgc_cpu = NULL;
    int ret;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        ret = ECGFAIL;
        fprintf(stdout, "ERROR: failed to create the new %s cgroup for %s\n", relpath, cgroup_strerror(ret));
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get %s cgroup information for %s(%d)\n", relpath, cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return -1;
    }

    /* get the cpu controller */
    cgc_cpu = cgroup_get_controller(cg, MOUNT_CPU_NAME);
    if (NULL == cgc_cpu) {
        fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPU_NAME, relpath);
        cgroup_free(&cg);
        return -1;
    }

    if (cpushares && (0 != (ret = cgroup_set_value_uint64(cgc_cpu, CPU_SHARES, cpushares)))) {
        fprintf(stderr, "ERROR: failed to set %s as %lu for %s\n", CPU_SHARES, cpushares, cgroup_strerror(ret));
        cgroup_free_controllers(cg);
        cgroup_free(&cg);
        return -1;
    }

    /* update controller into kernel */
    if (0 != (ret = cgroup_modify_cgroup(cg))) {
        fprintf(stderr,
            "ERROR: failed to modify cgroup for %s "
            "when modifying values!\n",
            cgroup_strerror(ret));
        cgroup_free_controllers(cg);
        cgroup_free(&cg);
        return -1;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    return 0;
}

/*
 * function name: cgexec_update_remain_cgroup
 * description  : get the value of Remain Cgroup and
 *                update them into kernel cgroup
 * arguments    :
 *           grp: the configuration information of workload group
 *                which has the same level as Remain Cgroup
 *           cls: the group ID of Class which has the workload group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when updating value of workload group.
 */
static int cgexec_update_remain_cgroup(gscgroup_grp_t* grp, int cls)
{
    char* relpath = NULL;
    int i, j, ret, rempct = GROUP_ALL_PERCENT;
    char rempath[16];
    u_int64_t cpushares, ioweight;
    errno_t sret;

    /*
     * calculate the remain percent of group
     * whose level is larger than specified workload group
     * count from 2 is for discarding the TopWD group
     */
    for (i = 2; i < grp->ginfo.wd.wdlevel; i++) {
        /* calculate the remain percentage */
        for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
            if (cgutil_vaddr[j]->used && cgutil_vaddr[j]->ginfo.wd.cgid == cls &&
                cgutil_vaddr[j]->ginfo.wd.wdlevel == i)
                break;
        }

        rempct -= cgutil_vaddr[j]->ginfo.wd.percent;
    }

    /* update the workload whose level is larger than specified workload */
    for (i = grp->ginfo.wd.wdlevel; i <= cgutil_vaddr[cls]->ginfo.cls.maxlevel; i++) {
        for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
            if (cgutil_vaddr[j]->used && cgutil_vaddr[j]->ginfo.wd.cgid == cls &&
                cgutil_vaddr[j]->ginfo.wd.wdlevel == i)
                break;
        }

        /* get the parent path of the workload group */
        relpath = gscgroup_get_parent_wdcg_path(j, cgutil_vaddr, current_nodegroup);
        if (NULL == relpath)
            return -1;

        /* get the remain group path */
        sret = snprintf_s(rempath,
            sizeof(rempath),
            sizeof(rempath) - 1,
            "%s:%d/",
            GSCGROUP_REMAIN_WORKLOAD,
            cgutil_vaddr[j]->ginfo.wd.wdlevel);
        securec_check_intval(sret, free(relpath), -1);

        sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
        securec_check_errno(sret, free(relpath), -1);

        /* update the remain cgroup */
        rempct -= cgutil_vaddr[j]->ginfo.wd.percent;

        cpushares = (u_int64_t)MAX_CLASS_CPUSHARES * rempct / GROUP_ALL_PERCENT;
        ioweight = (u_int64_t)IO_WEIGHT_CALC(MAX_IO_WEIGHT, rempct);

        ret = cgexec_update_remain_value(relpath, cpushares, ioweight);
        if (-1 == ret) {
            free(relpath);
            relpath = NULL;
            return -1;
        }

        free(relpath);
        relpath = NULL;
    }

    return 0;
}

/*
 * function name: cgexec_update_cgroup_value
 * description  : update the Cgroup information
 *                based on the value of group configuration information.
 * arguments    :
 *           grp: the configuration information of group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when updating dynamic value and fiexed value.
 */
static int cgexec_update_cgroup_value(gscgroup_grp_t* grp)
{
    char* relpath = NULL;
    struct cgroup* cg = NULL;
    long cpushares = grp->ainfo.shares;
    struct cgroup_controller* cgc_cpu = NULL;
    int ret;

    /* get the relative path */
    if (NULL == (relpath = gscgroup_get_relative_path(grp->gid, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", relpath);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get %s cgroup information for %s(%d)\n", relpath, cgroup_strerror(ret), ret);
        free(relpath);
        relpath = NULL;
        cgroup_free(&cg);
        return -1;
    }

    /* get the CPU controller */
    cgc_cpu = cgroup_get_controller(cg, MOUNT_CPU_NAME);
    if (NULL == cgc_cpu) {
        fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPU_NAME, grp->grpname);
        free(relpath);
        relpath = NULL;
        cgroup_free(&cg);
        return -1;
    }

    /* when it is dynamic value, it updates the cpu.shares value */
    if (0 == cgutil_opt.fixed || cgutil_opt.recover) {
        if (cpushares && (0 != (ret = cgroup_set_value_uint64(cgc_cpu, CPU_SHARES, cpushares)))) {
            fprintf(stderr, "ERROR: failed to set %s as %ld for %s\n", CPU_SHARES, cpushares, cgroup_strerror(ret));
            goto error;
        }
    }

    /* when it is recovering the group, it update the cpuset value in here */
    if (cgutil_opt.recover && grp->cpuset[0]) {
        /* get the CPUSET controller */
        struct cgroup_controller* cgc_cpus = cgroup_get_controller(cg, MOUNT_CPUSET_NAME);
        if (NULL == cgc_cpus) {
            fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
            goto error;
        }

        /* get cpuset value with controller */
        if (0 != (ret = cgroup_set_value_string(cgc_cpus, CPUSET_CPUS, grp->cpuset))) {
            fprintf(stderr, "ERROR: failed to set %s as %s for %s\n", CPUSET_CPUS, grp->cpuset, cgroup_strerror(ret));
            goto error;
        }
    }

    /* modify the value into kernel */
    if (0 != (ret = cgroup_modify_cgroup(cg))) {
        fprintf(stderr,
            "ERROR: failed to modify cgroup for %s "
            "when updating %s group!\n",
            cgroup_strerror(ret),
            grp->grpname);
        goto error;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    free(relpath);
    relpath = NULL;
    return 0;

error:
    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    free(relpath);
    relpath = NULL;
    return -1;
}

/*
 * @Description: search workload group id with class id.
 * @IN cls: class id
 * @Return: workload group id
 * @See also:
 */
static int cgexec_search_workload_group(int cls)
{
    int i, wd = 0, cmp = -1;
    char* tmpstr = strchr(cgutil_opt.wdname, ':');
    size_t wdname_len = strlen(cgutil_opt.wdname);

    /* search workload group */
    for (i = WDCG_START_ID; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used == 0 || cgutil_vaddr[i]->ginfo.wd.cgid != cls)
            continue;

        /* workload name with level or no level */
        if (tmpstr != NULL)
            cmp = strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.wdname);
        else {
            if (':' == cgutil_vaddr[i]->grpname[wdname_len])
                cmp = strncmp(cgutil_vaddr[i]->grpname, cgutil_opt.wdname, wdname_len);
        }

        if (0 == cmp) {
            wd = i;
            break;
        }
    }

    return wd;
}

/*
 * function name: cgexec_create_default_cgroup
 * description  : create a cgroup on the specified path based on the values
 * arguments    :
 *       relpath: the relative path of Cgroup
 *     cpushares: the value of cpu.shares
 *      ioweight: the value of blkio.weight
 *      cpuset  : the value of cpu.cpus
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when creating new Cgroup.
 */
static int cgexec_create_default_cgroup(char* relpath, int cpushares, int ioweight, char* cpuset)
{
    int ret;
    struct cgroup* cg = NULL;
    struct cgroup_controller* cgc_cpu = NULL;
    struct cgroup_controller* cgc_cpuset = NULL;
    struct cgroup_controller* cgc_cpuacct = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "failed to create the new cgroup for %s\n", relpath);
        return -1;
    }

    /* set the uid and gid */
    ret = cgroup_set_uid_gid(cg,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid);
    if (ret) {
        fprintf(stderr, "ERROR: failed to set uid and gid for %s!\n", cgroup_strerror(ret));
        cgroup_free(&cg);
        return -1;
    }

    /* add the controller */
    cgc_cpu = cgroup_add_controller(cg, MOUNT_CPU_NAME);
    if (NULL == cgc_cpu) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPU_NAME, relpath);
        cgroup_free(&cg);
        return -1;
    }

    /* set the cpu.shares value */
    if (cpushares && (0 != (ret = cgroup_set_value_uint64(cgc_cpu, CPU_SHARES, cpushares)))) {
        fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPU_SHARES, cpushares, cgroup_strerror(ret));

        goto error;
    }

    /* set the cpuset.cpus value */
    cgc_cpuset = cgroup_add_controller(cg, MOUNT_CPUSET_NAME);
    if (NULL == cgc_cpuset) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPUSET_NAME, relpath);
        goto error;
    }

    if (*cpuset) {
        /* set the cpuset.mems value */
        if (0 != (ret = cgroup_set_value_string(cgc_cpuset, CPUSET_MEMS, cgutil_mems))) {
            fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPUSET_MEMS, 0, cgroup_strerror(ret));
            goto error;
        }

        /* set the cpuset.cpus value */
        if (0 != (ret = cgroup_set_value_string(cgc_cpuset, CPUSET_CPUS, cpuset))) {
            fprintf(stderr, "ERROR: failed to set %s as %s for %s\n", CPUSET_CPUS, cpuset, cgroup_strerror(ret));
            goto error;
        }
    }

    /* add the controller */
    cgc_cpuacct = cgroup_add_controller(cg, MOUNT_CPUACCT_NAME);
    if (NULL == cgc_cpuacct) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPUACCT_NAME, relpath);
        goto error;
    }

    /* set the cpu.usage value */
    if (0 != (ret = cgroup_set_value_uint64(cgc_cpuacct, CPUACCT_USAGE, 0))) {
        fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPUACCT_USAGE, 0, cgroup_strerror(ret));

        goto error;
    }

    /* create the Cgroup on kernel */
    ret = cgroup_create_cgroup(cg, 0);
    if (ret) {
        fprintf(stderr, "ERROR: can't create cgroup for %s\n", cgroup_strerror(ret));
        goto error;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    return 0;

error:
    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return -1;
}

/*
 * function name: cgexec_create_remain_cgroup
 * description  : create the remain cgroup based on the same level workload group
 * arguments    :
 *           grp: the workload group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when creating new Cgroup.
 */
static int cgexec_create_remain_cgroup(gscgroup_grp_t* grp)
{
    char* relpath = NULL;
    long cpushares;
    long ioweight;
    int i, changed = 0;
    char rempath[16];
    errno_t sret;

    if (NULL == (relpath = gscgroup_get_parent_wdcg_path(grp->gid, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* add the remain path dir */
    sret = snprintf_s(
        rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d", GSCGROUP_REMAIN_WORKLOAD, grp->ginfo.wd.wdlevel);
    securec_check_intval(sret, free(relpath), -1);

    sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
    securec_check_errno(sret, free(relpath), -1);

    /* get the class group */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used && cgutil_vaddr[i]->gid == grp->ginfo.wd.cgid)
            break;
    }

    if (i > CLASSCG_END_ID) {
        free(relpath);
        relpath = NULL;
        return -1;
    }

    if (grp->ginfo.cls.maxlevel == 1 && GROUP_ALL_PERCENT == cgutil_vaddr[i]->ginfo.cls.rempct) {
        changed = 1;
        cgutil_vaddr[i]->ginfo.cls.rempct = NORMALWD_PERCENT;
    }

    cpushares = MAX_CLASS_CPUSHARES * cgutil_vaddr[i]->ginfo.cls.rempct / GROUP_ALL_PERCENT;
    ioweight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, cgutil_vaddr[i]->ginfo.cls.rempct);

    if (i != grp->ginfo.wd.cgid) {
        sret = snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, 
                          "%s", cgutil_vaddr[grp->ginfo.wd.cgid]->cpuset);
        securec_check_intval(sret, free(relpath), -1);
    }

    (void)cgexec_create_default_cgroup(relpath, cpushares, ioweight, cgutil_vaddr[i]->cpuset);

    if (changed)
        cgutil_vaddr[i]->ginfo.cls.rempct = GROUP_ALL_PERCENT;

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * function name: cgexec_set_blkio_throttle_value
 * description  : set the blkio throttle value when creating new cgroup
 *              : based on configure file
 */
int cgexec_set_blkio_throttle_value(const char* relpath, const char* name, const char* value)
{
    int ret;
    char *p = NULL, *q = NULL, *head = NULL, *i = NULL;
    struct cgroup* cg = NULL;
    struct cgroup_controller* cgc = NULL;

    /* allocate new cgroup structure */
    if ((cg = cgexec_get_cgroup(relpath)) == NULL)
        return -1;

    /* get controller */
    cgc = cgroup_get_controller(cg, MOUNT_BLKIO_NAME);
    if (cgc == NULL) {
        cgroup_free(&cg);
        return -1;
    }

    head = strdup(value);
    if (head == NULL) {
        cgroup_free_controllers(cg);
        cgroup_free(&cg);
        return -1;
    }
    p = head;

    do {
        q = strchr(p, '\n');
        if (q != NULL)
            *q++ = '\0';

        i = p;
        while (*i++) {
            if (*i == '\t')
                *i = ' ';
        }

        ret = cgroup_set_value_string(cgc, name, p);
        if (ret) {
            fprintf(stderr, "failed to set %s as %s for %s\n", name, p, cgroup_strerror(ret));
            p = q;
            continue;
        }

        /* update controller into kernel */
        if (0 != (ret = cgroup_modify_cgroup(cg))) {
            fprintf(stderr,
                "failed to modify cgroup for %s "
                "when modifying values!\n",
                cgroup_strerror(ret));
            p = q;
            continue;
        }

        p = q;
    } while (q != NULL);

    free(head);
    head = NULL;
    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    return 0;
}

/*
 * function name: cgexec_create_new_cgroup
 * description  : create the new Cgroup based on configuration information
 * arguments    :
 *           grp: the configuration information
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when creating new Cgroup.
 */
int cgexec_create_new_cgroup(gscgroup_grp_t* grp)
{
    char* relpath = NULL;
    int ret;
    struct cgroup* cg = NULL;
    struct cgroup_controller* cg_controllers[MOUNT_SUBSYS_KINDS] = {0};

    if (NULL == (relpath = gscgroup_get_relative_path(grp->gid, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "failed to create the new cgroup for %s\n", relpath);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    /* set the uid and gid */
    ret = cgroup_set_uid_gid(cg,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid);
    if (ret) {
        fprintf(stderr, "ERROR: failed to set uid and gid for %s!\n", cgroup_strerror(ret));
        free(relpath);
        relpath = NULL;
        cgroup_free(&cg);
        return -1;
    }

    /* add the controller */
    cg_controllers[MOUNT_CPU_ID] = cgroup_add_controller(cg, MOUNT_CPU_NAME);
    if (NULL == cg_controllers[MOUNT_CPU_ID]) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPU_NAME, grp->grpname);
        free(relpath);
        relpath = NULL;
        cgroup_free(&cg);
        return -1;
    }

    /* set the cpu.shares value */
    if (grp->ainfo.shares &&
        (0 != (ret = cgroup_set_value_uint64(cg_controllers[MOUNT_CPU_ID], CPU_SHARES, grp->ainfo.shares)))) {
        fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPU_SHARES, grp->ainfo.shares, cgroup_strerror(ret));
        goto error;
    }

    cg_controllers[MOUNT_CPUSET_ID] = cgroup_add_controller(cg, MOUNT_CPUSET_NAME);
    if (NULL == cg_controllers[MOUNT_CPUSET_ID]) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
        goto error;
    }

    /* set the cpu.cpus value */
    if (*grp->cpuset) {
        if ((0 != (ret = cgroup_set_value_string(cg_controllers[MOUNT_CPUSET_ID], CPUSET_MEMS, cgutil_mems)))) {
            fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPUSET_MEMS, 0, cgroup_strerror(ret));
            goto error;
        }

        if (0 != (ret = cgroup_set_value_string(cg_controllers[MOUNT_CPUSET_ID], CPUSET_CPUS, grp->cpuset))) {
            fprintf(stderr, "ERROR: failed to set %s as %s for %s\n", CPUSET_CPUS, grp->cpuset, cgroup_strerror(ret));
            goto error;
        }
    }
    /* add cpuacct controllor */
    cg_controllers[MOUNT_CPUACCT_ID] = cgroup_add_controller(cg, MOUNT_CPUACCT_NAME);
    if (NULL == cg_controllers[MOUNT_CPUACCT_ID]) {
        fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", MOUNT_CPUACCT_NAME, grp->grpname);
        goto error;
    }

    if (0 != (ret = cgroup_set_value_int64(cg_controllers[MOUNT_CPUACCT_ID], CPUACCT_USAGE, 0))) {
        fprintf(stderr, "ERROR: failed to set %s as %d for %s\n", CPUACCT_USAGE, 0, cgroup_strerror(ret));
        goto error;
    }

    /* create the Cgroup on kernel */
    ret = cgroup_create_cgroup(cg, 0);
    if (ret) {
        fprintf(stderr, "ERROR: can't create cgroup for %s\n", cgroup_strerror(ret));
        goto error;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    free(relpath);
    relpath = NULL;
    return 0;

error:
    free(relpath);
    relpath = NULL;
    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return -1;
}

/*
 * function name: cgexec_create_workload_cgroup
 * description  : create the new Cgroup based on configuration information
 * arguments    :
 *           grp: the configuration information of workload group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when creating new workload Cgroup.
 */
static int cgexec_create_workload_cgroup(gscgroup_grp_t* grp)
{
    int cgid = grp->ginfo.wd.cgid;
    gscgroup_grp_t* cls_grp = cgutil_vaddr[cgid];
    int nextlevel = cls_grp->ginfo.cls.maxlevel + 1;
    int i, j;

    /* skip the workload */
    if (nextlevel > grp->ginfo.wd.wdlevel)
        return 0;
    /* when the workload is the next level workload group */
    if (nextlevel == grp->ginfo.wd.wdlevel) {
        if (-1 == cgexec_create_new_cgroup(grp))
            return -1;

        if (-1 == cgexec_create_remain_cgroup(grp))
            return -1;
    }
    /* need to create all parent workload group firstly */
    else if (nextlevel < grp->ginfo.wd.wdlevel) {
        cls_grp->ginfo.cls.rempct += grp->ginfo.wd.percent;

        for (i = nextlevel; i < grp->ginfo.wd.wdlevel; i++) {
            for (j = grp->gid; j <= WDCG_END_ID; j++) {
                if (cgutil_vaddr[j]->used && cgid == cgutil_vaddr[j]->ginfo.wd.cgid &&
                    i == cgutil_vaddr[j]->ginfo.wd.wdlevel)
                    break;
            }

            if (j > WDCG_END_ID) {
                fprintf(stderr, "can't find the parent workload!\n");
                return -1;
            }

            if (-1 == cgexec_create_new_cgroup(cgutil_vaddr[j]))
                return -1;

            cls_grp->ginfo.cls.rempct -= cgutil_vaddr[j]->ginfo.wd.percent;

            if (-1 == cgexec_create_remain_cgroup(cgutil_vaddr[j]))
                return -1;

            /* set the maxlevel value of class group */
            cls_grp->ginfo.cls.maxlevel = i;
        }

        if (-1 == cgexec_create_new_cgroup(grp))
            return -1;

        cls_grp->ginfo.cls.rempct -= grp->ginfo.wd.percent;

        if (-1 == cgexec_create_remain_cgroup(grp))
            return -1;
    }

    /* set the maxlevel of Class group */
    cgutil_vaddr[cgid]->ginfo.cls.maxlevel += 1;

    return 0;
}

/*
 * function name: cgexec_create_timeshare_cgroup
 * description  : create the all timeshare Cgroup of the specified Class Cgroup
 * arguments    :
 *           grp: the configuration information of Class group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when creating new Class Cgroup.
 */
static int cgexec_create_timeshare_cgroup(gscgroup_grp_t* grp)
{
    char* toppath = NULL;
    char* relpath = NULL;
    long cpushares;
    long ioweight;
    int j, ret;

    /* create the top timeshare cgroup */
    cpushares = DEFAULT_CPU_SHARES;
    ioweight = DEFAULT_IO_WEIGHT;

    /* get the top timeshare path */
    toppath = gscgroup_get_topts_path(grp->gid, cgutil_vaddr, current_nodegroup);
    if (NULL == toppath)
        return -1;

    /* create the top level Cgroup */
    ret = cgexec_create_default_cgroup(toppath, cpushares, ioweight, grp->cpuset);
    if (-1 == ret) {
        free(toppath);
        toppath = NULL;
        return -1;
    }

    /* allocate memory for path of timeshare cgroup */
    if (NULL == (relpath = (char*)malloc(GPNAME_PATH_LEN))) {
        fprintf(stderr, "ERROR: failed to allocate memory for path!\n");
        free(toppath);
        toppath = NULL;
        return -1;
    }

    /* create the default timeshare cgroups */
    for (j = TSCG_START_ID; j <= TSCG_END_ID; j++) {
        int rc = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", toppath, cgutil_vaddr[j]->grpname);
        securec_check_intval(rc, free(toppath); free(relpath), -1);

        cpushares = cgutil_vaddr[j]->ainfo.shares;
        ioweight = cgutil_vaddr[j]->ainfo.weight;

        ret = cgexec_create_default_cgroup(relpath, cpushares, ioweight, grp->cpuset);
        if (-1 == ret) {
            free(toppath);
            toppath = NULL;
            free(relpath);
            relpath = NULL;
            return -1;
        }
    }

    free(toppath);
    toppath = NULL;
    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * function name: cgexec_delete_default_cgroup
 * description  : delete the Cgroup based on configuration information
 * arguments    :
 *           grp: the Group configuration information
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when dropping a Cgroup.
 */
static int cgexec_delete_default_cgroup(gscgroup_grp_t* grp)
{
    char* relpath = NULL;

    relpath = gscgroup_get_relative_path(grp->gid, cgutil_vaddr, current_nodegroup);
    if (NULL == relpath)
        return -1;

    (void)cgexec_delete_cgroups(relpath);

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * function name: cgexec_create_nodegroup_default_cgroups
 * description  : create default cgroups based on node group name
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_create_nodegroup_default_cgroups(void)
{
    int i, ret = 0;
    errno_t sret;
    char* cpuset = NULL;
    char cpu_allset[CPUSET_LEN] = {0};

    /* get memory set */
    if (-1 == cgexec_get_cgroup_cpuset_info(TOPCG_GAUSSDB, &cpuset)) {
        fprintf(stderr, "ERROR: failed to get cpusets and mems during creating default nodegroup cgroups.\n");
        return -1;
    }

    /* set gaussdb default cpuset value */
    if ((cpuset != NULL) && *cpuset != '\0') {
        sret = snprintf_s(cpu_allset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpuset);
        securec_check_intval(sret, free(cpuset), -1);
        free(cpuset);
        cpuset = NULL;
    } else {
        sret = snprintf_s(cpu_allset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_allset);
        securec_check_intval(sret, , -1);
    }

    sret = snprintf_s(cgutil_vaddr[TOPCG_CLASS]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpu_allset);
    securec_check_intval(sret, , -1);

    /* create nodegroup cgroup */
    if (-1 == (ret = cgexec_create_new_cgroup(cgutil_vaddr[TOPCG_CLASS]))) {
        fprintf(stderr, "failed to create %s cgroup!\n", cgutil_vaddr[TOPCG_CLASS]->grpname);
        return -1;
    }

    /* create all Cgroup except the timeshare Cgroup */
    for (i = CLASSCG_START_ID; i <= WDCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used)
            continue;

        /* update the cpuset info */
        sret = snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpu_allset);
        securec_check_intval(sret, , -1);

        if (i >= CLASSCG_START_ID && i <= CLASSCG_END_ID) {
            /* reset the maxlevel number */
            cgutil_vaddr[i]->ginfo.cls.maxlevel = 0;
            cgutil_vaddr[i]->ginfo.cls.rempct = GROUP_ALL_PERCENT;

            ret = cgexec_create_new_cgroup(cgutil_vaddr[i]);
        } else if (i >= WDCG_START_ID && i <= WDCG_END_ID) {
            int cls = cgutil_vaddr[i]->ginfo.wd.cgid;

            if (strncmp(cgutil_vaddr[i]->grpname, GSCGROUP_TOP_WORKLOAD, sizeof(GSCGROUP_TOP_WORKLOAD) - 1) == 0)
                cgconf_set_top_workload_group(i, cls);

            if (cgutil_vaddr[i]->ginfo.cls.maxlevel == 1)
                cgutil_vaddr[i]->ginfo.cls.rempct = GROUP_ALL_PERCENT;
            else if (cgutil_vaddr[cls]->ginfo.cls.maxlevel < cgutil_vaddr[i]->ginfo.wd.wdlevel)
                cgutil_vaddr[cls]->ginfo.cls.rempct -= cgutil_vaddr[i]->ginfo.cls.percent;

            ret = cgexec_create_workload_cgroup(cgutil_vaddr[i]);
        }

        if (-1 == ret) {
            fprintf(stderr, "failed to create %s cgroup!\n", cgutil_vaddr[i]->grpname);
            continue;
        }
    }

    /* create the timeshare group of each Class group */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used)
            continue;

        ret = cgexec_create_timeshare_cgroup(cgutil_vaddr[i]);
        if (-1 == ret) {
            fprintf(stderr, "failed to create timeshare cgroup for %s!\n", cgutil_vaddr[i]->grpname);
            return -1;
        }
    }

    return ret;
}

/*
 * function name: cgexec_create_default_cgroups
 * description  : when there is no Cgroups on kernel, it means that it need
 *                to create the Cgroups based on the default Configuration file.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_create_default_cgroups(void)
{
    int i, ret = 0;
    errno_t sret;

    /* the root Cgroup has exists after mounting Cgroup file system */
    if ((cgutil_is_sles11_sp2 || cgexec_check_SLESSP2_version()) && (cgutil_opt.refresh == 0 && cgutil_opt.revert == 0))
        (void)cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_ROOT]);

    /* create all Cgroup except the timeshare Cgroup */
    for (i = 1; i <= WDCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used)
            continue;

        /* set gaussdb default cpuset value */
        if (*cgutil_vaddr[TOPCG_GAUSSDB]->cpuset == '\0') {
            sret = snprintf_s(cgutil_vaddr[TOPCG_GAUSSDB]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_allset);
            securec_check_intval(sret, , -1);
        }

        /* Top Cgroup */
        // how to process quota and cpuset?
        // 1. if Gaussdb range is changed, all subdir's quota should be changed
        // so cgexec_check_top_cpuset the function is not enough to process this
        // 2. if cpusets is not the same as upper dir, it should caclucate the quota value

        if (i > TOPCG_GAUSSDB && *cgutil_vaddr[i]->cpuset == '\0') {
            sret = snprintf_s(
                cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);
            securec_check_intval(sret, , -1);
        }

        if (i < CLASSCG_START_ID)
            ret = cgexec_create_new_cgroup(cgutil_vaddr[i]);
        else if (i >= CLASSCG_START_ID && i <= CLASSCG_END_ID) {
            if (0 == cgutil_vaddr[i]->used)
                continue;

            /* reset the maxlevel number */
            cgutil_vaddr[i]->ginfo.cls.maxlevel = 0;
            cgutil_vaddr[i]->ginfo.cls.rempct = GROUP_ALL_PERCENT;

            ret = cgexec_create_new_cgroup(cgutil_vaddr[i]);
        } else if (i >= WDCG_START_ID && i <= WDCG_END_ID) {
            int cls = cgutil_vaddr[i]->ginfo.wd.cgid;

            if (strncmp(cgutil_vaddr[i]->grpname, GSCGROUP_TOP_WORKLOAD, sizeof(GSCGROUP_TOP_WORKLOAD) - 1) == 0)
                cgconf_set_top_workload_group(i, cls);

            if (cgutil_vaddr[i]->ginfo.cls.maxlevel == 1)
                cgutil_vaddr[i]->ginfo.cls.rempct = GROUP_ALL_PERCENT;
            else if (cgutil_vaddr[cls]->ginfo.cls.maxlevel < cgutil_vaddr[i]->ginfo.wd.wdlevel)
                cgutil_vaddr[cls]->ginfo.cls.rempct -= cgutil_vaddr[i]->ginfo.cls.percent;

            ret = cgexec_create_workload_cgroup(cgutil_vaddr[i]);
        }

        if (-1 == ret) {
            fprintf(stderr, "failed to create %s cgroup!\n", cgutil_vaddr[i]->grpname);
            continue;
        }
    }

    /* create the timeshare group of each Class group */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used)
            continue;

        ret = cgexec_create_timeshare_cgroup(cgutil_vaddr[i]);
        if (-1 == ret) {
            fprintf(stderr, "failed to create timeshare cgroup for %s!\n", cgutil_vaddr[i]->grpname);
            return -1;
        }
    }

    return ret;
}

/*
 * function name: cgexec_is_same_group
 * description  : check whether old workload group and the new group is the same.
 * return value : true yes, false no
 */
bool cgexec_is_same_group(const char* oldwd, const char* newwd)
{
    int len = strlen(newwd);

    if (':' == oldwd[len])
        return strncmp(oldwd, newwd, len) == 0;

    return false;
}

/*
 * function name: cgexec_create_class_cgroup
 * description  : when non-root user wants to create Class Cgroup or
 *                Workload Cgroup, it calls this function to do the things.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_create_class_cgroup(void)
{
    int i, cls = 0, find = 0;
    int percent = 0;
    char* toppath = NULL;

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0) {
            if (cls == 0)
                cls = i;
            continue;
        }

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.clsname)) {
            find = 1;
            cls = i;
            break;
        }
    }

    /* back up the config file */
    if (-1 == cgconf_backup_config_file()) {
        return -1;
    }

    /* create cgroup if it doesn't exist */
    if (find == 0) {
        if (cls == 0) {
            fprintf(stderr, "ERROR: failed to create %s cgroup for there is no class item!\n", cgutil_opt.clsname);
            return -1;
        } else /* create cgroup */
        {
            /* check the remain percentage */
            for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
                if (cgutil_vaddr[i]->used)
                    percent += cgutil_vaddr[i]->ginfo.cls.percent;
            }

            if (cgutil_opt.clspct) {
                if (cgutil_opt.clspct > (GROUP_ALL_PERCENT - percent)) {
                    fprintf(stderr,
                        "ERROR: there is no more resource for new cgroup %s.\n"
                        "the remain percentage is %d.\n",
                        cgutil_opt.clsname,
                        GROUP_ALL_PERCENT - percent);
                    return -1;
                }
            } else {
                if (DEFAULT_CLASS_PERCENT > (GROUP_ALL_PERCENT - percent))
                    cgutil_opt.clspct = GROUP_ALL_PERCENT - percent;
                else
                    cgutil_opt.clspct = DEFAULT_CLASS_PERCENT;

                if (cgutil_opt.clspct == 0) {
                    fprintf(stderr,
                        "ERROR: there is no more resource for new cgroup %s.\n"
                        "the remain percentage is %d.\n",
                        cgutil_opt.clsname,
                        GROUP_ALL_PERCENT - percent);
                    return -1;
                }
            }

            /* set the cgutil_vaddr item */
            cgconf_set_class_group(cls);

            if (-1 == cgexec_create_new_cgroup(cgutil_vaddr[cls])) {
                cgconf_reset_class_group(cls);
                return -1;
            }

            /* set the top wd item */
            for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
                if (cgutil_vaddr[i]->used == 0)
                    break;
            }

            cgconf_set_top_workload_group(i, cls);

            /* create the workload cgroup */
            if (-1 == cgexec_create_workload_cgroup(cgutil_vaddr[i])) {
                cgconf_reset_workload_group(i);
                return -1;
            }
        }
    } else {
        if (!cgutil_opt.wdname[0]) {
            fprintf(stderr, "ERROR: cannot create existed class %s.\n", cgutil_opt.clsname);
            return -1;
        }

        if (cgutil_opt.clssetpct == 1 || cgutil_opt.clspct) {
            fprintf(stderr,
                "ERROR: cannot specify existed class %s and \"-s\" together when create control group\n",
                cgutil_opt.clsname);
            return -1;
        }
    }

    /* find the group item if it is specified */
    if (cgutil_opt.wdname[0]) {
        if (cgutil_vaddr[cls]->ginfo.cls.maxlevel == MAX_WD_LEVEL) {
            fprintf(stderr,
                "ERROR: failed to create %s cgroup "
                "for %s cgroup has reach the maximum level!\n",
                cgutil_opt.wdname,
                cgutil_opt.clsname);
            return -1;
        }

        for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
            if (cgutil_vaddr[i]->used == 0)
                break;

            if (cgutil_vaddr[i]->ginfo.wd.cgid == cls &&
                cgexec_is_same_group(cgutil_vaddr[i]->grpname, cgutil_opt.wdname)) {
                fprintf(stderr,
                    "ERROR: failed to create %s cgroup "
                    "for %s has been existed for class %s \n",
                    cgutil_opt.wdname,
                    cgutil_vaddr[i]->grpname,
                    cgutil_vaddr[cls]->grpname);
                return -1;
            }
        }

        /* should make sure there is resource for timeshare cgroup */
        if (cgutil_opt.grppct) {
            if (cgutil_vaddr[cls]->ginfo.cls.rempct <= cgutil_opt.grppct) {
                fprintf(stderr,
                    "ERROR: there is no more resource for new cgroup %s.\n"
                    "the remain percentage is %d, available percentage is %d.\n",
                    cgutil_opt.wdname,
                    cgutil_vaddr[cls]->ginfo.cls.rempct,
                    cgutil_vaddr[cls]->ginfo.cls.rempct - 1);
                return -1;
            }
        } else {
            if (DEFAULT_WORKLOAD_PERCENT >= cgutil_vaddr[cls]->ginfo.cls.rempct)
                cgutil_opt.grppct = cgutil_vaddr[cls]->ginfo.cls.rempct - 1;
            else
                cgutil_opt.grppct = DEFAULT_WORKLOAD_PERCENT;
        }

        /* if timeshare has been created, drop them */
        if (find) {
            /* get the top timeshare path */
            toppath = gscgroup_get_topts_path(cgutil_vaddr[cls]->gid, cgutil_vaddr, current_nodegroup);
            if (NULL == toppath)
                return -1;

            if (-1 == cgexec_delete_cgroups(toppath)) {
                free(toppath);
                toppath = NULL;
                return -1;
            }

            free(toppath);
            toppath = NULL;
        }

        cgconf_set_workload_group(i, cls);

        /* create the workload cgroup */
        if (-1 == cgexec_create_workload_cgroup(cgutil_vaddr[i])) {
            cgconf_reset_workload_group(i);
            return -1;
        }

        /* create timeshare cgroup */
        if (-1 == cgexec_create_timeshare_cgroup(cgutil_vaddr[cls]))
            return -1;
    } else {
        if (find == 0 && -1 == cgexec_create_timeshare_cgroup(cgutil_vaddr[cls]))
            return -1;
    }

    return 0;
}

/*
 * function name: cgexec_delete_remain_cgroup
 * description  : When one workload Cgroup is deleted, it needs to call
 *                this function to delete the last remain Cgroup and its
 *                Child Cgroups(timeshare Cgroup)
 * arguments    :
 *           grp: the configuration of workload Cgroup which is the same level
 *                as the remain Cgroup
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_delete_remain_cgroup(gscgroup_grp_t* grp)
{
    char* relpath = NULL;
    char rempath[16];
    errno_t sret;

    relpath = gscgroup_get_parent_wdcg_path(grp->gid, cgutil_vaddr, current_nodegroup);
    if (NULL == relpath)
        return -1;

    /* add the remain path dir */
    sret = snprintf_s(
        rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d", GSCGROUP_REMAIN_WORKLOAD, grp->ginfo.wd.wdlevel);
    securec_check_intval(sret, free(relpath), -1);
    sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
    securec_check_errno(sret, free(relpath), -1);

    (void)cgexec_delete_cgroups(relpath);

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * function name: cgexec_copy_next_level_cgroup
 * description  : copy the specified workload Cgroup the upper level.
 * arguments    :
 *       relpath: the parent path of deleted cgroup
 *           grp: the configuration of workload group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_copy_next_level_cgroup(const char* relpath, gscgroup_grp_t* grp)
{
    int ret;
    char grpname[GPNAME_LEN];
    char* wdpath = NULL;
    char* p = NULL;
    struct cgroup *oldcg = NULL, *newcg = NULL;
    struct cgroup_controller* cgc[MOUNT_SUBSYS_KINDS];
    errno_t sret;

    /* get the current path of specified Cgroup */
    wdpath = gscgroup_get_relative_path(grp->gid, cgutil_vaddr, current_nodegroup);
    if (NULL == wdpath) {
        fprintf(stderr, "ERROR: failed to allocate memory for path!\n");
        return -1;
    }

    /* allocate new cgroup structure */
    oldcg = cgroup_new_cgroup(wdpath);
    if (oldcg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", wdpath);
        free(wdpath);
        wdpath = NULL;
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(oldcg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get %s cgroup information for %s(%d)\n", wdpath, cgroup_strerror(ret), ret);
        free(wdpath);
        wdpath = NULL;
        cgroup_free(&oldcg);
        return -1;
    }

    sret = memset_s(wdpath, GPNAME_PATH_LEN, 0, GPNAME_PATH_LEN);
    securec_check_errno(sret, free(wdpath); cgroup_free(&oldcg), -1);

    /* get the grpname without level */
    sret = strcpy_s(grpname, GPNAME_LEN, grp->grpname);
    securec_check_errno(sret, free(wdpath); cgroup_free(&oldcg);, -1);

    if ((p = strchr(grpname, ':')) != NULL)
        *p = '\0';

    /* get the new path of the workload cgroup */
    sret = snprintf_s(
        wdpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s%s:%d", relpath, grpname, grp->ginfo.wd.wdlevel - 1);
    securec_check_intval(sret, free(wdpath); cgroup_free(&oldcg), -1);

    /* allocate new cgroup structure */
    newcg = cgroup_new_cgroup(wdpath);
    if (newcg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", wdpath);
        free(wdpath);
        wdpath = NULL;
        cgroup_free(&oldcg);
        return -1;
    }

    /* set the uid and gid */
    ret = cgroup_set_uid_gid(newcg,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid,
        cgutil_passwd_user->pw_uid,
        cgutil_passwd_user->pw_gid);
    if (ret) {
        fprintf(stderr, "ERROR: failed to set uid and gid for %s!\n", cgroup_strerror(ret));
        free(wdpath);
        wdpath = NULL;
        cgroup_free(&oldcg);
        cgroup_free(&newcg);
        return -1;
    }

    /* add the controller */
    for (int i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        if (i == MOUNT_BLKIO_ID || i == MOUNT_MEMORY_ID)
            continue;

        cgc[i] = cgroup_add_controller(newcg, cgutil_subsys_table[i]);

        if (cgc[i] == NULL) {
            fprintf(stderr, "ERROR: failed to add %s controller for %s!\n", cgutil_subsys_table[i], grp->grpname);

            cgroup_free(&oldcg);

            goto error;
        }
    }

    ret = cgroup_create_cgroup(newcg, 0);
    if (ret) {
        fprintf(stderr, "ERROR: can't create cgroup for %s\n", cgroup_strerror(ret));
        cgroup_free(&oldcg);
        goto error;
    }

    /* copy the group from old cg */
    ret = cgroup_copy_cgroup(newcg, oldcg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to copy cgroup information for %s(%d)\n", cgroup_strerror(ret), ret);
        cgroup_free(&oldcg);
        goto error;
    }

    cgroup_free(&oldcg);

    ret = cgroup_modify_cgroup(newcg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to modify cgroup information for %s(%d)\n", cgroup_strerror(ret), ret);
        goto error;
    }

    free(wdpath);
    wdpath = NULL;
    cgroup_free_controllers(newcg);
    cgroup_free(&newcg);

    /* delete the old one */
    if (-1 == cgexec_delete_default_cgroup(grp))
        return -1;

    /* update the workload group */
    grp->ginfo.wd.wdlevel -= 1;

    sret = snprintf_s(
        grp->grpname, sizeof(grp->grpname), sizeof(grp->grpname) - 1, "%s:%d", grpname, grp->ginfo.wd.wdlevel);
    securec_check_intval(sret, , -1);

    return 0;

error:
    free(wdpath);
    wdpath = NULL;
    cgroup_free_controllers(newcg);
    cgroup_free(&newcg);
    return -1;
}

/*
 * function name: cgexec_delete_workload_cgroup
 * description  : delete the specified workload Cgroup
 * arguments    :
 *           grp: the configuration of workload group
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_delete_workload_cgroup(gscgroup_grp_t* grp)
{
    int wgid = grp->gid;
    int wglevel = grp->ginfo.wd.wdlevel;
    int cgid = grp->ginfo.wd.cgid;
    gscgroup_grp_t* cls_grp = cgutil_vaddr[cgid];
    int i, j, ret, rempct = GROUP_ALL_PERCENT;
    int cpushares, ioweight;
    char* relpath = NULL;
    char rempath[16];
    errno_t sret;

    /* it is the last level workload group */
    if (wglevel == cls_grp->ginfo.cls.maxlevel) {
        /* delete remain cgroup */
        if (-1 == cgexec_delete_remain_cgroup(grp))
            return -1;

        /* delete workload cgroup */
        if (-1 == cgexec_delete_default_cgroup(grp))
            return -1;

        /* reset remain percent */
        cls_grp->ginfo.cls.rempct += grp->ginfo.wd.percent;

        cgconf_reset_workload_group(grp->gid);
    } else {
        /* delete the first one */
        if (-1 == cgexec_delete_default_cgroup(grp))
            return -1;

        if (NULL == (relpath = gscgroup_get_parent_wdcg_path(grp->gid, cgutil_vaddr, current_nodegroup)))
            return -1;

        /* count from 2 is for discarding the TopWD group */
        for (i = 2; i < wglevel; i++) {
            /* calculate the remain percentage */
            for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
                if (cgutil_vaddr[j]->used && cgutil_vaddr[j]->ginfo.wd.cgid == cgid &&
                    cgutil_vaddr[j]->ginfo.wd.wdlevel == i)
                    break;
            }

            rempct -= cgutil_vaddr[j]->ginfo.wd.percent;
        }

        cls_grp->ginfo.cls.rempct += grp->ginfo.wd.percent;

        /* reset, can't use grp */
        cgconf_reset_workload_group(wgid);

        for (i = wglevel; i < cls_grp->ginfo.cls.maxlevel; i++) {
            /* get the next level workload */
            for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
                if (cgutil_vaddr[j]->used && cgutil_vaddr[j]->ginfo.wd.cgid == cgid &&
                    cgutil_vaddr[j]->ginfo.wd.wdlevel == (i + 1))
                    break;
            }

            /* copy the next workload into this level */
            if (-1 == cgexec_copy_next_level_cgroup(relpath, cgutil_vaddr[j])) {
                free(relpath);
                relpath = NULL;
                return -1;
            }

            /* add the remain path dir */
            sret = snprintf_s(rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d/", GSCGROUP_REMAIN_WORKLOAD, i);
            securec_check_intval(sret, free(relpath), -1);
            sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
            securec_check_errno(sret, free(relpath), -1);

            /* update the remain cgroup */
            rempct -= cgutil_vaddr[j]->ginfo.wd.percent;

            cpushares = MAX_CLASS_CPUSHARES * rempct / GROUP_ALL_PERCENT;
            ioweight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, rempct);

            ret = cgexec_update_remain_value(relpath, cpushares, ioweight);
            if (-1 == ret) {
                free(relpath);
                relpath = NULL;
                return -1;
            }
        }

        /* add the remain path dir */
        sret = snprintf_s(rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d/", GSCGROUP_REMAIN_WORKLOAD, i);
        securec_check_intval(sret, free(relpath), -1);
        sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
        securec_check_errno(sret, free(relpath), -1);

        (void)cgexec_delete_cgroups(relpath);
        free(relpath);
        relpath = NULL;
    }

    cls_grp->ginfo.cls.maxlevel -= 1;

    if (-1 == cgexec_create_timeshare_cgroup(cgutil_vaddr[cgid]))
        return -1;

    return 0;
}

/*
 * function name: cgexec_delete_class_cgroup
 * description  : delete the class Cgroup and workload Cgroup based on options
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_delete_class_cgroup(void)
{
    int i, cls = 0, wd = 0;

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.clsname)) {
            cls = i;
            break;
        }
    }

    /* backup the config file for recovery */
    if (-1 == cgconf_backup_config_file()) {
        return -1;
    }

    if (cls) {
        if (cgutil_opt.wdname[0]) {
            wd = cgexec_search_workload_group(cls);

            if (wd) {
                (void)cgexec_delete_workload_cgroup(cgutil_vaddr[wd]);
            } else {
                fprintf(stderr, "ERROR: the specified workload %s doesn't exist!\n", cgutil_opt.wdname);
                return -1;
            }
        } else {
            if (-1 == cgexec_delete_default_cgroup(cgutil_vaddr[cls]))
                return -1;

            cgconf_reset_class_group(cls);
        }
    } else {
        fprintf(stderr, "ERROR: the specified class %s doesn't exist!\n", cgutil_opt.clsname);
        return -1;
    }

    return 0;
}

/*
 * @Description: update cgroup cpuset value.
 * @IN relpath: relpath of the cgroup
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_cgroup_cpuset_value(char* relpath, char* cpuset)
{
    int ret;
    struct cgroup_controller* cgc = NULL;
    struct cgroup* cg = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", relpath);
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get %s cgroup information for %s(%d)\n", relpath, cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return -1;
    }

    /* get the CPUSET controller */
    cgc = cgroup_get_controller(cg, MOUNT_CPUSET_NAME);
    if (NULL == cgc) {
        cgroup_free(&cg);
        return -1;
    }

    /* get cpuset value with controller */
    if (0 != (ret = cgroup_set_value_string(cgc, CPUSET_CPUS, cpuset))) {
        fprintf(stderr, "ERROR: failed to set %s as %s for %s\n", CPUSET_CPUS, cpuset, cgroup_strerror(ret));
        goto error;
    }

    /* modify the value into kernel */
    if (0 != (ret = cgroup_modify_cgroup(cg))) {
        fprintf(stderr,
            "ERROR: failed to modify cgroup for %s "
            "when updating group!\n",
            cgroup_strerror(ret));
        goto error;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);

    return 0;

error:
    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return -1;
}

/*
 * @Description: update timeshare group cpuset value.
 * @IN grp: group info
 * @IN cpuset: cpuset value
 * @IN update: 0: update remain cgroup cpuset value and then update timeshare
 *             not 0: update timeshrare cpuset value and then update remain group
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_timeshare_cpuset(gscgroup_grp_t* grp, char* cpuset, unsigned char update)
{
    char* toppath = NULL;
    char* relpath = NULL;
    int i;

    /* get the top timeshare path */
    toppath = gscgroup_get_topts_path(grp->gid, cgutil_vaddr, current_nodegroup);
    if (NULL == toppath)
        return -1;

    /* If update flag is 0, we must update the toppath cgroup with cpuset value first */
    if (update == 0 && cgexec_update_cgroup_cpuset_value(toppath, cpuset) == -1) {
        fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
        free(toppath);
        toppath = NULL;
        return -1;
    }

    /* allocate memory for path of timeshare cgroup */
    if (NULL == (relpath = (char*)malloc(GPNAME_PATH_LEN))) {
        fprintf(stderr, "ERROR: failed to allocate memory for path!\n");
        free(toppath);
        toppath = NULL;
        return -1;
    }

    /* update all timeshare group cpuset value */
    for (i = TSCG_START_ID; i <= TSCG_END_ID; ++i) {
        int rc = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", toppath, cgutil_vaddr[i]->grpname);
        securec_check_intval(rc, free(toppath); free(relpath), -1);

        if (cgexec_update_cgroup_cpuset_value(relpath, cpuset) == -1) {
            fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
            goto error;
        }
    }

    /* if update is not 0, we can update timeshare group first, and then toppath group */
    if (update && cgexec_update_cgroup_cpuset_value(toppath, cpuset) == -1) {
        fprintf(stderr, "ERROR: failed to add %s controller in %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
        goto error;
    }

    free(toppath);
    toppath = NULL;
    free(relpath);
    relpath = NULL;

    return 0;

error:
    free(toppath);
    toppath = NULL;
    free(relpath);
    relpath = NULL;
    return -1;
}

/*
 * @Description: update group cpuset value.
 * @IN grp: group info
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_cgroup_cpuset(gscgroup_grp_t* grp, char* cpuset)
{
    char* relpath = NULL;

    if (strcmp(grp->cpuset, cpuset) == 0)
        return 0;

    /* get the relative path */
    if (NULL == (relpath = gscgroup_get_relative_path(grp->gid, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* update cgroup cpuset value with relative path */
    if (cgexec_update_cgroup_cpuset_value(relpath, cpuset) == -1) {
        fprintf(stderr, "ERROR: failed to update %s controller in %s!\n", MOUNT_CPUSET_NAME, grp->grpname);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    /* save new value as class cpuset value */
    errno_t sret = snprintf_s(grp->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpuset);
    securec_check_intval(sret, free(relpath), -1);

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * @Description: update 'topwd' group cpuset value.
 * @IN cls: class group id
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_topwd_cgroup_cpuset(int cls, char* cpuset)
{
    char* relpath = NULL;
    int i;
    char topwd[GPNAME_LEN];

    /* get 'topwd' cgroup full name*/
    errno_t rc = snprintf_s(topwd, sizeof(topwd), sizeof(topwd) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(rc, , -1);

    /* set the top wd item */
    for (i = WDCG_START_ID; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used && cgutil_vaddr[i]->ginfo.wd.cgid == cls &&
            strcmp(cgutil_vaddr[i]->grpname, topwd) == 0)
            break;
    }

    /* find 'topwd' group failed */
    if (i > WDCG_END_ID) {
        fprintf(stderr, "ERROR: Cannot find topwd for class: %s\n", cgutil_vaddr[i]->grpname);
        return -1;
    }

    /* get the relative path */
    if (NULL == (relpath = gscgroup_get_relative_path(cgutil_vaddr[i]->gid, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* update group cpuset value with relative path */
    if (cgexec_update_cgroup_cpuset_value(relpath, cpuset) == -1) {
        fprintf(stderr,
            "ERROR: failed to add %s controller in %s:%s!\n",
            MOUNT_CPUSET_NAME,
            cgutil_vaddr[cls]->grpname,
            topwd);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    /* save new value as class cpuset value */
    errno_t sret = snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpuset);
    securec_check_intval(sret, free(relpath);, -1);

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * @Description: check all workload group cpuset value for the class.
 * @IN cls: class group id
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_check_workload_cgroup_cpuset(int cls, const char* cpuset)
{
    int i;
    char topwd[GPNAME_LEN];

    /* get 'topwd' full name */
    errno_t rc = snprintf_s(topwd, sizeof(topwd), sizeof(topwd) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(rc, , -1);

    /* set all worload cpuset */
    for (i = WDCG_START_ID; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used && cgutil_vaddr[i]->ginfo.wd.cgid == cls &&
            strcmp(cgutil_vaddr[i]->grpname, topwd) != 0) {
            if (cgexec_check_cpuset_value(cpuset, cgutil_vaddr[i]->cpuset) < 0) {
                if (cgexec_update_cgroup_cpuset(cgutil_vaddr[i], cgutil_vaddr[cls]->cpuset) == -1)
                    return -1;
            }
        }
    }

    return 0;
}

/*
 * @Description: update all workload group cpuset value.
 * @IN grp: group info
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_all_workload_cgroup_cpuset(int cls, char* cpuset)
{
    int i;
    char topwd[GPNAME_LEN];

    /* get 'topwd' full name */
    errno_t rc = snprintf_s(topwd, sizeof(topwd), sizeof(topwd) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(rc, , -1);

    /* set all worload cpuset */
    for (i = WDCG_START_ID; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used && cgutil_vaddr[i]->ginfo.wd.cgid == cls &&
            strcmp(cgutil_vaddr[i]->grpname, topwd) != 0) {
            /* if the cpuset of upper levels groups alter larger, workload groups will be altered larger, too*/
            if (cgexec_update_cgroup_cpuset(cgutil_vaddr[i], cpuset) == -1)
                return -1;
        }
    }

    return 0;
}

/*
 * @Description: update remain group cpuset value.
 * @IN cls: class group id
 * @IN level: remain group level id
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_remain_cgroup_cpuset_value(int cls, int level, char* cpuset)
{
    char* relpath = NULL;
    char rempath[16];
    int j;
    errno_t sret;

    for (j = WDCG_START_ID; j <= WDCG_END_ID; ++j) {
        if (cgutil_vaddr[j]->used && cgutil_vaddr[j]->ginfo.wd.cgid == cls &&
            cgutil_vaddr[j]->ginfo.wd.wdlevel == level)
            break;
    }

    /* get the parent path of the workload group */
    relpath = gscgroup_get_parent_wdcg_path(j, cgutil_vaddr, current_nodegroup);
    if (NULL == relpath)
        return -1;

    /* get the remain group path */
    sret = snprintf_s(rempath,
        sizeof(rempath),
        sizeof(rempath) - 1,
        "%s:%d/",
        GSCGROUP_REMAIN_WORKLOAD,
        cgutil_vaddr[j]->ginfo.wd.wdlevel);
    securec_check_intval(sret, free(relpath), -1);

    sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
    securec_check_errno(sret, free(relpath), -1);

    /*update cgroup cpuset with relative path */
    if (cgexec_update_cgroup_cpuset_value(relpath, cpuset) == -1) {
        fprintf(stderr,
            "ERROR: failed to add %s controller in %s:%d!\n",
            MOUNT_CPUSET_NAME,
            GSCGROUP_REMAIN_WORKLOAD,
            cgutil_vaddr[j]->ginfo.wd.wdlevel);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    free(relpath);
    relpath = NULL;

    return 0;
}

/*
 * @Description: update remain group cpuset.
 * @IN cls: class group id
 * @IN cpuset: cpuset value
 * @IN update: update flag
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_remain_cgroup_cpuset(int cls, char* cpuset, unsigned char update)
{
    int i;

    if (update) {
        /* update the 'remain' group cpuset value from high level to low level */
        for (i = cgutil_vaddr[cls]->ginfo.cls.maxlevel; i >= 1; --i) {
            if (cgexec_update_remain_cgroup_cpuset_value(cls, i, cpuset) == -1) {
                fprintf(stderr,
                    "ERROR: failed to add %s controller in %s:%d, update: %d, cpuset: %s!\n",
                    MOUNT_CPUSET_NAME,
                    GSCGROUP_REMAIN_WORKLOAD,
                    i,
                    update,
                    cpuset);
                return -1;
            }
        }
    } else {
        /* update the 'remain' group cpuset value from low level to high level */
        for (i = 1; i <= cgutil_vaddr[cls]->ginfo.cls.maxlevel; ++i) {
            if (cgexec_update_remain_cgroup_cpuset_value(cls, i, cpuset) == -1) {
                fprintf(stderr,
                    "ERROR: failed to add %s controller in %s:%d, update: %d, cpuset: %s!\n",
                    MOUNT_CPUSET_NAME,
                    GSCGROUP_REMAIN_WORKLOAD,
                    i,
                    (int)update,
                    cpuset);
                return -1;
            }
        }
    }

    return 0;
}

/*
 * @Description: update 'class' group cpuset.
 * @IN cls: class group id
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_class_cpuset(int cls, char* cpuset)
{
    char largeset[CPUSET_LEN];

    (void)cgexec_check_workload_cgroup_cpuset(cls, cpuset);

    /* get large range with old cpuset and new cpuset */
    cgexec_get_large_cupset(cgutil_vaddr[cls]->cpuset, cpuset, largeset);

    /*
     * If we will set a group new cpuset value, we must make
     * sure the upper group has large range, we have to update
     * the group with large set first,
     * order: class -> remain -> timeshare
     * after that, we can update the group cpuset value as we wish.
     */
    if (strcmp(cgutil_vaddr[cls]->cpuset, largeset) != 0 &&
        (cgexec_update_cgroup_cpuset(cgutil_vaddr[cls], largeset) == -1 ||
            cgexec_update_remain_cgroup_cpuset(cls, largeset, 0) == -1 ||
            cgexec_update_timeshare_cpuset(cgutil_vaddr[cls], largeset, 0) == -1 ||
            cgexec_update_topwd_cgroup_cpuset(cls, cpuset) == -1)) {
        fprintf(stderr, "ERROR: failed to update cpuset for group in %s!\n", cgutil_vaddr[cls]->grpname);
        return -1;
    }

    /*
     * We set all workload group cpuset value with new value, it's
     * safe to update their value because the upper group has large
     * set value already, now we can update these upper group,
     * order: timeshare -> remain -> class
     */
    if (cgexec_update_all_workload_cgroup_cpuset(cls, cpuset) == -1 ||
        cgexec_update_timeshare_cpuset(cgutil_vaddr[cls], cpuset, 1) == -1 ||
        cgexec_update_remain_cgroup_cpuset(cls, cpuset, 1) == -1 ||
        cgexec_update_topwd_cgroup_cpuset(cls, cpuset) == -1 ||
        cgexec_update_cgroup_cpuset(cgutil_vaddr[cls], cpuset) == -1) {
        fprintf(stderr, "ERROR: failed to update cpuset for timeshare group in %s!\n", cgutil_vaddr[cls]->grpname);
        return -1;
    }

    return 0;
}

/*
 * @Description: update all class group cpuset value.
 * @IN grp: group info
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_all_class_cgroup_cpuset(char* cpuset)
{
    /* update all class group cpuset from default value to new cpuset */
    for (int i = CLASSCG_START_ID; i <= CLASSCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (cgexec_update_class_cpuset(i, cpuset) == -1)
            return -1;
    }

    return 0;
}

/*
 * @Description: update all backend group cpuset value.
 * @IN grp: group info
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_all_backend_cgroup_cpuset(char* cpuset)
{
    /* update all backend group cpuset from default value to new cpuset */
    for (int i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (cgexec_update_cgroup_cpuset(cgutil_vaddr[i], cpuset) == -1)
            return -1;
    }

    return 0;
}

/*
 * @Description: update top group cpuset top group, include: GAUSSDB, BACKEND, CLASS.
 * @IN top: top group id
 * @IN cpuset: cpuset value
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_update_top_group_cpuset(int top, char* cpuset)
{
    if (top == TOPCG_GAUSSDB) {
        char largeset[CPUSET_LEN];

        DIR* dir = NULL;
        struct dirent* de = NULL;

        char path[MAXPGPATH] = {0};
        char subpath[MAXPGPATH] = {0};
        struct stat statbuf;
        errno_t rc;
        int ret = -1;
        bool ummap_flag = false;

        rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
        securec_check_errno(rc, , -1);
        /* Update the default configuration file */
        cgexec_get_large_cupset(cgutil_vaddr[TOPCG_GAUSSDB]->cpuset, cpuset, largeset);

        if (cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_GAUSSDB], largeset) == -1 ||
            cgexec_update_top_group_cpuset(TOPCG_BACKEND, cpuset) == -1 ||
            cgexec_update_top_group_cpuset(TOPCG_CLASS, cpuset) == -1) {
            fprintf(stdout, "ERROR: update all cgroup cpuset failed.\n");
            return -1;
        }

        rc = snprintf_s(path,
            sizeof(path),
            sizeof(path) - 1,
            "%s/%s:%s",
            cgutil_opt.mpoints[0],
            GSCGROUP_TOP_DATABASE,
            cgutil_passwd_user->pw_name);
        securec_check_intval(rc, , -1);

        if (NULL == (dir = opendir(path)))
            return -1;

        while (NULL != (de = readdir(dir))) {
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                continue;

            rc = snprintf_s(subpath, sizeof(subpath), sizeof(subpath) - 1, "%s/%s", path, de->d_name);
            securec_check_intval(rc, (void)closedir(dir);, -1);

            /* check if it is directory */
            ret = stat(subpath, &statbuf);
            if (0 != ret || !S_ISDIR(statbuf.st_mode))
                continue;

            if (NULL != strstr(de->d_name, GSCGROUP_TOP_BACKEND))
                continue;

            if (NULL != strstr(de->d_name, GSCGROUP_TOP_CLASS))
                continue;

            if (cgutil_vaddr[0] != NULL) {
                (void)munmap(cgutil_vaddr[0], GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));
                ummap_flag = true;
            }

            rc = snprintf_s(
                cgutil_opt.nodegroup, sizeof(cgutil_opt.nodegroup), sizeof(cgutil_opt.nodegroup) - 1, "%s", de->d_name);
            securec_check_intval(rc, (void)closedir(dir);, -1);

            current_nodegroup = cgutil_opt.nodegroup;

            /* get the configuration infor of logical cluster */
            if (-1 == cgconf_parse_nodegroup_config_file()) {
                (void)closedir(dir);
                return -1;
            }

            /* update the cpuset of logical cluster */
            if (cgexec_update_top_group_cpuset(TOPCG_CLASS, cpuset) == -1) {
                fprintf(
                    stdout, "ERROR: update all cgroup cpuset of %s logical cluster failed.\n", cgutil_opt.nodegroup);
                (void)closedir(dir);
                return -1;
            }

            if (cgexec_reset_cpuset_cgroups(TOPCG_CLASS, 0) == -1) {
                fprintf(stdout, "ERROR: failed to reset cpuset of %s logical cluster.\n", cgutil_opt.nodegroup);
                (void)closedir(dir);
                return -1;
            }
        }

        (void)closedir(dir);

        /* reset nodegroup */
        if (ummap_flag == true) {
            *cgutil_opt.nodegroup = '\0';
            current_nodegroup = NULL;
            if (-1 == cgconf_parse_config_file()) {
                fprintf(stdout, "ERROR: failed to parse the default configuration file.\n");
                return -1;
            }
        }
        return cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_GAUSSDB], cpuset);
    }

    if (top == TOPCG_BACKEND) {
        char largeset[CPUSET_LEN];

        /* get large range with old cpuset and new cpuset */
        cgexec_get_large_cupset(cgutil_vaddr[TOPCG_BACKEND]->cpuset, cpuset, largeset);

        if (cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_BACKEND], largeset) == -1 ||
            cgexec_update_all_backend_cgroup_cpuset(cpuset) == -1) {
            fprintf(stdout, "ERROR: update Backend cpuset failed.\n");
            return -1;
        }

        /* update 'DEFAULT_BACKEND' and 'VACUUM' group cpuset, and then update 'BACKEND' to new cpuset value */
        return cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_BACKEND], cpuset);
    }

    if (top == TOPCG_CLASS) {
        char largeset[CPUSET_LEN];

        /* get large range with old cpuset and new cpuset */
        cgexec_get_large_cupset(cgutil_vaddr[TOPCG_CLASS]->cpuset, cpuset, largeset);

        if (cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_CLASS], largeset) == -1 ||
            cgexec_update_all_class_cgroup_cpuset(cpuset) == -1) {
            fprintf(stdout, "ERROR: update Class cpuset failed.\n");
            return -1;
        }

        /* update 'CLASS' group to new cpuset value */
        return cgexec_update_cgroup_cpuset(cgutil_vaddr[TOPCG_CLASS], cpuset);
    }

    return 0;
}

/*
 * function name: cgexec_update_dynamic_class_cgroup
 * description  : when the dynamic value of class cgroup or
 *                workload cgroup is update, it updates the class configuration
 *                and workload configuration corresponding.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_dynamic_class_cgroup(void)
{
    int i, cls = 0, wd = 0;
    int percent = 0;

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.clsname)) {
            cls = i;
            break;
        }
    }

    if (cls) {
        if (cgutil_opt.clspct && (cgutil_opt.clspct > cgutil_vaddr[cls]->ginfo.cls.percent)) {
            /* check the remain percentage */
            for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
                if (cgutil_vaddr[i]->used && (i != cls))
                    percent += cgutil_vaddr[i]->ginfo.cls.percent;
            }

            if ((GROUP_ALL_PERCENT - percent) < cgutil_opt.clspct) {
                fprintf(stderr,
                    "ERROR: there is no more resource for updated cgroup %s.\n"
                    "the remain percentage is %d.\n",
                    cgutil_opt.clsname,
                    GROUP_ALL_PERCENT - percent);
                return -1;
            }
        }

        if (cgutil_opt.clspct && cgutil_opt.clspct != cgutil_vaddr[cls]->ginfo.cls.percent) {
            /* set the cgutil_vaddr item */
            cgconf_update_class_group(cgutil_vaddr[cls]);

            if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[cls]))
                return -1;
        }

        if (cgutil_opt.wdname[0]) {
            wd = cgexec_search_workload_group(cls);

            if (wd && cgutil_opt.grppct) {
                if (cgutil_opt.grppct >= (cgutil_vaddr[cls]->ginfo.cls.rempct + cgutil_vaddr[wd]->ginfo.wd.percent)) {
                    fprintf(stderr,
                        "ERROR: there is no more resource for updated cgroup %s.\n"
                        "the remain percentage is %d.\n",
                        cgutil_opt.wdname,
                        cgutil_vaddr[cls]->ginfo.cls.rempct + cgutil_vaddr[wd]->ginfo.wd.percent);
                    return -1;
                }

                if (cgutil_opt.grppct != cgutil_vaddr[wd]->ginfo.wd.percent) {
                    cgconf_update_workload_group(cgutil_vaddr[wd]);

                    if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[wd]))
                        return -1;

                    if (-1 == cgexec_update_remain_cgroup(cgutil_vaddr[wd], cls))
                        return -1;
                }
            } else if (wd == 0) {
                fprintf(stderr, "ERROR: the specified workload group %s doesn't exist!\n", cgutil_opt.wdname);
                return -1;
            }
        }
    } else {
        fprintf(stderr, "ERROR: the specified class group %s doesn't exist!\n", cgutil_opt.clsname);
        return -1;
    }

    return 0;
}

/*
 * @Description: check dynamic backend percent.
 * @IN bkd: backend group id
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
static int cgexec_check_dynamic_backend_percent(int bkd)
{
    int i = 0;
    int percent = 0;

    if (cgutil_opt.bkdpct > 0 && cgutil_opt.bkdpct > cgutil_vaddr[bkd]->ginfo.cls.percent) {
        /* check the remain percentage */
        for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
            if (cgutil_vaddr[i]->used && (i != bkd))
                percent += cgutil_vaddr[i]->ginfo.cls.percent;
        }

        if ((GROUP_ALL_PERCENT - percent) < cgutil_opt.bkdpct) {
            fprintf(stderr,
                "ERROR: there is no more resource for updated cgroup %s.\n"
                "the remain percentage is %d.\n",
                cgutil_opt.bkdname,
                GROUP_ALL_PERCENT - percent);
            return -1;
        }
    }

    if (cgutil_opt.bkdpct > 0 && cgutil_opt.bkdpct != cgutil_vaddr[bkd]->ginfo.cls.percent) {
        /* set the cgutil_vaddr item */
        cgconf_update_backend_group(cgutil_vaddr[bkd]);

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[bkd]))
            return -1;
    }

    return 0;
}

/*
 * function name: cgexec_update_dynamic_backend_cgroup
 * description  : update the dynamic value of backend cgroup
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_dynamic_backend_cgroup(void)
{
    int i, bkd = 0;

    /* check if the class exists */
    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.bkdname)) {
            bkd = i;
            break;
        }
    }

    if (bkd) {
        if (cgexec_check_dynamic_backend_percent(bkd) == -1)
            return -1;
    } else {
        fprintf(stderr, "ERROR: the specified backend group %s doesn't exist!\n", cgutil_opt.bkdname);
        return -1;
    }

    return 0;
}

/*
 * function name: cgexec_update_top_group_percent
 * description  : update the dynamic value of Top Cgroup; it include
 *                Root Cgroup, Guassdb:user Cgroup, Class Cgroup
 *                and Backend Cgroup.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_top_group_percent(void)
{
    if (0 == strcmp(cgutil_opt.topname, GSCGROUP_ROOT)) {
        if (geteuid() != 0) {
            fprintf(stderr, "ERROR: non-root user can't modify the Root cgroup!\n");
            return -1;
        }

        if (cgutil_opt.toppct < 10) {
            cgutil_opt.toppct = 10;
        }

        if (cgutil_vaddr[TOPCG_ROOT]->ginfo.top.percent == cgutil_opt.toppct)
            return 0;

        cgutil_vaddr[TOPCG_ROOT]->ginfo.top.percent = cgutil_opt.toppct;

        cgutil_vaddr[TOPCG_ROOT]->ainfo.weight = MAX_IO_WEIGHT * cgutil_opt.toppct / GROUP_ALL_PERCENT;

        if ((cgutil_is_sles11_sp2 || cgexec_check_SLESSP2_version()) &&
            (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_ROOT])))
            return -1;
    } else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_DATABASE) ||
               0 == strcmp(cgutil_opt.topname, cgutil_vaddr[TOPCG_GAUSSDB]->grpname)) {
        if (cgutil_vaddr[TOPCG_GAUSSDB]->ginfo.top.percent == cgutil_opt.toppct)
            return 0;

        cgutil_vaddr[TOPCG_GAUSSDB]->ginfo.top.percent = cgutil_opt.toppct;
        cgutil_vaddr[TOPCG_GAUSSDB]->ainfo.shares =
            DEFAULT_CPU_SHARES * cgutil_opt.toppct / (GROUP_ALL_PERCENT - cgutil_opt.toppct);
        cgutil_vaddr[TOPCG_GAUSSDB]->percent =
            cgutil_vaddr[TOPCG_ROOT]->percent * cgutil_opt.toppct / GROUP_ALL_PERCENT;

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_GAUSSDB]))
            return -1;

        cgconf_update_top_percent();
    } else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_BACKEND)) {
        if (cgutil_vaddr[TOPCG_BACKEND]->ginfo.top.percent == cgutil_opt.toppct)
            return 0;

        cgutil_vaddr[TOPCG_BACKEND]->ginfo.top.percent = cgutil_opt.toppct;
        cgutil_vaddr[TOPCG_BACKEND]->ainfo.shares = DEFAULT_CPU_SHARES * cgutil_opt.toppct / 10;
        cgutil_vaddr[TOPCG_BACKEND]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, cgutil_opt.toppct);
        cgutil_vaddr[TOPCG_BACKEND]->percent =
            cgutil_vaddr[TOPCG_GAUSSDB]->percent * cgutil_opt.toppct / GROUP_ALL_PERCENT;

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_BACKEND]))
            return -1;

        cgconf_update_backend_percent();

        cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent = GROUP_ALL_PERCENT - cgutil_opt.toppct;
        cgutil_vaddr[TOPCG_CLASS]->ainfo.shares = DEFAULT_CPU_SHARES * (GROUP_ALL_PERCENT - cgutil_opt.toppct) / 10;
        cgutil_vaddr[TOPCG_CLASS]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, GROUP_ALL_PERCENT - cgutil_opt.toppct);
        cgutil_vaddr[TOPCG_CLASS]->percent =
            cgutil_vaddr[TOPCG_GAUSSDB]->percent * (GROUP_ALL_PERCENT - cgutil_opt.toppct) / GROUP_ALL_PERCENT;

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_CLASS]))
            return -1;

        cgconf_update_class_percent();
    } else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_CLASS)) {
        if (cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent == cgutil_opt.toppct)
            return 0;

        cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent = cgutil_opt.toppct;
        cgutil_vaddr[TOPCG_CLASS]->ainfo.shares = DEFAULT_CPU_SHARES * cgutil_opt.toppct / 10;
        cgutil_vaddr[TOPCG_CLASS]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, cgutil_opt.toppct);
        cgutil_vaddr[TOPCG_CLASS]->percent =
            cgutil_vaddr[TOPCG_GAUSSDB]->percent * cgutil_opt.toppct / GROUP_ALL_PERCENT;

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_CLASS]))
            return -1;

        cgconf_update_class_percent();

        cgutil_vaddr[TOPCG_BACKEND]->ginfo.top.percent = GROUP_ALL_PERCENT - cgutil_opt.toppct;
        cgutil_vaddr[TOPCG_BACKEND]->ainfo.shares = DEFAULT_CPU_SHARES * (GROUP_ALL_PERCENT - cgutil_opt.toppct) / 10;
        cgutil_vaddr[TOPCG_BACKEND]->ainfo.weight =
            IO_WEIGHT_CALC(MAX_IO_WEIGHT, GROUP_ALL_PERCENT - cgutil_opt.toppct);
        cgutil_vaddr[TOPCG_BACKEND]->percent =
            cgutil_vaddr[TOPCG_GAUSSDB]->percent * (GROUP_ALL_PERCENT - cgutil_opt.toppct) / GROUP_ALL_PERCENT;

        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[TOPCG_BACKEND]))
            return -1;

        cgconf_update_backend_percent();
    } else {
        fprintf(stderr, "ERROR: the specified top group %s doesn't exist!\n", cgutil_opt.topname);
        return -1;
    }

    return 0;
}
/*
 * function name: cgexec_update_top_group_cpuset_userset
 * description  : update top level cpuset by user set "-f"
 * @IN topname  : top group name to be updated.
 * @IN cpuset   : user set cpuset to be updated.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_update_top_group_cpuset_userset(const char* topname, char* cpuset)
{
    int topstart = 0;
    int topend = 0;
    int toplength = 0;

    int rcs = sscanf_s(cpuset, "%d-%d", &topstart, &topend);
    if (rcs != 2) {
        fprintf(stderr,
            "%s:%d failed on calling "
            "security function.\n",
            __FILE__,
            __LINE__);
        return -1;
    }

    toplength = topend - topstart + 1;

    /* we cannot changed the root group */
    if (strcmp(topname, GSCGROUP_ROOT) == 0) {
        fprintf(stdout, "ERROR: cpuset of Root can not be changed.\n");
        return -1;
    }
    if (strcmp(topname, GSCGROUP_TOP_DATABASE) == 0 || strcmp(topname, cgutil_vaddr[TOPCG_GAUSSDB]->grpname) == 0) {
        /*
         * when updating cpuset for top classes,
         * we need update all the belonging lower level groups by percentage.
         */
        if (cgexec_update_top_group_cpuset(TOPCG_GAUSSDB, cpuset) == -1 ||
            cgexec_reset_cpuset_cgroups(TOPCG_GAUSSDB, 0) == -1)
            return -1;
        /*
         * each time we use "-f" to set cpuset,
         * we need reset quota to let this group not be influenced next time
         * when we set cpuset percentage.
         */
        cgutil_vaddr[TOPCG_GAUSSDB]->ainfo.quota = 0;
    }

    return 0;
}
/*
 * function name: cgexec_update_dynamic_top_cgroup
 * description  : update the dynamic value of Top Cgroup; it include
 *                Root Cgroup, Guassdb:user Cgroup, Class Cgroup
 *                and Backend Cgroup.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_dynamic_top_cgroup(void)
{
    if (cgutil_opt.toppct > 0 && cgexec_update_top_group_percent() == -1) {
        return -1;
    }

    if (*cgutil_opt.sets)
        return cgexec_update_top_group_cpuset_userset(cgutil_opt.topname, cgutil_opt.sets);

    return 0;
}

/*
 * function name: cgexec_update_fixed_class_cgroup
 * description  : update the cpuset value of Class group and Workload group by user set "--fixed"
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_fixed_class_cgroup(void)
{
    int i, cls = 0, wd = 0;
    char cpusets[CPUSET_LEN];
    int need_reset = 0;

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.clsname)) {
            cls = i;
            break;
        }
    }

    if (cls) {
        if (cgutil_opt.wdname[0]) {
            wd = cgexec_search_workload_group(cls);

            if (wd) {
                if (cgutil_opt.setspct) {
                    /*
                     * step 1 check whether the newly set percentage makes the whole percentage higher than 100%.
                     * result:
                     * setslength = -1:higher than 100%.
                     * setslength = 0: cpusets have been set well, go to step 3.
                     * setslength > 0: cpusets is empty yet and need reset, go to step 2.
                     */
                    if ((need_reset = cgexec_check_cpuset_percent(cls, wd, cpusets)) == -1)
                        return -1;

                    /*step 2: defragment the other workload groups.  */
                    if (need_reset > 0 && (cgexec_reset_cpuset_cgroups(cls, wd) == -1))
                        return -1;

                    /* step 3: update the workload group. */
                    if (cgexec_update_cgroup_cpuset(cgutil_vaddr[wd], cpusets) == -1)
                        return -1;

                    cgutil_vaddr[wd]->ainfo.quota = cgutil_opt.setspct;

                    return 0;
                } else if (cgutil_opt.setfixed) {
                    if (cgexec_update_cgroup_cpuset(cgutil_vaddr[wd], cgutil_vaddr[cls]->cpuset) == -1)
                        return -1;

                    cgutil_vaddr[wd]->ainfo.quota = 0;

                    return 0;
                }
            } else {
                fprintf(stderr, "ERROR: the specified workload group %s doesn't exist!\n", cgutil_opt.wdname);
                return -1;
            }
        }
        if (cgutil_opt.setspct) {
            /*
             * step 1: check whether the newly set percentage of class is legal or not.
             * return value = -1: illegal
             * return value = 0: legal and no need to do defragment.
             * return value > 0: need defragment
             */
            if ((need_reset = cgexec_check_cpuset_percent(TOPCG_CLASS, cls, cpusets)) == -1)
                return -1;

            /* step 2: degragment the other class groups, reset their belonging workload groups by percentage. */
            if (need_reset > 0 && (cgexec_reset_cpuset_cgroups(TOPCG_CLASS, cls) == -1))
                return -1;

            /*step 3: update the class group. */
            if (cgexec_update_class_cpuset(cls, cpusets) == -1) {
                fprintf(stderr, "ERROR: update cpuset for class \"%s\" failed.\n", cgutil_vaddr[cls]->grpname);
                return -1;
            }

            /*
             * step 4: update the belonging workload groups.
             * 0 means no group need be ignored in the reseting list
             */
            if (cgexec_reset_cpuset_cgroups(cls, 0) == -1) {
                fprintf(stderr, "ERROR: reset workload cpuset for class \"%s\" failed.\n", cgutil_vaddr[cls]->grpname);
                return -1;
            }
            cgutil_vaddr[cls]->ainfo.quota = cgutil_opt.setspct;

        } else if (cgutil_opt.setfixed) {
            if (cgexec_update_class_cpuset(cls, cgutil_vaddr[TOPCG_CLASS]->cpuset) == -1)
                return -1;

            if (cgexec_reset_cpuset_cgroups(cls, 0) == -1) {
                fprintf(stderr, "ERROR: reset workload cpuset for class \"%s\" failed.\n", cgutil_vaddr[cls]->grpname);
                return -1;
            }

            cgutil_vaddr[cls]->ainfo.quota = 0;
        }
    } else {
        fprintf(stderr, "ERROR: the specified class group %s doesn't exist!\n", cgutil_opt.clsname);
        return -1;
    }

    return 0;
}

/*
 * function name: cgexec_update_fixed_backend_cgroup
 * description  : update the cpuset value of Backend group by user set "--fixed"
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_fixed_backend_cgroup(void)
{
    int i, bkd = 0;
    char cpuset[CPUSET_LEN];
    int need_reset = 0;

    /* check if the class exists */
    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.bkdname)) {
            bkd = i;
            break;
        }
    }

    if (bkd) {
        /* set cpuset by percentage*/
        if (cgutil_opt.setspct) {
            /* the same steps with updating workload groups.*/
            if ((need_reset = cgexec_check_cpuset_percent(TOPCG_BACKEND, bkd, cpuset)) == -1)
                return -1;

            if (need_reset > 0 && (cgexec_reset_cpuset_cgroups(TOPCG_BACKEND, bkd) == -1))
                return -1;

            if (cgexec_update_cgroup_cpuset(cgutil_vaddr[bkd], cpuset) == -1)
                return -1;

            cgutil_vaddr[bkd]->ainfo.quota = cgutil_opt.setspct;
        } else if (cgutil_opt.setfixed) {
            if (cgexec_update_cgroup_cpuset(cgutil_vaddr[bkd], cgutil_vaddr[TOPCG_BACKEND]->cpuset) == -1)
                return -1;

            cgutil_vaddr[bkd]->ainfo.quota = 0;
        }
    } else {
        fprintf(stderr, "ERROR: the specified backend group %s doesn't exist!\n", cgutil_opt.bkdname);
        return -1;
    }

    return 0;
}

/*
 * function name: cgexec_update_fixed_top_cgroup
 * description  : update cpuset of Top group by user set "--fixed"
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_update_fixed_top_cgroup(void)
{
    char cpusets[CPUSET_LEN];
    int need_reset = 0;
    int top = 0;

    if (0 == strcmp(cgutil_opt.topname, GSCGROUP_ROOT)) {
        fprintf(stderr, "ERROR: users can't modify the Root cgroup with \"--fixed\"!\n");
        return -1;
    } else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_DATABASE) ||
               0 == strcmp(cgutil_opt.topname, cgutil_vaddr[TOPCG_GAUSSDB]->grpname)) {
        top = TOPCG_GAUSSDB;
        if (cgutil_opt.setspct) {
            fprintf(stderr, "ERROR: users can't modify the cpu cores percentage of Gaussdb cgroup with \"--fixed\"!\n");
            return -1;
        }
    } else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_BACKEND))
        top = TOPCG_BACKEND;
    else if (0 == strcmp(cgutil_opt.topname, GSCGROUP_TOP_CLASS))
        top = TOPCG_CLASS;
    else {
        fprintf(stderr, "ERROR: the specified top group %s doesn't exist!\n", cgutil_opt.topname);
        return -1;
    }

    if (top) {
        if (cgutil_opt.setspct) {
            if ((need_reset = cgexec_check_cpuset_percent(TOPCG_GAUSSDB, top, cpusets)) == -1)
                return -1;

            if (need_reset && (cgexec_reset_cpuset_cgroups(TOPCG_GAUSSDB, top) == -1))
                return -1;

            if (cgexec_update_top_group_cpuset(top, cpusets) == -1 || cgexec_reset_cpuset_cgroups(top, 0) == -1)
                return -1;

            cgutil_vaddr[top]->ainfo.quota = cgutil_opt.setspct;
        } else if (cgutil_opt.setfixed) {
            if (cgexec_update_top_group_cpuset(top, cgutil_vaddr[TOPCG_GAUSSDB]->cpuset) == -1 ||
                cgexec_reset_cpuset_cgroups(top, 0) == -1)
                return -1;

            cgutil_vaddr[top]->ainfo.quota = 0;
        }
    }

    return 0;
}

/*
 **************** EXTERNAL FUNCTION ********************************
 */

/*
 * function name: cgexec_check_SLESSP2_version
 * description  : check if the current OS version is SLES SP2
 * return value :
 *             1: is the sles sp2
 *             0: is not the sles sp2, supposed as sles sp1
 *            -1: abnormal
 *
 * Note: Search "io" column in /proc/cgroups.
 *       It need to check the value if the next release supports
 *       Redhat or Euler version.
 */
int cgexec_check_SLESSP2_version(void)
{
    char buf[PROCLINE_LEN];
    FILE* f = NULL;

    f = fopen("/proc/cgroups", "r");

    if (f == NULL)
        return -1;

    while (NULL != fgets(buf, PROCLINE_LEN, f)) {
        /* example from proc:
         * #subsys_name hierarchy   num_cgroups enabled
         * cpu  0   1   1
         *
         * search "blkio" column
         */

        if (strstr(buf, MOUNT_BLKIO_NAME) != NULL) {
            cgutil_is_sles11_sp2 = 1;
            fclose(f);
            return 1;
        }
    }

    fclose(f);
    return 0;
}

/*
 * @Description: check whether execute upgrade.
 * @IN void
 * @Return:  1: upgrade 0: not upgrade
 * @See also:
 */
int cgexec_check_mount_for_upgrade(void)
{
    int i, ret, old_mp = 0;

    errno_t sret;

    /* Only root user can do upgrade */
    if (geteuid() != 0)
        return 0;

    /*
     * if cpuset and cpuacct has not mounted, we will check whether
     * cpu or blkio is mounted on default point, if yes, we must unmount
     * them firstly, and then mount all sub system with new mount point.
     * if no system in default point, we need not umount them.
     */
    for (i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        /* ignore blkio and memory. */
        if (i == MOUNT_BLKIO_ID || i == MOUNT_MEMORY_ID)
            continue;

        if (*cgutil_opt.mpoints[i]) {
            if (strcmp(cgutil_opt.mpoints[i], GSCGROUP_MOUNT_POINT_OLD) == 0) {
                char fname[256];
                int cnt = 0;
                struct dirent* file = NULL;
                DIR* dir = opendir(GSCGROUP_MOUNT_POINT_OLD);

                if (dir == NULL) {
                    fprintf(stderr, "ERROR: failed to open %s.\n", GSCGROUP_MOUNT_POINT_OLD);
                    return -1;
                }

                sret = snprintf_s(
                    fname, sizeof(fname), sizeof(fname) - 1, "%s:%s", "Gaussdb", cgutil_passwd_user->pw_name);
                securec_check_intval(sret, closedir(dir), -1);

                /* if other user has created cgroup in the default mout point, we cannot unmount the point. */
                while ((file = readdir(dir)) != NULL) {
                    if (file->d_type != DT_DIR || strcmp(file->d_name, fname) == 0)
                        continue;

                    if (file->d_type == DT_DIR && strncmp(file->d_name, "Gaussdb:", 8) == 0)
                        ++cnt;
                }

                closedir(dir);

                if (cnt > 0) {
                    fprintf(stderr,
                        "ERROR: The other user has cgroups in \"%s\", upgrade failed.\n",
                        GSCGROUP_MOUNT_POINT_OLD);
                    return -1;
                }

                old_mp++;
            }
        }
    }

    /* if cgroups are mounted on old path, only umount once time */
    if (old_mp) {
        char cmd[128];

        /* more than one cgroups have been mounted under /dev/cgroups */
        if (old_mp > 1) {
            (void)cgroup_init(); /* init first */
            (void)cgptree_drop_cgroups();
        }

        sret = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "umount %s", GSCGROUP_MOUNT_POINT_OLD);
        securec_check_intval(sret, , -1);

        ret = system(cmd);
        if (CheckSystemSucess(ret) == -1) {
            fprintf(stderr, "ERROR: failed to umount cgroup under %s!\n", GSCGROUP_MOUNT_POINT_OLD);
            return -1;
        }

        /* get new mount points and mount them */
        (void)cgexec_get_mount_points();
        (void)cgexec_mount_root_cgroup();
    } else
        cgutil_opt.upgrade = 0;

    return 0;
}

/*
 * @Description: get all cgroup sub system's mount points.
 * @IN void
 * @Return:  0: normal -1: abnormal
 * @See also:
 */
int cgexec_get_mount_points(void)
{
    struct mntent* ent = NULL;
    char mntent_buffer[5 * FILENAME_MAX];

    struct mntent temp_ent;
    int i;

    errno_t rc;
    rc = memset_s(&temp_ent, sizeof(temp_ent), 0, sizeof(temp_ent));
    securec_check_errno(rc, , -1);

    /* reset mount points */
    rc = memset_s(cgutil_opt.mpoints, MOUNT_SUBSYS_KINDS * MAXPGPATH, 0, MOUNT_SUBSYS_KINDS * MAXPGPATH);
    securec_check_errno(rc, , -1);

    /* open '/proc/mounts' to load mount points */
    FILE* proc_mount = fopen("/proc/mounts", "re");

    if (proc_mount == NULL)
        return -1;

    while ((ent = getmntent_r(proc_mount, &temp_ent, mntent_buffer, sizeof(mntent_buffer))) != NULL) {
        /* not cgroup, pass */
        if (strcmp(ent->mnt_type, "cgroup") != 0)
            continue;

        for (i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
            if (hasmntopt(ent, cgutil_subsys_table[i]) == NULL)
                continue;

            /* get mount point */
            rc = snprintf_s(cgutil_opt.mpoints[i],
                sizeof(cgutil_opt.mpoints[i]),
                sizeof(cgutil_opt.mpoints[i]) - 1,
                "%s",
                ent->mnt_dir);
            securec_check_intval(rc, fclose(proc_mount), -1);
        }
    }

    fclose(proc_mount);

    return 0;
}

/*
 * @Description: detect if cgroup file system has been mounted.
 * @IN void
 * @Return:   1: has been mounted on the specified directory
 *            0: hasn't been mounted
 *           -1: has been mounted on other directory
 * @See also:
 */
int cgexec_detect_cgroup_mount(void)
{
    int i, j;

    if (cgutil_opt.cflag <= 0) {
        return 1;
    }

    for (i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        if (i == MOUNT_BLKIO_ID)
            continue;

        /* a subsys has not mounted, we must make sure its mount point is valid. */
        if (*cgutil_opt.mpoints[i] == '\0') {
            if (cgutil_opt.mpflag == 0) {
                /* no new mount point, make sure default point is valid */
                for (j = 0; j < MOUNT_SUBSYS_KINDS; ++j)
                    if (strcmp(cgutil_opt.mpoints[j], GSCGROUP_MOUNT_POINT) == 0)
                        return -1;
            }

            return 0;
        }
    }

    return 1;
}

static int RemoveExistSymbolLink(const char* mpoint)
{
    int ret;
    char cmd[MAX_COMMAND_LENGTH];
    struct stat statbuf;
    errno_t rc;
    
    rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
    securec_check_c(rc, "\0", "\0");
    /* If the mount point is already exist, directory should be remove */                
    ret = lstat(mpoint, &statbuf);
    if (S_ISLNK(statbuf.st_mode)) {
        rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "rm %s", mpoint);
        securec_check_ss_c(rc, "\0", "\0");

        ret = system(cmd);
        if (CheckSystemSucess(ret) == -1) {
            fprintf(stderr, "ERROR: failed to remove exist symbol link %s!\n", mpoint);
            return -1;
        }
    }
    return 0;
}

static int MountCgroupInternal(const char* mpoint, const char* type)
{
    int ret;
    char cmd[MAX_COMMAND_LENGTH];
    struct stat statbuf;
    errno_t rc;
    
    rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
    securec_check_c(rc, "\0", "\0");
    /* check new mount point directory */
    ret = stat(mpoint, &statbuf);
    if (ret != 0 || !S_ISDIR(statbuf.st_mode)) {
        if (mkdir(mpoint, S_IRWXU) != 0) {
            fprintf(stderr, "ERROR: failed to create %s directory!\n", mpoint);
            return -1;
        }
    }

    rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1,
        "mount -t cgroup -o %s %s %s", type, type, mpoint);
    securec_check_ss_c(rc, "\0", "\0");
    ret = system(cmd);
    if (CheckSystemSucess(ret) == -1) {
        fprintf(stderr, "ERROR: failed to mount cgroup under %s!\n", mpoint);
        return -1;
    }
    fprintf(stderr, "LOG: mount %s success.\n", type);
    return 0;
}

static int LinkCpuCgroup(const char* target, const char* source)
{
    int ret;
    char cmd[MAX_COMMAND_LENGTH];
    errno_t rc;

    rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "ln -s %s %s", source, target);
    securec_check_ss_c(rc, "\0", "\0");

    ret = system(cmd);
    if (CheckSystemSucess(ret) == -1) {
        fprintf(stderr, "ERROR: failed to mount cgroup under %s!\n", target);
        return -1;
    }

    return 0;
}

static int CgexecRemountCpuCgroup(const char* path, const char* tmp_mpoint)
{
    int ret;
    char mpoint[MOUNT_POINT_LENGTH];
    errno_t rc;

    rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, 
        "%s/cpu", path);
    securec_check_ss_c(rc, "\0", "\0");
    ret = RemoveExistSymbolLink(mpoint);
    if (ret != 0) {
        return ret;
    }
    ret = LinkCpuCgroup(mpoint, tmp_mpoint);
    if (ret != 0) {
        return ret;
    }
    rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, 
        "%s/cpuacct", path);
    securec_check_ss_c(rc, "\0", "\0");
    ret = RemoveExistSymbolLink(mpoint);
    if (ret != 0) {
        return ret;
    }
    ret = LinkCpuCgroup(mpoint, tmp_mpoint);

    return ret;
}

static int CgexecMountCpuCgroup(const char* path)
{
    int ret;
    char tmp_mpoint[MOUNT_POINT_LENGTH];
    char mpoint[MOUNT_POINT_LENGTH];
    errno_t rc;

    /* cpu and cpuacct sub-system should mount cpu,cpuacct sub-system */
    rc = snprintf_s(tmp_mpoint, sizeof(tmp_mpoint), sizeof(tmp_mpoint) - 1, 
        "%s/cpu,cpuacct", path);
    securec_check_ss_c(rc, "\0", "\0");
    ret = MountCgroupInternal(tmp_mpoint, "cpu,cpuacct");
    if (ret != 0) {
        /* mount failed means that cpu and cpuacct not mount together */
        rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, 
            "%s/cpu", path);
        securec_check_ss_c(rc, "\0", "\0");
        ret = MountCgroupInternal(mpoint, cgutil_subsys_table[MOUNT_CPU_ID]);
        if (ret != 0) {
            return ret;
        }
        rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, 
            "%s/cpuacct", path);
        securec_check_ss_c(rc, "\0", "\0");
        ret = MountCgroupInternal(mpoint, cgutil_subsys_table[MOUNT_CPUACCT_ID]);
    } else {
        ret = CgexecRemountCpuCgroup(path, tmp_mpoint);
    }

    return ret;
}

/*
 * @Description: mount the Cgroup file system on the Root directory.
 * @IN void
 * @Return:   -1: abnormal 0: normal
 * @See also:
 */
int cgexec_mount_root_cgroup(void)
{
    int i, ret;

    char mpoint[MOUNT_POINT_LENGTH];
    char* path = NULL;
    struct stat statbuf;

    errno_t rc;
    rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
    securec_check_c(rc, "\0", "\0");

    if (cgutil_opt.mpflag)
        path = cgutil_opt.mpoint;
    else
        path = GSCGROUP_MOUNT_POINT;

    if (CheckBackendEnv(path) != 0) {
        return -1;
    }
    /* Create mount point directory */
    ret = stat(path, &statbuf);
    if (0 != ret || !S_ISDIR(statbuf.st_mode)) {
        if (mkdir(path, S_IRWXU | (S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) != 0) {
            fprintf(stderr, "ERROR: failed to create %s directory!\n", path);
            return -1;
        }
        /* change the right to 755 */
        (void)chmod(path, S_IRWXU | (S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH));
    }

    for (i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        /* 'blkio' is invalid, ignore it */
        if (i == MOUNT_BLKIO_ID)
            continue;

        /* If the subsys has not mounted, we will use new point to mount. */
        if (*cgutil_opt.mpoints[i] == '\0') {
            /* cpu and cpuacct sub-system all mount on cpu,cpuacct in new linux system */
            if (i == MOUNT_CPU_ID) {
                ret = CgexecMountCpuCgroup(path);
                i++;
            } else {
                rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, "%s/%s", path, cgutil_subsys_table[i]);
                securec_check_ss_c(rc, "\0", "\0");
                ret = MountCgroupInternal(mpoint, cgutil_subsys_table[i]);
            }
        }
    }

    return 0;
}

static int CgexecUmountRootCgroupInternal(const char* path, int index)
{
    int ret;
    char cmd[MAX_COMMAND_LENGTH], mpoint[MOUNT_POINT_LENGTH];
    errno_t rc;

    /* get mount point full name */
    rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, "%s/%s", path, cgutil_subsys_table[index]);
    securec_check_ss_c(rc, "\0", "\0");

    /* we will unmount the point which you specify. */
    if (strcmp(cgutil_opt.mpoints[index], mpoint) == 0) {
        rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "umount %s", mpoint);
        securec_check_ss_c(rc, "\0", "\0");

        ret = system(cmd);
        if (CheckSystemSucess(ret) == -1) {
            fprintf(stderr, "ERROR: failed to umount cgroup under %s!\n", mpoint);
            return -1;
        }
        fprintf(stderr, "LOG: umount cgroup under %s!\n", mpoint);
    } else if (index == MOUNT_CPU_ID || index == MOUNT_CPUACCT_ID) {
        /* check new mount point directory */
        RemoveExistSymbolLink(mpoint);

        if (index == MOUNT_CPU_ID) {
            rc = snprintf_s(mpoint, sizeof(mpoint), sizeof(mpoint) - 1, "%s/cpu,cpuacct", path);
            securec_check_ss_c(rc, "\0", "\0");
            if (*cgutil_opt.mpoints[index] && strcmp(cgutil_opt.mpoints[index], mpoint) == 0) {
                rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "umount %s", mpoint);
                securec_check_ss_c(rc, "\0", "\0");

                ret = system(cmd);
                if (CheckSystemSucess(ret) == -1) {
                    fprintf(stderr, "ERROR: failed to umount cgroup under %s!\n", mpoint);
                    return -1;
                }
            }
        }
    }

    return 0;
}

/*
 * @Description: umount the Cgroup file system.
 * @IN void
 * @Return:   -1: abnormal 0: normal
 * @See also:
 */
int cgexec_umount_root_cgroup(void)
{
    int i, ret;
    char cmd[MAX_COMMAND_LENGTH];
    char* path = NULL;
    errno_t rc;

    if (cgutil_opt.mpflag)
        path = cgutil_opt.mpoint;
    else
        path = GSCGROUP_MOUNT_POINT;

    if (CheckBackendEnv(path) != 0) {
        return -1;
    }
    for (i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        /* 'blkio' is invalid, ignore it */
        if (i == MOUNT_BLKIO_ID)
            continue;

        if (*cgutil_opt.mpoints[i] == '\0') {
            continue;
        }

        /* It has mounted on old default point, we unmount it only once. */
        if (strcmp(cgutil_opt.mpoints[i], GSCGROUP_MOUNT_POINT_OLD) == 0) {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "umount %s", GSCGROUP_MOUNT_POINT_OLD);
            securec_check_ss_c(rc, "\0", "\0");

            ret = system(cmd);
            if (CheckSystemSucess(ret) == -1) {
                fprintf(stderr, "ERROR: failed to umount cgroup under %s!\n", GSCGROUP_MOUNT_POINT_OLD);
                return -1;
            }

            break;
        }

        ret = CgexecUmountRootCgroupInternal(path, i);
    }

    return 0;
}

/*
 * function name: cgexec_delete_cgroups
 * description  : delete the Cgroup based on relative path
 * arguments    :
 *       relpath: the relative path
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 * Note: the function is used when dropping a Cgroup.
 */
int cgexec_delete_cgroups(char* relpath)
{
    int ret;
    struct cgroup* cg = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", relpath);
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(
            stdout, "ERROR: failed to get '%s' cgroup information for %s(%d)\n", relpath, cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return -1;
    }

    (void)cgroup_delete_cgroup_ext(cg, CGFLAG_DELETE_RECURSIVE | CGFLAG_DELETE_IGNORE_MIGRATION);

    cgroup_free(&cg);

    return 0;
}

/*
 * function name: cgexec_create_groups
 * description  : main entry of create Cgroup;
 *                When the user is root, it needs to check if Cgroups have
 *                been created. If it didn't, the default Cgroups will be created.
 *                When the user is non-root user and the Cgroups didn't exist,
 *                it needs to report an error.
 *                Only Class and Workload Cgroup can be created, but it doesn't
 *                allow to create workload Cgroup for DefaultClass Cgroup.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_create_groups(void)
{
    int cgcnt;
    struct stat buf;
    int ret;
    size_t len = sizeof(cgutil_opt.mpoint) + 1 + sizeof(GSCGROUP_TOP_DATABASE) + 1 + USERNAME_LEN + 1 +
                 sizeof(GSCGROUP_TOP_CLASS);
    char* cgpath = (char*)malloc(len);
    errno_t sret;

    if (cgpath == NULL)
        return -1;

    sret = memset_s(&buf, sizeof(buf), 0, sizeof(buf));
    securec_check_errno(sret, free(cgpath), -1);

    if (geteuid() == 0) {
        /* create cm cgroup, this function could check if directory exists.
         * So we need not check it firstly.
         */
        (void)cgexec_create_cm_default_cgroup();

        cgcnt = cgexec_get_cgroup_number();
        if (1 == cgcnt) {
            /* create the default cgroups */
            (void)cgexec_create_default_cgroups();
        } else {
            /* check if cgroups have been created for the specified user */
            sret = sprintf_s(cgpath,
                len,
                "%s/%s:%s/%s",
                cgutil_opt.mpoints[MOUNT_CPU_ID],
                GSCGROUP_TOP_DATABASE,
                cgutil_opt.user,
                GSCGROUP_TOP_CLASS);
            securec_check_intval(sret, free(cgpath), -1);

            ret = stat(cgpath, &buf);

            if (0 != ret)
                (void)cgexec_create_default_cgroups();
        }

        /* default class has no exception data, we will set a default one */
        if (gsutil_exception_is_valid(cgutil_vaddr[CLASSCG_START_ID], EXCEPT_ALL_KINDS) == 0) {
            cgutil_vaddr[CLASSCG_START_ID]->except[EXCEPT_PENALTY].skewpercent = DEFAULT_CPUSKEWPCT;
            cgutil_vaddr[CLASSCG_START_ID]->except[EXCEPT_PENALTY].qualitime = DEFAULT_QUALITIME;
        }
    } else {
        /* check if cgroups have been created for the specified user */
        sret = sprintf_s(cgpath,
            len,
            "%s/%s:%s/%s",
            cgutil_opt.mpoints[MOUNT_CPU_ID],
            GSCGROUP_TOP_DATABASE,
            cgutil_passwd_user->pw_name,
            GSCGROUP_TOP_CLASS);
        securec_check_intval(sret, free(cgpath), -1);

        ret = stat(cgpath, &buf);

        if (0 != ret) {
            fprintf(stderr,
                "ERROR: There are no cgroups for %s! Please remount it by root.\n",
                cgutil_passwd_user->pw_name);
            free(cgpath);
            cgpath = NULL;
            return -1;
        }
    }

    /* create nodegroup info */
    if (cgutil_opt.nodegroup[0]) {
        size_t nglen = sizeof(cgutil_opt.mpoint) + 1 + sizeof(GSCGROUP_TOP_DATABASE) + 1 + USERNAME_LEN + 1 +
                       strlen(cgutil_opt.nodegroup);
        char* ngcgpath = (char*)malloc(nglen);

        if (ngcgpath == NULL) {
            free(cgpath);
            cgpath = NULL;
            return -1;
        }

        /* check if cgroups have been created for the specified nodegroup  */
        sret = sprintf_s(ngcgpath,
            nglen,
            "%s/%s:%s/%s",
            cgutil_opt.mpoints[MOUNT_CPU_ID],
            GSCGROUP_TOP_DATABASE,
            cgutil_passwd_user->pw_name,
            cgutil_opt.nodegroup);
        securec_check_intval(sret, free(cgpath); free(ngcgpath), -1);

        ret = stat(ngcgpath, &buf);
        /* if the nodegroup doesn't exist, create the default cgroups */
        if (0 != ret) {
            /* rename the origin cgroup into nodegroup cgroup */
            if (cgutil_opt.rename) {
                /* delete old cgroups */
                (void)cgptree_drop_nodegroup_cgroups(GSCGROUP_TOP_CLASS);
            }

            /* create nodegroup default cgroups by mapping info */
            if (-1 == cgexec_create_nodegroup_default_cgroups()) {
                free(ngcgpath);
                ngcgpath = NULL;
                free(cgpath);
                cgpath = NULL;
                return -1;
            }

            if (cgutil_opt.rename) {
                void* vaddr = cgconf_map_nodegroup_conffile();
                size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);

                if (NULL == vaddr) {
                    fprintf(stderr, "ERROR: node group config file is removed during rename!");
                    free(ngcgpath);
                    ngcgpath = NULL;
                    free(cgpath);
                    cgpath = NULL;
                    return -1;
                }

                for (int i = 0; i < CLASSCG_START_ID; i++) {
                    gscgroup_grp_t* tmpvaddr = (gscgroup_grp_t*)vaddr + i;
                    sret = memcpy_s(cgutil_vaddr[i], sizeof(gscgroup_grp_t), tmpvaddr, sizeof(gscgroup_grp_t));
                    securec_check_errno(sret, (void)munmap(vaddr, cglen); free(cgpath); free(ngcgpath);, -1);
                }

                /* unmap the vaddr for default group */
                (void)munmap(vaddr, GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));

                /* unmap the vaddr for default group */
                (void)munmap(cgutil_vaddr[0], GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));

                /* parse the origin group for nodegroup */
                if (-1 == cgconf_parse_nodegroup_config_file()) {
                    free(ngcgpath);
                    ngcgpath = NULL;
                    free(cgpath);
                    cgpath = NULL;
                    return -1;
                }

                current_nodegroup = cgutil_opt.nodegroup;

                /* create nodegroup default cgroups by mapping info */
                if (-1 == cgexec_create_nodegroup_default_cgroups()) {
                    free(ngcgpath);
                    ngcgpath = NULL;
                    free(cgpath);
                    cgpath = NULL;
                    return -1;
                }
            }
        }

        free(ngcgpath);
        ngcgpath = NULL;
    }

    free(cgpath);
    cgpath = NULL;

    /* create Class group */
    if (cgutil_opt.clsname[0]) {
        if (0 == strcmp(cgutil_opt.clsname, GSCGROUP_DEFAULT_CLASS)) {
            fprintf(stderr,
                "ERROR: Can't create Default Workload Cgroup "
                "for DefaultClass Cgroup!\n");
            return -1;
        }

        /* create this class cgroup */
        if (-1 == cgexec_create_class_cgroup()) {
            cgconf_remove_backup_conffile();
        }
    }

    return 0;
}

/*
 * function name: cgexec_drop_nodegroup_cgroups
 * description  : drop cgroups of the specified nodegroup
 *
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_drop_nodegroup_cgroups(void)
{
    /* delete the configure file */
    char* cfgpath = NULL;

    cfgpath = cgconf_get_config_path(false);

    if (cfgpath != NULL && !cgutil_opt.rename) {
        (void)unlink(cfgpath);
    } else if (cfgpath == NULL) {
        return -1;
    }

    /* remove unused backup file */
    cgconf_remove_backup_conffile();

    /* drop the nodegroup Cgroup tree */
    (void)cgptree_drop_nodegroup_cgroups(cgutil_opt.nodegroup);

    if (cgutil_opt.rename) {
        void* vaddr = NULL;
        size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
        errno_t sret;

        /* delete old cgroups */
        (void)cgptree_drop_nodegroup_cgroups(GSCGROUP_TOP_CLASS);

        /* map the original Cgroup Configuration file */
        vaddr = cgconf_map_origin_conffile();
        if (NULL == vaddr) {
            fprintf(stderr, "ERROR: failed to create and map the configure file!\n");
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        for (int i = 0; i < CLASSCG_START_ID; i++) {
            gscgroup_grp_t* tmpvaddr = (gscgroup_grp_t*)vaddr + i;
            sret = memcpy_s(cgutil_vaddr[i], sizeof(gscgroup_grp_t), tmpvaddr, sizeof(gscgroup_grp_t));
            securec_check_errno(sret, (void)munmap(vaddr, cglen); free(cfgpath);, -1);
        }

        /* unmap the vaddr for default group */
        (void)munmap(vaddr, GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t));

        current_nodegroup = NULL;

        /* create nodegroup default cgroups by mapping info */
        if (-1 == cgexec_create_nodegroup_default_cgroups()) {
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        cgutil_opt.nodegroup[0] = '\0';  // reset the node group info

        /* rename the configuration file */
        char* old_confpath = cgconf_get_config_path(false);
        if (NULL == old_confpath) {
            fprintf(stderr, "ERROR: failed to get the configuration path,configuration path is NULL.");
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        if (-1 == rename(cfgpath, old_confpath)) {
            fprintf(stderr, "ERROR: failed to rename %s to %s.", cfgpath, old_confpath);
            free(cfgpath);
            cfgpath = NULL;
            free(old_confpath);
            old_confpath = NULL;
            return -1;
        }
        free(old_confpath);
        old_confpath = NULL;
    }

    free(cfgpath);
    cfgpath = NULL;
    return 0;
}

/*
 * function name: cgexec_drop_groups
 * description  : when there is no specified Class group, root user will
 *                delete Gaussdb group. Otherwise, it will delete
 *                the Class name. When "-M" option is specified, it umounts
 *                cgroup file system.
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_drop_groups(void)
{
    /* root user drops common user's cgroups and umount */
    if ('\0' != cgutil_opt.user[0] && geteuid() == 0 && '\0' == cgutil_opt.clsname[0]) {
        (void)cgptree_drop_cgroups();
    }

    /* root user drops cm cgroup */
    if ('\0' != cgutil_opt.user[0] && geteuid() == 0) {
        (void)cgexec_delete_cm_cgroup();
    }

    /* root user drops common user's cgroups and umount */
    if (geteuid() != 0 && '\0' != cgutil_opt.nodegroup[0] && '\0' == cgutil_opt.clsname[0]) {
        (void)cgexec_drop_nodegroup_cgroups();
    }

    if ('\0' != cgutil_opt.clsname[0]) {
        if (0 == strcmp(cgutil_opt.clsname, GSCGROUP_DEFAULT_CLASS)) {
            fprintf(stderr, "ERROR: Can't drop DefaultClass Cgroup!\n");
            return -1;
        }

        if (-1 == cgexec_delete_class_cgroup()) {
            cgconf_remove_backup_conffile();
        }
    }

    if (geteuid() == 0 && cgutil_opt.umflag) {
        fprintf(stdout, "ERROR: Cgroup is mounted. Ready to umount cgroup!\n");

        /* mount the cgroup */
        (void)cgexec_umount_root_cgroup();
    }

    return 0;
}

/*
 * function name: cgexec_update_groups
 * description  : update the dynamic value or fixed value based on options
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexec_update_groups(void)
{
    int ret = 0;

    /* back up the config file */
    if (-1 == cgconf_backup_config_file())
        return -1;

    if (0 == cgutil_opt.fixed) {
        if ('\0' != cgutil_opt.clsname[0])
            ret = cgexec_update_dynamic_class_cgroup();

        if ('\0' != cgutil_opt.bkdname[0])
            ret = cgexec_update_dynamic_backend_cgroup();

        if ('\0' != cgutil_opt.topname[0])
            ret = cgexec_update_dynamic_top_cgroup();
    } else if (cgutil_opt.fixed) {
        if ('\0' != cgutil_opt.clsname[0])
            ret = cgexec_update_fixed_class_cgroup();

        if ('\0' != cgutil_opt.bkdname[0])
            ret = cgexec_update_fixed_backend_cgroup();

        if ('\0' != cgutil_opt.topname[0])
            ret = cgexec_update_fixed_top_cgroup();
    }

    /* remove the backup file */
    if (-1 == ret) {
        cgconf_remove_backup_conffile();
    }

    return 0;
}

/* get Root information */
int cgexec_get_cgroup_cpuset_info(int cnt, char** cpuset)
{
    char* relpath = NULL;
    struct cgroup* cg = NULL;
    struct cgroup_controller* cgc_cpu = NULL;
    int ret;

    /* get the relative path */
    if (NULL == (relpath = gscgroup_get_relative_path(cnt, cgutil_vaddr, current_nodegroup)))
        return -1;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        fprintf(stdout, "ERROR: failed to create the new cgroup for %s\n", relpath);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "ERROR: failed to get %s cgroup information for %s(%d)\n", relpath, cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        free(relpath);
        relpath = NULL;
        return -1;
    }

    free(relpath);
    relpath = NULL;

    /* get the CPU controller */
    cgc_cpu = cgroup_get_controller(cg, MOUNT_CPUSET_NAME);
    if (NULL == cgc_cpu) {
        fprintf(stderr, "ERROR: failed to add %s controller in %d!\n", MOUNT_CPU_NAME, cnt);
        cgroup_free(&cg);
        return -1;
    }

    /* get cpuset value with controller */
    if (0 != (ret = cgroup_get_value_string(cgc_cpu, CPUSET_CPUS, cpuset))) {
        fprintf(stderr, "ERROR: failed to get %s for %s\n", CPUSET_CPUS, cgroup_strerror(ret));
        goto error;
    }

    /* Get the mems info */
    if (cnt == TOPCG_ROOT) {
        char* cpumems = NULL;
        if (0 != (ret = cgroup_get_value_string(cgc_cpu, CPUSET_MEMS, &cpumems))) {
            fprintf(stderr, "ERROR: failed to get %s for %s\n", CPUSET_MEMS, cgroup_strerror(ret));
            goto error;
        }

        errno_t sret = snprintf_s(cgutil_mems, CPUSET_LEN, CPUSET_LEN - 1, "%s", cpumems);
        securec_check_intval(sret, free(cpumems); cgroup_free_controllers(cg); cgroup_free(&cg), -1);

        free(cpumems);
        cpumems = NULL;
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return 0;
error:
    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return -1;
}

/*
 * @Description: get current memory count.
 * @OUT mems: memory set
 * @IN size: memory set size
 * @Return: memory set
 * @See also:
 */
char* cgexec_get_cgroup_cpuset_mems(char* mems, int size)
{
    int ret = 1;
    char cmd[128];
    char line[128];

    FILE* fp = NULL;

    /* open '/proc/cpuinf' to search 'physical id' count to get memory set */
    errno_t sret = snprintf_s(cmd,
        sizeof(cmd),
        sizeof(cmd) - 1,
        "%s",
        "lscpu | grep \"NUMA node(s)\" | awk -F: '{print $2}'| sed 's/\\ //g'");
    securec_check_intval(sret, , mems);

    if ((fp = popen(cmd, "r")) != NULL) {
        if (fgets(line, sizeof(line), fp) != NULL) {
            /* get count */
            ret = atoi(line);

            if (ret == 0)
                ret = 1;
        }

        pclose(fp);
    }

    /* get memory set */
    sret = snprintf_s(mems, size, size - 1, "%d-%d", 0, ret - 1);
    securec_check_intval(sret, , mems);

    return mems;
}

/*
 * @Description: update config cpuset
 * @IN void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
int cgexec_check_top_cpuset(void)
{
    /*
     * Check whether the maximum number of configuration file is
     * compatible with the total number of cores in the current
     * node. If compatible, no update is required
     */
    if (cgexec_check_cpuset_value(cgutil_allset, cgutil_vaddr[TOPCG_GAUSSDB]->cpuset) == 0)
        return 0;

    /* not compatible, we have to update the configuration file */
    for (int i = 0; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        errno_t sret = snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_allset);
        securec_check_intval(sret, , -1);
    }

    return 0;
}

/*
 * @Description     : update cpu core percentage and cpusets values recursively.
 *                  : the previous "-f" is replaced by "--fixed", so
 *                  : groups with "cpusets" are all
 *                  : transfered to percentage of the high level
 * @IN high         : high level group id.
 * @IN extended     : the total quota is out of range, so cpusets of the groups
 *                  : and the belonging groups are all the same with the
 *                  : high level groups, except those who has been set "quota"
 *                  : already.
 * @Return          :
 * @See also:
 */
static void cgexec_update_fixed_config(int high, int extended)
{
    int forstart = 0, forend = 0; /* start and end value of the loop */
    int i = 0;
    int lowlen = 0, highlen = 0;    /* low and high level cpuset length */
    int start = 0, end = 0;         /* only used to call function cgexec_get_cpuset_length*/
    int lowstart = 0, lowend = 0;   /* low group cpuset start and end value */
    int highstart = 0, highend = 0; /* high group cpuset start and end value */
    int part_quota = 0;             /* the current quota transfered from cpusets */
    int sum_quota = 0;              /* sum of the quota values */
    errno_t sret = 0;               /* securec_check return value */
    char sets[CPUSET_LEN];          /* the calculated cpuset to be updated */
    bool flag = false;              /*flag to indicate first time enter the loop */
    char topwd[GPNAME_LEN];

    /* get 'topwd' cgroup full name*/
    sret = snprintf_s(topwd, sizeof(topwd), sizeof(topwd) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(sret, , );

    /* if high has been the lowest level, the recursion is interrupted */
    if (cgexec_get_cgroup_id_range(high, &forstart, &forend) == -1)
        return;

    /* the cpuset length of the high level group */
    highlen = cgexec_get_cpuset_length(cgutil_vaddr[high]->cpuset, &highstart, &highend);

    sum_quota = cgexec_check_fixed_percent(high);

    for (i = forstart; i <= forend; i++) {
        /* the low level group is ignored in the reseting list */
        if (cgutil_vaddr[i]->used == 0)
            continue;

        /* only the workload groups belonging to high class is considered */
        if ((high >= CLASSCG_START_ID && high <= CLASSCG_END_ID) && (high != cgutil_vaddr[i]->ginfo.wd.cgid))
            continue;

        /* transfer cpuset to quota */
        if (!cgutil_vaddr[i]->ainfo.quota) {
            /* get length of the low level cpuset */
            if (*cgutil_vaddr[i]->cpuset != '\0')
                lowlen = cgexec_get_cpuset_length(cgutil_vaddr[i]->cpuset, &start, &end);

            /* quota is still set to 0 */
            if (lowlen == highlen || !lowlen || extended ||
                (strcmp(cgutil_vaddr[i]->grpname, topwd) == 0 && (i >= WDCG_START_ID) && (i <= WDCG_END_ID))) {
                sret =
                    snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[high]->cpuset);
                securec_check_intval(sret, , );

                /* update the configure recursively */
                if (i < WDCG_START_ID)
                    cgexec_update_fixed_config(i, extended);

                /* extended is only available for the belonging groups of the current group */
                if (extended)
                    extended = 0;

                continue;
            }

            /* transfer cpuset to quota */
            part_quota = cgexec_trans_cpusets_to_percent(highlen, lowlen);

            /* the new quota plus the total quota is out of range */
            if (part_quota + sum_quota > GROUP_ALL_PERCENT) {
                /*
                 * the belonging groups will be marked as extended,
                 * and the cpusets will be the same with high level ones.
                 */
                extended = 1;

                sret =
                    snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[high]->cpuset);
                securec_check_intval(sret, , );
            } else {
                /* alter the quota configure for the successfully transfered groups */
                sum_quota += part_quota;
                cgutil_vaddr[i]->ainfo.quota = part_quota;
            }
        }

        /* reset the configure file for all groups with quota value */
        if (cgutil_vaddr[i]->ainfo.quota) {
            /* the low level groups (same level groups with "low") cpu core length */
            lowlen = cgexec_trans_percent_to_cpusets(highlen, cgutil_vaddr[i]->ainfo.quota);
            /*
             * only the first time enter the loop, flag is false
             * the cpu cores are allocated sequentially within high group cpu core range.
             * the first group to be reset is allocated from "highstart",
             * the next are allocated following the previous group "lowend" + 1
             */
            lowstart = flag ? (lowend + 1) : highstart;
            lowend = lowstart + lowlen - 1;

            /*
             * the previous steps guarantee the total quota not out of range,
             * so here we only need check whether the left cpu cores are enough or not,
             * and and the not enough cases will be handled in the same way with
             * cgexec_check_cpuset_percent.
             */
            if (lowend > highend) {
                lowstart = highend - lowlen + 1;
                lowend = highend;
            }

            /* "sets" restore the cpuset to be reset*/
            cgexec_get_cpu_core_range(sets, lowstart, lowend);

            sret = snprintf_s(cgutil_vaddr[i]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", sets);
            securec_check_intval(sret, , );

            /* next time enter the loop, flag will be true*/
            if (!flag)
                flag = true;
        }

        /* update the configure recursively */
        if (i < WDCG_START_ID)
            cgexec_update_fixed_config(i, extended);

        /* extended is only available for the belonging groups of the current group */
        if (extended)
            extended = 0;
    }
}

/*
 * function name: cgexec_refresh_groups_internal
 * description  : refresh groups internal function
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexec_refresh_groups_internal(void)
{
    // update quota and cpusets for all the control groups recursively
    cgexec_update_fixed_config(TOPCG_GAUSSDB, 0);

    /* create default groups.
     * if an error happened, then return -1.
     */
    if (cgexec_create_default_cgroups()) {
        return -1;
    }
    if (cgexec_create_cm_default_cgroup()) {
        return -1;
    }

    return 0;
}

/*
 * @Description: refresh cgroup with configure file.
 * @IN size: void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
int cgexec_refresh_original_groups(void)
{
    /* delete backend and class group */
    for (int idx = TOPCG_BACKEND; idx <= TOPCG_CLASS; ++idx) {
        if (cgutil_vaddr[idx]->used == 0)
            continue;

        if (-1 == cgexec_delete_default_cgroup(cgutil_vaddr[idx]))
            return -1;
    }

    /*
     * We have to check if the configured maximum number of cores can
     * be used on the current node, and if not, we will try to update
     * the configuration file for compatibility with the new node
     */
    if (cgexec_check_top_cpuset() == -1) {
        fprintf(stderr, "ERROR: update top cpuset error.");
        return -1;
    }

    /* create the default cgroups */
    return cgexec_refresh_groups_internal();
}

/*
 * @Description: refresh cgroup with configure file of nodegroup.
 * @IN size: void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
int cgexec_refresh_nodegroup_groups(void)
{
    /* delete logical cluster group */
    if (-1 == cgptree_drop_nodegroup_cgroups(cgutil_opt.nodegroup))
        return -1;

    /* create the default cgroups */
    return cgexec_create_nodegroup_default_cgroups();
}

/*
 * @Description: refresh cgroup with configure file.
 * @IN size: void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
int cgexec_refresh_groups(void)
{
    if ('\0' == cgutil_opt.nodegroup[0])
        return cgexec_refresh_original_groups();
    else
        return cgexec_refresh_nodegroup_groups();
}

/*
 * @Description: revert cgroup configure.
 * @IN size: void
 * @Return:  -1: abnormal 0: normal
 * @See also:
 */
int cgexec_revert_groups(void)
{
    for (int cls = CLASSCG_START_ID + 1; cls <= CLASSCG_END_ID; ++cls) {
        if (cgutil_vaddr[cls]->used == 0)
            continue;

        /* delete all class group except default class group */
        if (-1 == cgexec_delete_default_cgroup(cgutil_vaddr[cls]))
            return -1;

        /* reset all group configure */
        cgconf_reset_class_group(cls);
    }

    /* revert configure file */
    cgconf_revert_config_file();

    /* create default groups.
     * if an error happened, then return -1.
     */
    if (cgexec_create_default_cgroups())
        return -1;
    if (cgexec_create_cm_default_cgroup())
        return -1;

    return 0;
}

/*
 * @Description: check if changes happened on both groups
 * @IN cur: current group
 * @IN bak: backup group
 * @Return:  1: updated 0:no updated
 * @See also:
 */
int cgexec_check_update_groups(gscgroup_grp_t* cur, gscgroup_grp_t* bak)
{
    int offset = offsetof(gscgroup_grp_t, ainfo);
    int size = sizeof(alloc_info_t);

    if (0 == memcmp((void*)((char*)cur + offset), (void*)((char*)bak + offset), size))
        return 0;
    else
        return 1;
}

/*
 * @Description: recover groups by updating the percent groups
 * @IN id: the id of updated group
 * @Return:
 *     -1: abnormal 0: normal
 * @See also:
 */
int cgexec_recover_update_percent_groups(int id)
{
    errno_t sret;

    /* class group changed */
    if (id >= CLASSCG_START_ID && id <= CLASSCG_END_ID) {
        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr_back[id]))
            return -1;
    } else if (id >= WDCG_START_ID && id <= WDCG_END_ID) {
        int cls = cgutil_vaddr_back[id]->ginfo.wd.cgid;

        /* need to update all class groups and their workload groups */
        sret = memcpy_s(cgutil_vaddr[cls], sizeof(gscgroup_grp_t), cgutil_vaddr_back[cls], sizeof(gscgroup_grp_t));
        securec_check_errno(sret, , -1);

        /* update the os cgroups */
        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr_back[id]))
            return -1;

        /* need to update the workload group */
        sret = memcpy_s(cgutil_vaddr[id], sizeof(gscgroup_grp_t), cgutil_vaddr_back[id], sizeof(gscgroup_grp_t));
        securec_check_errno(sret, , -1);

        /* update the remain group */
        if (-1 == cgexec_update_remain_cgroup(cgutil_vaddr[id], cls))
            return -1;
    }

    return 0;
}

/*
 * @Description: recover groups by updating the fixed class groups
 * @IN id: the id of updated group
 * @IN cpuset: input string of cpuset
 * @IN reverse: flag if it is reverse or not
 * @Return:
 *     -1: abnormal 0: normal
 * @See also:
 */
int cgexec_recover_update_fixed_class_group(int id, char* cpuset, int reverse)
{
    errno_t sret;

    if (!reverse) /* from down to up */
    {
        /* copy the value into class group */
        sret = strcpy_s(cgutil_vaddr[id]->cpuset, CPUSET_LEN, cpuset);
        securec_check_errno(sret, , -1);

        /* update the class group into cgroup fs */
        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[id]))
            return -1;

        /* update the remain group */
        for (int j = 1; j <= cgutil_vaddr[id]->ginfo.cls.maxlevel; ++j) {
            if (-1 == cgexec_update_remain_cgroup_cpuset_value(id, j, cpuset))
                return -1;
        }

        /* update the timeshare group */
        if (-1 == cgexec_update_timeshare_cpuset(cgutil_vaddr[id], cpuset, 0))
            return -1;

        /* update the TopWD group */
        if (-1 == cgexec_update_topwd_cgroup_cpuset(id, cpuset))
            return -1;
    } else {
        /* update the timeshare group */
        if (-1 == cgexec_update_timeshare_cpuset(cgutil_vaddr[id], cpuset, 1))
            return -1;

        /* update the remain group */
        for (int j = cgutil_vaddr[id]->ginfo.cls.maxlevel; j >= 1; --j) {
            if (-1 == cgexec_update_remain_cgroup_cpuset_value(id, j, cpuset))
                return -1;
        }

        /* update the TopWD group */
        if (-1 == cgexec_update_topwd_cgroup_cpuset(id, cpuset))
            return -1;

        /* copy the value into class group */
        sret = strcpy_s(cgutil_vaddr[id]->cpuset, GPNAME_LEN, cpuset);
        securec_check_errno(sret, , -1);

        /* update the class group into cgroup fs */
        if (-1 == cgexec_update_cgroup_value(cgutil_vaddr[id]))
            return -1;
    }

    return 0;
}

/*
 * @Description: recover groups by updating the quota groups
 * @IN id: the id of updated group
 * @Return:
 *     -1: abnormal 0: normal
 * @See also:
 */
int cgexec_recover_update_quota_groups(int id)
{
    /* class group changed */
    if (id >= CLASSCG_START_ID && id <= CLASSCG_END_ID) {
        /* reset the default value as Top Class group */
        for (int i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
            if (cgutil_vaddr[i]->used == 0 || (i != id && cgutil_vaddr[i]->ainfo.quota == 0))
                continue;

            /* update the remain and timeshare group based on backup value */
            if (-1 == cgexec_recover_update_fixed_class_group(i, cgutil_vaddr[TOPCG_CLASS]->cpuset, 0))
                return -1;
        }

        /* update all workload group */
        for (int j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
            if (cgutil_vaddr_back[j]->used == 0 || cgutil_vaddr_back[j]->ginfo.wd.wdlevel == 1)
                continue;

            /* update the workload group */
            if (-1 == cgexec_update_cgroup_value(cgutil_vaddr_back[j]))
                return -1;
        }

        /* re-update the Class Group */
        for (int i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
            if (cgutil_vaddr[i]->used == 0 || (i != id && cgutil_vaddr[i]->ainfo.quota == 0))
                continue;

            if (-1 == cgexec_recover_update_fixed_class_group(i, cgutil_vaddr_back[i]->cpuset, 1))
                return -1;
        }
    } else if (id >= WDCG_START_ID && id <= WDCG_END_ID) {
        int cls = cgutil_vaddr_back[id]->ginfo.wd.cgid;

        for (int i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
            if (cgutil_vaddr_back[i]->used == 0 || cgutil_vaddr_back[i]->ginfo.wd.cgid != cls ||
                cgutil_vaddr_back[i]->ginfo.wd.wdlevel == 1)
                continue;

            /* update the workload group */
            if (-1 == cgexec_update_cgroup_value(cgutil_vaddr_back[i]))
                return -1;
        }
    }

    return 0;
}

/*
 * @Description: recover groups by creating new cgroups
 * @IN cls_add : class id
 * @IN wd_add  : array of all new workload groups
 * @Return:
 *     -1: abnormal 0: normal
 * @See also:
 */
int cgexec_recover_create_groups(int cls_add, const int* wd_add)
{
    int wld = 0, level = 0, i = 0, tmpcls;
    int tmpwld[MAX_WD_LEVEL] = {0};
    errno_t sret;

    sret = memset_s(tmpwld, sizeof(tmpwld), 0, sizeof(tmpwld));
    securec_check_errno(sret, , -1);

    /* get the class info */
    wld = wd_add[0];
    tmpcls = cgutil_vaddr_back[wld]->ginfo.wd.cgid;

    /* verify the information */
    if (cls_add && cls_add != tmpcls) {
        fprintf(stderr, "ERROR: new workload group doesn't match class group!\n");
        return -1;
    }

    if (cls_add == 0 && wd_add[1]) {
        fprintf(stderr,
            "ERROR: find more than one added workload group "
            "when only workload group is recovering.\n");
        return -1;
    }

    /* search workload group */
    for (i = WDCG_START_ID; i <= WDCG_END_ID; ++i) {
        if (cgutil_vaddr_back[i]->used && cgutil_vaddr_back[i]->ginfo.wd.cgid == tmpcls) {
            level = cgutil_vaddr_back[i]->ginfo.wd.wdlevel;
            tmpwld[level - 1] = i;
        }
    }

    /* delete class group firstly if only workload group should be added */
    if (cls_add == 0 && -1 == cgexec_delete_default_cgroup(cgutil_vaddr[tmpcls]))
        return -1;

    /* copy class group info */
    sret = memcpy_s(cgutil_vaddr[tmpcls], sizeof(gscgroup_grp_t), cgutil_vaddr_back[tmpcls], sizeof(gscgroup_grp_t));
    securec_check_errno(sret, , -1);

    /* reset the values */
    cgutil_vaddr[tmpcls]->ginfo.cls.maxlevel = 0;
    cgutil_vaddr[tmpcls]->ginfo.cls.rempct = 100;
    cgconf_update_class_percent();

    /* create class group */
    if (-1 == cgexec_create_new_cgroup(cgutil_vaddr[tmpcls])) {
        cgconf_reset_class_group(tmpcls);
        return -1;
    }

    /* create workload group in a loop */
    for (i = 0; i < MAX_WD_LEVEL; i++) {
        wld = tmpwld[i];
        if (wld == 0)
            break;

        /* copy workload group info */
        sret = memcpy_s(cgutil_vaddr[wld], sizeof(gscgroup_grp_t), cgutil_vaddr_back[wld], sizeof(gscgroup_grp_t));
        securec_check_errno(sret, , -1);

        if (i) /* level > 1, is not TopWD */
            cgutil_vaddr[tmpcls]->ginfo.cls.rempct -= cgutil_vaddr[wld]->ginfo.wd.percent;

        /* create the workload cgroup */
        if (-1 == cgexec_create_workload_cgroup(cgutil_vaddr[wld])) {
            cgconf_reset_workload_group(wld);
            return -1;
        }
    }

    /* create timeshare cgroup */
    if (-1 == cgexec_create_timeshare_cgroup(cgutil_vaddr[tmpcls]))
        return -1;

    return 0;
}

/*
 * @Description: recover the last group when failure happened
 * @Return:
 *     -1: abnormal 0: normal
 * @See also:
 */
int cgexec_recover_groups(void)
{
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    int cls_add = 0, cls_del = 0, wd_del = 0;
    int clspct_update = 0, wdpct_update = 0, quota_update = 0, other_update = 0;
    int wd_add[MAX_WD_LEVEL] = {0};
    int j = 0;
    errno_t sret;

    /* reset the array */
    sret = memset_s(wd_add, sizeof(wd_add), 0, sizeof(wd_add));
    securec_check_errno(sret, , -1);

    /* get the mapping address of backup file */
    vaddr = cgconf_map_backup_conffile(false);
    if (NULL == vaddr)
        return -1;

    for (int i = 0; i < GSCGROUP_ALLNUM; ++i) {
        cgutil_vaddr_back[i] = (gscgroup_grp_t*)vaddr + i;

        /* find the different memory region of group entry */
        if ((i >= CLASSCG_START_ID && i <= WDCG_END_ID) &&
            (0 != memcmp(cgutil_vaddr[i], cgutil_vaddr_back[i], sizeof(gscgroup_grp_t)))) {
            if (i >= CLASSCG_START_ID && i <= CLASSCG_END_ID) {
                if (cgutil_vaddr[i]->used && cgutil_vaddr_back[i]->used == 0)
                    cls_del = i;
                else if (cgutil_vaddr[i]->used == 0 && cgutil_vaddr_back[i]->used)
                    cls_add = i;
                else if (cgutil_vaddr[i]->used && cgutil_vaddr_back[i]->used &&
                         cgexec_check_update_groups(cgutil_vaddr[i], cgutil_vaddr_back[i])) {
                    /* dynamic update */
                    if (cgutil_vaddr[i]->ginfo.cls.percent != cgutil_vaddr_back[i]->ginfo.cls.percent)
                        clspct_update = i;
                    else if (cgutil_vaddr[i]->ainfo.quota != cgutil_vaddr_back[i]->ainfo.quota)
                        quota_update = i;
                    else
                        other_update = i;
                }
            } else if (i > WDCG_START_ID) {
                if (cgutil_vaddr[i]->used && cgutil_vaddr_back[i]->used == 0)
                    wd_del = i;
                else if (cgutil_vaddr[i]->used == 0 && cgutil_vaddr_back[i]->used) {
                    if (j == MAX_WD_LEVEL) {
                        fprintf(stderr, "ERROR: configure file has more than %d different workload!\n", MAX_WD_LEVEL);
                        goto error;
                    }
                    wd_add[j++] = i;
                } else if (cgutil_vaddr[i]->used && cgutil_vaddr_back[i]->used &&
                           cgexec_check_update_groups(cgutil_vaddr[i], cgutil_vaddr_back[i])) {
                    /* dynamic update */
                    if (cgutil_vaddr[i]->ginfo.wd.percent != cgutil_vaddr_back[i]->ginfo.wd.percent)
                        wdpct_update = i;
                    else if (cgutil_vaddr[i]->ainfo.quota != cgutil_vaddr_back[i]->ainfo.quota) {
                        if (quota_update) {
                            fprintf(stderr,
                                "ERROR: cpu core quota has been set on %d, "
                                "it should not be appear on %d again!\n",
                                quota_update,
                                i);
                            goto error;
                        }
                        quota_update = i;
                    } else
                        other_update = i;
                }
            }
        }
    }

    /* -u class update */
    if (clspct_update && -1 == cgexec_recover_update_percent_groups(clspct_update))
        goto error;

    /* -u workload update */
    if (wdpct_update && -1 == cgexec_recover_update_percent_groups(wdpct_update))
        goto error;

    /* -u --fixed update */
    if (quota_update && -1 == cgexec_recover_update_quota_groups(quota_update))
        goto error;

    /* like blkio throttle update */
    if (clspct_update == 0 && wdpct_update == 0 && quota_update == 0 && other_update &&
        (-1 == cgexec_update_cgroup_value(cgutil_vaddr_back[other_update])))
        goto error;

    /* delete the class group directly */
    if (cls_del) {
        if (-1 == cgexec_delete_default_cgroup(cgutil_vaddr[cls_del]))
            goto error;
        cgconf_reset_class_group(cls_del);
    } else if (cls_del == 0 && wd_del) /* delete the workload group */
    {
        (void)cgexec_delete_workload_cgroup(cgutil_vaddr[wd_del]);
    }

    /* add class and workload group */
    if (wd_add[0]) {
        if (-1 == cgexec_recover_create_groups(cls_add, wd_add))
            goto error;
    }

    /* recover the configuration file finally */
    sret = memcpy_s(cgutil_vaddr[0], cglen, vaddr, cglen);
    securec_check_errno(sret, (void)munmap(vaddr, cglen); cgconf_remove_backup_conffile();, -1);

    (void)munmap(vaddr, cglen);
    cgconf_remove_backup_conffile();
    return 0;

error:
    securec_check_errno(sret, (void)munmap(vaddr, cglen);, -1);
    cgconf_remove_backup_conffile();
    return -1;
}

/*
 * @Description: mount control groups.
 * @IN : void
 * @Return:  void
 * @See also:
 */
void cgexec_mount_cgroups(void)
{
    /* mount the cgroup */
    (void)cgexec_mount_root_cgroup();
}

/*
 * @Description: unmount control groups.
 * @IN : void
 * @Return:  void
 * @See also:
 */
void cgexec_umount_cgroups(void)
{
    /* umount the cgroup */
    (void)cgexec_umount_root_cgroup();
}

/*
 * function name: cgexec_create_cm_default_cgroup
 * description  : create cm default cgroup
 * arguments    : void
 * return value :
 *            -1: abnormal
 *             0: normal
 * Note: the function is used when creating new Cgroup.
 */

int cgexec_create_cm_default_cgroup(void)
{
    int ret = 0;
    errno_t rc = EOK;
    char cgpath[GPNAME_PATH_LEN] = {0};
    struct stat buf;

    rc = memset_s(&buf, sizeof(buf), 0, sizeof(buf));
    securec_check_errno(rc, , -1);
    rc = sprintf_s(cgpath, GPNAME_PATH_LEN, "%s/%s:%s", cgutil_opt.mpoints[0], GSCGROUP_CM, cgutil_opt.user);
    securec_check_ss_c(rc, "\0", "\0");

    // creating cm cgroup must be run by root user.
    if (geteuid() != 0)
        return 0;

    if (0 == stat(cgpath, &buf)) {
        fprintf(stderr, "'%s' exists, omit to create this cgroup.\n", cgpath);
        return 0;
    }

    rc = sprintf_s(cgpath, GPNAME_PATH_LEN, "%s:%s", GSCGROUP_CM, cgutil_opt.user);
    securec_check_ss_c(rc, "\0", "\0");

    ret = cgexec_create_default_cgroup(cgpath, DEFAULT_CM_CPUSHARES, DEFAULT_IO_WEIGHT, cgutil_allset);
    if (ret == -1) {
        fprintf(stderr, "can not create cm cgroup, cgpath is %s.\n", cgpath);
        return -1;
    }

    return 0;
}

/* delete cm cgroup */
int cgexec_delete_cm_cgroup(void)
{
    int ret = 0;
    errno_t rc = EOK;
    char cgpath[GPNAME_PATH_LEN] = {0};
    struct stat buf;

    rc = memset_s(&buf, sizeof(buf), 0, sizeof(buf));
    securec_check_errno(rc, , -1);
    rc = sprintf_s(cgpath, GPNAME_PATH_LEN, "%s/%s:%s", cgutil_opt.mpoints[0], GSCGROUP_CM, cgutil_opt.user);
    securec_check_ss_c(rc, "\0", "\0");

    if (0 != stat(cgpath, &buf)) {
        return -1;
    }

    rc = sprintf_s(cgpath, GPNAME_PATH_LEN, "%s:%s", GSCGROUP_CM, cgutil_opt.user);
    securec_check_ss_c(rc, "\0", "\0");

    ret = cgexec_delete_cgroups(cgpath);
    if (ret == -1) {
        fprintf(stderr, "can not create cm cgroup,cgpath is %s.\n", cgpath);
        return -1;
    }

    return 0;
}
