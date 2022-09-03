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
 * -------------------------------------------------------------------------
 *
 * cgconf.cpp
 *    Cgroup configration file process functions
 *
 * IDENTIFICATION
 *    src/bin/gs_cgroup/cgconf.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> /* stat */
#include <sys/stat.h>
#include <sys/mman.h> /* mmap */
#include <fcntl.h>

#include "securec.h"
#include "cgutil.h"

/*
 *****************  STATIC FUNCTIONS ************************
 */

/*
 * function name: cgconf_get_group_type
 * description  : get the string of group type
 * arguments    : group type enum type value
 * return value : the string of group type
 */
char* cgconf_get_group_type(group_type gtype)
{
    if (gtype == GROUP_TOP)
        return "Top";
    else if (gtype == GROUP_CLASS)
        return "CLASS";
    else if (gtype == GROUP_BAKWD)
        return "BAKWD";
    else if (gtype == GROUP_DEFWD)
        return "DEFWD";
    else if (gtype == GROUP_TSWD)
        return "TSWD";

    return NULL;
}

/*
 * function name: cgconf_set_root_group
 * description  : set the default value of root group in configuration file
 *
 * Note: The root group can't set the IO relative weight.
 *       The percentage is calculated based on 1000.
 */
static void cgconf_set_root_group(void)
{
    errno_t sret;
    cgutil_vaddr[TOPCG_ROOT]->used = 1;
    cgutil_vaddr[TOPCG_ROOT]->gid = TOPCG_ROOT;
    cgutil_vaddr[TOPCG_ROOT]->gtype = GROUP_TOP;
    sret = strncpy_s(cgutil_vaddr[TOPCG_ROOT]->grpname, GPNAME_LEN, GSCGROUP_ROOT, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TOPCG_ROOT]->ginfo.top.percent = 100 * DEFAULT_IO_WEIGHT / MAX_IO_WEIGHT;
    cgutil_vaddr[TOPCG_ROOT]->ainfo.weight = DEFAULT_IO_WEIGHT;
    cgutil_vaddr[TOPCG_ROOT]->percent = 1000;

    /* set root group as default cpu set */
    sret = snprintf_s(cgutil_vaddr[TOPCG_ROOT]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_allset);
    securec_check_intval(sret, , );
}

/*
 * function name: cgconf_set_gauss_group
 * description  : set the default value of Gaussdb group in configuration file
 *
 * Note: The IO weight value is set as 1000 (MAX_IO_WEIGHT).
 *       It supposes that the gaussdb can use the maximum IO resource.
 *       The percentage is calculated based on CPU shares value.
 */
static void cgconf_set_gauss_group(void)
{
    errno_t rc;

    cgutil_vaddr[TOPCG_GAUSSDB]->used = 1;
    cgutil_vaddr[TOPCG_GAUSSDB]->gid = TOPCG_GAUSSDB;
    cgutil_vaddr[TOPCG_GAUSSDB]->gtype = GROUP_TOP;
    if ('\0' == cgutil_opt.nodegroup[0])
        rc = snprintf_s(cgutil_vaddr[TOPCG_GAUSSDB]->grpname,
            GPNAME_LEN,
            GPNAME_LEN - 1,
            "%s:%s",
            GSCGROUP_TOP_DATABASE,
            cgutil_opt.user);
    else
        rc = snprintf_s(cgutil_vaddr[TOPCG_GAUSSDB]->grpname,
            GPNAME_LEN,
            GPNAME_LEN - 1,
            "%s:%s",
            GSCGROUP_TOP_DATABASE,
            cgutil_passwd_user->pw_name);
    securec_check_intval(rc, , );
    cgutil_vaddr[TOPCG_GAUSSDB]->ginfo.top.percent =
        100 * DEFAULT_GAUSS_CPUSHARES / (DEFAULT_CPU_SHARES + DEFAULT_GAUSS_CPUSHARES);
    cgutil_vaddr[TOPCG_GAUSSDB]->ainfo.shares = DEFAULT_GAUSS_CPUSHARES;
    cgutil_vaddr[TOPCG_GAUSSDB]->ainfo.weight = MAX_IO_WEIGHT;
    cgutil_vaddr[TOPCG_GAUSSDB]->percent =
        cgutil_vaddr[TOPCG_ROOT]->percent * DEFAULT_GAUSS_CPUSHARES / (DEFAULT_CPU_SHARES + DEFAULT_GAUSS_CPUSHARES);

    /* set root group as root group cpu set */
    if (*cgutil_vaddr[TOPCG_GAUSSDB]->cpuset == '\0') {
        rc = snprintf_s(
            cgutil_vaddr[TOPCG_GAUSSDB]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_ROOT]->cpuset);
        securec_check_intval(rc, , );
    }
}

/*
 * function name: cgconf_set_top_backend_group
 * description  : set the default value of Top Backend group
 *
 */
static void cgconf_set_top_backend_group(void)
{
    errno_t sret;
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
    if (*cgutil_vaddr[TOPCG_BACKEND]->cpuset == '\0') {
        sret = snprintf_s(
            cgutil_vaddr[TOPCG_BACKEND]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);
        securec_check_intval(sret, , );
    }
}

/*
 * function name: cgconf_set_top_class_group
 * description  : set the default value of Top Class group
 *
 */
static void cgconf_set_top_class_group(void)
{
    errno_t sret;
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
    if (*cgutil_vaddr[TOPCG_CLASS]->cpuset == '\0') {
        sret = snprintf_s(
            cgutil_vaddr[TOPCG_CLASS]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);
        securec_check_intval(sret, , );
    }
}

/*
 * function name: cgconf_set_nodegroup_top_group
 * description  : set the default value of Nodegroup Top group
 *
 */
static void cgconf_set_nodegroup_top_group(void)
{
    errno_t sret;
    cgutil_vaddr[TOPCG_CLASS]->used = 1;
    cgutil_vaddr[TOPCG_CLASS]->gid = TOPCG_CLASS;
    cgutil_vaddr[TOPCG_CLASS]->gtype = GROUP_TOP;
    sret = strncpy_s(cgutil_vaddr[TOPCG_CLASS]->grpname, GPNAME_LEN, cgutil_opt.nodegroup, GPNAME_LEN - 1);
    securec_check_errno(sret, , );
    cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent = TOP_CLASS_PERCENT;
    cgutil_vaddr[TOPCG_CLASS]->ainfo.shares = DEFAULT_CPU_SHARES * TOP_CLASS_PERCENT / 10;
    cgutil_vaddr[TOPCG_CLASS]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, TOP_CLASS_PERCENT);
    cgutil_vaddr[TOPCG_CLASS]->percent = cgutil_vaddr[TOPCG_GAUSSDB]->percent * TOP_CLASS_PERCENT / 100;
    /* set root group as gaussdb group cpu set */
    if (*cgutil_vaddr[TOPCG_CLASS]->cpuset == '\0') {
        sret = snprintf_s(
            cgutil_vaddr[TOPCG_CLASS]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_GAUSSDB]->cpuset);
        securec_check_intval(sret, , );
    }
}

/*
 * function name: cgconf_set_default_backend_group
 * description  : set the default value of default backend group
 *
 */
void cgconf_set_default_backend_group(void)
{
    errno_t sret;
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
    if (*cgutil_vaddr[BACKENDCG_START_ID]->cpuset == '\0') {
        sret = snprintf_s(cgutil_vaddr[BACKENDCG_START_ID]->cpuset,
            CPUSET_LEN,
            CPUSET_LEN - 1,
            "%s",
            cgutil_vaddr[TOPCG_BACKEND]->cpuset);
        securec_check_intval(sret, , );
    }
}

/*
 * function name: cgconf_set_vacuum_group
 * description  : set the default value of vacuum backend group
 *
 */
void cgconf_set_vacuum_group(void)
{
    errno_t sret;
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
    if (*cgutil_vaddr[BACKENDCG_START_ID + 1]->cpuset == '\0') {
        sret = snprintf_s(cgutil_vaddr[BACKENDCG_START_ID + 1]->cpuset,
            CPUSET_LEN,
            CPUSET_LEN - 1,
            "%s",
            cgutil_vaddr[TOPCG_BACKEND]->cpuset);
        securec_check_intval(sret, , );
    }
}

/*
 * function name: cgconf_set_default_class_group
 * description  : set the default value of default class group
 *
 */
void cgconf_set_default_class_group(void)
{
    errno_t sret;
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
}

/*
 * function name: cgconf_set_default_top_workload_group
 * description  : set the default value of top workload group
 *
 */
static void cgconf_set_default_top_workload_group(void)
{
    char tmpstr[GPNAME_LEN];
    errno_t sret;

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
}

/*
 * function name: cgconf_set_default_timeshare_group
 * description  : set the default value of default timeshare group
 *
 */
static void cgconf_set_default_timeshare_group(void)
{
    errno_t sret;
    /* low group of default group */
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

/*
 * @Description: reset cgroup configure.
 * @Return: void
 * @See also:
 */
void cgconf_reset_cgroup_config(void)
{
    /* create the default top group */
    cgconf_set_root_group();
    cgconf_set_gauss_group();
    cgconf_set_top_backend_group();

    /* create the default vacuum group under top backend group */
    cgconf_set_default_backend_group();
    cgconf_set_vacuum_group();

    if (cgutil_opt.nodegroup[0] == '\0' || cgutil_opt.rename) {
        cgconf_set_top_class_group();
    } else {
        /* create the nodegroup top group */
        cgconf_set_nodegroup_top_group();
    }

    /* create the default class group under top class group */
    cgconf_set_default_class_group();
    cgconf_set_default_top_workload_group();

    /* create the top/rush/high/medium/low timeshare group of
        default class group */
    cgconf_set_default_timeshare_group();
}
/*
 * @Description: revert io configure.
 * @IN iovalue: iovalue to be reverted
 * @Return: void
 * @See also:
 */
void cgconf_revert_blkio_value(char* iovalue)
{
    char *p = NULL;
    char *q = NULL;
    char *head = NULL;
    char *i = NULL;
    errno_t sret;

    if ((head = strdup(iovalue)) == NULL) {
        fprintf(stderr, "revert blkio failed, cannot alloc memory.");
        return;
    }

    sret = memset_s(iovalue, IODATA_LEN, 0, IODATA_LEN);
    securec_check_errno(sret, free(head), );

    p = head;
    do {
        q = strchr(p, '\n');
        if (q != NULL) {
            *q++ = '\0';
        }
        i = p;
        while (*i++) {
            if (*i == '\t') {
                *i = ' ';
                break;
            }
        }
        i++;
        /*
         * set the blkio throttle values (iopsread/iopswrite/bpsread/bpswrite)
         * of the device to 0 this device will be reverted.
         */
        *i = '0';

        while (*i++) {
            *i = '\0';
        }
        if (iovalue[0]) {
            sret = sprintf_s(iovalue + strlen(iovalue), IODATA_LEN - strlen(iovalue), "\n%s", p);
            securec_check_intval(sret, free(head), );
        } else {
            sret = sprintf_s(iovalue, IODATA_LEN, "%s", p);
            securec_check_intval(sret, free(head), );
        }
        p = q;
    } while (q != NULL);
    free(head);
    head = NULL;
}
/*
 * @Description: revert configure file.
 * @IN void
 * @Return:  void
 * @See also:
 */
void cgconf_revert_config_file(void)
{
    int i = 0;

    /* get current user name */
    errno_t sret = snprintf_s(
        cgutil_opt.user, sizeof(cgutil_opt.user), sizeof(cgutil_opt.user) - 1, "%s", cgutil_passwd_user->pw_name);
    securec_check_intval(sret, , );

    for (i = 1; i < GSCGROUP_ALLNUM; ++i) {
        cgutil_vaddr[i]->used = 0;
        *cgutil_vaddr[i]->cpuset = '\0';
        cgutil_vaddr[i]->ainfo.quota = 0;
        if (cgutil_is_sles11_sp2 || cgexec_check_SLESSP2_version()) {
            if (cgutil_vaddr[i]->ainfo.iopsread[0]) {
                cgconf_revert_blkio_value(cgutil_vaddr[i]->ainfo.iopsread);
            }

            if (cgutil_vaddr[i]->ainfo.iopswrite[0]) {
                cgconf_revert_blkio_value(cgutil_vaddr[i]->ainfo.iopswrite);
            }

            if (cgutil_vaddr[i]->ainfo.bpsread[0]) {
                cgconf_revert_blkio_value(cgutil_vaddr[i]->ainfo.bpsread);
            }

            if (cgutil_vaddr[i]->ainfo.bpswrite[0]) {
                cgconf_revert_blkio_value(cgutil_vaddr[i]->ainfo.bpswrite);
            }
        }
    }

    /* reset configure */
    cgconf_reset_cgroup_config();
}

/*
 * function name: cgconf_generate_default_config_file
 * description  : generate the default configuration file
 *
 */
void cgconf_generate_default_config_file(void* vaddr)
{
    int i = 0;

    for (i = 0; i < GSCGROUP_ALLNUM; i++) {
        cgutil_vaddr[i] = (gscgroup_grp_t*)vaddr + i;
        cgutil_vaddr[i]->used = 0;
    }

    /* reset configure */
    cgconf_reset_cgroup_config();
}

/*
 * function name: cgconf_update_backend_percent
 * description  : update the percentage value of backend group
 * Note: this function is called after updating Backend group value
 */
void cgconf_update_backend_percent(void)
{
    int i;
    int percent = 0;

    /* get the used percent of all backend */
    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used) {
            percent += cgutil_vaddr[i]->ginfo.cls.percent;
        }	
    }

    /* update the percent of each backend */
    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used) {
            if (percent == 0) {
                fprintf(stderr, "ERROR: the percentage value of backend group is zero.\n");
                continue;
            }
            cgutil_vaddr[i]->percent =
                cgutil_vaddr[TOPCG_BACKEND]->percent * cgutil_vaddr[i]->ginfo.cls.percent / percent;

        }
    }
}

/*
 * function name: cgconf_update_backend_group
 * description  : reset the percent and calculate the CPU shares and IO weight
                  of the specified group
 * argument     : the data structure of backend group
 *
 * Note: this function is called after updating Backend group value
 */
void cgconf_update_backend_group(gscgroup_grp_t* grp)
{
    grp->ginfo.cls.percent = cgutil_opt.bkdpct;

    grp->ainfo.shares = DEFAULT_CPU_SHARES * grp->ginfo.cls.percent / 10;

    grp->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, grp->ginfo.cls.percent);

    grp->percent = cgutil_vaddr[TOPCG_BACKEND]->percent * grp->ginfo.cls.percent / 100;
}

/*
 * function name: cgconf_update_class_percent
 * description  : update the percentage value of all class group and
                  its all workload group
 *
 * Note: this function is called after updating Class group value
 */
void cgconf_update_class_percent(void)
{
    int i;
    int j;
    int percent = 0;

    /* get total percent of all class group */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used)
            percent += cgutil_vaddr[i]->ginfo.cls.percent;
    }

    /* update the percentage of class and workload group */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used) {
            if (percent == 0) {
                continue;
            }
            cgutil_vaddr[i]->percent =
                cgutil_vaddr[TOPCG_CLASS]->percent * cgutil_vaddr[i]->ginfo.cls.percent / percent;

            if (cgutil_vaddr[i]->percent == 0)
                cgutil_vaddr[i]->percent = 1;
        }

        for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
            if (cgutil_vaddr[j]->used == 0 || cgutil_vaddr[j]->ginfo.wd.cgid != cgutil_vaddr[i]->gid)
                continue;

            cgutil_vaddr[j]->percent = cgutil_vaddr[i]->percent * cgutil_vaddr[j]->ginfo.wd.percent / 100;

            if (cgutil_vaddr[j]->percent == 0)
                cgutil_vaddr[j]->percent = 1;
        }
    }
}

/*
 * function name: cgconf_update_top_percent
 * description  : update the percentage value of all Backend and Class group
 *
 * Note: this function is called after updating Gaussdb group value
 */
void cgconf_update_top_percent(void)
{
    cgutil_vaddr[TOPCG_BACKEND]->percent =
        cgutil_vaddr[TOPCG_GAUSSDB]->percent * cgutil_vaddr[TOPCG_BACKEND]->ginfo.top.percent / 100;
    cgconf_update_backend_percent();

    cgutil_vaddr[TOPCG_CLASS]->percent =
        cgutil_vaddr[TOPCG_GAUSSDB]->percent * cgutil_vaddr[TOPCG_CLASS]->ginfo.top.percent / 100;
    cgconf_update_class_percent();
}

/*
 * function name: cgconf_set_class_group
 * description  : set the class group fields when creating new class
 *
 * Note: this function is called after creating new Class group
 */
void cgconf_set_class_group(int gid)
{
    errno_t sret;
    cgutil_vaddr[gid]->used = 1;
    cgutil_vaddr[gid]->gid = gid;
    cgutil_vaddr[gid]->gtype = GROUP_CLASS;
    cgutil_vaddr[gid]->ginfo.cls.tgid = TOPCG_CLASS;
    cgutil_vaddr[gid]->ginfo.cls.maxlevel = 0;

    cgutil_vaddr[gid]->ginfo.cls.percent = cgutil_opt.clspct;
    cgutil_vaddr[gid]->ginfo.cls.rempct = 100;

    sret = strncpy_s(cgutil_vaddr[gid]->grpname, GPNAME_LEN, cgutil_opt.clsname, GPNAME_LEN - 1);
    securec_check_errno(sret, , );

    cgutil_vaddr[gid]->ainfo.shares = DEFAULT_CPU_SHARES * cgutil_vaddr[gid]->ginfo.cls.percent / 10;

    cgutil_vaddr[gid]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, cgutil_vaddr[gid]->ginfo.cls.percent);

    sret = snprintf_s(cgutil_vaddr[gid]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[TOPCG_CLASS]->cpuset);

    securec_check_intval(sret, , );

    cgconf_update_class_percent();
}

/*
 * function name: cgconf_reset_class_group
 * description  : reset the class group fields when dropping a class
 *
 * Note: this function is called after dropping a Class group
 */
void cgconf_reset_class_group(int gid)
{
    int i;
    errno_t sret;

    for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (cgutil_vaddr[i]->ginfo.wd.cgid == gid) {
            sret = memset_s(cgutil_vaddr[i], sizeof(gscgroup_grp_t), 0, sizeof(gscgroup_grp_t));
            securec_check_errno(sret, , );
        }
    }

    sret = memset_s(cgutil_vaddr[gid], sizeof(gscgroup_grp_t), 0, sizeof(gscgroup_grp_t));
    securec_check_errno(sret, , );

    cgconf_update_class_percent();
}

/*
 * function name: cgconf_update_class_group
 * description  : update the class group value
 *
 * Note: this function is called after updating Class group
 */
void cgconf_update_class_group(gscgroup_grp_t* grp)
{
    grp->ginfo.cls.percent = cgutil_opt.clspct;

    grp->ainfo.shares = DEFAULT_CPU_SHARES * grp->ginfo.cls.percent / 10;

    grp->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, grp->ginfo.cls.percent);

    cgconf_update_class_percent();
}

/*
 * function name: cgconf_set_top_workload_group
 * description  : set the default value of top workload group
 *
 */
void cgconf_set_top_workload_group(int wdgid, int clsgid)
{
    char tmpstr[GPNAME_LEN];
    errno_t sret;

    cgutil_vaddr[wdgid]->used = 1;
    cgutil_vaddr[wdgid]->gid = wdgid;
    cgutil_vaddr[wdgid]->gtype = GROUP_DEFWD;
    cgutil_vaddr[wdgid]->ginfo.wd.cgid = clsgid;
    cgutil_vaddr[wdgid]->ginfo.wd.wdlevel = 1;
    cgutil_vaddr[wdgid]->ginfo.wd.percent = TOPWD_PERCENT;
    sret = snprintf_s(tmpstr, sizeof(tmpstr), sizeof(tmpstr) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, 1);
    securec_check_intval(sret, , );

    sret = strncpy_s(cgutil_vaddr[wdgid]->grpname, GPNAME_LEN, tmpstr, GPNAME_LEN - 1);
    securec_check_errno(sret, , );

    cgutil_vaddr[wdgid]->ainfo.shares = MAX_CLASS_CPUSHARES * TOPWD_PERCENT / 100;
    cgutil_vaddr[wdgid]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, TOPWD_PERCENT);
    cgutil_vaddr[wdgid]->percent = cgutil_vaddr[clsgid]->percent * cgutil_vaddr[wdgid]->ginfo.wd.percent / 100;
    /* set it's cpuset in configure file */
    sret = snprintf_s(cgutil_vaddr[wdgid]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[clsgid]->cpuset);
    securec_check_intval(sret, , );
}

/*
 * function name: cgconf_set_workload_group
 * description  : set the workload group fields when creating new workload group
 *
 * Note: this function is called after creating new Workload group
 */
void cgconf_set_workload_group(int wdgid, int clsgid)
{
    char tmpstr[GPNAME_LEN];

    errno_t sret;

    cgutil_vaddr[wdgid]->used = 1;
    cgutil_vaddr[wdgid]->gid = wdgid;
    cgutil_vaddr[wdgid]->gtype = GROUP_DEFWD;
    cgutil_vaddr[wdgid]->ginfo.wd.cgid = clsgid;
    cgutil_vaddr[wdgid]->ginfo.wd.wdlevel = cgutil_vaddr[clsgid]->ginfo.cls.maxlevel + 1;
    cgutil_vaddr[wdgid]->ginfo.wd.percent = cgutil_opt.grppct;
    cgutil_vaddr[clsgid]->ginfo.cls.rempct -= cgutil_opt.grppct;
    sret = snprintf_s(
        tmpstr, sizeof(tmpstr), sizeof(tmpstr) - 1, "%s:%d", cgutil_opt.wdname, cgutil_vaddr[wdgid]->ginfo.wd.wdlevel);
    securec_check_intval(sret, , );
    sret = strncpy_s(cgutil_vaddr[wdgid]->grpname, GPNAME_LEN, tmpstr, GPNAME_LEN - 1);
    securec_check_intval(sret, , );

    cgutil_vaddr[wdgid]->ainfo.shares = MAX_CLASS_CPUSHARES * cgutil_vaddr[wdgid]->ginfo.wd.percent / 100;
    cgutil_vaddr[wdgid]->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, cgutil_vaddr[wdgid]->ginfo.wd.percent);
    cgutil_vaddr[wdgid]->percent = cgutil_vaddr[clsgid]->percent * cgutil_vaddr[wdgid]->ginfo.wd.percent / 100;

    sret = snprintf_s(cgutil_vaddr[wdgid]->cpuset, CPUSET_LEN, CPUSET_LEN - 1, "%s", cgutil_vaddr[clsgid]->cpuset);

    securec_check_intval(sret, , );
}

/*
 * function name: cgconf_reset_workload_group
 * description  : reset the workload group fields when dropping a workload
 *
 * Note: this function is called after dropping a Workload group
 */
void cgconf_reset_workload_group(int wdgid)
{
    errno_t sret;
    sret = memset_s(cgutil_vaddr[wdgid], sizeof(gscgroup_grp_t), 0, sizeof(gscgroup_grp_t));
    securec_check_errno(sret, , );
}

/*
 * function name: cgconf_update_workload_group
 * description  : update the workload group value
 * Note: this function is called after updating Workload group
 */
void cgconf_update_workload_group(gscgroup_grp_t* grp)
{
    int clsgid = grp->ginfo.wd.cgid;

    cgutil_vaddr[clsgid]->ginfo.cls.rempct += grp->ginfo.wd.percent;
    grp->ginfo.wd.percent = cgutil_opt.grppct;
    cgutil_vaddr[clsgid]->ginfo.cls.rempct -= cgutil_opt.grppct;

    grp->ainfo.shares = MAX_CLASS_CPUSHARES * grp->ginfo.wd.percent / 100;
    grp->ainfo.weight = IO_WEIGHT_CALC(MAX_IO_WEIGHT, grp->ginfo.wd.percent);
    grp->percent = cgutil_vaddr[clsgid]->percent * grp->ginfo.wd.percent / 100;
}

/*
 * function name: cgconf_convert_group
 * description  : fill the old group inforation into new group
 *
 */
void cgconf_convert_group(gscgroup_grp_t* newgrp, gscgroup_old_grp_t* oldgrp)
{
    errno_t sret;

    newgrp->used = oldgrp->used;
    newgrp->gid = oldgrp->gid;

    newgrp->gtype = oldgrp->gtype;

    /* set the group internal info */
    newgrp->ginfo.cls.tgid = oldgrp->ginfo.cls.tgid;
    newgrp->ginfo.cls.maxlevel = oldgrp->ginfo.cls.maxlevel;
    newgrp->ginfo.cls.percent = oldgrp->ginfo.cls.percent;
    newgrp->ginfo.cls.rempct = oldgrp->ginfo.cls.rempct;

    sret = strncpy_s(newgrp->grpname, sizeof(newgrp->grpname), oldgrp->grpname, sizeof(oldgrp->grpname) - 1);
    securec_check_errno(sret, , );

    /* set the allocation info */
    newgrp->ainfo.shares = oldgrp->ainfo.shares;
    newgrp->ainfo.weight = oldgrp->ainfo.weight;
    newgrp->ainfo.quota = oldgrp->ainfo.quota;

    sret = strncpy_s(newgrp->ainfo.iopsread,
        sizeof(newgrp->ainfo.iopsread),
        oldgrp->ainfo.iopsread,
        sizeof(oldgrp->ainfo.iopsread) - 1);
    securec_check_errno(sret, , );
    sret = strncpy_s(newgrp->ainfo.iopswrite,
        sizeof(newgrp->ainfo.iopswrite),
        oldgrp->ainfo.iopswrite,
        sizeof(oldgrp->ainfo.iopswrite) - 1);
    securec_check_errno(sret, , );
    sret = strncpy_s(
        newgrp->ainfo.bpsread, sizeof(newgrp->ainfo.bpsread), oldgrp->ainfo.bpsread, sizeof(oldgrp->ainfo.bpsread) - 1);
    securec_check_errno(sret, , );
    sret = strncpy_s(newgrp->ainfo.bpswrite,
        sizeof(newgrp->ainfo.bpswrite),
        oldgrp->ainfo.bpswrite,
        sizeof(oldgrp->ainfo.bpswrite) - 1);
    securec_check_errno(sret, , );

    /* set the exception info */
    for (int i = 0; i < EXCEPT_ALL_KINDS; ++i) {
        newgrp->except[i].blocktime = oldgrp->except[i].blocktime;
        newgrp->except[i].elapsedtime = oldgrp->except[i].elapsedtime;
        newgrp->except[i].allcputime = oldgrp->except[i].allcputime;
        newgrp->except[i].qualitime = oldgrp->except[i].qualitime;
        newgrp->except[i].skewpercent = oldgrp->except[i].skewpercent;
    }

    sret = strncpy_s(newgrp->cpuset, sizeof(newgrp->cpuset), oldgrp->cpuset, sizeof(oldgrp->cpuset) - 1);
    securec_check_errno(sret, , );

    newgrp->percent = (unsigned int)oldgrp->percent;
}

/*
 * function name: cgconf_generate_file_by_root
 * description  : generate the configuration file by root user
 *
 * Note: the configuration file must exist in the "etc" directory
 */
int cgconf_generate_file_by_root(long fsize, char* cfgpath)
{
    void* vaddr = NULL;
    errno_t sret;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);

    /* when deleting the configure file, it must exist! */
    if (cgutil_opt.dflag && (-1 == fsize)) {
        fprintf(stderr, "ERROR: the user %s doesn't exist.\n", cgutil_opt.user);
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    }

    /* file doesn't exist, create new one */
    if (fsize == -1) {
        vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
        if (NULL == vaddr) {
            fprintf(stderr, "ERROR: failed to create and map the configure file %s!\n", cfgpath);
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        sret = memset_s(vaddr, cglen, 0, cglen);
        securec_check_errno(sret, free(cfgpath), -1);

        /* rewrite the mapping file */
        cgconf_generate_default_config_file(vaddr);
    } else {
        fprintf(stderr, "ERROR: the file %s has been corrupted, Please remove it and recreate.\n", cfgpath);
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    }

    free(cfgpath);
    cfgpath = NULL;
    return 0;
}

/*
 * function name: cgconf_generate_file_by_user
 * description  : generate the configuration file by non-root user
 *
 * Note: the configuration file must exist in the "etc" directory
 */
int cgconf_generate_file_by_user(long fsize, char* cfgpath)
{
    void* vaddr = NULL;
    errno_t sret;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);

    if (fsize == -1) {
        if ('\0' == cgutil_opt.nodegroup[0]) {
            fprintf(stderr,
                "ERROR: the configure file %s doesn't exist!\n"
                "HINT: please create it by root user!\n",
                cfgpath);
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        if (cgutil_opt.nodegroup[0] && 0 == cgutil_opt.cflag) {
            fprintf(stderr,
                "ERROR: the configure file %s doesn't exist!\n"
                "HINT: please create it before using it!\n",
                cfgpath);
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        } else {
            /* change origin cluster to virtual cluster */
            if (cgutil_opt.rename) {
                int old_cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 +
                                      sizeof(GSCFG_PREFIX) + 1 + strlen(cgutil_passwd_user->pw_name) +
                                      sizeof(GSCFG_SUFFIX) + 1;
                char* old_cfgpath = (char*)malloc(old_cfgpath_len);
                if (old_cfgpath == NULL) {
                    free(cfgpath);
                    cfgpath = NULL;
                    return -1;
                }

                sret = snprintf_s(old_cfgpath,
                    old_cfgpath_len,
                    old_cfgpath_len - 1,
                    "%s/%s/%s_%s%s",
                    cgutil_opt.hpath,
                    GSCGROUP_CONF_DIR,
                    GSCFG_PREFIX,
                    cgutil_passwd_user->pw_name,
                    GSCFG_SUFFIX);
                if (sret != EOK) {
                    fprintf(stderr, "ERROR: failed to construct old cgroup config path");
                    free(old_cfgpath);
                    old_cfgpath = NULL;
                    free(cfgpath);
                    cfgpath = NULL;
                    return -1;
                }

                /* rename the old cfgpath to new cfgpath */
                int ret = rename(old_cfgpath, cfgpath);
                if (ret != 0) {
                    fprintf(stderr, "ERROR: failed to rename %s to %s!\n", cfgpath, old_cfgpath);
                    free(old_cfgpath);
                    old_cfgpath = NULL;
                    free(cfgpath);
                    cfgpath = NULL;
                    return -1;
                }

                /* reset configure path */
                free(cfgpath);
                cfgpath = NULL;
                cfgpath = old_cfgpath;
            }

            vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
            if (NULL == vaddr) {
                fprintf(stderr, "ERROR: failed to create and map the configure file %s!\n", cfgpath);
                free(cfgpath);
                cfgpath = NULL;
                return -1;
            }

            sret = memset_s(vaddr, cglen, 0, cglen);
            securec_check_errno(sret, free(cfgpath), -1);
            /* rewrite the mapping file */
            cgconf_generate_default_config_file(vaddr);
        }
    } else {
        fprintf(stderr,
            "ERROR: the configure file size cannot match the current cgroup!\n"
            "HINT: please remove the configure file %s and recreate it!\n",
            cfgpath);
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    }

    free(cfgpath);
    cfgpath = NULL;
    return 0;
}

/*
 * function name: cgconf_parse_config_file
 * description  : parse the configuration file and set the global variable
 *
 * Note: the configuration file must exist in the "etc" directory
 */
int cgconf_parse_nodegroup_config_file(void)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    int i = 0;
    char* cfgpath = NULL;
    size_t cfgpath_len;
    errno_t sret;

    if ('\0' == cgutil_opt.nodegroup[0]) {
        fprintf(stderr, "ERROR: the nodegroup should be specified!\n");
        return -1;
    }

    cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + strlen(cgutil_opt.nodegroup) + 1 +
                  sizeof(GSCFG_PREFIX) + 1 + strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + 1;
    cfgpath = (char*)malloc(cfgpath_len);
    if (cfgpath == NULL) {
        return -1;
    }

    /* get the etc directory */
    sret = snprintf_s(cfgpath,
        cfgpath_len,
        cfgpath_len - 1,
        "%s/%s/%s.%s_%s%s",
        cgutil_opt.hpath,
        GSCGROUP_CONF_DIR,
        cgutil_opt.nodegroup,
        GSCFG_PREFIX,
        cgutil_passwd_user->pw_name,
        GSCFG_SUFFIX);
    securec_check_intval(sret, free(cfgpath), -1);
    /* get the configure file */
    fsize = gsutil_filesize(cfgpath);
    /* configure file doesn't exist or size is not the same */
    if (-1 == fsize || fsize != (long)cglen) {
        fprintf(stderr,
            "ERROR: the nodegroup configure file doesn't exist or "
            "the size of the nodegroup configure file doesn't match!\n");
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    }

    vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
    if (NULL == vaddr) {
        fprintf(stderr, "failed to create and map the configure file %s!\n", cfgpath);
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    }

    for (i = 0; i < GSCGROUP_ALLNUM; i++) {
        cgutil_vaddr[i] = (gscgroup_grp_t*)vaddr + i;

        if (i == TOPCG_CLASS) {
            sret = strcpy_s(cgutil_vaddr[i]->grpname, GPNAME_LEN, cgutil_opt.nodegroup);
            securec_check_intval(sret, free(cfgpath), -1);
        }
    }

    free(cfgpath);
    cfgpath = NULL;
    return 0;
}

/*
 * function name: cgconf_get_config_path
 * description  : get the configuration file
 *
 * Note: the configuration file must exist in the "etc" directory
 */
char* cgconf_get_config_path(bool backup)
{
    char* cfgpath = NULL;
    size_t cfgpath_len;
    errno_t sret;

    if ('\0' == cgutil_opt.nodegroup[0]) {
        if (false == backup) {
            cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + sizeof(GSCFG_PREFIX) + 1 +
                          strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + 1;
        } else {
            cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + sizeof(GSCFG_PREFIX) + 1 +
                          strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + sizeof(GSCFG_BACKUP) + 1;
        }
    } else {
        if (false == backup) {
            cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + strlen(cgutil_opt.nodegroup) +
                          1 + sizeof(GSCFG_PREFIX) + 1 + strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + 1;
        } else {
            cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + strlen(cgutil_opt.nodegroup) +
                          1 + sizeof(GSCFG_PREFIX) + 1 + strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) +
                          sizeof(GSCFG_BACKUP) + 1;
        }
    }

    cfgpath = (char*)malloc(cfgpath_len);
    if (cfgpath == NULL) {
        return NULL;
    }

    /* get the etc directory */
    if ('\0' == cgutil_opt.nodegroup[0]) {
        if (false == backup) {
            sret = snprintf_s(cfgpath,
                cfgpath_len,
                cfgpath_len - 1,
                "%s/%s/%s_%s%s",
                cgutil_opt.hpath,
                GSCGROUP_CONF_DIR,
                GSCFG_PREFIX,
                cgutil_passwd_user->pw_name,
                GSCFG_SUFFIX);
        } else {
            sret = snprintf_s(cfgpath,
                cfgpath_len,
                cfgpath_len - 1,
                "%s/%s/%s_%s%s%s",
                cgutil_opt.hpath,
                GSCGROUP_CONF_DIR,
                GSCFG_PREFIX,
                cgutil_passwd_user->pw_name,
                GSCFG_SUFFIX,
                GSCFG_BACKUP);
        }
    } else {
        if (false == backup) {
            sret = snprintf_s(cfgpath,
                cfgpath_len,
                cfgpath_len - 1,
                "%s/%s/%s.%s_%s%s",
                cgutil_opt.hpath,
                GSCGROUP_CONF_DIR,
                cgutil_opt.nodegroup,
                GSCFG_PREFIX,
                cgutil_passwd_user->pw_name,
                GSCFG_SUFFIX);
        } else {
            sret = snprintf_s(cfgpath,
                cfgpath_len,
                cfgpath_len - 1,
                "%s/%s/%s.%s_%s%s%s",
                cgutil_opt.hpath,
                GSCGROUP_CONF_DIR,
                cgutil_opt.nodegroup,
                GSCFG_PREFIX,
                cgutil_passwd_user->pw_name,
                GSCFG_SUFFIX,
                GSCFG_BACKUP);
        }
    }

    securec_check_intval(sret, free(cfgpath), NULL);

    return cfgpath;
}

bool cgconf_gid_invalid(void)
{
    if (cgutil_vaddr[TOPCG_ROOT] == NULL || cgutil_vaddr[TOPCG_GAUSSDB] == NULL ||
        cgutil_vaddr[TOPCG_BACKEND] == NULL || cgutil_vaddr[BACKENDCG_START_ID] == NULL ||
        cgutil_vaddr[BACKENDCG_START_ID + 1] == NULL || cgutil_vaddr[TOPCG_CLASS] == NULL ||
        cgutil_vaddr[CLASSCG_START_ID] == NULL || cgutil_vaddr[WDCG_START_ID] == NULL ||
        cgutil_vaddr[TSCG_START_ID] == NULL) {
        return true;
    }
    if (cgutil_vaddr[TOPCG_ROOT]->gid != TOPCG_ROOT || cgutil_vaddr[TOPCG_GAUSSDB]->gid != TOPCG_GAUSSDB ||
        cgutil_vaddr[TOPCG_BACKEND]->gid != TOPCG_BACKEND ||
        cgutil_vaddr[BACKENDCG_START_ID]->gid != BACKENDCG_START_ID ||
        cgutil_vaddr[BACKENDCG_START_ID + 1]->gid != (BACKENDCG_START_ID + 1) ||
        cgutil_vaddr[TOPCG_CLASS]->gid != TOPCG_CLASS || cgutil_vaddr[CLASSCG_START_ID]->gid != CLASSCG_START_ID ||
        cgutil_vaddr[WDCG_START_ID]->gid != WDCG_START_ID || cgutil_vaddr[TSCG_START_ID]->gid != TSCG_START_ID) {
        return true;
    }
    return false;
}

/*
 * function name: cgconf_parse_config_file
 * description  : parse the configuration file and set the global variable
 * Note: the configuration file must exist in the "etc" directory
 */
int cgconf_parse_config_file(void)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    int i = 0;
    int ret = -1;
    char* cfgpath = NULL;

    /* get the configure path */
    cfgpath = cgconf_get_config_path(false);
    if (NULL == cfgpath) {
        return -1;
    }

    fsize = gsutil_filesize(cfgpath);
    /* configure file doesn't exist or size is not the same*/
    if (-1 == fsize || fsize != (long)cglen) {
        if (geteuid() == 0) {
            ret = cgconf_generate_file_by_root(fsize, cfgpath);
        } else {
            if (cgutil_opt.cflag && *cgutil_opt.nodegroup && *cgutil_opt.clsname == '\0')
                ret = cgconf_generate_file_by_user(fsize, cfgpath);
            else if (*cgutil_opt.nodegroup) {
                free(cfgpath);
                cfgpath = NULL;
                fprintf(stderr, "ERROR: the specified node group %s doesn't exist!\n", cgutil_opt.nodegroup);
            } else {
                free(cfgpath);
                cfgpath = NULL;
            }
        }

        if (-1 == ret) {
            return -1; /* cfgpath has been freed, no need free here */
        }
    } else {
        vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);

        if (NULL == vaddr) {
            fprintf(stderr, "failed to create and map the configure file %s!\n", cfgpath);
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        for (i = 0; i < GSCGROUP_ALLNUM; i++) {
            cgutil_vaddr[i] = (gscgroup_grp_t*)vaddr + i;
            if (cgutil_vaddr[i]->gid >= GSCGROUP_ALLNUM) {
                fprintf(stderr, "cgroup gid in configure file %s is out of range !\n", cfgpath);
                free(cfgpath);
                cfgpath = NULL;
                return -1;
            }
        }
        if (cgconf_gid_invalid()) {
            fprintf(stderr, "cgroup gid in configure file %s is invalid !\n", cfgpath);
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        free(cfgpath);
        cfgpath = NULL;
    }
    return 0;
}

/*
 * function name: cgconf_map_nodegroup_conffile
 * description  : return the mapping information of original configuration file
 *
 */
void* cgconf_map_nodegroup_conffile(void)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    char* cfgpath = NULL;
    size_t cfgpath_len;
    errno_t sret;
    cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + strlen(cgutil_opt.nodegroup) + 1 +
                  sizeof(GSCFG_PREFIX) + 1 + strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + 1;

    cfgpath = (char*)malloc(cfgpath_len);
    if (NULL == cfgpath) {
        return NULL;
    }

    /* get the etc directory */
    sret = snprintf_s(cfgpath,
        cfgpath_len,
        cfgpath_len - 1,
        "%s/%s/%s.%s_%s%s",
        cgutil_opt.hpath,
        GSCGROUP_CONF_DIR,
        cgutil_opt.nodegroup,
        GSCFG_PREFIX,
        cgutil_passwd_user->pw_name,
        GSCFG_SUFFIX);
    securec_check_intval(sret, free(cfgpath), NULL);
    fsize = gsutil_filesize(cfgpath);
    /* make sure that the file exists when recovering */
    if (-1 == fsize) {
        free(cfgpath);
        cfgpath = NULL;
        return NULL;
    }

    /* configure file doesn't exist or size is not the same */
    vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
    free(cfgpath);
    cfgpath = NULL;

    return vaddr;
}

/*
 * function name: cgconf_map_origin_conffile
 * description  : return the mapping information of original configuration file
 */
void* cgconf_map_origin_conffile(void)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    char* cfgpath = NULL;
    size_t cfgpath_len;
    errno_t sret;

    cfgpath_len = strlen(cgutil_opt.hpath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + sizeof(GSCFG_PREFIX) + 1 +
                  strlen(cgutil_passwd_user->pw_name) + sizeof(GSCFG_SUFFIX) + 1;

    cfgpath = (char*)malloc(cfgpath_len);
    if (NULL == cfgpath) {
        return NULL;
    }

    sret = snprintf_s(cfgpath,
        cfgpath_len,
        cfgpath_len - 1,
        "%s/%s/%s_%s%s",
        cgutil_opt.hpath,
        GSCGROUP_CONF_DIR,
        GSCFG_PREFIX,
        cgutil_passwd_user->pw_name,
        GSCFG_SUFFIX);

    securec_check_intval(sret, free(cfgpath), NULL);
    fsize = gsutil_filesize(cfgpath);
    /* make sure that the file exists when recovering */
    if (-1 == fsize) {
        free(cfgpath);
        cfgpath = NULL;
        return NULL;
    }

    /* configure file doesn't exist or size is not the same */
    vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
    free(cfgpath);
    cfgpath = NULL;

    return vaddr;
}

/*
 * function name: cgconf_map_backup_conffile
 * description  : return the mapping information of backup file
 */
void* cgconf_map_backup_conffile(bool flag)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    char* cfgpath = NULL;

    /* get the configure path */
    cfgpath = cgconf_get_config_path(true);
    if (NULL == cfgpath) {
        return NULL;
    }

    fsize = gsutil_filesize(cfgpath);
    /* make sure that the file exists when recovering */
    if (flag == false && -1 == fsize) {
        free(cfgpath);
        cfgpath = NULL;
        return NULL;
    }

    /* configure file doesn't exist or size is not the same*/
    vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, cgutil_passwd_user);
    free(cfgpath);
    cfgpath = NULL;

    return vaddr;
}

/*
 * function name: cgconf_backup_config_file
 * description  : backup the configuration file when creating/dropping/updating cgroups
 *
 * Note: the configuration file must exist in the "etc" directory
 */
int cgconf_backup_config_file(void)
{
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    errno_t sret;

    if (geteuid() == 0) {
        return 0;
    }

    vaddr = cgconf_map_backup_conffile(true);
    if (NULL == vaddr) {
        fprintf(stderr, "failed to create and map the backup configure file!\n");
        return -1;
    }

    sret = memcpy_s(vaddr, cglen, cgutil_vaddr[0], cglen);
    securec_check_errno(sret, (void)munmap(vaddr, cglen);, -1);

    (void)munmap(vaddr, cglen);

    return 0;
}

/*
 * function name: cgconf_remove_backup_conffile
 * description  : remove the backuping configuration file when creating/dropping/updating cgroups
 *
 * Note: the configuration file must exist in the "etc" directory
 */
void cgconf_remove_backup_conffile(void)
{
    char* cfgpath = NULL;

    if (geteuid() == 0) {
        return;
    }

    /* get the configure path */
    cfgpath = cgconf_get_config_path(true);
    if (NULL == cfgpath) {
        return;
    }

    (void)unlink(cfgpath);

    free(cfgpath);
    cfgpath = NULL;
}

#define CGCONFIG_DISPLAY_CPU_QUOTA(cg)                                        \
    {                                                                         \
        if ((cg) && (cg)->ainfo.quota) {                                      \
            fprintf(stdout, " Quota(%%): %2d", cgutil_vaddr[i]->ainfo.quota); \
        }                                                                     \
        fprintf(stdout, " Cores: %s", cgutil_vaddr[i]->cpuset);               \
    }
/*
 * function name: cgconf_display_exception_detail
 * description  : display the group exception detail information
 *
 */
static void cgconf_display_exception_detail(int gid, int kinds)
{
    int i = 0;

    for (i = 0; i < kinds; ++i) {
        if (gsutil_exception_kind_is_valid(cgutil_vaddr[gid], i) == 0)
            continue;

        if (i == EXCEPT_ABORT) {
            fprintf(stdout, "%s: ", gsutil_print_exception_flag(i));
            if (cgutil_vaddr[gid]->except[i].blocktime > 0)
                fprintf(stdout, "BlockTime=%u ", cgutil_vaddr[gid]->except[i].blocktime);
            if (cgutil_vaddr[gid]->except[i].elapsedtime > 0)
                fprintf(stdout, "ElapsedTime=%u ", cgutil_vaddr[gid]->except[i].elapsedtime);
            if (cgutil_vaddr[gid]->except[i].spoolsize > 0)
                fprintf(stdout, "SpillSize=%ld ", cgutil_vaddr[gid]->except[i].spoolsize);
            if (cgutil_vaddr[gid]->except[i].broadcastsize > 0)
                fprintf(stdout, "BroadcastSize=%ld ", cgutil_vaddr[gid]->except[i].broadcastsize);
            if (cgutil_vaddr[gid]->except[i].allcputime > 0)
                fprintf(stdout, "AllCpuTime=%u ", cgutil_vaddr[gid]->except[i].allcputime);
            if (cgutil_vaddr[gid]->except[i].qualitime > 0)
                fprintf(stdout, "QualificationTime=%u ", cgutil_vaddr[gid]->except[i].qualitime);
            if (cgutil_vaddr[gid]->except[i].skewpercent > 0)
                fprintf(stdout, "CPUSkewPercent=%u ", cgutil_vaddr[gid]->except[i].skewpercent);
        } else {
            fprintf(stdout, "%s: ", gsutil_print_exception_flag(i));
            if (cgutil_vaddr[gid]->except[i].allcputime > 0)
                fprintf(stdout, "AllCpuTime=%u ", cgutil_vaddr[gid]->except[i].allcputime);
            if (cgutil_vaddr[gid]->except[i].qualitime > 0)
                fprintf(stdout, "QualificationTime=%u ", cgutil_vaddr[gid]->except[i].qualitime);
            if (cgutil_vaddr[gid]->except[i].skewpercent > 0)
                fprintf(stdout, "CPUSkewPercent=%u ", cgutil_vaddr[gid]->except[i].skewpercent);
        }

        fprintf(stdout, "\n");
    }
}
/*
 * function name: cgconf_display_exception
 * description  : display the group exception information
 *
 */
static void cgconf_display_exception(void)
{
    int cls = 0;
    int wd = 0;
    int flag = 0;
    int pflag = 0;
    int kinds = EXCEPT_ALL_KINDS;

    fprintf(stdout, "\n\nGroup Exception information is listed:");

    /* check if the class exists */
    for (cls = CLASSCG_START_ID; cls <= CLASSCG_END_ID; cls++) {
        if (cgutil_vaddr[cls]->used == 0) {
            continue;
        }

        flag = 0;

        if (gsutil_exception_is_valid(cgutil_vaddr[cls], kinds) != 0) {
            fprintf(stdout,
                "\nGID: %3d Type: %-6s Class: %-16s\n",
                (int)cgutil_vaddr[cls]->gid,
                "EXCEPTION",
                cgutil_vaddr[cls]->grpname);

            cgconf_display_exception_detail(cls, kinds);

            ++flag;
            ++pflag;
        }

        for (wd = WDCG_START_ID; wd <= WDCG_END_ID; wd++) {
            if (cgutil_vaddr[wd]->used == 0 || cgutil_vaddr[wd]->ginfo.wd.cgid != cls ||
                gsutil_exception_is_valid(cgutil_vaddr[wd], kinds) == 0)
                continue;

            if (flag == 0) {
                /* display the Class group information */
                fprintf(stdout,
                    "\nGID: %3d Type: %-6s Class: %-16s",
                    (int)cgutil_vaddr[cls]->gid,
                    "EXCEPTION",
                    cgutil_vaddr[cls]->grpname);
                ++flag;
            }

            fprintf(stdout,
                "\nGID: %3d Type: %-6s Group: %s:%-16s\n",
                (int)cgutil_vaddr[wd]->gid,
                "EXCEPTION",
                cgutil_vaddr[cls]->grpname,
                cgutil_vaddr[wd]->grpname);

            cgconf_display_exception_detail(wd, kinds);

            ++pflag;
        }
    }

    if (pflag == 0) {
        fprintf(stdout, "\n");
    }
}

/*
 * function name: cgconf_display_groups
 * description  : display the configuration file information
 *
 */
void cgconf_display_groups(void)
{
    int i;

    /* display the top group information */
    fprintf(stdout, "\nTop Group information is listed:");

    for (i = 0; i <= TOPCG_END_ID; i++) {
        if ('\0' != cgutil_opt.nodegroup[0] && i != TOPCG_CLASS)
            continue;

        fprintf(stdout,
            "\nGID: %3d Type: %-6s Percent(%%): %4u(%3d) Name: %-20s ",
            cgutil_vaddr[i]->gid,
            cgconf_get_group_type(cgutil_vaddr[i]->gtype),
            cgutil_vaddr[i]->percent,
            cgutil_vaddr[i]->ginfo.top.percent,
            cgutil_vaddr[i]->grpname);

        CGCONFIG_DISPLAY_CPU_QUOTA(cgutil_vaddr[i]);
    }

    /* display the Backend group information */
    if ('\0' == cgutil_opt.nodegroup[0])
        fprintf(stdout, "\n\nBackend Group information is listed:");

    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used || '\0' != cgutil_opt.nodegroup[0])
            continue;

        fprintf(stdout,
            "\nGID: %3d Type: %-6s Name: %-16s "
            "TopGID: %3d Percent(%%): %3u(%2d)",
            cgutil_vaddr[i]->gid,
            cgconf_get_group_type(cgutil_vaddr[i]->gtype),
            cgutil_vaddr[i]->grpname,
            cgutil_vaddr[i]->ginfo.cls.tgid,
            cgutil_vaddr[i]->percent,
            cgutil_vaddr[i]->ginfo.cls.percent);

        CGCONFIG_DISPLAY_CPU_QUOTA(cgutil_vaddr[i]);
    }

    /* display the Class group information */
    fprintf(stdout, "\n\nClass Group information is listed:");

    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used)
            continue;

        fprintf(stdout,
            "\nGID: %3d Type: %-6s Name: %-16s TopGID: %3d "
            "Percent(%%): %3u(%2d) MaxLevel: %d RemPCT: %3d",
            cgutil_vaddr[i]->gid,
            cgconf_get_group_type(cgutil_vaddr[i]->gtype),
            cgutil_vaddr[i]->grpname,
            cgutil_vaddr[i]->ginfo.cls.tgid,
            cgutil_vaddr[i]->percent,
            cgutil_vaddr[i]->ginfo.cls.percent,
            cgutil_vaddr[i]->ginfo.cls.maxlevel,
            cgutil_vaddr[i]->ginfo.cls.rempct);

        CGCONFIG_DISPLAY_CPU_QUOTA(cgutil_vaddr[i]);
    }

    /* display the Workload group information */
    fprintf(stdout, "\n\nWorkload Group information is listed:");

    for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
        if (0 == cgutil_vaddr[i]->used ||
            0 == strncmp(cgutil_vaddr[i]->grpname, GSCGROUP_TOP_WORKLOAD, sizeof(GSCGROUP_TOP_WORKLOAD) - 1))
            continue;

        fprintf(stdout,
            "\nGID: %3d Type: %-6s Name: %-16s ClsGID: %3d "
            "Percent(%%): %3u(%2d) WDLevel: %2d",
            cgutil_vaddr[i]->gid,
            cgconf_get_group_type(cgutil_vaddr[i]->gtype),
            cgutil_vaddr[i]->grpname,
            cgutil_vaddr[i]->ginfo.wd.cgid,
            cgutil_vaddr[i]->percent,
            cgutil_vaddr[i]->ginfo.wd.percent,
            cgutil_vaddr[i]->ginfo.wd.wdlevel);

        CGCONFIG_DISPLAY_CPU_QUOTA(cgutil_vaddr[i]);
    }

    /* display the Timeshare group information */
    fprintf(stdout, "\n\nTimeshare Group information is listed:");

    for (i = TSCG_START_ID; i <= TSCG_END_ID; i++) {
        fprintf(stdout,
            "\nGID: %3d Type: %-6s Name: %-16s Rate: %d",
            cgutil_vaddr[i]->gid,
            cgconf_get_group_type(cgutil_vaddr[i]->gtype),
            cgutil_vaddr[i]->grpname,
            cgutil_vaddr[i]->ginfo.ts.rate);
    }

    cgconf_display_exception();

    fprintf(stdout, "\n");
    (void)fflush(stdout);
}
