/*
 * Copyright (c) 2019 Huawei Technologies Co.,Ltd.
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
 * cgexcp.cpp
 *    Cgroup exceptional data process
 *
 * IDENTIFICATION
 *    src/bin/gs_cgroup/cgexcp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <errno.h>
#include <libcgroup.h>
#include <linux/version.h>
#include <sys/utsname.h>

#include "cgutil.h"

#define EXCP_PARSE_KEY(p, val)                                                                                       \
    {                                                                                                                \
        char *tmp = NULL, *bad = NULL;                                                                               \
        tmp = strchr(p, '=');                                                                                        \
        *tmp++ = '\0';                                                                                               \
        val = (unsigned long)strtoul(tmp, &bad, 10);                                                                 \
        if (*tmp == '\0' || (bad && *bad)) {                                                                         \
            fprintf(stderr, "ERROR: Exception format string, value \"%s\" is invalid!\n", (*tmp == '\0') ? " " : tmp); \
            return (-1);                                                                                             \
        }                                                                                                            \
    }

/*
 * function name: cgexcp_skewpercent_is_invalid
 * description  : check skew percent whether is invalid
 * return value : 0: valid, 1: invalid
 */
static int cgexcp_skewpercent_is_invalid(const except_data_t* except)
{
    if ((except->skewpercent > 0 && except->qualitime > 0) || (except->skewpercent <= 0 && except->qualitime <= 0))
        return 0;

    return 1;
}

/*
 * function name: cgexcp_exception_save
 * description  : save the exceptional data into the config file
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int cgexcp_exception_save(gscgroup_grp_t* grp)
{
    char *p = NULL;
    char *q = NULL;
    char eflag;
    unsigned long val;
    int err = 0;
    p = cgutil_opt.edata;
    eflag = cgutil_opt.eflag;

    do {
        while (*p == ' ') {
            p++;
        }

        q = strchr(p, ',');
        if (q != NULL) {
            *q++ = '\0';
        }
        if (strncasecmp("BlockTime=", p, sizeof("BlockTime=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'BlockTime\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            if (IS_EXCEPT_FLAG(eflag, EXCEPT_PENALTY)) {
                fprintf(stderr,
                    "ERROR: threshold \'BlockTime\' "
                    "for \"penalty\" is invalid!\n");
                err = -1;
                break;
            }

            grp->except[eflag - 1].blocktime = (unsigned int)val;
        } else if (strncasecmp("ElapsedTime=", p, sizeof("ElapsedTime=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'ElapsedTime\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            if (IS_EXCEPT_FLAG(eflag, EXCEPT_PENALTY)) {
                fprintf(stderr,
                    "ERROR: threshold \'ElapsedTime\', "
                    "for \"penalty\" is invalid!\n");
                err = -1;
                break;
            }

            grp->except[eflag - 1].elapsedtime = (unsigned int)val;
        } else if (strncasecmp("SpillSize=", p, sizeof("SpillSize=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'SpillSize\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            if (IS_EXCEPT_FLAG(eflag, EXCEPT_PENALTY)) {
                fprintf(stderr,
                    "ERROR: threshold \'SpillSize\', "
                    "for \"penalty\" is invalid!\n");
                err = -1;
                break;
            }

            grp->except[eflag - 1].spoolsize = (int64)val;
        } else if (strncasecmp("BroadcastSize=", p, sizeof("BroadcastSize=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'BroadcastSize\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            if (IS_EXCEPT_FLAG(eflag, EXCEPT_PENALTY)) {
                fprintf(stderr,
                    "ERROR: threshold \'BroadcastSize\', "
                    "for \"penalty\" is invalid!\n");
                err = -1;
                break;
            }

            grp->except[eflag - 1].broadcastsize = (int64)val;
        } else if (strncasecmp("AllCpuTime=", p, sizeof("AllCpuTime=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'AllCpuTime\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            grp->except[eflag - 1].allcputime = (unsigned int)val;
        } else if (strncasecmp("QualificationTime=", p, sizeof("QualificationTime=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > UINT_MAX) {
                fprintf(stderr,
                    "ERROR: threshold \'QualificationTime\', "
                    "value limit exceeded, it should be 0~%u!\n",
                    UINT_MAX);
                err = -1;
                break;
            }

            grp->except[eflag - 1].qualitime = (unsigned int)val;
        } else if (strncasecmp("CPUSkewPercent=", p, sizeof("CPUSkewPercent=") - 1) == 0) {
            EXCP_PARSE_KEY(p, val);

            if (val > 100) {
                fprintf(stderr,
                    "ERROR: threshold \'CPUSkewPercent\', "
                    "value '%u' is invalid, it must be 0~100!\n",
                    (unsigned int)val);
                err = -1;
                break;
            }

            grp->except[eflag - 1].skewpercent = (unsigned int)val;
        } else {
            fprintf(stderr, "ERROR: exception key string '%s' doesn't be supported!\n", p);
            err = -1;
            break;
        }
        p = q;
    } while ((q != NULL) && *q);

    if (cgexcp_skewpercent_is_invalid(&grp->except[eflag - 1])) {
        grp->except[eflag - 1].skewpercent = 0;
        grp->except[eflag - 1].qualitime = 0;
        fprintf(stderr,
            "ERROR: exception key string '%s' is invalid, "
            "\'CPUSkewPercent\' must be specified together with \'QualificationTime\'!\n",
            cgutil_opt.edata);

        return -1;
    }

    return err;
}

/*
 * function name: cgexcp_class_exception
 * description  : deal with the class exception
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
int cgexcp_class_exception(void)
{
    int i;
    int cls = 0;
    int wd = 0;
    int cmp = -1;
    char* tmpstr = NULL;
    size_t wdname_len;

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (cgutil_vaddr[i]->used == 0)
            continue;

        if (0 == strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.clsname)) {
            cls = i;
            break;
        }
    }

    /* back up the config file */
    if (-1 == cgconf_backup_config_file()) {
        return -1;
    }

    if (cls) {
        if (IS_EXCEPT_FLAG(cgutil_opt.eflag, EXCEPT_ABORT) && cgutil_opt.wdname[0]) {
            wdname_len = strlen(cgutil_opt.wdname);
            tmpstr = strchr(cgutil_opt.wdname, ':');

            for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
                if (cgutil_vaddr[i]->used == 0 || cgutil_vaddr[i]->ginfo.wd.cgid != cls)
                    continue;

                /* workload name with level or no level */
                if (tmpstr != NULL)
                    cmp = strcmp(cgutil_vaddr[i]->grpname, cgutil_opt.wdname);
                else {
                    if (':' == cgutil_vaddr[i]->grpname[wdname_len])
                        cmp = strncmp(cgutil_vaddr[i]->grpname, cgutil_opt.wdname, wdname_len);
                }

                if (cmp == 0) {
                    wd = i;
                    break;
                }
            }

            if (wd) {
                if (-1 == cgexcp_exception_save(cgutil_vaddr[wd])) {
                    cgconf_remove_backup_conffile();
                    return -1;
                }
            } else {
                fprintf(stderr, "ERROR: the specified workload %s doesn't exist!\n", cgutil_opt.wdname);
                cgconf_remove_backup_conffile();
                return -1;
            }
        } else {
            if (-1 == cgexcp_exception_save(cgutil_vaddr[cls])) {
                cgconf_remove_backup_conffile();
                return -1;
            }
        }
    } else {
        fprintf(stderr, "ERROR: the specified class %s doesn't exist!\n", cgutil_opt.clsname);
        cgconf_remove_backup_conffile();
        return -1;
    }

    return 0;
}
