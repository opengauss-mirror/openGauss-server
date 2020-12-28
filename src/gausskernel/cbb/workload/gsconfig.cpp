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
 * gsconfig.cpp
 *    common functions for processing the configuration file
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/gsconfig.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "workload/gscgroup.h"
#include "securec.h"

/*
 * function name: gscgroup_get_parent_wdcg_path
 * description  : return the parent path of specified workload
 * arguments    :
 *           cnt: the ID of workload group
 *         vaddr: the array of configuration group
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
char* gscgroup_get_parent_wdcg_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* group_name)
{
    gscgroup_grp_t* grp = vaddr[cnt];
    int cgid = grp->ginfo.wd.cgid;
    gscgroup_grp_t* cls_grp = vaddr[cgid];
    int level = grp->ginfo.wd.wdlevel;
    int j;
    char* relpath = NULL;
    char rempath[16];
    errno_t sret;

    char* nodegroup = group_name;

    if (nodegroup && 0 == strcmp(nodegroup, "installation")) { // DEFAULT_NODE_GROUP
        nodegroup = "Class";
    }

    if ((relpath = (char*)malloc(GPNAME_PATH_LEN)) == NULL) {
        return NULL;
    }

    /* nodegroup path */
    if (nodegroup != NULL) {
        sret = snprintf_s(
            relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s/%s/", vaddr[1]->grpname, nodegroup, cls_grp->grpname);
        securec_check_intval(sret, free(relpath), NULL);
    } else { /* The top path of class */
        sret = snprintf_s(relpath,
            GPNAME_PATH_LEN,
            GPNAME_PATH_LEN - 1,
            "%s/%s/%s/",
            vaddr[1]->grpname,
            GSCGROUP_TOP_CLASS,
            cls_grp->grpname);
        securec_check_intval(sret, free(relpath), NULL);
    }
    /* get the name of wd group */
    for (j = 1; j < level; j++) {
        /* add the remain path dir */
        sret = snprintf_s(rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d/", GSCGROUP_REMAIN_WORKLOAD, j);
        securec_check_intval(sret, free(relpath), NULL);
        sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
        securec_check_berrno(sret, free(relpath), NULL);
    }

    return relpath;
}

/*
 * function name: gscgroup_get_topts_path
 * description  : return the top timeshare path of specified class
 * arguments    :
 *           cnt: the ID of Class group
 *         vaddr: the array of configuration group
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
char* gscgroup_get_topts_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* group_name)
{
    gscgroup_grp_t* grp = vaddr[cnt];
    int level = grp->ginfo.cls.maxlevel;
    int j;
    char* relpath = NULL;
    char rempath[16];
    errno_t sret;
    const char* nodegroup = group_name;

    if (nodegroup && 0 == strcmp(nodegroup, "installation")) { // DEFAULT_NODE_GROUP
        nodegroup = "Class";
    }

    if ((relpath = (char*)malloc(GPNAME_PATH_LEN)) == NULL) {
        return NULL;
    }

    /* nodegroup path */
    if (nodegroup != NULL) {
        sret = snprintf_s(relpath,
            GPNAME_PATH_LEN,
            GPNAME_PATH_LEN - 1,
            "%s/%s/%s/",
            vaddr[TOPCG_GAUSSDB]->grpname,
            nodegroup,
            grp->grpname);
        securec_check_intval(sret, free(relpath), NULL);
    } else { /* The top path of class */
        sret = snprintf_s(relpath,
            GPNAME_PATH_LEN,
            GPNAME_PATH_LEN - 1,
            "%s/%s/%s/",
            vaddr[TOPCG_GAUSSDB]->grpname,
            GSCGROUP_TOP_CLASS,
            grp->grpname);
        securec_check_intval(sret, free(relpath), NULL);
    }
    /* get the name of wd group */
    for (j = 1; j <= level; j++) {
        /* add the remain path dir */
        sret = snprintf_s(rempath, sizeof(rempath), sizeof(rempath) - 1, "%s:%d/", GSCGROUP_REMAIN_WORKLOAD, j);
        securec_check_intval(sret, free(relpath), NULL);
        sret = strcat_s(relpath, GPNAME_PATH_LEN, rempath);
        securec_check_berrno(sret, free(relpath), NULL);
    }

    sret = strcat_s(relpath, GPNAME_PATH_LEN, GSCGROUP_TOP_TIMESHARE);
    securec_check_berrno(sret, free(relpath), NULL);

    return relpath;
}

/*
 * function name: gscgroup_get_relative_path
 * description  : return the relative path of specified group
 * arguments    :
 *           cnt: the ID of any group
 *         vaddr: the array of configuration group
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
char* gscgroup_get_relative_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* group_name)
{
    char* relpath = NULL;
    gscgroup_grp_t* grp = vaddr[cnt];
    errno_t sret;

    char* nodegroup = group_name;

    if (nodegroup && 0 == strcmp(nodegroup, "installation")) { // DEFAULT_NODE_GROUP
        nodegroup = "Class";
    }

    if (cnt == TOPCG_ROOT) { /* the root cgroup */
        relpath = strdup("");
    } else if (cnt == TOPCG_GAUSSDB) { /* the database top cgroup */
        relpath = strdup(grp->grpname);
    } else if (cnt >= WDCG_START_ID && cnt <= WDCG_END_ID) {
        relpath = gscgroup_get_parent_wdcg_path(cnt, vaddr, nodegroup);
        if (relpath == NULL) {
            return NULL;
        }

        sret = strcat_s(relpath, GPNAME_PATH_LEN, grp->grpname);
        securec_check_berrno(sret, free(relpath), NULL);
    } else {
        if ((relpath = (char*)malloc(GPNAME_PATH_LEN)) == NULL) {
            return NULL;
        }

        if (cnt > 1 && cnt <= TOPCG_END_ID) {
            sret = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", vaddr[1]->grpname, grp->grpname);
            securec_check_intval(sret, free(relpath), NULL);
        } else if (cnt >= BACKENDCG_START_ID && cnt <= BACKENDCG_END_ID) {
            sret = snprintf_s(relpath,
                GPNAME_PATH_LEN,
                GPNAME_PATH_LEN - 1,
                "%s/%s/%s",
                vaddr[1]->grpname,
                GSCGROUP_TOP_BACKEND,
                grp->grpname);
            securec_check_intval(sret, free(relpath), NULL);
        } else if (cnt >= CLASSCG_START_ID && cnt <= CLASSCG_END_ID) {
            /* nodegroup path */
            if (nodegroup != NULL) {
                sret = snprintf_s(relpath,
                    GPNAME_PATH_LEN,
                    GPNAME_PATH_LEN - 1,
                    "%s/%s/%s",
                    vaddr[1]->grpname,
                    nodegroup,
                    grp->grpname);
                securec_check_intval(sret, free(relpath), NULL);
            } else {
                sret = snprintf_s(relpath,
                    GPNAME_PATH_LEN,
                    GPNAME_PATH_LEN - 1,
                    "%s/%s/%s",
                    vaddr[1]->grpname,
                    GSCGROUP_TOP_CLASS,
                    grp->grpname);
                securec_check_intval(sret, free(relpath), NULL);
            }
        }
    }

    return relpath;
}
