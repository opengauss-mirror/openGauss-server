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
 * gscgroup.cpp
 *     provide the external function for init/free/print cgroup and
 *   attach task into cgroup.
 *
 * IDENTIFICATION
 *      src/gausskernel/cbb/workload/gscgroup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <libcgroup.h>
#include <sys/mman.h> /* mmap */

#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "workload/commgr.h"
#include "workload/gscgroup.h"
#include "workload/workload.h"
#include "workload/ctxctl.h"

#define WLM_CGROUP_HASH_SIZE 32

/* the variable of TopWD cgroup structure */
static THR_LOCAL struct cgroup* gscgroup_topwd = NULL;

/* the variable to save the current cgroup structure */
static THR_LOCAL struct cgroup* gscgroup_curwd = NULL;

/*
 * **************** STATIC FUNCTIONS ************************
 */
/*
 * function name: gscgroup_parse_config_file
 * description  : parse the configuration file of Cgroups
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int gscgroup_parse_config_file(const char* ngname, gscgroup_grp_t** ngaddr)
{
    long fsize = 0;
    void* vaddr = NULL;
    size_t cglen = GSCGROUP_ALLNUM * sizeof(gscgroup_grp_t);
    int i = 0;
    int sret;
    char* cfgpath = NULL;
    char* exepath = NULL;
    char real_exepath[PATH_MAX + 1] = {'\0'};
    size_t cfgpath_len = 0;
    struct passwd* passwd_user = NULL;

    passwd_user = getpwuid(geteuid());
    if (!passwd_user) {
        ereport(WARNING,
            (errmsg("can't get the passwd_user by euid %u."
                    "please check the running user!",
                geteuid())));
        return -1;
    }

    exepath = gs_getenv_r("GAUSSHOME");
    if (!exepath || realpath(exepath, real_exepath) == NULL) {
        ereport(WARNING, (errmsg("environment variable $GAUSSHOME is not set!")));
        return -1;
    }
    
    check_backend_env(real_exepath);
    if (!ngname) {
        cfgpath_len = strlen(real_exepath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + sizeof(GSCFG_PREFIX) + 1 +
                      USERNAME_LEN + sizeof(GSCFG_SUFFIX) + 1;
    } else {
        cfgpath_len = strlen(real_exepath) + 1 + sizeof(GSCGROUP_CONF_DIR) + 1 + strlen(ngname) + 1 +
                      sizeof(GSCFG_PREFIX) + 1 + USERNAME_LEN + sizeof(GSCFG_SUFFIX) + 1;
    }

    cfgpath = (char*)malloc(cfgpath_len);
    if (cfgpath == NULL) {
        return -1;
    }

    /* get the etc directory */
    if (!ngname) {
        sret = snprintf_s(cfgpath,
            cfgpath_len,
            cfgpath_len - 1,
            "%s/%s/%s_%s%s",
            real_exepath,
            GSCGROUP_CONF_DIR,
            GSCFG_PREFIX,
            passwd_user->pw_name,
            GSCFG_SUFFIX);
    } else {
        sret = snprintf_s(cfgpath,
            cfgpath_len,
            cfgpath_len - 1,
            "%s/%s/%s.%s_%s%s",
            real_exepath,
            GSCGROUP_CONF_DIR,
            ngname,
            GSCFG_PREFIX,
            passwd_user->pw_name,
            GSCFG_SUFFIX);
    }

    securec_check_intval(sret, free(cfgpath), -1);

    fsize = gsutil_filesize(cfgpath);

    /* configure file doesn't exist or size is not the same */
    if (fsize != (long)cglen) {
        ereport(LOG,
            (errmsg("the configure file %s doesn't exist or "
                    "the size of configure file has changed. "
                    "Please create it by root user!",
                cfgpath)));
        free(cfgpath);
        cfgpath = NULL;
        return -1;
    } else {
        vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, passwd_user);

        if (NULL == vaddr) {
            ereport(LOG,
                (errmsg("failed to create and map "
                        "the configure file %s!",
                    cfgpath)));
            free(cfgpath);
            cfgpath = NULL;
            return -1;
        }

        for (i = 0; i < GSCGROUP_ALLNUM; i++) {
            if (NULL == ngaddr) {
                g_instance.wlm_cxt->gscgroup_vaddr[i] = (gscgroup_grp_t*)vaddr + i;
            } else {
                ngaddr[i] = (gscgroup_grp_t*)((gscgroup_grp_t*)vaddr + i);
            }
        }
    }

    free(cfgpath);
    cfgpath = NULL;
    return 0;
}

/*
 * function name: gscgroup_get_group_type
 * description  : return the group type string for later displaying
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
char* gscgroup_get_group_type(group_type gtype)
{
    if (gtype == GROUP_TOP) {
        return "Top";
    } else if (gtype == GROUP_CLASS) {
        return "CLASS";
    } else if (gtype == GROUP_BAKWD) {
        return "BAKWD";
    } else if (gtype == GROUP_DEFWD) {
        return "DEFWD";
    } else if (gtype == GROUP_TSWD) {
        return "TSWD";
    }

    return NULL;
}

static void gscgroup_print_exception_detail(StringInfoData* buf, int gid, int kinds)
{
    int i = 0;
    WLMNodeGroupInfo* MyNodeGroup = t_thrd.wlm_cxt.thread_node_group;

    for (i = 0; i < kinds; ++i) {
        if (MyNodeGroup->vaddr == NULL || gsutil_exception_kind_is_valid(MyNodeGroup->vaddr[gid], i) == 0) {
            continue;
        }

        if (i == EXCEPT_ABORT) {
            appendStringInfo(buf, _("%s: "), gsutil_print_exception_flag(i));
            if (MyNodeGroup->vaddr[gid]->except[i].blocktime > 0) {
                appendStringInfo(buf, _("BlockTime=%u "), MyNodeGroup->vaddr[gid]->except[i].blocktime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].elapsedtime > 0) {
                appendStringInfo(buf, _("ElapsedTime=%u "), MyNodeGroup->vaddr[gid]->except[i].elapsedtime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].spoolsize > 0) {
                appendStringInfo(buf, _("SpillSize=%ld "), MyNodeGroup->vaddr[gid]->except[i].spoolsize);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].broadcastsize > 0) {
                appendStringInfo(buf, _("BroadcastSize=%ld "), MyNodeGroup->vaddr[gid]->except[i].broadcastsize);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].allcputime > 0) {
                appendStringInfo(buf, _("AllCpuTime=%u "), MyNodeGroup->vaddr[gid]->except[i].allcputime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].qualitime > 0) {
                appendStringInfo(buf, _("QualificationTime=%u "), MyNodeGroup->vaddr[gid]->except[i].qualitime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].skewpercent > 0) {
                appendStringInfo(buf, _("CPUSkewPercent=%u "), MyNodeGroup->vaddr[gid]->except[i].skewpercent);
            }
        } else {
            appendStringInfo(buf, _("%s: "), gsutil_print_exception_flag(i));
            if (MyNodeGroup->vaddr[gid]->except[i].allcputime > 0) {
                appendStringInfo(buf, _("AllCpuTime=%u "), MyNodeGroup->vaddr[gid]->except[i].allcputime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].qualitime > 0) {
                appendStringInfo(buf, _("QualificationTime=%u "), MyNodeGroup->vaddr[gid]->except[i].qualitime);
            }
            if (MyNodeGroup->vaddr[gid]->except[i].skewpercent > 0) {
                appendStringInfo(buf, _("CPUSkewPercent=%u "), MyNodeGroup->vaddr[gid]->except[i].skewpercent);
            }
        }

        appendStringInfo(buf, _("\n"));
    }
}

void gscgroup_print_exception(StringInfoData* buf)
{
    int cls = 0;
    int wd = 0;
    int flag = 0;
    int pflag = 0;
    int kinds = EXCEPT_ALL_KINDS;
    WLMNodeGroupInfo* MyNodeGroup = t_thrd.wlm_cxt.thread_node_group;

    appendStringInfo(buf, _("\n\nGroup Exception information is listed:"));

    /* check if the class exists */
    for (cls = CLASSCG_START_ID; cls <= CLASSCG_END_ID; cls++) {
#ifdef ENABLE_UT
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[cls] == NULL) {
            break;
        }
#endif
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[cls]->used == 0) {
            continue;
        }

        flag = 0;

        if (gsutil_exception_is_valid(MyNodeGroup->vaddr[cls], kinds) != 0) {
            appendStringInfo(buf,
                _("\nGID: %3d Type: %-6s Class: %-16s\n"),
                MyNodeGroup->vaddr[cls]->gid,
                "EXCEPTION",
                MyNodeGroup->vaddr[cls]->grpname);

            gscgroup_print_exception_detail(buf, cls, kinds);

            ++flag;
            ++pflag;
        }

        for (wd = WDCG_START_ID; wd <= WDCG_END_ID; wd++) {
#ifdef ENABLE_UT
            if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[wd] == NULL) {
                break;
            }
#endif
            if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[wd]->used == 0 || MyNodeGroup->vaddr[wd]->ginfo.wd.cgid != cls ||
                gsutil_exception_is_valid(MyNodeGroup->vaddr[wd], kinds) == 0) {
                continue;
            }

            if (flag == 0) {
                /* display the Class group information */
                appendStringInfo(buf,
                    _("\nGID: %3d Type: %-6s Class: %-16s"),
                    MyNodeGroup->vaddr[cls]->gid,
                    "EXCEPTION",
                    MyNodeGroup->vaddr[cls]->grpname);
                ++flag;
            }

            appendStringInfo(buf,
                _("\nGID: %3d Type: %-6s Group: %s:%-16s\n"),
                MyNodeGroup->vaddr[wd]->gid,
                "EXCEPTION",
                MyNodeGroup->vaddr[cls]->grpname,
                MyNodeGroup->vaddr[wd]->grpname);

            gscgroup_print_exception_detail(buf, wd, kinds);

            ++pflag;
        }
    }

    if (pflag == 0) {
        appendStringInfo(buf, _("\n"));
    }
}

/*
 * function name: gscgroup_print
 * description  : print the configuration file information
 *
 */
static char* gscgroup_print(void)
{
    StringInfoData buf;
    int i;
    WLMNodeGroupInfo* MyNodeGroup = t_thrd.wlm_cxt.thread_node_group;

    initStringInfo(&buf);

    if (buf.len != 0) {
        appendStringInfoChar(&buf, '\n');
    }

    if (g_instance.wlm_cxt->gscgroup_init_done != 1) {
        appendStringInfo(&buf,
            _("\nThere is no Cgroup configuration information "
              "for without initialization!"));
        return buf.data;
    }

    /* display the top group information */
    appendStringInfo(&buf, _("\nTop Group information is listed:"));

    for (i = 0; i <= TOPCG_END_ID; i++) {
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[i]->used == 0) {
            continue;
        }

        appendStringInfo(&buf,
            _("\nGID: %3d Type: %-6s Percent(%%): %4u(%3d) "
              "Name: %-20s"),
            MyNodeGroup->vaddr[i]->gid,
            gscgroup_get_group_type(MyNodeGroup->vaddr[i]->gtype),
            MyNodeGroup->vaddr[i]->percent,
            MyNodeGroup->vaddr[i]->ginfo.top.percent,
            MyNodeGroup->vaddr[i]->grpname);

        if (MyNodeGroup->vaddr[i]->ainfo.quota && MyNodeGroup->vaddr[i]->ainfo.quota > 0) {
            appendStringInfo(&buf, _("Quota(%%): %2d"), MyNodeGroup->vaddr[i]->ainfo.quota);
        }
    }

    /* display the Backend group information */
    appendStringInfo(&buf, _("\n\nBackend Group information is listed:"));

    for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; i++) {
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[i]->used == 0) {
            continue;
        }

        appendStringInfo(&buf,
            _("\nGID: %3d Type: %-6s Name: %-16s "
              "TopGID: %3d Percent(%%): %3u(%2d)"),
            MyNodeGroup->vaddr[i]->gid,
            gscgroup_get_group_type(MyNodeGroup->vaddr[i]->gtype),
            MyNodeGroup->vaddr[i]->grpname,
            MyNodeGroup->vaddr[i]->ginfo.cls.tgid,
            MyNodeGroup->vaddr[i]->percent,
            MyNodeGroup->vaddr[i]->ginfo.cls.percent);

        if (MyNodeGroup->vaddr[i]->ainfo.quota > 0) {
            appendStringInfo(&buf, _("Quota(%%): %2d"), MyNodeGroup->vaddr[i]->ainfo.quota);
        }
    }

    /* display the Class group information */
    appendStringInfo(&buf, _("\n\nClass Group information is listed:"));

    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[i]->used == 0) {
            continue;
        }

        appendStringInfo(&buf,
            _("\nGID: %3d Type: %-6s Name: %-16s TopGID: %3d "
              "Percent(%%): %3u(%2d) MaxLevel: %d RemPCT: %2d"),
            MyNodeGroup->vaddr[i]->gid,
            gscgroup_get_group_type(MyNodeGroup->vaddr[i]->gtype),
            MyNodeGroup->vaddr[i]->grpname,
            MyNodeGroup->vaddr[i]->ginfo.cls.tgid,
            MyNodeGroup->vaddr[i]->percent,
            MyNodeGroup->vaddr[i]->ginfo.cls.percent,
            MyNodeGroup->vaddr[i]->ginfo.cls.maxlevel,
            MyNodeGroup->vaddr[i]->ginfo.cls.rempct);

        if (MyNodeGroup->vaddr[i]->ainfo.quota > 0) {
            appendStringInfo(&buf, _("Quota(%%): %2d"), MyNodeGroup->vaddr[i]->ainfo.quota);
        }
    }

    /* display the Workload group information */
    appendStringInfo(&buf, _("\n\nWorkload Group information is listed:"));

    for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[i]->used == 0) {
            continue;
        }

        appendStringInfo(&buf,
            _("\nGID: %3d Type: %-6s Name: %-16s ClsGID: %3d "
              "Percent(%%): %3u(%2d) WDLevel: %d "),
            MyNodeGroup->vaddr[i]->gid,
            gscgroup_get_group_type(MyNodeGroup->vaddr[i]->gtype),
            MyNodeGroup->vaddr[i]->grpname,
            MyNodeGroup->vaddr[i]->ginfo.wd.cgid,
            MyNodeGroup->vaddr[i]->percent,
            MyNodeGroup->vaddr[i]->ginfo.wd.percent,
            MyNodeGroup->vaddr[i]->ginfo.wd.wdlevel);

        if (MyNodeGroup->vaddr[i]->ainfo.quota > 0) {
            appendStringInfo(&buf, _("Quota(%%): %2d"), MyNodeGroup->vaddr[i]->ainfo.quota);
        }
    }

    /* display the Timeshare group information */
    appendStringInfo(&buf, _("\n\nTimeshare Group information is listed:"));

    for (i = TSCG_START_ID; i <= TSCG_END_ID; i++) {
        if (MyNodeGroup->vaddr == NULL || MyNodeGroup->vaddr[i]->used == 0) {
            continue;
        }

        appendStringInfo(&buf,
            _("\nGID: %3d Type: %-6s Name: %-16s Rate: %d"),
            MyNodeGroup->vaddr[i]->gid,
            gscgroup_get_group_type(MyNodeGroup->vaddr[i]->gtype),
            MyNodeGroup->vaddr[i]->grpname,
            MyNodeGroup->vaddr[i]->ginfo.ts.rate);
    }

    gscgroup_print_exception(&buf);

    appendStringInfo(&buf, _("\n"));

    return buf.data;
}

/*
 * function name: gscgroup_initialize_hashtbl
 * description  : initialize the hash table for Cgroups
 *
 */
static void gscgroup_initialize_hashtbl(HTAB** htab)
{
    HASHCTL hash_ctl;

    errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(u_sess->wlm_cxt->group_keyname);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.entrysize = sizeof(gscgroup_entry_t);
    hash_ctl.hash = string_hash;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;

    *htab = hash_create("wlm cgroup hash table",
        WLM_CGROUP_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);
}

/*
 * function name: gscgroup_enter_hashtbl
 * description  : register the key-values into hash table
 * arguments    :
 *          name: the string of "classname:workloadname"
 *            cg: cgroup structure
 * return value :
 *            -1: abnormal
 *             0: normal
 *
 */
static int gscgroup_enter_hashtbl(WLMNodeGroupInfo* ng, const char* name, struct cgroup* cg)
{
    bool found = false;
    gscgroup_entry_t* cgentry = NULL;
    int sret;

    Assert(ng != NULL);

    if (ng->cgroups_htab == NULL || cg == NULL) {
        return -1;
    }

    USE_AUTO_LWLOCK(WorkloadCGroupHashLock, LW_EXCLUSIVE);

    cgentry = (gscgroup_entry_t*)hash_search(ng->cgroups_htab, name, HASH_ENTER_NULL, &found);
    if (cgentry == NULL) {
        ereport(LOG,
            (errmsg("Failed to allocate memory "
                    "when entering new hash value! ")));
        return -1;
    }

    if (!found) {
        cgentry->cg = cg;
        cgentry->percent = gscgroup_get_percent(ng, name);

        sret = snprintf_s(cgentry->name, sizeof(cgentry->name), sizeof(cgentry->name) - 1, "%s", name);
        securec_check_intval(sret, , -1);

        /* reset old cgroup info */
        cgentry->oldcg = NULL;
    } else {
        cgentry->oldcg = cgentry->cg;
        cgentry->cg = cg;
        cgentry->percent = gscgroup_get_percent(ng, name);

        sret = snprintf_s(cgentry->name, sizeof(cgentry->name), sizeof(cgentry->name) - 1, "%s", name);
        securec_check_intval(sret, , -1);
    }

    /* reset all cpu info */
    cgentry->cpuUsedAcct = 0;
    cgentry->cpuUtil = 0;
    cgentry->cpuCount = 0;
    cgentry->usedCpuCount = 0;
    cgentry->cpuLastAcct = 0;
    cgentry->lastTime = 0;

    return 0;
}

/*
 * function name: gscgroup_lookup_hashtbl
 * description  : retrieve cgroup structure based on keyname
 * arguments    :
 *          name: the string of "classname:workloadname"
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
gscgroup_entry_t* gscgroup_lookup_hashtbl(WLMNodeGroupInfo* ng, const char* name)
{
    gscgroup_entry_t* cgentry = NULL;

    if (ng->cgroups_htab == NULL || !StringIsValid(name)) {
        return NULL;
    }

    USE_AUTO_LWLOCK(WorkloadCGroupHashLock, LW_SHARED);

    cgentry = (gscgroup_entry_t*)hash_search(ng->cgroups_htab, name, HASH_FIND, NULL);

    return cgentry;
}

/*
 * function name: gscgroup_get_cgroup
 * description  : retrieve cgroup structure based on relative path
 * arguments    :
 *       relpath: the relative path of cgroup
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
static struct cgroup* gscgroup_get_cgroup(char* relpath)
{
    int ret = 0;
    struct cgroup* cg = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        ereport(WARNING, (errmsg("Cgroup %s failed to call cgroup_new_cgroup", relpath)));
        return NULL;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Cgroup get_cgroup %s information: %s(%d)", relpath, cgroup_strerror(ret), ret)));
        cgroup_free(&cg);
        return NULL;
    }

    return cg;
}

/*
 * function name: gscgroup_is_timeshare
 * description  : check if the specified name is timeshare Cgroup
 * arguments    :
 *         gname: group name
 * return value :
 *            -1: abnormal
 *         other: normal
 *
 */
int gscgroup_is_timeshare(const char* gname)
{
    int i = 0;

    if (g_instance.wlm_cxt->gscgroup_config_parsed <= 0) {
        return -1;
    }

    /* register all timeshare group of the class */
    for (i = TSCG_START_ID; i <= TSCG_END_ID; i++) {
        if (0 == strcmp(gname, g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname)) {
            break;
        }
    }

    if (i > TSCG_END_ID) {
        return -1;
    }

    return i;
}

/*
 * function name: gscgroup_is_class
 * description  : check if the specified name is class Cgroup
 * arguments    :
 *         gname: group name
 * return value :
 *            -1: abnormal
 *         other: normal
 *
 */
int gscgroup_is_class(WLMNodeGroupInfo* ng, const char* gname)
{
    int i = 0;

    if (ng == NULL) {
        ng = &g_instance.wlm_cxt->MyDefaultNodeGroup;
    }

    /* check if the class exists */
    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (ng->vaddr[i] == NULL) {
            continue;
        }
        if (ng->vaddr[i]->used && (0 == strcmp(ng->vaddr[i]->grpname, gname))) {
            break;
        }
    }

    if (i > CLASSCG_END_ID) {
        return -1;
    }

    return i;
}

/*
 * function name: gscgroup_is_workload
 * description  : check if the specified name is workload Cgroup
 * arguments    :
 *           cls: the class group id
 *         gname: group name
 * return value :
 *            -1: abnormal
 *         other: normal
 *
 */
int gscgroup_is_workload(WLMNodeGroupInfo* ng, int cls, const char* gname)
{
    int i = 0;
    int wd = -1;
    int cmp = -1;
    size_t len;
    char* tmpstr = NULL;

    if (ng == NULL) {
        ng = &g_instance.wlm_cxt->MyDefaultNodeGroup;
    }

    len = strlen(gname);
    tmpstr = strchr((char*)gname, ':');

    for (i = WDCG_START_ID; i <= WDCG_END_ID; i++) {
        if (ng->vaddr[i]->used == 0 || ng->vaddr[i]->ginfo.wd.cgid != cls) {
            continue;
        }

        /* workload name with level or no level */
        if (tmpstr != NULL) {
            if (ng->vaddr[i]->grpname[tmpstr - gname] ==  ':') {
                cmp = strncmp(ng->vaddr[i]->grpname, gname, tmpstr - gname);
            }
        } else {
            if (ng->vaddr[i]->grpname[len] == ':') {
                cmp = strncmp(ng->vaddr[i]->grpname, gname, len);
            }
        }

        if (cmp == 0) {
            wd = i;
            break;
        }
    }

    return wd;
}

/*
 * function name: gscgroup_check_level
 * description  : check whether workload level is larger than max level of the class
 * arguments    :
 *           cls: the class group id
 *         level: workload level
 * return value :
 *            -1: abnormal
 *         other: normal
 *
 */
int gscgroup_check_level(int cls, const char* level)
{
    int a = -1;
    char* bad = NULL;

    a = (int)strtol(level, &bad, 10);

    if (bad && *bad) {
        return -1;
    }

    if (a > MAX_WD_LEVEL || (a < 1)) {
        return -1;
    }

    return a;
}

/*
 * function name: gscgroup_lookup_cgroup
 * description  : retrieve the cgroup structure based on the control group name;
 *                If the control group doesn't exist, return the cgroup of
 *                default workload;
 *                If it can be found in the configuration file, it should enter
 *                the new workload cgroup into hash table;
 *                If the class of workload has been deleted, it should give
 *                an warning to mention that;
 * arguments    :
 *         gname: the control_group string in the resource pool
 *         found: find cgroup or not
 * return value :
 *         the default workload value if it can't be found;
 *         other: normal
 *
 */
struct cgroup* gscgroup_lookup_cgroup(WLMNodeGroupInfo* ng, const char* gname, bool* found)
{
    struct cgroup* cg = NULL;
    gscgroup_entry_t* cg_entry = NULL;
    char *p = NULL;
    char *q = NULL;
    int cls, sret;

    if (found != NULL) {
        *found = false;
    }

    gscgroup_topwd = g_instance.wlm_cxt->gscgroup_deftopwdcg;

    if (!StringIsValid(gname) || !CGroupIsValid(gname) || (ng == NULL && 0 == strcmp(gname, GSCGROUP_DEFAULT_CGNAME))) {
        return g_instance.wlm_cxt->gscgroup_defwdcg;
    }

    if ((ng == NULL) || ((cg_entry = gscgroup_lookup_hashtbl(ng, gname)) == NULL)) {
        return g_instance.wlm_cxt->gscgroup_defwdcg;
    }

    cg = cg_entry->cg;

    if (u_sess->attr.attr_resource.enable_cgroup_switch) {
        q = pstrdup(gname);

        p = strchr(q, ':');

        if (p && (*p++ = '\0', (cls = gscgroup_is_class(ng, q)) != -1)) {
            char keyname[GPNAME_LEN];

            /* retrieve the TopWD cgroup structure */
            sret = snprintf_s(keyname,
                sizeof(keyname),
                sizeof(keyname) - 1,
                "%s:%s:%d",
                ng->vaddr[cls]->grpname,
                GSCGROUP_TOP_WORKLOAD,
                WD_TOP_LEVEL);
            securec_check_intval(sret, pfree(q), cg);

            if ((cg_entry = gscgroup_lookup_hashtbl(ng, keyname)) != NULL) {
                gscgroup_topwd = cg_entry->cg;
            }
        }

        pfree(q);
    }

    if (found != NULL) {
        *found = true;
    }

    return cg;
}

static int GscgroupParseCpuinfo(const char* cpuset)
{
    int len = strlen(cpuset);
    int count = 0;
    int low = 0;
    int num = 0;
    bool is_range = false;
    int i;

    for (i = 0; i < len; i++) {
        if (cpuset[i] == ',') {
            if (is_range) {
                count += num - low + 1;
            } else {
                count += 1;
            }
            num = 0;
            is_range = false;
        } else if (cpuset[i] == '-') {
            low = num;
            num = 0;
            is_range = true;
        } else if (cpuset[i] >= '0' && cpuset[i] <= '9') {
            num = num * 10 + cpuset[i] - '0';
        } else {
            ereport(WARNING, (errmsg("Unknow character %c for cpuset.", cpuset[i])));
            return 1;
        }
    }

    if (is_range) {
        count += num - low + 1;
    } else {
        count += 1;
    }

    return (count == 0) ? 1 : count;
}

/*
 * function name: gscgroup_get_cgroup_cpuinfo
 * description  : get control group cpu info, include cpuset and cpuacct
 * arguments    :
 *                _in_ cg : control_group
 *                _out_ setcnt : how many cores of the control group
 *                _out_ usage : cpu usage of the control group
 * return value : -1:    abnormal
 *                other: normal
 *
 */
int gscgroup_get_cgroup_cpuinfo(struct cgroup* cg, int* setcnt, int64* usage)
{
    struct cgroup_controller* cgc_set = NULL;
    struct cgroup_controller* cgc_acct = NULL;
    char* cpuset = NULL;

    char* relpath = gsutil_get_cgroup_name(cg);

    if (relpath == NULL) {
        return -1;
    }

    /* create a new group with the path */
    struct cgroup* new_cg = cgroup_new_cgroup(relpath);

    if (new_cg == NULL) {
        return -1;
    }

    /* get all controller of the group */
    if (cgroup_get_cgroup(new_cg) != 0) {
        cgroup_free(&new_cg);
        return -1;
    }

    /* get the CPUSET controller and its value */
    if (setcnt && (cgc_set = cgroup_get_controller(new_cg, MOUNT_CPUSET_NAME)) != NULL &&
        cgroup_get_value_string(cgc_set, CPUSET_CPUS, &cpuset) == 0) {
        *setcnt = GscgroupParseCpuinfo(cpuset);
    }
    free(cpuset);

    /* get the CPUACCT controller and its value */
    if (usage && ((cgc_acct = cgroup_get_controller(new_cg, MOUNT_CPUACCT_NAME)) == NULL ||
                     cgroup_get_value_int64(cgc_acct, CPUACCT_USAGE, usage) != 0)) {
        cgroup_free(&new_cg);
        return -1;
    }

    cgroup_free(&new_cg);

    return 0;
}

/*
 * *************** EXTERNAL FUNCTION ********************************
 */
/*
 * function name: gscgroup_free
 * description  : free hash table and the mapping file when exiting process
 */
void gscgroup_free(void)
{
    /* Due to the threads framework, the postmaster main thread doesn't care
     * if other threads are running.
     * It may cause other threads to use the freed memory.
     * So this function skips the free operation and let OS to do that.
     */
}

/*
 * function name: gscgroup_build_cgroups_htab
 * description  : build cgroup hash table for one logical cluster
 */
int gscgroup_build_cgroups_htab(WLMNodeGroupInfo* ng)
{
    int sret;
    char *relpath = NULL;
    char *toppath = NULL;
    char keyname[GPNAME_LEN] = {0};
    char tmpname[GPNAME_LEN] = {0};
    struct cgroup* cg = NULL;

    for (int i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (ng->vaddr[i]->used) {
            if (i == CLASSCG_START_ID) {
                sret = snprintf_s(
                    tmpname, sizeof(tmpname), sizeof(tmpname) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, WD_TOP_LEVEL);
                securec_check_intval(sret, , -1);
            }

            relpath = gscgroup_get_relative_path(i, ng->vaddr, ng->group_name);
            if (!relpath) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for relative path of %s Group!",
                        ng->vaddr[i]->grpname)));
                return -1;
            }

            if (!(cg = gscgroup_get_cgroup(relpath))) {
                free(relpath);
                relpath = NULL;
                return -1;
            }

            (void)gscgroup_enter_hashtbl(ng, ng->vaddr[i]->grpname, cg);
            free(relpath);
            relpath = NULL;

            /* register all workload group of the class */
            for (int j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
                if (ng->vaddr[j]->used && ng->vaddr[j]->ginfo.wd.cgid == i) {
                    relpath = gscgroup_get_relative_path(j, ng->vaddr, ng->group_name);
                    if (!relpath) {
                        ereport(LOG,
                            (errmsg("Failed to allocate memory "
                                    "for relative path of %s Group!",
                                ng->vaddr[j]->grpname)));
                        return -1;
                    }

                    if (!(cg = gscgroup_get_cgroup(relpath))) {
                        free(relpath);
                        relpath = NULL;
                        return -1;
                    }

                    sret = snprintf_s(keyname,
                        sizeof(keyname),
                        sizeof(keyname) - 1,
                        "%s:%s",
                        ng->vaddr[i]->grpname,
                        ng->vaddr[j]->grpname);
                    securec_check_intval(sret, free(relpath), -1);

                    (void)gscgroup_enter_hashtbl(ng, gscgroup_convert_cgroup(keyname), cg);
                    free(relpath);
                    relpath = NULL;
                }
            }

            relpath = NULL;

            /* get the top timeshare path of this class */
            toppath = gscgroup_get_topts_path(i, ng->vaddr, ng->group_name);
            if (!toppath) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for Timeshare path of %s Group!",
                        ng->vaddr[i]->grpname)));
                return -1;
            }

            if (!(relpath = (char*)malloc(GPNAME_PATH_LEN))) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for timeshare path of %s Group!",
                        ng->vaddr[i]->grpname)));
                free(toppath);
                toppath = NULL;
                return -1;
            }

            /* register all timeshare group of the class */
            for (int j = TSCG_START_ID; j <= TSCG_END_ID; j++) {
                sret =
                    snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", toppath, ng->vaddr[j]->grpname);
                securec_check_intval(sret, free(relpath); free(toppath), -1);

                if (!(cg = gscgroup_get_cgroup(relpath))) {
                    free(relpath);
                    relpath = NULL;
                    free(toppath);
                    toppath = NULL;
                    return -1;
                }

                sret = snprintf_s(keyname,
                    sizeof(keyname),
                    sizeof(keyname) - 1,
                    "%s:%s",
                    ng->vaddr[i]->grpname,
                    ng->vaddr[j]->grpname);
                securec_check_intval(sret, free(relpath); free(toppath), -1);

                (void)gscgroup_enter_hashtbl(ng, gscgroup_convert_cgroup(keyname), cg);
            }

            free(relpath);
            relpath = NULL;
            free(toppath);
            toppath = NULL;
        }
    }

    return 0;
}

/*
 * function name: gscgroup_map_node_group
 * description  : map the node group configuration file
 */
int gscgroup_map_node_group(WLMNodeGroupInfo* ng)
{
    int ret = 0;

    if (ng == NULL) {
        return -1;
    }

    if ((ret = gscgroup_parse_config_file(ng->group_name, ng->vaddr)) == -1) {
        return -1;
    }

    if (ng->cgroups_htab == NULL) {
        gscgroup_initialize_hashtbl(&ng->cgroups_htab);
        pthread_mutex_init(&ng->cgroups_mutex, NULL);
    }

    if ((ret = gscgroup_build_cgroups_htab(ng)) == -1) {
        return -1;
    }

    return 0;
}

/*
 * function name: gscgroup_map_ng_conf
 * description  : map the node group configuration file
 */
int gscgroup_map_ng_conf(char* ngname)
{
    /* get the node group info */
    int ret = 0;

    WLMNodeGroupInfo* ng = WLMMustGetNodeGroupFromHTAB(ngname);
    if (ng->cgroups_htab == NULL) {
        gscgroup_initialize_hashtbl(&ng->cgroups_htab);
        pthread_mutex_init(&ng->cgroups_mutex, NULL);
    }

    if (-1 == (ret = gscgroup_parse_config_file(ngname, ng->vaddr))) {
        ereport(LOG, (errmsg("Failed to parse cgroup config file for %s virtual cluster.", ngname)));
        return -1;
    }

    if (!g_instance.wlm_cxt->gscgroup_init_done) {
        ereport(LOG, (errmsg("Please mount Cgroup before mapping %s node group configuration.", ngname)));
        return -1;
    }

    if (-1 == (ret = gscgroup_build_cgroups_htab(ng))) {
        ereport(LOG, (errmsg("Failed to build cgroup hash table for %s virtual cluster.", ngname)));
        return -1;
    }

    return 0;
}

/* function for gs_lcctl tool to map the new created logic cluster */
Datum gs_cgroup_map_ng_conf(PG_FUNCTION_ARGS)
{
    int ret = 0;
    int i = 0;
    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", "/", "..", NULL};
    char* ngname = PG_GETARG_CSTRING(0);

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin user can use this function")));
    }

    if (NULL == ngname || *ngname == '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of node group: NULL or \"\"")));
        PG_RETURN_BOOL(false);
    }

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)ngname, danger_character_list[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Error: environment variable \"%s\" contain invaild symbol \"%s\".\n",
                        ngname, danger_character_list[i])));
        }
    }

    if (!g_instance.wlm_cxt->gscgroup_init_done) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                errmsg("Cgroup is not mounted or enable_control_group is false")));
        PG_RETURN_BOOL(false);
    }

    ret = gscgroup_map_ng_conf(ngname);
    if (ret == -1) {
        PG_RETURN_BOOL(false);
    }

    PG_RETURN_BOOL(true);
}

/*
 * function name: gscgroup_init
 * description  : init the cgroup and parse configuration file
 */
void gscgroup_init(void)
{
    int ret = 0;
    int i, j, bkd, sret;
    char *relpath = NULL;
    char *toppath = NULL;
    char keyname[GPNAME_LEN] = {0};
    char tmpname[GPNAME_LEN] = {0};
    struct cgroup* cg = NULL;

    /* initialize cgroup */
    ret = cgroup_init();
    if (ret) {
        ereport(WARNING, (errmsg("Cgroup initialization failed: %s", cgroup_strerror(ret))));
        return;
    }

    if (-1 == (ret = gscgroup_parse_config_file(NULL, NULL))) {
        ereport(LOG, (errmsg("Failed to parse cgroup config file.")));
        return;
    }

    /* create the hash table */
    gscgroup_initialize_hashtbl(&g_instance.wlm_cxt->MyDefaultNodeGroup.cgroups_htab);

    errno_t errval = memcpy_s(g_instance.wlm_cxt->MyDefaultNodeGroup.vaddr,
        sizeof(g_instance.wlm_cxt->MyDefaultNodeGroup.vaddr),
        g_instance.wlm_cxt->gscgroup_vaddr,
        sizeof(g_instance.wlm_cxt->MyDefaultNodeGroup.vaddr));
    securec_check_errval(errval, , LOG);

    /* get the cpu count value */
    g_instance.wlm_cxt->gscgroup_cpucnt = gsutil_get_cpu_count();

    relpath = gscgroup_get_relative_path(TOPCG_GAUSSDB, g_instance.wlm_cxt->gscgroup_vaddr);
    if (relpath != NULL) {
        /* get the root control group */
        g_instance.wlm_cxt->gscgroup_rootcg = gscgroup_get_cgroup(relpath);
        free(relpath);
        relpath = NULL;
    }

    for (i = CLASSCG_START_ID; i <= CLASSCG_END_ID; i++) {
        if (g_instance.wlm_cxt->gscgroup_vaddr[i]->used) {
            if (i == CLASSCG_START_ID) {
                sret = snprintf_s(
                    tmpname, sizeof(tmpname), sizeof(tmpname) - 1, "%s:%d", GSCGROUP_TOP_WORKLOAD, WD_TOP_LEVEL);
                securec_check_intval(sret, ,);
            }

            relpath = gscgroup_get_relative_path(i, g_instance.wlm_cxt->gscgroup_vaddr);
            if (!relpath) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for relative path of %s Group!",
                        g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname)));
                return;
            }

            if (!(cg = gscgroup_get_cgroup(relpath))) {
                free(relpath);
                relpath = NULL;
                return;
            }

            (void)gscgroup_enter_hashtbl(
                &g_instance.wlm_cxt->MyDefaultNodeGroup, g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname, cg);
            free(relpath);
            relpath = NULL;

            /* register all workload group of the class */
            for (j = WDCG_START_ID; j <= WDCG_END_ID; j++) {
                if (g_instance.wlm_cxt->gscgroup_vaddr[j]->used &&
                    g_instance.wlm_cxt->gscgroup_vaddr[j]->ginfo.wd.cgid == i) {
                    relpath = gscgroup_get_relative_path(j, g_instance.wlm_cxt->gscgroup_vaddr);
                    if (!relpath) {
                        ereport(LOG,
                            (errmsg("Failed to allocate memory "
                                    "for relative path of %s Group!",
                                g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname)));
                        return;
                    }

                    if (!(cg = gscgroup_get_cgroup(relpath))) {
                        free(relpath);
                        relpath = NULL;
                        return;
                    }

                    if (g_instance.wlm_cxt->gscgroup_deftopwdcg == NULL && *tmpname &&
                        0 == strcmp(g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname, tmpname)) {
                        g_instance.wlm_cxt->gscgroup_deftopwdcg = cg;
                    }

                    sret = snprintf_s(keyname,
                        sizeof(keyname),
                        sizeof(keyname) - 1,
                        "%s:%s",
                        g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname,
                        g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname);
                    securec_check_intval(sret, free(relpath), );

                    (void)gscgroup_enter_hashtbl(
                        &g_instance.wlm_cxt->MyDefaultNodeGroup, gscgroup_convert_cgroup(keyname), cg);
                    free(relpath);
                    relpath = NULL;
                }
            }

            relpath = NULL;

            /* get the top timeshare path of this class */
            toppath = gscgroup_get_topts_path(i, g_instance.wlm_cxt->gscgroup_vaddr);
            if (!toppath) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for Timeshare path of %s Group!",
                        g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname)));
                return;
            }

            if (!(relpath = (char*)malloc(GPNAME_PATH_LEN))) {
                ereport(LOG,
                    (errmsg("Failed to allocate memory "
                            "for timeshare path of %s Group!",
                        g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname)));
                free(toppath);
                toppath = NULL;
                return;
            }

            /* register all timeshare group of the class */
            for (j = TSCG_START_ID; j <= TSCG_END_ID; j++) {
                sret = snprintf_s(relpath,
                    GPNAME_PATH_LEN,
                    GPNAME_PATH_LEN - 1,
                    "%s/%s",
                    toppath,
                    g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname);
                securec_check_intval(sret, free(relpath); free(toppath), );

                if (!(cg = gscgroup_get_cgroup(relpath))) {
                    free(relpath);
                    relpath = NULL;
                    free(toppath);
                    toppath = NULL;
                    return;
                }

                /* set the default cgroup structure for later using */
                if (i == CLASSCG_START_ID &&
                    strcmp(g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname, GSCGROUP_DEFAULT_CGNAME) == 0) {
                    g_instance.wlm_cxt->gscgroup_defwdcg = cg;
                }

                sret = snprintf_s(keyname,
                    sizeof(keyname),
                    sizeof(keyname) - 1,
                    "%s:%s",
                    g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname,
                    g_instance.wlm_cxt->gscgroup_vaddr[j]->grpname);
                securec_check_intval(sret, free(relpath); free(toppath), );

                (void)gscgroup_enter_hashtbl(
                    &g_instance.wlm_cxt->MyDefaultNodeGroup, gscgroup_convert_cgroup(keyname), cg);
            }

            free(relpath);
            relpath = NULL;
            free(toppath);
            toppath = NULL;
        }
    }

    bkd = BACKENDCG_START_ID;

    relpath = gscgroup_get_relative_path(bkd, g_instance.wlm_cxt->gscgroup_vaddr);
    if (!relpath) {
        ereport(LOG,
            (errmsg("Failed to allocate memory "
                    "for relative path of %s Group!",
                g_instance.wlm_cxt->gscgroup_vaddr[bkd]->grpname)));
        return;
    }

    if (!(cg = gscgroup_get_cgroup(relpath))) {
        free(relpath);
        relpath = NULL;
        return;
    }

    free(relpath);
    relpath = NULL;

    g_instance.wlm_cxt->gscgroup_bkdcg = cg;

    if (g_instance.attr.attr_resource.enable_vacuum_control) {
        /* check if the backend group exists */
        for (i = BACKENDCG_START_ID; i <= BACKENDCG_END_ID; ++i) {
            if (g_instance.wlm_cxt->gscgroup_vaddr[i]->used == 0) {
                continue;
            }

            if (0 == strcmp(g_instance.wlm_cxt->gscgroup_vaddr[i]->grpname, GSCGROUP_VACUUM)) {
                bkd = i;
                break;
            }
        }

        relpath = gscgroup_get_relative_path(bkd, g_instance.wlm_cxt->gscgroup_vaddr);
        if (relpath == NULL) {
            ereport(LOG,
                (errmsg("Failed to allocate memory "
                        "for relative path of %s Group!",
                    g_instance.wlm_cxt->gscgroup_vaddr[bkd]->grpname)));
            return;
        }

        ereport(DEBUG1, (errmsg("vacuum group path: %s", relpath)));
        if ((cg = gscgroup_get_cgroup(relpath)) == NULL) {
            free(relpath);
            relpath = NULL;
            return;
        }

        free(relpath);
        relpath = NULL;

        g_instance.wlm_cxt->gscgroup_vaccg = cg;
    } else {
        g_instance.wlm_cxt->gscgroup_vaccg = g_instance.wlm_cxt->gscgroup_bkdcg;
    }

    g_instance.wlm_cxt->gscgroup_config_parsed = 1;

    if (u_sess->attr.attr_resource.use_workload_manager && u_sess->attr.attr_resource.enable_control_group) {
        g_instance.wlm_cxt->gscgroup_init_done = 1;
    }
}

/*
 * function name: gscgroup_attach_backend_task
 * description  : attach backend thread into the specified Cgroup
 */
int gscgroup_attach_backend_task(const char* gname, bool is_noexcept)
{
    int ret;
    struct cgroup* cg = NULL;

    if (g_instance.wlm_cxt->gscgroup_vaddr[0] == NULL || gname == NULL || '\0' == *gname) {
        return 0;
    }

    if (0 == strcmp(gname, GSCGROUP_DEFAULT_BACKEND)) {
        cg = g_instance.wlm_cxt->gscgroup_bkdcg;
    } else if (0 == strcmp(gname, GSCGROUP_VACUUM)) {
        cg = g_instance.wlm_cxt->gscgroup_vaccg;
    } else if (0 == strcmp(gname, GSCGROUP_TOP_DATABASE)) {
        cg = g_instance.wlm_cxt->gscgroup_rootcg;
    }

    if (cg == NULL) {
        return 0;
    }

    /* attach current thread into the cgroup */
    ret = cgroup_attach_task(cg);
    if (ret != 0 && !is_noexcept) {
        ereport(WARNING,
            (errmsg("Cgroup failed to attach (tid %d) "
                    "into \"%s\" group: %s(%d)",
                gettid(),
                gname,
                cgroup_strerror(ret),
                ret)));
    }

    return ret;
}

/*
 * function name: gscgroup_attach_task
 * description  : attach current query thread into the specified Cgroup
 */
void gscgroup_attach_task(WLMNodeGroupInfo* ng, const char* gname)
{
    int ret;
    struct cgroup* cg = NULL;

    /* get the cgroup structure */
    cg = gscgroup_lookup_cgroup(ng, gname);

    gscgroup_curwd = cg;

    /* attach current thread into the cgroup */
    ret = cgroup_attach_task(cg);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Cgroup failed to attach (tid %d) into \"%s\" group: %s(%d)",
            gettid(), gname, cgroup_strerror(ret), ret),
            errhint("Maybe the path of Cgroup \"%s\" has been changed or removed.", gname)));
    }

    u_sess->wlm_cxt->cgroup_state = CG_USING; /* reset the value */
}

/*
 * function name: gscgroup_attach_task_batch
 * description  : attach query threads into a group
 * arguments    :
 *                _in_ gname: group name
 *                _in_ tid: thread pid
 *                __inout is_first: a flag, it must be set 1 as first flag
 * return value : -1: attach failed
 *                0 : attach success
 */
int gscgroup_attach_task_batch(WLMNodeGroupInfo* ng, const char* gname, pid_t tid, int* is_first)
{
    static THR_LOCAL struct cgroup* cg = NULL;

    int ret = 0;

    /*
     * Is it first time to attach this group? If yes, we need
     * get the cgroup info with group name from the group hash
     * table. If not, we will use the last cgroup info to attach.
     */
    if (*is_first > 0) {
        /* get the cgroup structure */
        cg = gscgroup_lookup_cgroup(ng, gname);
        *is_first = 0;
    }

    /*
     * We must check the cgroup info whether is valid,
     * because 'is_first' maybe is not a correct value
     * first time.
     */
    if (!cg) {
        return -1;
    }

    /* attach current thread into the cgroup */
    ret = cgroup_attach_task_pid(cg, tid);
    if (ret != 0) {
        ereport(WARNING,
            (errmsg("Cgroup failed to attach (tid %d) "
                    "into \"%s\" group: %s(%d)",
                tid,
                gname,
                cgroup_strerror(ret),
                ret)));
    }

    return ret;
}

/*
 * function name: gscgroup_switch_topwd
 * description  : attach current query thread into topwd Cgroup
 */
void gscgroup_switch_topwd(WLMNodeGroupInfo* ng)
{
    int ret;

    (void)gscgroup_lookup_cgroup(ng, u_sess->wlm_cxt->control_group);

    /* attach current thread into the cgroup */
    ret = cgroup_attach_task(gscgroup_topwd);
    if (ret != 0) {
        ereport(WARNING,
            (errmsg("Cgroup failed to attach (tid %d) "
                    "into \"%s\" group: %s(%d)",
                gettid(),
                GSCGROUP_TOP_WORKLOAD,
                cgroup_strerror(ret),
                ret)));
    } else {
        ereport(LOG,
            (errmsg("Switch cgroup from \"%s\" to \"%s\"", u_sess->wlm_cxt->control_group, GSCGROUP_TOP_WORKLOAD)));
    }

    u_sess->wlm_cxt->cgroup_state = CG_USING; /* reset the value */
}

/*
 * function name: gscgroup_switch_curwd
 * description  : attach current query thread into current Cgroup
 */
void gscgroup_switch_vacuum(void)
{
    int ret = 0;

    if (g_instance.wlm_cxt->gscgroup_vaddr[0] == NULL || g_instance.wlm_cxt->gscgroup_vaccg == NULL) {
        return;
    }

    /* attach current thread into the cgroup */
    ret = cgroup_attach_task(g_instance.wlm_cxt->gscgroup_vaccg);
    if (ret != 0) {
        ereport(WARNING,
            (errmsg("Cgroup failed to attach (tid %d) "
                    "into \"%s\" group: %s(%d)",
                gettid(),
                GSCGROUP_VACUUM,
                cgroup_strerror(ret),
                ret)));
    }

    ereport(LOG, (errmsg("Switch cgroup from \"%s\" to \"%s\"", u_sess->wlm_cxt->control_group, GSCGROUP_VACUUM)));

    u_sess->wlm_cxt->cgroup_state = CG_USING; /* reset the value */
}

/*
 * function name: gscgroup_get_next_group
 * description  : get the next timeshare group, like this:
 *                Medium -> Low, High -> Medium
 */
int gscgroup_get_next_group(WLMNodeGroupInfo* ng, char* gname)
{
    char *p = NULL;
    char *q = NULL;
    char *name = NULL;
    int cls, wd, sret;

    /* cgroup is not valid */
    if (g_instance.wlm_cxt->gscgroup_init_done <= 0) {
        return -1;
    }

    if (0 == strcmp(gname, GSCGROUP_DEFAULT_CGNAME)) {
        sret = snprintf_s(gname, NAMEDATALEN, NAMEDATALEN - 1, "%s", GSCGROUP_LOW_TIMESHARE);
        securec_check_intval(sret, , 1);
        return 1;
    }

    q = name = pstrdup(gname);

    p = strchr(q, ':');
    if (!p) {
        /* check if it is the group of timeshare */
        if (-1 != (wd = gscgroup_is_timeshare(q))) { // each nodegroup has the same timeshare
            int i = 0;

            pfree(name);

            for (i = wd - 1; i >= TSCG_START_ID; --i) {
                if (StringIsValid(ng->vaddr[i]->grpname)) {
                    sret = snprintf_s(gname, NAMEDATALEN, NAMEDATALEN - 1, "%s", ng->vaddr[i]->grpname);
                    securec_check_intval(sret, , 1);
                    return 1;
                }
            }

            return 0;
        }

        return -1;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == (cls = gscgroup_is_class(ng, q))) {
        ereport(WARNING, (errmsg("%s cgroup has been delete from OS. ", gname)));
        pfree(name);
        return -1;
    }

    /* check if name is a timeshare name */
    if (-1 != (wd = gscgroup_is_timeshare(p))) {
        int i = 0;

        pfree(name);

        for (i = wd - 1; i >= TSCG_START_ID; --i) {
            if (StringIsValid(ng->vaddr[i]->grpname)) {
                sret = snprintf_s(
                    gname, NAMEDATALEN, NAMEDATALEN - 1, "%s:%s", ng->vaddr[cls]->grpname, ng->vaddr[i]->grpname);
                securec_check_intval(sret, , 1);
                return 1;
            }
        }

        return 0;
    }

    pfree(name);
    return -1;
}
/*
 * function name: gscgroup_get_top_group_name
 * description  : get the top workload group name
 */
char* gscgroup_get_top_group_name(WLMNodeGroupInfo* ng, char* gname, int len)
{
    char *p = NULL;
    char *q = NULL;
    char name[NAMEDATALEN];
    int sret;

    if (!StringIsValid(gname)) {
        return NULL;
    }

    sret = snprintf_s(name, sizeof(name), sizeof(name) - 1, "%s", gname);
    securec_check_intval(sret, , NULL);

    q = name;

    p = strchr(q, ':');
    if (!p) {
        sret = snprintf_s(gname, len, len - 1, "%s:%s", GSCGROUP_DEFAULT_CLASS, GSCGROUP_TOP_WORKLOAD);
        securec_check_intval(sret, , gname);

        return gname;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == gscgroup_is_class(ng, q)) {
        ereport(LOG, (errmsg("%s cgroup has been delete from OS. ", gname)));

        return NULL;
    }

    sret = snprintf_s(gname, len, len - 1, "%s:%s", q, GSCGROUP_TOP_WORKLOAD);
    securec_check_intval(sret, , gname);

    return gname;
}

/*
 * pg_control_group_config: print the cgroup config on current node
 */
Datum pg_control_group_config(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_control_group_config");
    }
    char* config = gscgroup_print();
    text* result_config = cstring_to_text(config);
    pfree_ext(config);
    PG_RETURN_TEXT_P(result_config);
}

/*
 * function name: gscgroup_check_group_name
 * description  : check if the group name has been set
 * arguments    : control group name
 * return value : -1: abnormal value
 *                 0: normal value
 *                 1: need handle as a parent resource pool
 */
int gscgroup_check_group_name(WLMNodeGroupInfo* ng, const char* gname)
{
    char *p = NULL;
    char *q = NULL;
    char *name = NULL;
    char *r = NULL;
    int cls, wd, sret;

    if (g_instance.wlm_cxt->gscgroup_config_parsed == 0 || !StringIsValid(gname)) {
        /* group is not valid */
        sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
            sizeof(u_sess->wlm_cxt->group_keyname),
            sizeof(u_sess->wlm_cxt->group_keyname) - 1,
            "%s",
            GSCGROUP_INVALID_GROUP);
        securec_check_intval(sret, , -1);

        return -1;
    }

    q = name = pstrdup(gname);

    if (q == NULL) {
        return -1;
    }

    p = strchr(q, ':');
    /* check if it is the group of timeshare */
    if (!p) {
        if (-1 != gscgroup_is_timeshare(q)) {
            /* the string of DefaultClass:gname */
            sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
                sizeof(u_sess->wlm_cxt->group_keyname),
                sizeof(u_sess->wlm_cxt->group_keyname) - 1,
                "%s:%s",
                GSCGROUP_DEFAULT_CLASS,
                q);
            securec_check_intval(sret, pfree(name), -1);

            pfree(name);

            return 0;
        }
        /* the cases of the class */
        if (-1 != gscgroup_is_class(ng, q)) {
            sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
                sizeof(u_sess->wlm_cxt->group_keyname),
                sizeof(u_sess->wlm_cxt->group_keyname) - 1,
                "%s:%s",
                q,
                GSCGROUP_MEDIUM_TIMESHARE);
            securec_check_intval(sret, pfree(name), -1);

            pfree(name);

            return 1;
        }

        /* other cases are forbidden */
        sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
            sizeof(u_sess->wlm_cxt->group_keyname),
            sizeof(u_sess->wlm_cxt->group_keyname) - 1,
            "%s",
            GSCGROUP_INVALID_GROUP);
        securec_check_intval(sret, pfree(name), -1);

        ereport(LOG, (errmsg("%s isn't the name of timeshare group!", gname)));
        pfree(name);
        return -1;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == (cls = gscgroup_is_class(ng, q))) {
        sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
            sizeof(u_sess->wlm_cxt->group_keyname),
            sizeof(u_sess->wlm_cxt->group_keyname) - 1,
            "%s",
            GSCGROUP_INVALID_GROUP);
        securec_check_intval(sret, pfree(name), -1);

        ereport(LOG, (errmsg("%s in %s isn't the name of class group!", q, gname)));
        pfree(name);
        return -1;
    }

    r = strchr((char*)p, ':');

    if (r != NULL) {
        *r++ = '\0';
    }

    /* firstly check if it is the group of timeshare */
    if (-1 != (wd = gscgroup_is_timeshare(p)) ||
        (-1 != (wd = gscgroup_is_workload(ng, cls, p)) && (r == NULL || (-1 != gscgroup_check_level(cls, r))))) {
        sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
            sizeof(u_sess->wlm_cxt->group_keyname),
            sizeof(u_sess->wlm_cxt->group_keyname) - 1,
            "%s:%s",
            q,
            p);
        securec_check_intval(sret, pfree(name), -1);

        pfree(name);
        return 0;
    }

    sret = snprintf_s(u_sess->wlm_cxt->group_keyname,
        sizeof(u_sess->wlm_cxt->group_keyname),
        sizeof(u_sess->wlm_cxt->group_keyname) - 1,
        "%s",
        GSCGROUP_INVALID_GROUP);
    securec_check_intval(sret, pfree(name), -1);

    ereport(LOG,
        (errmsg("%s in %s isn't the name of timeshare group "
                "or workload group!",
            p,
            gname)));
    pfree(name);
    return -1;
}

/*
 * function name: gscgroup_get_percent
 * description  : retrieve the percentage value based on the control group
 * return value :
 *            -1: abnormal value
 *         other: normal value
 */
int gscgroup_get_percent(WLMNodeGroupInfo* ng, const char* gname)
{
    char *p = NULL;
    char *q = NULL;
    char *name = NULL;
    int cls, wd, pct;

    if (g_instance.wlm_cxt->gscgroup_init_done == 0 || !StringIsValid(gname) || !CGroupIsValid(gname)) {
        return -1;
    }

    q = name = pstrdup(gname);

    p = strchr(q, ':');
    if (!p) {
        cls = CLASSCG_START_ID;

        if (-1 != (wd = gscgroup_is_timeshare(name))) {
            pct = ng->vaddr[cls]->percent * ng->vaddr[wd]->ginfo.ts.rate / TS_ALL_RATE;
            pfree(name);
            return pct;
        }

        return -1;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == (cls = gscgroup_is_class(ng, q))) {
        pfree(name);
        return -1;
    }

    /* firstly check if it is the group of timeshare */
    if (-1 != (wd = gscgroup_is_timeshare(p))) {
        pct = ng->vaddr[cls]->ginfo.cls.rempct * ng->vaddr[cls]->percent * ng->vaddr[wd]->ginfo.ts.rate /
              (GROUP_ALL_PERCENT * TS_ALL_RATE);
        pfree(name);
        return pct;
    }

    if (-1 != (wd = gscgroup_is_workload(ng, cls, p))) {
        pfree(name);
        return ng->vaddr[wd]->percent;
    }

    pfree(name);
    return -1;
}

/*
 * function name: gscgroup_get_grpconf
 * description  : retrieve the group configuration information based on gname string
 * return value :
 *          NULL: abnormal value
 *         other: normal value
 */
gscgroup_grp_t* gscgroup_get_grpconf(WLMNodeGroupInfo* ng, const char* gname)
{
    char *p = NULL;
    char *q = NULL;
    char name[NAMEDATALEN];
    int cls, wd;

    if (g_instance.wlm_cxt->gscgroup_init_done <= 0 || !StringIsValid(gname)) {
        return NULL;
    }

    errno_t rc = strcpy_s(name, sizeof(name), gname);
    securec_check(rc, "\0", "\0");

    q = name;

    p = strchr(q, ':');
    if (!p) {
        return (gscgroup_is_timeshare(name) != -1) ? ng->vaddr[CLASSCG_START_ID] : NULL;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == (cls = gscgroup_is_class(ng, q))) {
        return NULL;
    }

    /* firstly check if it is the group of timeshare */
    if (-1 != (wd = gscgroup_is_timeshare(p))) {
        return ng->vaddr[cls];
    }

    if (-1 != (wd = gscgroup_is_workload(ng, cls, p))) {
        return ng->vaddr[wd];
    }

    return NULL;
}

/*
 * function name: gscgroup_get_timeshare_relpath
 * description  : get relative path for the timeshare group
 * arguments    :
 *                _in_ cls: class id
 *                _in_ wd: workload group id
 *                _out_ relpath: the relative path
 * return value : relative path
 */
char* gscgroup_get_timeshare_relpath(WLMNodeGroupInfo* ng, int cls, int wd, char* relpath)
{
    int sret;
    char* toppath = NULL;

    /* get the top timeshare path of this class */
    toppath = gscgroup_get_topts_path(cls, ng->vaddr, ng->group_name);
    if (!toppath) {
        ereport(LOG,
            (errmsg("Failed to allocate memory "
                    "for Timeshare path of %s Group!",
                ng->vaddr[cls]->grpname)));

        return NULL;
    }

    /* get the relative path */
    sret = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", toppath, ng->vaddr[wd]->grpname);
    securec_check_intval(sret, free(toppath), NULL);

    free(toppath);
    toppath = NULL;

    return relpath;
}

/*
 * function name: gscgroup_get_workload_relpath
 * description  : get relative path for the workload group
 * arguments    :
 *                _in_ wd: workload group id
                  _out_ relpath: the relative path
 * return value : relative path
 */
char* gscgroup_get_workload_relpath(WLMNodeGroupInfo* ng, int wd, char* relpath)
{
    int sret;

    /* get the workload group path */
    char* tmppath = gscgroup_get_relative_path(wd, ng->vaddr, ng->group_name);

    if (tmppath == NULL) {
        return NULL;
    }

    /* the workload group path is the relative path */
    sret = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s", tmppath);
    securec_check_intval(sret, free(tmppath), NULL);

    free(tmppath);
    tmppath = NULL;

    return relpath;
}

/*
 * function name: gscgroup_get_cgroup_relpath
 * description  : get relative path for the control group
 * arguments    :
 *                _in_ cgname: group name
 *                _out_ relpath: the relative path
 * return value : relative path
 */
char* gscgroup_get_cgroup_relpath(WLMNodeGroupInfo* ng, const char* cgname, char* relpath)
{
    char *p = NULL;
    char *q = NULL;
    char name[GPNAME_LEN];
    int cls, wd, sret;

    /* make a copy of the group name to use */
    sret = snprintf_s(name, sizeof(name), sizeof(name) - 1, "%s", cgname);
    securec_check_intval(sret, , NULL);

    q = name;

    /* If the group name is not like this: clsname:wgname, it's not a valid name. */
    if ((p = strchr(q, ':')) == NULL) {
        if ((cls = gscgroup_is_class(ng, q)) == -1) {
            return NULL;
        }

        return gscgroup_get_workload_relpath(ng, cls, relpath);
    }

    *p++ = '\0';

    /* cannot find the class, failed to get relative path */
    if ((cls = gscgroup_is_class(ng, q)) == -1) {
        return NULL;
    }

    if ((wd = gscgroup_is_timeshare(p)) != -1) { /* it's timeshare */
        return gscgroup_get_timeshare_relpath(ng, cls, wd, relpath);
    } else if ((wd = gscgroup_is_workload(ng, cls, p)) != -1) { /* it's workload group */
        return gscgroup_get_workload_relpath(ng, wd, relpath);
    }

    return NULL;
}

/*
 * function name: gscgroup_check_and_update_entry
 * description  : check and update relative path for the control group
 *                in the group hash table
 * arguments    :
 *                _in_ entry: group entry info
 * return value : void
 */
void gscgroup_check_and_update_entry(WLMNodeGroupInfo* ng, gscgroup_entry_t* entry)
{
    char relpath[GPNAME_PATH_LEN] = {0};
    char* path = NULL;

    /* failed to get the relative path */
    if (gscgroup_get_cgroup_relpath(ng, entry->name, relpath) == NULL) {
        ereport(DEBUG1, (errmsg("Cannot find the relative path for the group \"%s\"", entry->name)));
        return;
    }

    if ((path = gsutil_get_cgroup_name(entry->cg)) == NULL) {
        ereport(LOG, (errmsg("Cannot get current cgroup relpath.")));
        return;
    }

    /* The relpath has changed, we must update the info in the hash table. */
    if (strcmp(relpath, path) != 0) {
        struct cgroup* cg = NULL;

        if ((cg = gscgroup_get_cgroup(relpath)) == NULL) {
            return;
        }

        ereport(DEBUG2, (errmsg("change group \'%s\' relpath to %s", entry->name, relpath)));

        /* release old cgroup */
        if (entry->oldcg) {
            cgroup_free(&entry->oldcg);
        }

        entry->oldcg = entry->cg;
        entry->cg = cg;
    }

    return;
}

/*
 * @Description: get cpu jiffies from the file "/proc/stat"
 * @IN:         cpu_count_down: need collect cpustat.
 *              cpustat is collected every 8 seconds,
 *              and checking 'cpu' total jiffies and 'user' jiffies are needed for iostat.
 *              iostat is collected every second,
 *              and checking 'cpu0' total jiffies is enough for iostat.
 * @Return:     void
 * @See also:
 */
uint64 gscgroup_read_stat_cpu(void)
{
    errno_t rc;
    FILE* cpufp = NULL;
    char line[8192];
    uint64 cpu_user, cpu_nice, cpu_sys, cpu_idle, cpu_iowait, cpu_hardirq;
    uint64 cpu_softirq, cpu_steal, cpu_guest, cpu_guest_nice;
    uint64 all = 0;

    if ((cpufp = fopen("/proc/stat", "r")) == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", FILE_CPUSTAT)));
        return -1;
    }

    while (fgets(line, sizeof(line), cpufp) != NULL) {
        /* fisrt line -- total cpu */
        if (strncmp(line, "cpu ", 4) == 0) {
            /* get the total jiffies */
            rc = sscanf_s(line + 5,
                "%lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
                &cpu_user,
                &cpu_nice,
                &cpu_sys,
                &cpu_idle,
                &cpu_iowait,
                &cpu_hardirq,
                &cpu_softirq,
                &cpu_steal,
                &cpu_guest,
                &cpu_guest_nice);
            if (rc != 10) {
                fprintf(stderr,
                    "%s:%d failed on calling "
                    "security function.\n",
                    __FILE__,
                    __LINE__);
                fclose(cpufp);
                return -1;
            }

            all = cpu_user + cpu_nice + cpu_sys + cpu_idle + cpu_iowait + cpu_hardirq + cpu_steal + cpu_softirq;
            break;
        }
    }

    fclose(cpufp);
    return all;
}

/*
 * @Description: udpate cpu info for a cgroup.
 * @OUT entry: cgroup entry
 * @Return:  void
 * @See also:
 */
void gscgroup_get_cpuinfo(gscgroup_entry_t* entry)
{
    int setcnt = 0;
    int64 usage = 0;
    int64 usageAcct = 0;

    long secs;
    int usecs;
    int sleeptime = 1;
    TimestampTz stop_time = GetCurrentTimestamp();

    if (entry->lastTime) {
        TimestampDifference(entry->lastTime, stop_time, &secs, &usecs);
        sleeptime = secs * 1000 + usecs / 1000 + 1;
    }

    /* get cpuset and cpuacct */
    if (gscgroup_get_cgroup_cpuinfo(entry->cg, &setcnt, &usage) == -1) {
        return;
    }

    /* compute cpu usage percent */
    if (setcnt > 0 && entry->lastTime) {
        entry->cpuCount = setcnt;
        usageAcct = usage - entry->cpuLastAcct;
        if (usageAcct >= 0) {
            entry->cpuUsedAcct = usageAcct;
        } else {
            entry->cpuUsedAcct = 0;
            ereport(LOG, (errmsg("Cgroup %s is probably reset, please check if it is avalaible.", entry->name)));
        }
        entry->usedCpuCount = entry->cpuUsedAcct * 1000 / sleeptime * GROUP_ALL_PERCENT / NANOSECS_PER_SEC;
        entry->cpuUtil = entry->usedCpuCount * GROUP_ALL_PERCENT / entry->cpuCount;
    }

    entry->lastTime = stop_time;
    entry->cpuLastAcct = usage;

    ereport(DEBUG3,
        (errmsg("group name: %s, setcnt: %d, usage %ld, cpuUsedAcct: %ld, usedCpuCount: %d, cpuUtil: %d, "
                "cpuLastAcct: %ld",
            entry->name,
            setcnt,
            usage,
            entry->cpuUsedAcct,
            entry->usedCpuCount,
            entry->cpuUtil,
            entry->cpuLastAcct)));
}

/*
 * function name: gscgroup_update_hashtbl_cpuinfo
 * description  : check and update relative path for the control group
 * arguments    : void
 * return value : void
 */
void gscgroup_update_hashtbl_cpuinfo(WLMNodeGroupInfo* ng)
{
    gscgroup_entry_t* entry = NULL;
    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, ng->cgroups_htab);

    /* check every group in the group hash table */
    while ((entry = (gscgroup_entry_t*)hash_seq_search(&hash_seq)) != NULL) {
        /* check and update entry */
        gscgroup_check_and_update_entry(ng, entry);

        if (IS_PGXC_DATANODE) {
            gscgroup_get_cpuinfo(entry);
        }
    }
}

/*
 * function name: gscgroup_get_cpu_usage_percent
 * description  : get cpu usage percent
 * arguments    :
 *                _in_ gname: group name
 * return value : cpu usage percent
 */
int gscgroup_get_cpu_usage_percent(WLMNodeGroupInfo* ng, const char* gname)
{
    gscgroup_entry_t* cgentry = NULL;

    /* check the name and get the old info from the group hash table */
    if (!CGroupIsValid(gname) || (cgentry = gscgroup_lookup_hashtbl(ng, gname)) == NULL) {
        return -1;
    }

    ereport(DEBUG2, (errmsg("usagepct: %d, setcnt: %d", cgentry->cpuUtil, cgentry->cpuCount)));

    return (GROUP_ALL_PERCENT - cgentry->cpuUtil) * cgentry->cpuCount / GROUP_ALL_PERCENT;
}

/*
 * function name: gscgroup_update_hashtbl
 * description  : update cgroup info in the hash table
 * return value : void
 */
void gscgroup_update_hashtbl(WLMNodeGroupInfo* ng, const char* keyname)
{
    int cls, wd, sret;
    char *relpath = NULL;
    char *p = NULL;
    char *q = NULL;
    char tmpname[GPNAME_LEN] = {0};
    struct cgroup* cg = NULL;
    gscgroup_entry_t* cgentry = NULL;

    if (!CGroupIsValid(keyname) || (cgentry = gscgroup_lookup_hashtbl(ng, keyname)) != NULL) {
        return;
    }

    errno_t rc = strcpy_s(tmpname, sizeof(tmpname), keyname);
    securec_check(rc, "\0", "\0");

    q = tmpname;

    p = strchr(q, ':');
    if (!p) {
        /* check if name is a class name */
        if (-1 == (cls = gscgroup_is_class(ng, q))) {
            return;
        }

        relpath = gscgroup_get_relative_path(cls, ng->vaddr, ng->group_name);
        if (!relpath) {
            ereport(LOG,
                (errmsg("Failed to allocate memory "
                        "for relative path of %s Group!",
                    g_instance.wlm_cxt->gscgroup_vaddr[cls]->grpname)));
            return;
        }

        if (!(cg = gscgroup_get_cgroup(relpath))) {
            free(relpath);
            relpath = NULL;
            return;
        }

        (void)gscgroup_enter_hashtbl(ng, keyname, cg);
        free(relpath);
        relpath = NULL;

        return;
    }

    *p++ = '\0';

    /* check if name is a class name */
    if (-1 == (cls = gscgroup_is_class(ng, q))) {
        return;
    }

    /* firstly check if it is the group of timeshare */
    if (-1 != (wd = gscgroup_is_timeshare(p))) {
        relpath = NULL;

        /* get the top timeshare path of this class */
        char* toppath = gscgroup_get_topts_path(cls, ng->vaddr, ng->group_name);
        if (!toppath) {
            ereport(LOG,
                (errmsg("Failed to allocate memory "
                        "for Timeshare path of %s Group!",
                    ng->vaddr[cls]->grpname)));
            return;
        }

        if (!(relpath = (char*)malloc(GPNAME_PATH_LEN))) {
            ereport(LOG,
                (errmsg("Failed to allocate memory "
                        "for timeshare path of %s Group!",
                    ng->vaddr[cls]->grpname)));
            free(toppath);
            toppath = NULL;
            return;
        }

        sret = snprintf_s(relpath, GPNAME_PATH_LEN, GPNAME_PATH_LEN - 1, "%s/%s", toppath, ng->vaddr[wd]->grpname);
        securec_check_intval(sret, free(relpath); free(toppath),);

        free(toppath);
        toppath = NULL;

        if (!(cg = gscgroup_get_cgroup(relpath))) {
            free(relpath);
            relpath = NULL;
            return;
        }
    } else if (-1 != (wd = gscgroup_is_workload(ng, cls, p))) {
        if ((relpath = gscgroup_get_relative_path(wd, ng->vaddr, ng->group_name)) == NULL) {
            ereport(LOG,
                (errmsg("Failed to allocate memory "
                        "for relative path of %s Group!",
                    ng->vaddr[wd]->grpname)));
            return;
        }

        if ((cg = gscgroup_get_cgroup(relpath)) == NULL) {
            free(relpath);
            relpath = NULL;
            return;
        }
    }

    /* retrieve the TopWD cgroup structure */
    if (strncmp(p, GSCGROUP_TOP_WORKLOAD, sizeof(GSCGROUP_TOP_WORKLOAD) - 1) != 0) {
        sret = snprintf_s(tmpname,
            sizeof(tmpname),
            sizeof(tmpname) - 1,
            "%s:%s:%d",
            ng->vaddr[cls]->grpname,
            GSCGROUP_TOP_WORKLOAD,
            WD_TOP_LEVEL);
        securec_check_intval(sret, free(relpath), );

        gscgroup_update_hashtbl(ng, tmpname);
    }

    (void)gscgroup_enter_hashtbl(ng, keyname, cg);
    free(relpath);
    relpath = NULL;
}

/*
 * @Description: get cgroup info internal.
 * @IN cg: cgroup
 * @OUT info: cgroup info, include cpu shares, cpuset, cpu account
 * @Return:  cgroup info
 * @See also:
 */
gscgroup_info_t* gscgroup_get_cgroup_info_internal(struct cgroup* cg, gscgroup_info_t* info)
{
    struct cgroup_controller* cgc = NULL;
    struct cgroup_controller* cgc_set = NULL;
    struct cgroup_controller* cgc_acct = NULL;
    char* cpuset = NULL;

    int64 usage;
    int64 shares;

    const int ret = 0;
    errno_t sret;

    char* relpath = gsutil_get_cgroup_name(cg);
    if (relpath == NULL) {
        return info;
    }

    /* create a new group with the path */
    struct cgroup* new_cg = cgroup_new_cgroup(relpath);
    if (new_cg == NULL) {
        return info;
    }

    /* get all controller of the group */
    if (cgroup_get_cgroup(new_cg) != 0) {
        ereport(LOG,
            (errmsg("get cgroup failed. errno: %d, "
                    "reason: %s",
                ret,
                cgroup_strerror(ret))));
        cgroup_free(&new_cg);
        return info;
    }

    /* get the cpu shares */
    if ((cgc = cgroup_get_controller(new_cg, MOUNT_CPU_NAME)) != NULL &&
        (cgroup_get_value_int64(cgc, CPU_SHARES, &shares) == 0)) {
        info->shares = shares;
    }

    /* get the cpuset */
    if ((cgc_set = cgroup_get_controller(new_cg, MOUNT_CPUSET_NAME)) != NULL &&
        (cgroup_get_value_string(cgc_set, CPUSET_CPUS, &cpuset) == 0)) {
        sret = snprintf_s(info->cpuset, sizeof(info->cpuset), sizeof(info->cpuset) - 1, "%s", cpuset);
        if (cpuset != NULL) {
            free(cpuset);
            cpuset = NULL;
        }
        securec_check_intval(sret, cgroup_free(&new_cg), info);
    }

    /* get the cpu account */
    if ((cgc_acct = cgroup_get_controller(new_cg, MOUNT_CPUACCT_NAME)) != NULL &&
        (cgroup_get_value_int64(cgc_acct, CPUACCT_USAGE, &usage) == 0)) {
        info->entry.cpuUsedAcct = usage;
    }

    cgroup_free(&new_cg);

    return info;
}

/*
 * @Description: get cgroup info
 * @OUT num: count of cgroup info
 * @Return:  cgroup info
 * @See also:
 */
gscgroup_info_t* gscgroup_get_cgroup_info(int* num)
{
    gscgroup_info_t* info_list = NULL;
    gscgroup_entry_t* entry = NULL;
    HASH_SEQ_STATUS hash_seq;

    int i = 0;
    int sret;

    /* check cgroup initialization */
    if (g_instance.wlm_cxt->gscgroup_init_done <= 0) {
        ereport(WARNING, (errmsg("There is no Cgroup information for without initialization!")));
        return NULL;
    }

    WLMNodeGroupInfo* ng = &g_instance.wlm_cxt->MyDefaultNodeGroup;

    USE_AUTO_LWLOCK(WorkloadCGroupHashLock, LW_SHARED);

    /* no cgroup in the memory */
    if ((*num = hash_get_num_entries(ng->cgroups_htab)) <= 0) {
        return NULL;
    }

    /* all cgroup list */
    info_list = (gscgroup_info_t*)palloc0(*num * sizeof(gscgroup_info_t));

    hash_seq_init(&hash_seq, ng->cgroups_htab);

    while ((entry = (gscgroup_entry_t*)hash_seq_search(&hash_seq)) != NULL) {
        gscgroup_info_t* info = info_list + i;

        info->valid = true;

        /* get each cgroup info */
        (void)gscgroup_get_cgroup_info_internal(entry->cg, info);

        info->entry.percent = gscgroup_get_percent(ng, entry->name);
        info->entry.cpuUtil = entry->cpuUtil;

        sret = snprintf_s(info->entry.name, sizeof(info->entry.name), sizeof(info->entry.name) - 1, "%s", entry->name);
        securec_check_intval(sret, pfree(info_list), NULL);
        sret = snprintf_s(
            info->relpath, sizeof(info->relpath), sizeof(info->relpath) - 1, "%s", gsutil_get_cgroup_name(entry->cg));
        securec_check_intval(sret, pfree(info_list), NULL);

        /* the cgroup is valid or not */
        if (info->entry.percent < 0 || info->shares == 0) {
            info->valid = false;
        }

        sret = snprintf_s(info->nodegroup, sizeof(info->nodegroup), sizeof(info->nodegroup) - 1, "%s", ng->group_name);
        securec_check_intval(sret, pfree(info_list), NULL);

        ++i;
    }

    *num = i;

    return info_list;
}

/*
 * @Description: check whether the parent_group and the child_group share the same class or not.
 * @IN cgroup1: left cgroup
 * @IN cgroup2: right cgroup
 * @Return:  whether is in a same class
 * @See also:
 */
bool gscgroup_is_brother_group(const char* cgroup1, const char* cgroup2)
{
    const char* p = NULL;
    const char* q = NULL;

    p = strchr(cgroup1, ':');
    q = strchr(cgroup2, ':');
    /* search ':' first */
    if ((p == NULL) && (q == NULL)) {
        return strcmp(cgroup1, cgroup2) == 0;
    }

    if ((q == NULL) || (p == NULL)) {
        return false;
    }

    /* check length */
    if (p - cgroup1 != q - cgroup2) {
        return false;
    }

    return strncmp(cgroup1, cgroup2, p - cgroup1) == 0;
}
/*
 * @Description: check whether the parent_group and the child_group share the same class or not.
 * @IN parentcg: parent cgroup
 * @IN childcg: child cgroup
 * @Return:  whether is in a same class
 * @See also:
 */
bool gscgroup_is_child_group(const char* parentcg, const char* childcg)
{
    const char* q = NULL;

    /* search ':' first */
    if ((strchr(parentcg, ':') != NULL) || ((q = strchr(childcg, ':')) == NULL)) {
        return false;
    }

    if ((int)strlen(parentcg) != q - childcg) {
        return false;
    }

    return strncmp(parentcg, childcg, q - childcg) == 0;
}

/*
 * @Description: remove level info of the cgroup name
 * @IN gname: group name
 * @Return: new group name
 * @See also:
 */
char* gscgroup_convert_cgroup(char* gname)
{
    char* p = strchr(gname, ':');
    char* q = strrchr(gname, ':');

    if (p != q) {
        *q = '\0';
    }

    return gname;
}
