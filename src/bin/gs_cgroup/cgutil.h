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
 * cgutil.h
 *    main function file for gs_cgroup utility
 *
 * IDENTIFICATION
 *    src/bin/gs_cgroup/cgutil.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GSCGROUP_H
#define GSCGROUP_H

#include <sys/types.h>
#include <pwd.h>

#include "workload/gscgroup.h"
#include "bin/elog.h"

#define fprintf(stream, fmt, ...)                \
    do {                                         \
        if (stdout == stream) {                  \
            (void)fprintf(stdout, fmt, ##__VA_ARGS__); \
        } else {                                 \
            write_stderr(fmt, ##__VA_ARGS__);    \
        }                                        \
    } while (0)

/* option data structure */
typedef struct {
    char eflag;                                  /* exception info flag */
    char cflag;                                  /* if to create cgroup */
    char dflag;                                  /* if to drop cgroup */
    char uflag;                                  /* if to update cgroup */
    char mflag;                                  /* if to mount cgroup */
    char mpflag;                                 /* if to set mount point */
    char umflag;                                 /* if to umount cgroup */
    char fixed;                                  /* flag to set cpu core percentage or io valus */
    int setspct;                                 /* percentage of cpu cores for different groups */
    char setfixed;                               /* flag to set --fixed -g/-s/-t/-b */
    char display;                                /* display the gscgroup.cfg information */
    char ptree;                                  /* display Cgroup tree information */
    char refresh;                                /* refresh cgroup with new conf */
    char upgrade;                                /* upgrade cgroup */
    char revert;                                 /* revert cgroup */
    char recover;                                /* recover cgroup */
    char clssetpct;                              /* specify --fixed and -s when create cgroup */
    char rename;                                 /* specify --rename when create nodegroup cgroup */
    int toppct;                                  /* top group percentage */
    int bkdpct;                                  /* backend group percentage */
    int clspct;                                  /* class percentage in all class */
    int grppct;                                  /* group percentage in a specified class */
    int shares;                                  /* cpu shares for users */
    int weight;                                  /* blkio weight for users */
    char edata[EXCEPT_LEN];                      /* exception data */
    char hpath[MAXPGPATH];                       /* GAUSSHOME path */
    char mpoint[MAXPGPATH];                      /* mount point path */
    char mpoints[MOUNT_SUBSYS_KINDS][MAXPGPATH]; /* subsys mount points */
    char topname[GPNAME_LEN];                    /* top group name */
    char bkdname[GPNAME_LEN];                    /* backend group name */
    char clsname[GPNAME_LEN];                    /* class name */
    char wdname[GPNAME_LEN];                     /* workload name */
    char user[USERNAME_LEN];                     /* user name */
    char sets[CPUSET_LEN];                       /* cpuset value */
    char nodegroup[GPNAME_LEN];                  /* name of nodegroup */
} cgutil_opt_t;

extern gscgroup_grp_t* cgutil_vaddr[GSCGROUP_ALLNUM];
extern cgutil_opt_t cgutil_opt;
extern struct passwd* cgutil_passwd_user;
extern int cgutil_is_sles11_sp2;
extern int cgutil_cpucnt;
extern char cgutil_allset[CPUSET_LEN];
extern char cgutil_mems[CPUSET_LEN];
extern char* current_nodegroup;

/* from cgexec.cpp */
extern int cgexec_check_SLESSP2_version(void);
extern int cgexec_mount_root_cgroup(void);
extern int cgexec_umount_root_cgroup(void);
extern void cgexec_mount_cgroups(void);
extern void cgexec_umount_cgroups(void);

extern int cgexec_delete_cgroups(char* relpath);

extern int cgexec_create_groups(void);
extern int cgexec_drop_groups(void);
extern int cgexec_update_groups(void);
extern int cgexec_refresh_groups(void);
extern void cgexec_upgrade_groups(void);
extern int cgexec_revert_groups(void);
extern int cgexec_recover_groups(void);
extern int cgexec_check_mount_for_upgrade(void);
extern char* cgexec_get_cgroup_cpuset_mems(char* mems, int size);
extern int cgexec_get_mount_points(void);
extern int cgexec_check_cpuset_value(const char* clsset, const char* grpset);

/* from cgexcp.cpp */
extern int cgexcp_class_exception(void);

/* from cgptree.cpp */
extern int cgptree_display_cgroups(void);
extern int cgptree_drop_cgroups(void);
extern int cgptree_drop_nodegroup_cgroups(const char* name);

/* from cgconf.cpp */
extern int cgconf_parse_config_file(void);
extern int cgconf_backup_config_file(void);
extern void* cgconf_map_backup_conffile(bool flag);
extern void cgconf_remove_backup_conffile(void);
extern void cgconf_revert_config_file(void);
extern void cgconf_display_groups(void);
extern int cgconf_exception_is_valid(gscgroup_grp_t* grp, int);

extern void cgconf_set_class_group(int gid);
extern void cgconf_reset_class_group(int gid);
extern void cgconf_set_workload_group(int wgid, int cgid);
extern void cgconf_reset_workload_group(int wgid);

extern void cgconf_set_top_workload_group(int wdgid, int clsgid);

extern void cgconf_update_workload_group(gscgroup_grp_t* grp);
extern void cgconf_update_class_group(gscgroup_grp_t* grp);
extern void cgconf_update_backend_group(gscgroup_grp_t* grp);

extern void cgconf_update_class_percent(void);
extern void cgconf_update_backend_percent(void);
extern void cgconf_update_top_percent(void);

extern int cgconf_parse_nodegroup_config_file(void);

extern char* cgconf_get_config_path(bool);

extern int cgexec_get_cgroup_cpuset_info(int cnt, char** cpuset);

extern void* cgconf_map_origin_conffile(void);
extern void* cgconf_map_nodegroup_conffile(void);
extern int cgexec_create_cm_default_cgroup(void);
extern int cgexec_delete_cm_cgroup(void);
#endif
