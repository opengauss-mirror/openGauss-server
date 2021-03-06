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
 * ---------------------------------------------------------------------------------------
 * 
 * gscgroup.h
 *     header file to export the functions to use cgroup
 * 
 * IDENTIFICATION
 *        src/include/workload/gscgroup.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __GS_CGROUPS__
#define __GS_CGROUPS__

#ifndef gettid

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>

#include "c.h"
#include "gs_threadlocal.h"

#include "datatype/timestamp.h"

#define gettid() (pid_t) syscall(__NR_gettid)

#endif

#include "securec.h"
#include "securec_check.h"

#define securec_check_berrno(errno, express, retval) \
    {                                                \
        if (errno != EOK) {                          \
            express;                                 \
            return retval;                           \
        }                                            \
    }

#define check_errno(errno, express, retval, file, line) \
    {                                                   \
        if (EOK != errno) {                             \
            fprintf(stderr,                             \
                "%s:%d failed on calling "              \
                "security function.\n",                 \
                file,                                   \
                line);                                  \
            express;                                    \
            return retval;                              \
        }                                               \
    }

#define securec_check_errno(errno, express, retval) check_errno(errno, express, retval, __FILE__, __LINE__)

#define check_intval(errno, express, retval, file, line) \
    {                                                    \
        if (errno == -1) {                               \
            fprintf(stderr,                              \
                "%s:%d failed on calling "               \
                "security function.\n",                  \
                file,                                    \
                line);                                   \
            express;                                     \
            return retval;                               \
        }                                                \
    }

#define securec_check_intval(val, express, retval) check_intval(val, express, retval, __FILE__, __LINE__)

/*
 * Cgroups is divided into 4 purposes:
 *     Backend Cgroup: used to control the backend threads
 *     Class Cgroup: used to specify the resource percentage for classes
 *     DefaultWD Cgroup: used to control the default workload threads
 *     Timeshare Cgroup: used to control the timeshare threads
 *
 * Usually, Gaussdb cgroup has 80% dynamic resource of the whole system,
 * but it can't beyond the 95% hard limitation.
 *
 * The resource Ratio for Class and Backend will be 1:2 as default.
 *
 * Each class can be assigned the specified resource percentage from
 * Class cgroup based on requirement.
 *
 * Under each class cgroup, the default workload cgroup can be assigned the
 * specified resource percentage; and its maximum level will be 5 as default.
 *
 * Each Class cgroup has the timeshare cgroups;
 * The resource for timeshare is the remained resource of the class;
 * So it must make sure that all resource for default workload is not the total of user.
 *
 * The logic graph of cgroup is deployed.
 * a. /dev/cgroups is the mount point of cgroups
 * b. Gaussdb is the top cgroup of Gauss Database
 * c. Backend is the cgroup of backend threads
 * e. Class is the top cgroup of users' query threads
 * f. The name rule for default workload cgroups should be made up with "name:level"
 * h. The default Class and default workload cgroup are provided
 * i. At most, there are 6 levels for each user (4 levels for defaultwd and 2 level for timeshare)
 * j. There are only 2 cgroups for default workload in each level
 * f. Rush/High/Medium/Low is the timeshare cgroup in the bottom line of each tree path


                /dev/cgroups/		 ------- mount point
                    |
    tasks (non-gaussdb threads)		Gaussdb/ ------- database topdir
                        |
    Backend/	tasks(no threads)	Class/	 ------- top class dir
    |					|
    Vacuum/ ... tasks(backend threads)	DefaultClass/ Class1/ ... 	--- class dir
    |					|
    tasks(vacuum thread)			DefaultWD:1/	RemainWD:1/ tasks(no threads) --- defaultwd dir (Level 1)
                        |		|
                        tasks (query)	DefaultWD:2/ RemainWD:2/ tasks(no threads) --- defaultwd dir (Level 2)
                                        |
                                        Timeshare:3/ tasks(no threads) --- top timeshare dir (Level 3)
                                        |
                            Rush:4/	High:4/	Medium:4/ Low:4/ tasks(no threads)  --- timeshare dir (Level 4)
                                    |
                                tasks(query threads)


 * Default values:
 * 0. The top level cgroup number is 4: mountpoint, Gaussdb, Backend and Class;
 * 1. The maximum number for backend cgroup is 16;
 * 2. The maximum number for class cgroup is 64;
 * 3. The maximum number for defaultwd and timeshare cgroup is 256 (64 * 4 = all timeshare cgroups);
 *    If user want to create one defaultwd cgroup, but there is no slot, it will fail.
 * 4.
 */

#define MOUNT_SUBSYS_KINDS 5
#define MOUNT_CPU_ID 0
#define MOUNT_CPUACCT_ID 1    /* cpu and cpuacct could not be separated */
#define MOUNT_BLKIO_ID 2
#define MOUNT_CPUSET_ID 3
#define MOUNT_MEMORY_ID 4

#define MOUNT_CPU_NAME "cpu"
#define MOUNT_BLKIO_NAME "blkio"
#define MOUNT_CPUSET_NAME "cpuset"
#define MOUNT_CPUACCT_NAME "cpuacct"
#define MOUNT_MEMORY_NAME "memory"

#define GSCGROUP_CONF_DIR "etc"
#define GSCFG_BACKUP ".bak"

#define GSCGROUP_MOUNT_POINT "/sys/fs/cgroup"
#define GSCGROUP_MOUNT_POINT_OLD "/dev/cgroups"

#define GSCGROUP_ROOT "Root"
#define GSCGROUP_TOP_DATABASE "Gaussdb"
#define GSCGROUP_CM "CM"
#define GSCGROUP_TOP_BACKEND "Backend"
#define GSCGROUP_TOP_CLASS "Class"
#define GSCGROUP_DEFAULT_CLASS "DefaultClass"
#define GSCGROUP_REMAIN_WORKLOAD "RemainWD"
#define GSCGROUP_TOP_TIMESHARE "Timeshare"
#define GSCGROUP_RUSH_TIMESHARE "Rush"
#define GSCGROUP_HIGH_TIMESHARE "High"
#define GSCGROUP_MEDIUM_TIMESHARE "Medium"
#define GSCGROUP_LOW_TIMESHARE "Low"
#define GSCGROUP_DEFAULT_BACKEND "DefaultBackend"
#define GSCGROUP_VACUUM "Vacuum"
#define GSCGROUP_TOP_WORKLOAD "TopWD"
#define GSCGROUP_INVALID_GROUP "InvalidGroup"

#define GSCGROUP_DEFAULT_CGNAME GSCGROUP_MEDIUM_TIMESHARE

#define GSCGROUP_TOPNUM 4      /* 4 groups for top cgroups */
#define GSCGROUP_BAKNUM 16     /* 16 groups for backend threads */
#define GSCGROUP_CLASSNUM 64   /* 64 groups for classes */
#define GSCGROUP_WDNUM_OLD 256 /* 256 groups for default workload threads */
#define GSCGROUP_WDNUM 640     /* 640 groups for default workload threads */
#define GSCGROUP_TSNUM 4       /* 4 groups for timeshare workload threads */
#define GSCGROUP_ALLNUM_OLD \
    (GSCGROUP_TOPNUM + GSCGROUP_BAKNUM + GSCGROUP_CLASSNUM + GSCGROUP_WDNUM_OLD + GSCGROUP_TSNUM)
#define GSCGROUP_ALLNUM (GSCGROUP_TOPNUM + GSCGROUP_BAKNUM + GSCGROUP_CLASSNUM + GSCGROUP_WDNUM + GSCGROUP_TSNUM)

/* Timeshare Rate Value */
#define TS_RUSH_RATE 8   /* the rate of rush timeshare group */
#define TS_HIGH_RATE 4   /* the rate of high timeshare group */
#define TS_MEDIUM_RATE 2 /* the rate of medium timeshare group */
#define TS_LOW_RATE 1    /* the rate of low timeshare group */
#define TS_ALL_RATE (TS_RUSH_RATE + TS_HIGH_RATE + TS_MEDIUM_RATE + TS_LOW_RATE)

#define TOPCG_START_ID 0
#define TOPCG_ROOT 0
#define TOPCG_GAUSSDB 1
#define TOPCG_BACKEND 2
#define TOPCG_CLASS 3
#define TOPCG_END_ID (GSCGROUP_TOPNUM - 1) /*3*/

#define BACKENDCG_START_ID (TOPCG_END_ID + 1)                       /*4*/
#define BACKENDCG_END_ID (BACKENDCG_START_ID + GSCGROUP_BAKNUM - 1) /*19*/

#define CLASSCG_START_ID (BACKENDCG_END_ID + 1)                   /*20*/
#define CLASSCG_END_ID (CLASSCG_START_ID + GSCGROUP_CLASSNUM - 1) /*83*/

#define WDCG_START_ID (CLASSCG_END_ID + 1)                       /*84*/
#define WDCG_END_ID_OLD (WDCG_START_ID + GSCGROUP_WDNUM_OLD - 1) /*84+255*/
#define WDCG_END_ID (WDCG_START_ID + GSCGROUP_WDNUM - 1)         /*84+640-1*/

#define TSCG_START_ID_OLD (WDCG_END_ID_OLD + 1)
#define TSCG_START_ID (WDCG_END_ID + 1)
#define TSCG_END_ID (TSCG_START_ID + GSCGROUP_TSNUM - 1)

#define DEFAULT_CPU_SHARES 1024
#define DEFAULT_GAUSS_CPUSHARES 5120 /* about 5x non-database resource */
#define DEFAULT_CM_CPUSHARES 8192    /* about 8x non-database resource */
#define MAX_CLASS_CPUSHARES 10000    /* for easy calculation */

#define TOP_BACKEND_PERCENT 40                        /* 40% percentage of gaussdb resource */
#define TOP_CLASS_PERCENT (100 - TOP_BACKEND_PERCENT) /* 60% percentage of gaussdb resource */

#define DEFAULT_BACKEND_PERCENT 80                        /* 80% percentage of top backend resource */
#define VACUUM_PERCENT 20                                 /* 20% percentage of top backend resource */
#define DEFAULT_CLASS_PERCENT 20                          /* 20% percentage of top class resource */
#define OTHER_CLASS_PERCENT (100 - DEFAULT_CLASS_PERCENT) /* 60% percentage of other class resource */
#define DEFAULT_WORKLOAD_PERCENT 20                       /* 20% percentage of default class resource */
#define TOPWD_PERCENT 90                                  /* 90% percentage of Class used for top wd */

#define DEFAULT_CPU_PERIOD 100000
#define DEFAULT_IO_WEIGHT 500
#define MIN_IO_WEIGHT 100
#define MAX_IO_WEIGHT 1000

#define GROUP_ALL_PERCENT 100
#define TOPWD_PERCENT 90
#define NORMALWD_PERCENT 10

#define DEFAULT_CPUSKEWPCT 30
#define DEFAULT_QUALITIME 1800

/* gscgroup.cfg saves the cgroups information and is placed under etc directory */
#define GSCFG_PREFIX "gscgroup"
#define GSCFG_SUFFIX ".cfg"

#define WD_TOP_LEVEL 1
#define MAX_WD_LEVEL 10
#define GPNAME_LEN 64 /* name:level */
#define IODATA_LEN 96 /* support about 6 ~ 8 disk devices */
#define EXCEPT_LEN 256
#define USERNAME_LEN 56 /* 64 - sizeof("Gaussdb:") */
#define GPNAME_PATH_LEN (GPNAME_LEN * (MAX_WD_LEVEL + 1))
#define PROCLINE_LEN 4096
#define CPUSET_OLD_LEN 8
#define CPUSET_LEN 64
#define SUBSYS_LEN 8

#define NANOSECS_PER_SEC ((int64)(1000000000))

#define IO_WEIGHT_CALC(weight, percent) \
    ((weight * (percent) / 100) > MIN_IO_WEIGHT ? (weight * (percent) / 100) : MIN_IO_WEIGHT)

#define CPU_SHARES "cpu.shares"
#define CPU_QUOTA "cpu.cfs_quota_us"
#define CPU_PERIOD "cpu.cfs_period_us"
#define BLKIO_WEIGHT "blkio.weight"
#define BLKIO_BPSREAD "blkio.throttle.read_bps_device"
#define BLKIO_IOPSREAD "blkio.throttle.read_iops_device"
#define BLKIO_BPSWRITE "blkio.throttle.write_bps_device"
#define BLKIO_IOPSWRITE "blkio.throttle.write_iops_device"
#define CPUSET_CPUS "cpuset.cpus"
#define CPUSET_MEMS "cpuset.mems"
#define CPUACCT_USAGE "cpuacct.usage"

#define EXCEPT_ALL_KINDS 2

#define EXCEPT_ERROR -2
#define EXCEPT_NONE -1
#define EXCEPT_ABORT 0
#define EXCEPT_PENALTY 1

#define EXCEPT_FLAG(eflag) ((eflag) + 1)
#define IS_EXCEPT_FLAG(eflag, except) ((eflag) == EXCEPT_FLAG((except)))

#define CGroupIsValid(group) (NULL != (void*)group && *group && strcmp(group, GSCGROUP_INVALID_GROUP) != 0)
#define CGroupIsDefault(group) (NULL != (void*)group && *group && strcmp(group, "DefaultClass:Medium") == 0)

#define V1R5_VERSION 1 /* used for upgrade from v1r5 */
#define V1R6_VERSION 2 /* used for upgrade from v1r6 */

/*
 * get the cpuset start value
 * astart: the upper level cpu core start value
 * aend:   the upper level cpu core end value
 * bsum:   the sum of cpu cores except the current updating group
 * bmax:   the max cpu core of the same level other groups
 * blen:   the length of the cpu cores of the current updating group
 */
#define GET_CPUSET_START_VALUE(astart, aend, bsum, bmax, blen)                                               \
    (((bsum) + (blen) <= (aend) - (astart) + 1)                                                              \
            ? (((aend) - (bmax) >= (blen)) ? (((bsum) > 0) ? ((bmax) + 1) : (astart)) : ((astart) + (bsum))) \
            : ((aend) - (blen) + 1))

/* type of all cgroups */
typedef enum {
    GROUP_NONE,
    GROUP_TOP,   /* the top cgroup */
    GROUP_CLASS, /* the class cgroup with the given resource,
                    not to control any threads */
    GROUP_BAKWD, /* the cgroup to control the backend threads */
    GROUP_DEFWD, /* the default workload cgroup for each class,
                    to control the query threads in the given level */
    GROUP_TSWD   /* the timeshare group for each user,
                    to control the query threads in the bottom level */
} group_type;

/* type of resource allocation */
typedef enum {
    ALLOC_DYNAMIC, /* the dynamic allocation method */
    ALLOC_FIXED    /* the fixed allocation method */
} alloc_type;

typedef struct {
    int shares;                 /* the value of cpu.shares */
    int weight;                 /* the value of blkio.weight */
    int quota;                  /* the percentage of cpuset. */
    char iopsread[IODATA_LEN];  /* the value of blkio.throttle.read_iops_device */
    char iopswrite[IODATA_LEN]; /* the value of blkio.throttle.write_iops_device */
    char bpsread[IODATA_LEN];   /* the value of blkio.throttle.read_bps_device */
    char bpswrite[IODATA_LEN];  /* the value of blkio.throttle.write_bps_device */
} alloc_old_info_t;

typedef struct {
    int shares;                 /* the value of cpu.shares */
    int weight;                 /* the value of blkio.weight */
    int quota;                  /* the percentage of cpuset. */
    int spare;                  /* not used */
    char iopsread[IODATA_LEN];  /* the value of blkio.throttle.read_iops_device */
    char iopswrite[IODATA_LEN]; /* the value of blkio.throttle.write_iops_device */
    char bpsread[IODATA_LEN];   /* the value of blkio.throttle.read_bps_device */
    char bpswrite[IODATA_LEN];  /* the value of blkio.throttle.write_bps_device */
} alloc_info_t;

typedef struct {
    int percent; /* the percent value in gaussdb */
} gscgroup_top_t;

typedef struct {
    int tgid;     /* top gid */
    int maxlevel; /* the maxlevel of defaultwd cgroup */
    int percent;  /* the percentage value in top class */
    int rempct;   /* the remain percentage for workload */
} gscgroup_class_t;

typedef struct {
    int cgid;    /* class gid */
    int wdlevel; /* the level of the defaultwd cgroup */
    int percent; /* the percentage value in parent class */
} gscgroup_wd_t;

typedef struct {
    int cgid; /* class gid */
    int rate; /* the rate based on the default value */
} gscgroup_ts_t;

typedef union {
    gscgroup_top_t top;   /* top group special information */
    gscgroup_class_t cls; /* class group special information */
    gscgroup_wd_t wd;     /* defaultwd group special information */
    gscgroup_ts_t ts;     /* timeshare group special information */
} group_info_t;

/* the throttle value for exception verification */
typedef struct {
    unsigned int blocktime;   /* the start time  - the submit time of query */
    unsigned int elapsedtime; /* the end time - the start time */
    unsigned int allcputime;  /* the CPU time of all the node */
    unsigned int qualitime;   /* the qualification time of all the node */
    unsigned int skewpercent; /* the CPU skew percent is the cuptime/allcputime */
    unsigned int spare[5];    /* not used */
} except_old_data_t;

/* the throttle value for exception verification */
typedef struct {
    unsigned int blocktime;   /* the start time  - the submit time of query */
    unsigned int elapsedtime; /* the end time - the start time */
    unsigned int allcputime;  /* the CPU time of all the node */
    unsigned int qualitime;   /* the qualification time of all the node */
    unsigned int skewpercent; /* the CPU skew percent is the cuptime/allcputime */
    unsigned int reserved;    /* reserved flag */
    int64 spoolsize;          /* spill size, unit is MB*/
    int64 broadcastsize;      /* broadcast size, unit is MB*/
    unsigned int spare[14];   /* not used */
} except_data_t;

/* old version group information */
typedef struct {
    unsigned short used;                        /* flag if this group has been used */
    unsigned short gid;                         /* the group id */
    group_type gtype;                           /* top, backend, class, defaultwd or timeshare */
    group_info_t ginfo;                         /* group special information */
    unsigned short percent;                     /* the percentage value calculated by dynamic or fixed value */
    char grpname[GPNAME_LEN];                   /* the cgroup name */
    alloc_old_info_t ainfo;                     /* resource alloc information */
    except_old_data_t except[EXCEPT_ALL_KINDS]; /* the exception data for aborting */
    char cpuset[CPUSET_OLD_LEN];                /* cpuset info */
    int spare[3];                               /* not used */
} gscgroup_old_grp_t;

/* group information */
typedef struct {
    unsigned short used;                    /* flag if this group has been used */
    unsigned short gid;                     /* the group id */
    group_type gtype;                       /* top, backend, class, defaultwd or timeshare */
    group_info_t ginfo;                     /* group special information */
    char grpname[GPNAME_LEN];               /* the cgroup name */
    alloc_info_t ainfo;                     /* resource alloc information */
    except_data_t except[EXCEPT_ALL_KINDS]; /* the exception data for aborting */
    char cpuset[CPUSET_LEN];                /* cpuset info */
    unsigned int percent;                   /* the percentage value calculated by dynamic or fixed value */
    int spare[63];                          /* not used , total size is 1024 bytes */
} gscgroup_grp_t;

typedef struct {
    char name[GPNAME_LEN]; /* classname:workloadname */
    int percent;           /* percent of the group */
    int cpuUtil;           /* percent of the cpu usage */
    int cpuCount;          /* the count of the cpu cores */
    int usedCpuCount;      /* used cpu cores */
    int64 cpuUsedAcct;     /* cpu usage value */
    int64 cpuLastAcct;     /* last cpu value */
    TimestampTz lastTime;  /* last collect time */
    struct cgroup* cg;     /* cgroup structure */
    struct cgroup* oldcg;  /* old cgroup structure */
} gscgroup_entry_t;

typedef struct {
    gscgroup_entry_t entry;        /* cgroup entry */
    char relpath[GPNAME_PATH_LEN]; /* relative path */
    char cpuset[CPUSET_LEN];       /* cpu set */
    char nodegroup[GPNAME_LEN];    /* node group name */
    int shares;                    /* cpu shares */
    bool valid;                    /* cgroup is valid or not */
} gscgroup_info_t;

typedef enum { GSCGROUP_NONE_STMT = 0, GSCGROUP_NORMAL_STMT, GSCGROUP_TOP_STMT, GSCGROUP_VACUUM_STMT } gscgroup_stmt_t;

/* get the file size */
extern long gsutil_filesize(const char* fname);

/* return mapping address */
extern void* gsutil_filemap(const char* fname, size_t nbytes, int prot, int flags, struct passwd* passwd_user);

/* get the parent path of workload group */
extern char* gscgroup_get_parent_wdcg_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* nodegroup = NULL);

/* get the top timeshare path of class group */
extern char* gscgroup_get_topts_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* nodegroup = NULL);
/* get the relative path of any specified group */
extern char* gscgroup_get_relative_path(int cnt, gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM], char* nodegroup = NULL);
/* print exception flag */
extern char* gsutil_print_exception_flag(int eflag);

/* check the exception whether is valid */
extern int gsutil_exception_is_valid(gscgroup_grp_t* grp, int kinds);

/* check the exception kiind whether is valid */
extern int gsutil_exception_kind_is_valid(gscgroup_grp_t* grp, int kind);

/* get the cpu count */
extern int gsutil_get_cpu_count();

#endif
