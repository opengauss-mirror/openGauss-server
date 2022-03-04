/**
 * @file cm_cgroup.cpp
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-06
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <mntent.h>
#include <libcgroup.h>
#include <pwd.h>

#include "cm/elog.h"
#include "cm/cm_cgroup.h"
#include "securec.h"

#define GSCGROUP_CM "CM"
#define GPNAME_PATH_LEN 1024

static struct cgroup* gscgroup_get_cgroup(const char* relpath);

typedef int errno_t;

/*
 * function name: gscgroup_cm_init
 * description  : This function return a pointer of cm cgroup path
 *                if initialization were successful or else return NULL.
 * Note         : Please note,the caller must free the return value.
 */

char* gscgroup_cm_init()
{
    int ret = 0;
    errno_t rc;

    struct mntent* ent = NULL;
    char* mntent_buffer = NULL;
    struct mntent temp_ent = {0};

    char abspath[GPNAME_PATH_LEN] = {0};
    char* relpath = NULL;
    size_t mnt_dir_len = 0;
    size_t relpath_len;

    struct passwd* pw = NULL;
    struct stat buf = {0};

    write_runlog(LOG, "starting: gscgroup_cm_init().\n");
    ret = cgroup_init();
    if (ret) {
        write_runlog(WARNING, "Cgroup initialization failed: %s.\n", cgroup_strerror(ret));
        return NULL;
    }

    write_runlog(LOG, "finished: cgroup_init().\n");
    /* open '/proc/mounts' to load mount points */
    FILE* proc_mount = fopen("/proc/mounts", "re");
    if (proc_mount == NULL) {
        goto error;
    }

    /* The buffer is too big, so it can not be stored in the stack space. */
    mntent_buffer = (char*)malloc(4 * FILENAME_MAX);
    if (mntent_buffer == NULL) {
        write_runlog(ERROR, "Failed to allocate memory: Out of memory. RequestSize=%d.\n", 4 * FILENAME_MAX);
        goto error;
    }

    while ((ent = getmntent_r(proc_mount, &temp_ent, mntent_buffer, 4 * FILENAME_MAX)) != NULL) {
        if (strcmp(ent->mnt_type, "cgroup") != 0 || strstr(ent->mnt_opts, "cpu") == NULL) {
            continue;
        } else {
            /* get the cm cgroup abspath */
            pw = getpwuid(getuid());
            if (pw == NULL) {
                goto error;
            }

            mnt_dir_len = strlen(ent->mnt_dir);
            rc = sprintf_s(abspath, GPNAME_PATH_LEN, "%s/%s:%s", ent->mnt_dir, GSCGROUP_CM, pw->pw_name);
            if (rc == -1) {
                goto error;
            }

            break;
        }
    }

    if (stat(abspath, &buf) != 0) {
        write_runlog(WARNING, "can not get cm cgroup dir path at %s.\n", abspath);
        goto error;
    }

    relpath_len = strlen(abspath) - mnt_dir_len;
    relpath = (char*)malloc(relpath_len);
    if (relpath == NULL) {
        goto error;
    }

    /* eg. 'mnt_dir_len + 1' may be '/dev/cgroup/cpu/' ,
     * so we should plus 1 that represents '/'.
     */
    rc = strcpy_s(relpath, relpath_len, abspath + mnt_dir_len + 1);
    if (rc == -1) {
        write_runlog(WARNING, "can not strcpy cgroup relpath from abspath. abspath is %s.\n", abspath);
        goto error;
    }
    if (proc_mount != NULL) {
        fclose(proc_mount);
    }
    if (mntent_buffer != NULL) {
        free(mntent_buffer);
    }

    write_runlog(LOG, "get cm cgroup relpath succeed, this path is %s.\n", relpath);
    return relpath;

error:
    if (proc_mount != NULL) {
        fclose(proc_mount);
    }
    if (relpath != NULL) {
        free(relpath);
    }
    if (mntent_buffer != NULL) {
        free(mntent_buffer);
    }

    write_runlog(WARNING, "CM cgroup initilization failed.\n");
    return NULL;
}

/*
 * function name: gscgroup_cm_attach_task
 * description  : attach cm process into a specified cgroup.
 */
void gscgroup_cm_attach_task(const char* relpath)
{
    int ret;
    struct cgroup* cg = NULL;

    /* get the cgroup structure */
    cg = gscgroup_get_cgroup(relpath);

    if (NULL == cg) {
        write_runlog(WARNING, "can not get cgroup from relpath, relpath is %s.\n", relpath);
        return;
    } else {
        /* attach current thread into the cgroup */
        ret = cgroup_attach_task(cg);
        if (ret != 0) {
            write_runlog(WARNING,
                "Cgroup failed to attach "
                "into \"%s\" group: %s(%d).\n",
                relpath,
                cgroup_strerror(ret),
                ret);
        }
    }

    cgroup_free(&cg);
}

/*
 * function name: gscgroup_cm_attach_task_pid
 * description  : attach a process into a specified cgroup.
 */
void gscgroup_cm_attach_task_pid(const char* relpath, pid_t tid)
{
    int ret;
    struct cgroup* cg = NULL;

    /* get the cgroup structure */
    cg = gscgroup_get_cgroup(relpath);

    if (NULL == cg) {
        write_runlog(WARNING, "can not get cgroup from relpath, relpath is %s.\n", relpath);
    } else {
        /* attach current thread into the cgroup */
        ret = cgroup_attach_task_pid(cg, tid);
        if (ret != 0) {
            write_runlog(WARNING,
                "Cgroup failed to attach "
                "into \"%s\" group: %s(%d).\n",
                relpath,
                cgroup_strerror(ret),
                ret);
        }
    }

    cgroup_free(&cg);
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
static struct cgroup* gscgroup_get_cgroup(const char* relpath)
{
    int ret = 0;
    struct cgroup* cg = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        write_runlog(WARNING, "Cgroup %s failed to call cgroup_new_cgroup.\n", relpath);
        return NULL;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        write_runlog(WARNING, "Cgroup get_cgroup %s information: %s(%d).\n", relpath, cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return NULL;
    }

    return cg;
}
