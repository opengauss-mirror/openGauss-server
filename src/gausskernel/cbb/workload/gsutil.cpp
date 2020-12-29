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
 * gsutil.cpp
 *    Workload manager common functions
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/gsutil.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h> /* stat */
#include <sys/stat.h>
#include <sys/mman.h> /* mmap */
#include <fcntl.h>
#include <pwd.h>

#include "securec.h"

#include "workload/gscgroup.h"

/*
 * function name: gsutil_filesize
 * description  : return the file size
 * return value :
 *            -1: abnormal
 *         other: normal
 *
 */
long gsutil_filesize(const char* fname)
{
    struct stat buf;
    errno_t rc = memset_s(&buf, sizeof(buf), 0, sizeof(buf));
    securec_check_errno(rc, , -1);
    long ret = stat(fname, &buf);

    return (-1 == ret ? -1 : buf.st_size);
}

/*
 * function name: gsutil_filemap
 * description  : return the mapping address of a file;
 *                if the file doesn't exist, create new one and map address.
 * arguments    :
 *         fname: file name
 *        nbytes: the specified file size
 *          prot: protection flag
 *         flags: mapping flag
 *   passwd_user: user information
 * return value :
 *          NULL: abnormal
 *         other: normal
 *
 */
void* gsutil_filemap(const char* fname, size_t nbytes, int prot, int flags, struct passwd* passwd_user)
{
    long fsize;
    int fcreate = 0;
    int fgrow = 0;
    int fshrink = 0;
    int fd = 0;
    int file_flags = O_RDWR;
    int fmode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
    void* vadd = NULL;
    int ret;
    struct stat statbuf;
    errno_t rc;
        
    rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
    securec_check_c(rc, "\0", "\0");
    ret = lstat(fname, &statbuf);
    if ((ret == 0) && S_ISLNK(statbuf.st_mode)) {
        fprintf(stderr, "ERROR: The file name is a symbol link [%s]!\n", fname);
        return NULL;
    }

    fsize = gsutil_filesize(fname);
    if (-1 == fsize) {
        if (nbytes == 0) { /* no size */
            return NULL;
        }

        fcreate = 1;
        file_flags |= O_CREAT | O_TRUNC;
    } else if (fsize < (long)nbytes) {
        fgrow = 1;
    } else if (fsize > (long)nbytes) {
        fshrink = 1;
    }

    fsize = (long)nbytes;

    if (-1 == (fd = open(fname, file_flags, fmode))) {
        return NULL;
    }

    if (-1 == fchown(fd, passwd_user->pw_uid, passwd_user->pw_gid)) {
        close(fd);
        return NULL;
    }

    if (fcreate) {
        if ((-1 == lseek(fd, fsize - 1, SEEK_SET)) || (-1 == write(fd, "", 1))) {
            close(fd);
            (void)unlink(fname);
            return NULL;
        }
    } else if (fgrow) {
        if ((-1 == lseek(fd, fsize - 1, SEEK_SET)) || (-1 == write(fd, "", 1))) {
            close(fd);
            return NULL;
        }
    } else if (fshrink) {
        if (-1 == ftruncate(fd, fsize)) {
            close(fd);
            return NULL;
        }
    }

    if (MAP_FAILED == (vadd = mmap(NULL, (size_t)fsize, prot, flags, fd, 0))) {
        close(fd);
        if (fcreate) {
            unlink(fname);
        }
        return NULL;
    }

    close(fd);

    return vadd;
}

/*
 * function name: gsutil_get_cpu_count
 * description  : get the cpu core count on this host
 * return value :
 *           -1 : the abnormal value
 *
 */
int gsutil_get_cpu_count(void)
{
    char pathbuf[4096] = {0};
    int ret = 0;
    int cpucnt = 0;
    errno_t rc;

    if (0 == access("/sys/devices/system", F_OK)) {
        do {
            rc = snprintf_s(pathbuf, sizeof(pathbuf), sizeof(pathbuf) - 1, "/sys/devices/system/cpu/cpu%d", cpucnt);
            securec_check_intval(rc, , -1);

            ret = access(pathbuf, F_OK);
            if (ret == 0) {
                cpucnt++;
            }
        } while (ret == 0);
    } else {
        FILE* fd = NULL;

        if ((fd = fopen("/proc/cpuinfo", "r")) == NULL) {
            return -1;
        }

        while (fgets(pathbuf, sizeof(pathbuf), fd)) {
            if (strncmp("processor", pathbuf, strlen("processor")) == 0) {
                cpucnt++;
            }
        }
        fclose(fd);
    }

    return cpucnt ? cpucnt : -1;
}
/*
 * function name: gsutil_print_exception_flag
 * description  : print name of exception flag
 * return value : exception name
 */
char* gsutil_print_exception_flag(int eflag)
{
    char* ename[] = {"ABORT", "PENALTY"};

    if (eflag < 0 || eflag >= EXCEPT_ALL_KINDS) {
        return "Unknown";
    }

    return ename[eflag];
}
/*
 * function name: gsutil_exception_kind_is_valid
 * description  : check a kind of exception whether is valid
 * return value : 1: valid
 *                0: invalid
 */
int gsutil_exception_kind_is_valid(gscgroup_grp_t* grp, int kind)
{
    if (grp->except[kind].blocktime > 0 || grp->except[kind].elapsedtime > 0 || grp->except[kind].spoolsize > 0 ||
        grp->except[kind].allcputime > 0 || grp->except[kind].skewpercent > 0 || grp->except[kind].qualitime > 0 ||
        grp->except[kind].broadcastsize > 0) {
        return 1;
    }

    return 0;
}
/*
 * function name: gsutil_exception_is_valid
 * description  : check all kinds of exception whether are valid
 * return value : 1: valid
 *                0: invalid
 */
int gsutil_exception_is_valid(gscgroup_grp_t* grp, int kinds)
{
    int i = 0;

    for (i = 0; i < kinds; ++i) {
        if (gsutil_exception_kind_is_valid(grp, i)) {
            return 1;
        }
    }

    return 0;
}

/*
 * @Description: get cgroup name from cgroup
 * @IN cg: control group
 * @Return: cgroup name
 * @See also:
 */
char* gsutil_get_cgroup_name(struct cgroup* cg)
{
    /* check cgroup */
    if (cg == NULL) {
        return NULL;
    }

    return (char*)cg;
}

