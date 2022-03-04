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
 * ioschdl.cpp
 *    mainly about two things:
 *    1. Create a backend thread to collect IO information of database system.
 *    2. dispatch IO requests.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/ioschdl.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/atomic.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "workload/commgr.h"
#include "workload/workload.h"
#include "workload/statctl.h"
#include <mntent.h>
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/poolutils.h"
#endif

static void WLMmonitor_MainLoop(void);
static void WLMmonitor_worker(int type);

static void WLMarbiter_MainLoop(void);

extern void WLMSigHupHandler(SIGNAL_ARGS);
extern void WLMParseIOInfo(StringInfo msg, void* info, int size);
extern void WLMUpdateSessionIops(void);
extern void WLMCollectInstanceStat(void);
extern void WLMPersistentInstanceStat(void);
extern void WLMCleanupHistoryInstanceStat(void);

extern THR_LOCAL bool g_pq_interrupt_happened;

#define UPDATETABLE(table, value, num)  \
    {                                   \
        int i = 0;                      \
        for (i = 0; i < (num) - 1; i++) { \
            table[i] = table[i + 1];    \
        }                               \
        table[i] = (value);             \
    }

/******************************************************************************
functions----get disk IO information of the specified device
*******************************************************************************/
/*
 * @Description: get hz -- how many ticks per seconds.
 * @Return: void
 * @See also:
 */
static void WLMmonitor_get_HZ(void)
{
    long ticks;

    if ((ticks = sysconf(_SC_CLK_TCK)) == -1) {
        ereport(LOG, (errmsg("get hz failed")));
    }

    g_instance.wlm_cxt->io_context.hz = (unsigned int)ticks;
    ereport(DEBUG1, (errmsg("hz is %u", g_instance.wlm_cxt->io_context.hz)));
}

/*
 * @Description: get the average values.
 *               mainly handle the overflown case
 *
 * @IN:         value1: the previous value
 * @IN:         value2: the current value
 * @IN:         itv: the time interval between two monitoring times
 * @IN:         unit: might be hz or 100.
 *              hz: get average value per second.
 *                  itv is interval of total cpu time read from '/proc/stat',
 *                  which is real time multiplied by 'hz'.
 *                  So hz is needed be divided to get the real time interval.
 *              100: get the percentage of the values
 *
 * @Return:     the difference of value1 and value2 per second
 * @See also:
 */
double WLMmonitor_get_average_value(uint64 value1, uint64 value2, uint64 itv, uint32 unit)
{
    if ((value2 < value1) && (value1 <= 0xffffffff)) {
        /* Counter's type was unsigned long and has overflown */
        return ((double)((value2 - value1) & 0xffffffff)) / itv * unit;
    } else {
        return AVERAGE_DISK_VALUE(value1, value2, itv, unit);
    }
}

/*
 * get the device name
 */
void WLMmonitor_get_device_name(const char* name, char* devicename)
{
    char devicebuf[MAX_DEVICE_DIR];
    errno_t rc;
    int ret = readlink(name, devicebuf, MAX_DEVICE_DIR);
    if (ret != -1 && ret < MAX_DEVICE_DIR) {
        devicebuf[ret] = '\0';
        if (strncmp("/dev/", devicebuf, strlen("/dev/")) == 0) {
            rc = strncpy_s(
                devicename, MAX_DEVICE_DIR, devicebuf + strlen("/dev/"), strlen(devicebuf + strlen("/dev/")));
            securec_check(rc, "\0", "\0");
        } else {
            char* tmp = devicebuf;
            while (*tmp != '\0' && (*tmp == '.' || *tmp == '/')) {
                tmp++;
            }

            rc = strcpy_s(devicename, MAX_DEVICE_DIR, tmp);
            securec_check(rc, "\0", "\0");
        }
    } else {
        rc = strncpy_s(devicename, MAX_DEVICE_DIR, name + strlen("/dev/"), strlen(name + strlen("/dev/")));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * @Description: get the mounted device name by reading the file "/proc/mounts"
 * @IN:         datadir: data directory.
 * @OUT:        devicename: device name like /dev/sda2
 * @Return:     void
 * @See also:
 */
void WLMmonitor_dn_get_disk(const char* datadir, char* devicename)
{
    unsigned int buf_len = 0;
    unsigned int len_count = 0;
    FILE* mtfp = NULL;
    struct mntent* ent = NULL;
    char mntent_buffer[4 * FILENAME_MAX];
    struct mntent temp_ent;

    mtfp = fopen(FILE_MOUNTS, "r");

    if (NULL == mtfp) {
        ereport(LOG, (errmsg("cannot open file %s.\n", FILE_MOUNTS)));
        return;
    }

    while ((ent = getmntent_r(mtfp, &temp_ent, mntent_buffer, sizeof(mntent_buffer))) != NULL) {
        buf_len = strlen(ent->mnt_dir);

        /*
         * get the file system with type of ext* or xfs.
         * find the best fit for the data directory
         */
        if (strncmp(ent->mnt_dir, datadir, buf_len) == 0 && strlen(datadir) >= buf_len &&
            (strncmp(ent->mnt_type, "ext", 3) == 0 || strcmp(ent->mnt_type, "xfs") == 0) && len_count < buf_len) {
            if (strlen(ent->mnt_fsname) > strlen("/dev/") && strncmp("/dev/", ent->mnt_fsname, strlen("/dev/")) == 0) {
                len_count = buf_len;
                WLMmonitor_get_device_name(ent->mnt_fsname, devicename);
            }
        }
    }

    ereport(DEBUG3, (errmsg("devicename is %s", devicename)));

    fclose(mtfp);
}

/*
 * @Description: get diskstats from the file "/proc/diskstats"
 * @IN:         device: device name of the data directory
 * @Return:     block io info
 * @See also:
 */
void WLMmonitor_read_diskstats_stat(const char* device, struct blkio_info* cur_blkio_info)
{
    FILE* iofp = NULL;
    char line[1024] = {0};
    char dev_name[MAX_DEVICE_DIR] = {0};
    int i;
    uint64 tot_ticks;
    uint64 rd_ios, rd_merges_or_rd_sec, rd_ticks_or_wr_sec, wr_ios;
    uint64 wr_merges, rd_sec_or_wr_ios, wr_sec;
    uint32 major, minor;
    uint32 rq_ticks, ios_pgr, wr_ticks;

    if ((iofp = fopen(FILE_DISKSTAT, "r")) == NULL) {
        ereport(LOG, (errmsg("failed to open file %s", FILE_DISKSTAT)));
        return;
    }

    while (fgets(line, 1024, iofp) != NULL) {
        i = sscanf_s(line,
            "%u %u %s %lu %lu %lu %lu %lu %lu %lu %u %u %lu %u",
            &major,
            &minor,
            dev_name,
            MAX_DEVICE_DIR - 1,
            &rd_ios,
            &rd_merges_or_rd_sec,
            &rd_sec_or_wr_ios,
            &rd_ticks_or_wr_sec,
            &wr_ios,
            &wr_merges,
            &wr_sec,
            &wr_ticks,
            &ios_pgr,
            &tot_ticks,
            &rq_ticks);

        if (i == 14) {
            if (strcmp(dev_name, device) != 0) {
                continue;
            }
            cur_blkio_info->rd_ios = rd_ios;
            cur_blkio_info->wr_ios = wr_ios;
            cur_blkio_info->tot_ticks = tot_ticks;
        } else {
            fprintf(stderr,
                "%s:%d failed on calling "
                "security function.\n",
                __FILE__,
                __LINE__);
            fclose(iofp);
            return;
        }
    }

    if (*dev_name == '\0') {
        ereport(LOG, (errmsg("cannot get the information of the file %s.\n", FILE_DISKSTAT)));
    }

    fclose(iofp);
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
void WLMmonitor_read_stat_cpu(bool cpu_count_down)
{
    errno_t rc;
    FILE* cpufp = NULL;
    char line[8192];
    const int curr = 1;
    uint64 cpu_user = 0;
    uint64 cpu_nice = 0;
    uint64 cpu_sys = 0;
    uint64 cpu_idle = 0;
    uint64 cpu_iowait = 0;
    uint64 cpu_hardirq = 0;
    uint64 cpu_softirq = 0;
    uint64 cpu_steal = 0;
    uint64 cpu_guest = 0;
    uint64 cpu_guest_nice = 0;

    if ((cpufp = fopen(FILE_CPUSTAT, "r")) == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", FILE_CPUSTAT)));
        return;
    }
    while (fgets(line, sizeof(line), cpufp) != NULL) {
        /* fisrt line -- total cpu */
        if (strncmp(line, "cpu ", 4) == 0) {
            /* for non smp iostat or cpustat, get the total jiffies */
            if (1 == g_instance.wlm_cxt->io_context.cpu_nr || !cpu_count_down) {
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
                /* There are some hosts that we can not get ten CPU parameters, such as kernel 2.6. */
                if (rc < 9) {
                    ereport(WARNING, (errmsg("can not get whole cpu informations, we only get %d.\n", rc)));
                }

                g_instance.wlm_cxt->io_context.cputime_user[curr] = cpu_user;
                g_instance.wlm_cxt->io_context.uptime[curr] =
                    cpu_user + cpu_nice + cpu_sys + cpu_idle + cpu_iowait + cpu_hardirq + cpu_steal + cpu_softirq;

                /* there is no 'cpu0' for non smp, just break the loop */
                if (1 == g_instance.wlm_cxt->io_context.cpu_nr) {
                    g_instance.wlm_cxt->io_context.uptime0[curr] = g_instance.wlm_cxt->io_context.uptime[curr];
                    break;
                }
            }
        }

        /* for smp, cpu0 is enough for iostat */
        if (g_instance.wlm_cxt->io_context.cpu_nr > 1 && strncmp(line, "cpu0", 4) == 0) {
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
            if (rc < 9) {
                ereport(WARNING, (errmsg("can not get whole cpu informations, we only get %d.\n", rc)));
            }

            g_instance.wlm_cxt->io_context.uptime0[curr] =
                cpu_user + cpu_nice + cpu_sys + cpu_idle + cpu_iowait + cpu_hardirq + cpu_steal + cpu_softirq;
            break;
        }
    }
    fclose(cpufp);
    return;
}

/*
 * @Description: get interval between the two values
 * @IN:         oldvalue: the previous value
 * @IN:         newvalue: the current value
 * @Return:     void
 * @See also:
 */
static uint64 WLMmonitor_get_interval(uint64 oldvalue, uint64 newvalue)
{
    uint64 itv = 0;

    itv = newvalue - oldvalue;

    // interval cannot be 0
    if (!itv) {
        itv = 1;
    }

    return itv;
}

/*
 * @Description: get total cpustat
 * @Return:     void
 * @See also:
 */
static void WLMmonitor_cpustat()
{
    const int curr = 1;

    // get interval of the two jiffies
    uint64 itv = WLMmonitor_get_interval(
        g_instance.wlm_cxt->io_context.uptime[!curr], g_instance.wlm_cxt->io_context.uptime[curr]);
    g_instance.wlm_cxt->io_context.uptime[!curr] = g_instance.wlm_cxt->io_context.uptime[curr];

    USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_EXCLUSIVE);

    // get cpu util by percentage
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util =
        WLMmonitor_get_average_value(g_instance.wlm_cxt->io_context.cputime_user[!curr],
            g_instance.wlm_cxt->io_context.cputime_user[curr],
            itv,
            FULL_PERCENT);
    g_instance.wlm_cxt->io_context.cputime_user[!curr] = g_instance.wlm_cxt->io_context.cputime_user[curr];

    if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util) {
        ereport(DEBUG1, (errmsg("cpustat: %.2f", g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util)));
    }

    // every CPUSTAT_WAKE_TIME, cpustat is checked
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util_count = CPUSTAT_WAKE_TIME;
}

/*
 * @Description: get iostat
 * @Return:     void
 * @See also:
 */
static void WLMmonitor_disk_iostat()
{
    errno_t rc;
    int below = UTIL_LOW;
    int i = 0;
    int total_util = 0;
    const int curr = 1;

    // get interval of total jiffies of cpu0
    uint64 itv = WLMmonitor_get_interval(
        g_instance.wlm_cxt->io_context.uptime0[!curr], g_instance.wlm_cxt->io_context.uptime0[curr]);
    g_instance.wlm_cxt->io_context.uptime0[!curr] = g_instance.wlm_cxt->io_context.uptime0[curr];

    USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_EXCLUSIVE);

    // get block io info for the specified device
    WLMmonitor_read_diskstats_stat(
        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device, &g_instance.wlm_cxt->io_context.new_blkio);

    // get iostat
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.rs =
        WLMmonitor_get_average_value(g_instance.wlm_cxt->io_context.old_blkio.rd_ios,
            g_instance.wlm_cxt->io_context.new_blkio.rd_ios,
            itv,
            g_instance.wlm_cxt->io_context.hz);
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws =
        WLMmonitor_get_average_value(g_instance.wlm_cxt->io_context.old_blkio.wr_ios,
            g_instance.wlm_cxt->io_context.new_blkio.wr_ios,
            itv,
            g_instance.wlm_cxt->io_context.hz);
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util =
        WLMmonitor_get_average_value(g_instance.wlm_cxt->io_context.old_blkio.tot_ticks,
            g_instance.wlm_cxt->io_context.new_blkio.tot_ticks,
            itv,
            g_instance.wlm_cxt->io_context.hz) /
        10.0; /* tot_ticks unit: ms, itv/HZ unit:s, util is percentage, unit: %. */

    rc = memcpy_s(&g_instance.wlm_cxt->io_context.old_blkio,
        sizeof(blkio_info),
        &g_instance.wlm_cxt->io_context.new_blkio,
        sizeof(blkio_info));
    securec_check(rc, "\0", "\0");

    // get write percentage of the total iops
    if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util) {
        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.w_ratio =
            (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.rs +
                g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws)
                ? (FULL_PERCENT * g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws /
                      (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.rs +
                          g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws))
                : FULL_PERCENT;
    }

    /*
     * util lower than UTIL_LOW_THRESHOLD, util is labeled to be 0,
     * util higher than UTIL_HIGH_THRESHOLD, util is labeled to be 2,
     * between the two threshold, util is labeled to be 1
     */
    if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util > UTIL_LOW_THRESHOLD &&
        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util < UTIL_HIGH_THRESHOLD) {
        below = UTIL_MEDIUM;
    } else if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util > UTIL_HIGH_THRESHOLD) {
        below = UTIL_HIGH;
    }

    ereport(DEBUG1,
        (errmsg("iostat: %.2f|%.2f|%.2f|%.2f",
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.rs,
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws,
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.w_ratio,
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util)));

    UPDATETABLE(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device_util_tbl, below, UTIL_COUNT_LEN);

    /*
     * get total util for the past UTIL_COUNT_LEN seconds
     * check whether utils for the past UTIL_count_LEN seconds
     * have been all higher than UTIL_HIGH_THRESHOLD, or all below UTIL_LOW_THRESHOLD
     */
    for (i = 0; i < UTIL_COUNT_LEN; i++) {
        total_util += g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device_util_tbl[i];
        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.total_tbl_util = total_util;
    }

    ereport(DEBUG1, (errmsg("total util is %d", total_util)));

    /*
     * util table is reset when util is all higher than UTIL_HIGH_THRESHOLD,
     */
    if (total_util == UTIL_HIGH * UTIL_COUNT_LEN) {
        // reset util table
        rc = memset_s(
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device_util_tbl, UTIL_COUNT_LEN, 0, UTIL_COUNT_LEN);
        securec_check(rc, "\0", "\0");
    }

    // iostat is updated every second
    g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.tick_count = IOLIMIT_WAKE_TIME;
}

/******************************************************************************
        main thread function
*******************************************************************************/
/* initialize the global variables */
void WLMIOContextInitialize(void)
{
    pthread_mutex_init(&g_instance.wlm_cxt->io_context.waiting_list_mutex, NULL);
    g_instance.wlm_cxt->io_context.cpu_nr = gsutil_get_cpu_count();
}

/*
 * @Description: wlm monitor thread initialize
 * @Return: void
 * @See also:
 */
static void WLMmonitor_init()
{
    /* initialize the io context information */
    WLMIOContextInitialize();

    errno_t rc;

    rc = memset_s(
        &g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat, sizeof(WLMmonitor_iostat), 0, sizeof(WLMmonitor_iostat));
    securec_check(rc, "\0", "\0");

    WLMmonitor_get_HZ();

    char devicename[MAX_DEVICE_DIR] = {0};

    /* get device name of the mount directory */
    WLMmonitor_dn_get_disk(t_thrd.proc_cxt.DataDir, devicename);

    if (*devicename) {
        g_instance.wlm_cxt->io_context.device_init = true;
        rc = strncpy_s(
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device, MAX_DEVICE_DIR, devicename, MAX_DEVICE_DIR - 1);
        securec_check(rc, "\0", "\0");
    } else {
        ereport(LOG,
            (errmsg("cannot find the mount device of data directory %s, io_priority is not available.",
                t_thrd.proc_cxt.DataDir)));
    }
}

/*
 * @Description: get mean value of the values listed in the 'table'
 * @IN         : table : table of the values
 * @IN         : num   : number of the values of the table
 * @Return     : mean value of the table
 * @See also:
 */
static int WLMmonitor_get_mean_iops(const int* table, int num)
{
    int i = 0;
    int mean_iops = 0;

    for (i = 0; i < num; i++) {
        mean_iops += table[i];
    }

    mean_iops /= num;

    return mean_iops;
}

/*
 * @Description: get IO collect info for queries and users
 * @IN         : ioinfo: IO info structure, to be updated
 * @Return     :
 * @See also:
 */
static void WLMmonitor_update_iocollect_info(WLMIoGeninfo* ioinfo)
{
    // get current iops
    ioinfo->curr_iops = ioinfo->io_count_persec;

    // get peak iops
    if (ioinfo->peak_iops < ioinfo->curr_iops) {
        ioinfo->peak_iops = ioinfo->curr_iops;
    }

    // io_count_persec reset to 0 every second
    ioinfo->io_count_persec = 0;

    // collect info is updated every second
    ioinfo->tick_count_down = IOLIMIT_WAKE_TIME;
}

/*
 * @Description: get current iops_limits for user or queries
 *               mainly consider the case specifying io_priority
 * @IN         : io_priority: io_priority user set
 * @IN         : ioinfo: ioinfo to be updated
 * @Return     :
 * @See also:
 */
static void WLMmonitor_assign_iops_limits(int io_priority, int iostate, WLMIoGeninfo* ioinfo)
{
    int mean_iops = 0;

    UPDATETABLE(ioinfo->hist_iops_tbl, ioinfo->io_count, PAST_TIME_COUNT);

    // reset io_count
    ioinfo->io_count = 0;

    USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_SHARED);

    /*
     * when write proportion has been low enough,
     * IO control for load data is canceled.
     * If it has been lasted for 3 seconds
     * that util is lower than LOW_THRESHOLD,
     * IO control is canceled.
     */
    if (!io_priority ||
        (iostate == IOSTATE_WRITE &&
            (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.w_ratio < W_RATIO_THRESHOLD)) ||
        (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.total_tbl_util == UTIL_LOW * UTIL_COUNT_LEN)) {
        if (io_priority) {
            ereport(DEBUG1,
                (errmsg("curr_iops_limit is set to 0 because write ratio is too low,"
                        "or total util is 0")));
        }

        ioinfo->curr_iops_limit = 0;
        return;
    }

    /* get mean_iops for the past several seconds */
    mean_iops = WLMmonitor_get_mean_iops(ioinfo->hist_iops_tbl, PAST_TIME_COUNT);

    /*
     * entrance of io_priority taking effect
     * ---- current util higher than UTIL_HIGH_THRESHHOLD
     * if util has been lasted for UTIL_COUNT_LEN seconds,
     * then IO need be further controled
     * on the basis of the current mean iops
     */
    if ((!ioinfo->curr_iops_limit && g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util >= UTIL_HIGH_THRESHOLD) ||
        (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.total_tbl_util == UTIL_HIGH * UTIL_COUNT_LEN)) {
        ioinfo->curr_iops_limit = mean_iops * io_priority / FULL_PERCENT + 1;
    }

    /*
     * iops cannot be lower than IOPS_LOW_LIMIT
     * might need make a distinction between row and cstore insert
     */
    if (ioinfo->curr_iops_limit && (ioinfo->curr_iops_limit < IOPS_LOW_LIMIT)) {
        ioinfo->curr_iops_limit = IOPS_LOW_LIMIT;
    }

    ereport(DEBUG1,
        (errmsg("query curr_iops_limit is set to %d, for io_priority %d", ioinfo->curr_iops_limit, io_priority)));
    /*
     * only when curr_iops_limit is not 0
     * need wait for prio_count_down times.
     */
    if (io_priority == IOPRIORITY_LOW) {
        ioinfo->prio_count_down = IOPRIORITY_LOW_WAKE_TIME;
    } else if (io_priority == IOPRIORITY_MEDIUM) {
        ioinfo->prio_count_down = IOPRIORITY_MEDIUM_WAKE_TIME;
    } else if (io_priority == IOPRIORITY_HIGH) {
        ioinfo->prio_count_down = IOPRIORITY_HIGH_WAKE_TIME;
    }

    return;
}

/*
 * @Description   : get iostat collect info and reset the iops_limits for user and queries
 * @IN io_priority: user set io_priority
 * @IN iostate    : IOSTATE_WRITE or not
 * @IN ioinfo     : general info of iostat for users and queries
 * @OUT ioinfo    : the updated ioinfo
 */
static void WLMmonitor_update_ioinfo_internal(int io_priority, int iostate, WLMIoGeninfo* ioinfo, int type)
{
    if (type == FETCH_IOSTAT) {
        if (ioinfo->tick_count_down) {
            ioinfo->tick_count_down--;
        }

        /* update io collect info every second */
        if (!ioinfo->tick_count_down) {
            WLMmonitor_update_iocollect_info(ioinfo);
        }

        if (ioinfo->curr_iops) {
            ereport(DEBUG1, (errmsg("current iops is %d, for priority %d", ioinfo->curr_iops, io_priority)));
        }
    }

    if (type == RESET_IOSTAT) {
        if (ioinfo->prio_count_down) {
            ioinfo->prio_count_down--;
        }

        /*
         * assign iops_limit for io_priority
         * --might update every second (curr_iops_limit is 0)
         * --might update every time interval (curr_iops_limit is non zero)
         * for example, if io_priority is 'Low',
         * and curr_iops_limit is not 0 (io_priority takes effect)
         * then, IO requests are collected and enqueued
         * for 3s, during which, no need update ioinfo for the scheduling process,
         * while if curr_iops_limit is 0,
         * (IO utilization might be too low
         * or write proportion is too low to continue IO control)
         * then IO utilization is checked every second to
         * assign a new curr_iops_limit for io_priority.
         */
        if (!ioinfo->prio_count_down) {
            WLMmonitor_assign_iops_limits(io_priority, iostate, ioinfo);
        }
    }

    return;
}

/*
 * @Description   : reset user ioinfo
 * @IN            : void
 * @RETURN        : void
 */
static void WLMmonitor_reset_user_ioinfo(int type)
{
    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    UserData* userdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
        // super user doesn't need collect IO info or control IO
        if (userdata->userid == BOOTSTRAP_SUPERUSERID) {
            continue;
        }

        // collect ioinfo and update curr_iops_limits calculted from io_priority
        if (userdata->respool != NULL) {
            WLMmonitor_update_ioinfo_internal(userdata->respool->io_priority, IOSTATE_NONE, &userdata->ioinfo, type);
        }
    }
}

/*
 * @Description   : reset session ioinfo
 * @IN            : void
 * @RETURN        : void
 */
static void WLMmonitor_reset_session_iostat(int type)
{
    long intval_secs = 0;
    int intval_microsecs = 0;

    if (g_instance.wlm_cxt->io_context.cur_timestamp) {
        g_instance.wlm_cxt->io_context.prev_timestamp = g_instance.wlm_cxt->io_context.cur_timestamp;
    } else {
        g_instance.wlm_cxt->io_context.prev_timestamp = GetCurrentTimestamp();
    }
    g_instance.wlm_cxt->io_context.cur_timestamp = GetCurrentTimestamp();

    /* first time enter this function, prev_timestamp = cur_timestamp */
    TimestampDifference(g_instance.wlm_cxt->io_context.prev_timestamp,
        g_instance.wlm_cxt->io_context.cur_timestamp,
        &intval_secs,
        &intval_microsecs);

    if (intval_microsecs > USECS_PER_SEC / 2) {
        intval_secs++;
    }

    HASH_SEQ_STATUS* hash_seq = &g_instance.wlm_cxt->stat_manager.iostat_info_seq;

    for (size_t bucketId = 0; bucketId < NUM_IO_STAT_PARTITIONS; ++bucketId) {
        int lockId = GetIoStatLockId(bucketId);
        (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);

        /* no record to update */
        if ((hash_get_num_entries(g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucketId].hashTable)) <= 0) {
            LWLockRelease(GetMainLWLockByIndex(lockId));
            continue;
        }

        WLMDNodeIOInfo* info = NULL;

        // scan collectinfo hashtable
        hash_seq_init(hash_seq, g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucketId].hashTable);

        /* get all info to update with data node info */
        while ((info = (WLMDNodeIOInfo*)hash_seq_search(hash_seq)) != NULL) {
            WLMmonitor_update_ioinfo_internal(info->io_priority, info->io_state, &info->io_geninfo, type);
            ereport(DEBUG2, (errmodule(MOD_WLM),
                errmsg("[IOSTAT] queryId is: %lu, current iops is: %d",
                info->qid.queryId, info->io_geninfo.curr_iops)));
        }
        LWLockRelease(GetMainLWLockByIndex(lockId));
    }
}

/*
 * @Description   : wlm main worker function
 * @IN            : void
 * @RETURN       : void
 */
static void WLMmonitor_worker(int type)
{
    if (type == FETCH_IOSTAT) {
        // iostat is updated per IOLIMIT_WAKE_TIME * WLM_AUTOWAKE_INTERVAL seconds
        if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.tick_count) {
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.tick_count--;
        }

        // cpu_util is updated per CPUSTAT_WAKE_TIME * WLM_AUTOWAKE_INTERVAL seconds
        if (g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util_count) {
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util_count--;
        }

        // get disk iostat and cpustat --util, r/s, w/s %user
        if (!g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.tick_count) {
            /*
             * read /proc/stat
             * -- if cpu_util_count is 0, the total cpu jiffies are needed,
             * else, for smp, read cpu0 is enough
             */
            WLMmonitor_read_stat_cpu(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util_count);

            // might not get device name
            if (g_instance.wlm_cxt->io_context.device_init) {
                WLMmonitor_disk_iostat();
            }

            // get cpustat
            if (!g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util_count) {
                WLMmonitor_cpustat();
            }
        }
    }

    // get IO statistics for queries and reset it
    WLMmonitor_reset_session_iostat(type);

    // get IO statistics for users and reset it
    WLMmonitor_reset_user_ioinfo(type);
}

/*
 * @Description: Dequeue the list
 * @IN :void
 * @RETURN: void
 * @See also:
 */
void RequestSchedulingFromQueue(void)
{
    IORequestEntry* rqust = NULL;
    ListCell* prev = NULL;
    ListCell* rqustinfo = NULL;

    WLMContextLock rq_lock(&g_instance.wlm_cxt->io_context.waiting_list_mutex);

    rq_lock.Lock();

    if (list_length(g_instance.wlm_cxt->io_context.waiting_list) > 0) {
        rqustinfo = list_head(g_instance.wlm_cxt->io_context.waiting_list);
    }

    while (rqustinfo != NULL) {
        rqust = (IORequestEntry*)lfirst(rqustinfo);

        // might has been freed
        if (rqust != NULL) {
            ereport(DEBUG1, (errmsg("IO request count_down is %d", rqust->count_down)));

            // might not dequeue this time
            if (rqust->count_down) {
                rqust->count_down--;
            }

            // need wait for count_down times
            if (rqust->count_down) {
                prev = rqustinfo;
                rqustinfo = lnext(rqustinfo);
                continue;
            }

            (void)pthread_cond_signal(&(rqust->io_proceed_cond));
        }

        if (prev != NULL) {
            g_instance.wlm_cxt->io_context.waiting_list =
                list_delete_cell(g_instance.wlm_cxt->io_context.waiting_list, rqustinfo, prev);
            rqustinfo = lnext(prev);
        } else {
            rqustinfo = lnext(rqustinfo);
            g_instance.wlm_cxt->io_context.waiting_list =
                list_delete_first(g_instance.wlm_cxt->io_context.waiting_list);
        }
    }

    rq_lock.UnLock();
}

/*
 * @Description: wlm monitor main loop
 * @IN :void
 * @RETURN: void
 * @See also:
 */
static void WLMmonitor_MainLoop(void)
{
    TimestampTz last_monitor_time = GetCurrentTimestamp();
    TimestampTz next_timeout_time = 0;
    int verify_count = 0;

    /* instance statistics parameters */
    TimestampTz instance_monitor_last_time = GetCurrentTimestamp();
    TimestampTz instance_persistent_last_time = GetCurrentTimestamp();
    TimestampTz instance_cleanup_last_time = GetCurrentTimestamp();

    while (PostmasterIsAlive()) {
        if (t_thrd.wlm_cxt.wlm_init_done && !t_thrd.wlm_cxt.wlm_xact_start) {
            StartTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = true;
        }

        CHECK_FOR_INTERRUPTS();

        if (t_thrd.wlm_cxt.wlm_got_sigterm) {
            t_thrd.wlm_cxt.wlm_got_sigterm = false;

            break;
        }

        if (t_thrd.wlm_cxt.wlm_got_sighup) {
            t_thrd.wlm_cxt.wlm_got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }

        ResetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

        /*
         * Sleep until a signal is received, or until a poll is forced by
         * PGARCH_AUTOWAKE_INTERVAL having passed since last_copy_time, or
         * until postmaster dies.
         */
        if (IS_PGXC_DATANODE) {
            int timeout;  // timeout in milliseconds

            // reset wlmmonitor parameters
            WLMmonitor_worker(RESET_IOSTAT);

            TimestampTz curtime = GetCurrentTimestamp();

            timeout = WLM_AUTOWAKE_INTERVAL * USECS_PER_SEC - (curtime - last_monitor_time);

            if (timeout > 0) {
                WaitLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                    timeout / 1000);
            }

            last_monitor_time = GetCurrentTimestamp();

            // reset wlmmonitor parameters
            WLMmonitor_worker(FETCH_IOSTAT);

            // dequeue the IO requests
            RequestSchedulingFromQueue();

            TimestampTz time_now = GetCurrentTimestamp();
            if (time_now > next_timeout_time) {
                /* Update cgroup cpu info */
                WLMUpdateCgroupCPUInfo();
                next_timeout_time = TimestampTzPlusMilliseconds(time_now, 10 * MSECS_PER_SEC);
            }
        }

        if (IS_PGXC_COORDINATOR) {
            // update iostat for each query
            WLMCleanUpIoInfo();

            if (u_sess->attr.attr_resource.enable_resource_track) {
                WLMUpdateSessionIops();
            }

            /* do dynamic workload records synchronazation per 1 mins */
            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                if (is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName)) {
                    dywlm_server_check_resource_pool();
                }
            }
            /* dynamic workload also need wake up simple query */
            WLMCheckResourcePool();

            /* verify global list for parallel control */
            if (verify_count >= 5) {
                verify_count = 0;

                if (g_instance.wlm_cxt->dynamic_workload_inited) {
                    dywlm_client_verify_register();
                } else if (u_sess->attr.attr_resource.enable_verify_statements) {
                    // check all node group later
                    WLMVerifyGlobalParallelControl(&g_instance.wlm_cxt->MyDefaultNodeGroup.parctl);
                }
            } else {
                ++verify_count;
            }

            /* Update cgroup cpu info */
            WLMUpdateCgroupCPUInfo();

            pg_usleep(10 * USECS_PER_SEC);
        }

        /* collect + persistent instance statistics */
        if (u_sess->attr.attr_resource.use_workload_manager &&
            u_sess->attr.attr_resource.enable_instance_metric_persistent) {
            TimestampTz now = GetCurrentTimestamp();
            if (now > instance_persistent_last_time +
                          g_instance.wlm_cxt->instance_manager.persistence_interval * USECS_PER_SEC) {
                /* persistent instance statistics */
                WLMPersistentInstanceStat();
                instance_persistent_last_time = now;
            }

            /* reduce mis-fire on CNs */
            if (IS_PGXC_COORDINATOR) {
                g_instance.wlm_cxt->instance_manager.last_timestamp =
                    g_instance.wlm_cxt->instance_manager.recent_timestamp;
                g_instance.wlm_cxt->instance_manager.recent_timestamp = now;
                /* collect instance statistics */
                WLMCollectInstanceStat();
            } else {
                if (now > instance_monitor_last_time +
                              g_instance.wlm_cxt->instance_manager.collect_interval * USECS_PER_SEC) {
                    g_instance.wlm_cxt->instance_manager.last_timestamp =
                        g_instance.wlm_cxt->instance_manager.recent_timestamp;
                    g_instance.wlm_cxt->instance_manager.recent_timestamp = now;
                    /* collect instance statistics */
                    WLMCollectInstanceStat();
                    instance_monitor_last_time = now;
                }
            }

            /* cleanup history instance statistics */
            if (now >
                instance_cleanup_last_time + g_instance.wlm_cxt->instance_manager.cleanup_interval * USECS_PER_SEC) {
                /* cleanup history instance statistics */
                WLMCleanupHistoryInstanceStat();
                instance_cleanup_last_time = now;
            }
        }

        if (t_thrd.wlm_cxt.wlm_xact_start) {
            CommitTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = false;
        }
    }
}

/*
 * Description: Receive SIGTERM and time to die.
 *
 * Parameters:
 *  @in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void WLMmonitorSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.wlm_cxt.wlm_got_sigterm = true;

    SetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    errno = save_errno;
}

/*
 * @Description: main function of the thread
 * @IN :void
 * @RETURN: void
 * @See also:
 */
NON_EXEC_STATIC void WLMmonitorMain(void)
{
    sigjmp_buf local_sigjmp_buf;

    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "WLMmonitor";

    if (u_sess->proc_cxt.MyProcPort->remote_host) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }

    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    /* Identify myself via ps */
    init_ps_display("wlm monitor worker process", "", "", "");

    SetProcessingMode(InitProcessing);

    t_thrd.bootstrap_cxt.MyAuxProcType = WLMMonitorWorkerProcess;

    errno_t rc = memset_s(
        &u_sess->wlm_cxt->wlm_params, sizeof(u_sess->wlm_cxt->wlm_params), 0, sizeof(u_sess->wlm_cxt->wlm_params));
    securec_check(rc, "\0", "\0");

    u_sess->wlm_cxt->wlm_params.qid.queryId = (ENABLE_THREAD_POOL ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);

    rc = snprintf_s(t_thrd.wlm_cxt.collect_info->cginfo.cgroup,
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup),
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup) - 1,
        "%s",
        GSCGROUP_DEFAULT_BACKEND);
    securec_check_ss(rc, "\0", "\0");

    t_thrd.wlm_cxt.collect_info->sdetail.statement = "WLM monitor update and verify local info";

    u_sess->attr.attr_common.application_name = pstrdup("WorkloadMonitor");

    t_thrd.wlm_cxt.parctl_state.special = 1;

    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        /*
         * If we exit, first try and clean connections and send to
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the openGauss thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when openGauss thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }

    t_thrd.wlm_cxt.wlm_init_done = false;
    t_thrd.wlm_cxt.wlm_xact_start = false;

    (void)gspqsignal(SIGHUP, WLMSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, WLMmonitorSigTermHandler);
    (void)gspqsignal(SIGQUIT, quickdie);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* Early initialization */
    BaseInit();

    WLMInitPostgres();

    SetProcessingMode(NormalProcessing);

    /* initialize latch used in main loop */
    InitLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    if (t_thrd.wlm_cxt.wlm_got_sigterm) {
        t_thrd.wlm_cxt.wlm_got_sigterm = false;

        proc_exit(0);
    }

    int curTryCounter;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, we must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * Forget any pending QueryCancel request, since we're returning to
         * the idle loop anyway, and cancel the statement timer if running.
         */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        if (IS_PGXC_COORDINATOR && hash_get_seq_num() > 0) {
            release_all_seq_scan();
        }

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        if (t_thrd.wlm_cxt.wlm_xact_start) {
            AbortCurrentTransaction();
            t_thrd.wlm_cxt.wlm_xact_start = false;
            t_thrd.wlm_cxt.wlm_init_done = false;
        }
        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         *   Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        return;
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    if (IS_PGXC_COORDINATOR) {
        /* init transaction to execute query */
        WLMInitTransaction(&t_thrd.wlm_cxt.wlm_init_done);

        pg_usleep(10 * USECS_PER_SEC);

        /* initialize current pool handles, it's also only once */
        exec_init_poolhandles();

        /*
         * If the PGXC_NODE system table is not prepared, the number of CN / DN
         * can not be obtained, if we can not get the number of DN or CN, that
         * will make the collection module can not complete the task, so the
         * thread need restart
         */
        if (IS_PGXC_COORDINATOR && (u_sess->pgxc_cxt.NumDataNodes == 0 || u_sess->pgxc_cxt.NumCoords == 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }
    }

    /* wait until g_statManager is available */
    while (!WLMIsInfoInit()) {
        if (t_thrd.wlm_cxt.wlm_got_sigterm) {
            ereport(LOG, (errmsg("WLMmonitor thread exit!")));
            t_thrd.wlm_cxt.wlm_got_sigterm = false;
            proc_exit(0);
        }
        pg_usleep(1000);
    }

    /*
     * Identify myself via ps
     */
    ereport(LOG, (errmsg("WLMmonitor thread is starting up.")));

    // initialize thread
    WLMmonitor_init();

    // main loop of the thread
    WLMmonitor_MainLoop();

    /* If transaction has started, we must commit it here. */
    if (t_thrd.wlm_cxt.wlm_xact_start) {
        CommitTransactionCommand();
        t_thrd.wlm_cxt.wlm_xact_start = false;
    }

    t_thrd.wlm_cxt.wlm_init_done = false;

    proc_exit(0);
}

/**************************************************************************************
    main thread function for arbiter - - CCN/CN collect info from DNs for arbitration
***************************************************************************************/
/*
 * Description: Receive SIGTERM and time to die.
 *
 * Parameters:
 *  @in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void WLMarbiterSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.wlm_cxt.wlm_got_sigterm = true;

    t_thrd.int_cxt.InterruptByCN = true;
    
    /*
     * send SIGUSER2 to pooler
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;
        t_thrd.int_cxt.PoolValidateCancelPending = true;

        /*  Set flag to handle SIGUSER2 and SIGTERM during connection */
        if (t_thrd.proc_cxt.pooler_connection_inprogress) {
            g_pq_interrupt_happened = true;
        }
    }

    SetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    errno = save_errno;
}

/*
 * @Description: wlm arbiter main loop
 * @IN :void
 * @RETURN: void
 * @See also:
 */
static void WLMarbiter_MainLoop(void)
{
    TimestampTz next_timeout_time = 0;

    while (PostmasterIsAlive()) {
        if (t_thrd.wlm_cxt.wlm_init_done && !t_thrd.wlm_cxt.wlm_xact_start) {
            StartTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = true;
        }

        CHECK_FOR_INTERRUPTS();

        if (t_thrd.wlm_cxt.wlm_got_sigterm) {
            t_thrd.wlm_cxt.wlm_got_sigterm = false;

            break;
        }

        if (t_thrd.wlm_cxt.wlm_got_sighup) {
            t_thrd.wlm_cxt.wlm_got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }

        ResetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

        /*
         * Sleep until a signal is received, or until a poll is forced by
         * PGARCH_AUTOWAKE_INTERVAL having passed since last_copy_time, or
         * until postmaster dies.
         */
        if (IS_PGXC_COORDINATOR && g_instance.wlm_cxt->dynamic_workload_inited &&
            is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName)) {
            /* do dynamic workload records synchronazation per 1 mins */
            TimestampTz curtime = GetCurrentTimestamp();
            if (curtime > next_timeout_time) {
                dywlm_server_sync_records();
                next_timeout_time = TimestampTzPlusMilliseconds(curtime, MSECS_PER_MIN);
            }
        }

        ereport(DEBUG3, (errmsg("> > > > > > WLM Arbiter: CN/DN log")));
        pg_usleep(10 * USECS_PER_SEC);

        /* Work on CNs and CCN, not only CCN */
        if (g_instance.wlm_cxt->stat_manager.stop == 0) {
            dywlm_server_collector();
        }

        if (t_thrd.wlm_cxt.wlm_xact_start) {
            CommitTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = false;
        }
    }
}

/*
 * @Description: main function of the thread arbiter
 * @IN :void
 * @RETURN: void
 * @See also:
 */
NON_EXEC_STATIC void WLMarbiterMain(void)
{
    sigjmp_buf local_sigjmp_buf;

    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "WLMarbiter";

    if (u_sess->proc_cxt.MyProcPort->remote_host != NULL) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }

    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    /* Identify myself via ps */
    init_ps_display("wlm arbiter worker process", "", "", "");

    SetProcessingMode(InitProcessing);

    t_thrd.bootstrap_cxt.MyAuxProcType = WLMArbiterWorkerProcess;

    errno_t rc = memset_s(
        &u_sess->wlm_cxt->wlm_params, sizeof(u_sess->wlm_cxt->wlm_params), 0, sizeof(u_sess->wlm_cxt->wlm_params));
    securec_check(rc, "\0", "\0");

    u_sess->wlm_cxt->wlm_params.qid.queryId = (ENABLE_THREAD_POOL ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);

    rc = snprintf_s(t_thrd.wlm_cxt.collect_info->cginfo.cgroup,
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup),
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup) - 1,
        "%s",
        GSCGROUP_DEFAULT_BACKEND);
    securec_check_ss(rc, "\0", "\0");

    t_thrd.wlm_cxt.collect_info->sdetail.statement = "WLM arbiter sync info by CCN and CNs";

    u_sess->attr.attr_common.application_name = pstrdup("WLMArbiter");

    t_thrd.wlm_cxt.parctl_state.special = 1;

    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        /*
         * If we exit, first try and clean connections and send to
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the openGauss thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when openGauss thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }

    t_thrd.wlm_cxt.wlm_init_done = false;
    t_thrd.wlm_cxt.wlm_xact_start = false;

    (void)gspqsignal(SIGHUP, WLMSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, WLMarbiterSigTermHandler);
    (void)gspqsignal(SIGQUIT, quickdie);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* Early initialization */
    BaseInit();
    WLMInitPostgres();

    SetProcessingMode(NormalProcessing);

    /* initialize latch used in main loop */
    InitLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    if (t_thrd.wlm_cxt.wlm_got_sigterm) {
        t_thrd.wlm_cxt.wlm_got_sigterm = false;

        proc_exit(0);
    }

    int curTryCounter;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, we must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * Forget any pending QueryCancel request, since we're returning to
         * the idle loop anyway, and cancel the statement timer if running.
         */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        if (IS_PGXC_COORDINATOR && hash_get_seq_num() > 0) {
            release_all_seq_scan();
        }

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        if (t_thrd.wlm_cxt.wlm_xact_start) {
            AbortCurrentTransaction();
            t_thrd.wlm_cxt.wlm_xact_start = false;
            t_thrd.wlm_cxt.wlm_init_done = false;
        }
        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         *   Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        return;
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    if (IS_PGXC_COORDINATOR) {
        /* init transaction to execute query */
        WLMInitTransaction(&t_thrd.wlm_cxt.wlm_init_done);

        pg_usleep(10 * USECS_PER_SEC);

        /* initialize current pool handles, it's also only once */
        exec_init_poolhandles();

        /*
         * If the PGXC_NODE system table is not prepared, the number of CN / DN
         * can not be obtained, if we can not get the number of DN or CN, that
         * will make the collection module can not complete the task, so the
         * thread need restart
         */
        if (IS_PGXC_COORDINATOR && (u_sess->pgxc_cxt.NumDataNodes == 0 || u_sess->pgxc_cxt.NumCoords == 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }
    }

    /* wait until g_statManager is available */
    while (!WLMIsInfoInit()) {
        if (t_thrd.wlm_cxt.wlm_got_sigterm) {
            ereport(LOG, (errmsg("WLMarbiter thread exit!")));
            t_thrd.wlm_cxt.wlm_got_sigterm = false;
            proc_exit(0);
        }
        pg_usleep(1000);
    }

    /*
     * Identify myself via ps
     */
    ereport(LOG, (errmsg("WLMarbiter thread is starting up.")));

    // main loop of the thread
    WLMarbiter_MainLoop();

    /* If transaction has started, we must commit it here. */
    if (t_thrd.wlm_cxt.wlm_xact_start) {
        CommitTransactionCommand();
        t_thrd.wlm_cxt.wlm_xact_start = false;
    }

    t_thrd.wlm_cxt.wlm_init_done = false;

    proc_exit(0);
}

/**************************************************************
 * IO scheduling
 *************************************************************/
/*
 * @Description: enqueue the requests
 * @IN :rqust : request structure to be enqueued
 * @RETURN: void
 * @See also:
 */
void RequestEnqueue(IORequestEntry* rqust)
{
    WLMContextLock rq_lock(&g_instance.wlm_cxt->io_context.waiting_list_mutex);

    rq_lock.Lock();

    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    g_instance.wlm_cxt->io_context.waiting_list = lappend(g_instance.wlm_cxt->io_context.waiting_list, rqust);

    ereport(DEBUG1, (errmsg("An IO REQUEST (%dcount) HAS BEEN ENQUEUED(%d)!", rqust->count, rqust->count_down)));

    PG_TRY();
    {
        /* check for pending interrupts before waiting. */
        CHECK_FOR_INTERRUPTS();

        t_thrd.int_cxt.ImmediateInterruptOK = true;

        rq_lock.ConditionWait(&(rqust->io_proceed_cond));

        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;

        /* we must make sure we have the lock now */
        if (!rq_lock.IsOwner()) {
            rq_lock.Lock(true);
        }

        g_instance.wlm_cxt->io_context.waiting_list =
            list_delete_ptr(g_instance.wlm_cxt->io_context.waiting_list, rqust);

        pfree(rqust);

        rqust = NULL;

        rq_lock.UnLock();

        PG_RE_THROW();
    }
    PG_END_TRY();

    rq_lock.UnLock();

    ereport(DEBUG1, (errmsg("An IO REQUEST HAS BEEN DEQUEUED!")));

    return;
}

/*
 * @Description: check whether current iops has reached the "iops_limits"
 * @IN         : io_count: current iops
 * @IN         : count: IO requests counts
 * @IN         : iops_limits: user set iops_limits
 * @RETURN     : true: IO is available  false: IO is not available, need euqueue
 * @See also:
 */
bool IsIoLimitAvail(int io_count, int count, int iops_limits)
{
    if (!iops_limits) {
        return true;
    }

    // count may be larger than current iops_limits in some cases.
    if (count > iops_limits) {
        return true;
    }

    if (count + io_count > iops_limits) {
        return false;
    }

    return true;
}

/*
 * @Description: check whether current iops has reached the user set IO limits
 * @IN         : count: IO requests counts
 * @RETURN     : 0 :  IO requests need not be nequeued
 *               > 0 :IO requests need be enqueued,
 *               and how many times the requests need wait for to dequeue.
 * @See also:
 */
int IsIOAvail(int count)
{
    WLMAutoLWLock stat_lock(WorkloadIoStatHashLock, LW_SHARED);
    WLMAutoLWLock user_lock(WorkloadUserInfoLock, LW_SHARED);

    stat_lock.AutoLWLockAcquire();
    user_lock.AutoLWLockAcquire();

    if (IS_PGXC_DATANODE && u_sess->wlm_cxt->wlm_params.ioptr != NULL) {
        WLMDNodeIOInfo* info = (WLMDNodeIOInfo*)u_sess->wlm_cxt->wlm_params.ioptr;
        UserData* userdata = (UserData*)info->userptr;

        /*
         * check order:
         * 1. whether query has reached io_limits
         * 2. whether user has reached io_limits
         * 3. whether query has reached io_limits calculated from io_priority
         * 4. whether user has reached io_limits calculated from io_priority
         */
        if (info->iops_limits) {
            if (!IsIoLimitAvail(info->io_geninfo.io_count_persec, count, info->iops_limits)) {
                return info->io_geninfo.tick_count_down;
            }
        } else if (userdata != NULL && userdata->respool != NULL && userdata->respool->iops_limits) {
            if (!IsIoLimitAvail(userdata->ioinfo.io_count_persec, count, userdata->respool->iops_limits)) {
                return userdata->ioinfo.tick_count_down;
            }
        }

        if (info->io_priority) {
            if (!IsIoLimitAvail(info->io_geninfo.io_count, count, info->io_geninfo.curr_iops_limit)) {
                return info->io_geninfo.prio_count_down;
            }
        } else if (userdata != NULL && userdata->respool && userdata->respool->io_priority) {
            if (!IsIoLimitAvail(userdata->ioinfo.io_count, count, userdata->ioinfo.curr_iops_limit)) {
                return userdata->ioinfo.prio_count_down;
            }
        }
    }

    user_lock.AutoLWLockRelease();
    stat_lock.AutoLWLockRelease();

    return 0;
}

/*
 * @Description: create a request structure
 * @IN         : IOtype: read/write  -- remain for extension and debug
 *             : count: how many read/write will be sent this time
 *             : count_down: count down times of the requests
 * @RETURN     : IO request pointer
 * @See also:
 */
IORequestEntry* MakeRequestInMcxt(int IOtype, int count, int count_down)
{
    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    IORequestEntry* rqst = (IORequestEntry*)palloc(sizeof(IORequestEntry));

    rqst->rqst_type = IOtype;
    rqst->count = count;
    pthread_cond_init(&(rqst->io_proceed_cond), NULL);

    rqst->count_down = count_down;

    return rqst;
}

/*
 * @Description: IO scheduler
 * @IN         : IOtype: read/write  -- not used, for extension and debug
 *             : count: how many read/write will be sent this time
 * @RETURN     : void
 * @See also:
 */
void ConsultIOScheduler(int io_type, int count)
{
    /* check whether IO is available */
    int count_down = IsIOAvail(count);

    if (!count_down) {
        return;
    }

    /* low priority  */
    IORequestEntry* rqust = MakeRequestInMcxt(io_type, count, count_down);

    /* enqueue the request */
    RequestEnqueue(rqust);

    // destroy the request entry
    pfree(rqust);

    return;
}

/*
 * @Description: update IO info for collection
 * @IN         : count: count times
 * @RETURN     : void
 * @See also:
 */
void WLMUpdateIOInfo(int count)
{
    if (u_sess->wlm_cxt->wlm_params.ioptr) {
        WLMDNodeIOInfo* info = (WLMDNodeIOInfo*)u_sess->wlm_cxt->wlm_params.ioptr;
        UserData* userdata = (UserData*)info->userptr;

        if (userdata != NULL) {
            gs_atomic_add_32(&userdata->ioinfo.io_count, count);
            gs_atomic_add_32(&userdata->ioinfo.io_count_persec, count);
        }

        gs_atomic_add_32(&info->io_geninfo.io_count, count);
        gs_atomic_add_32(&info->io_geninfo.io_count_persec, count);
    }
}

/*
 * @Description: update IO info for collection and consult IO monitor whether IO is available or not
 * @IN         : type: IO type --- for extension and debug.
 *             : count: count of the IO requests
 * @RETURN     : void
 * @See also:
 */
void IOSchedulerAndUpdateInternal(int type, int count)
{
    // IO scheduler
    if (u_sess->wlm_cxt->wlm_params.iocontrol) {
        ConsultIOScheduler(type, count);
    }

    // update iops
    WLMUpdateIOInfo(count);
}

/*
 * @Description: update IO info for collection and consult IO monitor whether IO is available or not
 * @IN         : type: IO type --- for extension and debug.
 *             : count: count of the IO requests
 *             : store_type: treat row storage or column store in different strategies
 * @RETURN     : void
 * @See also:
 */
void IOSchedulerAndUpdate(int type, int count, int store_type)
{
    if (!IS_PGXC_DATANODE || (IS_PGXC_COORDINATOR && IsConnFromCoord())) {
        return;
    }

    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (g_wlm_params->iotrack || g_wlm_params->iocontrol) {
        // for row store, iops is calculted in unit of IOCONTROL_UNIT
        if (store_type == IO_TYPE_ROW) {
            g_wlm_params->iocount += count;

            if (g_wlm_params->iocount >= u_sess->attr.attr_resource.io_control_unit) {
                IOSchedulerAndUpdateInternal(type, 1);

                g_wlm_params->iocount = 0;
            }
        // for column store, iops is calculted in unit of 1
        } else if (store_type == IO_TYPE_COLUMN) {
            IOSchedulerAndUpdateInternal(type, count);
        }
    }
}

/*
 * @Description: update IO read/write bytes for statistics
 * @IN         : type: read/write operation
 *               size: read/write size
 * @RETURN     : void
 */
void WLMUpdateIOInfoSize(int type, int count, int size)
{
    if (type == IO_TYPE_READ) {
        gs_atomic_add_64((int64*)&g_instance.wlm_cxt->instance_manager.logical_read_bytes[1], size);
        gs_atomic_add_64((int64*)&g_instance.wlm_cxt->instance_manager.read_counts[1], count);
    } else if (type == IO_TYPE_WRITE) {
        gs_atomic_add_64((int64*)&g_instance.wlm_cxt->instance_manager.logical_write_bytes[1], size);
        gs_atomic_add_64((int64*)&g_instance.wlm_cxt->instance_manager.write_counts[1], count);
    }

    /* do not change BuildUserInfoHash lock logical, multi-thread create/alter/drop
     * user condition, might cause g_wlm_params.userdata using wrong address,
     * might cause g_wlm_params.userdata not match CurrentUserId */
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;
    if ((g_wlm_params->complicate && g_wlm_params->userdata != NULL) ||
        (g_wlm_params->complicate_stream && g_wlm_params->userdata != NULL)) {

        UserData* userdata = (UserData*)g_wlm_params->userdata;
        if (userdata != NULL) {
            gs_atomic_add_32((int32*)&userdata->referenceCnt, 1);
            if (type == IO_TYPE_READ) {
                gs_atomic_add_64((int64*)&userdata->ioinfo.read_bytes[1], size);
                gs_atomic_add_64((int64*)&userdata->ioinfo.read_counts[1], count);
            } else if (type == IO_TYPE_WRITE) {
                gs_atomic_add_64((int64*)&userdata->ioinfo.write_bytes[1], size);
                gs_atomic_add_64((int64*)&userdata->ioinfo.write_counts[1], count);
            }
            gs_atomic_add_32((int32*)&userdata->referenceCnt, -1);
        }
    }
}

/*
 * @Description: IOStatistics is used for track IO resource
 * @IN         : type: IO type: read or write.
 *             : count: count of the IO requests
 *             : size: read/write bytes
 * @RETURN     : void
 */
void IOStatistics(int type, int count, int size)
{
    if (!IS_PGXC_DATANODE) {
        return;
    }

    WLMUpdateIOInfoSize(type, count, size);
}

/*
 * @Description: check whether node is IO cost node or not
 * @IN         : node: node of the plan tree
 * @RETURN     : true: is io cost node; false: is not io cost node
 * @See also:
 */
bool IsIOCostNode(NodeTag node)
{
    switch (node) {
        case T_SeqScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            return true;
        default:
            return false;
    }

    return false;
}

/*
 * @Description: get IO total cost for the plan tree, and update iocomplex in parctl_state
 * @IN         : root: root info of the plan tree
 *             : node: node of the plan tree
 *             : IOCost: total_cost of the node
 * @RETURN     :
 * @See also:
 */
void WLMmonitor_check_and_update_IOCost(PlannerInfo* root, NodeTag node, Cost IOcost)
{
    if (IsIOCostNode(node)) {
        root->glob->IOTotalCost += IOcost;
    }

    if (!t_thrd.wlm_cxt.parctl_state.iocomplex && root->glob->IOTotalCost >= IO_MIN_COST) {
        t_thrd.wlm_cxt.parctl_state.iocomplex = 1;
    }
}
