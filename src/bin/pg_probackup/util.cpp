/*-------------------------------------------------------------------------
 *
 * util.c: log messages to log file or stderr, and misc code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "catalog/pg_control.h"

#include <signal.h>
#include <time.h>

#include <unistd.h>

#include <sys/stat.h>
#include "tool_common.h"
#include "common/fe_memutils.h"
#include "storage/file/fio_device.h"

static const char *statusName[] =
{
    "UNKNOWN",
    "OK",
    "ERROR",
    "RUNNING",
    "MERGING",
    "MERGED",
    "DELETING",
    "DELETED",
    "DONE",
    "ORPHAN",
    "CORRUPT"
};

static const char *devTypeName[] =
{
    "FILE",
    "DSS",
    "UNKNOWN",
    "UNKNOWN"
};

uint32 NUM_65536 = 65536;
uint32 NUM_10000 = 10000;

const char *
base36enc(long unsigned int value)
{
    const char    base36[36 + 1] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    /* log(2**64) / log(36) = 12.38 => max 13 char + '\0' */
    static char    buffer[14];
    unsigned int offset = sizeof(buffer);

    buffer[--offset] = '\0';
    do {
        buffer[--offset] = base36[value % 36];
    } while (value /= 36);

    return &buffer[offset];
}

/*
 * Same as base36enc(), but the results must be released by the user.
 */
char *
base36enc_dup(long unsigned int value)
{
    const char    base36[36 + 1] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    /* log(2**64) / log(36) = 12.38 => max 13 char + '\0' */
    char        buffer[14];
    unsigned int offset = sizeof(buffer);

    buffer[--offset] = '\0';
    do {
        buffer[--offset] = base36[value % 36];
    } while (value /= 36);

    return strdup(&buffer[offset]);
}

long unsigned int
base36dec(const char *text)
{
    return strtoul(text, NULL, 36);
}

static void
checkControlFile(ControlFileData *ControlFile)
{
    pg_crc32c   crc;
    /* Calculate CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);
    /* Then compare it */
    if (!EQ_CRC32C(crc, ControlFile->crc))
        elog(ERROR, "Calculated CRC checksum does not match value stored in file.\n"
             "Either the file is corrupt, or it has a different layout than this program\n"
             "is expecting. The results below are untrustworthy.");

    if ((ControlFile->pg_control_version % NUM_65536 == 0 || ControlFile->pg_control_version % NUM_65536 > NUM_10000) &&
        ControlFile->pg_control_version / NUM_65536 != 0)
        elog(ERROR, "possible byte ordering mismatch\n"
                 "The byte ordering used to store the pg_control file might not match the one\n"
                 "used by this program. In that case the results below would be incorrect, and\n"
                 "the PostgreSQL installation would be incompatible with this data directory.");
}

static void checkSSControlFile(ControlFileData* ControlFile, char* last, size_t size)
{
    pg_crc32c   crc;
    /* Calculate CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
    COMP_CRC32C(crc, last, size);
    FIN_CRC32C(crc);
    ControlFile->crc = crc;

    if ((ControlFile->pg_control_version % NUM_65536 == 0 || ControlFile->pg_control_version % NUM_65536 > NUM_10000) &&
        ControlFile->pg_control_version / NUM_65536 != 0)
        elog(ERROR, "possible byte ordering mismatch\n"
                 "The byte ordering used to store the pg_control file might not match the one\n"
                 "used by this program. In that case the results below would be incorrect, and\n"
                 "the PostgreSQL installation would be incompatible with this data directory.");
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void
digestControlFile(ControlFileData *ControlFile, char *src, size_t size)
{
    errno_t rc;
    char* oldSrc = src;
    char* tmpDssSrc;
    size_t clearSize = 0;
    bool dssMode = IsDssMode();
    int instanceId = instance_config.dss.instance_id;
    int64 instanceId64 = (int64) instanceId;
    size_t instanceIdSize = (size_t) instanceId;
    size_t compareSize = PG_CONTROL_SIZE;
    /* control file contents need special handle in dss mode */
    if (dssMode) {
        // dms support (MAX_INSTANCEID + 1) instance, and last page for all control.
        compareSize = 1 + MAX_INSTANCEID - MIN_INSTANCEID;
        compareSize = (compareSize + 1) * PG_CONTROL_SIZE;
        src += instanceId64 * PG_CONTROL_SIZE;
        // in here, we clear all control page except instance page and last page.
        if (instanceId != MIN_INSTANCEID) {
            clearSize = instanceIdSize * PG_CONTROL_SIZE;
            rc = memset_s(oldSrc, clearSize, 0, clearSize);
            securec_check_c(rc, "\0", "\0");
        }
        if (instanceId != MAX_INSTANCEID) {
            clearSize = (size_t) (MAX_INSTANCEID - instanceIdSize) * PG_CONTROL_SIZE;
            tmpDssSrc = oldSrc;
            tmpDssSrc += (instanceId64 + 1) * PG_CONTROL_SIZE;
            rc = memset_s(tmpDssSrc, clearSize, 0, clearSize);
            securec_check_c(rc, "\0", "\0");
        }
    }
    
    if (size != compareSize)
            elog(ERROR, "unexpected control file size %d, expected %d",
                 (int) size, compareSize);

    rc = memcpy_s(ControlFile, sizeof(ControlFileData), src, sizeof(ControlFileData));
    securec_check_c(rc, "\0", "\0");

    /* Additional checks on control file */
    if (dssMode) {
        tmpDssSrc = oldSrc;
        tmpDssSrc += (REFORMER_CTL_INSTANCEID) * PG_CONTROL_SIZE;
        checkSSControlFile(ControlFile, tmpDssSrc, PG_CONTROL_SIZE);
        /* Calculate the control file CRC */
        pg_crc32c   crc;
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, src, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);
        ((ControlFileData*)src)->crc = crc;
    } else {
        checkControlFile(ControlFile);
    }
}

/*
 * Write ControlFile to pg_control
 */
static void
writeControlFile(ControlFileData *ControlFile, const char *path, fio_location location)
{
    int            fd;
    char       *buffer = NULL;

    buffer = (char *)pg_malloc(PG_CONTROL_SIZE);
    errno_t rc = memcpy_s(buffer, sizeof(ControlFileData), ControlFile, sizeof(ControlFileData));
    securec_check_c(rc, "\0", "\0");
    /* Write pg_control */
    fd = fio_open(path,
                  O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, location);
    if (fd < 0)
        elog(ERROR, "Failed to open file: %s", path);

    if (fio_write(fd, buffer, PG_CONTROL_SIZE) != PG_CONTROL_SIZE)
        elog(ERROR, "Failed to overwrite file: %s", path);

    if (fio_flush(fd) != 0)
        elog(ERROR, "Failed to sync file: %s", path);

    fio_close(fd);
    pg_free(buffer);
}

/*
 * Write Dss buffer to pg_control
 */
static void
writeDssControlFile(char* src, size_t srcLen, const char *path, fio_location location)
{
    int            fd;
    /* Write pg_control */
    fd = fio_open(path,
                  O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, location);
    if (fd < 0)
        elog(ERROR, "Failed to open file: %s", path);

    if (fio_write(fd, src, srcLen) != (ssize_t)srcLen)
        elog(ERROR, "Failed to overwrite file: %s", path);

    if (fio_flush(fd) != 0)
        elog(ERROR, "Failed to sync file: %s", path);

    if (fio_close(fd) != 0) {
        elog(ERROR, "Failed to close file: %s", path);
    }
}

/*
 * Utility shared by backup and restore to fetch the current timeline
 * used by a node.
 */
TimeLineID
get_current_timeline(PGconn *conn)
{

    PGresult   *res;
    TimeLineID tli = 0;
    char       *val;

    res = pgut_execute_extended(conn,
                   "SELECT timeline_id FROM pg_control_checkpoint()", 0, NULL, true, true);

    if (PQresultStatus(res) == PGRES_TUPLES_OK)
        val = PQgetvalue(res, 0, 0);
    else
        return get_current_timeline_from_control(false);

    if (!parse_uint32(val, &tli, 0))
    {
        PQclear(res);
        elog(WARNING, "Invalid value of timeline_id %s", val);

        /* TODO 3.0 remove it and just error out */
        return get_current_timeline_from_control(false);
    }

    return tli;
}

/* Get timeline from pg_control file */
TimeLineID
get_current_timeline_from_control(bool safe)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t      size;
    fio_location location;

    /* First fetch file... */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, instance_config.pgdata, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    if (safe && buffer == NULL)
        return 0;

    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.checkPointCopy.ThisTimeLineID;
}

/*
 * Get last check point record ptr from pg_tonrol.
 */
XLogRecPtr
get_checkpoint_location(PGconn *conn)
{
#if PG_VERSION_NUM >= 90600
    PGresult   *res;
    uint32        lsn_hi;
    uint32        lsn_lo;
    XLogRecPtr    lsn;

#if PG_VERSION_NUM >= 100000
    res = pgut_execute(conn,
                       "SELECT checkpoint_lsn FROM pg_catalog.pg_control_checkpoint()",
                       0, NULL);
#else
    res = pgut_execute(conn,
                       "SELECT checkpoint_location FROM pg_catalog.pg_control_checkpoint()",
                       0, NULL);
#endif
    XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
    PQclear(res);
    /* Calculate LSN */
    lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

    return lsn;
#else
    char       *buffer;
    size_t        size;
    ControlFileData ControlFile;
    fio_location location;

    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, instance_config.pgdata, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.checkPoint;
#endif
}

uint64
get_system_identifier(const char *pgdata_path)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;
    fio_location location;

    /* First fetch file... */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, pgdata_path, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    
    if (buffer == NULL)
        return 0;
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.system_identifier;
}

uint64
get_remote_system_identifier(PGconn *conn)
{
#if PG_VERSION_NUM >= 90600
    PGresult   *res;
    uint64        system_id_conn;
    char       *val;

    res = pgut_execute(conn,
                       "SELECT system_identifier FROM pg_catalog.pg_control_system()",
                       0, NULL);
    val = PQgetvalue(res, 0, 0);
    if (!parse_uint64(val, &system_id_conn, 0))
    {
        PQclear(res);
        elog(ERROR, "%s is not system_identifier", val);
    }
    PQclear(res);

    return system_id_conn;
#else
    char       *buffer;
    size_t        size;
    ControlFileData ControlFile;
    fio_location location;

    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, instance_config.pgdata, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.system_identifier;
#endif
}

uint32
get_xlog_seg_size(char *pgdata_path)
{
#if PG_VERSION_NUM >= 110000
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;
    fio_location location;

    /* First fetch file... */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, pgdata_path, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.xlog_seg_size;
#else
    return (uint32) XLogSegSize;
#endif
}

uint32
get_data_checksum_version(bool safe)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;
    fio_location location;

    /* First fetch file... */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, instance_config.pgdata, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    if (buffer == NULL)
        return 0;
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return 0; // ControlFile.data_checksum_version;
}

pg_crc32c
get_pgcontrol_checksum(const char *fullpath)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;

    /* First fetch file in backup dir ... */
    buffer = slurpFile(fullpath, &size, false, FIO_BACKUP_HOST);
    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    return ControlFile.crc;
}

void
get_redo(const char *pgdata_path, RedoParams *redo)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;
    fio_location location;

    /* First fetch file... */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, pgdata_path, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }

    digestControlFile(&ControlFile, buffer, size);
    pg_free(buffer);

    redo->lsn = ControlFile.checkPointCopy.redo;
    redo->tli = ControlFile.checkPointCopy.ThisTimeLineID;

    if (ControlFile.minRecoveryPoint > 0 &&
        ControlFile.minRecoveryPoint < redo->lsn)
    {
        redo->lsn = ControlFile.minRecoveryPoint;
    }

    if (ControlFile.backupStartPoint > 0 &&
        ControlFile.backupStartPoint < redo->lsn)
    {
        redo->lsn = ControlFile.backupStartPoint;
        redo->tli = ControlFile.checkPointCopy.ThisTimeLineID;
    }

}

void
parse_vgname_args(const char* args)
{
    char *vgname = xstrdup(args);
    if (strstr(vgname, "/") != NULL)
        elog(ERROR, "invalid token \"/\" in vgname");

    char *comma = strstr(vgname, ",");
    if (comma == NULL) {
        instance_config.dss.vgdata = vgname;
        instance_config.dss.vglog = const_cast<char*>("");
        return;
    }

    instance_config.dss.vgdata = xstrdup(vgname);
    comma = strstr(instance_config.dss.vgdata, ",");
    comma[0] = '\0';
    instance_config.dss.vglog = comma + 1;
    if (strstr(instance_config.dss.vgdata, ",") != NULL)
        elog(ERROR, "invalid vgname args, should be two volume group names, example: \"+data,+log\"");
    if (strstr(instance_config.dss.vglog, ",") != NULL)
        elog(ERROR, "invalid vgname args, should be two volume group names, example: \"+data,+log\"");
}

bool
is_ss_xlog(const char *ss_dir)
{
    char ss_xlog[MAXPGPATH] = {0};
    char ss_notify[MAXPGPATH] = {0};
    char ss_snapshots[MAXPGPATH] = {0};
    int rc = EOK;
    int instance_id = instance_config.dss.instance_id;

    rc = sprintf_s(ss_xlog, sizeof(ss_xlog), "%s%d", "pg_xlog", instance_id);
    securec_check_ss_c(rc, "\0", "\0");

    rc = sprintf_s(ss_notify, sizeof(ss_notify), "%s%d", "pg_notify", instance_id);
    securec_check_ss_c(rc, "\0", "\0");

    rc = sprintf_s(ss_snapshots, sizeof(ss_snapshots), "%s%d", "pg_snapshots", instance_id);
    securec_check_ss_c(rc, "\0", "\0");

    if (IsDssMode() && strlen(instance_config.dss.vglog) &&
        (pg_strcasecmp(ss_dir, ss_xlog) == 0 ||
        pg_strcasecmp(ss_dir, ss_notify) == 0 ||
        pg_strcasecmp(ss_dir, ss_notify) == 0)) {
        return true;
    }
    return false;
}

void
ss_createdir(const char *ss_dir, const char *vgdata, const char *vglog)
{
    char path[MAXPGPATH] = {0};
    char link_path[MAXPGPATH] = {0};
    int rc = EOK;

    rc = sprintf_s(link_path, sizeof(link_path), "%s/%s", vgdata, ss_dir);
    securec_check_ss_c(rc, "\0", "\0");
    rc = sprintf_s(path, sizeof(path), "%s/%s", vglog, ss_dir);
    securec_check_ss_c(rc, "\0", "\0");

    dir_create_dir(path, DIR_PERMISSION);

    /* if xlog link is already exist, destroy it and recreate */
    if (unlink(link_path) != 0) {
        elog(ERROR, "can not remove xlog dir \"%s\" : %s", link_path, strerror(errno));
    }
    
    if (symlink(path, link_path) < 0) {
        elog(ERROR, "can not link dss xlog dir \"%s\" to dss xlog dir \"%s\": %s", link_path, path,
            strerror(errno));
    }
}

bool
ss_create_if_pg_replication(pgFile* dir, const char* vgdata, const char* vglog)
{
    if (pg_strcasecmp(dir->rel_path, "pg_replication") == 0) {
        char path[MAXPGPATH];
        errno_t rc = sprintf_s(path, sizeof(path), "%s/%s", vglog, dir->rel_path);
        securec_check_ss_c(rc, "\0", "\0");
        char link_path[MAXPGPATH];
        rc = sprintf_s(link_path, sizeof(link_path), "%s/%s", vgdata, dir->rel_path);
        securec_check_ss_c(rc, "\0", "\0");
        dir_create_dir(path, DIR_PERMISSION);

        /* if link is already exist, destroy it and recreate */
        if (unlink(link_path) != 0) {
            elog(ERROR, "can not remove pg_replication dir \"%s\" : %s", link_path,
                strerror(errno));
        }

        if (symlink(path, link_path) < 0) {
            elog(ERROR, "can not link dss  dir \"%s\" to dss  dir \"%s\": %s", link_path, path,
                strerror(errno));
        }
        return true;
    } else {
        return false;
    }
}

bool
ss_create_if_doublewrite(pgFile* dir, const char* vgdata, int instance_id)
{
    char ss_doublewrite[MAXPGPATH];
    errno_t rc = sprintf_s(ss_doublewrite, sizeof(ss_doublewrite), "%s%d", "pg_doublewrite", instance_id);
    securec_check_ss_c(rc, "\0", "\0");
    if (pg_strcasecmp(dir->rel_path, ss_doublewrite) == 0) {
        rc = sprintf_s(ss_doublewrite, sizeof(ss_doublewrite), "%s/%s", vgdata, dir->rel_path);
        securec_check_ss_c(rc, "\0", "\0");
        dir_create_dir(ss_doublewrite, DIR_PERMISSION);
        return true;
    } else {
        return false;
    }
}

/*
 * Rewrite minRecoveryPoint of pg_control in backup directory. minRecoveryPoint
 * 'as-is' is not to be trusted.
 */
void
set_min_recovery_point(pgFile *file, const char *fullpath,
                       XLogRecPtr stop_backup_lsn)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t      size;
    fio_location location;

    /* First fetch file content */
    location = is_dss_file(T_XLOG_CONTROL_FILE) ? FIO_DSS_HOST : FIO_DB_HOST;
    if (IsDssMode()) {
        buffer = slurpFile(T_XLOG_CONTROL_FILE, &size, false, location);
    } else {
        char xlog_ctl_file_path[MAXPGPATH] = {'\0'};
        join_path_components(xlog_ctl_file_path, instance_config.pgdata, T_XLOG_CONTROL_FILE);
        buffer = slurpFile(xlog_ctl_file_path, &size, false, location);
    }
    digestControlFile(&ControlFile, buffer, size);

    elog(LOG, "Current minRecPoint %X/%X",
        (uint32) (ControlFile.minRecoveryPoint  >> 32),
        (uint32) ControlFile.minRecoveryPoint);

    elog(LOG, "Setting minRecPoint to %X/%X",
        (uint32) (stop_backup_lsn  >> 32),
        (uint32) stop_backup_lsn);

    ControlFile.minRecoveryPoint = stop_backup_lsn;

    /* Update checksum in pg_control header */
    INIT_CRC32C(ControlFile.crc);
    COMP_CRC32C(ControlFile.crc, (char *) &ControlFile,
                offsetof(ControlFileData, crc));
    FIN_CRC32C(ControlFile.crc);

    /* overwrite pg_control */
    writeControlFile(&ControlFile, fullpath, FIO_LOCAL_HOST);

    /* Update pg_control checksum in backup_list */
    file->crc = ControlFile.crc;

    pg_free(buffer);
}

/*
 * Copy pg_control file to backup. We do not apply compression to this file.
 */
void
copy_pgcontrol_file(const char *from_fullpath, fio_location from_location,
                    const char *to_fullpath, fio_location to_location, pgFile *file)
{
    ControlFileData ControlFile;
    char       *buffer;
    size_t        size;

    buffer = slurpFile(from_fullpath, &size, false, from_location);

    /* 
     * In dss mode, we need to set the parameter list_stable  
     * on the pg_control file's last page to 0.
     */
    if (is_dss_type(file->type)) {
        ss_reformer_ctrl_t *reformerCtrl;
        reformerCtrl = (ss_reformer_ctrl_t *)(buffer + (MAX_INSTANCEID + 1) * PG_CONTROL_SIZE);
        reformerCtrl->list_stable = 0;
        /* Calculate the reformer_ctrl CRC */
        pg_crc32c   crc;
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *) reformerCtrl, offsetof(ss_reformer_ctrl_t, crc));
        FIN_CRC32C(crc);
        reformerCtrl->crc = crc;
    }

    digestControlFile(&ControlFile, buffer, size);
    file->crc = ControlFile.crc;
    file->read_size = size;
    file->write_size = size;
    file->uncompressed_size = size;
    
    /* Write pg_control */
    if (is_dss_type(file->type)) {
        writeDssControlFile(buffer, size, to_fullpath, to_location);
    } else {
        writeControlFile(&ControlFile, to_fullpath, to_location);
    }
    pg_free(buffer);
}

/*
 * Parse string representation of the server version.
 */
uint32
parse_server_version(const char *server_version_str)
{
    int            nfields;
    uint32        result = 0;
    int            major_version = 0;
    int            minor_version = 0;

    nfields = sscanf_s(server_version_str, "%d.%d", &major_version, &minor_version);
    if (nfields == 2)
    {
        /* Server version lower than 10 */
        if (major_version > 10)
            elog(ERROR, "Server version format doesn't match major version %d", major_version);
        result = major_version * 10000 + minor_version * 100;
    }
    else if (nfields == 1)
    {
        if (major_version < 10)
            elog(ERROR, "Server version format doesn't match major version %d", major_version);
        result = major_version * 10000;
    }
    else
        elog(ERROR, "Unknown server version format %s", server_version_str);

    return result;
}

/*
 * Parse string representation of the program version.
 */
uint32
parse_program_version(const char *program_version)
{
    int            nfields;
    int            major = 0,
                minor = 0,
                micro = 0;
    uint32        result = 0;

    if (program_version == NULL || program_version[0] == '\0')
        return 0;

    nfields = sscanf_s(program_version, "%d.%d.%d", &major, &minor, &micro);
    if (nfields == 3)
        result = major * 10000 + minor * 100 + micro;
    else
        elog(ERROR, "Unknown program version format %s", program_version);

    return result;
}

const char *
status2str(BackupStatus status)
{
    if (status < BACKUP_STATUS_INVALID || BACKUP_STATUS_CORRUPT < status)
        return "UNKNOWN";

    return statusName[status];
}

BackupStatus
str2status(const char *status)
{
    for (int i = 0; i <= BACKUP_STATUS_CORRUPT; i++)
    {
        if (pg_strcasecmp(status, statusName[i]) == 0) {
            return (BackupStatus)i;
        }
    }
    return BACKUP_STATUS_INVALID;
}

const char *dev2str(device_type_t type)
{
    return devTypeName[type];
}

device_type_t str2dev(const char *dev)
{
    for (int i = 0; i < (int)DEV_TYPE_NUM; i++) {
        if (pg_strcasecmp(dev, devTypeName[i]) == 0)
            return (device_type_t)i;
    }
    return DEV_TYPE_INVALID;
}

bool
datapagemap_is_set(datapagemap_t *map, BlockNumber blkno)
{
    int            offset;
    int            bitno;

    offset = blkno / 8;
    bitno = blkno % 8;

    /* enlarge or create bitmap if needed */
    if (map->bitmapsize <= offset)
    {
        int            oldsize = map->bitmapsize;
        int            newsize;

        /*
         * The minimum to hold the new bit is offset + 1. But add some
         * headroom, so that we don't need to repeatedly enlarge the bitmap in
         * the common case that blocks are modified in order, from beginning
         * of a relation to the end.
         */
        newsize = offset + 1;
        newsize += 10;

        map->bitmap = (unsigned char *)pg_realloc(map->bitmap, newsize);

        /* zero out the newly allocated region */
        errno_t rc = memset_s(&map->bitmap[oldsize], newsize - oldsize, 0, newsize - oldsize);
        securec_check(rc, "\0", "\0");

        map->bitmapsize = newsize;
    }

    

    /* check the bit */
    return map->bitmap[offset] & (1 << bitno);
}

/*
 * A debugging aid. Prints out the contents of the page map.
 */
void
datapagemap_print_debug(datapagemap_t *map)
{
    datapagemap_iterator_t *iter;
    BlockNumber blocknum;

    iter = datapagemap_iterate(map);
    while (datapagemap_next(iter, &blocknum))
        elog(INFO, "  block %u", blocknum);

    pg_free(iter);
}

/*
 * Return pid of postmaster process running in given pgdata.
 * Return 0 if there is none.
 * Return 1 if postmaster.pid is mangled.
 */
pid_t
check_postmaster(const char *pgdata)
{
    FILE  *fp;
    pid_t  pid;
    char   pid_file[MAXPGPATH];

    errno_t rc = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1,
               "%s/postmaster.pid", pgdata);
    securec_check_ss_c(rc, "\0", "\0");
    canonicalize_path(pid_file);
    fp = fopen(pid_file, "r");
    if (fp == NULL)
    {
        /* No pid file, acceptable*/
        if (errno == ENOENT)
            return 0;
        else
            elog(ERROR, "Cannot open file \"%s\": %s",
                pid_file, strerror(errno));
    }

    if (fscanf_s(fp, "%i", &pid) != 1)
    {
        /* something is wrong with the file content */
        pid = 1;
    }

    if (pid > 1)
    {
        if (kill(pid, 0) != 0)
        {
            /* process no longer exists */
            if (errno == ESRCH)
                pid = 0;
            else
                elog(ERROR, "Failed to send signal 0 to a process %d: %s",
                        pid, strerror(errno));
        }
    }

    fclose(fp);
    return pid;
}

/*
 * Replace the actual password with *'s.
 */
void replace_password(int argc, char** argv, const char* optionName)
{
    int count = 0;
    char* pchPass = NULL;
    char* pchTemp = NULL;

    // Check if password option is specified in command line
    for (count = 0; count < argc; count++) {
        // Password can be specified by optionName
        if (strncmp(optionName, argv[count], strlen(optionName)) == 0) {
            pchTemp = strchr(argv[count], '=');
            if (pchTemp != NULL) {
                pchPass = pchTemp + 1;
            } else if ((NULL != strstr(argv[count], optionName)) && (strlen(argv[count]) > strlen(optionName))) {
                pchPass = argv[count] + strlen(optionName);
            } else {
                pchPass = argv[(int)(count + 1)];
            }

            // Replace first char of password with * and rest clear it
            if (strlen(pchPass) > 0) {
                *pchPass = '*';
                pchPass = pchPass + 1;
                while ('\0' != *pchPass) {
                    *pchPass = '\0';
                    pchPass++;
                }
            }

            break;
        }
    }
}
