/*-------------------------------------------------------------------------
 *
 * show.c: show backup information.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include "json.h"
#include "common/fe_memutils.h"

#define half_rounded(x)   (((x) + ((x) < 0 ? 0 : 1)) / 2)
#define MIN_SPACE 1048576
/* struct to align fields printed in plain format */
typedef struct ShowBackendRow
{
    const char *instance;
    const char *version;
    char        backup_id[20];
    char        recovery_time[100];
    const char *mode;
    const char *wal_mode;
    char        tli[20];
    char        duration[20];
    char        data_bytes[20];
    char        wal_bytes[20];
    char        zratio[20];
    char        start_lsn[20];
    char        stop_lsn[20];
    char        type[20];
    char        oss_status[20];
    const char *status;
} ShowBackendRow;

/* struct to align fields printed in plain format */
typedef struct ShowArchiveRow
{
    char        tli[20];
    char        parent_tli[20];
    char        switchpoint[20];
    char        min_segno[MAXFNAMELEN];
    char        max_segno[MAXFNAMELEN];
    char        n_segments[20];
    char        size[20];
    char        zratio[20];
    const char *status;
    char        n_backups[20];
} ShowArchiveRow;

static void show_instance_start(void);
static void show_instance_end(void);
static void show_instance(InstanceConfig *instance, time_t requested_backup_id, bool show_name);
static void print_backup_json_object(PQExpBuffer buf, pgBackup *backup);
static int show_backup(const char *instance_name, time_t requested_backup_id);

static void show_instance_plain(const char *instance_name, device_type_t instance_type,
                                parray *backup_list, bool show_name);
static void show_instance_json(const char *instance_name, parray *backup_list);

static void show_instance_archive(InstanceConfig *instance);
static void show_archive_plain(const char *instance_name, uint32 xlog_seg_size,
                               parray *timelines_list, bool show_name);
static void show_archive_json(const char *instance_name, uint32 xlog_seg_size,
                              parray *tli_list);

static PQExpBufferData show_buf;
static bool first_instance = true;
static int32 json_level = 0;

/*
 * Entry point of pg_probackup SHOW subcommand.
 */
int
do_show(const char *instance_name, time_t requested_backup_id, bool show_archive)
{
    size_t i;

    if (instance_name == NULL &&
        requested_backup_id != INVALID_BACKUP_ID)
        elog(ERROR, "You must specify --instance to use (-i, --backup-id) option");

    if (show_archive &&
        requested_backup_id != INVALID_BACKUP_ID)
        elog(ERROR, "You cannot specify --archive and (-i, --backup-id) options together");

    /*
     * if instance_name is not specified,
     * show information about all instances in this backup catalog
     */
    if (instance_name == NULL)
    {
        parray *instances = catalog_get_instance_list();

        show_instance_start();
        for (i = 0; i < parray_num(instances); i++)
        {
            InstanceConfig *instance = (InstanceConfig *)parray_get(instances, i);
            char backup_instance_path[MAXPGPATH];

            errno_t rc = sprintf_s(backup_instance_path, MAXPGPATH, "%s/%s/%s",
                                    backup_path, BACKUPS_DIR, instance->name);
            securec_check_ss_c(rc, "\0", "\0");

            if (show_archive)
                show_instance_archive(instance);
            else
                show_instance(instance, INVALID_BACKUP_ID, true);
        }
        show_instance_end();

        parray_walk(instances, pfree);
        parray_free(instances);

        return 0;
    }
    /* always use */
    else if (show_format == SHOW_JSON ||
             requested_backup_id == INVALID_BACKUP_ID)
    {
        show_instance_start();
        InstanceConfig *instance = readInstanceConfigFile(instance_name);

        if (show_archive)
        {
            if (instance == NULL) {
                return 0;
            }
            show_instance_archive(instance);
        }
        else
            show_instance(instance, requested_backup_id, false);

        show_instance_end();

        return 0;
    }
    else
    {
        if (show_archive)
        {
            InstanceConfig *instance = readInstanceConfigFile(instance_name);
            if (instance == NULL) {
                return 0;
            }
            show_instance_archive(instance);
        }
        else
            show_backup(instance_name, requested_backup_id);

        return 0;
    }
}

void
pretty_size(int64 size, char *buf, size_t len)
{
    int64     limit = 10 * 1024;
    int64     limit2 = limit * 2 - 1;
    errno_t   rc = 0;
    
    if (len == 0) {
        return;
    }
    if (size <= 0)
    {
        rc = strncpy_s(buf, len, "0", len - 1);
        securec_check_c(rc, "", "");
        return;
    }

    if (Abs(size) < limit)
    {
        rc = snprintf_s(buf, len, len - 1, "%dB", (int) size);
        securec_check_ss_c(rc, "\0", "\0");
    }
    else
    {
        size >>= 9;
        if (Abs(size) < limit2)
        {
                rc = snprintf_s(buf, len, len - 1, "%dkB", (int) half_rounded(size));
                securec_check_ss_c(rc, "\0", "\0");
        }
        else
        {
            size >>= 10;
            if (Abs(size) < limit2)
            {
                rc = snprintf_s(buf, len, len - 1, "%dMB", (int) half_rounded(size));
                securec_check_ss_c(rc, "\0", "\0");
            }
            else
            {
                size >>= 10;
                if (Abs(size) < limit2)
                {
                    rc = snprintf_s(buf, len, len - 1, "%dGB", (int) half_rounded(size));
                    securec_check_ss_c(rc, "\0", "\0");
                }
                else
                {
                    size >>= 10;
                    rc = snprintf_s(buf, len, len - 1, "%dTB", (int) half_rounded(size));
                    securec_check_ss_c(rc, "\0", "\0");
                }
            }
        }
    }
}

void
pretty_time_interval(double time, char *buf, size_t len)
{
    int     num_seconds = 0;
    int     milliseconds = 0;
    int     seconds = 0;
    int     minutes = 0;
    int     hours = 0;
    int     days = 0;
    errno_t rc = 0;

    num_seconds = (int) time;
    if (len == 0) {
        return;
    }
    if (time <= 0)
    {
        rc = strncpy_s(buf, len, "0", len - 1);
        securec_check_c(rc, "", "");
        return;
    }

    days = num_seconds / (24 * 3600);
    num_seconds %= (24 * 3600);

    hours = num_seconds / 3600;
    num_seconds %= 3600;

    minutes = num_seconds / 60;
    num_seconds %= 60;

    seconds = num_seconds;
    milliseconds = (int)((time - (int) time) * 1000.0);

    if (days > 0)
    {
        rc = snprintf_s(buf, len, len - 1, "%dd:%dh", days, hours);
        securec_check_ss_c(rc, "\0", "\0");
        return;
    }

    if (hours > 0)
    {
        rc = snprintf_s(buf, len, len - 1, "%dh:%dm", hours, minutes);
        securec_check_ss_c(rc, "\0", "\0");
        return;
    }

    if (minutes > 0)
    {
        rc = snprintf_s(buf, len, len - 1, "%dm:%ds", minutes, seconds);
        securec_check_ss_c(rc, "\0", "\0");
        return;
    }

    if (seconds > 0)
    {
        if (milliseconds > 0)
        {
            rc = snprintf_s(buf, len, len - 1, "%ds:%dms", seconds, milliseconds);
            securec_check_ss_c(rc, "\0", "\0");
        }
        else
        {
            rc = snprintf_s(buf, len, len - 1, "%ds", seconds);
            securec_check_ss_c(rc, "\0", "\0");
        }
        return;
    }

    rc = snprintf_s(buf, len, len - 1, "%dms", milliseconds);
    securec_check_ss_c(rc, "\0", "\0");
    return;
}

/*
 * Initialize instance visualization.
 */
static void
show_instance_start(void)
{
    initPQExpBuffer(&show_buf);
    PQExpBuffer    buf = &show_buf;
    if (show_format == SHOW_PLAIN)
        return;

    first_instance = true;
    json_level = 0;
    appendPQExpBufferChar(&show_buf, '{');
    json_level++;

    json_add_key(buf, "backup_info", json_level);
    json_add(buf, JT_BEGIN_ARRAY, &json_level);
}

/*
 * Finalize instance visualization.
 */
static void
show_instance_end(void)
{
        PQExpBuffer    buf = &show_buf;
    if (show_format == SHOW_JSON) {
        json_add(buf, JT_END_ARRAY, &json_level);
        appendPQExpBufferStr(&show_buf, "\n}\n");
    }

    fputs(show_buf.data, stdout);
    termPQExpBuffer(&show_buf);
}

/*
 * Show brief meta information about all backups in the backup instance.
 */
static void
show_instance(InstanceConfig *instance, time_t requested_backup_id, bool show_name)
{
    parray       *backup_list;
    const char *instance_name;
    device_type_t instance_type;

    instance_name = instance->name;
    instance_type = instance->dss.enable_dss ? DEV_TYPE_DSS : DEV_TYPE_FILE;
    backup_list = catalog_get_backup_list(instance_name, requested_backup_id);

    if (show_format == SHOW_PLAIN)
        show_instance_plain(instance_name, instance_type, backup_list, show_name);
    else if (show_format == SHOW_JSON)
        show_instance_json(instance_name, backup_list);
    else
        elog(ERROR, "Invalid show format %d", (int) show_format);

    /* cleanup */
    parray_walk(backup_list, pgBackupFree);
    parray_free(backup_list);
}

/* helper routine to print backup info as json object */
static void
print_backup_json_object(PQExpBuffer buf, pgBackup *backup)
{
    TimeLineID    parent_tli = 0;
    char        timestamp[100] = "----";
    char        lsn[20];
    errno_t      rc = 0;

    json_add(buf, JT_BEGIN_OBJECT, &json_level);

    json_add_value(buf, "id", base36enc(backup->start_time), json_level,
                    true);

    if (backup->parent_backup != 0)
        json_add_value(buf, "parent-backup-id",
                        base36enc(backup->parent_backup), json_level, true);

    json_add_value(buf, "backup-mode", pgBackupGetBackupMode(backup),
                    json_level, true);

    json_add_value(buf, "wal", backup->stream ? "STREAM": "ARCHIVE",
                    json_level, true);

    json_add_value(buf, "compress-alg",
                    deparse_compress_alg(backup->compress_alg), json_level,
                    true);

    json_add_key(buf, "compress-level", json_level);
    appendPQExpBuffer(buf, "%d", backup->compress_level);

    json_add_value(buf, "from-replica",
                   backup->from_replica ? "true" : "false", json_level,
                   true);

    json_add_key(buf, "block-size", json_level);
    appendPQExpBuffer(buf, "%u", backup->block_size);

    json_add_key(buf, "xlog-block-size", json_level);
    appendPQExpBuffer(buf, "%u", backup->wal_block_size);

    json_add_key(buf, "checksum-version", json_level);
    appendPQExpBuffer(buf, "%u", backup->checksum_version);

    json_add_value(buf, "program-version", backup->program_version,
                    json_level, true);
    json_add_value(buf, "server-version", backup->server_version,
                    json_level, true);

    json_add_key(buf, "current-tli", json_level);
    appendPQExpBuffer(buf, "%u", backup->tli);

    json_add_key(buf, "parent-tli", json_level);

    /* Only incremental backup can have Parent TLI */
    if (backup->parent_backup_link)
        parent_tli = backup->parent_backup_link->tli;

    appendPQExpBuffer(buf, "%u", parent_tli);

    rc = snprintf_s(lsn, lengthof(lsn), lengthof(lsn) - 1, "%X/%X",
                (uint32) (backup->start_lsn >> 32), (uint32) backup->start_lsn);
    securec_check_ss_c(rc, "\0", "\0");
    json_add_value(buf, "start-lsn", lsn, json_level, true);

    rc = snprintf_s(lsn, lengthof(lsn), lengthof(lsn) - 1, "%X/%X",
                (uint32) (backup->stop_lsn >> 32), (uint32) backup->stop_lsn);
    securec_check_ss_c(rc, "\0", "\0");
    json_add_value(buf, "stop-lsn", lsn, json_level, true);

    time2iso(timestamp, lengthof(timestamp), backup->start_time);
    json_add_value(buf, "start-time", timestamp, json_level, true);

    if (backup->end_time)
    {
        time2iso(timestamp, lengthof(timestamp), backup->end_time);
        json_add_value(buf, "end-time", timestamp, json_level, true);
    }

    json_add_key(buf, "recovery-xid", json_level);
    appendPQExpBuffer(buf, XID_FMT, backup->recovery_xid);

    if (backup->recovery_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
        json_add_value(buf, "recovery-time", timestamp, json_level, true);
    }

    if (backup->recovery_name)
	{
		json_add_value(buf, "recovery-name", backup->recovery_name, json_level, true);
	}

    if (backup->expire_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->expire_time);
        json_add_value(buf, "expire-time", timestamp, json_level, true);
    }

    if (backup->data_bytes != BYTES_INVALID)
    {
        json_add_key(buf, "data-bytes", json_level);
        appendPQExpBuffer(buf, INT64_FORMAT, backup->data_bytes);
    }

    if (backup->wal_bytes != BYTES_INVALID)
    {
        json_add_key(buf, "wal-bytes", json_level);
        appendPQExpBuffer(buf, INT64_FORMAT, backup->wal_bytes);
    }

    if (backup->uncompressed_bytes >= 0)
    {
        json_add_key(buf, "uncompressed-bytes", json_level);
        appendPQExpBuffer(buf, INT64_FORMAT, backup->uncompressed_bytes);
    }

    if (backup->uncompressed_bytes >= 0)
    {
        json_add_key(buf, "pgdata-bytes", json_level);
        appendPQExpBuffer(buf, INT64_FORMAT, backup->pgdata_bytes);
    }

    if (backup->uncompressed_bytes >= 0)
    {
        json_add_key(buf, "dssdata-bytes", json_level);
        appendPQExpBuffer(buf, INT64_FORMAT, backup->dssdata_bytes);
    }

    if (backup->external_dir_str)
        json_add_value(buf, "external-dirs", backup->external_dir_str,
                        json_level, true);

    json_add_value(buf, "status", status2str(backup->status), json_level,
                    true);

    if (backup->note)
        json_add_value(buf, "note", backup->note,
                    json_level, true);

    if (backup->content_crc != 0)
    {
        json_add_key(buf, "content-crc", json_level);
        appendPQExpBuffer(buf, "%u", backup->content_crc);
    }

    json_add(buf, JT_END_OBJECT, &json_level);
}

/*
 * Show detailed meta information about specified backup.
 */
static int
show_backup(const char *instance_name, time_t requested_backup_id)
{
    size_t i;
    pgBackup   *backup = NULL;
    parray       *backups;

    backups = catalog_get_backup_list(instance_name, INVALID_BACKUP_ID);

    /* Find requested backup */
    for (i = 0; i < parray_num(backups); i++)
    {
        pgBackup   *tmp_backup = (pgBackup *) parray_get(backups, i);

        /* found target */
        if (tmp_backup->start_time == requested_backup_id)
        {
            backup = tmp_backup;
            break;
        }
    }

    if (backup == NULL)
    {
        // TODO for 3.0: we should ERROR out here.
        elog(INFO, "Requested backup \"%s\" is not found.",
             /* We do not need free base36enc's result, we exit anyway */
             base36enc(requested_backup_id));
        /* This is not error */
        parray_walk(backups, pgBackupFree);
        parray_free(backups);
        return 0;
    }

    if (show_format == SHOW_PLAIN)
        pgBackupWriteControl(stdout, backup);
    else
        elog(ERROR, "Invalid show format %d", (int) show_format);

    /* cleanup */
    parray_walk(backups, pgBackupFree);
    parray_free(backups);

    return 0;
}

static void process_time(pgBackup *backup, ShowBackendRow *row)
{
    if (backup->status == BACKUP_STATUS_RUNNING)
        pretty_time_interval(difftime(current_time, backup->start_time),
                        row->duration, lengthof(row->duration));
    else if (backup->merge_time != (time_t) 0)
        pretty_time_interval(difftime(backup->end_time, backup->merge_time),
                        row->duration, lengthof(row->duration));
    else if (backup->end_time != (time_t) 0)
        pretty_time_interval(difftime(backup->end_time, backup->start_time),
                        row->duration, lengthof(row->duration));
    else
        strncpy_s(row->duration, sizeof(row->duration), "----", sizeof(row->duration));
}

/*
 * Show instance backups in plain format.
 */
static void
show_instance_plain(const char *instance_name, device_type_t instance_type,  parray *backup_list, bool show_name)
{
#define SHOW_FIELDS_COUNT 16
    int            i;
    const char *names[SHOW_FIELDS_COUNT] =
                    { "Instance", "Version", "ID", "Recovery Time",
                      "Mode", "WAL Mode", "TLI", "Time", "Data", "WAL",
                      "Zratio", "Start LSN", "Stop LSN", "Type", "S3 Status", "Status" };
    const char *field_formats[SHOW_FIELDS_COUNT] =
                    { " %-*s ", " %-*s ", " %-*s ", " %-*s ",
                      " %-*s ", " %-*s ", " %-*s ", " %*s ", " %*s ", " %*s ",
                      " %*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s "};
    uint32        widths[SHOW_FIELDS_COUNT];
    uint32        widths_sum = 0;
    ShowBackendRow *rows = NULL;
    TimeLineID parent_tli = 0;
    int64 disk_available_bytes = 0;
    struct statfs diskInfo = {0};

    for (i = 0; i < SHOW_FIELDS_COUNT; i++)
        widths[i] = strlen(names[i]);

    rows = (ShowBackendRow *) palloc(parray_num(backup_list) *
                                     sizeof(ShowBackendRow));
    if (rows == NULL) {
        elog(ERROR, "Out of memory");
    }

    /*
     * Fill row values and calculate maximum width of each field.
     */
    for (i = 0; (size_t)i < parray_num(backup_list); i++)
    {
        pgBackup   *backup = (pgBackup *)parray_get(backup_list, i);
        ShowBackendRow *row = &rows[i];
        int            cur = 0;
        float        zratio = 1;

        /* Instance */
        row->instance = instance_name;
        widths[cur] = Max(widths[cur], strlen(row->instance));
        cur++;

        /* Version */
        row->version = backup->server_version[0] ?
            backup->server_version : "----";
        widths[cur] = Max(widths[cur], strlen(row->version));
        cur++;

        /* ID */
        errno_t rc = snprintf_s(row->backup_id, lengthof(row->backup_id), lengthof(row->backup_id) - 1, "%s",
                base36enc(backup->start_time));
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->backup_id));
        cur++;

        /* Recovery Time */
        if (backup->recovery_time != (time_t) 0)
            time2iso(row->recovery_time, lengthof(row->recovery_time),
                     backup->recovery_time);
        else {
            rc = strncpy_s(row->recovery_time, sizeof(row->recovery_time), "----", sizeof(row->recovery_time) - 1);
            securec_check_c(rc, "\0", "\0");
        }
        widths[cur] = Max(widths[cur], strlen(row->recovery_time));
        cur++;

        /* Mode */
        row->mode = pgBackupGetBackupMode(backup);
        widths[cur] = Max(widths[cur], strlen(row->mode));
        cur++;

        /* WAL mode*/
        row->wal_mode = backup->stream ? "STREAM": "ARCHIVE";
        widths[cur] = Max(widths[cur], strlen(row->wal_mode));
        cur++;

        /* Current/Parent TLI */
        if (backup->parent_backup_link != NULL)
            parent_tli = backup->parent_backup_link->tli;

        rc = snprintf_s(row->tli, lengthof(row->tli), lengthof(row->tli) - 1, "%u/%u",
                        backup->tli,
                        backup->backup_mode == BACKUP_MODE_FULL ? 0 : parent_tli);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->tli));
        cur++;

        /* Time */
        process_time(backup, row);
        widths[cur] = Max(widths[cur], strlen(row->duration));
        cur++;

        /* Data */
        pretty_size(backup->data_bytes, row->data_bytes,
                    lengthof(row->data_bytes));
        widths[cur] = Max(widths[cur], strlen(row->data_bytes));
        cur++;

        /* WAL */
        pretty_size(backup->wal_bytes, row->wal_bytes,
                    lengthof(row->wal_bytes));
        widths[cur] = Max(widths[cur], strlen(row->wal_bytes));
        cur++;

        /* Zratio (compression ratio) */
        if (backup->uncompressed_bytes != BYTES_INVALID &&
            (backup->uncompressed_bytes > 0 && backup->data_bytes > 0))
        {
            zratio = (float)backup->uncompressed_bytes / (backup->data_bytes);
            rc = snprintf_s(row->zratio, lengthof(row->zratio), lengthof(row->zratio) - 1, "%.2f", zratio);
            securec_check_ss_c(rc, "\0", "\0");
        }
        else {
            rc = snprintf_s(row->zratio, lengthof(row->zratio), lengthof(row->zratio) - 1, "%.2f", zratio);
            securec_check_ss_c(rc, "\0", "\0");
        }

        widths[cur] = Max(widths[cur], strlen(row->zratio));
        cur++;

        /* Start LSN */
        rc = snprintf_s(row->start_lsn, lengthof(row->start_lsn), lengthof(row->start_lsn) - 1,
                        "%X/%X",
                        (uint32) (backup->start_lsn >> 32),
                        (uint32) backup->start_lsn);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->start_lsn));
        cur++;

        /* Stop LSN */
        rc = snprintf_s(row->stop_lsn, lengthof(row->stop_lsn), lengthof(row->stop_lsn) - 1,
                        "%X/%X",
                        (uint32) (backup->stop_lsn >> 32),
                        (uint32) backup->stop_lsn);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->stop_lsn));
        cur++;

        /* Type (FILE OR DSS) */
        rc = snprintf_s(row->type, lengthof(row->type), lengthof(row->type) - 1, "%s", dev2str(instance_type));
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], (uint32)strlen(row->type));
        cur++;

        /* S3 Status (LOCAL OR S3) */
        rc = snprintf_s(row->oss_status, lengthof(row->oss_status), lengthof(row->oss_status) - 1, "%s", ossStatus2str(backup->oss_status));
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], (uint32)strlen(row->oss_status));
        cur++;

        /* Status */
        int staRet = statfs(backup->root_dir, &diskInfo);
        if (staRet < 0) {
            elog(ERROR, "Get disk free space failed!");
        }
        disk_available_bytes = (int64)diskInfo.f_bavail * diskInfo.f_bsize;
        int ret = system("ps ux | grep gs_probackup | grep -v grep | grep -v show");
        if ((ret != 0) && (backup->status == BACKUP_STATUS_RUNNING)) {
            if (disk_available_bytes <= MIN_SPACE) {
                elog(WARNING, "Label is abnormal, and insufficient memory to modify file status,",
                    "please clear disk space");
            } else {
                backup->status = BACKUP_STATUS_ERROR;
                write_backup(backup, true);
            }
        }
        row->status = status2str(backup->status);
        widths[cur] = Max(widths[cur], strlen(row->status));
    }

    for (i = 0; i < SHOW_FIELDS_COUNT; i++)
        widths_sum += widths[i] + 2 /* two space */;

    if (show_name)
        appendPQExpBuffer(&show_buf, "\nBACKUP INSTANCE '%s'\n", instance_name);

    /*
     * Print header.
     */
    for (i = 0; (uint32)i < widths_sum; i++)
        appendPQExpBufferChar(&show_buf, '=');
    appendPQExpBufferChar(&show_buf, '\n');

    for (i = 0; i < SHOW_FIELDS_COUNT; i++)
    {
        appendPQExpBuffer(&show_buf, field_formats[i], widths[i], names[i]);
    }
    appendPQExpBufferChar(&show_buf, '\n');

    for (i = 0; (uint32)i < widths_sum; i++)
        appendPQExpBufferChar(&show_buf, '=');
    appendPQExpBufferChar(&show_buf, '\n');

    /*
     * Print values.
     */
    for (i = 0; (size_t)i < parray_num(backup_list); i++)
    {
        ShowBackendRow *row = &rows[i];
        int            cur = 0;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->instance);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->version);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->backup_id);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->recovery_time);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->mode);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->wal_mode);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->tli);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->duration);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->data_bytes);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->wal_bytes);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->zratio);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->start_lsn);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->stop_lsn);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->type);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->oss_status);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->status);
        cur++;

        appendPQExpBufferChar(&show_buf, '\n');
    }

    pfree(rows);
}

/*
 * Show instance backups in json format.
 */
static void
show_instance_json(const char *instance_name, parray *backup_list)
{
    int            i;
    PQExpBuffer    buf = &show_buf;

    if (!first_instance)
        appendPQExpBufferChar(buf, ',');

    /* Begin of instance object */
    json_add(buf, JT_BEGIN_OBJECT, &json_level);

    json_add_value(buf, "instance", instance_name, json_level, true);
    json_add_key(buf, "backups", json_level);

    /*
     * List backups.
     */
    json_add(buf, JT_BEGIN_ARRAY, &json_level);

    for (i = 0; (size_t)i < parray_num(backup_list); i++)
    {
        pgBackup   *backup = (pgBackup *)parray_get(backup_list, i);

        if (i != 0)
            appendPQExpBufferChar(buf, ',');

        print_backup_json_object(buf, backup);
    }

    /* End of backups */
    json_add(buf, JT_END_ARRAY, &json_level);

    /* End of instance object */
    json_add(buf, JT_END_OBJECT, &json_level);

    first_instance = false;
}

/*
 * show information about WAL archive of the instance
 */
static void
show_instance_archive(InstanceConfig *instance)
{
    parray *timelineinfos = nullptr;

    if (!instance) {
        return;
    }

    timelineinfos = catalog_get_timelines(instance);

    if (show_format == SHOW_PLAIN)
        show_archive_plain(instance->name, instance->xlog_seg_size, timelineinfos, true);
    else if (show_format == SHOW_JSON)
        show_archive_json(instance->name, instance->xlog_seg_size, timelineinfos);
    else
        elog(ERROR, "Invalid show format %d", (int) show_format);

    parray_walk(timelineinfos, timelineInfoFree);
    parray_free(timelineinfos);
}

static void
show_archive_plain(const char *instance_name, uint32 xlog_seg_size,
                   parray *tli_list, bool show_name)
{
    char segno_tmp[MAXFNAMELEN];
    parray *actual_tli_list = parray_new();
#define SHOW_ARCHIVE_FIELDS_COUNT 10
    int            i;
    const char *names[SHOW_ARCHIVE_FIELDS_COUNT] =
                    { "TLI", "Parent TLI", "Switchpoint",
                      "Min Segno", "Max Segno", "N segments", "Size", "Zratio", "N backups", "Status"};
    const char *field_formats[SHOW_ARCHIVE_FIELDS_COUNT] =
                    { " %-*s ", " %-*s ", " %-*s ", " %-*s ",
                      " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s "};
    uint32        widths[SHOW_ARCHIVE_FIELDS_COUNT];
    uint32        widths_sum = 0;
    ShowArchiveRow *rows;

    for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
        widths[i] = strlen(names[i]);

    /* Ignore empty timelines */
    for (i = 0; (size_t)i < parray_num(tli_list); i++)
    {
        timelineInfo *tlinfo = (timelineInfo *) parray_get(tli_list, i);

        if (tlinfo->n_xlog_files > 0)
            parray_append(actual_tli_list, tlinfo);
    }

    rows = (ShowArchiveRow *) gs_palloc0(parray_num(actual_tli_list) *
                                     sizeof(ShowArchiveRow));

    /*
     * Fill row values and calculate maximum width of each field.
     */
    for (i = 0; (size_t)i < parray_num(actual_tli_list); i++)
    {
        timelineInfo *tlinfo = (timelineInfo *) parray_get(actual_tli_list, i);
        ShowArchiveRow *row = &rows[i];
        int            cur = 0;
        float        zratio = 0;

        /* TLI */
        errno_t rc = snprintf_s(row->tli, lengthof(row->tli), lengthof(row->tli) - 1, "%u",
                 tlinfo->tli);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->tli));
        cur++;

        /* Parent TLI */
        rc = snprintf_s(row->parent_tli, lengthof(row->parent_tli), lengthof(row->parent_tli) - 1,
                 "%u",
                 tlinfo->parent_tli);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->parent_tli));
        cur++;

        /* Switchpoint LSN */
        rc = snprintf_s(row->switchpoint, lengthof(row->switchpoint),
                 lengthof(row->switchpoint) - 1,
                 "%X/%X",
                 (uint32) (tlinfo->switchpoint >> 32),
                 (uint32) tlinfo->switchpoint);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->switchpoint));
        cur++;

        /* Min Segno */
        GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, tlinfo->begin_segno, xlog_seg_size);
        rc = snprintf_s(row->min_segno, lengthof(row->min_segno), lengthof(row->min_segno) - 1, "%s",
                   segno_tmp);
        securec_check_ss_c(rc, "\0", "\0");

        widths[cur] = Max(widths[cur], strlen(row->min_segno));
        cur++;

        /* Max Segno */
        GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, tlinfo->end_segno, xlog_seg_size);
        rc = snprintf_s(row->max_segno, lengthof(row->max_segno), lengthof(row->max_segno) - 1, "%s",
                   segno_tmp);
        securec_check_ss_c(rc, "\0", "\0");

        widths[cur] = Max(widths[cur], strlen(row->max_segno));
        cur++;

        /* N files */
        rc = snprintf_s(row->n_segments, lengthof(row->n_segments), lengthof(row->n_segments) - 1, "%lu",
                 tlinfo->n_xlog_files);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->n_segments));
        cur++;

        /* Size */
        pretty_size(tlinfo->size, row->size,
                    lengthof(row->size));
        widths[cur] = Max(widths[cur], strlen(row->size));
        cur++;

        /* Zratio (compression ratio) */
        if (tlinfo->size != 0)
            zratio = ((float)xlog_seg_size*tlinfo->n_xlog_files) / tlinfo->size;

        rc = snprintf_s(row->zratio, lengthof(row->zratio), lengthof(row->zratio) - 1, "%.2f", zratio);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->zratio));
        cur++;

        /* N backups */
        rc = snprintf_s(row->n_backups, lengthof(row->n_backups), lengthof(row->n_backups) - 1, "%lu",
                 tlinfo->backups?parray_num(tlinfo->backups):0);
        securec_check_ss_c(rc, "\0", "\0");
        widths[cur] = Max(widths[cur], strlen(row->n_backups));
        cur++;

        /* Status */
        if (tlinfo->lost_segments == NULL)
            row->status = "OK";
        else
            row->status = "DEGRADED";
        widths[cur] = Max(widths[cur], strlen(row->status));
        cur++;
    }

    for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
        widths_sum += widths[i] + 2 /* two space */;

    if (show_name)
        appendPQExpBuffer(&show_buf, "\nARCHIVE INSTANCE '%s'\n", instance_name);

    /*
     * Print header.
     */
    for (i = 0; (uint32)i < widths_sum; i++)
        appendPQExpBufferChar(&show_buf, '=');
    appendPQExpBufferChar(&show_buf, '\n');

    for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
    {
        appendPQExpBuffer(&show_buf, field_formats[i], widths[i], names[i]);
    }
    appendPQExpBufferChar(&show_buf, '\n');

    for (i = 0; (uint32)i < widths_sum; i++)
        appendPQExpBufferChar(&show_buf, '=');
    appendPQExpBufferChar(&show_buf, '\n');

    /*
     * Print values.
     */
    for (i = parray_num(actual_tli_list) - 1; i >= 0; i--)
    {
        ShowArchiveRow *row = &rows[i];
        int            cur = 0;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->tli);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->parent_tli);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->switchpoint);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->min_segno);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->max_segno);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->n_segments);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->size);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->zratio);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->n_backups);
        cur++;

        appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
                          row->status);
        cur++;
        appendPQExpBufferChar(&show_buf, '\n');
    }

    pfree(rows);
    //TODO: free timelines
    parray_free(actual_tli_list);
}

static void
show_archive_json(const char *instance_name, uint32 xlog_seg_size,
                  parray *tli_list)
{
    int            i,j;
    PQExpBuffer    buf = &show_buf;
    parray *actual_tli_list = parray_new();
    char segno_tmp[MAXFNAMELEN];

    if (!first_instance)
        appendPQExpBufferChar(buf, ',');

    /* Begin of instance object */
    json_add(buf, JT_BEGIN_OBJECT, &json_level);

    json_add_value(buf, "instance", instance_name, json_level, true);
    json_add_key(buf, "timelines", json_level);

    /* Ignore empty timelines */

    for (i = 0; (size_t)i < parray_num(tli_list); i++)
    {
        timelineInfo *tlinfo = (timelineInfo *) parray_get(tli_list, i);

        if (tlinfo->n_xlog_files > 0)
            parray_append(actual_tli_list, tlinfo);
    }

    /*
     * List timelines.
     */
    json_add(buf, JT_BEGIN_ARRAY, &json_level);

    for (i = parray_num(actual_tli_list) - 1; i >= 0; i--)
    {
        timelineInfo  *tlinfo = (timelineInfo  *) parray_get(actual_tli_list, i);
        char        tmp_buf[MAXFNAMELEN];
        float        zratio = 0;

        if ((size_t)i != (parray_num(actual_tli_list) - 1))
            appendPQExpBufferChar(buf, ',');

        json_add(buf, JT_BEGIN_OBJECT, &json_level);

        json_add_key(buf, "tli", json_level);
        appendPQExpBuffer(buf, "%u", tlinfo->tli);

        json_add_key(buf, "parent-tli", json_level);
        appendPQExpBuffer(buf, "%u", tlinfo->parent_tli);

        errno_t rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%X/%X",
                 (uint32) (tlinfo->switchpoint >> 32), (uint32) tlinfo->switchpoint);
        securec_check_ss_c(rc, "\0", "\0");
        json_add_value(buf, "switchpoint", tmp_buf, json_level, true);

        GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, tlinfo->begin_segno, xlog_seg_size);
        rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s", segno_tmp);
        securec_check_ss_c(rc, "\0", "\0");
        json_add_value(buf, "min-segno", tmp_buf, json_level, true);

        GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, tlinfo->end_segno, xlog_seg_size);
        rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s", segno_tmp);
        securec_check_ss_c(rc, "\0", "\0");
        json_add_value(buf, "max-segno", tmp_buf, json_level, true);

        json_add_key(buf, "n-segments", json_level);
        appendPQExpBuffer(buf, "%lu", tlinfo->n_xlog_files);

        json_add_key(buf, "size", json_level);
        appendPQExpBuffer(buf, "%lu", tlinfo->size);

        json_add_key(buf, "zratio", json_level);
        if (tlinfo->size != 0)
            zratio = ((float)xlog_seg_size*tlinfo->n_xlog_files) / tlinfo->size;
        appendPQExpBuffer(buf, "%.2f", zratio);

        if (tlinfo->closest_backup != NULL) {
            rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s",
                        base36enc(tlinfo->closest_backup->start_time));
            securec_check_ss_c(rc, "\0", "\0");
        }
        else {
            rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s", "");
            securec_check_ss_c(rc, "\0", "\0");
        }

        json_add_value(buf, "closest-backup-id", tmp_buf, json_level, true);

        if (tlinfo->lost_segments == NULL)
            json_add_value(buf, "status", "OK", json_level, true);
        else
            json_add_value(buf, "status", "DEGRADED", json_level, true);

        json_add_key(buf, "lost-segments", json_level);

        if (tlinfo->lost_segments != NULL)
        {
            json_add(buf, JT_BEGIN_ARRAY, &json_level);

            for (j = 0; (size_t)j < parray_num(tlinfo->lost_segments); j++)
            {
                xlogInterval *lost_segments = (xlogInterval *) parray_get(tlinfo->lost_segments, j);

                if (j != 0)
                    appendPQExpBufferChar(buf, ',');

                json_add(buf, JT_BEGIN_OBJECT, &json_level);

                GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, lost_segments->begin_segno, xlog_seg_size);
                rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s", segno_tmp);
                securec_check_ss_c(rc, "\0", "\0");
                json_add_value(buf, "begin-segno", tmp_buf, json_level, true);

                GetXLogFileName(segno_tmp, MAXFNAMELEN, tlinfo->tli, lost_segments->end_segno, xlog_seg_size);
                rc = snprintf_s(tmp_buf, lengthof(tmp_buf), lengthof(tmp_buf) - 1, "%s", segno_tmp);
                securec_check_ss_c(rc, "\0", "\0");
                json_add_value(buf, "end-segno", tmp_buf, json_level, true);
                json_add(buf, JT_END_OBJECT, &json_level);
            }
            json_add(buf, JT_END_ARRAY, &json_level);
        }
        else
            appendPQExpBuffer(buf, "[]");

        json_add_key(buf, "backups", json_level);

        if (tlinfo->backups != NULL)
        {
            json_add(buf, JT_BEGIN_ARRAY, &json_level);
            for (j = 0; (size_t)j < parray_num(tlinfo->backups); j++)
            {
                pgBackup *backup = (pgBackup *)parray_get(tlinfo->backups, j);

                if (j != 0)
                    appendPQExpBufferChar(buf, ',');

                print_backup_json_object(buf, backup);
            }

            json_add(buf, JT_END_ARRAY, &json_level);
        }
        else
            appendPQExpBuffer(buf, "[]");

        /* End of timeline */
        json_add(buf, JT_END_OBJECT, &json_level);
    }

    /* End of timelines object */
    json_add(buf, JT_END_ARRAY, &json_level);

    /* End of instance object */
    json_add(buf, JT_END_OBJECT, &json_level);

    first_instance = false;
}
