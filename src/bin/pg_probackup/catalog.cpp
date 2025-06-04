/*-------------------------------------------------------------------------
 *
 * catalog.c: backup catalog operation
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"


#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "file.h"
#include "configuration.h"
#include "common/fe_memutils.h"
#include "oss/include/restore.h"
#include "oss/include/oss_operator.h"
#include "common_cipher.h"

static pgBackup* get_closest_backup(timelineInfo *tlinfo);
static pgBackup* get_oldest_backup(timelineInfo *tlinfo);
static const char *backupModes[] = {"", "PTRACK", "FULL"};
static pgBackup *readBackupControlFile(const char *path);

static bool exit_hook_registered = false;
static parray *lock_files = NULL;

static void uncompress_decrypt_directory(const char *instance_name_str);

static timelineInfo *
timelineInfoNew(TimeLineID tli)
{
    timelineInfo *tlinfo = (timelineInfo *) pgut_malloc(sizeof(timelineInfo));
    errno_t rc = 0;
    rc = memset_s(tlinfo, sizeof(timelineInfo),0, sizeof(timelineInfo));
    securec_check(rc, "\0", "\0");
    tlinfo->tli = tli;
    tlinfo->switchpoint = InvalidXLogRecPtr;
    tlinfo->parent_link = NULL;
    tlinfo->xlog_filelist = parray_new();
    tlinfo->anchor_lsn = InvalidXLogRecPtr;
    tlinfo->anchor_tli = 0;
    tlinfo->n_xlog_files = 0;
    return tlinfo;
}

/* free timelineInfo object */
void
timelineInfoFree(void *tliInfo)
{
    timelineInfo *tli = (timelineInfo *) tliInfo;

    parray_walk(tli->xlog_filelist, pgFileFree);
    parray_free(tli->xlog_filelist);

    if (tli->backups)
    {
        parray_walk(tli->backups, pgBackupFree);
        parray_free(tli->backups);
    }

    pfree(tliInfo);
}

/* Iterate over locked backups and delete locks files */
void
unlink_lock_atexit(bool fatal, void *userdata)
{
    int i;

    if (lock_files == NULL)
    return;

    for (i = 0; i < (int)parray_num(lock_files); i++)
    {
        char    *lock_file = (char *) parray_get(lock_files, i);
        int res;

        res = fio_unlink(lock_file, FIO_BACKUP_HOST);
        if (res != 0 && errno != ENOENT)
            elog(WARNING, "%s: %s", lock_file, strerror(errno));
    }

    parray_walk(lock_files, pfree);
    parray_free(lock_files);
    lock_files = NULL;
}

/*
 * Read backup meta information from BACKUP_CONTROL_FILE.
 * If no backup matches, return NULL.
 */
pgBackup *
read_backup(const char *root_dir)
{
    char    conf_path[MAXPGPATH];

    join_path_components(conf_path, root_dir, BACKUP_CONTROL_FILE);

    return readBackupControlFile(conf_path);
}

/*
 * Save the backup status into BACKUP_CONTROL_FILE.
 *
 * We need to reread the backup using its ID and save it changing only its
 * status.
 */
void
write_backup_status(pgBackup *backup, BackupStatus status,
                            const char *instance_name, bool strict)
{
    pgBackup   *tmp;

    tmp = read_backup(backup->root_dir);
    if (!tmp)
    {
        /*
         * Silently exit the function, since read_backup already logged the
         * warning message.
         */
        return;
    }

    backup->status = status;
    tmp->status = backup->status;
    tmp->root_dir = pgut_strdup(backup->root_dir);

    write_backup(tmp, strict);

    pgBackupFree(tmp);
}

bool fill_file(char *buffer, char *lock_file, int fd, bool strict)
{
    errno = 0;
    if (fio_write(fd, buffer, strlen(buffer)) != (int)strlen(buffer))
    {
        int     save_errno = errno;

        fio_close(fd);
        fio_unlink(lock_file, FIO_BACKUP_HOST);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;

        /* In lax mode if we failed to grab lock because of 'out of space error',
         * then treat backup as locked.
         * Only delete command should be run in lax mode.
         */
        if (!strict && errno == ENOSPC)
            return true;

        elog(ERROR, "Could not write lock file \"%s\": %s",
            lock_file, strerror(errno));
    }
    if (fio_flush(fd) != 0)
    {
        int     save_errno = errno;

        fio_close(fd);
        fio_unlink(lock_file, FIO_BACKUP_HOST);
        errno = save_errno;
        elog(ERROR, "Could not write lock file \"%s\": %s",
            lock_file, strerror(errno));
    }
    if (fio_close(fd) != 0)
    {
        int     save_errno = errno;

        fio_unlink(lock_file, FIO_BACKUP_HOST);
        errno = save_errno;
        elog(ERROR, "Could not write lock file \"%s\": %s",
            lock_file, strerror(errno));
    }

    return false;

}

/*
 * Create exclusive lockfile in the backup's directory.
 */
bool
lock_backup(pgBackup *backup, bool strict)
{
    char    lock_file[MAXPGPATH];
    int fd;
    char    buffer[MAXPGPATH * 2 + 256];
    int ntries;
    int len;
    int     encoded_pid;
    pid_t   my_pid,
                my_p_pid;
    int nRet = 0;
    
    join_path_components(lock_file, backup->root_dir, BACKUP_CATALOG_PID);

    /*
     * If the PID in the lockfile is our own PID or our parent's or
     * grandparent's PID, then the file must be stale (probably left over from
     * a previous system boot cycle).  We need to check this because of the
     * likelihood that a reboot will assign exactly the same PID as we had in
     * the previous reboot, or one that's only one or two counts larger and
     * hence the lockfile's PID now refers to an ancestor shell process.  We
     * allow pg_ctl to pass down its parent shell PID (our grandparent PID)
     * via the environment variable PG_GRANDPARENT_PID; this is so that
     * launching the postmaster via pg_ctl can be just as reliable as
     * launching it directly.  There is no provision for detecting
     * further-removed ancestor processes, but if the init script is written
     * carefully then all but the immediate parent shell will be root-owned
     * processes and so the kill test will fail with EPERM.  Note that we
     * cannot get a false negative this way, because an existing postmaster
     * would surely never launch a competing postmaster or pg_ctl process
     * directly.
     */
    my_pid = getpid();
#ifndef WIN32
    my_p_pid = getppid();
#else

    /*
     * Windows hasn't got getppid(), but doesn't need it since it's not using
     * real kill() either...
     */
    my_p_pid = 0;
#endif

    /*
     * We need a loop here because of race conditions.  But don't loop forever
     * (for example, a non-writable $backup_instance_path directory might cause a failure
     * that won't go away).  100 tries seems like plenty.
     */
    for (ntries = 0;; ntries++)
    {
        bool is_create = false;
        /*
         * Try to create the lock file --- O_EXCL makes this atomic.
         *
         * Think not to make the file protection weaker than 0600.  See
         * comments below.
         */
        fd = fio_open(lock_file, O_RDWR | O_CREAT | O_EXCL, FIO_BACKUP_HOST);
        if (fd >= 0)
            break;      /* Success; exit the retry loop */

        /*
         * Couldn't create the pid file. Probably it already exists.
         */
        is_create = (errno != EEXIST && errno != EACCES) || ntries > 100;
        if (is_create)
            elog(ERROR, "Could not create lock file \"%s\": %s",
                lock_file, strerror(errno));

        /*
         * Read the file to get the old owner's PID.  Note race condition
         * here: file might have been deleted since we tried to create it.
         */
        fd = fio_open(lock_file, O_RDONLY, FIO_BACKUP_HOST);
        if (fd < 0)
        {
            if (errno == ENOENT)
                continue;   /* race condition; try again */
            elog(ERROR, "Could not open lock file \"%s\": %s",
                lock_file, strerror(errno));
        }
        if ((len = fio_read(fd, buffer, sizeof(buffer) - 1)) < 0)
            elog(ERROR, "Could not read lock file \"%s\": %s",
                lock_file, strerror(errno));
        fio_close(fd);

        if (len == 0)
            elog(ERROR, "Lock file \"%s\" is empty", lock_file);

        buffer[len] = '\0';
        encoded_pid = atoi(buffer);

        if (encoded_pid <= 0)
            elog(ERROR, "Bogus data in lock file \"%s\": \"%s\"",
                lock_file, buffer);

        /*
         * Check to see if the other process still exists
         *
         * Per discussion above, my_pid, my_p_pid can be
         * ignored as false matches.
         *
         * Normally kill() will fail with ESRCH if the given PID doesn't
         * exist.
         */
        if (encoded_pid != my_pid && encoded_pid != my_p_pid)
        {
            if (kill(encoded_pid, 0) == 0)
            {
                elog(WARNING, "Process %d is using backup %s and still is running",
                    encoded_pid, base36enc(backup->start_time));
                return false;
            }
            else
            {
                if (errno == ESRCH)
                    elog(WARNING, "Process %d which used backup %s no longer exists",
                         encoded_pid, base36enc(backup->start_time));
                else
                    elog(WARNING, "Failed to send signal 0 to a process %d, this may be a kernal process",
                        encoded_pid);
            }
        }

        /*
         * Looks like nobody's home.  Unlink the file and try again to create
         * it.  Need a loop because of possible race condition against other
         * would-be creators.
         */
        if (fio_unlink(lock_file, FIO_BACKUP_HOST) < 0)
            elog(ERROR, "Could not remove old lock file \"%s\": %s",
                 lock_file, strerror(errno));
    }

    /*
     * Successfully created the file, now fill it.
     */
    nRet = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1,"%d\n", my_pid);
    securec_check_ss_c(nRet, "\0", "\0");

    if (fill_file(buffer, lock_file, fd, strict))
        return true;

    /*
    * Arrange to unlink the lock file(s) at proc_exit.
    */
    if (!exit_hook_registered)
    {
        exit_hook_registered = true;
    }

    /* Use parray so that the lock files are unlinked in a loop */
    if (lock_files == NULL)
        lock_files = parray_new();
    parray_append(lock_files, pgut_strdup(lock_file));

    return true;
}

/*
 * Get backup_mode in string representation.
 */
const char *
pgBackupGetBackupMode(pgBackup *backup)
{
    return backupModes[backup->backup_mode];
}

static bool
IsDir(const char *dirpath, const char *entry, fio_location location)
{
    char    path[MAXPGPATH];
    struct stat	st;
    int nRet = 0;

    nRet = snprintf_s(path, MAXPGPATH,MAXPGPATH - 1, "%s/%s", dirpath, entry);
    securec_check_ss_c(nRet, "\0", "\0");

    return fio_stat(path, &st, false, location) == 0 && S_ISDIR(st.st_mode);
}

/*
 * Create list of instances in given backup catalog.
 *
 * Returns parray of "InstanceConfig" structures, filled with
 * actual config of each instance.
 */
parray *
catalog_get_instance_list(void)
{
    char    path[MAXPGPATH];
    DIR     *dir;
    struct dirent *dent;
    parray   *instances;

    instances = parray_new();

    /* open directory and list contents */
    join_path_components(path, backup_path, BACKUPS_DIR);
    dir = opendir(path);
    if (dir == NULL)
        elog(ERROR, "Cannot open directory \"%s\": %s",
            path, strerror(errno));

    while (errno = 0, (dent = readdir(dir)) != NULL)
    {
        char    child[MAXPGPATH];
        struct stat st;
        InstanceConfig *instance;

        /* skip entries point current dir or parent dir */
        if (strcmp(dent->d_name, ".") == 0 ||
            strcmp(dent->d_name, "..") == 0)
            continue;

        join_path_components(child, path, dent->d_name);

        if (lstat(child, &st) == -1)
            elog(ERROR, "Cannot stat file \"%s\": %s",
                child, strerror(errno));

        if (!S_ISDIR(st.st_mode))
            continue;

        instance = readInstanceConfigFile(dent->d_name);
        if (instance != NULL) {
            parray_append(instances, instance);
        }
    }

    /* TODO 3.0: switch to ERROR */
    if (parray_num(instances) == 0)
        elog(WARNING, "This backup catalog contains no backup instances. Backup instance can be added via 'add-instance' command.");

    if (errno)
        elog(ERROR, "Cannot read directory \"%s\": %s",
            path, strerror(errno));

    if (closedir(dir))
        elog(ERROR, "Cannot close directory \"%s\": %s",
            path, strerror(errno));

    return instances;
}

/*
 * Create list of backups.
 * If 'requested_backup_id' is INVALID_BACKUP_ID, return list of all backups.
 * The list is sorted in order of descending start time.
 * If valid backup id is passed only matching backup will be added to the list.
 */
parray *
catalog_get_backup_list(const char *instance_name, time_t requested_backup_id)
{
    DIR *data_dir = NULL;
    struct dirent *data_ent = NULL;
    parray  *backups = NULL;
    int     i;
    char backup_instance_path[MAXPGPATH];
    int nRet = 0;

    uncompress_decrypt_directory(instance_name);

    nRet = snprintf_s(backup_instance_path,MAXPGPATH,MAXPGPATH - 1, "%s/%s/%s",
                            backup_path, BACKUPS_DIR, instance_name);
    securec_check_ss_c(nRet, "\0", "\0");

    /* open backup instance backups directory */
    data_dir = fio_opendir(backup_instance_path, FIO_BACKUP_HOST);
    if (data_dir == NULL)
    {
        elog(WARNING, "cannot open directory \"%s\": %s", backup_instance_path,
                strerror(errno));
        goto err_proc;
    }

    /* scan the directory and list backups */
    backups = parray_new();
    for (; (data_ent = fio_readdir(data_dir)) != NULL; errno = 0)
    {
        char    backup_conf_path[MAXPGPATH];
        char    data_path[MAXPGPATH];
        pgBackup   *backup = NULL;

        /* skip not-directory entries and hidden entries */
        if (!IsDir(backup_instance_path, data_ent->d_name, FIO_BACKUP_HOST)
            || data_ent->d_name[0] == '.')
            continue;

        /* open subdirectory of specific backup */
        join_path_components(data_path, backup_instance_path, data_ent->d_name);

        /* read backup information from BACKUP_CONTROL_FILE */
        nRet = snprintf_s(backup_conf_path, MAXPGPATH, MAXPGPATH - 1,"%s/%s", data_path, BACKUP_CONTROL_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        backup = readBackupControlFile(backup_conf_path);

        if (!backup)
        {
            backup = pgut_new(pgBackup);
            pgBackupInit(backup);
            backup->start_time = base36dec(data_ent->d_name);
        }
        else if (strcmp(base36enc(backup->start_time), data_ent->d_name) != 0)
        {
            elog(WARNING, "backup ID in control file \"%s\" doesn't match name of the backup folder \"%s\"",
                base36enc(backup->start_time), backup_conf_path);
        }

        backup->root_dir = pgut_strdup(data_path);

        backup->database_dir = (char *)pgut_malloc(MAXPGPATH);
        join_path_components(backup->database_dir, backup->root_dir, DATABASE_DIR);

        /* Initialize page header map */
        init_header_map(backup);

        /* TODO: save encoded backup id */
        backup->backup_id = backup->start_time;
        if (requested_backup_id != INVALID_BACKUP_ID
            && requested_backup_id != backup->start_time)
        {
            pgBackupFree(backup);
            continue;
        }
        parray_append(backups, backup);

        if (errno && errno != ENOENT)
        {
            elog(WARNING, "cannot read data directory \"%s\": %s",
                data_ent->d_name, strerror(errno));
            goto err_proc;
        }
    }
    if (errno)
    {
        elog(WARNING, "cannot read backup root directory \"%s\": %s",
            backup_instance_path, strerror(errno));
        goto err_proc;
    }

    fio_closedir(data_dir);
    data_dir = NULL;

    parray_qsort(backups, pgBackupCompareIdDesc);

    /* Link incremental backups with their ancestors.*/
    for (i = 0; i < (int)parray_num(backups); i++)
    {
        pgBackup   *curr = (pgBackup  *)parray_get(backups, i);
        pgBackup  **ancestor;
        pgBackup    key;

        if (curr->backup_mode == BACKUP_MODE_FULL)
            continue;

        key.start_time = curr->parent_backup;
        ancestor = (pgBackup **) parray_bsearch(backups, &key,
                    pgBackupCompareIdDesc);
        if (ancestor)
            curr->parent_backup_link = *ancestor;
    }

    return backups;

err_proc:
    if (data_dir)
        fio_closedir(data_dir);
    if (backups)
        parray_walk(backups, pgBackupFree);
    parray_free(backups);

    elog(ERROR, "Failed to get backup list");

    return NULL;
}

/*
 * Create list of backup datafiles.
 * If 'requested_backup_id' is INVALID_BACKUP_ID, exit with error.
 * If valid backup id is passed only matching backup will be added to the list.
 * TODO this function only used once. Is it really needed?
 */
parray *
get_backup_filelist(pgBackup *backup, bool strict)
{
    parray  *files = NULL;
    char    backup_filelist_path[MAXPGPATH];

    join_path_components(backup_filelist_path, backup->root_dir, DATABASE_FILE_LIST);
    files = dir_read_file_list(NULL, NULL, backup_filelist_path, FIO_BACKUP_HOST, backup->content_crc);

    /* redundant sanity? */
    if (!files)
        elog(strict ? ERROR : WARNING, "Failed to get file list for backup %s", base36enc(backup->start_time));

    return files;
}

/*
 * Lock list of backups. Function goes in backward direction.
 */
void
catalog_lock_backup_list(parray *backup_list, int from_idx, int to_idx, bool strict)
{
    int     start_idx,
            end_idx;
    int     i;

    if (parray_num(backup_list) == 0)
        return;

    start_idx = Max(from_idx, to_idx);
    end_idx = Min(from_idx, to_idx);

    for (i = start_idx; i >= end_idx; i--)
    {
        pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);
        if (!lock_backup(backup, strict))
            elog(ERROR, "Cannot lock backup %s directory",
                base36enc(backup->start_time));
    }
}

/*
 * Find the latest valid child of latest valid FULL backup on given timeline
 */
pgBackup *
catalog_get_last_data_backup(parray *backup_list, TimeLineID tli, time_t current_start_time)
{
    int     i;
    pgBackup   *full_backup = NULL;
    pgBackup   *tmp_backup = NULL;
    char    *invalid_backup_id;

    /* backup_list is sorted in order of descending ID */
    for (i = 0; i < (int)parray_num(backup_list); i++)
    {
        pgBackup *backup = (pgBackup *) parray_get(backup_list, i);

        if ((backup->backup_mode == BACKUP_MODE_FULL &&
            (backup->status == BACKUP_STATUS_OK ||
             backup->status == BACKUP_STATUS_DONE)) && backup->tli == tli)
        {
            full_backup = backup;
            break;
        }
    }

    /* Failed to find valid FULL backup to fulfill ancestor role */
    if (!full_backup)
        return NULL;

    elog(LOG, "Latest valid FULL backup: %s",
        base36enc(full_backup->start_time));

    /* FULL backup is found, lets find his latest child */
    for (i = 0; i < (int)parray_num(backup_list); i++)
    {
        pgBackup *backup = (pgBackup *) parray_get(backup_list, i);

        /* only valid descendants are acceptable for evaluation */
        if ((backup->status == BACKUP_STATUS_OK ||
            backup->status == BACKUP_STATUS_DONE))
        {
            switch (scan_parent_chain(backup, &tmp_backup))
            {
                /* broken chain */
                case ChainIsBroken:
                    invalid_backup_id = base36enc_dup(tmp_backup->parent_backup);

                    elog(WARNING, "Backup %s has missing parent: %s. Cannot be a parent",
                        base36enc(backup->start_time), invalid_backup_id);
                    pg_free(invalid_backup_id);
                    continue;

                /* chain is intact, but at least one parent is invalid */
                case ChainIsInvalid:
                    invalid_backup_id = base36enc_dup(tmp_backup->start_time);

                    elog(WARNING, "Backup %s has invalid parent: %s. Cannot be a parent",
                        base36enc(backup->start_time), invalid_backup_id);
                    pg_free(invalid_backup_id);
                    continue;

                /* chain is ok */
                case ChainIsOk:
                    /* Yes, we could call is_parent() earlier - after choosing the ancestor,
                    * but this way we have an opportunity to detect and report all possible
                    * anomalies.
                    */
                    if (is_parent(full_backup->start_time, backup, true))
                        return backup;
            }
        }
        /* skip yourself */
        else if (backup->start_time == current_start_time)
            continue;
        else
        {
            elog(WARNING, "Backup %s has status: %s. Cannot be a parent.",
                base36enc(backup->start_time), status2str(backup->status));
        }
    }

    return NULL;
}


void get_ancestor_backup(timelineInfo *tmp_tlinfo, pgBackup **ancestor_backup)
{
    if (tmp_tlinfo->parent_link->backups)
    {
        for (int i = 0; i < (int)parray_num(tmp_tlinfo->parent_link->backups); i++)
        {
            pgBackup *backup = (pgBackup *) parray_get(tmp_tlinfo->parent_link->backups, i);

            if (backup->backup_mode == BACKUP_MODE_FULL &&
                (backup->status == BACKUP_STATUS_OK ||
                 backup->status == BACKUP_STATUS_DONE) &&
                 backup->stop_lsn <= tmp_tlinfo->switchpoint)
            {
                *ancestor_backup = backup;
                break;
            }
        }
    }
}
/*
 * For multi-timeline chain, look up suitable parent for incremental backup.
 * Multi-timeline chain has full backup and one or more descendants located
 * on different timelines.
 */
pgBackup *
get_multi_timeline_parent(parray *backup_list, parray *tli_list,
                            TimeLineID current_tli, time_t current_start_time,
                            InstanceConfig *instance)
{
    int           i;
    timelineInfo *my_tlinfo = NULL;
    timelineInfo *tmp_tlinfo = NULL;
    pgBackup     *ancestor_backup = NULL;

    /* there are no timelines in the archive */
    if (parray_num(tli_list) == 0)
    return NULL;

    /* look for current timelineInfo */
    for (i = 0; i < (int)parray_num(tli_list); i++)
    {
    timelineInfo  *tlinfo = (timelineInfo  *) parray_get(tli_list, i);

    if (tlinfo->tli == current_tli)
    {
    my_tlinfo = tlinfo;
    break;
    }
    }

    if (my_tlinfo == NULL)
        return NULL;

    /* Locate tlinfo of suitable full backup.
     * Consider this example:
     *  t3                    s2-------X <-! We are here
     *                        /
     *  t2         s1----D---*----E--->
     *             /
     *  t1--A--B--*---C------->
     *
     * A, E - full backups
     * B, C, D - incremental backups
     *
     * We must find A.
     */
    tmp_tlinfo = my_tlinfo;
    while (tmp_tlinfo->parent_link)
    {
        /* if timeline has backups, iterate over them */
        get_ancestor_backup(tmp_tlinfo, &ancestor_backup);

        if (ancestor_backup)
            break;

        tmp_tlinfo = tmp_tlinfo->parent_link;
    }

    /* failed to find valid FULL backup on parent timelines */
    if (!ancestor_backup)
        return NULL;
    else
        elog(LOG, "Latest valid full backup: %s, tli: %i",
            base36enc(ancestor_backup->start_time), ancestor_backup->tli);

    /* At this point we found suitable full backup,
    * now we must find his latest child, suitable to be
    * parent of current incremental backup.
    * Consider this example:
    *  t3                    s2-------X <-! We are here
    *                        /
    *  t2         s1----D---*----E--->
    *             /
    *  t1--A--B--*---C------->
    *
    * A, E - full backups
    * B, C, D - incremental backups
    *
    * We found A, now we must find D.
    */

    /* Optimistically, look on current timeline for valid incremental backup, child of ancestor */
    if (my_tlinfo->backups)
    {
        /* backups are sorted in descending order and we need latest valid */
        for (i = 0; i < (int)parray_num(my_tlinfo->backups); i++)
        {
            pgBackup *tmp_backup = NULL;
            pgBackup *backup = (pgBackup *) parray_get(my_tlinfo->backups, i);

            /* found suitable parent */
            if (scan_parent_chain(backup, &tmp_backup) == ChainIsOk &&
                is_parent(ancestor_backup->start_time, backup, false))
                return backup;
        }
    }

    /* Iterate over parent timelines and look for a valid backup, child of ancestor */
    tmp_tlinfo = my_tlinfo;
    while (tmp_tlinfo->parent_link)
    {

        /* if timeline has backups, iterate over them */
        if (tmp_tlinfo->parent_link->backups)
        {
            for (i = 0; i < (int)parray_num(tmp_tlinfo->parent_link->backups); i++)
            {
                pgBackup *tmp_backup = NULL;
                pgBackup *backup = (pgBackup *) parray_get(tmp_tlinfo->parent_link->backups, i);

                /* We are not interested in backups
                * located outside of our timeline history
                */
                if (backup->stop_lsn > tmp_tlinfo->switchpoint)
                    continue;

                if (scan_parent_chain(backup, &tmp_backup) == ChainIsOk &&
                    is_parent(ancestor_backup->start_time, backup, true))
                    return backup;
            }
        }

        tmp_tlinfo = tmp_tlinfo->parent_link;
    }

    return NULL;
}

/* create backup directory in $BACKUP_PATH */
int
pgBackupCreateDir(pgBackup *backup)
{
    int i;
    char    path[MAXPGPATH];
    parray *subdirs = parray_new();

    parray_append(subdirs, pg_strdup(DATABASE_DIR));

    /* Add external dirs containers */
    if (backup->external_dir_str)
    {
        parray *external_list;

        external_list = make_external_directory_list(backup->external_dir_str,
                                                                                 false);
        for (i = 0; i < (int)parray_num(external_list); i++)
        {
            char    temp[MAXPGPATH];
            /* Numeration of externaldirs starts with 1 */
            makeExternalDirPathByNum(temp, EXTERNAL_DIR, i+1);
            parray_append(subdirs, pg_strdup(temp));
        }
        free_dir_list(external_list);
    }

    pgBackupGetPath(backup, path, lengthof(path), NULL);

    if (!dir_is_empty(path, FIO_BACKUP_HOST))
        elog(ERROR, "backup destination is not empty \"%s\"", path);

    fio_mkdir(path, DIR_PERMISSION, FIO_BACKUP_HOST);
    backup->root_dir = pgut_strdup(path);

    backup->database_dir = (char *)pgut_malloc(MAXPGPATH);
    join_path_components(backup->database_dir, backup->root_dir, DATABASE_DIR);

    if (IsDssMode())
    {
        /* prepare dssdata_dir */
        backup->dssdata_dir = (char *)pgut_malloc(MAXPGPATH);
        join_path_components(backup->dssdata_dir, backup->root_dir, DSSDATA_DIR);

        /* add into subdirs array, which will be create later */
        parray_append(subdirs, pg_strdup(DSSDATA_DIR));
    }

    /* block header map */
    init_header_map(backup);

    /* create directories for actual backup files */
    for (i = 0; i < (int)parray_num(subdirs); i++)
    {
        join_path_components(path, backup->root_dir, (const char *)parray_get(subdirs, i));
        fio_mkdir(path, DIR_PERMISSION, FIO_BACKUP_HOST);
    }

    free_dir_list(subdirs);
    return 0;
}


/*
 * save information about backups belonging to each timeline
 */
void save_backupinfo_belong_timelines(InstanceConfig *instance, parray *timelineinfos)
{
    int i,j;
    parray *backups;
    
    backups = catalog_get_backup_list(instance->name, INVALID_BACKUP_ID);

    for (i = 0; i < (int)parray_num(timelineinfos); i++)
    {
        timelineInfo *tlinfo = (timelineInfo *)parray_get(timelineinfos, i);
        for (j = 0; j < (int)parray_num(backups); j++)
        {
            pgBackup *backup = (pgBackup  *)parray_get(backups, j);
            if (tlinfo->tli == backup->tli)
            {
                if (tlinfo->backups == NULL)
                tlinfo->backups = parray_new();

                parray_append(tlinfo->backups, backup);
            }
        }
    }

    /* determine oldest backup and closest backup for every timeline */
    for (i = 0; i < (int)parray_num(timelineinfos); i++)
    {
        timelineInfo *tlinfo = (timelineInfo *)parray_get(timelineinfos, i);

        tlinfo->oldest_backup = get_oldest_backup(tlinfo);
        tlinfo->closest_backup = get_closest_backup(tlinfo);
    }
    return;
}

void get_tlinfo(timelineInfo **tl_info, TimeLineID tli, XLogSegNo segno, parray *timelineinfos, pgFile *file)
{
    timelineInfo *tlinfo = *tl_info;
    
    /* new file belongs to new timeline */
    if (!tlinfo || tlinfo->tli != tli)
    {
        tlinfo = timelineInfoNew(tli);
        parray_append(timelineinfos, tlinfo);
    }
    /*
    * As it is impossible to detect if segments before segno are lost,
    * or just do not exist, do not report them as lost.
    */
    else if (tlinfo->n_xlog_files != 0)
    {
        /* check, if segments are consequent */
        XLogSegNo expected_segno = tlinfo->end_segno + 1;

        /*
         * Some segments are missing. remember them in lost_segments to report.
         * Normally we expect that segment numbers form an increasing sequence,
         * though it's legal to find two files with equal segno in case there
         * are both compressed and non-compessed versions. For example
         * 000000010000000000000002 and 000000010000000000000002.gz
         *
         */
        if (segno != expected_segno && segno != tlinfo->end_segno)
        {
            xlogInterval *interval = (xlogInterval *)palloc(sizeof(xlogInterval));;
            interval->begin_segno = expected_segno;
            interval->end_segno = segno - 1;

            if (tlinfo->lost_segments == NULL)
                tlinfo->lost_segments = parray_new();

            parray_append(tlinfo->lost_segments, interval);
        }
    }

    if (tlinfo->begin_segno == 0)
        tlinfo->begin_segno = segno;
    
    /* this file is the last for this timeline so far */
    tlinfo->end_segno = segno;
    /* update counters */
    tlinfo->n_xlog_files++;
    tlinfo->size += file->size;
    
    *tl_info = tlinfo;
}

/*
 * Create list of timelines.
 * walk through files and collect info about timelines.
 * TODO: '.partial' and '.part' segno information should be added to tlinfo.
 */
parray *
walk_files_collect_timelines(InstanceConfig *instance)
{
    int i;
    parray *xlog_files_list = parray_new();
    parray *timelineinfos;
    timelineInfo *tlinfo = nullptr;
    char    arclog_path[MAXPGPATH];

    /* for fancy reporting */
    int nRet = 0;
    errno_t rc = 0;

    /* read all xlog files that belong to this archive */
    nRet = snprintf_s(arclog_path, MAXPGPATH,MAXPGPATH - 1,"%s/%s/%s", backup_path, "wal", instance->name);
    securec_check_ss_c(nRet, "\0", "\0");
    dir_list_file(xlog_files_list, arclog_path, false, false, false, false, true, 0, FIO_BACKUP_HOST);
    parray_qsort(xlog_files_list, pgFileCompareName);

    timelineinfos = parray_new();

    /* walk through files and collect info about timelines */
    for (i = 0; i < (int)parray_num(xlog_files_list); i++)
    {
        pgFile *file = (pgFile *) parray_get(xlog_files_list, i);
        TimeLineID tli;
        xlogFile *wal_file = NULL;

        /*
        * Regular WAL file.
        * IsXLogFileName() cannot be used here
        */
        if (strspn(file->name, "0123456789ABCDEF") == XLOG_FNAME_LEN)
        {
            int result = 0;
            uint32 log, seg;
            XLogSegNo segno = 0;
            char suffix[MAXFNAMELEN];

            result = sscanf_s(file->name, "%08X%08X%08X.%s",
                &tli, &log, &seg, (char *) &suffix);

            /* sanity */
            if (result < 3)
            {
                elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
                    continue;
            }

            /* get segno from log */
            GetXLogSegNoFromScrath(segno, log, seg, instance->xlog_seg_size);

            /* regular WAL file with suffix */
            if (result == 4)
            {
                bool is_valid_tli = false;
                bool is_partial_fname = IsPartialXLogFileName(file->name) ||
                    IsPartialCompressXLogFileName(file->name);
                bool is_tmp_fname = IsTempXLogFileName(file->name) ||
                    IsTempCompressXLogFileName(file->name);
                
                
                /* backup history file. Currently we don't use them */
                if (IsBackupHistoryFileName(file->name))
                {
                    elog(VERBOSE, "backup history file \"%s\"", file->name);

                    is_valid_tli = !tlinfo || tlinfo->tli != tli;
                    if (is_valid_tli)
                    {
                        tlinfo = timelineInfoNew(tli);
                        parray_append(timelineinfos, tlinfo);
                    }

                    /* append file to xlog file list */
                    wal_file = (xlogFile *)palloc(sizeof(xlogFile));
                    //wal_file->file = *file;
                    rc = memcpy_s(&wal_file->file,  sizeof(wal_file->file),file, sizeof(wal_file->file));
                    securec_check_c(rc, "\0", "\0");
                    wal_file->segno = segno;
                    wal_file->type = BACKUP_HISTORY_FILE;
                    wal_file->keep = false;
                    parray_append(tlinfo->xlog_filelist, wal_file);
                    continue;
                }
                /* partial WAL segment */
                else if (is_partial_fname)
                {                    
                    elog(VERBOSE, "partial WAL file \"%s\"", file->name);
                    is_valid_tli = !tlinfo || tlinfo->tli != tli;
                    if (is_valid_tli)
                    {
                        tlinfo = timelineInfoNew(tli);
                        parray_append(timelineinfos, tlinfo);
                    }

                    /* append file to xlog file list */
                    wal_file = (xlogFile *)palloc(sizeof(xlogFile));
                    //wal_file->file = *file;
                    rc = memcpy_s(&wal_file->file,sizeof(wal_file->file), file, sizeof(wal_file->file));
                    securec_check_c(rc, "\0", "\0");
                    wal_file->segno = segno;
                    wal_file->type = PARTIAL_SEGMENT;
                    wal_file->keep = false;
                    parray_append(tlinfo->xlog_filelist, wal_file);
                    continue;
                }
                /* temp WAL segment */
                else if (is_tmp_fname)
                {
                    elog(VERBOSE, "temp WAL file \"%s\"", file->name);

                    is_valid_tli = !tlinfo || tlinfo->tli != tli;
                    if (is_valid_tli)
                    {
                        tlinfo = timelineInfoNew(tli);
                        parray_append(timelineinfos, tlinfo);
                    }

                    /* append file to xlog file list */
                    wal_file = (xlogFile *)palloc(sizeof(xlogFile));
                    //wal_file->file = *file;
                    rc = memcpy_s(&wal_file->file, sizeof(wal_file->file),file, sizeof(wal_file->file));
                    securec_check_c(rc, "\0", "\0");
                    wal_file->segno = segno;
                    wal_file->type = TEMP_SEGMENT;
                    wal_file->keep = false;
                    parray_append(tlinfo->xlog_filelist, wal_file);
                    continue;
                }
                /* we only expect compressed wal files with .gz suffix */
                else if (strcmp(suffix, "gz") != 0)
                {
                    elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
                    continue;
                }
            }

            get_tlinfo(&tlinfo, tli, segno, timelineinfos, file);

            /* append file to xlog file list */
            wal_file = (xlogFile *)palloc(sizeof(xlogFile));
            //wal_file->file = *file;
            rc = memcpy_s(&wal_file->file, sizeof(wal_file->file),file, sizeof(wal_file->file));
            securec_check_c(rc, "\0", "\0");
            wal_file->segno = segno;
            wal_file->type = SEGMENT;
            wal_file->keep = false;
            parray_append(tlinfo->xlog_filelist, wal_file);
        }
#ifdef SUPPORT_MULTI_TIMELINE
        /* timeline history file */
        else if (IsTLHistoryFileName(file->name))
        {
            TimeLineHistoryEntry *tln;
            bool is_valid_tli = flase;
            int ret;

            ret = sscanf_s(file->name, "%08X.history", &tli);
            if (ret != 1) {
                elog(LOG, "format filename \"%s\" wrong", file->name);
                break;
            }
            
            timelines = read_timeline_history(arclog_path, tli, true);

            is_valid_tli = !tlinfo || tlinfo->tli != tli;
            if (is_valid_tli)
            {
                tlinfo = timelineInfoNew(tli);
                parray_append(timelineinfos, tlinfo);
                /*
                * 1 is the latest timeline in the timelines list.
                * 0 - is our timeline, which is of no interest here
                */
                tln = (TimeLineHistoryEntry *) parray_get(timelines, 1);
                tlinfo->switchpoint = tln->end;
                tlinfo->parent_tli = tln->tli;

                /* find parent timeline to link it with this one */
                for (j = 0; j < parray_num(timelineinfos); j++)
                {
                    timelineInfo *cur = (timelineInfo *) parray_get(timelineinfos, j);
                    if (cur->tli == tlinfo->parent_tli)
                    {
                        tlinfo->parent_link = cur;
                        break;
                    }
                }
            }

            parray_walk(timelines, pfree);
            parray_free(timelines);
        }
#endif
        else
            elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
    }

    /* save information about backups belonging to each timeline */
    save_backupinfo_belong_timelines(instance, timelineinfos);

    if (xlog_files_list) {
        parray_walk(xlog_files_list, pfree);
        parray_free(xlog_files_list);
    }

    return timelineinfos;
}

void get_anchor_backup(timelineInfo *tlinfo, int *count, InstanceConfig *instance)
{
    for (int j = 0; j < (int)parray_num(tlinfo->backups); j++)
    {
        pgBackup *backup = (pgBackup *)parray_get(tlinfo->backups, j);

        /* sanity */
        if (XLogRecPtrIsInvalid(backup->start_lsn) ||
            backup->tli == 0)
            continue;

        /* skip invalid backups */
        if (backup->status != BACKUP_STATUS_OK &&
            backup->status != BACKUP_STATUS_DONE)
            continue;

        /*
        * Pinned backups should be ignored for the
        * purpose of retention fulfillment, so skip them.
        */
        if (backup->expire_time > 0 &&
            backup->expire_time > current_time)
        {
            elog(LOG, "Pinned backup %s is ignored for the "
                "purpose of WAL retention",
                 base36enc(backup->start_time));
            continue;
        }

        (*count)++;

        if (*count == (int)instance->wal_depth)
        {
            elog(LOG, "On timeline %i WAL is protected from purge at %X/%X",
                tlinfo->tli,
                (uint32) (backup->start_lsn >> 32),
                (uint32) (backup->start_lsn));

            tlinfo->anchor_lsn = backup->start_lsn;
            tlinfo->anchor_tli = backup->tli;
            break;
        }
    }
}

/*
 * determine anchor_lsn and keep_segments for every timeline 
 */
void anchor_lsn_keep_segments_timelines(InstanceConfig *instance, parray *timelineinfos)
{
    int i,j;

    /* for fancy reporting */
    char begin_segno_str[MAXFNAMELEN];
    char end_segno_str[MAXFNAMELEN];

     /*
    * WAL retention for now is fairly simple.
    * User can set only one parameter - 'wal-depth'.
    * It determines how many latest valid(!) backups on timeline
    * must have an ability to perform PITR:
    * Consider the example:
    *
    * ---B1-------B2-------B3-------B4--------> WAL timeline1
    *
    * If 'wal-depth' is set to 2, then WAL purge should produce the following result:
    *
    *    B1       B2       B3-------B4--------> WAL timeline1
    *
    * Only valid backup can satisfy 'wal-depth' condition, so if B3 is not OK or DONE,
    * then WAL purge should produce the following result:
    *    B1       B2-------B3-------B4--------> WAL timeline1
    *
    * Complicated cases, such as branched timelines are taken into account.
    * wal-depth is applied to each timeline independently:
    *
    *         |--------->                       WAL timeline2
    * ---B1---|---B2-------B3-------B4--------> WAL timeline1
    *
    * after WAL purge with wal-depth=2:
    *
    *         |--------->                       WAL timeline2
    *    B1---|   B2       B3-------B4--------> WAL timeline1
    *
    * In this example WAL retention prevents purge of WAL required by tli2
    * to stay reachable from backup B on tli1.
    *
    * To protect WAL from purge we try to set 'anchor_lsn' and 'anchor_tli' in every timeline.
    * They are usually comes from 'start-lsn' and 'tli' attributes of backup
    * calculated by 'wal-depth' parameter.
    * With 'wal-depth=2' anchor_backup in tli1 is B3.

    * If timeline has not enough valid backups to satisfy 'wal-depth' condition,
    * then 'anchor_lsn' and 'anchor_tli' taken from from 'start-lsn' and 'tli
    * attribute of closest_backup.
    * The interval of WAL starting from closest_backup to switchpoint is
    * saved into 'keep_segments' attribute.
    * If there is several intermediate timelines between timeline and its closest_backup
    * then on every intermediate timeline WAL interval between switchpoint
    * and starting segment is placed in 'keep_segments' attributes:
    *
    *                |--------->                       WAL timeline3
    *         |------|                 B5-----B6-->    WAL timeline2
    *    B1---|   B2       B3-------B4------------>    WAL timeline1
    *
    * On timeline where closest_backup is located the WAL interval between
    * closest_backup and switchpoint is placed into 'keep_segments'.
    * If timeline has no 'closest_backup', then 'wal-depth' rules cannot be applied
    * to this timeline and its WAL must be purged by following the basic rules of WAL purging.
    *
    * Third part is handling of ARCHIVE backups.
    * If B1 and B2 have ARCHIVE wal-mode, then we must preserve WAL intervals
    * between start_lsn and stop_lsn for each of them in 'keep_segments'.
    */

    /* determine anchor_lsn and keep_segments for every timeline */
    for (i = 0; i < (int)parray_num(timelineinfos); i++)
    {
        int count = 0;
        timelineInfo *tlinfo = (timelineInfo *)parray_get(timelineinfos, i);

        /*
         * Iterate backward on backups belonging to this timeline to find
         * anchor_backup. NOTE Here we rely on the fact that backups list
         * is ordered by start_lsn DESC.
         */
        if (tlinfo->backups)
        {
            get_anchor_backup(tlinfo, &count, instance);
        }

        /*
         * Failed to find anchor backup for this timeline.
         * We cannot just thrown it to the wolves, because by
         * doing that we will violate our own guarantees.
         * So check the existence of closest_backup for
         * this timeline. If there is one, then
         * set the 'anchor_lsn' and 'anchor_tli' to closest_backup
         * 'start-lsn' and 'tli' respectively.
         *                      |-------------B5----------> WAL timeline3
         *                |-----|-------------------------> WAL timeline2
         *     B1    B2---|        B3     B4-------B6-----> WAL timeline1
         *
         * wal-depth=2
         *
         * If number of valid backups on timelines is less than 'wal-depth'
         * then timeline must(!) stay reachable via parent timelines if any.
         * If closest_backup is not available, then general WAL purge rules
         * are applied.
         */
        if (XLogRecPtrIsInvalid(tlinfo->anchor_lsn))
        {
            /*
             * Failed to find anchor_lsn in our own timeline.
             * Consider the case:
             * -------------------------------------> tli5
             * ----------------------------B4-------> tli4
             *                     S3`--------------> tli3
             *      S1`------------S3---B3-------B6-> tli2
             * B1---S1-------------B2--------B5-----> tli1
             *
             * B* - backups
             * S* - switchpoints
             * wal-depth=2
             *
             * Expected result:
             *            TLI5 will be purged entirely
             *                             B4-------> tli4
             *                     S2`--------------> tli3
             *      S1`------------S2   B3-------B6-> tli2
             * B1---S1             B2--------B5-----> tli1
             */
            pgBackup *closest_backup = NULL;
            xlogInterval *interval = NULL;
            TimeLineID tli = 0;
            /* check if tli has closest_backup */
            if (!tlinfo->closest_backup)
                /* timeline has no closest_backup, wal retention cannot be
                 * applied to this timeline.
                 * Timeline will be purged up to oldest_backup if any or
                 * purge entirely if there is none.
                 * In example above: tli5 and tli4.
                 */
                continue;

            /* sanity for closest_backup */
            if (XLogRecPtrIsInvalid(tlinfo->closest_backup->start_lsn) ||
                tlinfo->closest_backup->tli == 0)
                continue;

            /*
            * Set anchor_lsn and anchor_tli to protect whole timeline from purge
            * In the example above: tli3.
            */
            tlinfo->anchor_lsn = tlinfo->closest_backup->start_lsn;
            tlinfo->anchor_tli = tlinfo->closest_backup->tli;

            /* closest backup may be located not in parent timeline */
            closest_backup = tlinfo->closest_backup;

            tli = tlinfo->tli;

            /*
            * Iterate over parent timeline chain and
            * look for timeline where closest_backup belong
            */
            while (tlinfo->parent_link)
            {
                /* In case of intermediate timeline save to keep_segments
                * begin_segno and switchpoint segment.
                * In case of final timelines save to keep_segments
                * closest_backup start_lsn segment and switchpoint segment.
                */
                XLogRecPtr switchpoint = tlinfo->switchpoint;

                tlinfo = tlinfo->parent_link;

                if (tlinfo->keep_segments == NULL)
                    tlinfo->keep_segments = parray_new();

                /* in any case, switchpoint segment must be added to interval */
                interval = (xlogInterval *)palloc(sizeof(xlogInterval));
                GetXLogSegNo(switchpoint, interval->end_segno, instance->xlog_seg_size);

                /* Save [S1`, S2] to keep_segments */
                if (tlinfo->tli != closest_backup->tli)
                    interval->begin_segno = tlinfo->begin_segno;
                /* Save [B1, S1] to keep_segments */
                else
                    GetXLogSegNo(closest_backup->start_lsn, interval->begin_segno, instance->xlog_seg_size);

                /*
                * TODO: check, maybe this interval is already here or
                * covered by other larger interval.
                */

                GetXLogFileName(begin_segno_str, MAXFNAMELEN, tlinfo->tli, interval->begin_segno,
                    instance->xlog_seg_size);
                GetXLogFileName(end_segno_str, MAXFNAMELEN, tlinfo->tli, interval->end_segno, instance->xlog_seg_size);

                elog(LOG, "Timeline %i to stay reachable from timeline %i "
                                "protect from purge WAL interval between "
                                "%s and %s on timeline %i",
                                tli, closest_backup->tli, begin_segno_str,
                                end_segno_str, tlinfo->tli);

                parray_append(tlinfo->keep_segments, interval);
                continue;
            }
            continue;
        }

        /* Iterate over backups left */
        for (j = count; tlinfo->backups && (j < (int)parray_num(tlinfo->backups)); j++)
        {
            XLogSegNo   segno = 0;
            xlogInterval *interval = NULL;
            pgBackup *backup = (pgBackup *)parray_get(tlinfo->backups, j);

            /*
            * We must calculate keep_segments intervals for ARCHIVE backups
            * with start_lsn less than anchor_lsn.
            */

            /* STREAM backups cannot contribute to keep_segments */
            if (backup->stream)
                continue;

            /* sanity */
            if (XLogRecPtrIsInvalid(backup->start_lsn) ||
                backup->tli == 0)
                continue;

            /* no point in clogging keep_segments by backups protected by anchor_lsn */
            if (backup->start_lsn >= tlinfo->anchor_lsn)
                continue;

            /* append interval to keep_segments */
            interval = (xlogInterval *)palloc(sizeof(xlogInterval));
            GetXLogSegNo(backup->start_lsn, segno, instance->xlog_seg_size);
            interval->begin_segno = segno;
            GetXLogSegNo(backup->stop_lsn, segno, instance->xlog_seg_size);

            /*
             * On replica it is possible to get STOP_LSN pointing to contrecord,
             * so set end_segno to the next segment after STOP_LSN just to be safe.
             */
            if (backup->from_replica)
                interval->end_segno = segno + 1;
            else
                interval->end_segno = segno;

            GetXLogFileName(begin_segno_str, MAXFNAMELEN, tlinfo->tli, interval->begin_segno, instance->xlog_seg_size);
            GetXLogFileName(end_segno_str, MAXFNAMELEN, tlinfo->tli, interval->end_segno, instance->xlog_seg_size);

            elog(LOG, "Archive backup %s to stay consistent "
                            "protect from purge WAL interval "
                            "between %s and %s on timeline %i",
                            base36enc(backup->start_time),
                            begin_segno_str, end_segno_str, backup->tli);

            if (tlinfo->keep_segments == NULL)
                tlinfo->keep_segments = parray_new();

            parray_append(tlinfo->keep_segments, interval);
        }
    }
    return ;
    
}


/*
 * Protect WAL segments from deletion by setting 'keep' flag.
 */
void protect_wal_segments(InstanceConfig *instance, parray *timelineinfos)
{
    int i,j,k;
    /*
     * Protect WAL segments from deletion by setting 'keep' flag.
     * We must keep all WAL segments after anchor_lsn (including), and also segments
     * required by ARCHIVE backups for consistency - WAL between [start_lsn, stop_lsn].
     */
    for (i = 0; i < (int)parray_num(timelineinfos); i++)
    {
        XLogSegNo   anchor_segno = 0;
        timelineInfo *tlinfo = (timelineInfo *)parray_get(timelineinfos, i);

        /*
        * At this point invalid anchor_lsn can be only in one case:
        * timeline is going to be purged by regular WAL purge rules.
        */
        if (XLogRecPtrIsInvalid(tlinfo->anchor_lsn))
            continue;

        /*
        * anchor_lsn is located in another timeline, it means that the timeline
        * will be protected from purge entirely.
        */
        if (tlinfo->anchor_tli > 0 && tlinfo->anchor_tli != tlinfo->tli)
            continue;

        GetXLogSegNo(tlinfo->anchor_lsn, anchor_segno, instance->xlog_seg_size);

        for (j = 0; j < (int)parray_num(tlinfo->xlog_filelist); j++)
        {
            xlogFile *wal_file = (xlogFile *) parray_get(tlinfo->xlog_filelist, j);

            if (wal_file->segno >= anchor_segno)
            {
                wal_file->keep = true;
                continue;
            }

            /* no keep segments */
            if (!tlinfo->keep_segments)
                continue;

        /* Protect segments belonging to one of the keep invervals */
            for (k = 0; k < (int)parray_num(tlinfo->keep_segments); k++)
            {
                xlogInterval *keep_segments = (xlogInterval *) parray_get(tlinfo->keep_segments, k);

                if ((wal_file->segno >= keep_segments->begin_segno) &&
                    wal_file->segno <= keep_segments->end_segno)
                {
                    wal_file->keep = true;
                    break;
                }
            }
        }
    }
    return;
}

/*
 * Create list of timelines.
 * TODO: '.partial' and '.part' segno information should be added to tlinfo.
 */
parray *
catalog_get_timelines(InstanceConfig *instance)
{
    parray *timelineinfos;

    timelineinfos = walk_files_collect_timelines(instance);
    /* determine which WAL segments must be kept because of wal retention */
    if (instance->wal_depth == 0)
        return timelineinfos;

    /*
    * determine anchor_lsn and keep_segments for every timeline 
    */
    anchor_lsn_keep_segments_timelines(instance, timelineinfos);

    /*
    * Protect WAL segments from deletion by setting 'keep' flag.
    */
    protect_wal_segments(instance, timelineinfos);


    return timelineinfos;
}

/*
 * Iterate over parent timelines and look for valid backup
 * closest to given timeline switchpoint.
 *
 * If such backup doesn't exist, it means that
 * timeline is unreachable. Return NULL.
 */
pgBackup*
get_closest_backup(timelineInfo *tlinfo)
{
    pgBackup *closest_backup = NULL;
    int i;

    /*
    * Iterate over backups belonging to parent timelines
    * and look for candidates.
    */
    while (tlinfo->parent_link && !closest_backup)
    {
        parray *backup_list = tlinfo->parent_link->backups;
        if (backup_list != NULL)
        {
            for (i = 0; i < (int)parray_num(backup_list); i++)
            {
                pgBackup   *backup = (pgBackup *)parray_get(backup_list, i);

                /*
                * Only valid backups made before switchpoint
                * should be considered.
                */
                if (!XLogRecPtrIsInvalid(backup->stop_lsn) &&
                    XRecOffIsValid(backup->stop_lsn) &&
                    backup->stop_lsn <= tlinfo->switchpoint &&
                    (backup->status == BACKUP_STATUS_OK ||
                    backup->status == BACKUP_STATUS_DONE))
                {
                    /* Check if backup is closer to switchpoint than current candidate */
                    if (!closest_backup || backup->stop_lsn > closest_backup->stop_lsn)
                        closest_backup = backup;
                }
            }
        }

        /* Continue with parent */
        tlinfo = tlinfo->parent_link;
    }

    return closest_backup;
}

/*
 * Find oldest backup in given timeline
 * to determine what WAL segments of this timeline
 * are reachable from backups belonging to it.
 *
 * If such backup doesn't exist, it means that
 * there is no backups on this timeline. Return NULL.
 */
pgBackup*
get_oldest_backup(timelineInfo *tlinfo)
{
    pgBackup *oldest_backup = NULL;
    int i;
    parray *backup_list = tlinfo->backups;

    if (backup_list != NULL)
    {
        for (i = 0; i < (int)parray_num(backup_list); i++)
        {
            pgBackup   *backup = (pgBackup *)parray_get(backup_list, i);

            /* Backups with invalid START LSN can be safely skipped */
            if (XLogRecPtrIsInvalid(backup->start_lsn) ||
                !XRecOffIsValid(backup->start_lsn))
                continue;

            /*
            * Check if backup is older than current candidate.
            * Here we use start_lsn for comparison, because backup that
            * started earlier needs more WAL.
            */
            if (!oldest_backup || backup->start_lsn < oldest_backup->start_lsn)
                oldest_backup = backup;
        }
    }

    return oldest_backup;
}

/*
 * Overwrite backup metadata.
 */
void
do_set_backup(const char *instance_name, time_t backup_id,
                            pgSetBackupParams *set_backup_params)
{
    pgBackup    *target_backup = NULL;
    parray  *backup_list = NULL;

    if (!set_backup_params)
        elog(ERROR, "Nothing to set by 'set-backup' command");

    backup_list = catalog_get_backup_list(instance_name, backup_id);
    if (parray_num(backup_list) != 1)
        elog(ERROR, "Failed to find backup %s", base36enc(backup_id));

    target_backup = (pgBackup *) parray_get(backup_list, 0);

    /* Pin or unpin backup if requested */
    if (set_backup_params->ttl >= 0 || set_backup_params->expire_time > 0)
        pin_backup(target_backup, set_backup_params);

    if (set_backup_params->note)
        add_note(target_backup, set_backup_params->note);

    if (set_backup_params->oss_status >=  OSS_STATUS_LOCAL && set_backup_params->oss_status < OSS_STATUS_NUM) {
        target_backup->oss_status = set_backup_params->oss_status;
        /* Update backup.control */
        write_backup(target_backup, true);
    }

    parray_walk(backup_list, pgBackupFree);
    parray_free(backup_list);
}

/*
 * Set 'expire-time' attribute based on set_backup_params, or unpin backup
 * if ttl is equal to zero.
 */
void
pin_backup(pgBackup	*target_backup, pgSetBackupParams *set_backup_params)
{

    /* sanity, backup must have positive recovery-time */
    if (target_backup->recovery_time <= 0)
        elog(ERROR, "Failed to set 'expire-time' for backup %s: invalid 'recovery-time'",
            base36enc(target_backup->backup_id));

    /* Pin comes from ttl */
    if (set_backup_params->ttl > 0)
        target_backup->expire_time = target_backup->recovery_time + set_backup_params->ttl;
    /* Unpin backup */
    else if (set_backup_params->ttl == 0)
    {
        /* If backup was not pinned in the first place,
        * then there is nothing to unpin.
        */
        if (target_backup->expire_time == 0)
        {
            elog(WARNING, "Backup %s is not pinned, nothing to unpin",
                base36enc(target_backup->start_time));
            return;
        }
        target_backup->expire_time = 0;
    }
    /* Pin comes from expire-time */
    else if (set_backup_params->expire_time > 0)
        target_backup->expire_time = set_backup_params->expire_time;
    else
        /* nothing to do */
        return;

    /* Update backup.control */
    write_backup(target_backup, true);

    if (set_backup_params->ttl > 0 || set_backup_params->expire_time > 0)
    {
        char    expire_timestamp[100];

        time2iso(expire_timestamp, lengthof(expire_timestamp), target_backup->expire_time);
        elog(INFO, "Backup %s is pinned until '%s'", base36enc(target_backup->start_time),
            expire_timestamp);
    }
    else
        elog(INFO, "Backup %s is unpinned", base36enc(target_backup->start_time));

    return;
}

/*
 * Add note to backup metadata or unset already existing note.
 * It is a job of the caller to make sure that note is not NULL.
 */
void
add_note(pgBackup *target_backup, const char *note)
{
    /* unset note */
    if (pg_strcasecmp(note, "none") == 0)
    {
        target_backup->note = NULL;
        elog(INFO, "Removing note from backup %s",
            base36enc(target_backup->start_time));
    }
    else
    {
        /* Currently we do not allow string with newlines as note,
        * because it will break parsing of backup.control.
        * So if user provides string like this "aaa\nbbbbb",
        * we save only "aaa"
        * Example: tests.set_backup.SetBackupTest.test_add_note_newlines
        */
        char *note_string = (char *)pgut_malloc(MAX_NOTE_SIZE);
        int ret = sscanf_s(note, "%[^\n]", note_string, MAX_NOTE_SIZE);
        if (ret == -1)
            elog(INFO, "No note added for backup.");

        target_backup->note = note_string;
        elog(INFO, "Adding note to backup %s: '%s'",
            base36enc(target_backup->start_time), target_backup->note);
    }

    /* Update backup.control */
    write_backup(target_backup, true);
}

/*
 * Write information about backup.in to stream "out".
 */
void
pgBackupWriteControl(FILE *out, pgBackup *backup)
{
    char    timestamp[100];

    fio_fprintf(out, "#Configuration\n");
    fio_fprintf(out, "backup-mode = %s\n", pgBackupGetBackupMode(backup));
    fio_fprintf(out, "stream = %s\n", backup->stream ? "true" : "false");
    fio_fprintf(out, "compress-alg = %s\n",
        deparse_compress_alg(backup->compress_alg));
    fio_fprintf(out, "compress-level = %d\n", backup->compress_level);
    fio_fprintf(out, "from-replica = %s\n", backup->from_replica ? "true" : "false");

    fio_fprintf(out, "\n#Compatibility\n");
    fio_fprintf(out, "block-size = %u\n", backup->block_size);
    fio_fprintf(out, "xlog-block-size = %u\n", backup->wal_block_size);
    fio_fprintf(out, "checksum-version = %u\n", backup->checksum_version);
    if (backup->program_version[0] != '\0')
        fio_fprintf(out, "program-version = %s\n", backup->program_version);
    if (backup->server_version[0] != '\0')
        fio_fprintf(out, "server-version = %s\n", backup->server_version);

    fio_fprintf(out, "\n#Result backup info\n");
    fio_fprintf(out, "timelineid = %d\n", backup->tli);
    /* LSN returned by pg_start_backup */
    fio_fprintf(out, "start-lsn = %X/%X\n",
        (uint32) (backup->start_lsn >> 32),
        (uint32) backup->start_lsn);
    /* LSN returned by pg_stop_backup */
    fio_fprintf(out, "stop-lsn = %X/%X\n",
        (uint32) (backup->stop_lsn >> 32),
        (uint32) backup->stop_lsn);

    time2iso(timestamp, lengthof(timestamp), backup->start_time);
    fio_fprintf(out, "start-time = '%s'\n", timestamp);
    if (backup->merge_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->merge_time);
        fio_fprintf(out, "merge-time = '%s'\n", timestamp);
    }
    if (backup->end_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->end_time);
        fio_fprintf(out, "end-time = '%s'\n", timestamp);
    }
    fio_fprintf(out, "recovery-xid = " XID_FMT "\n", backup->recovery_xid);
    if (backup->recovery_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
        fio_fprintf(out, "recovery-time = '%s'\n", timestamp);
    }
    if (backup->expire_time > 0)
    {
        time2iso(timestamp, lengthof(timestamp), backup->expire_time);
        fio_fprintf(out, "expire-time = '%s'\n", timestamp);
    }

    if (backup->recovery_name[0] != '\0')
	{
		fio_fprintf(out, "recovery-name = '%s'\n", backup->recovery_name);
	}

    if (backup->merge_dest_backup != 0)
        fio_fprintf(out, "merge-dest-id = '%s'\n", base36enc(backup->merge_dest_backup));

    /*
     * Size of PGDATA directory. The size does not include size of related
     * WAL segments in archive 'wal' directory.
     */
    if (backup->data_bytes != BYTES_INVALID)
        fio_fprintf(out, "data-bytes = " INT64_FORMAT "\n", backup->data_bytes);

    if (backup->wal_bytes != BYTES_INVALID)
        fio_fprintf(out, "wal-bytes = " INT64_FORMAT "\n", backup->wal_bytes);

    if (backup->uncompressed_bytes >= 0)
        fio_fprintf(out, "uncompressed-bytes = " INT64_FORMAT "\n", backup->uncompressed_bytes);

    if (backup->pgdata_bytes >= 0)
        fio_fprintf(out, "pgdata-bytes = " INT64_FORMAT "\n", backup->pgdata_bytes);

    fio_fprintf(out, "status = %s\n", status2str(backup->status));

    /* 'parent_backup' is set if it is incremental backup */
    if (backup->parent_backup != 0)
        fio_fprintf(out, "parent-backup-id = '%s'\n", base36enc(backup->parent_backup));

    /* print external directories list */
    if (backup->external_dir_str)
        fio_fprintf(out, "external-dirs = '%s'\n", backup->external_dir_str);

    if (backup->note)
        fio_fprintf(out, "note = '%s'\n", backup->note);

    if (backup->content_crc != 0)
        fio_fprintf(out, "content-crc = %u\n", backup->content_crc);
    
    fio_fprintf(out, "\n#Database Storage type\n");
    fio_fprintf(out, "storage-type = %s\n", dev2str(backup->storage_type));

    fio_fprintf(out, "\n#S3 Storage status\n");
    fio_fprintf(out, "s3-status = %s\n", ossStatus2str(backup->oss_status));
}

/*
 * Save the backup content into BACKUP_CONTROL_FILE.
 * TODO: honor the strict flag
 */
void
write_backup(pgBackup *backup, bool strict)
{
    FILE   *fp = NULL;
    char    path[MAXPGPATH];
    char    path_temp[MAXPGPATH];
    char    buf[4096];
    int nRet = 0;

    join_path_components(path, backup->root_dir, BACKUP_CONTROL_FILE);
    nRet = snprintf_s(path_temp, sizeof(path_temp), sizeof(path_temp) - 1,"%s.tmp", path);
    securec_check_ss_c(nRet, "\0", "\0");
    canonicalize_path(path_temp);
    fp = fopen(path_temp, PG_BINARY_W);
    if (fp == NULL) {
        elog(ERROR, "Cannot open control file \"%s\": %s",
            path_temp, strerror(errno));
        return;
    }

    if (chmod(path_temp, FILE_PERMISSION) == -1) {
        fclose(fp);
        elog(ERROR, "Cannot change mode of \"%s\": %s", path_temp,
         strerror(errno));
        return;
    }

    setvbuf(fp, buf, _IOFBF, sizeof(buf));

    pgBackupWriteControl(fp, backup);

    if (fflush(fp) != 0){
        fclose(fp);
        elog(ERROR, "Cannot flush control file \"%s\": %s",
         path_temp, strerror(errno));
    }

    if (fsync(fileno(fp)) < 0){
        fclose(fp);
        elog(ERROR, "Cannot sync control file \"%s\": %s",
         path_temp, strerror(errno));
    }

    if (fclose(fp) != 0)
        elog(ERROR, "Cannot close control file \"%s\": %s",
         path_temp, strerror(errno));

    if (rename(path_temp, path) < 0)
        elog(ERROR, "Cannot rename file \"%s\" to \"%s\": %s",
        path_temp, path, strerror(errno));

    if (current.media_type == MEDIA_TYPE_OSS) {
        uploadConfigFile(path, path);
    }
}

void flush_and_close_file(pgBackup *backup, bool sync, FILE *out, char *control_path_temp)
{
    if (sync)
        FIN_FILE_CRC32(true, backup->content_crc);

    if (fflush(out) != 0)
        elog(ERROR, "Cannot flush file list \"%s\": %s",
         control_path_temp, strerror(errno));

    if (sync && fsync(fileno(out)) < 0)
        elog(ERROR, "Cannot sync file list \"%s\": %s",
         control_path_temp, strerror(errno));

    if (fclose(out) != 0)
        elog(ERROR, "Cannot close file list \"%s\": %s",
         control_path_temp, strerror(errno));
}

inline int write_compress_option(pgFile *file, char *line, int remain_len, int len)
{
    if (file->is_datafile && file->compressed_file) {
        int nRet =
            snprintf_s(line + len, (uint32)(remain_len - len), (uint32)((remain_len - len) - 1),
                       ",\"compressedFile\":\"%d\",\"compressedChunkSize\":\"%d\",\"compressedAlgorithm\":\"%d\"", 1,
                       file->compressed_chunk_size, file->compressed_algorithm);
        securec_check_ss_c(nRet, "\0", "\0");
        return nRet;
    }
    return 0;
}

/*
 * Output the list of files to backup catalog DATABASE_FILE_LIST
 */
void
write_backup_filelist(pgBackup *backup, parray *files, const char *root,
                                    parray *external_list, bool sync)
{
    FILE    *out;
    char    control_path[MAXPGPATH];
    char    control_path_temp[MAXPGPATH];
    size_t  i = 0;
#define BUFFERSZ 1024*1024
    char    *buf;
    int64   backup_size_on_disk = 0;
    int64   uncompressed_size_on_disk = 0;
    int64   wal_size_on_disk = 0;
    int nRet = 0;

    join_path_components(control_path, backup->root_dir, DATABASE_FILE_LIST);
    nRet = snprintf_s(control_path_temp, sizeof(control_path_temp), sizeof(control_path_temp) - 1, "%s.tmp", control_path);
    securec_check_ss_c(nRet, "\0", "\0");

    out = fopen(control_path_temp, PG_BINARY_W);
    if (out == NULL)
        elog(ERROR, "Cannot open file list \"%s\": %s", control_path_temp,
         strerror(errno));

    if (chmod(control_path_temp, FILE_PERMISSION) == -1)
        elog(ERROR, "Cannot change mode of \"%s\": %s", control_path_temp,
         strerror(errno));

    buf = (char *)pgut_malloc(BUFFERSZ);
    setvbuf(out, buf, _IOFBF, BUFFERSZ);

    if (sync)
        INIT_FILE_CRC32(true, backup->content_crc);

    /* print each file in the list */
    for (i = 0; i < parray_num(files); i++)
    {
        int       len = 0;
        char      line[BLCKSZ];
        pgFile   *file = (pgFile *) parray_get(files, i);
        int remainLen = BLCKSZ;
        bool is_regular_file = S_ISREG(file->mode) && file->write_size > 0;

        /* Ignore disappeared file */
        if (file->write_size == FILE_NOT_FOUND)
            continue;

        if (S_ISDIR(file->mode))
        {
            backup_size_on_disk += 4096;
            uncompressed_size_on_disk += 4096;
        }

        /* Count the amount of the data actually copied */
        if (is_regular_file)
        {
            bool is_xlog = IsXLogFileName(file->name) && file->external_dir_num == 0;
            /*
             * Size of WAL files in 'pg_wal' is counted separately
             * TODO: in 3.0 add attribute is_walfile
             */
            if (is_xlog)
                wal_size_on_disk += file->write_size;
            else
            {
                backup_size_on_disk += file->write_size;
                uncompressed_size_on_disk += file->uncompressed_size;
            }
        }

        nRet = snprintf_s(line, remainLen,remainLen - 1,"{\"path\":\"%s\", \"size\":\"" INT64_FORMAT "\", "
                         "\"mode\":\"%u\", \"is_datafile\":\"%u\", "
                         "\"is_cfs\":\"%u\", \"crc\":\"%u\", "
                         "\"compress_alg\":\"%s\", \"external_dir_num\":\"%d\", "
                         "\"dbOid\":\"%u\", \"file_type\":\"%d\"",
                        file->rel_path, file->write_size, file->mode,
                        file->is_datafile ? 1 : 0,
                        file->is_cfs ? 1 : 0,
                        file->crc,
                        deparse_compress_alg(file->compress_alg),
                        file->external_dir_num,
                        file->dbOid,
                        (int)file->type);
        securec_check_ss_c(nRet, "\0", "\0");
        len = nRet;

        if (file->is_datafile)
        {
            nRet = snprintf_s(line+len, remainLen - len,remainLen - len - 1,",\"segno\":\"%d\"", file->segno);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
            /* persistence compress option */
            len += write_compress_option(file, line, remainLen, len);
        }
        
        if (file->linked)
        {
            nRet = snprintf_s(line+len, remainLen - len,remainLen - len - 1,",\"linked\":\"%s\"", file->linked);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
        }

        if (file->n_blocks > 0)
        {
            nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, ",\"n_blocks\":\"%i\"", file->n_blocks);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
        }

        if (file->n_headers > 0)
        {
            nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, ",\"n_headers\":\"%i\"", file->n_headers);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
            nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, ",\"hdr_crc\":\"%u\"", file->hdr_crc);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
            nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, ",\"hdr_off\":\"%li\"", file->hdr_off);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
            nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, ",\"hdr_size\":\"%i\"", file->hdr_size);
            securec_check_ss_c(nRet, "\0", "\0");
            len += nRet;
        }

        nRet =  snprintf_s(line+len,remainLen - len,remainLen - len - 1, "}\n");
        securec_check_ss_c(nRet, "\0", "\0");
        len += nRet;

        if (sync)
            COMP_FILE_CRC32(true, backup->content_crc, line, strlen(line));

        fprintf(out, "%s", line);
    }

    flush_and_close_file(backup, sync, out, control_path_temp);
    
    if (rename(control_path_temp, control_path) < 0)
        elog(ERROR, "Cannot rename file \"%s\" to \"%s\": %s",
         control_path_temp, control_path, strerror(errno));

    if (current.media_type == MEDIA_TYPE_OSS) {
        uploadConfigFile(control_path, control_path);
    }    

    /* use extra variable to avoid reset of previous data_bytes value in case of error */
    backup->data_bytes = backup_size_on_disk;
    backup->uncompressed_bytes = uncompressed_size_on_disk;

    if (backup->stream)
        backup->wal_bytes = wal_size_on_disk;

    free(buf);
}

void set_backup_status(char *status, pgBackup *backup)
{
    if (status)
    {
        if (strcmp(status, "OK") == 0)
            backup->status = BACKUP_STATUS_OK;
        else if (strcmp(status, "ERROR") == 0)
            backup->status = BACKUP_STATUS_ERROR;
        else if (strcmp(status, "RUNNING") == 0)
            backup->status = BACKUP_STATUS_RUNNING;
        else if (strcmp(status, "MERGING") == 0)
            backup->status = BACKUP_STATUS_MERGING;
        else if (strcmp(status, "MERGED") == 0)
            backup->status = BACKUP_STATUS_MERGED;
        else if (strcmp(status, "DELETING") == 0)
            backup->status = BACKUP_STATUS_DELETING;
        else if (strcmp(status, "DELETED") == 0)
            backup->status = BACKUP_STATUS_DELETED;
        else if (strcmp(status, "DONE") == 0)
            backup->status = BACKUP_STATUS_DONE;
        else if (strcmp(status, "ORPHAN") == 0)
            backup->status = BACKUP_STATUS_ORPHAN;
        else if (strcmp(status, "CORRUPT") == 0)
            backup->status = BACKUP_STATUS_CORRUPT;
        else
            elog(WARNING, "Invalid STATUS \"%s\"", status);
        free(status);
    }
}

/*
 * Read BACKUP_CONTROL_FILE and create pgBackup.
 *  - Comment starts with ';'.
 *  - Do not care section.
 */
static pgBackup *
readBackupControlFile(const char *path)
{
    pgBackup   *backup = pgut_new(pgBackup);
    char    *backup_mode = NULL;
    char    *start_lsn = NULL;
    char    *stop_lsn = NULL;
    char    *status = NULL;
    char    *parent_backup = NULL;
    char    *merge_dest_backup = NULL;
    char    *program_version = NULL;
    char    *server_version = NULL;
    char    *compress_alg = NULL;
	char    *recovery_name = NULL;
    int     parsed_options;
    char    *storage_type = NULL;
    char    *oss_status = NULL;
    errno_t rc = 0;

    ConfigOption options[] =
    {
        {'s', 0, "backup-mode",			&backup_mode, SOURCE_FILE_STRICT},
        {'u', 0, "timelineid",			&backup->tli, SOURCE_FILE_STRICT},
        {'s', 0, "start-lsn",			&start_lsn, SOURCE_FILE_STRICT},
        {'s', 0, "stop-lsn",			&stop_lsn, SOURCE_FILE_STRICT},
        {'t', 0, "start-time",			&backup->start_time, SOURCE_FILE_STRICT},
        {'t', 0, "merge-time",			&backup->merge_time, SOURCE_FILE_STRICT},
        {'t', 0, "end-time",			&backup->end_time, SOURCE_FILE_STRICT},
        {'U', 0, "recovery-xid",		&backup->recovery_xid, SOURCE_FILE_STRICT},
        {'t', 0, "recovery-time",		&backup->recovery_time, SOURCE_FILE_STRICT},
        {'t', 0, "expire-time",			&backup->expire_time, SOURCE_FILE_STRICT},
        {'I', 0, "data-bytes",			&backup->data_bytes, SOURCE_FILE_STRICT},
        {'I', 0, "wal-bytes",			&backup->wal_bytes, SOURCE_FILE_STRICT},
        {'I', 0, "uncompressed-bytes",	&backup->uncompressed_bytes, SOURCE_FILE_STRICT},
        {'I', 0, "pgdata-bytes",		&backup->pgdata_bytes, SOURCE_FILE_STRICT},
        {'u', 0, "block-size",			&backup->block_size, SOURCE_FILE_STRICT},
        {'u', 0, "xlog-block-size",		&backup->wal_block_size, SOURCE_FILE_STRICT},
        {'u', 0, "checksum-version",	&backup->checksum_version, SOURCE_FILE_STRICT},
        {'s', 0, "program-version",		&program_version, SOURCE_FILE_STRICT},
        {'s', 0, "server-version",		&server_version, SOURCE_FILE_STRICT},
        {'b', 0, "stream",				&backup->stream, SOURCE_FILE_STRICT},
        {'s', 0, "status",				&status, SOURCE_FILE_STRICT},
        {'s', 0, "parent-backup-id",	&parent_backup, SOURCE_FILE_STRICT},
        {'s', 0, "merge-dest-id",		&merge_dest_backup, SOURCE_FILE_STRICT},
        {'s', 0, "compress-alg",		&compress_alg, SOURCE_FILE_STRICT},
        {'u', 0, "compress-level",		&backup->compress_level, SOURCE_FILE_STRICT},
        {'b', 0, "from-replica",		&backup->from_replica, SOURCE_FILE_STRICT},
        {'s', 0, "external-dirs",		&backup->external_dir_str, SOURCE_FILE_STRICT},
        {'s', 0, "note",				&backup->note, SOURCE_FILE_STRICT},
        {'s', 0, "recovery-name",		&recovery_name, SOURCE_FILE_STRICT},
        {'u', 0, "content-crc",			&backup->content_crc, SOURCE_FILE_STRICT},
        {'s', 0, "storage-type",        &storage_type, SOURCE_FILE_STRICT},
        {'s', 0, "s3-status",           &oss_status, SOURCE_FILE_STRICT},
        {0}
    };

    pgBackupInit(backup);

    if (current.media_type == MEDIA_TYPE_OSS) {
        restoreConfigFile(path);
    }

    if (fio_access(path, F_OK, FIO_BACKUP_HOST) != 0)
    {
        elog(WARNING, "Control file \"%s\" doesn't exist", path);
        pgBackupFree(backup);
        return NULL;
    }

    parsed_options = config_read_opt(path, options, WARNING, true, true);

    if (parsed_options == 0)
    {
        elog(WARNING, "Control file \"%s\" is empty", path);
        pgBackupFree(backup);
        backup = NULL;
        goto finish;
    }

    if (recovery_name)
    {
        rc = snprintf_s(backup->recovery_name, lengthof(backup->recovery_name),
                        lengthof(backup->recovery_name) - 1, recovery_name);
        securec_check_ss_c(rc, "\0", "\0");
    }

    if (backup->start_time == 0)
    {
        elog(WARNING, "Invalid ID/start-time, control file \"%s\" is corrupted", path);
        pgBackupFree(backup);
        backup = NULL;
        goto finish;
    }

    if (backup_mode)
    {
        backup->backup_mode = parse_backup_mode(backup_mode);
    }

    if (start_lsn)
    {
        uint32 xlogid;
        uint32 xrecoff;

        if (sscanf_s(start_lsn, "%X/%X", &xlogid, &xrecoff) == 2)
            backup->start_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
        else
            elog(WARNING, "Invalid START_LSN \"%s\"", start_lsn);
    }

    if (stop_lsn)
    {
        uint32 xlogid;
        uint32 xrecoff;

        if (sscanf_s(stop_lsn, "%X/%X", &xlogid, &xrecoff) == 2)
            backup->stop_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
        else
            elog(WARNING, "Invalid STOP_LSN \"%s\"", stop_lsn);
    }

    set_backup_status(status, backup);

    if (parent_backup)
    {
        backup->parent_backup = base36dec(parent_backup);
    }

    if (merge_dest_backup)
    {
        backup->merge_dest_backup = base36dec(merge_dest_backup);
    }

    if (program_version)
    {
        rc = strncpy_s(backup->program_version, sizeof(backup->program_version),program_version,
            sizeof(backup->program_version) - 1);
        securec_check_c(rc, "", "");
    }

    if (server_version)
    {
        rc = strncpy_s(backup->server_version, sizeof(backup->server_version),server_version,
            sizeof(backup->server_version) - 1);
        securec_check_c(rc, "", "");
    }

    if (compress_alg) {
        backup->compress_alg = parse_compress_alg(compress_alg);
    }

    if (storage_type) {
        backup->storage_type = str2dev(storage_type);
    }

    if (oss_status) {
        backup->oss_status = str2ossStatus(oss_status);
    }

    if (current.media_type == MEDIA_TYPE_OSS) {
        remove(path);
    }
    

finish:
    pg_free(backup_mode);
    pg_free(start_lsn);
    pg_free(stop_lsn);
    pg_free(status);
    pg_free(parent_backup);
    pg_free(merge_dest_backup);
    pg_free(program_version);
    pg_free(server_version);
    pg_free(compress_alg);
    pg_free(recovery_name);
    pg_free(storage_type);
    pg_free(oss_status);

    return backup;
}

BackupMode
parse_backup_mode(const char *value)
{
    const char *v = value;
    size_t  len;

    /* Skip all spaces detected */
    while (IsSpace(*v))
        v++;
    len = strlen(v);

    if (len > 0 && pg_strncasecmp("full", v, len) == 0)
        return BACKUP_MODE_FULL;
    else if (len > 0 && pg_strncasecmp("ptrack", v, len) == 0)
        return BACKUP_MODE_DIFF_PTRACK;

    /* Backup mode is invalid, so leave with an error */
    elog(ERROR, "invalid backup-mode \"%s\"", value);
    return BACKUP_MODE_INVALID;
}

MediaType
parse_media_type(const char *value)
{
    const char *v = value;
    size_t  len;

    /* Skip all spaces detected */
    while (IsSpace(*v))
        v++;
    len = strlen(v);

    if (len > 0 && pg_strncasecmp("s3", v, len) == 0)
        return MEDIA_TYPE_OSS;
    else if (len > 0 && pg_strncasecmp("disk", v, len) == 0)
        return MEDIA_TYPE_DISK;

    /* media type is invalid, so leave with an error */
    elog(ERROR, "invalid media_type \"%s\"", value);
    return MEDIA_TYPE_UNKNOWN;
}

const char *
deparse_backup_mode(BackupMode mode)
{
    switch (mode)
    {
        case BACKUP_MODE_FULL:
            return "full";
        case BACKUP_MODE_DIFF_PTRACK:
            return "ptrack";
        case BACKUP_MODE_INVALID:
            return "invalid";
    }

    return NULL;
}

CompressAlg
parse_compress_alg(const char *arg)
{
    size_t  len;

    /* Skip all spaces detected */
    while (isspace((unsigned char)*arg))
        arg++;
    len = strlen(arg);

    if (len == 0)
        elog(ERROR, "compress algorithm is empty");

    if (pg_strncasecmp("zlib", arg, len) == 0)
        return ZLIB_COMPRESS;
    else if (pg_strncasecmp("lz4", arg, len) == 0)
        return LZ4_COMPRESS;
    else if (pg_strncasecmp("zstd", arg, len) == 0)
        return ZSTD_COMPRESS;
    else if (pg_strncasecmp("pglz", arg, len) == 0)
        return PGLZ_COMPRESS;
    else if (pg_strncasecmp("none", arg, len) == 0)
        return NONE_COMPRESS;
    else
        elog(ERROR, "invalid compress algorithm value \"%s\"", arg);

    return NOT_DEFINED_COMPRESS;
}

const char*
deparse_compress_alg(int alg)
{
    switch (alg)
    {
        case NONE_COMPRESS:
        case NOT_DEFINED_COMPRESS:
            return "none";
        case ZLIB_COMPRESS:
            return "zlib";
        case PGLZ_COMPRESS:
            return "pglz";
        case LZ4_COMPRESS:
            return "lz4";
        case ZSTD_COMPRESS:
            return "zstd";
    }

    return NULL;
}

/*
 * Fill PGNodeInfo struct with default values.
 */
void
pgNodeInit(PGNodeInfo *node)
{
    node->block_size = 0;
    node->wal_block_size = 0;
    node->checksum_version = 0;

    node->is_superuser = false;
    node->pgpro_support = false;

    node->server_version = 0;
    node->server_version_str[0] = '\0';
}

/*
 * Fill pgBackup struct with default values.
 */
void
pgBackupInit(pgBackup *backup)
{
    backup->backup_id = INVALID_BACKUP_ID;
    backup->backup_mode = BACKUP_MODE_INVALID;
    backup->status = BACKUP_STATUS_INVALID;
    backup->tli = 0;
    backup->start_lsn = 0;
    backup->stop_lsn = 0;
    backup->start_time = (time_t) 0;
    backup->merge_time = (time_t) 0;
    backup->end_time = (time_t) 0;
    backup->recovery_xid = 0;
    backup->recovery_time = (time_t) 0;
    backup->expire_time = (time_t) 0;

    backup->data_bytes = BYTES_INVALID;
    backup->wal_bytes = BYTES_INVALID;
    backup->uncompressed_bytes = 0;
    backup->pgdata_bytes = 0;

    backup->compress_alg = COMPRESS_ALG_DEFAULT;
    backup->compress_level = COMPRESS_LEVEL_DEFAULT;

    backup->block_size = BLCKSZ;
    backup->wal_block_size = XLOG_BLCKSZ;
    backup->checksum_version = 0;

    backup->stream = false;
    backup->from_replica = false;
    backup->parent_backup = INVALID_BACKUP_ID;
    backup->merge_dest_backup = INVALID_BACKUP_ID;
    backup->parent_backup_link = NULL;
    backup->program_version[0] = '\0';
    backup->server_version[0] = '\0';
    backup->recovery_name[0] = '\0';
    backup->external_dir_str = NULL;
    backup->root_dir = NULL;
    backup->database_dir = NULL;
    backup->files = NULL;
    backup->note = NULL;
    backup->content_crc = 0;
    backup->dssdata_bytes = 0;
    backup->oss_status = OSS_STATUS_INVALID;
    backup->media_type = MEDIA_TYPE_UNKNOWN;
}

/* free pgBackup object */
void
pgBackupFree(void *backup)
{
    pgBackup *b = (pgBackup *) backup;

    pg_free(b->external_dir_str);
    pg_free(b->root_dir);
    pg_free(b->database_dir);
    pg_free(b->note);
    pg_free(backup);
}

/* Compare two pgBackup with their IDs (start time) in ascending order */
int
pgBackupCompareId(const void *l, const void *r)
{
    pgBackup *lp = *(pgBackup **)l;
    pgBackup *rp = *(pgBackup **)r;

    if (lp->start_time > rp->start_time)
        return 1;
    else if (lp->start_time < rp->start_time)
        return -1;
    else
        return 0;
}

/* Compare two pgBackup with their IDs in descending order */
int
pgBackupCompareIdDesc(const void *l, const void *r)
{
    return -pgBackupCompareId(l, r);
}

/*
 * Construct absolute path of the backup directory.
 * If subdir is not NULL, it will be appended after the path.
 */
void
pgBackupGetPath(const pgBackup *backup, char *path, size_t len, const char *subdir)
{
    pgBackupGetPath2(backup, path, len, subdir, NULL);
}

/*
 * Construct absolute path of the backup directory.
 * Append "subdir1" and "subdir2" to the backup directory.
 */
void
pgBackupGetPath2(const pgBackup *backup, char *path, size_t len,
                                    const char *subdir1, const char *subdir2)
{
    int nRet = 0;
    /* If "subdir1" is NULL do not check "subdir2" */
    if (!subdir1){
        nRet = snprintf_s(path, len, len - 1,"%s/%s", backup_instance_path,
            base36enc(backup->start_time));
        securec_check_ss_c(nRet, "\0", "\0");
    }
    else if (!subdir2) {
        nRet = snprintf_s(path, len,len - 1, "%s/%s/%s", backup_instance_path,
            base36enc(backup->start_time), subdir1);
        securec_check_ss_c(nRet, "\0", "\0");
    }
    /* "subdir1" and "subdir2" is not NULL */
    else {
        nRet = snprintf_s(path, len,len - 1, "%s/%s/%s/%s", backup_instance_path,
            base36enc(backup->start_time), subdir1, subdir2);
         securec_check_ss_c(nRet, "\0", "\0");
    }
}

/*
 * independent from global variable backup_instance_path
 * Still depends from backup_path
 */
void
pgBackupGetPathInInstance(const char *instance_name,
                            const pgBackup *backup, char *path, size_t len,
                            const char *subdir1, const char *subdir2)
{
    char    backup_instance_path[MAXPGPATH];
    int nRet = 0;

   nRet = snprintf_s(backup_instance_path, MAXPGPATH, MAXPGPATH - 1,"%s/%s/%s",
        backup_path, BACKUPS_DIR, instance_name);
   securec_check_ss_c(nRet, "\0", "\0");

    /* If "subdir1" is NULL do not check "subdir2" */
    if (!subdir1) {
        nRet = snprintf_s(path, len, len - 1,"%s/%s", backup_instance_path,
            base36enc(backup->start_time));
        securec_check_ss_c(nRet, "\0", "\0");
    }
    else if (!subdir2) {
        nRet = snprintf_s(path, len,len - 1, "%s/%s/%s", backup_instance_path,
            base36enc(backup->start_time), subdir1);
        securec_check_ss_c(nRet, "\0", "\0");
    }
    /* "subdir1" and "subdir2" is not NULL */
    else {
        nRet = snprintf_s(path, len, len - 1,"%s/%s/%s/%s", backup_instance_path,
            base36enc(backup->start_time), subdir1, subdir2);
        securec_check_ss_c(nRet, "\0", "\0");
    }
}

/*
 * Check if multiple backups consider target backup to be their direct parent
 */
bool
is_prolific(parray *backup_list, pgBackup *target_backup)
{
    int i;
    int child_counter = 0;

    for (i = 0; i < (int)parray_num(backup_list); i++)
    {
        pgBackup   *tmp_backup = (pgBackup *) parray_get(backup_list, i);

        /* consider only OK and DONE backups */
        if (tmp_backup->parent_backup == target_backup->start_time &&
            (tmp_backup->status == BACKUP_STATUS_OK ||
            tmp_backup->status == BACKUP_STATUS_DONE))
        {
            child_counter++;
            if (child_counter > 1)
                return true;
        }
    }

    return false;
}

/*
 * Find parent base FULL backup for current backup using parent_backup_link
 */
pgBackup*
find_parent_full_backup(pgBackup *current_backup)
{
    pgBackup   *base_full_backup = NULL;
    base_full_backup = current_backup;

    /* sanity */
    if (!current_backup)
        elog(ERROR, "Target backup cannot be NULL");

    while (base_full_backup->parent_backup_link != NULL)
    {
        base_full_backup = base_full_backup->parent_backup_link;
    }

    if (base_full_backup->backup_mode != BACKUP_MODE_FULL)
    {
        if (base_full_backup->parent_backup)
            elog(WARNING, "Backup %s is missing",
                base36enc(base_full_backup->parent_backup));
        else
            elog(WARNING, "Failed to find parent FULL backup for %s",
                base36enc(current_backup->start_time));
        return NULL;
    }

    return base_full_backup;
}

/*
 * Iterate over parent chain and look for any problems.
 * Return 0 if chain is broken.
 *  result_backup must contain oldest existing backup after missing backup.
 *  we have no way to know if there are multiple missing backups.
 * Return 1 if chain is intact, but at least one backup is !OK.
 *  result_backup must contain oldest !OK backup.
 * Return 2 if chain is intact and all backups are OK.
 *	result_backup must contain FULL backup on which chain is based.
 */
int
scan_parent_chain(pgBackup *current_backup, pgBackup **result_backup)
{
    pgBackup   *target_backup = NULL;
    pgBackup   *invalid_backup = NULL;

    if (!current_backup)
        elog(ERROR, "Target backup cannot be NULL");

    target_backup = current_backup;

    while (target_backup->parent_backup_link)
    {
        if (target_backup->status != BACKUP_STATUS_OK &&
            target_backup->status != BACKUP_STATUS_DONE)
        /* oldest invalid backup in parent chain */
        invalid_backup = target_backup;


        target_backup = target_backup->parent_backup_link;
    }

    /* Previous loop will skip FULL backup because his parent_backup_link is NULL */
    if (target_backup->backup_mode == BACKUP_MODE_FULL &&
        (target_backup->status != BACKUP_STATUS_OK &&
         target_backup->status != BACKUP_STATUS_DONE))
    {
        invalid_backup = target_backup;
    }

    /* found chain end and oldest backup is not FULL */
    if (target_backup->backup_mode != BACKUP_MODE_FULL)
    {
        /* Set oldest child backup in chain */
        *result_backup = target_backup;
        return ChainIsBroken;
    }

    /* chain is ok, but some backups are invalid */
    if (invalid_backup)
    {
        *result_backup = invalid_backup;
        return ChainIsInvalid;
    }

    *result_backup = target_backup;
    return ChainIsOk;
}

/*
 * Determine if child_backup descend from parent_backup
 * This check DO NOT(!!!) guarantee that parent chain is intact,
 * because parent_backup can be missing.
 * If inclusive is true, then child_backup counts as a child of himself
 * if parent_backup_time is start_time of child_backup.
 */
bool
is_parent(time_t parent_backup_time, pgBackup *child_backup, bool inclusive)
{
    if (!child_backup)
        elog(ERROR, "Target backup cannot be NULL");

    if (inclusive && child_backup->start_time == parent_backup_time)
        return true;

    while (child_backup->parent_backup_link &&
        child_backup->parent_backup != parent_backup_time)
    {
        child_backup = child_backup->parent_backup_link;
    }

    if (child_backup->parent_backup == parent_backup_time)
        return true;

    

    return false;
}

/*
 * Return backup index number.
 * Note: this index number holds true until new sorting of backup list
 */
int
get_backup_index_number(parray *backup_list, pgBackup *backup)
{
    int i;

    for (i = 0; i < (int)parray_num(backup_list); i++)
    {
        pgBackup   *tmp_backup = (pgBackup *) parray_get(backup_list, i);

        if (tmp_backup->start_time == backup->start_time)
            return i;
    }
    elog(WARNING, "Failed to find backup %s", base36enc(backup->start_time));
    return -1;
}

/* On backup_list lookup children of target_backup and append them to append_list */
void
append_children(parray *backup_list, pgBackup *target_backup, parray *append_list)
{
    int i;

    for (i = 0; i < (int)parray_num(backup_list); i++)
    {
        pgBackup *backup = (pgBackup *) parray_get(backup_list, i);

        /* check if backup is descendant of target backup */
        if (is_parent(target_backup->start_time, backup, false))
        {
            /* if backup is already in the list, then skip it */
            if (!parray_contains(append_list, backup))
                parray_append(append_list, backup);
        }
    }
}

/*
 *  * relpathbackend - construct path to a relation's file
 *   *
 *    * Result is a palloc'd string.
 *     */
char* relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
    char* path = NULL;

    return path;
}

/* decrypt and then uncompress the directory */
static void uncompress_decrypt_directory(const char *instance_name_str)
{
    errno_t rc;
    DIR *data_dir = NULL;
    struct dirent *data_ent = NULL;
    uint key_len = 0;
    uint hmac_len = MAX_HMAC_LEN;
    uint dec_buffer_len = 0;
    uint out_buffer_len = MAX_CRYPTO_MODULE_LEN;
    long int enc_file_pos = 0;
    long int enc_file_len = 0;
    char sys_cmd[MAXPGPATH] = {0};
    char* key = NULL;
    unsigned char hmac_read_buffer[MAX_HMAC_LEN +1] = {0};
    unsigned char hmac_cal_buffer[MAX_HMAC_LEN +1] = {0};
    unsigned char dec_buffer[MAX_CRYPTO_MODULE_LEN + 1] = {0};
    unsigned char out_buffer[MAX_CRYPTO_MODULE_LEN + 1] = {0};
    char enc_backup_file[MAXPGPATH] = {0};
    char dec_backup_file[MAXPGPATH] = {0};
    char backup_instance_path[MAXPGPATH] = {0};
    int algo;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    if (NULL == encrypt_dev_params) {
        return;
    }
    
    CryptoModuleParamsCheck(gen_key, encrypt_dev_params, encrypt_mode, encrypt_key, encrypt_salt, &key_type);

    initCryptoSession(&crypto_module_session);

    algo = transform_type(encrypt_mode);
    if (gen_key) {
        elog(ERROR, "only backup can use --gen-key command.");
    } else {
        key = SEC_decodeBase64(encrypt_key, &key_len);
        if (NULL == key) {
            clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
            elog(ERROR, "crypto module decode key error, please check --with-key.");
        }
    }

    rc = crypto_ctx_init_use(crypto_module_session, &crypto_module_keyctx, (ModuleSymmKeyAlgo)algo, 0, (unsigned char*)key, key_len);
	if (rc != 1) {
        pg_free(key);
		crypto_get_errmsg_use(NULL, errmsg);
        clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
		elog(ERROR, "crypto keyctx init error, errmsg:%s\n", errmsg);
    }

    algo = transform_hmac_type(encrypt_mode);
    if (algo != MODULE_ALGO_MAX) {
        rc = crypto_hmac_init_use(crypto_module_session, &crypto_hmac_keyctx, (ModuleSymmKeyAlgo)algo, (unsigned char*)key, key_len);
        if (rc != 1) {
            crypto_get_errmsg_use(NULL, errmsg);
            clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
            elog(ERROR, "crypto keyctx init error, errmsg:%s\n", errmsg);
        }
    }

    rc = sprintf_s(backup_instance_path, MAXPGPATH, "%s/%s/%s",
                                    backup_path, BACKUPS_DIR, instance_name_str);
    securec_check_ss_c(rc, "\0", "\0");

    data_dir = fio_opendir(backup_instance_path, FIO_BACKUP_HOST);
    if (data_dir == NULL) {
        pg_free(key);
        clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
        elog(ERROR, "cannot open directory \"%s\": %s", backup_instance_path, strerror(errno));
        return;
    }

    for(;(data_ent = fio_readdir(data_dir)) != NULL; errno = 0)
    {
        if (strstr(data_ent->d_name,"_enc")) {
            rc = sprintf_s(enc_backup_file, MAXPGPATH, "%s/%s", backup_instance_path, data_ent->d_name);
            securec_check_ss_c(rc, "\0", "\0");

            FILE* enc_backup_fd = fopen(enc_backup_file,"rb");
            if(NULL == enc_backup_fd) {
                pg_free(key);
                clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
                elog(ERROR, ("failed to create or open encrypt backup file."));
                return;
            }

            fseek(enc_backup_fd,0,SEEK_END);
            enc_file_len = ftell(enc_backup_fd);

            fseek(enc_backup_fd, 0, SEEK_SET);

            enc_file_pos = ftell(enc_backup_fd);

            rc = sprintf_s(dec_backup_file, MAXPGPATH, "%s/%s.tar", backup_instance_path, data_ent->d_name);
            securec_check_ss_c(rc, "\0", "\0");

            FILE* dec_file_fd = fopen(dec_backup_file,"wb");

            if (algo == MODULE_ALGO_MAX)
            {
                while(enc_file_pos < enc_file_len)
                {
                    memset_s(dec_buffer, MAX_CRYPTO_MODULE_LEN, 0, MAX_CRYPTO_MODULE_LEN);
                    memset_s(out_buffer, MAX_CRYPTO_MODULE_LEN, 0, MAX_CRYPTO_MODULE_LEN);

                    if(enc_file_pos + MAX_CRYPTO_MODULE_LEN < enc_file_len) {
                        fread(dec_buffer, 1, MAX_CRYPTO_MODULE_LEN, enc_backup_fd);
                        dec_buffer_len = MAX_CRYPTO_MODULE_LEN;
                        enc_file_pos += MAX_CRYPTO_MODULE_LEN;
                    } else {
                        fread(dec_buffer, 1, enc_file_len - enc_file_pos, enc_backup_fd);
                        dec_buffer_len = enc_file_len - enc_file_pos;
                        enc_file_pos = enc_file_len;
                    }

                    rc = crypto_encrypt_decrypt_use(crypto_module_keyctx, 0, (unsigned char*)dec_buffer, dec_buffer_len,
                                (unsigned char*)encrypt_salt, MAX_IV_LEN, (unsigned char*)out_buffer, (size_t*)&out_buffer_len, NULL);
                    if(rc != 1) {
                        crypto_get_errmsg_use(NULL, errmsg);
                        pg_free(key);
                        clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
                        elog(ERROR, ("failed to decrypt enc_backup_file, errmsg: %s"), errmsg);
                    }
                    fwrite(out_buffer, 1, out_buffer_len, dec_file_fd);
                }
            } else {
                while(enc_file_pos < enc_file_len)
                {
                    memset_s(dec_buffer, MAX_CRYPTO_MODULE_LEN, 0, MAX_CRYPTO_MODULE_LEN);
                    memset_s(out_buffer, MAX_CRYPTO_MODULE_LEN, 0, MAX_CRYPTO_MODULE_LEN);

                    if(enc_file_pos + MAX_CRYPTO_MODULE_LEN + MAX_HMAC_LEN < enc_file_len) {
                        fread(dec_buffer, 1, MAX_CRYPTO_MODULE_LEN, enc_backup_fd);
                        fread(hmac_read_buffer, 1, MAX_HMAC_LEN, enc_backup_fd);
                        dec_buffer_len = MAX_CRYPTO_MODULE_LEN;
                        enc_file_pos += (MAX_CRYPTO_MODULE_LEN + MAX_HMAC_LEN);
                    } else {
                        fread(dec_buffer, 1, enc_file_len - (enc_file_pos + MAX_HMAC_LEN), enc_backup_fd);
                        fread(hmac_read_buffer, 1, MAX_HMAC_LEN, enc_backup_fd);
                        dec_buffer_len = enc_file_len - (enc_file_pos + MAX_HMAC_LEN);
                        enc_file_pos = enc_file_len;
                    }

                    rc = crypto_encrypt_decrypt_use(crypto_module_keyctx, 0, (unsigned char*)dec_buffer, dec_buffer_len,
                                (unsigned char*)encrypt_salt, MAX_IV_LEN, (unsigned char*)out_buffer, (size_t*)&out_buffer_len, NULL);
                    if(rc != 1) {
                        pg_free(key);
                        crypto_get_errmsg_use(NULL, errmsg);
                        clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
                        elog(ERROR, ("failed to decrypt enc_backup_file, errmsg: %s"), errmsg);
                    }

                    rc = crypto_hmac_use(crypto_hmac_keyctx, (unsigned char*)out_buffer, out_buffer_len, hmac_cal_buffer, (size_t*)&hmac_len);
                    if(rc != 1) {
                        pg_free(key);
                        crypto_get_errmsg_use(NULL, errmsg);
                        clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);
                        elog(ERROR, ("failed to calculate hmac, errmsg: %s"), errmsg);
                    }

                    if (strncmp((char*)hmac_cal_buffer, (char*)hmac_read_buffer, (size_t)hmac_len) != 0) {
                        pg_free(key);
                        elog(ERROR, ("hmac verify failed\n"));
                    }

                    fwrite(out_buffer, 1, out_buffer_len, dec_file_fd);
                }
            }
            fclose(dec_file_fd);
            fclose(enc_backup_fd);
            clearCrypto(crypto_module_session, crypto_module_keyctx, crypto_hmac_keyctx);

            rc = sprintf_s(sys_cmd, MAXPGPATH, "tar -xPf %s/%s.tar", backup_instance_path, data_ent->d_name);

            if (!is_valid_cmd(sys_cmd)) {
                elog(ERROR, "cmd is rejected");
                return;
            }
            system(sys_cmd);
            rc = memset_s(sys_cmd, MAXPGPATH,0, MAXPGPATH);
            securec_check(rc, "\0", "\0");

            rc = sprintf_s(sys_cmd, MAXPGPATH, "rm -rf %s/%s.tar", backup_instance_path, data_ent->d_name);

            if (!is_valid_cmd(sys_cmd)) {
                elog(ERROR, "cmd is rejected");
                return;
            }
            system(sys_cmd);
            rc = memset_s(sys_cmd, MAXPGPATH,0, MAXPGPATH);
            securec_check(rc, "\0", "\0");
            enc_flag = true;
        }
    }

    if (data_dir) {
        fio_closedir(data_dir);
        data_dir = NULL;
    }
    pg_free(key);
}

/*
 * Function: delete_one_instance_backup_directory
 *
 * Return:
 *  void
 */
static void delete_one_instance_backup_directory(char *instance_name_str)
{
    char backup_instance_path[MAXPGPATH];
    DIR *data_dir = NULL;
    struct dirent *data_ent = NULL;
    char sys_cmd[MAXPGPATH] = {0};

    errno_t rc = sprintf_s(backup_instance_path, MAXPGPATH, "%s/%s/%s",
                                    backup_path, BACKUPS_DIR, instance_name_str);
    securec_check_ss_c(rc, "\0", "\0");

    data_dir = fio_opendir(backup_instance_path, FIO_BACKUP_HOST);
    if (data_dir == NULL)
    {
        elog(ERROR, "cannot open directory \"%s\": %s", backup_instance_path,
                strerror(errno));
        return;
    }

    for (;(data_ent = fio_readdir(data_dir)) != NULL; errno = 0)
    {
        if (IsDir(backup_instance_path, data_ent->d_name, FIO_BACKUP_HOST) && data_ent->d_name[0] != '.') {
            error_t rc = sprintf_s(sys_cmd, MAXPGPATH, "rm %s/%s -rf", backup_instance_path,data_ent->d_name);
            securec_check_ss_c(rc, "\0", "\0");
            if (!is_valid_cmd(sys_cmd)) {
                elog(ERROR, "cmd is rejected");
                return;
            }

            system(sys_cmd);
            rc = memset_s(sys_cmd, MAXPGPATH,0, MAXPGPATH);
            securec_check(rc, "\0", "\0");
        }
    }

    if (data_dir) {
        fio_closedir(data_dir);
        data_dir = NULL;
    }

}

/*
 * Function: delete_backup_directory
 * Description:
 *
 * Input:
 *  char *instance_name
 * Return:
 *  void
 */
void delete_backup_directory(char *instance_name_str)
{
    int i = 0;
    if(NULL == encrypt_dev_params || !enc_flag) {
        return;
    }

    if (instance_name_str == NULL) {
        parray *instances = catalog_get_instance_list();

        for (i = 0; i < parray_num(instances); i++)
        {
            InstanceConfig *instance = (InstanceConfig *)parray_get(instances, i);
            delete_one_instance_backup_directory(instance->name);
        }
        parray_walk(instances, pfree);
        parray_free(instances);

        return;
    }

    delete_one_instance_backup_directory(instance_name_str);

}

