/*-------------------------------------------------------------------------
 *
 * validate.c: validate backup files.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <dirent.h>

#include "thread.h"
#include "common/fe_memutils.h"
#include "storage/file/fio_device.h"
#include "logger.h"

static void *pgBackupValidateFiles(void *arg);
static void do_validate_instance(void);

static bool corrupted_backup_found = false;
static bool skipped_due_to_lock = false;

typedef struct
{
    const char *base_path;
    const char *dss_path;
    parray        *files;
    bool        corrupted;
    XLogRecPtr     stop_lsn;
    uint32        checksum_version;
    uint32        backup_version;
    BackupMode    backup_mode;
    const char    *external_prefix;
    HeaderMap   *hdr_map;

    /*
     * Return value from the thread.
     * 0 means there is no error, 1 - there is an error.
     */
    int            ret;
} validate_files_arg;

/* Progress Counter */
static int g_inregularFiles = 0;
static int g_doneFiles = 0;
static int g_totalFiles = 0;
static volatile bool g_progressFlag = false;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

static void *ProgressReportValidate(void *arg)
{
    if (g_totalFiles == 0) {
        return nullptr;
    }
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(g_doneFiles * 100 / g_totalFiles);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). validate file \r",
            progressBar, percent, g_doneFiles, g_totalFiles);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_cond, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }
    } while (((g_doneFiles + g_inregularFiles) < g_totalFiles) && !g_progressFlag);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). validate file \n",
        progressBar, percent, g_totalFiles, g_totalFiles);
    return nullptr;
}

bool pre_check_backup(pgBackup *backup)
{
    /* Check backup program version */
    if (parse_program_version(backup->program_version) > parse_program_version(PROGRAM_VERSION))
        elog(ERROR, "gs_probackup binary version is %s, but backup %s version is %s. "
            "gs_probackup do not guarantee to be forward compatible. "
            "Please upgrade gs_probackup binary.",
                PROGRAM_VERSION, base36enc(backup->start_time), backup->program_version);

    /* Check backup server version */
    if (strcmp(backup->server_version, PG_MAJORVERSION) != 0)
           elog(ERROR, "Backup %s has server version %s, but current gs_probackup binary "
                    "compiled with server version %s",
                base36enc(backup->start_time), backup->server_version, PG_MAJORVERSION);

    if (backup->status == BACKUP_STATUS_RUNNING)
    {
        elog(WARNING, "Backup %s has status %s, change it to ERROR and skip validation",
             base36enc(backup->start_time), status2str(backup->status));
        write_backup_status(backup, BACKUP_STATUS_ERROR, instance_name, true);
        corrupted_backup_found = true;
        return false;
    }

    /* Revalidation is attempted for DONE, ORPHAN and CORRUPT backups */
    if (backup->status != BACKUP_STATUS_OK &&
        backup->status != BACKUP_STATUS_DONE &&
        backup->status != BACKUP_STATUS_ORPHAN &&
        backup->status != BACKUP_STATUS_MERGING &&
        backup->status != BACKUP_STATUS_CORRUPT)
    {
        elog(WARNING, "Backup %s has status %s. Skip validation.",
                    base36enc(backup->start_time), status2str(backup->status));
        corrupted_backup_found = true;
        return false;
    }

    /* additional sanity */
    if (backup->backup_mode == BACKUP_MODE_FULL &&
        backup->status == BACKUP_STATUS_MERGING)
    {
        elog(WARNING, "Full backup %s has status %s, skip validation",
            base36enc(backup->start_time), status2str(backup->status));
        return false;
    }

    /* Check backup storage mode suitable */
    if (IsDssMode() != is_dss_type(backup->storage_type))
    {
        elog(WARNING, "Backup %s is not suit for instance %s, because they have different "
            "storage type. Change it to CORRUPT and skip validation.",
            base36enc((long unsigned int)backup->start_time), instance_name);
        write_backup_status(backup, BACKUP_STATUS_CORRUPT, instance_name, true);
        return false;
    }

    if (backup->status == BACKUP_STATUS_OK || backup->status == BACKUP_STATUS_DONE ||
        backup->status == BACKUP_STATUS_MERGING)
        elog(INFO, "Validating backup %s", base36enc(backup->start_time));
    else
        elog(INFO, "Revalidating backup %s", base36enc(backup->start_time));

    if (backup->backup_mode != BACKUP_MODE_FULL &&
        backup->backup_mode != BACKUP_MODE_DIFF_PTRACK)
        elog(WARNING, "Invalid backup_mode of backup %s", base36enc(backup->start_time));
    
    return true;
}

/*
 * Validate backup files.
 * TODO: partial validation.
 */
void
pgBackupValidate(pgBackup *backup, pgRestoreParams *params)
{
    char        base_path[MAXPGPATH];
    char        dss_path[MAXPGPATH];
    char        external_prefix[MAXPGPATH];
    parray       *files = NULL;
    bool        corrupted = false;
    bool        validation_isok = true;
    /* arrays with meta info for multi threaded validate */
    pthread_t  *threads = NULL;
    validate_files_arg *threads_args = NULL;
    int            i;

    if (!pre_check_backup(backup))
        return;

    join_path_components(base_path, backup->root_dir, DATABASE_DIR);
    join_path_components(dss_path, backup->root_dir, DSSDATA_DIR);
    join_path_components(external_prefix, backup->root_dir, EXTERNAL_DIR);
    files = get_backup_filelist(backup, false);
    g_totalFiles = parray_num(files);

    if (!files)
    {
        elog(WARNING, "Backup %s file list is corrupted", base36enc(backup->start_time));
        backup->status = BACKUP_STATUS_CORRUPT;
        write_backup_status(backup, BACKUP_STATUS_CORRUPT, instance_name, true);
        return;
    }

    /* setup threads */
    for (i = 0; i < g_totalFiles; i++)
    {
        pgFile       *file = (pgFile *) parray_get(files, i);
        pg_atomic_clear_flag(&file->lock);
    }

    /* init thread args with own file lists */
    threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
    if (threads == NULL) {
        elog(ERROR, "Out of memory");
    }
    threads_args = (validate_files_arg *)
        palloc(sizeof(validate_files_arg) * num_threads);
    if (threads_args == NULL) {
        elog(ERROR, "Out of memory");
    }
    
    elog(INFO, "Begin validate file");
    pthread_t progressThread;
    pthread_create(&progressThread, nullptr, ProgressReportValidate, nullptr);

    /* Validate files */
    thread_interrupted = false;
    for (i = 0; i < num_threads; i++)
    {
        validate_files_arg *arg = &(threads_args[i]);

        arg->base_path = base_path;
        arg->dss_path = dss_path;
        arg->files = files;
        arg->corrupted = false;
        arg->backup_mode = backup->backup_mode;
        arg->stop_lsn = backup->stop_lsn;
        arg->checksum_version = backup->checksum_version;
        arg->backup_version = parse_program_version(backup->program_version);
        arg->external_prefix = external_prefix;
        arg->hdr_map = &(backup->hdr_map);
        /* By default there are some error */
        threads_args[i].ret = 1;

        pthread_create(&threads[i], NULL, pgBackupValidateFiles, arg);
    }

    /* Wait theads */
    for (i = 0; i < num_threads; i++)
    {
        validate_files_arg *arg = &(threads_args[i]);

        pthread_join(threads[i], NULL);
        if (arg->corrupted)
            corrupted = true;
        if (arg->ret == 1)
            validation_isok = false;
    }
    if (!validation_isok)
        elog(ERROR, "Data files validation failed");

    pfree(threads);
    pfree(threads_args);

    g_progressFlag = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, nullptr);
    elog(INFO, "Finish validate file. ");

    /* cleanup */
    parray_walk(files, pgFileFree);
    parray_free(files);
    cleanup_header_map(&(backup->hdr_map));

    /* Update backup status */
    if (corrupted)
        backup->status = BACKUP_STATUS_CORRUPT;
    write_backup_status(backup, corrupted ? BACKUP_STATUS_CORRUPT :
                                            BACKUP_STATUS_OK, instance_name, true);

    if (corrupted)
        elog(WARNING, "Backup %s data files are corrupted", base36enc(backup->start_time));
    else
        elog(INFO, "Backup %s data files are valid", base36enc(backup->start_time));

    /* Issue #132 kludge */
    if (!corrupted &&
        ((parse_program_version(backup->program_version) == 20104)||
         (parse_program_version(backup->program_version) == 20105)||
         (parse_program_version(backup->program_version) == 20201)))
    {
        char path[MAXPGPATH];

        
        join_path_components(path, backup->root_dir, DATABASE_FILE_LIST);

        if (pgFileSize(path) >= (BLCKSZ*500))
        {
            elog(WARNING, "Backup %s is a victim of metadata corruption. "
                            "Additional information can be found here: "
                            "https://github.com/postgrespro/pg_probackup/issues/132",
                            base36enc(backup->start_time));
            backup->status = BACKUP_STATUS_CORRUPT;
            write_backup_status(backup, BACKUP_STATUS_CORRUPT, instance_name, true);
        }

    }
}

void check_crc(pgFile *file, char *file_fullpath, validate_files_arg *arguments)
{
    pg_crc32    crc;
    
    /*
     * If option skip-block-validation is set, compute only file-level CRC for
     * datafiles, otherwise check them block by block.
     * Currently we don't compute checksums for
     * cfs_compressed data files, so skip block validation for them.
     */
    if (!file->is_datafile || skip_block_validation || file->is_cfs)
    {
        /*
         * Pre 2.0.22 we use CRC-32C, but in newer version of pg_probackup we
         * use CRC-32.
         *
         * pg_control stores its content and checksum of the content, calculated
         * using CRC-32C. If we calculate checksum of the whole pg_control using
         * CRC-32C we get same checksum constantly. It might be because of the
         * CRC-32C algorithm.
         * To avoid this problem we need to use different algorithm, CRC-32 in
         * this case.
         *
         * Starting from 2.0.25 we calculate crc of pg_control differently.
         */
        if (arguments->backup_version >= 20025 &&
            strcmp(file->name, "pg_control") == 0 &&
            !file->external_dir_num)
            crc = get_pgcontrol_checksum(file_fullpath);
        else
            crc = pgFileGetCRC(file_fullpath,
                               arguments->backup_version <= 20021 ||
                               arguments->backup_version >= 20025,
                               false);
        if (crc != file->crc)
        {
            elog(WARNING, "Invalid CRC of backup file \"%s\" : %X. Expected %X",
                    file_fullpath, crc, file->crc);
            // arguments->corrupted = true;
        }
    }
    else
    {
        /*
         * validate relation block by block
         * check page headers, checksums (if enabled)
         * and compute checksum of the file
         */
        if (!validate_file_pages(file, file_fullpath, arguments->stop_lsn,
                              arguments->checksum_version,
                              arguments->backup_version,
                              arguments->hdr_map))
            arguments->corrupted = true;
    }
}

/*
 * Validate files in the backup.
 * NOTE: If file is not valid, do not use ERROR log message,
 * rather throw a WARNING and set arguments->corrupted = true.
 * This is necessary to update backup status.
 */
static void *
pgBackupValidateFiles(void *arg)
{
    int            i;
    validate_files_arg *arguments = (validate_files_arg *)arg;
    int            num_files = parray_num(arguments->files);
    int inregularFilesLocal = 0;
    for (i = 0; i < num_files; i++)
    {
        struct stat st;
        pgFile       *file = (pgFile *) parray_get(arguments->files, i);
        char        file_fullpath[MAXPGPATH];

        if (interrupted || thread_interrupted)
            elog(ERROR, "Interrupted during validate");

        /* Validate only regular files */
        if (!S_ISREG(file->mode)) {
            inregularFilesLocal++;
            continue;
        }
            

        if (!pg_atomic_test_set_flag(&file->lock))
            continue;
        
        pg_atomic_add_fetch_u32((volatile uint32*) &g_doneFiles, 1);
        if (progress)
            elog_file(INFO, "Progress: (%d/%d). Validate file \"%s\"",
                i + 1, num_files, file->rel_path);

        /*
         * Skip files which has no data, because they
         * haven't changed between backups.
         */
        if (file->write_size == BYTES_INVALID)
        {
            /* TODO: lookup corresponding merge bug */
            if (arguments->backup_mode == BACKUP_MODE_FULL)
            {
                /* It is illegal for file in FULL backup to have BYTES_INVALID */
                elog(WARNING, "Backup file \"%s\" has invalid size. Possible metadata corruption.",
                    file->rel_path);
                arguments->corrupted = true;
                break;
            }
            else
                continue;
        }

        /* no point in trying to open empty file */
        if (file->write_size == 0)
            continue;

        if (file->external_dir_num)
        {
            char temp[MAXPGPATH];

            makeExternalDirPathByNum(temp, arguments->external_prefix, file->external_dir_num);
            join_path_components(file_fullpath, temp, file->rel_path);
        }
        else if (is_dss_type(file->type))
            join_path_components(file_fullpath, arguments->dss_path, file->rel_path);
        else
            join_path_components(file_fullpath, arguments->base_path, file->rel_path);

        /* TODO: it is redundant to check file existence using stat */
        if (stat(file_fullpath, &st) == -1)
        {
            if (errno == ENOENT)
                elog(WARNING, "Backup file \"%s\" is not found", file_fullpath);
            else
                elog(WARNING, "Cannot stat backup file \"%s\": %s",
                    file_fullpath, strerror(errno));
            arguments->corrupted = true;
            break;
        }

        if (file->write_size != st.st_size)
        {
            elog(WARNING, "Invalid size of backup file \"%s\" : " INT64_FORMAT ". Expected %lu",
                 file_fullpath, (unsigned long) st.st_size, file->write_size);
            arguments->corrupted = true;
            break;
        }

        check_crc(file, file_fullpath, arguments);        
    }
    pg_atomic_write_u32((volatile uint32*) &g_inregularFiles, inregularFilesLocal);

    /* Data files validation is successful */
    arguments->ret = 0;

    return NULL;
}

/*
 * Validate all backups in the backup catalog.
 * If --instance option was provided, validate only backups of this instance.
 */
int
do_validate_all(void)
{
    corrupted_backup_found = false;
    skipped_due_to_lock = false;

    if (instance_name == NULL)
    {
        /* Show list of instances */
        char        path[MAXPGPATH];
        DIR           *dir;
        struct dirent *dent;

        /* open directory and list contents */
        join_path_components(path, backup_path, BACKUPS_DIR);
        dir = opendir(path);
        if (dir == NULL)
            elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));

        errno = 0;
        while ((dent = readdir(dir)))
        {
            char        conf_path[MAXPGPATH];
            char        child[MAXPGPATH];
            struct stat    st;

            /* skip entries point current dir or parent dir */
            if (strcmp(dent->d_name, ".") == 0 ||
                strcmp(dent->d_name, "..") == 0)
                continue;

            join_path_components(child, path, dent->d_name);

            if (lstat(child, &st) == -1)
                elog(ERROR, "cannot stat file \"%s\": %s", child, strerror(errno));

            if (!S_ISDIR(st.st_mode))
                continue;

            /*
             * Initialize instance configuration.
             */
            instance_name = dent->d_name;
            errno_t rc = sprintf_s(backup_instance_path, MAXPGPATH, "%s/%s/%s",
                    backup_path, BACKUPS_DIR, instance_name);
            securec_check_ss_c(rc, "\0", "\0");
            rc = sprintf_s(arclog_path, MAXPGPATH, "%s/%s/%s", backup_path, "wal", instance_name);
            securec_check_ss_c(rc, "\0", "\0");
            join_path_components(conf_path, backup_instance_path,
                                 BACKUP_CATALOG_CONF_FILE);
            if (config_read_opt(conf_path, instance_options, ERROR, false,
                                true) == 0)
            {
                elog(WARNING, "Configuration file \"%s\" is empty", conf_path);
                corrupted_backup_found = true;
                continue;
            }

            do_validate_instance();
        }
        
        if (closedir(dir)) {
            elog(ERROR, "Cannot close directory \"%s\": %s",
                 path, strerror(errno));
        }
    }
    else
    {
        do_validate_instance();
    }

    /* TODO: Probably we should have different exit code for every condition
     * and they combination:
     *  0 - all backups are valid
     *  1 - some backups are corrupt
     *  2 - some backups where skipped due to concurrent locks
     *  3 - some backups are corrupt and some are skipped due to concurrent locks
     */

    if (skipped_due_to_lock)
        elog(WARNING, "Some backups weren't locked and they were skipped");

    if (corrupted_backup_found)
    {
        elog(WARNING, "Some backups are not valid");
        return 1;
    }

    if (!skipped_due_to_lock && !corrupted_backup_found)
        elog(INFO, "All backups are valid");

    return 0;
}

bool find_ancestor_backup(pgBackup *current_backup, pgBackup **base_full_backup)
{
    if (current_backup->backup_mode != BACKUP_MODE_FULL)
    {
        pgBackup   *tmp_backup = NULL;
        int result;

        result = scan_parent_chain(current_backup, &tmp_backup);

        /* chain is broken */
        if (result == ChainIsBroken)
        {
            char       *parent_backup_id;
            /* determine missing backup ID */

            parent_backup_id = base36enc_dup(tmp_backup->parent_backup);
            corrupted_backup_found = true;

            /* orphanize current_backup */
            if (current_backup->status == BACKUP_STATUS_OK ||
                current_backup->status == BACKUP_STATUS_DONE)
            {
                write_backup_status(current_backup, BACKUP_STATUS_ORPHAN, instance_name, true);
                elog(WARNING, "Backup %s is orphaned because his parent %s is missing",
                        base36enc(current_backup->start_time),
                        parent_backup_id);
            }
            else
            {
                elog(WARNING, "Backup %s has missing parent %s",
                    base36enc(current_backup->start_time), parent_backup_id);
            }
            pg_free(parent_backup_id);
            return false;
        }
        /* chain is whole, but at least one parent is invalid */
        else if (result == ChainIsInvalid)
        {
            /* Oldest corrupt backup has a chance for revalidation */
            if (current_backup->start_time != tmp_backup->start_time)
            {
                char       *backup_id = base36enc_dup(tmp_backup->start_time);
                /* orphanize current_backup */
                if (current_backup->status == BACKUP_STATUS_OK ||
                    current_backup->status == BACKUP_STATUS_DONE)
                {
                    write_backup_status(current_backup, BACKUP_STATUS_ORPHAN, instance_name, true);
                    elog(WARNING, "Backup %s is orphaned because his parent %s has status: %s",
                            base36enc(current_backup->start_time), backup_id,
                            status2str(tmp_backup->status));
                }
                else
                {
                    elog(WARNING, "Backup %s has parent %s with status: %s",
                            base36enc(current_backup->start_time), backup_id,
                            status2str(tmp_backup->status));
                }
                pg_free(backup_id);
                return false;
            }
            *base_full_backup = find_parent_full_backup(current_backup);

            /* sanity */
            if (!*base_full_backup)
                elog(ERROR, "Parent full backup for the given backup %s was not found",
                        base36enc(current_backup->start_time));
        }
        /* chain is whole, all parents are valid at first glance,
         * current backup validation can proceed
         */
        else
            *base_full_backup = tmp_backup;
    }
    else
        *base_full_backup = current_backup;
    
    return true;
}
/*
 * Validate all backups in the given instance of the backup catalog.
 */
static void
do_validate_instance(void)
{
    int            i;
    int            j;
    parray       *backups;
    pgBackup   *current_backup = NULL;

    elog(INFO, "Validate backups of the instance '%s'", instance_name);

    /* Get list of all backups sorted in order of descending start time */
    backups = catalog_get_backup_list(instance_name, INVALID_BACKUP_ID);

    /* Examine backups one by one and validate them */
    for (i = 0; (size_t)i < parray_num(backups); i++)
    {
        pgBackup   *base_full_backup;

        current_backup = (pgBackup *) parray_get(backups, i);

        /* Find ancestor for incremental backup */
        if (!find_ancestor_backup(current_backup, &base_full_backup))
            continue;

        /* Do not interrupt, validate the next backup */
        if (!lock_backup(current_backup, true))
        {
            elog(WARNING, "Cannot lock backup %s directory, skip validation",
                 base36enc(current_backup->start_time));
            skipped_due_to_lock = true;
            continue;
        }
        /* Valiate backup files*/
        pgBackupValidate(current_backup, NULL);

        /* Validate corresponding WAL files */
        if (current_backup->status == BACKUP_STATUS_OK)
            validate_wal(current_backup, arclog_path, 0,
                         0, 0, base_full_backup->tli,
                         instance_config.xlog_seg_size);

        /*
         * Mark every descendant of corrupted backup as orphan
         */
        if (current_backup->status != BACKUP_STATUS_OK)
        {
            char       *current_backup_id;
            /* This is ridiculous but legal.
             * PAGE_b2 <- OK
             * PAGE_a2 <- OK
             * PAGE_b1 <- ORPHAN
             * PAGE_a1 <- CORRUPT
             * FULL    <- OK
             */

            corrupted_backup_found = true;
            current_backup_id = base36enc_dup(current_backup->start_time);

            for (j = i - 1; j >= 0; j--)
            {
                pgBackup   *backup = (pgBackup *) parray_get(backups, j);

                if (is_parent(current_backup->start_time, backup, false))
                {
                    if (backup->status == BACKUP_STATUS_OK ||
                        backup->status == BACKUP_STATUS_DONE)
                    {
                        write_backup_status(backup, BACKUP_STATUS_ORPHAN, instance_name, true);

                        elog(WARNING, "Backup %s is orphaned because his parent %s has status: %s",
                             base36enc(backup->start_time),
                             current_backup_id,
                             status2str(current_backup->status));
                    }
                }
            }
            free(current_backup_id);
        }

        /* For every OK backup we try to revalidate all his ORPHAN descendants. */
        if (current_backup->status == BACKUP_STATUS_OK)
        {
            /* revalidate all ORPHAN descendants
             * be very careful not to miss a missing backup
             * for every backup we must check that he is descendant of current_backup
             */
            for (j = i - 1; j >= 0; j--)
            {
                pgBackup   *backup = (pgBackup *) parray_get(backups, j);
                pgBackup   *tmp_backup = NULL;
                int result;

                //PAGE_b2 ORPHAN
                //PAGE_b1 ORPHAN          -----
                //PAGE_a5 ORPHAN              |
                //PAGE_a4 CORRUPT              |
                //PAGE_a3 missing             |
                //PAGE_a2 missing             |
                //PAGE_a1 ORPHAN              |
                //PAGE    OK <- we are here<-|
                //FULL OK

                if (is_parent(current_backup->start_time, backup, false))
                {
                    /* Revalidation make sense only if parent chain is whole.
                     * is_parent() do not guarantee that.
                     */
                    result = scan_parent_chain(backup, &tmp_backup);

                    if (result == ChainIsInvalid)
                    {
                        /* revalidation make sense only if oldest invalid backup is current_backup
                         */

                        if (tmp_backup->start_time != backup->start_time)
                            continue;

                        if (backup->status == BACKUP_STATUS_ORPHAN)
                        {
                            /* Do not interrupt, validate the next backup */
                            if (!lock_backup(backup, true))
                            {
                                elog(WARNING, "Cannot lock backup %s directory, skip validation",
                                     base36enc(backup->start_time));
                                skipped_due_to_lock = true;
                                continue;
                            }
                            /* Revalidate backup files*/
                            pgBackupValidate(backup, NULL);

                            if (backup->status == BACKUP_STATUS_OK)
                            {

                                /* Revalidation successful, validate corresponding WAL files */
                                validate_wal(backup, arclog_path, 0,
                                             0, 0, current_backup->tli,
                                             instance_config.xlog_seg_size);
                            }
                        }

                        if (backup->status != BACKUP_STATUS_OK)
                        {
                            corrupted_backup_found = true;
                            continue;
                        }
                    }
                }
            }
        }
    }

    /* cleanup */
    parray_walk(backups, pgBackupFree);
    parray_free(backups);
}
