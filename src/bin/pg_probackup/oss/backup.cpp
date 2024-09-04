/*-------------------------------------------------------------------------
 *
 * backup.cpp: Backup api used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "include/backup.h"
#include "storage/file/fio_device.h"
#include "common/fe_memutils.h"

/* Progress Counter */
static int g_doneFiles = 0;
static int g_totalFiles = 0;
static volatile bool g_progressFlag = false;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

static void handleZeroSizeFile(FileAppender *appender, pgFile* file);
static void *ProgressReportProbackup(void *arg);

void performBackup(backup_files_arg* arg)
{
    backupReaderThreadArgs* thread_args = (backupReaderThreadArgs*)palloc(sizeof(backupReaderThreadArgs) * current.readerThreadCount);
    initPerformBackup(arg, thread_args);
    backupDataFiles(arg);
    pfree(thread_args);
}

void initPerformBackup(backup_files_arg* arg, backupReaderThreadArgs* thread_args)
{
    startBackupSender();
    startBackupReaders(arg, thread_args);
}

void backupDataFiles(backup_files_arg* arg)
{    
    FileAppender* appender = NULL;
    /* bucket name: instance_name; object name: /instance_name/backupset_name/file-%d.pbk
     * instance_name complies with the naming rules of object storage service
     */
    appender = (FileAppender*)palloc(sizeof(FileAppender));
    if (appender == NULL) {
        elog(ERROR, "Failed to allocate memory for appender");
        return;
    }
    appender->baseFileName = pg_strdup(current.root_dir);
    initFileAppender(appender, FILE_APPEND_TYPE_FILES, 0, 0);
    /* backup starts */
    backupDirectories(appender, arg);
    backupFiles(appender, arg);
    /* clean up FileAppender and reader threads*/
    flushReaderContexts(arg);
    closeFileAppender(appender);
    destoryFileAppender(&appender);
    stopBackupReaders();
    destoryBackupReaderContexts();
    /* no more new data to be produced, backup ends */
    setSenderState(current.sender_cxt, SENDER_THREAD_STATE_FINISH);
    waitForSenderThread();
    stopBackupSender();
    destoryBackupSenderContext();
}

void backupDirectories(FileAppender* appender, backup_files_arg* arg)
{
    int totalFiles = (int)parray_num(arg->files_list);
    g_totalFiles = totalFiles;
    for (int i = 0; i < totalFiles; i++) {
        pgFile* dir = (pgFile*) parray_get(arg->files_list, i);
        /* if the entry was a directory, create it in the backup file */
        if (S_ISDIR(dir->mode)) {
            char dirpath[MAXPGPATH];
            int nRet = snprintf_s(dirpath, MAXPGPATH, MAXPGPATH - 1, "%s", dir->rel_path);
            securec_check_ss_c(nRet, "\0", "\0");
            // write into backup file
            appendDir(appender, dirpath, DIR_PERMISSION, dir->external_dir_num, dir->type);
            g_doneFiles++;
        }
    }
}

void appendDir(FileAppender* appender, const char* dirPath, uint32 permission,
               int external_dir_num, device_type_t type)
{
    FileAppenderSegHeader header;
    size_t pathLen = strlen(dirPath);
    header.type = FILE_APPEND_TYPE_DIR;
    header.size = pathLen;
    header.permission = permission;
    header.filesize = 0;
    header.crc = 0;
    header.external_dir_num = external_dir_num;
    header.file_type = type;
    writeHeader(&header, appender);
    writePayload((char*)dirPath, pathLen, appender);
}

void backupFiles(FileAppender* appender, backup_files_arg* arg)
{
    char    from_fullpath[MAXPGPATH];
    char    to_fullpath[MAXPGPATH];
    static time_t prev_time;
    time_t  start_time, end_time;
    char    pretty_time[20];
    int     n_backup_files_list = parray_num(arg->files_list);
    prev_time = current.start_time;

    /* Sort by size for load balancing */
    parray_qsort(arg->files_list, pgFileCompareSize);
    /* Sort the array for binary search */
    if (arg->prev_filelist)
        parray_qsort(arg->prev_filelist, pgFileCompareRelPathWithExternal);
    /* write initial backup_content.control file and update backup.control  */
    write_backup_filelist(&current, arg->files_list,
                                        instance_config.pgdata, arg->external_dirs, true);
    write_backup(&current, true);
    /* Init backup page header map */
    init_header_map(&current);
    /* Run threads */
    thread_interrupted = false;
    elog(INFO, "Start backing up files");
    time(&start_time);
    /* Create the thread for progress report */
    pthread_t progressThread;
    pthread_create(&progressThread, nullptr, ProgressReportProbackup, nullptr);

    /* backup a file */
    for (int i = 0; i < n_backup_files_list; i++) {
        pgFile  *prev_file = NULL;
        pgFile  *file = (pgFile *) parray_get(arg->files_list, i);
        /* We have already copied all directories */
        if (S_ISDIR(file->mode)) {
            continue;
        }
        /* check for interrupt */
        if (interrupted || thread_interrupted) {
            elog(ERROR, "interrupted during backup");
        }
        if (progress)
            elog_file(INFO, "Progress: (%d/%d). Process file \"%s\"",
                i + 1, n_backup_files_list, file->rel_path);
        /* update done_files */
        pg_atomic_add_fetch_u32((volatile uint32*) &g_doneFiles, 1);

        /* Handle zero sized files */
        if (file->size == 0) {
            file->write_size = 0;
            handleZeroSizeFile(appender, file);
            continue;
        }

        /* construct filepath */
        if (file->external_dir_num != 0) {
            char    external_dst[MAXPGPATH];
            char    *external_path = (char *)parray_get(arg->external_dirs,
                                            file->external_dir_num - 1);

            makeExternalDirPathByNum(external_dst,
                                        arg->external_prefix,
                                        file->external_dir_num);

            join_path_components(to_fullpath, external_dst, file->rel_path);
            join_path_components(from_fullpath, external_path, file->rel_path);
        } else if (is_dss_type(file->type)) {
            join_path_components(from_fullpath, arg->src_dss, file->rel_path);
            join_path_components(to_fullpath, arg->dst_dss, file->rel_path);
        } else {
            join_path_components(from_fullpath, arg->from_root, file->rel_path);
            join_path_components(to_fullpath, arg->to_root, file->rel_path);
        }

        /* Encountered some strange beast */
        if (!S_ISREG(file->mode)) {
            elog(WARNING, "Unexpected type %d of file \"%s\", skipping",
                    file->mode, from_fullpath);
        }

        /* Check that file exist in previous backup */
        if (current.backup_mode != BACKUP_MODE_FULL) {
            pgFile	**prev_file_tmp = NULL;
            prev_file_tmp = (pgFile **) parray_bsearch(arg->prev_filelist,
                             file, pgFileCompareRelPathWithExternal);
            if (prev_file_tmp) {
                /* File exists in previous backup */
                file->exists_in_prev = true;
                prev_file = *prev_file_tmp;
            }
        }

        /* special treatment for global/pg_control */
        if (file->external_dir_num == 0 && strcmp(file->name, PG_XLOG_CONTROL_FILE) == 0) {
            char* filename = last_dir_separator(to_fullpath);
            char* dirpath = strndup(to_fullpath, filename - to_fullpath + 1);
            fio_mkdir(dirpath, DIR_PERMISSION, FIO_BACKUP_HOST);
            pg_free(dirpath);
        }

        /* If the file size is less than 8MB,
         * a load-balancing reason prevents the direct writing of the appender file 
         */
        if (file->size <= FILE_BUFFER_SIZE && current.readerThreadCount > 0) {
            int thread_slot = getFreeReaderThread();
            while (thread_slot == -1) {
                flushReaderContexts(arg);
                thread_slot = getFreeReaderThread();
            }
            ReaderCxt* reader_cxt = &current.readerCxt[thread_slot];
            int current_fileidx = reader_cxt->fileCount;
            reader_cxt->file[current_fileidx] = file;
            reader_cxt->prefile[current_fileidx] = prev_file;
            reader_cxt->fromPath[current_fileidx] = pgut_strdup(from_fullpath);
            reader_cxt->toPath[current_fileidx] = pgut_strdup(to_fullpath);
            reader_cxt->appender = appender;
            reader_cxt->segType[current_fileidx] = FILE_APPEND_TYPE_FILE;
            reader_cxt->fileRemoved[current_fileidx] = false;
            reader_cxt->fileCount++;
            if (reader_cxt->fileCount == READER_THREAD_FILE_COUNT) {
                setReaderState(reader_cxt, READER_THREAD_STATE_START);
            }
        } else {
            if (file->is_datafile && !file->is_cfs) {
                backup_data_file(&(arg->conn_arg), file, from_fullpath, to_fullpath,
                                 arg->prev_start_lsn,
                                 current.backup_mode,
                                 instance_config.compress_alg,
                                 instance_config.compress_level,
                                 arg->nodeInfo->checksum_version,
                                 arg->hdr_map, false, appender, NULL);
            } else {
                backup_non_data_file(file, prev_file, from_fullpath, to_fullpath,
                                     current.backup_mode, current.parent_backup, true, appender, NULL);
            }
        }

        if (file->write_size == FILE_NOT_FOUND) {
            continue;
        }

        if (file->write_size == BYTES_INVALID) {
            elog(VERBOSE, "Skipping the unchanged file: \"%s\"", from_fullpath);
            continue;
        }
    }
    g_progressFlag = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, nullptr);

    /* ssh connection to longer needed */
    fio_disconnect();
    /* Close connection */
    if (arg->conn_arg.conn) {
        pgut_disconnect(arg->conn_arg.conn);
    }
    /* Data files transferring is successful */
    arg->ret = 0;
    elog(INFO, "Finish backuping file");
    time(&end_time);
    pretty_time_interval(difftime(end_time, start_time),
                                        pretty_time, lengthof(pretty_time));
    elog(INFO, "Backup files are backuped to s3, time elapsed: %s", pretty_time);
}


/* static function*/

static void handleZeroSizeFile(FileAppender *appender, pgFile* file)
{
    size_t pathLen = strlen(file->rel_path);
    FileAppenderSegHeader start_header;
    constructHeader(&start_header, FILE_APPEND_TYPE_FILE, pathLen, 0, file);
    writeHeader(&start_header, appender);
    writePayload((char*)file->rel_path, pathLen, appender);
    FileAppenderSegHeader end_header;
    constructHeader(&end_header, FILE_APPEND_TYPE_FILE_END, 0, 0, file);
    writeHeader(&end_header, appender);
}

/* copy from backup.c for static variables*/
static void *ProgressReportProbackup(void *arg)
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
        fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). backup file \r",
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
    } while ((g_doneFiles < g_totalFiles) && !g_progressFlag);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). backup file \n",
            progressBar, percent, g_doneFiles, g_totalFiles);
    return nullptr;
}