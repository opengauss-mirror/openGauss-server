#include "include/restore.h"
#include "include/oss_operator.h"
#include "include/thread.h"

#include "storage/file/fio_device.h"
#include "common/fe_memutils.h"

#define IsIllegalCharacter(c) ((c) != '/' && !isdigit((c)) && !isalpha((c)) && (c) != '_' && (c) != '-' && (c) != '.')
static fio_location location = FIO_BACKUP_HOST;
static pgFile* findpgFile(parray* files, char* to_path, int external_dir_num, device_type_t type);

void performRestoreOrValidate(pgBackup *dest_backup, bool isValidate)
{
    /* for validate */
    char        base_path[MAXPGPATH];
    char        dss_path[MAXPGPATH];
    char        external_prefix[MAXPGPATH];
    parray       *files = NULL;
    bool        corrupted = false;
    validate_files_arg arg;
    if (dest_backup->files == NULL) {
        files = get_backup_filelist(dest_backup, true);
        dest_backup->files = files;
        parray_qsort(dest_backup->files, pgFileCompareRelPathWithExternal);
    } else {
        files = dest_backup->files;
        parray_qsort(dest_backup->files, pgFileCompareRelPathWithExternal);
    }
    if (isValidate) {
        if (!pre_check_backup(dest_backup)) {
            return;
        }
        join_path_components(base_path, dest_backup->root_dir, DATABASE_DIR);
        join_path_components(dss_path, dest_backup->root_dir, DSSDATA_DIR);
        join_path_components(external_prefix, dest_backup->root_dir, EXTERNAL_DIR);
        if (!files) {
            elog(WARNING, "Backup %s file list is corrupted", base36enc(dest_backup->start_time));
            dest_backup->status = BACKUP_STATUS_CORRUPT;
            write_backup_status(dest_backup, BACKUP_STATUS_CORRUPT, instance_name, true);
            return;
        }
        arg.base_path = base_path;
        arg.dss_path = dss_path;
        arg.files = files;
        arg.corrupted = false;
        arg.backup_mode = dest_backup->backup_mode;
        arg.stop_lsn = dest_backup->stop_lsn;
        arg.checksum_version = dest_backup->checksum_version;
        arg.backup_version = parse_program_version(dest_backup->program_version);
        arg.external_prefix = external_prefix;
        arg.hdr_map = &(dest_backup->hdr_map);
        arg.ret = 1;
    }
    /* Initialize the buffer context */
    BufferCxt* bufferCxt = (BufferCxt*)palloc(sizeof(BufferCxt));
    if (bufferCxt == NULL) {
        elog(ERROR, "buffer context allocate failed: out of memory");
    }
    initBufferCxt(bufferCxt, SENDER_BUFFER_SIZE);
    restoreReaderThreadArgs args;
    args.bufferCxt = bufferCxt;
    args.dest_backup = dest_backup;
    pthread_t restoreReaderThread;
    pthread_create(&restoreReaderThread, nullptr, restoreReaderThreadMain, &args);

    char tempBuffer[BUFSIZE];
    BufferDesc* buff = NULL;
    BufferDesc* nextBuff = NULL;
    BufferDesc* prevBuff = NULL;
    FileAppenderSegDescriptor* desc = NULL;
    FileAppenderSegDescriptor* predesc = NULL;
    initSegDescriptor(&desc);
    initSegDescriptor(&predesc);
    const size_t segHdrLen = sizeof(FileAppenderSegHeader);
    char* buffOffset = NULL;
    size_t remainBuffLen = 0;
    int filenum = 0;
    while(true) {
        buff = tryGetNextFreeReadBuffer(bufferCxt);
        char* buffEnd = buffLoc(buff, bufferCxt) + buff->usedLen;
        if (remainBuffLen == 0) {
            buffOffset = buffLoc(buff, bufferCxt);
        }
        if (unlikely(prevBuff != NULL)) {
            remainBuffLen = remainBuffLen + buff->usedLen;
        } else {
            remainBuffLen = buffEnd - buffOffset;
        }
        while (segHdrLen <= remainBuffLen) {
            memcpy_s(predesc, sizeof(FileAppenderSegDescriptor), desc, sizeof(FileAppenderSegDescriptor));
            getSegDescriptor(desc, &buffOffset, &remainBuffLen, bufferCxt);
            if (prevBuff != NULL) {
                clearBuff(prevBuff);
                prevBuff = NULL;
            }
            // The payload spans across two buffs.
            if (desc->header.size > 0 && desc->header.size > remainBuffLen) {
                nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                while (nextBuff->bufId == buff->bufId) {
                    pg_usleep(WAIT_FOR_BUFF_SLEEP_TIME);
                    nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                }
                // rewind
                if (nextBuff->bufId == 0 && remainBuffLen > 0) {
                    desc->payload_offset = remainBuffLen;
                }
                remainBuffLen = remainBuffLen + nextBuff->usedLen;
            }
            parseSegDescriptor(desc, &buffOffset, &remainBuffLen, tempBuffer, bufferCxt, dest_backup, isValidate, &arg);
            if (isValidate && arg.corrupted) {
                remainBuffLen = 0;
                corrupted = true;
                break;
            }
            if (desc->header.type == FILE_APPEND_TYPE_FILES_END) {
                filenum++;
                if (filenum == bufferCxt->fileNum) {
                    break;
                }
                nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                while (nextBuff->bufId == buff->bufId) {
                    pg_usleep(WAIT_FOR_BUFF_SLEEP_TIME);
                    nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                }
                remainBuffLen = nextBuff->usedLen;
                buffOffset = buffLoc(nextBuff, bufferCxt);
            }
            if (nextBuff != NULL) {
                clearBuff(buff);
                buff = nextBuff;
                nextBuff = NULL;
            }
        }
        // The header spans across two buffs.
        if (remainBuffLen > 0) {
            prevBuff = buff;
            // rewind
            if (prevBuff->bufId == (buffNum(bufferCxt) - 1)) {
                desc->header_offset = remainBuffLen;
            }
            continue;
        }
        // reuse the buffer
        clearBuff(buff);
        if (filenum == bufferCxt->fileNum || corrupted) {
            break;
        }
    }

    args.bufferCxt->earlyExit = true;
    pthread_join(restoreReaderThread, nullptr);
    destorySegDescriptor(&desc);
    destroyBufferCxt(bufferCxt);
    if (isValidate) {
        elog(INFO, "Finish validate file.");
        if (corrupted) {
            dest_backup->status = BACKUP_STATUS_CORRUPT;
        }
        write_backup_status(dest_backup, corrupted ? BACKUP_STATUS_CORRUPT :
                            BACKUP_STATUS_OK, instance_name, true);
        if (corrupted) {
            elog(WARNING, "Backup %s data files are corrupted", base36enc(dest_backup->start_time));
        } else {
            elog(INFO, "Backup %s data files are valid", base36enc(dest_backup->start_time));
        }
    }
}

void restoreDir(const char* path, FileAppenderSegDescriptor* desc, pgBackup* dest_backup,
                parray* files, bool isValidate)
{
    if (isValidate) {
        return;
    }
    /* create directories */
    char to_path[MAXPGPATH];
    char from_root[MAXPGPATH];
    char dir_path[MAXPGPATH];
    errno_t rc;
    size_t pathlen = desc->header.size;
    rc = strncpy_s(to_path, pathlen + 1, path, pathlen);
    pgFile* dir = findpgFile(files, to_path, desc->header.external_dir_num, desc->header.file_type);
    if (dir == NULL) {
        elog(ERROR, "Cannot find dir \"%s\"", to_path);
    }

    if (dir->external_dir_num != 0) {
        char external_prefix[MAXPGPATH];
        join_path_components(external_prefix, dest_backup->root_dir, EXTERNAL_DIR);
        makeExternalDirPathByNum(from_root, external_prefix, desc->inputFile->external_dir_num);
    }
    else if (is_dss_type(dir->type)) {
        join_path_components(from_root, dest_backup->root_dir, DSSDATA_DIR);
    } else {
        join_path_components(from_root, dest_backup->root_dir, DATABASE_DIR);
    }
    join_path_components(dir_path, from_root, to_path);
    fio_mkdir(dir_path, desc->header.permission, location);
}

void openRestoreFile(const char* path, FileAppenderSegDescriptor* desc, pgBackup* dest_backup,
                     parray* files, bool isValidate, validate_files_arg* arg)
{
    char to_path[MAXPGPATH];
    char from_root[MAXPGPATH];
    char filepath[MAXPGPATH];
    errno_t rc;
    rc = strncpy_s(to_path, desc->header.size + 1, path, desc->header.size);
    securec_check_c(rc, "\0", "\0");
    desc->inputFile = findpgFile(files, to_path, desc->header.external_dir_num, desc->header.file_type);
    if (desc->inputFile == NULL) {
        elog(ERROR, "Cannot find file \"%s\"", to_path);
    }
    if (isValidate) {
        if (desc->inputFile->write_size == BYTES_INVALID) {
            if (arg->backup_mode == BACKUP_MODE_FULL) {
                /* It is illegal for file in FULL backup to have BYTES_INVALID */
                elog(WARNING, "Backup file \"%s\" has invalid size. Possible metadata corruption.",
                     desc->inputFile->rel_path);
                arg->corrupted = true;
            }
            return;
        }
        INIT_FILE_CRC32(true, desc->crc);
    } else {
        if (desc->inputFile->external_dir_num != 0) {
            char external_prefix[MAXPGPATH];
            join_path_components(external_prefix, dest_backup->root_dir, EXTERNAL_DIR);
            makeExternalDirPathByNum(from_root, external_prefix, desc->inputFile->external_dir_num);
        }
        else if (is_dss_type(desc->inputFile->type)) {
            join_path_components(from_root, dest_backup->root_dir, DSSDATA_DIR);
        } else {
            join_path_components(from_root, dest_backup->root_dir, DATABASE_DIR);
        }
        join_path_components(filepath, from_root, desc->inputFile->rel_path);

        if (desc->outputFile == NULL) {
            desc->outputFile = fio_fopen(filepath, PG_BINARY_W, location);
        } else if (desc->outputFile != NULL) {
            desc->outputFile = fio_fopen(filepath, PG_BINARY_R "+", location);
        }
        if (desc->outputFile == NULL) {
            elog(ERROR, "Cannot open restore file \"%s\": %s",
                    filepath, strerror(errno));
        }
        setvbuf(desc->outputFile, NULL, _IONBF, BUFSIZ);
    }
}

void closeRestoreFile(FileAppenderSegDescriptor* desc)
{
    if (desc->outputFile && fio_fclose(desc->outputFile) != 0) {
        elog(ERROR, "Cannot close file!", strerror(errno));
    }
    desc->outputFile = NULL;
    desc->inputFile = NULL;
}

void writeOrValidateRestoreFile(const char* data, FileAppenderSegDescriptor* desc,
                                bool isValidate, validate_files_arg* arg)
{
    pgFile* dest_file = desc->inputFile;
    if (dest_file == NULL) {
        return;
    }
    /* Restore or Validate destination file */
    if (isValidate) {
        if (!S_ISREG(dest_file->mode) || dest_file->write_size == 0 ||
            strcmp(dest_file->name, PG_XLOG_CONTROL_FILE) == 0) {
            return;
        }
        if (dest_file->write_size == BYTES_INVALID) {
            if (arg->backup_mode == BACKUP_MODE_FULL) {
                elog(WARNING, "Backup file \"%s\" has invalid size. Possible metadata corruption.",
                     dest_file->rel_path);
                arg->corrupted = true;
                return;
            }
            return;
        }
        COMP_FILE_CRC32(true, desc->crc, data, desc->header.size);
        if (desc->crc != desc->header.crc) {
            arg->corrupted = true;
            return;
        }
    } else if (desc->header.size > 0) {
        if (fio_fwrite(desc->outputFile, data, desc->header.size) != desc->header.size) {
            elog(ERROR, "Cannot write blocks of \"%s\": %s", desc->inputFile->rel_path, strerror(errno));
        }
    }
}

void restoreConfigDir()
{
    Oss::Oss* oss = getOssClient();
    char* bucket_name = getBucketName();
    char* prefix_name = backup_instance_path + 1;
    char dir_path[MAXPGPATH];
    char arclog_path[MAXPGPATH];
    int nRet = 0;
    fio_mkdir(backup_instance_path, DIR_PERMISSION, location);
    nRet = snprintf_s(arclog_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", backup_path, "wal", instance_name);
    securec_check_ss_c(nRet, "\0", "\0");
    fio_mkdir(arclog_path, DIR_PERMISSION, location);
    parray *obj_list = parray_new();
    oss->ListObjectsWithPrefix(bucket_name, prefix_name, obj_list);
    for (size_t i = 0; i < parray_num(obj_list); i++) {
        char* object = (char*)parray_get(obj_list, i);
        char* filename = last_dir_separator(object);
        char* dir_name = strndup(object, filename - object);
        if (strcmp(filename + 1, BACKUP_CATALOG_CONF_FILE) == 0) {
            pg_free(dir_name);
            continue;
        }
        if (strcmp(filename + 1, BACKUP_CONTROL_FILE) == 0) {
            join_path_components(dir_path, "/", dir_name);
            fio_mkdir(dir_path, DIR_PERMISSION, location);
            pg_free(dir_name);
        }
    }
    parray_free(obj_list);
}

void restoreConfigFile(const char* path, bool errorOk)
{
    Oss::Oss* oss = getOssClient();
    const char* object_name = NULL;
    const char* bucket_name = NULL;
    bucket_name = getBucketName();
    object_name = path;
    oss->GetObject(bucket_name, object_name, (char*)path, errorOk);
}

void uploadConfigFile(const char* path, const char* object_name)
{
    Oss::Oss* oss = getOssClient();
    const char* bucket_name = getBucketName();
    oss->RemoveObject(bucket_name, object_name);
    oss->PutObject(bucket_name, path, object_name);
    fio_unlink(path, location);
}

static pgFile* findpgFile(parray* files, char* to_path, int external_dir_num, device_type_t type)
{
    pgFile* resfile = NULL;
    pgFile tempfile;
    tempfile.rel_path = to_path;
    tempfile.external_dir_num = external_dir_num;
    tempfile.type = type;
    void* res = parray_bsearch(files, &tempfile, pgFileCompareRelPathWithExternal);
    if (res != NULL) {
        resfile = *(pgFile **)res;
    }
    return resfile;
}