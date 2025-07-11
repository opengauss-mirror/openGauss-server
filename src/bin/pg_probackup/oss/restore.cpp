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
    /* used if payload spans across two buffs */
    BufferDesc* nextBuff = NULL;
    /* used if header spans across two buffs */
    BufferDesc* prevBuff = NULL;
    FileAppenderSegDescriptor* desc = NULL;
    initSegDescriptor(&desc);
    char* buffOffset = NULL;
    size_t remainBuffLen = 0;
    int filenum = 0;

    /*
     * Expectly there is only one type of header in on backup instance,
     * or you have to adapt yourself.
     */
    uint32 headerVersion = 0xFFFFFFFF;
    size_t segHdrLen = 0;

    while(true) {
        buff = tryGetNextFreeReadBuffer(bufferCxt);
        markBufferFlag(buff, BUFF_FLAG_FILE_USED);
        /* expect all the header of backup has the same version. */
        if (unlikely(headerVersion == 0xFFFFFFFF)) {
            headerVersion = getSegHeaderVersion(buffLoc(buff, bufferCxt));
            desc->version = headerVersion;
            segHdrLen = getSegHeaderSize(headerVersion);
            if (headerVersion >= SEG_HEADER_VERSION_MAX) {
                elog(ERROR, "Invalid oss file versionL %u, max version: %u.",
                     headerVersion, SEG_HEADER_VERSION_MAX);
            } else {
                elog(INFO, "oss file version: %u", headerVersion);
            }
        }

        uint32 usedLen = buffUsedLen(buff);
        char* buffEnd = buffLoc(buff, bufferCxt) + usedLen;
        if (remainBuffLen == 0) {
            buffOffset = buffLoc(buff, bufferCxt);
        }
        if (unlikely(prevBuff != NULL)) {
            remainBuffLen = remainBuffLen + usedLen;
        } else {
            remainBuffLen = buffEnd - buffOffset;
        }
        while (segHdrLen <= remainBuffLen) {
            getSegDescriptor(desc, &buffOffset, &remainBuffLen, bufferCxt, headerVersion, segHdrLen);
            if (prevBuff != NULL) {
                /* set pre buffer unused */
                clearBuff(prevBuff);
                prevBuff = NULL;
            }
            // The payload spans across two buffs.
            if (((FileAppenderSegHeader*)(desc->header))->size > 0 &&
                ((FileAppenderSegHeader*)(desc->header))->size > remainBuffLen) {
                nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                while (nextBuff->bufId == buff->bufId) {
                    nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                }
                markBufferFlag(nextBuff, BUFF_FLAG_FILE_USED);
                // rewind
                if (nextBuff->bufId == 0 && remainBuffLen >= 0) {
                    desc->payload_offset = remainBuffLen;
                }
                remainBuffLen = remainBuffLen + buffUsedLen(nextBuff);
            }
            parseSegDescriptor(desc, &buffOffset, &remainBuffLen, tempBuffer, bufferCxt, dest_backup, isValidate, &arg);
            if (isValidate && arg.corrupted) {
                remainBuffLen = 0;
                corrupted = true;
                break;
            }
            if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILES_END) {
                filenum++;
                if (filenum == bufferCxt->fileNum) {
                    break;
                }
                nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                while (nextBuff->bufId == buff->bufId) {
                    nextBuff = tryGetNextFreeReadBuffer(bufferCxt);
                }
                markBufferFlag(nextBuff, BUFF_FLAG_FILE_USED);
                remainBuffLen = buffUsedLen(nextBuff);
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

    args.bufferCxt->earlyExit.store(true);
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
    uint32 version = desc->version;
    uint32 fileIndex = 0;
    if (version >= SEG_HEADER_PARALLEL_VERSION) {
        fileIndex = ((ParallelFileAppenderSegHeader*)(desc->header))->threadId;
    }

    /* create directories */
    char to_path[MAXPGPATH];
    char from_root[MAXPGPATH];
    char dir_path[MAXPGPATH];
    errno_t rc;
    FileAppenderSegHeader* commonSegHeader = (FileAppenderSegHeader*)(desc->header);
    size_t pathlen = commonSegHeader->size;
    rc = strncpy_s(to_path, pathlen + 1, path, pathlen);
    pgFile* dir = findpgFile(files, to_path, commonSegHeader->external_dir_num, commonSegHeader->file_type);
    if (dir == NULL) {
        elog(ERROR, "Cannot find dir \"%s\"", to_path);
    }

    if (dir->external_dir_num != 0) {
        char external_prefix[MAXPGPATH];
        join_path_components(external_prefix, dest_backup->root_dir, EXTERNAL_DIR);
        makeExternalDirPathByNum(from_root, external_prefix, desc->inputFile[fileIndex]->external_dir_num);
    }
    else if (is_dss_type(dir->type)) {
        join_path_components(from_root, dest_backup->root_dir, DSSDATA_DIR);
    } else {
        join_path_components(from_root, dest_backup->root_dir, DATABASE_DIR);
    }
    join_path_components(dir_path, from_root, to_path);
    fio_mkdir(dir_path, commonSegHeader->permission, location);
}

void openRestoreFile(const char* path, FileAppenderSegDescriptor* desc, pgBackup* dest_backup,
                     parray* files, bool isValidate, validate_files_arg* arg)
{
    char to_path[MAXPGPATH];
    char from_root[MAXPGPATH];
    char filepath[MAXPGPATH];
    uint32 version = desc->version;
    uint32 fileIndex = 0;
    if (version >= SEG_HEADER_PARALLEL_VERSION) {
        fileIndex = ((ParallelFileAppenderSegHeader*)(desc->header))->threadId;
    }
    FileAppenderSegHeader* commonSegHeader = (FileAppenderSegHeader*)(desc->header);

    errno_t rc;
    rc = strncpy_s(to_path, commonSegHeader->size + 1, path, commonSegHeader->size);
    securec_check_c(rc, "\0", "\0");
    desc->inputFile[fileIndex] = findpgFile(files, to_path, commonSegHeader->external_dir_num,
                                            commonSegHeader->file_type);

    if (desc->inputFile[fileIndex] == NULL) {
        elog(ERROR, "Cannot find file \"%s\"", to_path);
    }
    if (isValidate) {
        if (desc->inputFile[fileIndex]->write_size == BYTES_INVALID) {
            if (arg->backup_mode == BACKUP_MODE_FULL) {
                /* It is illegal for file in FULL backup to have BYTES_INVALID */
                elog(WARNING, "Backup file \"%s\" has invalid size. Possible metadata corruption.",
                     desc->inputFile[fileIndex]->rel_path);
                arg->corrupted = true;
            }
            return;
        }
        INIT_FILE_CRC32(true, desc->crc[fileIndex]);
    } else {
        if (desc->inputFile[fileIndex]->external_dir_num != 0) {
            char external_prefix[MAXPGPATH];
            join_path_components(external_prefix, dest_backup->root_dir, EXTERNAL_DIR);
            makeExternalDirPathByNum(from_root, external_prefix, desc->inputFile[fileIndex]->external_dir_num);
        }
        else if (is_dss_type(desc->inputFile[fileIndex]->type)) {
            join_path_components(from_root, dest_backup->root_dir, DSSDATA_DIR);
        } else {
            join_path_components(from_root, dest_backup->root_dir, DATABASE_DIR);
        }
        join_path_components(filepath, from_root, desc->inputFile[fileIndex]->rel_path);

        if (desc->outputFile[fileIndex] == NULL) {
            desc->outputFile[fileIndex] = fio_fopen(filepath, PG_BINARY_W, location);
        } else if (desc->outputFile[fileIndex] != NULL) {
            desc->outputFile[fileIndex] = fio_fopen(filepath, PG_BINARY_R "+", location);
        }
        if (desc->outputFile[fileIndex] == NULL) {
            elog(ERROR, "Cannot open restore file \"%s\": %s",
                    filepath, strerror(errno));
        }
        setvbuf(desc->outputFile[fileIndex], NULL, _IONBF, BUFSIZ);
    }
}

void closeRestoreFile(FileAppenderSegDescriptor* desc)
{
    uint32 version = desc->version;
    uint32 fileIndex = 0;
    if (version >= SEG_HEADER_PARALLEL_VERSION) {
        fileIndex = ((ParallelFileAppenderSegHeader*)(desc->header))->threadId;
    }

    if (desc->outputFile[fileIndex] && fio_fclose(desc->outputFile[fileIndex]) != 0) {
        elog(ERROR, "Cannot close file!", strerror(errno));
    }
    desc->outputFile[fileIndex] = NULL;
    desc->inputFile[fileIndex] = NULL;
}

void writeOrValidateRestoreFile(const char* data, FileAppenderSegDescriptor* desc,
                                bool isValidate, validate_files_arg* arg)
{
    uint32 version = desc->version;
    uint32 fileIndex = 0;
    uint32 payloadSize = ((FileAppenderSegHeader*)(desc->header))->size;
    pg_crc32 payloadCrc = ((FileAppenderSegHeader*)(desc->header))->crc;

    if (version >= SEG_HEADER_PARALLEL_VERSION) {
        fileIndex = ((ParallelFileAppenderSegHeader*)(desc->header))->threadId;
    }

    pgFile* destFile = desc->inputFile[fileIndex];
    if (destFile == NULL) {
        return;
    }
    /* Restore or Validate destination file */
    if (isValidate) {
        if (!S_ISREG(destFile->mode) || destFile->write_size == 0 ||
            strcmp(destFile->name, PG_XLOG_CONTROL_FILE) == 0) {
            return;
        }
        if (destFile->write_size == BYTES_INVALID) {
            if (arg->backup_mode == BACKUP_MODE_FULL) {
                elog(WARNING, "Backup file \"%s\" has invalid size. Possible metadata corruption.",
                     destFile->rel_path);
                arg->corrupted = true;
                return;
            }
            return;
        }
        COMP_FILE_CRC32(true, desc->crc[fileIndex], data, payloadSize);
        if (desc->crc[fileIndex] != payloadCrc) {
            arg->corrupted = true;
            return;
        }
    } else if (payloadSize > 0) {
        if (fio_fwrite(desc->outputFile[fileIndex], data, payloadSize) != payloadSize) {
            elog(ERROR, "Cannot write blocks of \"%s\": %s", desc->inputFile[fileIndex]->rel_path, strerror(errno));
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