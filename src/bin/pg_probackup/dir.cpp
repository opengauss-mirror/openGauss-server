/*-------------------------------------------------------------------------
 *
 * dir.c: directory operation utility.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "file.h"


#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "configuration.h"
#include "common/fe_memutils.h"
#include "PageCompression.h"
#include "storage/file/fio_device.h"
#include "oss/include/restore.h"

/*
 * The contents of these directories are removed or recreated during server
 * start so they are not included in backups.  The directories themselves are
 * kept and included as empty to preserve access permissions.
 */
const char *pgdata_exclude_dir[] =
{
    (const char *)PG_XLOG_DIR,
    /*
    * Skip temporary statistics files. PG_STAT_TMP_DIR must be skipped even
    * when stats_temp_directory is set because PGSS_TEXT_FILE is always created
    * there.
    */
    (const char *)"pg_stat_tmp",
    (const char *)"pgsql_tmp",

    /* Contents removed on startup, see dsm_cleanup_for_mmap(). */
    (const char *)"pg_dynshmem",

    /* Contents removed on startup, see AsyncShmemInit(). */
    (const char *)"pg_notify",

    /*
    * Old contents are loaded for possible debugging but are not required for
    * normal operation, see OldSerXidInit().
    */
    (const char *)"pg_serial",

    /* Contents removed on startup, see DeleteAllExportedSnapshotFiles(). */
    (const char *)"pg_snapshots",

    /* Contents zeroed on startup, see StartupSUBTRANS(). */
    (const char *)"pg_subtrans",

    /* end of list */
    NULL,   /* pg_log and pg_replslot will be set later */
    NULL
};

static const char *pgdata_exclude_files[] =
{
    /* Skip auto conf temporary file. */
    (const char *)"postgresql.auto.conf.tmp",

    /* Skip current log file temporary file */
    (const char *)"current_logfiles.tmp",
    (const char *)"recovery.conf",
    (const char *)"postmaster.pid",
    (const char *)"postmaster.opts",
    (const char *)"probackup_recovery.conf",
    (const char *)"recovery.signal",
    (const char *)"standby.signal",
    NULL
};

static const char *pgdata_exclude_files_non_exclusive[] =
{
    /*skip in non-exclusive backup */
    (const char *)"backup_label",
    (const char *)"tablespace_map",
    NULL
};

/* Tablespace mapping structures */

typedef struct TablespaceListCell
{
    struct TablespaceListCell *next;
    char    old_dir[MAXPGPATH];
    char    new_dir[MAXPGPATH];
} TablespaceListCell;

typedef struct TablespaceList
{
    TablespaceListCell *head;
    TablespaceListCell *tail;
} TablespaceList;

typedef struct TablespaceCreatedListCell
{
    struct TablespaceCreatedListCell *next;
    char    link_name[MAXPGPATH];
    char    linked_dir[MAXPGPATH];
} TablespaceCreatedListCell;

/*is not used now 
typedef struct TablespaceCreatedList
{
    TablespaceCreatedListCell *head;
    TablespaceCreatedListCell *tail;
} TablespaceCreatedList;
may be removed int the future */

static int pgCompareString(const void *str1, const void *str2);

static char dir_check_file(pgFile *file, bool backup_logs, bool backup_replslots);
static char check_in_tablespace(pgFile *file, bool in_tablespace);
static char check_db_dir(pgFile *file);
static char check_digit_file(pgFile *file);
static char check_nobackup_dir(pgFile *file);
static char check_in_dss(pgFile *file, int include_id);
static void dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
                                                    bool exclude, bool follow_symlink, bool backup_logs,
                                                    bool skip_hidden, int external_dir_num, fio_location location,
                                                    bool backup_replslots);
static void opt_path_map(ConfigOption *opt, const char *arg,
                                             TablespaceList *list, const char *type);

char check_logical_replslot_dir(const char *rel_path);

/* Tablespace mapping */
static TablespaceList tablespace_dirs = {NULL, NULL};
/* Extra directories mapping */
static TablespaceList external_remap_list = {NULL, NULL};

/*
 * Create directory, also create parent directories if necessary.
 */
int
dir_create_dir(const char *dir, mode_t mode)
{
    char    parent[MAXPGPATH];
    errno_t rc = 0;

    rc = strncpy_s(parent, MAXPGPATH,dir, MAXPGPATH - 1);
    securec_check_c(rc, "", "");
    get_parent_directory(parent);

    /* Create parent first */
    if (access(parent, F_OK) == -1)
        dir_create_dir(parent, mode);

    /* Create directory */
    if (mkdir(dir, mode) == -1)
    {
        if (is_file_exist(errno))    /* already exist */
            return 0;
        elog(ERROR, "cannot create directory \"%s\": %s", dir, strerror(errno));
    }

    return 0;
}

pgFile *
pgFileNew(const char *path, const char *rel_path, bool follow_symlink,
                    int external_dir_num, fio_location location)
{
    struct stat st;
    pgFile  *file;

    /* stat the file */
    if (fio_stat(path, &st, follow_symlink, location) < 0)
    {
        /* file not found is not an error case */
        if (is_file_delete(errno))
            return NULL;
        elog(ERROR, "cannot stat file \"%s\": %s", path,
            strerror(errno));
    }

    file = pgFileInit(rel_path);
    file->type = fio_device_type(path);
    file->size = st.st_size;
    file->mode = st.st_mode;
    file->mtime = st.st_mtime;
    file->external_dir_num = external_dir_num;

    return file;
}

pgFile *
pgFileInit(const char *rel_path)
{
    pgFile  *file;
    char    *file_name = NULL;
    errno_t rc = 0;

    file = (pgFile *) pgut_malloc(sizeof(pgFile));
    rc = memset_s(file,sizeof(pgFile), 0, sizeof(pgFile));
    securec_check(rc, "\0", "\0");

    file->rel_path = pgut_strdup(rel_path);
    canonicalize_path(file->rel_path);

    /* Get file name from the path */
    file_name = last_dir_separator(file->rel_path);

    if (file_name == NULL)
        file->name = file->rel_path;
    else
    {
        file_name++;
        file->name = file_name;
    }

    /* Number of blocks readed during backup */
    file->n_blocks = BLOCKNUM_INVALID;

    /* Number of blocks backed up during backup */
    file->n_headers = 0;

    /* set uncompressed file default */
    file->compressed_file = false;
    file->compressed_algorithm = 0;
    file->compressed_chunk_size = 0;

    return file;
}

/*
 * Delete file pointed by the pgFile.
 * If the pgFile points directory, the directory must be empty.
 */
void
pgFileDelete(mode_t mode, const char *full_path)
{
    if (S_ISDIR(mode))
    {
        if (rmdir(full_path) == -1)
        {
            if (is_file_delete(errno))
                return;
            else if (errno == ENOTDIR)    /* could be symbolic link */
                goto delete_file;

            elog(ERROR, "Cannot remove directory \"%s\": %s",
             full_path, strerror(errno));
        }
        return;
    }

    delete_file:
    if (remove(full_path) == -1)
    {
        if (is_file_delete(errno))
            return;
        elog(ERROR, "Cannot remove file \"%s\": %s", full_path,
            strerror(errno));
    }
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC(const char *file_path, bool use_crc32c, bool missing_ok)
{
    FILE    *fp = NULL;
    pg_crc32    crc = 0;
    char    *buf;
    size_t  len = 0;

    INIT_FILE_CRC32(use_crc32c, crc);

    /* open file in binary read mode */
    fp = fopen(file_path, PG_BINARY_R);
    if (fp == NULL)
    {
        if (is_file_delete(errno))
        {
            if (missing_ok)
            {
                FIN_FILE_CRC32(use_crc32c, crc);
                return crc;
            }
        }

        elog(ERROR, "Cannot open file \"%s\": %s", file_path, strerror(errno));
    }

    /* disable stdio buffering */
    setvbuf(fp, NULL, _IONBF, BUFSIZ);
    buf = (char *)pgut_malloc(STDIO_BUFSIZE);

    /* calc CRC of file */
    for (;;)
    {
        if (interrupted)
            elog(ERROR, "interrupted during CRC calculation");

        len = fread(buf, 1, STDIO_BUFSIZE, fp);

        if (ferror(fp))
            elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

        /* update CRC */
        COMP_FILE_CRC32(use_crc32c, crc, buf, len);

        if (feof(fp))
            break;
    }

    FIN_FILE_CRC32(use_crc32c, crc);
    fclose(fp);
    pg_free(buf);

    return crc;
}

#ifdef HAVE_LIBZ
/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRCgz(const char *file_path, bool use_crc32c, bool missing_ok)
{
    gzFile    fp;
    pg_crc32  crc = 0;
    int       len = 0;
    int       err;
    char     *buf;

    INIT_FILE_CRC32(use_crc32c, crc);

    /* open file in binary read mode */
    fp = gzopen(file_path, PG_BINARY_R);
    if (fp == NULL)
    {
        if (errno == ENOENT)
        {
            if (missing_ok)
            {
                FIN_FILE_CRC32(use_crc32c, crc);
                return crc;
            }
        }

        elog(ERROR, "Cannot open file \"%s\": %s",
        file_path, strerror(errno));
    }

    buf = (char *)pgut_malloc(STDIO_BUFSIZE);

    /* calc CRC of file */
    for (;;)
    {
        if (interrupted)
            elog(ERROR, "interrupted during CRC calculation");

        len = gzread(fp, buf, STDIO_BUFSIZE);

        if (len <= 0)
        {
            /* we either run into eof or error */
            if (gzeof(fp))
                break;
            else
            {
                const char *err_str = NULL;

                err_str = gzerror(fp, &err);
                elog(ERROR, "Cannot read from compressed file %s", err_str);
            }
        }

        /* update CRC */
        COMP_FILE_CRC32(use_crc32c, crc, buf, len);
    }

    FIN_FILE_CRC32(use_crc32c, crc);
    gzclose(fp);
    pg_free(buf);

    return crc;
}
#endif

void
pgFileFree(void *file)
{
    pgFile  *file_ptr;

    if (file == NULL)
        return;

    file_ptr = (pgFile *) file;

    pfree(file_ptr->linked);

    if (file_ptr->rel_path != NULL) {
        pfree(file_ptr->rel_path);

    } else if (file_ptr->name != NULL) {
        pfree(file_ptr->name);
    }

    pfree(file);
}

/* Compare two pgFile with their path in ascending order of ASCII code. */
int
pgFileMapComparePath(const void *f1, const void *f2)
{
    page_map_entry *f1p = *(page_map_entry **)f1;
    page_map_entry *f2p = *(page_map_entry **)f2;

    return strcmp(f1p->path, f2p->path);
}

/* Compare two pgFile with their name in ascending order of ASCII code. */
int
pgFileCompareName(const void *f1, const void *f2)
{
    pgFile *f1p = *(pgFile **)f1;
    pgFile *f2p = *(pgFile **)f2;

    return strcmp(f1p->name, f2p->name);
}

/*
 * Compare two pgFile with their relative path and external_dir_num in ascending
 * order of ASСII code.
 */
int
pgFileCompareRelPathWithExternal(const void *f1, const void *f2)
{
    pgFile *f1p = *(pgFile **)f1;
    pgFile *f2p = *(pgFile **)f2;
    int         res;

    res = strcmp(f1p->rel_path, f2p->rel_path);
    if (res == 0)
    {
        if (f1p->external_dir_num > f2p->external_dir_num || f1p->type > f2p->type)
            return 1;
        else if (f1p->external_dir_num < f2p->external_dir_num || f1p->type < f2p->type)
            return -1;
        else
            return 0;
    }
    return res;
}

/*
 * Compare two pgFile with their rel_path and external_dir_num
 * in descending order of ASCII code.
 */
int
pgFileCompareRelPathWithExternalDesc(const void *f1, const void *f2)
{
    return -pgFileCompareRelPathWithExternal(f1, f2);
}

/* Compare two pgFile with their linked directory path. */
int
pgFileCompareLinked(const void *f1, const void *f2)
{
    pgFile  *f1p = (pgFile *)const_cast<void*>(f1);
    pgFile  *f2p = (pgFile *)const_cast<void*>(f2);

    return strcmp(f1p->linked, f2p->linked);
}

/* Compare two pgFile with their size */
int
pgFileCompareSize(const void *f1, const void *f2)
{
    pgFile *f1p = *(pgFile **)f1;
    pgFile *f2p = *(pgFile **)f2;

    if (f1p->size > f2p->size)
        return 1;
    else if (f1p->size < f2p->size)
        return -1;
    else
        return 0;
}

static int
pgCompareString(const void *str1, const void *str2)
{
    return strcmp(*(char **) str1, *(char **) str2);
}

/* Compare two Oids */
int
pgCompareOid(const void *f1, const void *f2)
{
    Oid *v1 = (Oid *)const_cast<void*>(f1);
    Oid *v2 = (Oid *)const_cast<void*>(f2);

    if (*v1 > *v2)
        return 1;
    else if (*v1 < *v2)
        return -1;
    else
        return 0;
}


void
db_map_entry_free(void *entry)
{
    db_map_entry *m = (db_map_entry *) entry;

    free(m->datname);
    free(entry);
}

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When follow_symlink is true, symbolic link is ignored and only file or
 * directory linked to will be listed.
 */
void
dir_list_file(parray *files, const char *root, bool exclude, bool follow_symlink,
                        bool add_root, bool backup_logs, bool skip_hidden, int external_dir_num,
                        fio_location location, bool backup_replslots)
{
    pgFile  *file;

    file = pgFileNew(root, "", follow_symlink, external_dir_num, location);
    if (file == NULL)
    {
        /* For external directory this is not ok */
        if (external_dir_num > 0)
            elog(ERROR, "External directory is not found: \"%s\"", root);
        else
            return;
    }

    if (!S_ISDIR(file->mode))
    {
        if (external_dir_num > 0)
            elog(ERROR, " --external-dirs option \"%s\": directory or symbolic link expected",
                root);
        else
            elog(WARNING, "Skip \"%s\": unexpected file format", root);
        return;
    }
    if (add_root)
        parray_append(files, file);

    dir_list_file_internal(files, file, root, exclude, follow_symlink,
                                        backup_logs, skip_hidden, external_dir_num, location, backup_replslots);

    if (!add_root)
        pgFileFree(file);
}

#define CHECK_FALSE                     0
#define CHECK_TRUE                      1
#define CHECK_EXCLUDE_FALSE         2

/*
 * Check file or directory.
 *
 * Check for exclude.
 * Extract information about the file parsing its name.
 * Skip files:
 * - skip temp tables files
 * - skip unlogged tables files
 * Skip recursive tablespace content
 * Set flags for:
 * - database directories
 * - datafiles
 */
static char
dir_check_file(pgFile *file, bool backup_logs, bool backup_replslots)
{
    int     i;
    int     sscanf_res;
    char    ret;
    bool    in_tablespace = false;
    char    check_res;

    in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path);

    /* Check if we need to exclude file by name */
    if (S_ISREG(file->mode))
    {
        if (!exclusive_backup)
        {
            for (i = 0; pgdata_exclude_files_non_exclusive[i]; i++)
                if (strcmp(file->rel_path,
                pgdata_exclude_files_non_exclusive[i]) == 0)
                {
                    /* Skip */
                    elog(VERBOSE, "Excluding file: %s", file->name);
                    return CHECK_FALSE;
                }
        }

        for (i = 0; pgdata_exclude_files[i]; i++)
            if (strcmp(file->rel_path, pgdata_exclude_files[i]) == 0)
            {
                /* Skip */
                elog(VERBOSE, "Excluding file: %s", file->name);
                return CHECK_FALSE;
            }
    }
    /*
    * If the directory name is in the exclude list, do not list the
    * contents.
    */
    else if (S_ISDIR(file->mode) && !in_tablespace && file->external_dir_num == 0)
    {
        /*
        * If the item in the exclude list starts with '/', compare to
        * the absolute path of the directory. Otherwise compare to the
        * directory name portion.
        */
        for (i = 0; pgdata_exclude_dir[i]; i++)
        {
            /* relative path exclude */
            if (strcmp(file->rel_path, pgdata_exclude_dir[i]) == 0)
            {
                elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
                return CHECK_EXCLUDE_FALSE;
            }
        }

        if (!backup_logs)
        {
            if (strcmp(file->rel_path, PG_LOG_DIR) == 0)
            {
                /* Skip */
                elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
                return CHECK_EXCLUDE_FALSE;
            }
        }

        /*
         * Backup pg_replslot if it is specified.
         * It is generally not useful to backup the contents of this directory even
         * if the intention is to restore to another master. See backup.sgml for a
         * more detailed description.
         */
        if (!backup_replslots) {
            if (strcmp(file->rel_path, PG_REPLSLOT_DIR) == 0) {
                /* Skip */
                elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
                return CHECK_EXCLUDE_FALSE;
            }
        } else {
            /* 
             * Check file that under pg_replslot and judge whether it
             * belonged to logical replication slots for subscriptions.
             */
            if (strcmp(file->rel_path, PG_REPLSLOT_DIR) != 0 &&
                path_is_prefix_of_path(PG_REPLSLOT_DIR, file->rel_path)) {
                return check_logical_replslot_dir(file->rel_path);
            }
        }

        ret = check_nobackup_dir(file);
        if (ret != -1) {  /* -1 means need backup */
            return ret;
        }

    }

    /*
    * Do not copy tablespaces twice. It may happen if the tablespace is located
    * inside the PGDATA.
    */
    if (S_ISDIR(file->mode) &&
        strcmp(file->name, TABLESPACE_VERSION_DIRECTORY) == 0)
    {
        Oid tblspcOid;
        char    tmp_rel_path[MAXPGPATH];

        if (in_tablespace && IsDssMode())
            return CHECK_FALSE;
        /*
        * Valid path for the tablespace is
        * pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
        */
        if (!IsDssMode())
        {
            if (!in_tablespace)
                return CHECK_FALSE;
            sscanf_res = sscanf_s(file->rel_path, PG_TBLSPC_DIR "/%u/%s",
                                        &tblspcOid, tmp_rel_path, MAXPGPATH);
            if (sscanf_res == 0)
                return CHECK_FALSE;
        }
    }

    /* skip other instance files in dss mode */
    check_res = check_in_dss(file, instance_config.dss.instance_id);
    if (check_res != CHECK_TRUE) {
        return check_res;
    }

    ret = check_in_tablespace(file, in_tablespace);
    if (ret != -1) {
        return ret;
    }

    return check_db_dir(file);
}

static char check_in_dss(pgFile *file, int include_id)
{
    char instance_id[MAX_INSTANCEID_LEN];
    char top_path[MAXPGPATH];
    errno_t rc = EOK;
    int move = 0;

    if (!is_dss_type(file->type)) {
        return CHECK_TRUE;
    }

    /* step1 : skip other instance owner file or dir */
    strlcpy(top_path, file->rel_path, sizeof(top_path));
    get_top_path(top_path);

    rc = snprintf_s(instance_id, sizeof(instance_id), sizeof(instance_id) - 1, "%d", include_id);
    securec_check_ss_c(rc, "\0", "\0");

    move = (int)strlen(top_path) - (int)strlen(instance_id);
    if (move > 0 && move < MAXPGPATH && strcmp(top_path + move, instance_id) != 0) {
        char tail = top_path[strlen(top_path) - 1];
        /* Is this file or dir belongs to other instance? */
        if (tail >= '0' && tail <= '9') {
            return CHECK_FALSE;
        }
    }

    /* step2: recheck dir is in the exclude list, include id will be considered */
    if (S_ISDIR(file->mode)) {
        for (int i = 0; pgdata_exclude_dir[i]; i++) {
            int len = (int)strlen(pgdata_exclude_dir[i]);
            if (strncmp(top_path, pgdata_exclude_dir[i], len) == 0 &&
                strcmp(top_path + len, instance_id) == 0) {
                return CHECK_EXCLUDE_FALSE;
            }
        }
    }

    return CHECK_TRUE;
}

static char check_in_tablespace(pgFile *file, bool in_tablespace)
{
    if (in_tablespace)
    {
        int  sscanf_res;
        char tmp_rel_path[MAXPGPATH];

        sscanf_res = sscanf_s(file->rel_path, PG_TBLSPC_DIR "/%u/%[^/]/%u/",
                              &(file->tblspcOid), tmp_rel_path, sizeof(tmp_rel_path),
                              &(file->dbOid));

        /*
        * We should skip other files and directories rather than
        * TABLESPACE_VERSION_DIRECTORY, if this is recursive tablespace.
        */
        if (sscanf_res == 2 && strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) != 0)
            return CHECK_FALSE;

        if (sscanf_res == 3 && S_ISDIR(file->mode) &&
            strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) == 0)
            file->is_database = true;
    }
    else if (path_is_prefix_of_path("global", file->rel_path))
    {
        file->tblspcOid = GLOBALTABLESPACE_OID;

        if (S_ISDIR(file->mode) && strcmp(file->name, "global") == 0)
            file->is_database = true;
    }
    else if (path_is_prefix_of_path("base", file->rel_path))
    {
        file->tblspcOid = DEFAULTTABLESPACE_OID;

        /* skip "base" itself */
        if (strcmp(file->name, "base") != 0)
        {
            int ret = sscanf_s(file->rel_path, "base/%u/", &(file->dbOid));
            if (ret == -1)
                elog(INFO, "Cannot parse path \"%s\"", file->rel_path);
            if (S_ISDIR(file->mode))
                file->is_database = true;
        }
    }

    return -1;
}

static char check_nobackup_dir(pgFile *file)
{
    char ret = -1;   /* -1 means need backup */
    int i = 0;
    if (pgdata_nobackup_dir) {
        for (i = 0; i < (int)parray_num(pgdata_nobackup_dir); i++) {
            char *file_path = (char *)parray_get(pgdata_nobackup_dir, i);
            /* relative tablespace path exclude */
            if (strcmp(file->rel_path, file_path) == 0) {
                elog(VERBOSE, "Excluding external directory content: %s", file->rel_path);
                return CHECK_EXCLUDE_FALSE;
            }
        }
    }
    return ret;
}

char check_logical_replslot_dir(const char *rel_path)
{
    char ret = CHECK_FALSE;
    int i = 0;
    char *tmp = pg_strdup(rel_path);
    char *p;
#define DIRECTORY_DELIMITER "/"

    if (logical_replslot) {
        /* extract slot name from rel_path, such as sub1 from pg_replslot/sub1/snap */
        p = strtok(tmp, DIRECTORY_DELIMITER);
        if (p != NULL) {
            p = strtok(NULL, DIRECTORY_DELIMITER);
        }

        for (i = 0; p != NULL && i < (int)parray_num(logical_replslot); i++) {
            char *slotName = (char *)parray_get(logical_replslot, i);
            if (strcmp(p, slotName) == 0) {
                pfree(tmp);
                return CHECK_TRUE;
            }
        }
    } else {
        ret = CHECK_TRUE;
    }
    pfree(tmp);
    return ret;
}

static char check_db_dir(pgFile *file)
{
    char ret = -1;

    /* Do not backup ptrack_init files */
    if (S_ISREG(file->mode) && strcmp(file->name, "ptrack_init") == 0)
        return CHECK_FALSE;

    /*
    * Check files located inside database directories including directory
    * 'global'
    */
    if (S_ISREG(file->mode) && file->tblspcOid != 0 &&
        file->name && file->name[0])
    {
        if (strcmp(file->name, "pg_internal.init") == 0)
            return CHECK_FALSE;
        /* Do not backup ptrack2.x map files */
        else if (strcmp(file->name, "ptrack.map") == 0)
            return CHECK_FALSE;
        else if (strcmp(file->name, "ptrack.map.mmap") == 0)
            return CHECK_FALSE;
        else if (strcmp(file->name, "ptrack.map.tmp") == 0)
            return CHECK_FALSE;
        /* Do not backup temp files */
        else if (file->name[0] == 't' && isdigit(file->name[1]))
            return CHECK_FALSE;
        else if (isdigit(file->name[0]))
        {
            ret = check_digit_file(file);
        }
    }

    if (ret == -1) {
        ret = CHECK_TRUE;
    }

    return ret;
}

static char check_digit_file(pgFile *file)
{
    char    *fork_name;
    uint32     len;
    char    suffix[MAXPGPATH];
    int sscanf_res;

    if (PageCompression::SkipCompressedFile(file->name, MAXPGPATH)) {
        /* change oid_compress or oid.x_compress to oid or oid.x */
        uint32 offset = strlen(file->name) - strlen("_compress");
        file->name[offset] = '\0';
        sscanf_res = sscanf_s(file->name, "%u.%d.%s", &(file->relOid),
                              &(file->segno), suffix, sizeof(suffix));
        /* recover oid_compress or oid.x_compress from oid or oid.x */
        file->name[offset] = '_';
        if (sscanf_res == 0) {
            elog(ERROR, "Cannot parse compressed file name \"%s\"", file->name);
        } else if (sscanf_res == 1 || sscanf_res == 2) {
            file->is_datafile = true;
            file->segno = (sscanf_res == 1) ? 0 : file->segno;
        }

        return CHECK_TRUE;
    }

    fork_name = strstr(file->name, "_");
    if (fork_name)
    {
        /* Auxiliary fork of the relfile */
        if (strcmp(fork_name, "vm") == 0)
            file->forkName = vm;

        else if (strcmp(fork_name, "fsm") == 0)
            file->forkName = fsm;

        else if (strcmp(fork_name, "cfm") == 0)
            file->forkName = cfm;

        else if (strcmp(fork_name, "ptrack") == 0)
            file->forkName = ptrack;

        else if (strcmp(fork_name, "init") == 0)
            file->forkName = init;

        /* Do not backup ptrack files */
        if (file->forkName == ptrack)
            return CHECK_FALSE;
    }
    else
    {
        /*
        * snapfs files:
        * RELFILENODE.BLOCKNO.snapmap.SNAPID
        * RELFILENODE.BLOCKNO.snap.SNAPID
        */
        if (strstr(file->name, "snap") != NULL)
                return CHECK_TRUE;

        /* reloid.cfm */
        len = strlen(file->name);
        if (len > 3 && strcmp(file->name + len - 3, "cfm") == 0)
            return CHECK_TRUE;

        sscanf_res = sscanf_s(file->name, "%u.%d.%s", &(file->relOid),
                              &(file->segno), suffix, sizeof(suffix));
        if (sscanf_res == 0)
            elog(ERROR, "Cannot parse file name \"%s\"", file->name);
        else if (sscanf_res == 1 || sscanf_res == 2)
            file->is_datafile = true;
    }

    return -1;
}

static inline void SetFileCompressOption(pgFile *file, char *child)
{
    if (!file->is_datafile ||
        !PageCompression::SkipCompressedFile(child, strlen(child))) {
        return;
    }

    file->compressed_file = true;
    file->size = 0;
    file->compressed_chunk_size = 0;
    file->compressed_algorithm = 0;

    PageCompression *pageCompression = new(std::nothrow) PageCompression();
    if (pageCompression == NULL) {
        elog(ERROR, "compression page init failed");
        return;
    }
    if (pageCompression->Init(child, file->segno) != SUCCESS) {
        delete pageCompression;
        elog(ERROR, "Cannot parse compressed file name \"%s\"", file->name);
        return;
    }

    BlockNumber blknum = pageCompression->GetMaxBlockNumber();
    file->size = blknum * BLCKSZ;
    file->compressed_chunk_size = pageCompression->chunkSize;
    file->compressed_algorithm = pageCompression->algorithm;

    delete pageCompression;
    return;
}

bool SkipSomeDirFile(pgFile *file, struct dirent *dent, bool skipHidden)
{
    /* Skip entries point current dir or parent dir */
    if (S_ISDIR(file->mode) && (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)) {
        return false;
    }
    /* Skip recycle in dss mode */
    if (is_dss_type(file->type) && strcmp(dent->d_name, ".recycle") == 0)
    {
        elog(WARNING, "Skip .recycle");
        return false;
    }
    /* skip hidden files and directories */
    if (skipHidden && file->name[0] == '.') {
        elog(WARNING, "Skip hidden file: '%s'", file->name);
        return false;
    }
    /*
     * Add only files, directories and links. Skip sockets and other
     * unexpected file formats.
     */
    if (!S_ISDIR(file->mode) && !S_ISREG(file->mode) && !S_ISLNK(file->mode)) {
        elog(WARNING, "Skip '%s': unexpected file format", file->name);
        return false;
    }
    return true;
}

/*
 * List files in parent->path directory.  If "exclude" is true do not add into
 * "files" files from pgdata_exclude_files and directories from
 * pgdata_exclude_dir.
 */
static void
dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
                                        bool exclude, bool follow_symlink, bool backup_logs,
                                        bool skip_hidden, int external_dir_num, fio_location location,
                                        bool backup_replslots)
{
    DIR     *dir;
    struct dirent *dent;

    if (!S_ISDIR(parent->mode))
        elog(ERROR, "\"%s\" is not a directory", parent_dir);

    /* Open directory and list contents */
    dir = fio_opendir(parent_dir, location);
    if (dir == NULL)
    {
        if (is_file_delete(errno))
        {
            /* Maybe the directory was removed */
            return;
        }
        elog(ERROR, "Cannot open directory \"%s\": %s",
        parent_dir, strerror(errno));
    }

    errno = 0;
    while ((dent = fio_readdir(dir)))
    {
        pgFile  *file;
        char    child[MAXPGPATH];
        char    rel_child[MAXPGPATH];
        char    check_res;

        join_path_components(child, parent_dir, dent->d_name);
        join_path_components(rel_child, parent->rel_path, dent->d_name);

        file = pgFileNew(child, rel_child, follow_symlink, external_dir_num, location);
        if (file == NULL)
            continue;

        if (!SkipSomeDirFile(file, dent, skip_hidden)) {
            pgFileFree(file);
            continue;
        }

        if (exclude)
        {
            check_res = dir_check_file(file, backup_logs, backup_replslots);
            if (check_res == CHECK_FALSE)
            {
                /* Skip */
                pgFileFree(file);
                continue;
            }
            else if (check_res == CHECK_EXCLUDE_FALSE)
            {
                /* We add the directory itself which content was excluded */
                parray_append(files, file);
                continue;
            } else if (check_res == CHECK_TRUE) {
                /* just deal with compressed file, persist it's option */
                SetFileCompressOption(file, child);
            }
        }

        parray_append(files, file);

        /*
        * If the entry is a directory call dir_list_file_internal()
        * recursively.
        */
        if (S_ISDIR(file->mode))
            dir_list_file_internal(files, file, child, exclude, follow_symlink,
                backup_logs, skip_hidden, external_dir_num, location, backup_replslots);
    }

    if (errno && !is_file_delete(errno))
    {
        int errno_tmp = errno;
        fio_closedir(dir);
        elog(ERROR, "Cannot read directory \"%s\": %s",
            parent_dir, strerror(errno_tmp));
    }
    fio_closedir(dir);
}

/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 *
 * Copy of function get_tablespace_mapping() from pg_basebackup.c.
 */
static const char *
get_tablespace_mapping(const char *dir)
{
    TablespaceListCell *cell;

    for (cell = tablespace_dirs.head; cell; cell = cell->next)
        if (strcmp(dir, cell->old_dir) == 0)
            return cell->new_dir;

    return dir;
}

/*
 * Split argument into old_dir and new_dir and append to mapping
 * list.
 *
 * Copy of function tablespace_list_append() from pg_basebackup.c.
 */
static void
opt_path_map(ConfigOption *opt, const char *arg, TablespaceList *list,
                            const char *type)
{
    TablespaceListCell *cell = pgut_new(TablespaceListCell);
    char    *dst = nullptr;
    char    *dst_ptr = nullptr;
    const char *arg_ptr;
    errno_t rc = 0;

    rc = memset_s(cell, sizeof(TablespaceListCell), 0, sizeof(TablespaceListCell));
    securec_check(rc, "\0", "\0");
    dst_ptr = dst = cell->old_dir;
    for (arg_ptr = arg; *arg_ptr; arg_ptr++)
    {
        if (dst_ptr - dst >= MAXPGPATH)
            elog(ERROR, "directory name too long");

        if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=')
            ;   /* skip backslash escaping = */
        else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\'))
        {
            if (*cell->new_dir)
                elog(ERROR, "multiple \"=\" signs in %s mapping\n", type);
            else
                dst = dst_ptr = cell->new_dir;
        }
        else
            *dst_ptr++ = *arg_ptr;
    }

    if (!*cell->old_dir || !*cell->new_dir)
        elog(ERROR, "invalid %s mapping format \"%s\", "
            "must be \"OLDDIR=NEWDIR\"", type, arg);
            canonicalize_path(cell->old_dir);
            canonicalize_path(cell->new_dir);

    /*
    * This check isn't absolutely necessary.  But all tablespaces are created
    * with absolute directories, so specifying a non-absolute path here would
    * just never match, possibly confusing users.  It's also good to be
    * consistent with the new_dir check.
    */
    if (!is_absolute_path(cell->old_dir))
        elog(ERROR, "old directory is not an absolute path in %s mapping: %s\n",
            type, cell->old_dir);

    if (!is_absolute_path(cell->new_dir))
        elog(ERROR, "new directory is not an absolute path in %s mapping: %s\n",
            type, cell->new_dir);

    if (list->tail)
        list->tail->next = cell;
    else
        list->head = cell;
    list->tail = cell;

    if (strncmp(type, "tablespace", strlen(type)) == 0) {
        specify_tbsdir = true;
    } else if (strncmp(type, "external directory", strlen(type)) == 0) {
        specify_extdir = true;
    }
}

/* Parse tablespace mapping */
void
opt_tablespace_map(ConfigOption *opt, const char *arg)
{
    opt_path_map(opt, arg, &tablespace_dirs, "tablespace");
}

/* Parse external directories mapping */
void
opt_externaldir_map(ConfigOption *opt, const char *arg)
{
    opt_path_map(opt, arg, &external_remap_list, "external directory");
}

/*
 * Create directories from **dest_files** in **data_dir**.
 *
 * If **extract_tablespaces** is true then try to extract tablespace data
 * directories into their initial path using tablespace_map file.
 * Use **backup_dir** for tablespace_map extracting.
 *
 * Enforce permissions from backup_content.control. The only
 * problem now is with PGDATA itself.
 * TODO: we must preserve PGDATA permissions somewhere. Is it actually a problem?
 * Shouldn`t starting openGauss force correct permissions on PGDATA?
 *
 * TODO: symlink handling. If user located symlink in PG_TBLSPC_DIR, it will
 * be restored as directory.
 */
void
create_data_directories(parray *dest_files, const char *data_dir, const char *backup_dir,
                                                bool extract_tablespaces, bool incremental, fio_location location, bool is_restore)
{
    size_t     i = 0;
    parray      *links = NULL;
    mode_t  pg_tablespace_mode = DIR_PERMISSION;
    char    to_path[MAXPGPATH];
    errno_t rc = 0;

    /* get tablespace map */
    if (extract_tablespaces)
    {
        links = parray_new();
        read_tablespace_map(links, backup_dir);
        /* Sort links by a link name */
        parray_qsort(links, pgFileCompareName);
    }

    /*
    * We have no idea about tablespace permission
    * For PG < 11 we can just force default permissions.
    */
#if PG_VERSION_NUM >= 110000
    if (links)
    {
        /* For PG>=11 we use temp kludge: trust permissions on 'pg_tblspc'
        * and force them on every tablespace.
        * TODO: remove kludge and ask data_directory_mode
        * at the start of backup.
        */
        for (i = 0; i < parray_num(dest_files); i++)
        {
            pgFile  *file = (pgFile *) parray_get(dest_files, i);

            if (!S_ISDIR(file->mode))
                continue;

            /* skip external directory content */
            if (file->external_dir_num != 0)
                continue;

            /* look for 'pg_tblspc' directory  */
            if (strcmp(file->rel_path, PG_TBLSPC_DIR) == 0)
            {
                pg_tablespace_mode = file->mode;
                break;
            }
        }
    }
#endif

    /*
    * We iterate over dest_files and for every directory with parent 'pg_tblspc'
    * we must lookup this directory name in tablespace map.
    * If we got a match, we treat this directory as tablespace.
    * It means that we create directory specified in tablespace_map and
    * original directory created as symlink to it.
    */

    elog(LOG, "Restore directories and symlinks... in %s", data_dir);

    /* create directories */
    for (i = 0; i < parray_num(dest_files); i++)
    {
        char parent_dir[MAXPGPATH];
        pgFile       *dir = (pgFile *) parray_get(dest_files, i);

        /* skip undesirable type */
        if (is_dss_type(dir->type) != fio_is_dss(location))
            continue;

        if (!S_ISDIR(dir->mode))
            continue;

        /* skip external directory content */
        if (dir->external_dir_num != 0)
            continue;

        if (is_dss_type(dir->type) && is_restore) {
            if (is_ss_xlog(dir->rel_path)) {
                ss_createdir(dir->rel_path, instance_config.dss.vgdata, instance_config.dss.vglog);
                continue;
            }

            if (ss_create_if_doublewrite(dir, instance_config.dss.vgdata, instance_config.dss.instance_id)) {
                continue;
            }

            if (ss_create_if_pg_replication(dir, instance_config.dss.vgdata, instance_config.dss.vglog)) {
                continue;
            }
        }

        /* tablespace_map exists */
        if (links)
        {
            /* get parent dir of rel_path */
            rc = strncpy_s(parent_dir, MAXPGPATH, dir->rel_path, MAXPGPATH - 1);
            securec_check_c(rc, "", "");
            get_parent_directory(parent_dir);

            /* check if directory is actually link to tablespace */
            if (strcmp(parent_dir, PG_TBLSPC_DIR) == 0)
            {
                /* this directory located in pg_tblspc
                * check it against tablespace map
                */
                pgFile **link = (pgFile **) parray_bsearch(links, dir, pgFileCompareName);

                /* got match */
                if (link)
                {
                    const char *linked_path = get_tablespace_mapping((*link)->linked);

                    if (!is_absolute_path(linked_path) && !ss_is_absolute_path(linked_path))
                        elog(ERROR, "Tablespace directory is not an absolute path: %s\n",
                            linked_path);

                    join_path_components(to_path, data_dir, dir->rel_path);

                    elog(VERBOSE, "Create directory \"%s\" and symbolic link \"%s\"",
                        linked_path, to_path);

                    /* create tablespace directory */
                    fio_mkdir(linked_path, pg_tablespace_mode, location);

                    /* create link to linked_path */
                    if (fio_symlink(linked_path, to_path, incremental, location) < 0)
                        elog(ERROR, "Could not create symbolic link \"%s\": %s",
                            to_path, strerror(errno));

                    continue;
                }
            }
        }

        /* This is not symlink, create directory */
        elog(VERBOSE, "Create directory \"%s\"", dir->rel_path);

        join_path_components(to_path, data_dir, dir->rel_path);
        fio_mkdir(to_path, dir->mode, location);
    }

    if (extract_tablespaces)
    {
        parray_walk(links, pgFileFree);
        parray_free(links);
    }
}

/*
 * Read names of symbolic names of tablespaces with links to directories from
 * tablespace_map or tablespace_map.txt.
 */
void
read_tablespace_map(parray *files, const char *backup_dir)
{
    FILE    *fp;
    char    db_path[MAXPGPATH],
    map_path[MAXPGPATH];
    char    buf[MAXPGPATH * 2];
    errno_t rc = 0;

    join_path_components(db_path, backup_dir, DATABASE_DIR);
    join_path_components(map_path, db_path, PG_TABLESPACE_MAP_FILE);

    /* Exit if database/tablespace_map doesn't exist */
    if (current.media_type == MEDIA_TYPE_OSS) {
        restoreConfigFile(map_path, true);
    }

    if (!fileExists(map_path, FIO_BACKUP_HOST))
    {
        elog(LOG, "there is no file tablespace_map");
        return;
    }

    fp = fio_open_stream(map_path, FIO_BACKUP_HOST);
    if (fp == NULL)
        elog(ERROR, "cannot open \"%s\": %s", map_path, strerror(errno));

    while (fgets(buf, lengthof(buf), fp))
    {
        char    link_name[MAXPGPATH],
                    path[MAXPGPATH];
        pgFile  *file;

        if (sscanf_s(buf, "%s %s", link_name, sizeof(buf), path, sizeof(buf)) != 2)
            elog(ERROR, "invalid format found in \"%s\"", map_path);

        file = pgut_new(pgFile);
        rc = memset_s(file, sizeof(pgFile), 0, sizeof(pgFile));
        securec_check(rc, "\0", "\0");

        /* follow the convention for pgFileFree */
        file->name = pgut_strdup(link_name);
        file->linked = pgut_strdup(path);
        canonicalize_path(file->linked);

        parray_append(files, file);
    }

    if (ferror(fp))
        elog(ERROR, "Failed to read from file: \"%s\"", map_path);

    fio_close_stream(fp);
}

/*
 * Check that all tablespace mapping entries have correct linked directory
 * paths. Linked directories must be empty or do not exist, unless
 * we are running incremental restore, then linked directories can be nonempty.
 *
 * If tablespace-mapping option is supplied, all OLDDIR entries must have
 * entries in tablespace_map file.
 *
 *
 * TODO: maybe when running incremental restore with tablespace remapping, then
 * new tablespace directory MUST be empty? because there is no way
 * we can be sure, that files laying there belong to our instance.
 */
void
check_tablespace_mapping(pgBackup *backup, bool incremental, bool *tblspaces_are_empty)
{
    parray  *links;
    size_t  i;
    TablespaceListCell *cell;
    pgFile  *tmp_file = pgut_new(pgFile);

    links = parray_new();

    read_tablespace_map(links, backup->root_dir);
    /* Sort links by the path of a linked file*/

    elog(LOG, "check tablespace directories of backup %s",
        base36enc(backup->start_time));

    /* 1 - each OLDDIR must have an entry in tablespace_map file (links) */
    for (cell = tablespace_dirs.head; cell; cell = cell->next)
    {
        tmp_file->linked = cell->old_dir;

        for (i = 0; i < parray_num(links); i++) {
            pgFile *link = (pgFile *) parray_get(links, i);
            if (strncmp(link->linked, tmp_file->linked, strlen(tmp_file->linked)) == 0) {
                break;
            }
        }
        if (i == parray_num(links)) {
            elog(ERROR, "--tablespace-mapping option's old directory "
            "doesn't have an entry in tablespace_map file: \"%s\"",
            cell->old_dir);
        }
    }

    /* 2 - all linked directories must be empty */
    for (i = 0; i < parray_num(links); i++)
    {
        pgFile  *link = (pgFile *) parray_get(links, i);
        const char *linked_path = link->linked;
        TablespaceListCell *cell;

        for (cell = tablespace_dirs.head; cell; cell = cell->next)
            if (strcmp(link->linked, cell->old_dir) == 0)
            {
                linked_path = cell->new_dir;
                break;
            }

        if (!is_absolute_path(linked_path) && !ss_is_absolute_path(linked_path))
            elog(ERROR, "tablespace directory is not an absolute path: %s\n",
                linked_path);

        if (!dir_is_empty(linked_path, FIO_DB_HOST))
        {
            if (!incremental) {
                elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
                    linked_path);
            }
            *tblspaces_are_empty = false;
        }
    }

    free(tmp_file);
    parray_walk(links, pgFileFree);
    parray_free(links);
}

void
check_external_dir_mapping(pgBackup *backup, bool incremental)
{
    TablespaceListCell *cell;
    parray *external_dirs_to_restore;
    size_t  i = 0;

    elog(LOG, "check external directories of backup %s",
        base36enc(backup->start_time));

    if (!backup->external_dir_str)
    {
        if (external_remap_list.head)
            elog(ERROR, "--external-mapping option's old directory doesn't "
            "have an entry in list of external directories of current "
            "backup: \"%s\"", external_remap_list.head->old_dir);
        return;
    }

    external_dirs_to_restore = make_external_directory_list(
        backup->external_dir_str,
        false);
    /* 1 - each OLDDIR must have an entry in external_dirs_to_restore */
    for (cell = external_remap_list.head; cell; cell = cell->next)
    {
        bool    found = false;

        for (i = 0; i < parray_num(external_dirs_to_restore); i++)
        {
            char    *external_dir = (char *)parray_get(external_dirs_to_restore, i);

            if (strcmp(cell->old_dir, external_dir) == 0)
            {
                if (is_dss_file(cell->new_dir))
                    elog(ERROR, "New external directory path \"%s\" "
                        "contains dss path, which is not allow.", cell->new_dir);
                /* Swap new dir name with old one, it is used by 2-nd step */
                parray_set(external_dirs_to_restore, i,
                   pgut_strdup(cell->new_dir));
                pfree(external_dir);

                found = true;
                break;
            }
        }
        if (!found)
            elog(ERROR, "--external-mapping option's old directory doesn't "
                "have an entry in list of external directories of current "
                "backup: \"%s\"", cell->old_dir);
    }

    /* 2 - all linked directories must be empty */
    for (i = 0; i < parray_num(external_dirs_to_restore); i++)
    {
        char    *external_dir = (char *) parray_get(external_dirs_to_restore,
                                                                                i);

        if (!incremental && !dir_is_empty(external_dir, FIO_DB_HOST))
            elog(ERROR, "External directory is not empty: \"%s\"",
                external_dir);
    }

    free_dir_list(external_dirs_to_restore);
}

char *
get_external_remap(char *current_dir)
{
    TablespaceListCell *cell;

    for (cell = external_remap_list.head; cell; cell = cell->next)
    {
        char *old_dir = cell->old_dir;

        if (strcmp(old_dir, current_dir) == 0)
            return cell->new_dir;
    }
    return current_dir;
}

/* Parsing states for get_control_value() */
#define CONTROL_WAIT_NAME   1
#define CONTROL_INNAME          2
#define CONTROL_WAIT_COLON      3
#define CONTROL_WAIT_VALUE          4
#define CONTROL_INVALUE                 5
#define CONTROL_WAIT_NEXT_NAME      6

/*
 * Get value from json-like line "str" of backup_content.control file.
 *
 * The line has the following format:
 *   {"name1":"value1", "name2":"value2"}
 *
 * The value will be returned to "value_str" as string if it is not NULL. If it
 * is NULL the value will be returned to "value_int64" as int64.
 *
 * Returns true if the value was found in the line.
 */
static bool
get_control_value(const char *str, const char *name,
                                    char *value_str, int64 *value_int64, bool is_mandatory)
{
    int     state = CONTROL_WAIT_NAME;
    const char  *name_ptr = (const char *) name;
    const char  *buf = (const char *) str;
    char    buf_int64[32],  /* Buffer for "value_int64" */
                *buf_int64_ptr = buf_int64;

    /* Set default values */
    if (value_str)
        *value_str = '\0';
    else if (value_int64)
        *value_int64 = 0;

    while (*buf)
    {
        switch (state)
        {
            case CONTROL_WAIT_NAME:
                if (*buf == '"')
                    state = CONTROL_INNAME;
                else if (IsAlpha(*buf))
                    goto bad_format;
                break;
            case CONTROL_INNAME:
                /* Found target field. Parse value. */
                if (*buf == '"')
                    state = CONTROL_WAIT_COLON;
                /* Check next field */
                else if (*buf != *name_ptr)
                {
                    name_ptr = (const char *) name;
                    state = CONTROL_WAIT_NEXT_NAME;
                }
                else
                    name_ptr++;
                break;
            case CONTROL_WAIT_COLON:
                if (*buf == ':')
                    state = CONTROL_WAIT_VALUE;
                else if (!IsSpace(*buf))
                    goto bad_format;
                break;
            case CONTROL_WAIT_VALUE:
                if (*buf == '"')
                {
                    state = CONTROL_INVALUE;
                    buf_int64_ptr = buf_int64;
                }
                else if (IsAlpha(*buf))
                    goto bad_format;
                break;
            case CONTROL_INVALUE:
                /* Value was parsed, exit */
                if (*buf == '"')
                {
                    if (value_str)
                    {
                        *value_str = '\0';
                    }
                    else if (value_int64)
                    {
                        /* Length of buf_uint64 should not be greater than 31 */
                        if (buf_int64_ptr - buf_int64 >= 32)
                            elog(ERROR, "field \"%s\" is out of range in the line %s of the file %s",
                                name, str, DATABASE_FILE_LIST);

                        *buf_int64_ptr = '\0';
                        if (!parse_int64(buf_int64, value_int64, 0))
                        {
                            /* We assume that too big value is -1 */
                            if (errno == ERANGE)
                                *value_int64 = BYTES_INVALID;
                            else
                                goto bad_format;
                        }
                    }

                    return true;
                }
                else
                {
                    if (value_str)
                    {
                        *value_str = *buf;
                        value_str++;
                    }
                    else
                    {
                        *buf_int64_ptr = *buf;
                        buf_int64_ptr++;
                    }
                }
                break;
            case CONTROL_WAIT_NEXT_NAME:
                if (*buf == ',')
                    state = CONTROL_WAIT_NAME;
                break;
            default:
                /* Should not happen */
                break;
        }

        buf++;
    }

    /* There is no close quotes */
    if (state == CONTROL_INNAME || state == CONTROL_INVALUE)
        goto bad_format;

    /* Did not find target field */
    if (is_mandatory)
        elog(ERROR, "field \"%s\" is not found in the line %s of the file %s",
            name, str, DATABASE_FILE_LIST);
    return false;

    bad_format:
    elog(ERROR, "%s file has invalid format in line %s",
        DATABASE_FILE_LIST, str);
    return false;    /* Make compiler happy */
}

/*
 * Construct parray of pgFile from the backup content list.
 * If root is not NULL, path will be absolute path.
 */
parray *
dir_read_file_list(const char *root, const char *external_prefix,
                   const char *file_txt, fio_location location, pg_crc32 expected_crc)
{
    FILE    *fp;
    parray  *files;
    char     buf[BLCKSZ];
    char     stdio_buf[STDIO_BUFSIZE];
    pg_crc32 content_crc = 0;

    if (current.media_type == MEDIA_TYPE_OSS) {
        restoreConfigFile(file_txt);
    }

    fp = fio_open_stream(file_txt, location);
    if (fp == NULL)
        elog(ERROR, "cannot open \"%s\": %s", file_txt, strerror(errno));

    /* enable stdio buffering for local file */
    if (!fio_is_remote(location))
        setvbuf(fp, stdio_buf, _IOFBF, STDIO_BUFSIZE);

    files = parray_new();

    INIT_FILE_CRC32(true, content_crc);

    while (fgets(buf, lengthof(buf), fp))
    {
        char    path[MAXPGPATH];
        char    linked[MAXPGPATH];
        char    compress_alg_string[MAXPGPATH];
        int64   write_size,
                mode,   /* bit length of mode_t depends on platforms */
                is_datafile,
                is_cfs,
                external_dir_num,
                crc,
                segno,
                n_blocks,
                n_headers,
                dbOid,  /* used for partial restore */
                hdr_crc,
                hdr_off,
                hdr_size,
                file_type;
        pgFile  *file = nullptr;

        COMP_FILE_CRC32(true, content_crc, buf, strlen(buf));

        get_control_value(buf, "path", path, NULL, true);
        get_control_value(buf, "size", NULL, &write_size, true);
        get_control_value(buf, "mode", NULL, &mode, true);
        get_control_value(buf, "is_datafile", NULL, &is_datafile, true);
        get_control_value(buf, "is_cfs", NULL, &is_cfs, false);
        get_control_value(buf, "crc", NULL, &crc, true);
        get_control_value(buf, "compress_alg", compress_alg_string, NULL, false);
        get_control_value(buf, "external_dir_num", NULL, &external_dir_num, false);
        get_control_value(buf, "dbOid", NULL, &dbOid, false);
        get_control_value(buf, "file_type", NULL, &file_type, true);

        file = pgFileInit(path);
        file->write_size = (int64) write_size;
        file->mode = (mode_t) mode;
        file->is_datafile = is_datafile ? true : false;
        file->compressed_file = false;
        file->compressed_chunk_size = 0;
        file->compressed_algorithm = 0;
        file->is_cfs = is_cfs ? true : false;
        file->crc = (pg_crc32) crc;
        file->compress_alg = parse_compress_alg(compress_alg_string);
        file->external_dir_num = external_dir_num;
        file->dbOid = dbOid ? dbOid : 0;
        file->type = (device_type_t)file_type;

        /*
        * Optional fields
        */

        if (get_control_value(buf, "linked", linked, NULL, false) && linked[0])
        {
            file->linked = pgut_strdup(linked);
            canonicalize_path(file->linked);
        }

        /* read compress option from control file */
        int64 compressedFile = 0;
        if (get_control_value(buf, "compressedFile", NULL, &compressedFile, false)) {
            file->compressed_file = true;
            int64 compressed_algorithm = 0;
            int64 compressed_chunk_size = 0;
            (void)get_control_value(buf, "compressedAlgorithm", NULL, &compressed_algorithm, true);
            (void)get_control_value(buf, "compressedChunkSize", NULL, &compressed_chunk_size, true);
            file->compressed_algorithm = (uint8)compressed_algorithm;
            file->compressed_chunk_size = (uint16)compressed_chunk_size;
        }

        if (get_control_value(buf, "segno", NULL, &segno, false))
            file->segno = (int) segno;

        if (get_control_value(buf, "n_blocks", NULL, &n_blocks, false))
            file->n_blocks = (int) n_blocks;

        if (get_control_value(buf, "n_headers", NULL, &n_headers, false))
            file->n_headers = (int) n_headers;

        if (get_control_value(buf, "hdr_crc", NULL, &hdr_crc, false))
            file->hdr_crc = (pg_crc32) hdr_crc;

        if (get_control_value(buf, "hdr_off", NULL, &hdr_off, false))
            file->hdr_off = hdr_off;

        if (get_control_value(buf, "hdr_size", NULL, &hdr_size, false))
            file->hdr_size = (int) hdr_size;

        parray_append(files, file);
    }

    FIN_FILE_CRC32(true, content_crc);

    if (ferror(fp))
        elog(ERROR, "Failed to read from file: \"%s\"", file_txt);

    fio_close_stream(fp);

    if (expected_crc != 0 &&
        expected_crc != content_crc)
    {
        elog(WARNING, "Invalid CRC of backup control file '%s': %u. Expected: %u",
            file_txt, content_crc, expected_crc);
        parray_free(files);
        return NULL;
    }

    if (current.media_type == MEDIA_TYPE_OSS) {
        remove(file_txt);
    }

    return files;
}

/*
 * Check if directory empty.
 */
bool
dir_is_empty(const char *path, fio_location location)
{
    DIR     *dir;
    struct dirent *dir_ent;

    dir = fio_opendir(path, location);
    if (dir == NULL)
    {
        /* Directory in path doesn't exist */
        if (is_file_delete(errno))
            return true;
        elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));
    }

    errno = 0;
    while ((dir_ent = fio_readdir(dir)))
    {
        /* Skip entries point current dir or parent dir */
        if (strcmp(dir_ent->d_name, ".") == 0 ||
            strcmp(dir_ent->d_name, "..") == 0)
            continue;

        /* Skip recycle in dss mode */
        if (fio_is_dss(location) && strcmp(dir_ent->d_name, ".recycle") == 0)
            continue;

        /* Directory is not empty */
        fio_closedir(dir);
        return false;
    }
    if (errno)
        elog(ERROR, "cannot read directory \"%s\": %s", path, strerror(errno));

    fio_closedir(dir);

    return true;
}

/*
 * Return true if the path is a existing regular file.
 */
bool
fileExists(const char *path, fio_location location)
{
    struct stat buf;

    if (fio_stat(path, &buf, true, location) == -1 && is_file_delete(errno))
        return false;
    else if (!S_ISREG(buf.st_mode))
        return false;
    else
        return true;
}

off_t
pgFileSize(const char *path)
{
    struct stat buf;

    if (stat(path, &buf) == -1)
        elog(ERROR, "Cannot stat file \"%s\": %s", path, strerror(errno));

    return buf.st_size;
}

/*
 * Construct parray containing remapped external directories paths
 * from string like /path1:/path2
 */
parray *
make_external_directory_list(const char *colon_separated_dirs, bool remap)
{
    char    *p;
    parray  *list = parray_new();
    char    *tmp = pg_strdup(colon_separated_dirs);

#ifndef WIN32
#define EXTERNAL_DIRECTORY_DELIMITER ":"
#else
#define EXTERNAL_DIRECTORY_DELIMITER ";"
#endif

    p = strtok(tmp, EXTERNAL_DIRECTORY_DELIMITER);
    while(p!=NULL)
    {
        char    *external_path = pg_strdup(p);

        canonicalize_path(external_path);
        if (is_absolute_path(external_path) || ss_is_absolute_path(external_path))
        {
            if (remap)
            {
                char    *full_path = get_external_remap(external_path);

                if (full_path != external_path)
                {
                    full_path = pg_strdup(full_path);
                    pfree(external_path);
                    external_path = full_path;
                }
            }
            parray_append(list, external_path);
        }
        else
            elog(ERROR, "External directory \"%s\" is not an absolute path",
                external_path);

        p = strtok(NULL, EXTERNAL_DIRECTORY_DELIMITER);
    }
    pfree(tmp);
    return list;
}

/* Free memory of parray containing strings */
void
free_dir_list(parray *list)
{
    parray_walk(list, pfree);
    parray_free(list);
}

/* Append to string "path_prefix" int "dir_num" */
void
makeExternalDirPathByNum(char *ret_path, const char *path_prefix, const int dir_num)
{
    int nRet = 0;
    nRet = snprintf_s(ret_path,MAXPGPATH, MAXPGPATH - 1,  "%s%d", path_prefix, dir_num);
    securec_check_ss_c(nRet, "\0", "\0");
}

/* Check if "dir" presents in "dirs_list" */
bool
backup_contains_external(const char *dir, parray *dirs_list)
{
    void *search_result;

    if (!dirs_list) /* There is no external dirs in backup */
        return false;
    search_result = parray_bsearch(dirs_list, dir, pgCompareString);
    return search_result != NULL;
}

/*
 * Print database_map
 */
void
print_database_map(FILE *out, parray *database_map)
{
    size_t i = 0;

    for (i = 0; i < parray_num(database_map); i++)
    {
        db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, i);

        fio_fprintf(out, "{\"dbOid\":\"%u\", \"datname\":\"%s\"}\n",
            db_entry->dbOid, db_entry->datname);
    }

}

/*
 * Create file 'database_map' and add its meta to backup_files_list
 * NULL check for database_map must be done by the caller.
 */
void
write_database_map(pgBackup *backup, parray *database_map, parray *backup_files_list)
{
    FILE    *fp;
    pgFile  *file;
    char    database_dir[MAXPGPATH];
    char    database_map_path[MAXPGPATH];

    join_path_components(database_dir, backup->root_dir, DATABASE_DIR);
    join_path_components(database_map_path, database_dir, DATABASE_MAP);

    fp = fio_fopen(database_map_path, PG_BINARY_W, FIO_BACKUP_HOST);
    if (fp == NULL)
        elog(ERROR, "Cannot open database map \"%s\": %s", database_map_path,
            strerror(errno));

    print_database_map(fp, database_map);
    if (fio_fflush(fp) || fio_fclose(fp))
    {
        fio_unlink(database_map_path, FIO_BACKUP_HOST);
        elog(ERROR, "Cannot write database map \"%s\": %s",
            database_map_path, strerror(errno));
    }

    /* Add metadata to backup_content.control */
    file = pgFileNew(database_map_path, DATABASE_MAP, true, 0,
        FIO_BACKUP_HOST);
    if (file != nullptr) {
        file->crc = pgFileGetCRC(database_map_path, true, false);
        file->write_size = file->size;
        file->uncompressed_size = file->read_size;

        parray_append(backup_files_list, file);
    }
    if (current.media_type == MEDIA_TYPE_OSS) {
        uploadConfigFile(database_map_path, database_map_path);
    }
}

/*
 * read database map, return NULL if database_map in empty or missing
 */
parray *
read_database_map(pgBackup *backup)
{
    FILE    *fp;
    parray  *database_map;
    char    buf[MAXPGPATH];
    char    path[MAXPGPATH];
    char    database_map_path[MAXPGPATH];

    join_path_components(path, backup->root_dir, DATABASE_DIR);
    join_path_components(database_map_path, path, DATABASE_MAP);

    fp = fio_open_stream(database_map_path, FIO_BACKUP_HOST);
    if (fp == NULL)
    {
        /* It is NOT ok for database_map to be missing at this point, so
        * we should error here.
        * It`s a job of the caller to error if database_map is not empty.
        */
        elog(ERROR, "Cannot open \"%s\": %s", database_map_path, strerror(errno));
    }

    database_map = parray_new();

    while (fgets(buf, lengthof(buf), fp))
    {
        char datname[MAXPGPATH];
        int64 dbOid;

        db_map_entry *db_entry = (db_map_entry *) pgut_malloc(sizeof(db_map_entry));

        get_control_value(buf, "dbOid", NULL, &dbOid, true);
        get_control_value(buf, "datname", datname, NULL, true);

        db_entry->dbOid = dbOid;
        db_entry->datname = pgut_strdup(datname);

        parray_append(database_map, db_entry);
    }

    if (ferror(fp))
        elog(ERROR, "Failed to read from file: \"%s\"", database_map_path);

    fio_close_stream(fp);

    /* Return NULL if file is empty */
    if (parray_num(database_map) == 0)
    {
        parray_free(database_map);
        return NULL;
    }

    return database_map;
}
