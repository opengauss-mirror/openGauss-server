/* -------------------------------------------------------------------------
 *
 * file_ops.c
 *	  Helper functions for operating on files.
 *
 * Most of the functions in this file are helper functions for writing to
 * the target data directory. The functions check the --dry-run flag, and
 * do nothing if it's enabled. You should avoid accessing the target files
 * directly but if you do, make sure you honor the --dry-run mode!
 *
 * Portions Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>

#include "file_ops.h"
#include "logging.h"
#include "pg_rewind.h"
#include "pg_build.h"

#include "common/fe_memutils.h"
#include "common/build_query/build_query.h"
#include "replication/replicainternal.h"
#include "storage/file/fio_device.h"

#include <memory>
#define BLOCKSIZE (8 * 1024)

/*
 * Currently open target file.
 */
static int dstfd = -1;
static char dstpath[MAXPGPATH] = "";
static bool g_isRelDataFile = false;

static PageCompression* g_pageCompression = NULL;

static void create_target_dir(const char* path);
static void remove_target_dir(const char* path);
static void create_target_symlink(const char* path, const char* slink);
static void remove_target_symlink(const char* path);

static bool is_special_dir(const char* path);
static void get_file_path(const char* path, const char* file_name, char* file_path);
static bool directory_is_empty(const char* path);
static void copy_dir(const char* fromdir, char* todir);
static void restore_gaussdb_state(void);

/*
 * Open a target file for writing. If 'trunc' is true and the file already
 * exists, it will be truncated.
 * The path must be the relative path.
 */
void open_target_file(const char* path, bool trunc)
{
    int mode;
    int ss_c = 0;

    if (dry_run)
        return;

    if (dstfd != -1 && !trunc && strcmp(path, &dstpath[strlen(pg_data) + 1]) == 0)
        return; /* already open */

    /* fsync xlog file to prevent read checkpoint record failure after execute file map */
    if (dstfd != -1 && strstr(path, "pg_xlog") != NULL) {
        if (fsync(dstfd) !=0 ) {
            pg_fatal("could not fsync target file \"%s\": %s\n", dstpath, strerror(errno));
        }
    }

    close_target_file();

    ss_c = snprintf_s(dstpath, sizeof(dstpath), sizeof(dstpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    mode = O_WRONLY | O_CREAT | PG_BINARY;
    if (trunc)
        mode |= O_TRUNC;
    dstfd = open(dstpath, mode, 0600);
    if (dstfd < 0)
        pg_fatal("could not open target file \"%s\": %s\n", dstpath, strerror(errno));
    g_isRelDataFile = isRelDataFile(dstpath);
}

/*
 * Close target file, if it's open.
 */
void close_target_file(void)
{
    if (dstfd == -1) {
        return;
    }

    if (close(dstfd) != 0) {
        pg_fatal("could not close target file \"%s\": %s\n", dstpath, gs_strerror(errno));
    }

    dstfd = -1;
    CompressFileClose();
}

void write_target_range(char* buf, off_t begin, size_t size, int space, bool compressed)
{
    int writeleft;
    char* p = NULL;
    char* q = NULL;
    int ret;
    bool isempty = false;

    if (dry_run)
        return;

    /* The file start of the compressed table does not need to be an integer multiple of BlockSize. */
    if (!compressed && begin % BLOCKSIZE != 0) {
        (void)close(dstfd);
        dstfd = -1;
        pg_fatal("seek position %ld in target file \"%s\" is not in BLOCKSIZEs\n", size, dstpath);
    }

    if (lseek(dstfd, begin, SEEK_SET) == -1) {
        (void)close(dstfd);
        dstfd = -1;
        pg_fatal("could not seek in target file \"%s\": %s\n", dstpath, strerror(errno));
    }

    /* Empty the space if it it has no data */
    if (size == 0 && space != 0) {
        isempty = true;
        size = space;
        writeleft = size;
        q = (char*)malloc((size_t)space);
        if (q == NULL) {
            pg_fatal("out of memory during function call of write_target_range\n");
        }
        ret = memset_s(q, space, 0, space);
        securec_check_c(ret, "", "");
        p = q;
    } else {
        writeleft = size;
        p = buf;
    }

    while (writeleft > 0) {
        int writelen;

        errno = 0;
        writelen = write(dstfd, p, writeleft);
        if (writelen < 0) {
            /* if write didn't set errno, assume problem is no disk space */
            if (errno == 0)
                errno = ENOSPC;
            (void)close(dstfd);
            dstfd = -1;
            pg_fatal("could not write file \"%s\": %s\n", dstpath, strerror(errno));
            break;
        } else if (writelen % BLOCKSIZE != 0 && g_isRelDataFile) {
            pg_log(PG_WARNING, "write length %d is not in BLOCKSIZEs which may be risky, "
                "file %s, size %ld, write left %d\n", writelen, dstpath, size, writeleft);
        }

        p += writelen;
        writeleft -= writelen;
    }

    if (isempty && q != NULL) {
        free(q);
        q = NULL;
    }

    /* update progress report */
    fetch_done += size;
    progress_report(false);

    /* keep the file open, in case we need to copy more blocks in it */
}

void remove_target(file_entry_t* entry)
{
    Assert(entry->action == FILE_ACTION_REMOVE);
    pg_log(PG_DEBUG, "remove file %s, type% d\n", entry->path, entry->type);
    switch (entry->type) {
        case FILE_TYPE_DIRECTORY:
            remove_target_dir(entry->path);
            break;

        case FILE_TYPE_REGULAR:
            remove_target_file(entry->path, false);
            break;

        case FILE_TYPE_SYMLINK:
            remove_target_symlink(entry->path);
            break;

        default:
            break;
    }
}

void create_target(file_entry_t* entry)
{
    Assert(entry->action == FILE_ACTION_CREATE);

    switch (entry->type) {
        case FILE_TYPE_DIRECTORY:
            create_target_dir(entry->path);
            break;

        case FILE_TYPE_SYMLINK:
            create_target_symlink(entry->path, entry->link_target);
            break;

        case FILE_TYPE_REGULAR:
            /* can't happen. Regular files are created with open_target_file. */
            pg_fatal("invalid action (CREATE) for regular file\n");
            break;

        default:
            break;
    }
}

void remove_target_file(const char* path, bool missingok)
{
    char destpath[MAXPGPATH];
    int ss_c = 0;

    if (dry_run)
        return;

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");
    if (unlink(destpath) != 0) {
        if (errno == ENOENT && missingok) {
            return;
        }
        pg_fatal("could not remove file \"%s\": %s\n", destpath, strerror(errno));
    }
}

/*
 * The path must be the relative path.
 */
void truncate_target_file(const char* path, off_t newsize)
{
    char destpath[MAXPGPATH];
    int fd = -1;
    int ss_c = 0;

    if (dry_run)
        return;

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    fd = open(destpath, O_WRONLY, 0);
    if (fd < 0) {
        pg_fatal("could not open file \"%s\" for truncation: %s\n", destpath, strerror(errno));
        return;
    }

    if (ftruncate(fd, newsize) != 0) {
        pg_fatal("could not truncate file \"%s\" to %u bytes: %s\n", destpath, (unsigned int)newsize, strerror(errno));
        (void)close(fd);
        return;
    }

    (void)close(fd);
}

/*
 * The path must be the relative path.
 */
static void create_target_dir(const char* path)
{
    char destpath[MAXPGPATH];
    int ss_c = 0;

    if (dry_run)
        return;
    if (tablespaceDataIsValid(path) == false) {
        return;
    }

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");
    if (pg_mkdir_p(destpath, S_IRWXU) != 0)
        pg_fatal("could not create directory \"%s\": %s\n", destpath, strerror(errno));
}

/*
 * The path must be the relative path.
 */
static void remove_target_dir(const char* path)
{
    char destpath[MAXPGPATH];
    int ss_c = 0;

    if (dry_run)
        return;

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");
    if (rmdir(destpath) != 0)
        pg_fatal("could not remove directory \"%s\": %s\n", destpath, strerror(errno));
}

/*
 * The path must be the relative path.
 */
static void create_target_symlink(const char* path, const char* slink)
{
    char destpath[MAXPGPATH];
    char targetSlink[MAXPGPATH];
    int ss_c = 0;
    int ret = 0;
    int error_num = 0;

    if (dry_run)
        return;

    /* relative target link case */
    if (NULL != slink && slink[0] != '/') {
        ss_c =
            snprintf_s(targetSlink, sizeof(targetSlink), sizeof(targetSlink) - 1, "%s/pg_location/%s", pg_data, slink);
        securec_check_ss_c(ss_c, "\0", "\0");
    } else {
        ss_c = snprintf_s(targetSlink, sizeof(targetSlink), sizeof(targetSlink) - 1, "%s", slink);
        securec_check_ss_c(ss_c, "\0", "\0");
    }

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    switch (ret = pg_check_dir(targetSlink)) {
        case 0: {
            char* mkpath = strdup(targetSlink);
            if (mkpath == NULL) {
                pg_fatal("out pf memory\n");
                return;
            }
            if (pg_mkdir_p(mkpath, S_IRWXU) == -1) {
                error_num = errno;
                free(mkpath);
                pg_fatal("could not create tablespace directory \"%s\": %s\n", targetSlink, strerror(error_num));
                return;
            }
            free(mkpath);
            break;
        }
        case 1:
        case 2:
            break;
        default:
            error_num = errno;
            pg_fatal("could not access tablespace directory \"%s\": %s\n", targetSlink, strerror(error_num));
            return;
    }

    if (symlink(targetSlink, destpath) != 0) {
        error_num = errno;
        pg_fatal("could not create symbolic link at \"%s\": %s\n", destpath, strerror(error_num));
        return;
    }
}

static void remove_target_symlink(const char* path)
{
    char destpath[MAXPGPATH];
    int ss_c = 0;

    if (dry_run)
        return;

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "\0", "\0");
    if (unlink(destpath) != 0) {
        pg_fatal("could not remove symbolic link \"%s\": %s\n", destpath, strerror(errno));
    }
}

/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
char* slurpFile(const char* datadir, const char* path, size_t* filesize)
{
    int fd = -1;
    char* buffer = NULL;
    struct stat statbuf;
    char fullpath[MAXPGPATH];
    int len;
    int ss_c = 0;
    const int max_file_size = 0xFFFFF;  // max file size = 1M

    ss_c = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1) {
        pg_fatal("could not open file \"%s\" for reading: %s\n", fullpath, strerror(errno));
        return NULL;
    }

    if (fstat(fd, &statbuf) < 0) {
        (void)close(fd);
        pg_fatal("could not open file \"%s\" for reading: %s\n", fullpath, strerror(errno));
        return NULL;
    }

    len = statbuf.st_size;

    if (len < 0 || len > max_file_size) {
        (void)close(fd);
        pg_fatal("could not read file \"%s\": unexpected file size\n", fullpath);
        return NULL;
    }

    buffer = (char*)pg_malloc(len + 1);

    if (read(fd, buffer, len) != len) {
        (void)close(fd);
        pg_free(buffer);
        buffer = NULL;
        pg_fatal("could not read file \"%s\": %s\n", fullpath, strerror(errno));
        return NULL;
    }
    (void)close(fd);

    /* Zero-terminate the buffer. */
    buffer[len] = '\0';

    if (filesize != NULL)
        *filesize = len;
    return buffer;
}

/*
 * judge whether the file exists.
 */
bool is_file_exist(const char* path)
{
    struct stat statbuf;
    bool isExist = true;

    if (lstat(path, &statbuf) < 0) {
        if (errno != ENOENT)
            pg_fatal("could not stat file \"%s\": %s\n", path, gs_strerror(errno));

        isExist = false;
    }
    return isExist;
}

bool is_in_restore_process(const char* pg_data)
{
    char statusfile[MAXPGPATH];
    int ss_c = 0;
    struct stat statbuf;
    bool is_exist = true;

    ss_c = snprintf_s(statusfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pg_data, "pg_rewind.restore");
    securec_check_ss_c(ss_c, "", "");

    if (lstat(statusfile, &statbuf) < 0)
        is_exist = false;

    return is_exist;
}

/*
 * We should judge whether the file is valid.
 * if the file is '.' or '..', we shuold ignore it , then we don't delete the file.
 */
static bool is_special_dir(const char* path)
{
    return strcmp(path, ".") == 0 || strcmp(path, "..") == 0;
}

/*
 * The whole filepath consists of the directory and the filename.
 * Assemble the path and filename with '/'.
 *
 * file_name: the filename we should deal
 * path: the directory that the file belongs to
 * file_path: the whole file assembled
 */
static void get_file_path(const char* path, const char* file_name, char* file_path)
{
    int rc = 0;

    rc = strncpy_s(file_path, MAXPGPATH - strlen(file_path), path, strlen(path));
    securec_check_c(rc, "", "");

    if (file_path[strlen(path) - 1] != '/') {
        rc = strcat_s(file_path, MAXPGPATH, "/");
        securec_check_c(rc, "", "");
    }
    rc = strcat_s(file_path, MAXPGPATH, file_name);
    securec_check_c(rc, "", "");
}

/*
 * if the path is directory, delete the directory and the all files under the directory.
 * if the path is filename, delete the file.
 *
 * path: the file or directory should be deleted
 * remove_top: the file or directory 'path' should be deleted when the remove_top is true
 */
void delete_all_file(const char* path, bool remove_top)
{
    DIR* dir = NULL;
    dirent* dir_info = NULL;
    char file_path[MAXPGPATH];
    struct stat statbuf;

    if (lstat(path, &statbuf) < 0) {
        if (errno != ENOENT)
            pg_fatal("could not stat file \"%s\" in delete_all_file : %s\n", path, gs_strerror(errno));
    }

    if (S_ISDIR(statbuf.st_mode)) {
        if ((dir = opendir(path)) == NULL)
            return;
        while ((dir_info = readdir(dir)) != NULL) {
            get_file_path(path, dir_info->d_name, file_path);
            if (is_special_dir(dir_info->d_name))
                continue;
            delete_all_file(file_path, remove_top);
            if (remove_top) {
                rmdir(file_path);
            }
        }
        closedir(dir);
    } else {
        if (unlink(path) != 0)
            pg_fatal("could not remove file \"%s\": %s\n", path, gs_strerror(errno));
        return;
    }
    if (remove_top) {
        rmdir(path);
    }
}

/*
 * Create the whole path of the file and the directory.
 *
 * We will create every directory by spliting the backupPath with '\' if the directory doesn't exist.
 */
void create_backup_filepath(const char* fullpath, char* backupPath)
{
    char backupFilePath[MAXPGPATH];
    char* tempPath = backupPath;
    char* anchor = NULL;
    int len = 0;
    int rc = 0;
    struct stat statbuf;

    /*search the last subpath of the backupPath*/
    while ((anchor = strchr(tempPath, '/')) != NULL) {
        tempPath = anchor + 1;
    }

    if (lstat(fullpath, &statbuf) < 0) {
        if (errno != ENOENT)
            pg_fatal("could not stat file \"%s\" in create_backup_filepath : %s\n", fullpath, strerror(errno));
    }

    /*
     * If the last subpath is the directory, illustrate the result that backupPath is directory and
     * create all the directories of the backupPath.
     *
     *If the last path is filename, we should create the directories including the filename.
     */
    if (S_ISDIR(statbuf.st_mode)) {
        if (pg_mkdir_p(backupPath, S_IRWXU) != 0)
            pg_fatal("could not create file directory for backup\"%s\": %s\n", backupPath, strerror(errno));
    } else {
        len = tempPath - backupPath;
        rc = strncpy_s(backupFilePath, MAXPGPATH, backupPath, len);
        securec_check_c(rc, "", "");
        backupFilePath[len] = '\0';

        if (pg_mkdir_p(backupFilePath, S_IRWXU) != 0)
            pg_fatal("could not create file directory for backup\"%s\": %s\n", backupFilePath, strerror(errno));
    }
}

/*
 * There are three types of the path, we should deal with the paths with different functions.
 */

void backup_target(file_entry_t* entry, const char* lastoff)
{
    switch (entry->type) {
        case FILE_TYPE_DIRECTORY:
            backup_target_dir(entry->path);
            break;

        case FILE_TYPE_REGULAR:
            backup_target_file(entry->path, lastoff);
            break;

        case FILE_TYPE_SYMLINK:
            backup_target_symlink(entry->path);
            break;

        default:
            break;
    }
}

/*
 * backup directory into pg_rewind_bak if directory exists.
 */
void backup_target_dir(const char* path)
{
    char destpath[MAXPGPATH];
    char prefix_bak[20] = "pg_rewind_bak";
    int ss_c = 0;

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s/%s", pg_data, prefix_bak, path);
    securec_check_ss_c(ss_c, "", "");
    if (pg_mkdir_p(destpath, S_IRWXU) != 0)
        pg_fatal("could not create directory \"%s\": %s\n", destpath, strerror(errno));
}

/*
 * backup file into pg_rewind_bak if file exists.
 */
void backup_target_file(char* path, const char* lastoff)
{
    int ss_c = 0;
    char fullpath[MAXPGPATH];
    char backupPath[MAXPGPATH];
    const char prefix_bak[20] = "pg_rewind_bak";
    struct stat statbuf;
    struct stat statpath;
    char* buffer = NULL;
    int fd = 0;
    int backup_fd = 0;
    char xlogName[MAXFNAMELEN] = {0};

    /* open the source file */
    ssize_t len = 0;

    ss_c = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ss_c, "", "");

    ss_c = snprintf_s(backupPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", pg_data, prefix_bak, path);
    securec_check_ss_c(ss_c, "", "");

    /*we should skip the condition when file is fsm, vm or build_completed.done.*/
    if (NULL != strstr(path, "_fsm") || NULL != strstr(path, "_vm") ||
        NULL != strstr(path, "_cbm") || NULL != strstr(path, "build_completed.start") ||
        NULL != strstr(path, "build_completed.done"))
        return;

    /*we should not open the fullpath if it is the directory*/
    if (lstat(fullpath, &statpath) < 0) {
        pg_log(PG_DEBUG, "could not stat file \"%s\": %s\n", fullpath, strerror(errno));
        return;
    }

    if (S_ISDIR(statpath.st_mode)) {
        return;
    }

    if (NULL != strstr(path, "pg_xlog")) {
        if (sscanf_s(path, "pg_xlog/%s", xlogName, MAXFNAMELEN) != 1) {
            return;
        }
        if (strcmp(xlogName, lastoff) < 0) {
            return;
        }
    }

    if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
        return;

    if (fstat(fd, &statbuf) < 0)
        pg_fatal("could not open file \"%s\" for reading: %s in backup\n", fullpath, strerror(errno));

    len = statbuf.st_size;

    buffer = (char*)pg_malloc(len + 1);
    errno = 0;
    if (read(fd, buffer, len) != len && errno != 0)
        pg_fatal("could not read file \"%s\": %s in backup\n ", fullpath, strerror(errno));
    (void)close(fd);

    /* Zero-terminate the buffer. */
    buffer[len] = '\0';

    /*create the backup path*/
    create_backup_filepath(fullpath, backupPath);

    if ((backup_fd = open(backupPath, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR)) == -1)
        pg_fatal("could not open file \"%s\" for backup: %s\n", backupPath, strerror(errno));

    errno = 0;
    if (write(backup_fd, buffer, len) != len && errno != 0)
        pg_fatal("could not write data to file \"%s\": %s in backup\n", backupPath, strerror(errno));

    if (close(backup_fd) != 0)
        pg_fatal("could not close backup file \"%s\": %s in backup\n", backupPath, strerror(errno));
    free(buffer);
}

/*
 * backup the symlink in target directory
 *
 * if the slink doesn't exist, it will return and do nothing.
 */

void backup_target_symlink(const char* path)
{
    char destpath[MAXPGPATH] = {0};
    char targetSlink[MAXPGPATH] = {0};
    char slink[MAXPGPATH] = {0};
    const char prefix_bak[20] = "pg_rewind_bak";
    int ss_c = 0;
    int ret = 0;
    int rllen = 0;

    rllen = readlink(path, slink, sizeof(slink));
    if (rllen < 0) {
        pg_log(PG_WARNING, "could not read symbolic link \"%s\":for backuping\n", path);
        return;
    } else if (rllen >= (int)sizeof(slink)) {
        pg_log(PG_WARNING, "symbolic link \"%s\" target is too long for backuping\n", slink);
        return;
    }
    slink[rllen] = '\0';

    /* relative target link case */
    if (slink != NULL && slink[0] != '/') {
        ss_c =
            snprintf_s(targetSlink, sizeof(targetSlink), sizeof(targetSlink) - 1, "%s/pg_location/%s", pg_data, slink);
    } else {
        ss_c = snprintf_s(targetSlink, sizeof(targetSlink), sizeof(targetSlink) - 1, "%s", slink);
    }
    securec_check_ss_c(ss_c, "", "");

    ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "%s/%s/%s", pg_data, prefix_bak, path);
    securec_check_ss_c(ss_c, "", "");

    switch (ret = pg_check_dir(targetSlink)) {
        case 0: {
            char* mkpath = strdup(targetSlink);
            if (NULL == mkpath) {
                pg_fatal("out pf memory\n");
            }
            if (pg_mkdir_p(mkpath, S_IRWXU) == -1) {
                free(mkpath);
                pg_fatal("could not create tablespace directory \"%s\": %s\n", targetSlink, strerror(errno));
            }
            free(mkpath);
            mkpath = NULL;
            break;
        }
        case 1:
        case 2:
            break;
        default:
            pg_log(PG_WARNING, "could not access tablespace directory \"%s\": %s\n", targetSlink, strerror(errno));
            return;
    }

    if (symlink(targetSlink, destpath) != 0)
        pg_fatal("could not create symbolic link at \"%s\": %s\n", destpath, strerror(errno));
}

/*
 * Perform restore operation for file of action copy, should create a fake one so as to delete the copied one 
 * during build in target dir.
 */
void backup_fake_target_file(const char* path)
{
    char fullpath[MAXPGPATH] = {0};
    char backupPath[MAXPGPATH] = {0};
    char backupDirPath[MAXPGPATH] = {0};
    const char prefix_bak[20] = "pg_rewind_bak";
    char* tempPath = NULL;
    char* anchor = NULL;
    int ret = 0;
    int fd = -1;
    int len = 0;

    ret = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(ret, "", "");
    ret = snprintf_s(backupPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s.delete", pg_data, prefix_bak, path);
    securec_check_ss_c(ret, "", "");
    tempPath = backupPath;

    /* type for copy action entry can only be file. search the last subpath of the backupPath. */
    while ((anchor = strchr(tempPath, '/')) != NULL) {
        tempPath = anchor + 1;
    }

    len = tempPath - backupPath;
    ret = strncpy_s(backupDirPath, MAXPGPATH, backupPath, len);
    securec_check_c(ret, "", "");
    backupDirPath[len] = '\0';

    if (pg_mkdir_p(backupDirPath, S_IRWXU) != 0)
        pg_fatal("could not create file directory for backup\"%s\": %s\n", backupDirPath, strerror(errno));

    canonicalize_path(backupPath);
    fd = open(backupPath, O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        pg_fatal("could not create fake file \"%s\": %s\n", backupPath, strerror(errno));
    }
    (void)close(fd);
}

/*
 * Backuping all the xlog in standby isn't a good choice because it will cost much performance of incremetal building.
 * Bythe last common checkpoint, we should only backup the xlods which are newer than it.
 */
bool isOldXlog(const char* path, const char* lastoff)
{
    char xlogName[MAXFNAMELEN] = {0};
    bool isOld = false;
    if (sscanf_s(path, "pg_xlog/%s", xlogName, MAXFNAMELEN) != 1) {
        pg_log(PG_WARNING, "unrecognized result \"%s\" for current XLOG name\n", path);
        return false;
    }

    if (strcmp(xlogName, lastoff) < 0)
        isOld = true;
    else
        isOld = false;

    return isOld;
}

/*
 * restore_target_dir: copy a directory
 *
 * If remove_from is false, reserves the backup dir.
 */
bool restore_target_dir(const char* datadir_target, bool remove_from)
{
    char statusfile[MAXPGPATH];
    char fromdir[MAXPGPATH];
    char todir[MAXPGPATH];
    char tmpfilename[MAXPGPATH];
    char errmsg[MAXPGPATH];
    char pg_xlog_path[MAXPGPATH];
    char pg_xlog_bak_path[MAXPGPATH];
    int ss_c = 0;
    int fd = 0;

    ss_c = snprintf_s(fromdir, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, BACKUP_DIR);
    securec_check_ss_c(ss_c, "", "");

    /* pg_rewind_bak must exist and not empty. */
    if (!is_file_exist(fromdir) || directory_is_empty(fromdir)) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "could not stat file \"%s\": %s\n", fromdir, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    ss_c = snprintf_s(statusfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, RESTORE_REWIND_STATUS);
    securec_check_ss_c(ss_c, "", "");

    canonicalize_path(statusfile);
    if ((fd = open(statusfile, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) {
        ss_c = snprintf_s(errmsg,
            MAXPGPATH,
            MAXPGPATH - 1,
            "could not create file \"%s\": %s\n",
            RESTORE_REWIND_STATUS,
            gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    (void)close(fd);

    ss_c = snprintf_s(todir, MAXPGPATH, MAXPGPATH - 1, "%s", datadir_target);
    securec_check_ss_c(ss_c, "", "");

    ss_c = snprintf_s(pg_xlog_path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", todir);
    securec_check_ss_c(ss_c, "", "");

    ss_c = snprintf_s(pg_xlog_bak_path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", fromdir);
    securec_check_ss_c(ss_c, "", "");

    /* clear the pg_xlog/0000*. */
    if (is_file_exist(pg_xlog_bak_path)) {
        delete_all_file(pg_xlog_path, false);
    }

    /* copy data */
    copy_dir(fromdir, todir);

    ss_c = snprintf_s(tmpfilename, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, TAG_START);
    securec_check_ss_c(ss_c, "", "");

    if (is_file_exist(tmpfilename) && unlink(tmpfilename) < 0) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "failed to remove \"%s\": %s\n", TAG_START, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    ss_c = snprintf_s(tmpfilename, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, BUILD_PID);
    securec_check_ss_c(ss_c, "", "");
    if (is_file_exist(tmpfilename) && unlink(tmpfilename) < 0) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "failed to remove \"%s\": %s\n", BUILD_PID, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    restore_gaussdb_state();

    if (unlink(statusfile) < 0) {
        ss_c = snprintf_s(errmsg,
            MAXPGPATH,
            MAXPGPATH - 1,
            "failed to remove \"%s\": %s\n",
            RESTORE_REWIND_STATUS,
            gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    if (remove_from) {
        delete_all_file(fromdir, true);
    }

    pg_log(PG_PRINT, _("restore %s success\n"), BACKUP_DIR);
    return true;

go_exit:
    pg_log(PG_PRINT, _("%s"), errmsg);
    return false;
}

static void copy_dir(const char* fromdir, char* todir)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    char fromfile[MAXPGPATH];
    char tofile[MAXPGPATH];
    char targetfile[MAXPGPATH];
    char errmsg[MAXPGPATH];
    int ss_c = 0;

    /* source dir must exist */
    xldir = opendir(fromdir);
    if (xldir == NULL) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "could not stat file \"%s\": %s\n", fromdir, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    /* copy the file one by one. */
    while (errno = 0, (xlde = readdir(xldir)) != NULL) {
        struct stat fst;

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0) {
            continue;
        }

        if (strcmp(xlde->d_name, BACKUP_DIR) == 0) {
            continue;
        }

        ss_c = snprintf_s(fromfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", fromdir, xlde->d_name);
        securec_check_ss_c(ss_c, "", "");
        ss_c = snprintf_s(tofile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", todir, xlde->d_name);
        securec_check_ss_c(ss_c, "", "");

        if (lstat(fromfile, &fst) < 0) {
            ss_c = snprintf_s(
                errmsg, MAXPGPATH, MAXPGPATH - 1, "could not stat file \"%s\": %s", fromfile, gs_strerror(errno));
            securec_check_ss_c(ss_c, "", "");
            goto go_exit;
        }

        /* just delete corresponding file copied from primary during rewind */
        if (strstr(xlde->d_name, ".delete") != NULL) {
            ss_c = snprintf_s(
                targetfile, MAXPGPATH, MAXPGPATH - 1, "%s/%.*s", todir, strlen(xlde->d_name) - 7, xlde->d_name);
            securec_check_ss_c(ss_c, "", "");
            if (unlink(targetfile) != 0) {
                if (errno == ENOENT) {
                    continue;
                }
                ss_c = snprintf_s(errmsg, MAXPGPATH, MAXPGPATH - 1,
                    "could not remove file \"%s\": %s\n", targetfile, gs_strerror(errno));
                securec_check_ss_c(ss_c, "", "");
                goto go_exit;
            }
            continue;
        }

        if (S_ISDIR(fst.st_mode)) {
            copy_dir(fromfile, tofile);
        } else if (S_ISREG(fst.st_mode)) {
            copy_file(fromfile, tofile);
        }
    }

    if (errno) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "could not read directory \"%s\": %s\n", fromdir, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    if (closedir(xldir)) {
        ss_c = snprintf_s(
            todir, MAXPGPATH, MAXPGPATH - 1, "could not close directory \"%s\": %s\n", fromdir, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    return;

go_exit:
    pg_log(PG_PRINT, _("%s"), errmsg);
    exit(1);
    return; /* suppress compile warning. */
}

#define COPY_BUF_SIZE (8 * BLCKSZ)
void copy_file(const char* fromfile, char* tofile)
{
    char* buffer = NULL;
    int srcfd = 0;
    int dstfdCopy = 0;
    int dstflag = 0;
    int nbytes = 0;
    off_t offset = 0;
    char errmsg[MAXPGPATH];
    int ss_c = 0;

    /* Use palloc to ensure we get a maxaligned buffer */

    /* add extern BLCKSZ for protect memory overstep the boundary */
    buffer = (char*)malloc(COPY_BUF_SIZE + BLCKSZ);
    if (buffer == NULL) {
        pg_log(PG_ERROR, "malloc for buffer failed!");
        goto go_exit;
    }

    /* Open the files */
    srcfd = open(fromfile, O_RDONLY | PG_BINARY, 0);
    if (srcfd < 0) {
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "could not open file \"%s\": %s\n", fromfile, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    canonicalize_path(tofile);
    if (is_file_exist(tofile)) {
        unlink(tofile);
    }

    dstflag = (O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
    dstfdCopy = open(tofile, dstflag, S_IRUSR | S_IWUSR);
    if (dstfdCopy < 0) {
        (void)close(srcfd);
        ss_c = snprintf_s(
            errmsg, MAXPGPATH, MAXPGPATH - 1, "could not create file \"%s\": %s\n", tofile, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    /* Do the data copying. */
    for (offset = 0;; offset += nbytes) {
        nbytes = read(srcfd, buffer, COPY_BUF_SIZE);
        if (nbytes < 0) {
            (void)close(srcfd);
            (void)close(dstfdCopy);
            ss_c = snprintf_s(
                errmsg, MAXPGPATH, MAXPGPATH - 1, "could not read to file \"%s\": %s\n", fromfile, gs_strerror(errno));
            securec_check_ss_c(ss_c, "", "");
            goto go_exit;
        }
        if (nbytes == 0) {
            break;
        }

        errno = 0;
        if ((int)write(dstfdCopy, buffer, nbytes) != nbytes) {
            (void)close(srcfd);
            (void)close(dstfdCopy);
            ss_c = snprintf_s(
                errmsg, MAXPGPATH, MAXPGPATH - 1, "could not write to file \"%s\": %s", tofile, gs_strerror(errno));
            securec_check_ss_c(ss_c, "", "");
            goto go_exit;
        }
    }

    if (close(dstfdCopy)) {
        (void)close(srcfd);

        ss_c =
            snprintf_s(errmsg, MAXPGPATH, MAXPGPATH - 1, "could not close file \"%s\": %s", tofile, gs_strerror(errno));
        securec_check_ss_c(ss_c, "", "");
        goto go_exit;
    }

    (void)close(srcfd);
    if (buffer != NULL) {
        free(buffer);
        buffer = NULL;
    }
    return;
go_exit:
    if (buffer != NULL) {
        free(buffer);
        buffer = NULL;
    }
    pg_log(PG_PRINT, _("%s"), errmsg);
    exit(1);
    return; /* suppress compile warning. */
}

static void restore_gaussdb_state(void)
{
    GaussState state;
    errno_t tnRet = 0;

    tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check_c(tnRet, "\0", "\0");

    state.mode = STANDBY_MODE;
    state.conn_num = 2;
    state.state = NORMAL_STATE;
    state.sync_stat = false;
    state.build_info.build_mode = NONE_BUILD;
    UpdateDBStateFile(gaussdb_state_file, &state);

    return;
}

static bool directory_is_empty(const char* path)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    bool ret = true;

    xldir = opendir(path);
    if (xldir == NULL) {
        ret = false;
        goto go_exit;
    }

    while ((xlde = readdir(xldir)) != NULL) {
        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0) {
            continue;
        }
        ret = false;
        break;
    }

go_exit:
    if (xldir != NULL) {
        closedir(xldir);
        xldir = NULL;
    }
    return ret;
}

void delete_target_file(const char* file)
{
    if (is_file_exist(file)) {
        delete_all_file(file, false);
    }
}

bool tablespaceDataIsValid(const char* path)
{
    char destpath[MAXPGPATH];
    char buf[MAXPGPATH];
    int ss_c = 0;
    RelFileNode rnode = {0, 0, 0, 0};
    unsigned int segNo = 0;
    int columnid = 0;
    int nmatch = 0;

    if (dry_run) {
        return true;
    }

    if (strstr(path, "pg_tblspc/") == NULL) {
        return true;
    }
    nmatch = sscanf_s(path,
        "pg_tblspc/%u/%[^/]/%u/%u_C%d.%u",
        &rnode.spcNode,
        buf,
        sizeof(buf),
        &rnode.dbNode,
        &rnode.relNode,
        &columnid,
        &segNo);
    /*don't create any data if tbs is not valid*/
    if (nmatch > 1) {
        ss_c = snprintf_s(destpath, sizeof(destpath), sizeof(destpath) - 1, "pg_tblspc/%u", rnode.spcNode);
        securec_check_ss_c(ss_c, "\0", "\0");
        if (isPathInFilemap(destpath) == true) {
            return true;
        } else {
            return false;
        }
    }

    return true;
}

void CompressedFileTruncate(const char *path, const RewindCompressInfo *rewindCompressInfo)
{
    if (dry_run) {
        return;
    }
    /* sanity check */
    Assert(rewindCompressInfo->oldBlockNumber > rewindCompressInfo->newBlockNumber);

    /* construct full path */
    char fullPath[MAXPGPATH];
    errno_t rc = snprintf_s(fullPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pg_data, path);
    securec_check_ss_c(rc, "\0", "\0");
    /* call truncate of pageCompression */
    std::unique_ptr<PageCompression> pageCompression = std::make_unique<PageCompression>();
    /* segno is no used here */
    COMPRESS_ERROR_STATE result = pageCompression->Init(fullPath, InvalidBlockNumber, false);
    (void)FileProcessErrorReport(fullPath, result);
    result = pageCompression->TruncateFile(rewindCompressInfo->newBlockNumber);
    (void)FileProcessErrorReport(fullPath, result);
    pg_log(PG_DEBUG, "CompressedFileTruncate: %s\n", path);
}

void FetchCompressedFile(char* buf, BlockNumber blockNumber, int32 size, uint16 chunkSize, uint8 algorithm)
{
    CfsCompressOption cfsCompressOption{.chunk_size = chunkSize, .algorithm = algorithm};
    if (!g_pageCompression->WriteBufferToCurrentBlock(buf, blockNumber, size, &cfsCompressOption)) {
        pg_log(PG_ERROR, "FetchCompressedFile failed: \n");
    }
}

void CompressedFileInit(const char* fileName, bool rebuild)
{
    if (dry_run) {
        return;
    }

    if (g_pageCompression != NULL && strcmp(fileName, &g_pageCompression->GetInitPath()[strlen(pg_data) + 1]) == 0) {
        /* already open */
        return;
    }
    CompressFileClose();
    /* format full poth */
    char dstPath[MAXPGPATH];
    error_t rc = snprintf_s(dstPath, sizeof(dstPath), sizeof(dstPath) - 1, "%s/%s", pg_data, fileName);
    securec_check_ss_c(rc, "\0", "\0");
    g_pageCompression = new(std::nothrow) PageCompression();
    if (g_pageCompression == NULL) {
        return;
    }
    /* segment number only used for checksum */
    auto state = g_pageCompression->Init(dstPath, InvalidBlockNumber, rebuild);
    (void)FileProcessErrorReport(dstPath, state);
}

void CompressFileClose()
{
    if (g_pageCompression != NULL) {
        delete g_pageCompression;
        g_pageCompression = NULL;
    }
}

bool FileProcessErrorReport(const char *path, COMPRESS_ERROR_STATE errorState)
{
    auto errorStr = strerror(errno);
    switch (errorState) {
        case SUCCESS:
            return true;
        default:
            pg_fatal("process compressed file \"%s\": %s\n", path, errorStr);
            break;
    }
    return false;
}
