/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
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
 * fetchmot.cpp
 *    Receives and writes the current MOT checkpoint.
 *
 * IDENTIFICATION
 *    src/bin/pg_ctl/fetchmot.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include "postgres_fe.h"
#include "gs_tar_const.h"
#include "streamutil.h"
#include "libpq/libpq-fe.h"
#include "fetchmot.h"
#include "utils/builtins.h"
#include "common/fe_memutils.h"

#define disconnect_and_exit(code) \
    do {                          \
        PQfinish(conn);           \
        exit(code);               \
    } while (0)

static uint64 totaldone = 0;

static void CheckConnResult(PGconn* conn, const char* progname)
{
    PGresult* res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        fprintf(stderr, "%s: could not get COPY data stream: %s", progname, PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    PQclear(res);
}

static char* GenerateChpktHeader(const char* chkptName) 
{
    char* copybuf = (char*)xmalloc0(TAR_BLOCK_SIZE);
    int chkptDirLen = strlen(chkptName);
    errno_t errorno = strcpy_s(copybuf + 2, chkptDirLen + 1, chkptName);
    securec_check_c(errorno, "", "");

    copybuf[0] = '.';
    copybuf[1] = '/';
    copybuf[chkptDirLen + 2] = '/';
    copybuf[TAR_FILE_TYPE] = TAR_TYPE_DICTORY;

    for (int i = 0; i < 11; i++) {
        copybuf[TAR_LEN_LEFT + i] = '0';
    }
    errorno = sprintf_s(&copybuf[TAR_FILE_MODE], TAR_BLOCK_SIZE - TAR_FILE_MODE, "%07o ", FILE_PERMISSION);
    securec_check_ss_c(errorno, "", "");
    return copybuf;
}

static void MotReceiveAndAppendTarFile(const char* basedir, const char* chkptName, PGconn* conn, const char* progname,
                                       int compresslevel)
{
    CheckConnResult(conn, progname);

    FILE* tarfile = NULL;
    char filename[MAXPGPATH];
    char current_path[MAXPGPATH];
    int current_len_left = 0;
    int current_padding = 0;

    errno_t errorno = strncpy_s(current_path, sizeof(current_path), basedir, sizeof(current_path) - 1);
    securec_check_c(errorno, "", "");

#ifdef HAVE_LIBZ
    gzFile ztarfile = NULL;
    int duplicatedfd = -1;
#endif
    if (strcmp(basedir, "-") == 0) {
#ifdef HAVE_LIBZ
        if (compresslevel != 0) {
            duplicatedfd = dup(fileno(stdout));
            if (duplicatedfd == -1) {
                fprintf(stderr, _("%s: could not allocate dup fd by fileno(stdout): %s\n"), progname, strerror(errno));
                disconnect_and_exit(1);
            }

            ztarfile = gzdopen(duplicatedfd, "ab");
            if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK) {
                fprintf(stderr, _("%s: could not set compression level %d: %s\n"), progname, compresslevel,
                    get_gz_error(ztarfile));
                close(duplicatedfd);
                duplicatedfd = -1;
                disconnect_and_exit(1);
            }
            close(duplicatedfd);
            duplicatedfd = -1;
        } else
#endif
        tarfile = stdout;
        errorno = strcpy_s(filename, MAXPGPATH, "-");
        securec_check_c(errorno, "", "");
    } else {
        const char* formatName = (compresslevel != 0) ? "%s/base.tar.gz" : "%s/base.tar";
        errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, formatName, basedir);
        securec_check_ss_c(errorno, "", "");
#ifdef HAVE_LIBZ
        if (compresslevel != 0) {
            ztarfile = openGzFile(filename, compresslevel, "ab");
        } else
#endif
        {
            tarfile = fopen(filename, "ab");
        }
    }

    /* chkptName header */
    char* copybuf = GenerateChpktHeader(chkptName);
    int writeResult;
#ifdef HAVE_LIBZ
    if (ztarfile != NULL) {
        writeResult = !writeGzFile(ztarfile, copybuf, TAR_BLOCK_SIZE);
    } else
#endif
    {
        writeResult = fwrite(copybuf, TAR_BLOCK_SIZE, 1, tarfile) != 1;
    }

    if (writeResult) {
        fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
        disconnect_and_exit(1);
    }

    while (1) {
        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        int r = PQgetCopyData(conn, &copybuf, 0);
        if (r == -1) {
#ifdef HAVE_LIBZ
            if (ztarfile != NULL) {
                if (gzclose(ztarfile) != 0) {
                    fprintf(stderr, _("%s: could not close compressed file \"%s\": %s\n"), progname, filename,
                        get_gz_error(ztarfile));
                    disconnect_and_exit(1);
                }
            } else
#endif
            {
                if (strcmp(basedir, "-") != 0) {
                    if (fclose(tarfile) != 0) {
                        fprintf(stderr, _("%s: could not close file \"%s\": %s\n"), progname, filename,
                                strerror(errno));
                        disconnect_and_exit(1);
                    }
                    tarfile = NULL;
                }
            }
            break;
        } else if (r == TAR_READ_ERROR) {
            fprintf(stderr, "%s: could not read COPY data: %s", progname, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }
        if (current_len_left == 0 && current_padding == 0) {
            /* No current file, so this must be the header for a new file */
            if (r != TAR_BLOCK_SIZE) {
                fprintf(stderr, _("%s: invalid tar block header size: %d\n"), progname, r);
                disconnect_and_exit(1);
            }

            /* new file */
            int filemode;
            totaldone += TAR_BLOCK_SIZE;
            if (sscanf_s(copybuf + TAR_LEN_LEFT, "%201o", &current_len_left) != 1) {
                fprintf(stderr, "%s: could not parse file size\n", progname);
                disconnect_and_exit(1);
            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[TAR_FILE_MODE], "%07o ", (unsigned int*)&filemode) != 1) {
                fprintf(stderr, "%s: could not parse file mode\n", progname);
                disconnect_and_exit(1);
            }

            /*
             * All files are padded up to 512 bytes
             */
            current_padding = ((current_len_left + TAR_FILE_PADDING) & ~TAR_FILE_PADDING) - current_len_left;
            /*
             * First part of header is zero terminated filename.
             * when getting a checkpoint, file name can be either
             * the control file (written to base_dir), or a checkpoint
             * file (written to base_dir/chkpt_)
             */
            if (strstr(copybuf, "mot.ctrl")) {
                errorno =
                        snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, "mot.ctrl");
                securec_check_ss_c(errorno, "", "");
            } else {
                char* chkptOffset = strstr(copybuf, chkptName);
                char* cpBuffer = chkptOffset ? chkptOffset : copybuf;
                errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, cpBuffer);
                securec_check_ss_c(errorno, "", "");
            }

            if (filename[strlen(filename) - 1] == '/') {
                filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                errorno = strcpy_s(copybuf, strlen(filename), filename);
                securec_check_c(errorno, "", "");
                continue; /* directory or link handled */
            }
#ifdef HAVE_LIBZ
            if (ztarfile != NULL) {
                if (!writeGzFile(ztarfile, copybuf, r)) {
                    fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                    disconnect_and_exit(1);
                }
            } else
#endif
            {
                if (fwrite(copybuf, r, 1, tarfile) != 1) {
                    fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                    disconnect_and_exit(1);
                }
            }
            if (current_len_left == 0) {
                continue;
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (current_len_left == 0 && r == current_padding) {
                int writeResult;
#ifdef HAVE_LIBZ
                if (ztarfile != NULL) {
                    writeResult = !writeGzFile(ztarfile, copybuf, current_padding);
                } else
#endif
                {
                    writeResult = fwrite(copybuf, current_padding, 1, tarfile) != 1;
                }
                if (writeResult) {
                    fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename,
                        strerror(errno));
                    disconnect_and_exit(1);
                }
                totaldone += r;
                current_padding -= r;
                continue;
            }

            totaldone += r;
#ifdef HAVE_LIBZ
            if (ztarfile != NULL) {
                if (!writeGzFile(ztarfile, copybuf, r)) {
                    fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                    disconnect_and_exit(1);
                }
            } else
#endif
            {
                if (fwrite(copybuf, r, 1, tarfile) != 1) {
                    fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                    disconnect_and_exit(1);
                }
            }
            if (current_len_left == 0) {
                continue;
            }
            current_len_left -= r;
            if (current_len_left == 0 && current_padding == 0) {
                continue;
            }
        } /* continuing data in existing file */
    }     /* loop over all data blocks */
    if (tarfile != NULL) {
        fclose(tarfile);
        tarfile = NULL;
    }

    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
}

/*
 * Receive a tar format stream from the connection to the server, and unpack
 * the contents of it into a directory. Only files, directories and
 * symlinks are supported, no other kinds of special files.
 */
static void MotReceiveAndUnpackTarFile(const char* basedir, const char* chkptName, PGconn* conn, const char* progname)
{
    PGresult* res = NULL;
    char current_path[MAXPGPATH];
    char filename[MAXPGPATH];
    int current_len_left;
    int current_padding = 0;
    char* copybuf = NULL;
    FILE* file = NULL;

    errno_t errorno = strncpy_s(current_path, sizeof(current_path), basedir, sizeof(current_path) - 1);
    securec_check_c(errorno, "", "");

    /*
     * Get the COPY data
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        fprintf(stderr, "%s: could not get COPY data stream: %s", progname, PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    PQclear(res);

    while (1) {
        int r;

        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        r = PQgetCopyData(conn, &copybuf, 0);
        if (r == -1) {
            /*
             * End of chunk
             */
            if (file != NULL) {
                fclose(file);
                file = NULL;
            }

            break;
        } else if (r == -2) {
            fprintf(stderr, "%s: could not read COPY data: %s", progname, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

        if (file == NULL) {
            /* new file */
            int filemode;

            /*
             * No current file, so this must be the header for a new file
             */
            if (r != 2560) {
                fprintf(stderr, "%s: invalid tar block header size: %d\n", progname, r);
                disconnect_and_exit(1);
            }
            totaldone += 2560;

            if (sscanf_s(copybuf + 1048, "%201o", &current_len_left) != 1) {
                fprintf(stderr, "%s: could not parse file size\n", progname);
                disconnect_and_exit(1);
            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[1024], "%07o ", (unsigned int*)&filemode) != 1) {
                fprintf(stderr, "%s: could not parse file mode\n", progname);
                disconnect_and_exit(1);
            }

            /*
             * All files are padded up to 512 bytes
             */
            current_padding = ((current_len_left + 511) & ~511) - current_len_left;

            /*
             * First part of header is zero terminated filename.
             * when getting a checkpoint, file name can be either
             * the control file (written to base_dir), or a checkpoint
             * file (written to base_dir/chkpt_)
             */
            if (strstr(copybuf, "mot.ctrl")) {
                errorno =
                    snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, "mot.ctrl");
                securec_check_ss_c(errorno, "", "");
            } else {
                char* chkptOffset = strstr(copybuf, chkptName);
                if (chkptOffset) {
                    errorno = snprintf_s(
                        filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, chkptOffset);
                    securec_check_ss_c(errorno, "", "");
                } else {
                    errorno =
                        snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, copybuf);
                    securec_check_ss_c(errorno, "", "");
                }
            }

            if (filename[strlen(filename) - 1] == '/') {
                /*
                 * Ends in a slash means directory or symlink to directory
                 */
                if (copybuf[1080] == '5') {
                    /*
                     * Directory
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (mkdir(filename, S_IRWXU) != 0) {
                        fprintf(
                            stderr, "%s: could not create directory \"%s\": %s\n", progname, filename, strerror(errno));
                        disconnect_and_exit(1);
                    }
#ifndef WIN32
                    if (chmod(filename, (mode_t)filemode))
                        fprintf(stderr,
                            "%s: could not set permissions on directory \"%s\": %s\n",
                            progname,
                            filename,
                            strerror(errno));
#endif
                } else if (copybuf[1080] == '2') {
                    /*
                     * Symbolic link
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (symlink(&copybuf[1081], filename) != 0) {
                        fprintf(stderr,
                            "%s: could not create symbolic link from \"%s\" to \"%s\": %s\n",
                            progname,
                            filename,
                            &copybuf[1081],
                            strerror(errno));
                        disconnect_and_exit(1);
                    }
                } else {
                    fprintf(stderr, "%s: unrecognized link indicator \"%c\"\n", progname, copybuf[1080]);
                    disconnect_and_exit(1);
                }
                continue; /* directory or link handled */
            }

            canonicalize_path(filename);
            /*
             * regular file
             */
            file = fopen(filename, "wb");
            if (file == NULL) {
                fprintf(stderr, "%s: could not create file \"%s\": %s\n", progname, filename, strerror(errno));
                disconnect_and_exit(1);
            }

#ifndef WIN32
            if (chmod(filename, (mode_t)filemode))
                fprintf(
                    stderr, "%s: could not set permissions on file \"%s\": %s\n", progname, filename, strerror(errno));
#endif

            if (current_len_left == 0) {
                /*
                 * Done with this file, next one will be a new tar header
                 */
                fclose(file);
                file = NULL;
                continue;
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (current_len_left == 0 && r == current_padding) {
                /*
                 * Received the padding block for this file, ignore it and
                 * close the file, then move on to the next tar header.
                 */
                fclose(file);
                file = NULL;
                totaldone += r;
                continue;
            }

            if (fwrite(copybuf, r, 1, file) != 1) {
                fprintf(stderr, "%s: could not write to file \"%s\": %s\n", progname, filename, strerror(errno));
                fclose(file);
                file = NULL;
                disconnect_and_exit(1);
            }
            totaldone += r;

            current_len_left -= r;
            if (current_len_left == 0 && current_padding == 0) {
                /*
                 * Received the last block, and there is no padding to be
                 * expected. Close the file and move on to the next tar
                 * header.
                 */
                fclose(file);
                file = NULL;
                continue;
            }
        } /* continuing data in existing file */
    }     /* loop over all data blocks */

    if (file != NULL) {
        fprintf(stderr, "%s: COPY stream ended before last file was finished\n", progname);
        fclose(file);
        file = NULL;
        disconnect_and_exit(1);
    }

    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
}

/**
 * @brief Receives and writes the current MOT checkpoint.
 * @param basedir The directory in which to save the files in.
 * @param conn The connection to use in order to fetch.
 * @param progname The caller program name (for logging).
 * @param verbose Controls verbose output.
 * @return Boolean value denoting success or failure.
 */
void FetchMotCheckpoint(const char* basedir, PGconn* conn, const char* progname, bool verbose, const char format,
                        int compresslevel)
{
    PGresult* res = NULL;
    const char* fetchQuery = "FETCH_MOT_CHECKPOINT";
    const char* chkptPrefix = "chkpt_";
    char dirName[MAXPGPATH] = {0};
    char chkptName[MAXPGPATH] = {0};
    errno_t errorno = EOK;
    struct stat fileStat;

    if (conn == NULL) {
        fprintf(stderr, "%s: FetchMotCheckpoint: bad connection\n", progname);
        exit(1);
    }

    if (PQsendQuery(conn, fetchQuery) == 0) {
        fprintf(stderr,
            "%s: could not send fetch mot checkpoint cmd \"%s\": %s",
            progname,
            fetchQuery,
            PQerrorMessage(conn));
        disconnect_and_exit(1);
    }

    res = PQgetResult(conn);
    ExecStatusType curStatus = PQresultStatus(res);
    if (curStatus == PGRES_COMMAND_OK) {
        if (verbose) {
            fprintf(stderr, "%s: no mot checkpoint exists\n", progname);
        }
        PQclear(res);
        return;
    }

    if (curStatus != PGRES_TUPLES_OK) {
        fprintf(stderr,
            "%s: could not fetch mot checkpoint info: %s, status: %u",
            progname,
            PQerrorMessage(conn),
            curStatus);
        PQclear(res);
        disconnect_and_exit(1);
    }

    if (PQntuples(res) == 1) {
        char* chkptLine = PQgetvalue(res, 0, 0);
        if (NULL == chkptLine) {
            fprintf(stderr, "%s: cmd failed\n", progname);
            PQclear(res);
            disconnect_and_exit(1);
        }

        char* prefixStart = strstr(chkptLine, chkptPrefix);
        if (prefixStart == NULL) {
            fprintf(stderr, "%s: unable to parse checkpoint location\n", progname);
            PQclear(res);
            disconnect_and_exit(1);
        }

        errorno = strncpy_s(chkptName, sizeof(chkptName), prefixStart, sizeof(chkptName) - 1);
        securec_check_c(errorno, "", "");
        errorno = snprintf_s(dirName, sizeof(dirName), sizeof(dirName) - 1, "%s/%s", basedir, chkptName);
        securec_check_ss_c(errorno, "", "");

        if (verbose) {
            fprintf(stderr, "%s: mot checkpoint directory: %s\n", progname, dirName);
        }

        if (format == 'p') {
            if (stat(dirName, &fileStat) < 0) {
                if (pg_mkdir_p(dirName, S_IRWXU) == -1) {
                    fprintf(stderr, "%s: could not create directory \"%s\": %s\n", progname, dirName, strerror(errno));
                    PQclear(res);
                    disconnect_and_exit(1);
                }
            } else {
                fprintf(stderr, "%s: directory \"%s\" already exists, please remove it and try again\n", progname,
                        dirName);
                PQclear(res);
                disconnect_and_exit(1);
            }
            MotReceiveAndUnpackTarFile(basedir, chkptName, conn, progname);
        } else if (format == 't') {
            MotReceiveAndAppendTarFile(basedir, chkptName, conn, progname, compresslevel);
        } else {
            fprintf(stderr, "%s: unsupport format type: \"%c\".\n", progname, format);
            PQclear(res);
            disconnect_and_exit(1);
        }
        if (verbose) {
            fprintf(stderr, "%s: finished fetching mot checkpoint\n", progname);
        }
    } else {
        fprintf(stderr, "%s: failed to obtain mot checkpoint header\n", progname);
        PQclear(res);
        disconnect_and_exit(1);
    }

    PQclear(res);
}

static void TrimValue(char* value)
{
    if (value == NULL) {
        return;
    }

    const char* tmp = value;
    do {
        while (*tmp == ' ' || *tmp == '\'' || *tmp == '\"' || *tmp == '=' || *tmp == '\n' || *tmp == '\t') {
            ++tmp;
        }
    } while ((*value++ = *tmp++));
}

/**
 * @brief Parses a certain value for an option from a file.
 * @param fileName The file to parse from.
 * @param option Option to parse.
 * @return The option's value as a malloc'd buffer or NULL if
 * it was not found. Caller's responsibility to free this returned buffer.
 */
static char* GetOptionValueFromFile(const char* fileName, const char* option)
{
    FILE* file = NULL;
    char* line = NULL;
    char* ret = NULL;
    size_t len = 0;
    ssize_t read = 0;

    if (fileName == NULL || option == NULL) {
        return ret;
    }

    if ((file = fopen(fileName, "r")) == NULL) {
        fprintf(stderr, "could not open file \"%s\": %s\n", fileName, strerror(errno));
        return ret;
    }

    while ((read = getline(&line, &len, file)) != -1) {
        if (strncmp(line, option, strlen(option)) == 0) {
            char* offset = line + strlen(option);
            TrimValue(offset);
            ret = strdup(offset);
        }
    }

    if (line != NULL) {
        free(line);
    }
    fclose(file);
    return ret;
}

/**
 * @brief Gets the checkpoint_dir config value from the mot.conf file.
 * @param dataDir pg_data directory.
 * @return checkpoint_dir config value as a malloc'd buffer or NULL if
 * it was not found. Caller's responsibility to free this returned buffer.
 */
char* GetMotCheckpointDir(const char* dataDir)
{
    char confPath[1024] = {0};
    char* motChkptDir = NULL;

    /* see if we have an mot conf file configured */
    int nRet = sprintf_s(confPath, sizeof(confPath), "%s/%s", dataDir, "postgresql.conf");
    securec_check_ss_c(nRet, "\0", "\0");

    char* motConfPath = GetOptionValueFromFile(confPath, "mot_config_file");
    if (motConfPath != NULL) {
        motChkptDir = GetOptionValueFromFile(motConfPath, "checkpoint_dir");
        free(motConfPath);
        motConfPath = NULL;
    } else {
        nRet = sprintf_s(confPath, sizeof(confPath), "%s/%s", dataDir, "mot.conf");
        securec_check_ss_c(nRet, "\0", "\0");
        motChkptDir = GetOptionValueFromFile(confPath, "checkpoint_dir");
    }

    return motChkptDir;
}
