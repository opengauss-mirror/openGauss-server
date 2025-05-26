/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 *---------------------------------------------------------------------------------------
 *
 *  parser.cpp
 *
 * IDENTIFICATION
 *        src/bin/gds/parser.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#ifdef WIN32
#include "securec.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include "securec.h"
#include <string>
#include "storage/gds_utils.h"

#ifdef GDS_SERVER
#include "storage/parser.h"
#include "gds_mt.h"
#include "package.h"
#include "utils/memutils.h"
#endif

#ifdef OBS_SERVER
#include "storage/parser.h"
#include "c.h"
#include "pgstat.h"
#include "access/obs/obs_am.h"
#include "commands/obs_stream.h"
#include "utils/plog.h"
#endif

#ifdef WIN32
#include <io.h>

#define likely(x) (x)
#define unlikely(x) (x)
#define Min(x, y) ((x) > (y) ? (y) : (x))
#define F_OK (0)

#define access(file, flag) _access(file, flag)
#define unlink(file) _unlink(file)
#endif

#define INITIAL_BUF_LEN (4 * 1024 * 1024)
#define MAX_BLK_SIZE (1024 * 256)
#define MAX_SEGMENT_NUM 2147483600
#define SEGMENT_SIZE 2147483648
#define FILEHEADER_BUF_SIZE (1024 * 1024)
#define CHUNK_SZ 128
#define InvalidSymbol "../"
const int GDS_HEADER_LEN = 4;

#ifndef WIN32
#define LOG_PERM_GRPR (S_IRUSR | S_IWUSR | S_IRGRP)
#endif

using namespace std;
using namespace GDS;

#ifdef GDS_SERVER

extern gds_settings settings;
extern THR_LOCAL GDS_Connection* current_connection;

/* GDS server required declearations */
#define parser_log gs_ereport
#define parser_securec_check gds_securec_check
#define parser_securec_check_ss gds_securec_check_ss

/* extern from gds_main.cpp */
extern string UriToLocalPath(const char* strUri);
extern size_t GetDataSegmentSize();
extern char* gs_strerror(int errnum);
static void GetFileHeader(WritableParser* self, const char* path);
#endif

#ifdef OBS_SERVER
/* Backend required declearations */
#define parser_log elog
#define parser_securec_check(rc) securec_check(rc, "\0", "\0")
#define parser_securec_check_ss(rc) securec_check_ss(rc, "\0", "\0")

#ifndef ENABLE_LITE_MODE
static size_t SourceRead_OBS(Source* self, void* buffer, size_t len);
static bool SourceNext_OBS(Source* self);
#endif
#endif

static Source* CreateSource(const FileList* files, SourceType sourcetype);
static void DestroyParser(Parser* self);
static void DestroyReadableParser(ReadableParser* self);
static void DestroyWritableParser(WritableParser* self);
static void CleanupWritablParser(WritableParser* self);
static void NopCleanup(Parser* self);

static char* FindEolChar(char* s, size_t len, const char* eol, int* eol_cur, int* eol_cur_saved)
{
    char* end = NULL;
    char* cr = NULL;
    char* lf = NULL;
    end = s + len;
    if (eol == NULL) {
        while (s < end) {
            size_t chunk = (s + CHUNK_SZ < end) ? CHUNK_SZ : (end - s);
            cr = (char*)memchr(s, '\r', chunk);
            lf = (char*)memchr(s, '\n', chunk);
            if (cr != NULL) {
                if (lf != NULL && lf < cr)
                    return lf;
                return cr;
            } else if (lf != NULL)
                return lf;
            s += CHUNK_SZ;
        }
    } else {
        int eol_len = strlen(eol);
        *eol_cur_saved = *eol_cur;
        for (int i = 0; i < (int)len; i++) {
            if (s[i] == eol[*eol_cur]) {
                (*eol_cur)++;
                if (*eol_cur >= eol_len) {
                    return s + i + 1 + *eol_cur_saved - eol_len;
                }
                continue;
            } else if (*eol_cur >= 1) {
                // Ensure that every character in "s" is compared to eol.
                // Here we allow i back into -1, and on the beginning of the next loop it will be back to 0, as
                // expected.
                i--;
            }
            *eol_cur = 0;
            *eol_cur_saved = 0;
        }
    }
    return NULL;
}

static Source* CreateSource(const FileList* files, SourceType sourcetype)
{
    Source* self = NULL;

    try {
        self = new Source;
    } catch (std::bad_alloc&) {
        parser_log(LEVEL_ERROR, "failed to create source, out of memory");
    }

    if (files != NULL)
        self->SetFileList(*files);
    self->SetSourceType(sourcetype);
    self->SourceInit(files == NULL);

    return self;
}

/* specified for general file source reading */
static size_t SourceRead_File(Source* self, void* buffer, size_t len)
{
    size_t nread;
    const char* err_file = self->m_files[self->m_current - 1].c_str();

#if defined(USE_POSIX_FADVISE)
    off_t offset = lseek(fileno(self->m_fd), 0, SEEK_CUR);
    if (offset >= 0)
        (void)posix_fadvise(fileno(self->m_fd), offset + len, INITIAL_BUF_LEN, POSIX_FADV_WILLNEED);
#endif
    nread = fread(buffer, 1, len, self->m_fd);
    if (ferror(self->m_fd))
        parser_log(LEVEL_ERROR, "could not read source file: %s", err_file);

    return nread;
}

/* specified for pipe source reading */
static size_t SourceRead_FIFO(Source* self, void* buffer, size_t len)
{
    const char* err_file = self->m_files[0].c_str();
#ifndef WIN32
    ssize_t nread = read(self->m_fifo, buffer, len);
#else
    int nread = read(self->m_fifo, buffer, len);
#endif
    if (-1 == nread)
        parser_log(LEVEL_ERROR, "could not read source file: %s", err_file);
    return (size_t)nread;
}

/* specified for general file source checking next file */
static bool SourceNext_File(Source* self)
{
    const char* err_file = NULL;
    const char* current_file = NULL;

    if (self->m_fd != NULL) {
        fclose(self->m_fd);
        self->m_fd = NULL;
    }

    if ((size_t)(self->m_current) >= self->m_files.size())
        return false;
    while (self->m_fd == NULL && (size_t)(self->m_current) < self->m_files.size()) {
        current_file = self->m_files[self->m_current++].c_str();
        self->m_fd = fopen(current_file, "r");
        if (self->m_fd == NULL) {
            err_file = self->m_files[self->m_current - 1].c_str();
            parser_log(LEVEL_WARNING, "Unable to open %s", err_file);
        }
    }

    if (self->m_fd != NULL) {
        self->m_filename = self->m_files[self->m_current - 1];
#if defined(USE_POSIX_FADVISE)
        (void)posix_fadvise(
            fileno(self->m_fd), 0, INITIAL_BUF_LEN, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE | POSIX_FADV_WILLNEED);
#endif
    }

    return self->m_fd != NULL;
}

/* specified for pipe source checking next file */
static bool SourceNext_FIFO(Source* self)
{
    return false;
}

#ifdef GDS_SERVER
void Source::SourceWrite(evbuffer* buffer, size_t len)
{
    int nsave;
    int result;

    while (len > 0) {
        if (m_wused == MAX_BUFFER_SIZE)
            SourceFlush();
        nsave = Min(len, MAX_BUFFER_SIZE - m_wused);
        result = evbuffer_remove(buffer, m_writeBuf + m_wused, nsave);
        if (result == -1)
            parser_log(LEVEL_ERROR, "failed to copy data to write buffer");
        else if (result == 0)
            parser_log(LEVEL_ERROR, "invaild error recored");
        m_woffset += result;
        len -= result;
        m_wused += result;
    }
}
#endif

void Source::SourceWrite(const char* buffer, size_t len)
{
    int nsave;
    errno_t rc;

    while (len > 0) {
        if (m_wused == MAX_BUFFER_SIZE)
            SourceFlush();
        nsave = Min(len, MAX_BUFFER_SIZE - m_wused);
        rc = memcpy_s(m_writeBuf + m_wused, nsave, buffer, nsave);
        parser_securec_check(rc);
        m_woffset += nsave;
        len -= nsave;
        m_wused += nsave;
    }
}

size_t Source::SourceWriteInternal(const void* buffer, size_t len)
{
    size_t nwrite = 0;
    const char* err_file = m_files[m_current - 1].c_str();
    if (m_fd == NULL) {
        parser_log(LEVEL_ERROR, "could not write file: %s, caused by fd empty.", err_file);
    }
    nwrite = fwrite(buffer, 1, len, m_fd);
    if (ferror(m_fd)) {
        parser_log(LEVEL_ERROR, "could not write file %s with Error: %s", err_file, gs_strerror(errno));
    }
    return nwrite;
}

void Source::GenerateNewFile(const char* prefix, const char* suffix)
{
    char path[MAX_PATH_LEN] = {0};
    errno_t rc = EOK;

    ASSERT(prefix != NULL);

    for (int i = 0; i < MAX_SEGMENT_NUM; i++) {
        if (suffix != NULL)
            rc = snprintf_s(path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s.%s.%d", m_path.c_str(), prefix, suffix, i);
        else
            rc = snprintf_s(path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s.%d", m_path.c_str(), prefix, i);
        parser_securec_check_ss(rc);
#ifdef WIN32
        LinuxPathToWin(path);
#endif
        if (access(path, F_OK) != -1)
            continue;

        CloseCurrentFile();
        m_files.push_back(path);
        m_current++;
        m_fd = fopen(path, "w");
        if (m_fd == NULL) {
            parser_log(LEVEL_ERROR, "failed to create new file %s", path);
        }

#ifndef WIN32
        if (fchmod(fileno(m_fd), S_IRUSR | S_IWUSR) != 0) {
            parser_log(LEVEL_ERROR, "failed to chmod file %s with Error: %s", path, gs_strerror(errno));
        }
#endif
        m_woffset = 0;
        return;
    }
    parser_log(LEVEL_ERROR, "failed to generate new file, because of too many files in directory");
}

/**
 * Solve "GDS export" does not support CN retry.
 * At the beginning of the export task, judge whether the file exists.
 * If it exists, delete and recreate it.
 * @param prefix  Export file path prefix.
 * @param suffix  Export file path suffix.
 */
#ifdef GDS_SERVER
void Source::GenerateNewFileForExport(const char* prefix, const char* suffix)
{
    char path[MAX_PATH_LEN] = {0};
    errno_t rc = EOK;

    ASSERT(prefix != NULL);

    if (strstr(prefix, InvalidSymbol) != NULL) {
        parser_log(LEVEL_ERROR, "invalid path which include \"%s\"", InvalidSymbol);
    }
    for (int i = 0; i < MAX_SEGMENT_NUM; i++) {
        if (suffix != NULL) {
            rc = snprintf_s(path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s.%s.%d", m_path.c_str(), prefix, suffix, i);
        } else {
            rc = snprintf_s(path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s.%d", m_path.c_str(), prefix, i);
        }
        parser_securec_check_ss(rc);
#ifdef WIN32
        LinuxPathToWin(path);
#endif
        if (i < m_current) {
            if (access(path, F_OK) != 0) {
                parser_log(LEVEL_ERROR,
                    "export files incomplete because \"%s\" maybe deleted unexpected or no "
                    "permission to access,please delete rest of invalid export file.",
                    path);
            }
        } else {
            if (access(path, F_OK) == 0) {
                unlink(path);
                if (settings.debug_level >= DEBUG_NORMAL) {
                    gs_elog(
                        LEVEL_LOG, "delete export file during  segment file generate for cn retry or repeat export.");
                }
            }
            CloseCurrentFile();
            m_files.push_back(path);
            m_current++;
            CanonicalizePath(path);
            m_fd = fopen(path, "w");
            if (m_fd == NULL) {
                parser_log(LEVEL_ERROR,
                    "failed to create new file %s during segment file generate,please delete rest of invalid export "
                    "file with Error %s.",
                    path,
                    strerror(errno));
            }
#ifndef WIN32
            if (fchmod(fileno(m_fd), LOG_PERM_GRPR) != 0) {
                parser_log(LEVEL_ERROR,"Could not change permissions of file \"%s\"\n", path);
            }
#endif
            m_woffset = 0;
            return;
        }
    }
    parser_log(LEVEL_ERROR, "failed to generate new file, because of too many files in directory");
}
#endif

void Source::SourceFlush()
{
    char* buf = m_writeBuf;

    while (m_wused > 0) {
        int nwrite = SourceWriteInternal(buf, m_wused);
        buf += nwrite;
        m_wused -= nwrite;
    }
}

void Source::CloseCurrentFile()
{
    SourceFlush();
    if (m_fd != NULL) {
        fclose(m_fd);
    }
    m_fd = NULL;

    m_filename = "";
}

void Source::CloseCurrentFileNoFlush()
{
    if (m_fd != NULL) {
        fclose(m_fd);
    }
    m_fd = NULL;
    m_wused = 0;
    m_filename = "";
}

void Source::SourceInit(bool isWrite)
{
    if (isWrite) {
        m_writeBuf = (char*)malloc(MAX_BUFFER_SIZE);
        if (m_writeBuf == NULL)
            parser_log(LEVEL_ERROR, "failed to init source, out of memory");
    } else {
#ifndef WIN32
        if (1 == m_files.size() && m_sourcetype == SOURCE_TYPE_FILE) {
            struct stat status;
            const char* pipe_file = m_files[0].c_str();
            if (stat(pipe_file, &status))
                parser_log(LEVEL_ERROR, "failed to init source");
            /* detect fifo */
            if (S_ISFIFO(status.st_mode)) {
                if (-1 == (m_fifo = open(pipe_file, O_RDONLY)))
                    parser_log(LEVEL_ERROR, "failed to init source");
                SourceRead = SourceRead_FIFO;
                SourceNext = SourceNext_FIFO;
                return;
            }
        }
        string nullFile("/dev/null");
#else
        string nullFile("NUL");
#endif
        (void)m_files.insert(m_files.begin(), nullFile);

#ifdef GDS_SERVER
        SourceRead = SourceRead_File;
        SourceNext = SourceNext_File;
#else
        if (m_sourcetype == SOURCE_TYPE_OBS) {
#ifndef ENABLE_LITE_MODE
            SourceRead = SourceRead_OBS;
            SourceNext = SourceNext_OBS;
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
        } else {
            SourceRead = SourceRead_File;
            SourceNext = SourceNext_File;
        }
#endif

        if (!SourceNext(this))
            parser_log(LEVEL_ERROR, "No files can be opened");
    }
}

#ifdef WIN32
void LinuxPathToWin(char* path)
{
    char* slash = NULL;
    if (path == NULL)
        return;
    while (1) {
        slash = strrchr(path, '/');
        if (slash == NULL)
            break;

        *slash = '\\';
    }
}
#endif

static void NopReadLine(Parser* self, LineBuffer& buf)
{
    parser_log(LEVEL_ERROR, "parser doesn't support read");
}

static void NopWriteLine(Parser* self, struct evbuffer& buf, size_t len)
{
    parser_log(LEVEL_ERROR, "parser doesn't support write");
}

/**
 * @Description: CSV file parser for GDS
 * @in/out self: a CSV parser
 * @in/out buf: a buffer used to send all parsed CSV lines when which is filled.
 * @return: parser result
 * @Notice:
 * A lineBuffer mechanism is used to reduce memory frequent allocatoin
 * for performance optimization. a new parse result named RESULT_BUFFER_FULL is introduced
 * for this mechanism because the memory used by LineBuffer is fixed. With this new mechanism
 * some problems occured because the original logic implementation has been broken as following:
 * (1) After a complete line is parsed in_quote/last_was_esc/in_cr MUSED BE updated or reset;
 * (2) If the linebuffer is full a parsed complete line MUST BE put in linebuffer BEFORE next line parsing.
 */
template <bool skipData>
inline static ParserResult CSVReadLine(CSVParser* self, LineBuffer& buf)
{
    bool need_data = false;
    char* raw_buffer = self->rec_buf;
    char quotec = self->quote;
    char escapec = self->escape;
    Source* source = self->source;

    /*
     * All the following flags MUST BE synchronized to appropriate state before
     * each parsed and completed line is to be added to LineBuffer.
     */
    bool* in_quote = &(self->in_quote);
    bool* last_was_esc = &(self->lastWasEsc);
    bool* in_cr = &(self->in_cr);

    char c;
    int begin_index = self->cur;
    int raw_buf_ptr = self->cur_need_flush;

    /*
     * Flush already parsed lines to LineBuffer
     */
    if (raw_buf_ptr > begin_index) {
        if (!skipData) {
            int ret =
                buf.AppendLine(self->rec_buf + begin_index, raw_buf_ptr - begin_index, self->is_cur_line_completed);
            if (ret < 0) {
                parser_log(LEVEL_ERROR, "Failed to flush last line.");
            }
#ifdef OBS_SERVER
            else if (buf.IsInOverloadBufferAndCompleted()) {
                /*
                 * the data is alreay append in overload buffer and line is completed
                 * move the self->cur and return RESULT_BUFFER_FULL
                 */
                self->cur = raw_buf_ptr;
                self->cur_need_flush = raw_buf_ptr;
                return RESULT_BUFFER_FULL;
            }
#endif
        }

        /*
         * update the position of the alread parsed record.
         */
        self->cur = raw_buf_ptr;
        begin_index = raw_buf_ptr;
    }

    while (true) {
        if (need_data) {
            /*
             * If no record is found in the record buffer, read them from the input file.
             */
            size_t nread;
            if (!skipData) {
                int ret = buf.AppendLine(self->rec_buf + begin_index, raw_buf_ptr - begin_index, false);
                if (ret < 0) {
                    // need flash buffer to libevent
                    self->cur_need_flush = raw_buf_ptr;
                    self->is_cur_line_completed = false;
                    return RESULT_BUFFER_FULL;
                }
            }

            if ((nread = source->SourceRead(source, self->rec_buf, self->buf_len - 1)) == 0) {
                self->cur = raw_buf_ptr;
                self->cur_need_flush = raw_buf_ptr;
                return RESULT_EOF;
            }

            begin_index = 0;
            self->used_len = (int)nread;  // read buffer less than 2GB
            self->cur = 0;
            self->cur_need_flush = 0;
            raw_buf_ptr = 0;
            need_data = false;
        }

        if (raw_buf_ptr == self->used_len) {
            /*
             * If parsing has been done upto the last of the buffer, we read next data.
             */
            need_data = true;
            continue;
        }

        c = raw_buffer[raw_buf_ptr++];

        // is escape char
        if (unlikely(*in_quote && c == escapec))
            *last_was_esc = !*last_was_esc;

        // Check if current char is in quote
        if (unlikely(c == quotec && !*last_was_esc))
            *in_quote = !*in_quote;
        if (unlikely(c != escapec))
            *last_was_esc = false;

        /*
         * Check if we meet the EOL.
         * EOL can be: \n, \r\n, \r and not in quote
         * if yes, flush out data and break, else continue scaning the buffer.
         */

        if (c == '\n' && !*in_quote) {
            /*
             * a complete line has been parsed so in_cr(self->in_cr) MUST BE
             * reset to false.
             */
            *in_cr = false;

            if (!skipData) {
                int ret = buf.AppendLine(self->rec_buf + begin_index, raw_buf_ptr - begin_index, true);
                if (ret < 0) {
                    // need flash buffer to libevent
                    self->cur_need_flush = raw_buf_ptr;
                    self->is_cur_line_completed = true;
                    return RESULT_BUFFER_FULL;
                }
#ifdef OBS_SERVER
                else if (buf.IsInOverloadBufferAndCompleted()) {
                    /*
                     * the data is alreay append in overload buffer and line is completed
                     * move the self->cur and return RESULT_BUFFER_FULL
                     */
                    self->cur = raw_buf_ptr;
                    self->cur_need_flush = raw_buf_ptr;
                    return RESULT_BUFFER_FULL;
                }
#endif
            }
            break;
        } else if (c == '\r' && !*in_quote)
            *in_cr = true;
        else if (*in_cr) {
            raw_buf_ptr--;
            /*
             * a complete line has been parsed so in_cr(self->in_cr) MUST BE
             * reset to false.
             */
            *in_cr = false;

            if (!skipData) {
                int ret = buf.AppendLine(self->rec_buf + begin_index, raw_buf_ptr - begin_index, true);
                if (ret < 0) {
                    // need flash buffer to libevent
                    self->cur_need_flush = raw_buf_ptr;
                    self->is_cur_line_completed = true;
                    return RESULT_BUFFER_FULL;
                }
#ifdef OBS_SERVER
                else if (buf.IsInOverloadBufferAndCompleted()) {
                    /*
                     * the data is alreay append in overload buffer and line is completed
                     * move the self->cur and return RESULT_BUFFER_FULL
                     */
                    self->cur = raw_buf_ptr;
                    self->cur_need_flush = raw_buf_ptr;
                    return RESULT_BUFFER_FULL;
                }
#endif
            }
            break;
        }
    }

    self->cur = raw_buf_ptr;
    self->cur_need_flush = raw_buf_ptr;
    return RESULT_SUCCESS;
}

template <bool skipData>
inline static ParserResult TextReadLine(ReadableParser* self, LineBuffer& buf)
{
    bool need_data = false;
    char* eol = NULL;
    int remainLen;
    char* raw_buffer = self->rec_buf;
    Source* source = self->source;
    int eol_len = (self->eol == NULL) ? 1 : strlen(self->eol);

    while (true) {
        char* end = NULL;

        // if buffer is empty, read new data from files
        //
        if (need_data) {
            size_t nread;
            if ((nread = source->SourceRead(source, self->rec_buf, self->buf_len - 1)) == 0) {
                return RESULT_EOF;
            } else {
                self->cur = 0;
                self->used_len = nread;
                need_data = false;
            }
        }

        // Find EOL in the buffer
        // 1. eol char == '\n', we got a complete row.
        // 2. eol char == '\r', if '\r' is not the last char of buffer, we got a complete row.
        // else read more data and try again.
        //
        raw_buffer = self->rec_buf + self->cur;
        remainLen = self->used_len - self->cur;
        end = self->rec_buf + self->used_len;

        eol = FindEolChar(raw_buffer, remainLen, self->eol, &self->eol_cur, &self->eol_cur_saved);

        if (eol == NULL) {
            if (!skipData) {
                int ret = buf.AppendLine(raw_buffer, remainLen, false);
                if (ret < 0) {
                    // need flash buffer to libevent
                    return RESULT_BUFFER_FULL;
                }
            }
            need_data = true;
            self->cur = self->used_len;
            continue;
        } else if (self->eol == NULL && *eol == '\r') {
            if (eol == end - 1) {
                need_data = true;
                if (!skipData) {
                    int ret = buf.AppendLine(raw_buffer, (eol - raw_buffer), false);
                    if (ret < 0) {
                        // need flash buffer to libevent
                        return RESULT_BUFFER_FULL;
                    }
                }
                self->cur = self->used_len;
                continue;
            } else if (*(eol + 1) == '\n')
                eol++;
            break;
        } else
            break;
    }

    if (!skipData) {
        int ret = buf.AppendLine(raw_buffer, (eol - raw_buffer + eol_len - self->eol_cur_saved), true);
        if (ret < 0) {
            // need flash buffer to libevent
            return RESULT_BUFFER_FULL;
        }
#ifdef OBS_SERVER
        else if (buf.IsInOverloadBufferAndCompleted()) {
            /*
             * the data is alreay append in overload buffer and line is completed
             * move the self->cur and return RESULT_BUFFER_FULL
             */
            self->cur += eol - raw_buffer + 1;
            return RESULT_BUFFER_FULL;
        }
#endif
    }
    self->cur += eol - raw_buffer + eol_len - self->eol_cur_saved;
    self->eol_cur = 0;
    self->eol_cur_saved = 0;
    return RESULT_SUCCESS;
}

inline static ParserResult FixReadLine(FixParser* self, LineBuffer& buf)
{
    int remainLen;
    char* raw_buffer = NULL;
    int needRead = self->rowSize;
    Source* source = self->source;

    /*
     * skip the size of the un-completed row chunk which is already put in LineBuffer;
     */
    if (!buf.IsCurRowCompleted()) {
        needRead -= buf.GetCurRowLen();
    }

    while (needRead > 0) {
        // if buffer is empty, read new data from files
        //
        if (self->used_len == self->cur) {
            size_t nread;
            if ((nread = source->SourceRead(source, self->rec_buf, self->buf_len - 1)) == 0) {
                return RESULT_EOF;
            } else {
                self->cur = 0;
                self->used_len = nread;
            }
        }

        raw_buffer = self->rec_buf + self->cur;
        remainLen = self->used_len - self->cur;
        if (remainLen > 0) {
            int nread = Min(needRead, remainLen);
            needRead -= nread;
            int ret = buf.AppendLine(raw_buffer, nread, (needRead == 0));
            if (ret < 0) {
                // need flash buffer to libevent
                return RESULT_BUFFER_FULL;
            }

            self->cur += nread;
        }
    }

    while (true) {
        if (self->used_len == self->cur) {
            size_t nread;
            if ((nread = source->SourceRead(source, self->rec_buf, self->buf_len - 1)) == 0) {
                // End of file, flush data out.
                //
                if (buf.GetBufferUsedLen() > 0)
                    break;

                return RESULT_EOF;
            } else {
                self->cur = 0;
                self->used_len = nread;
            }
        }

        raw_buffer = self->rec_buf + self->cur;

        if (raw_buffer[0] == '\r') {
            self->cur++;
            continue;
        } else if (raw_buffer[0] == '\n')
            self->cur++;
        break;
    }

    return RESULT_SUCCESS;
}

template <FileFormat format>
ParserResult
#ifdef GDS_SERVER
    GenericReadLines(Parser* self, struct evbuffer& buf)
#else
    GenericReadLines(Parser* self)
#endif
{
    Source* source = self->source;
    ReadableParser* parser = (ReadableParser*)self;

    if (parser->eof)
        return RESULT_EOF;

    // read until line_buffers is full or reach EOF
    while (true) {

        ParserResult result = RESULT_SUCCESS;
        if (format == FORMAT_TEXT)
            result = TextReadLine<false>((ReadableParser*)self, self->line_buffers);
        else if (format == FORMAT_FIXED)
            result = FixReadLine((FixParser*)self, self->line_buffers);
        else if (format == FORMAT_CSV)
            result = CSVReadLine<false>((CSVParser*)self, self->line_buffers);

        if (RESULT_EOF == result) {
            if (source->SourceNext(source)) {
                parser->row_num = 0;
                parser->line_buffers.ResetRowNum();

                // Skip file header line
                //
                if (self->hasHeader) {
                    if (format == FORMAT_TEXT || format == FORMAT_FIXED)
                        (void)TextReadLine<true>((ReadableParser*)self, self->line_buffers);
                    else
                        (void)CSVReadLine<true>((CSVParser*)self, self->line_buffers);
                }
                return RESULT_NEW_ONE;
            } else {
                parser->eof = true;
                return RESULT_EOF;
            }
        } else if (result == RESULT_BUFFER_FULL) {
            return RESULT_BUFFER_FULL;
        }
    }
}

template <bool hasHeader>
ParserResult GenericWriteLines(Parser* self, struct evbuffer& buf, int len)
{
#ifdef GDS_SERVER
    Source* source = self->source;
    size_t segSize = GetDataSegmentSize();

    if (segSize > 0 && source->GetWriteOffset() > segSize) {
        source->GenerateNewFileForExport(((WritableParser*)self)->prefix, "dat");
        if (hasHeader)
            source->SourceWrite(((WritableParser*)self)->fileheader, ((WritableParser*)self)->headerSize);
    }

    source->SourceWrite(&buf, len);
#else
    parser_log(LEVEL_ERROR, "un-implemented code path %s", __FUNCTION__);
#endif
    return RESULT_SUCCESS;
}

static void ReadableParserInit(ReadableParser* self, CmdBegin* cmd, FileList* files, SourceType sourcetype)
{
    self->eof = false;
    if (self->buf_len == 0)
        self->buf_len = INITIAL_BUF_LEN;

    self->rec_buf = (char*)malloc(self->buf_len);
    if (self->rec_buf == NULL) {
        parser_log(LEVEL_ERROR, "memory alloc failed!\n");
    }
    self->used_len = 0;
    self->cur = 0;
    self->eol_cur = 0;
    self->eol_cur_saved = 0;

    self->hasHeader = cmd->m_header;

#ifdef GDS_SERVER
    self->line_buffer = evbuffer_new();
    if (self->line_buffer == NULL)
        parser_log(LEVEL_ERROR, "failed to init parser, out of memory");
#endif
    self->source = CreateSource(files, sourcetype);
    if (sourcetype != SOURCE_TYPE_OBS && (cmd->m_prefix != NULL)) {
        self->prefix = strdup(cmd->m_prefix);
        if (self->prefix == NULL)
            parser_log(LEVEL_ERROR, "failed to copy prefix, out of memory");
    }

    if (self->rec_buf == NULL)
        parser_log(LEVEL_ERROR, "failed to init parser, out of memory");
    self->rec_buf[0] = '\0';

    self->line_buffers.Init();
    self->cur_need_flush = 0;
    self->is_cur_line_completed = false;

    if (cmd->m_eol != NULL) {
        self->eol = strdup(cmd->m_eol);
        if (self->eol == NULL)
            parser_log(LEVEL_ERROR, "failed to copy eol, out of memory");
    }
}

static void CSVParserInit(CSVParser* self, CmdBegin* cmd, FileList* files, SourceType sourcetype)
{
    ReadableParserInit(self, cmd, files, sourcetype);
    self->quote = cmd->m_quote;
    self->escape = cmd->m_escape;
    self->quote = self->quote ? self->quote : '"';
    self->escape = self->escape ? self->escape : '"';
    self->escape = self->escape == self->quote ? '\0' : self->escape;
}

static void FixParserInit(FixParser* self, CmdBegin* cmd, FileList* files, SourceType sourcetype)
{
    ReadableParserInit(self, cmd, files, sourcetype);
    self->rowSize = cmd->m_fixSize;
    if (self->rowSize == 0)
        self->readlines = (ParserReadLineProc)GenericReadLines<FORMAT_TEXT>;
}

static void WritableParserInit(WritableParser* self, CmdBegin* cmd, FileList* files)
{
#ifdef GDS_SERVER
    self->hasHeader = false;
    self->line_buffer = evbuffer_new();
    self->source = CreateSource(NULL, SOURCE_TYPE_FILE);

    if ((self->line_buffer == NULL) || (self->source == NULL))
        parser_log(LEVEL_ERROR, "failed to init parser, out of memory");

    self->source->SetPath(UriToLocalPath(cmd->m_url));
    if (cmd->m_prefix == NULL) {
        parser_log(LEVEL_ERROR, "the given prefix is NULL");
    }
    self->prefix = strdup(cmd->m_prefix);
    if (self->prefix == NULL)
        parser_log(LEVEL_ERROR, "failed to copy prefix, out of memory");
    if (cmd->m_fileheader != NULL)
        GetFileHeader(self, UriToLocalPath(cmd->m_fileheader).c_str());
    self->source->GenerateNewFileForExport(self->prefix, "dat");
    if (cmd->m_fileheader != NULL) {
        self->source->SourceWrite(self->fileheader, self->headerSize);
        self->writelines = (ParserWriteLineProc)GenericWriteLines<true>;
    }
    if (cmd->m_eol != NULL) {
        self->eol = strdup(cmd->m_eol);
        if (self->eol == NULL)
            parser_log(LEVEL_ERROR, "failed to copy eol, out of memory");
    }
#else
    parser_log(LEVEL_ERROR, "un-implemented code path in %s", __FUNCTION__);
#endif
}

Parser* CreateCSVParser()
{
    errno_t rc;
    CSVParser* self = (CSVParser*)malloc(sizeof(CSVParser));

    if (NULL == self)
        parser_log(LEVEL_ERROR, "failed to create parser, out of memory");

    rc = memset_s(self, sizeof(CSVParser), 0, sizeof(CSVParser));
    parser_securec_check(rc);
    self->init = (ParserInitProc)CSVParserInit;
    self->readlines = (ParserReadLineProc)GenericReadLines<FORMAT_CSV>;
    self->writelines = (ParserWriteLineProc)NopWriteLine;
    self->destroy = (ParserDestroyProc)DestroyReadableParser;
    self->cleanup = (ParserDestroyProc)NopCleanup;
    self->in_quote = false;
    self->lastWasEsc = false;
    self->in_cr = false;
    return (Parser*)self;
}

Parser* CreateTextParser()
{
    errno_t rc;
    ReadableParser* self = (ReadableParser*)malloc(sizeof(ReadableParser));

    if (NULL == self)
        parser_log(LEVEL_ERROR, "failed to create parser, out of memory");

    rc = memset_s(self, sizeof(ReadableParser), 0, sizeof(ReadableParser));
    parser_securec_check(rc);
    self->init = (ParserInitProc)ReadableParserInit;
    self->readlines = (ParserReadLineProc)GenericReadLines<FORMAT_TEXT>;
    self->writelines = (ParserWriteLineProc)NopWriteLine;
    self->destroy = (ParserDestroyProc)DestroyReadableParser;
    self->cleanup = (ParserDestroyProc)NopCleanup;
    return (Parser*)self;
}

Parser* CreateFixedParser()
{
    errno_t rc;
    FixParser* self = (FixParser*)malloc(sizeof(FixParser));

    if (NULL == self)
        parser_log(LEVEL_ERROR, "failed to create parser, out of memory");

    rc = memset_s(self, sizeof(FixParser), 0, sizeof(FixParser));
    parser_securec_check(rc);
    self->init = (ParserInitProc)FixParserInit;
    self->readlines = (ParserReadLineProc)GenericReadLines<FORMAT_FIXED>;
    self->writelines = (ParserWriteLineProc)NopWriteLine;
    self->destroy = (ParserDestroyProc)DestroyReadableParser;
    self->cleanup = (ParserDestroyProc)NopCleanup;
    return (Parser*)self;
}

Parser* CreateWritableParser()
{
    errno_t rc;
    WritableParser* self = (WritableParser*)malloc(sizeof(WritableParser));

    if (NULL == self)
        parser_log(LEVEL_ERROR, "failed to create parser, out of memory");

    rc = memset_s(self, sizeof(WritableParser), 0, sizeof(WritableParser));
    parser_securec_check(rc);
    self->init = (ParserInitProc)WritableParserInit;
    self->readlines = (ParserReadLineProc)NopReadLine;
    self->writelines = (ParserWriteLineProc)GenericWriteLines<false>;
    self->destroy = (ParserDestroyProc)DestroyWritableParser;
    self->cleanup = (ParserDestroyProc)CleanupWritablParser;
    return (Parser*)self;
}

Parser* CreateParser(FileFormat format)
{
    Parser* parser = NULL;

    switch (format) {
        case FORMAT_TEXT:
            parser = CreateTextParser();
            break;
        case FORMAT_CSV:
            parser = CreateCSVParser();
            break;
        case FORMAT_FIXED:
            parser = CreateFixedParser();
            break;
        case FORMAT_REMOTEWRITE:
            parser = CreateWritableParser();
            break;
        default:
            parser_log(LEVEL_ERROR, "un-support format.");
    }
    return parser;
}

static void DestroyParser(Parser* self)
{
    if (self->source != NULL) {
        delete self->source;
        self->source = NULL;
    }
#ifdef GDS_SERVER
    if (self->line_buffer != NULL) {
        evbuffer_free(self->line_buffer);
        self->line_buffer = NULL;
    }
#endif
    if (self->prefix != NULL) {
        free(self->prefix);
        self->prefix = NULL;
    }
    if (self->eol != NULL) {
        free(self->eol);
        self->eol = NULL;
    }
    self->line_buffers.Clean();
    free(self);
}

static void DestroyReadableParser(ReadableParser* self)
{
    if (self->rec_buf != NULL) {
        free(self->rec_buf);
        self->rec_buf = NULL;
    }
    DestroyParser(self);
    self = NULL;
}

static void DestroyWritableParser(WritableParser* self)
{
    if (self->fileheader != NULL) {
        free(self->fileheader);
        self->fileheader = NULL;
    }

    self->source->SourceFlush();
    DestroyParser(self);
    self = NULL;
}

static void NopCleanup(Parser* self)
{}

static void CleanupWritablParser(WritableParser* self)
{
    FileList::iterator i;
    FileList* files = self->source->GetFileList();
    self->source->CloseCurrentFileNoFlush();

    // Delete uncommited files
    //
    for (i = files->begin(); i != files->end(); i++) {
        (void)unlink(i->c_str());
    }
}

#ifdef GDS_SERVER
static void GetFileHeader(WritableParser* self, const char* path)
{
    FILE* fd = NULL;
    int nread = 0;
    char* eol = NULL;

    self->fileheader = (char*)calloc(1, FILEHEADER_BUF_SIZE + 1);
    if (self->fileheader == NULL)
        parser_log(LEVEL_ERROR, "out of memory");

    fd = fopen(path, "r");
    if (fd == NULL)
        parser_log(LEVEL_ERROR, "failed to open \"%s\"", path);

    nread = fread(self->fileheader, 1, FILEHEADER_BUF_SIZE, fd);
    int err_no = ferror(fd);
    fclose(fd);

    if (nread <= 0) {
        if (err_no)
            parser_log(LEVEL_ERROR, "failed to read \"%s\"", path);
        else
            parser_log(LEVEL_ERROR, "no data to read from user-define header file \"%s\"", path);
    }

    self->fileheader[nread] = '\0';

    // if the user-define header file has multiple rows,
    // we will use the first row as the header line only.
    //
    eol = FindEolChar(self->fileheader, nread, NULL, NULL, NULL);
    if (eol != NULL) {
        if (*eol != '\n' && *(eol + 1) == '\n')
            eol++;
        *(++eol) = '\0';
        self->headerSize = eol - self->fileheader;
    } else if (nread < FILEHEADER_BUF_SIZE)
        self->headerSize = nread;
    else
        parser_log(LEVEL_ERROR, "user-define header cannot longer than 1MB");
}
#endif

void GDS::LineBuffer::Init()
{
    m_buf_len = MAX_BLK_SIZE;
    m_buf = (char*)malloc(m_buf_len);
    if (m_buf == NULL)
        parser_log(LEVEL_ERROR, "failed to init line buffer, out of memory");

    m_used_len = 0;
    m_row_num = 0;

    m_cur_line = m_buf;
    m_cur_line_len = 0;
    m_cur_line_completed = true;

    m_output = NULL;

#ifdef OBS_SERVER
    m_read_pos = 0;
    Assert(u_sess->cmd_cxt.OBSParserContext);
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->cmd_cxt.OBSParserContext);
    m_overload_buf = makeStringInfo();
    MemoryContextSwitchTo(oldcontext);
    m_overload_buf_completed = false;
#endif
}

void GDS::LineBuffer::Reset()
{
    if (!m_cur_line_completed && (m_buf != m_cur_line)) {
        // move uncomplete line to the front
        errno_t rc = memmove_s(m_buf, m_buf_len, m_cur_line, (m_cur_line_len + ROW_HEADER_SIZE));
        parser_securec_check(rc);

        m_used_len = m_cur_line_len + ROW_HEADER_SIZE;
        m_cur_line = m_buf;
    } else {
        m_used_len = 0;
        m_cur_line = m_buf;
        m_cur_line_len = 0;
    }

#ifdef OBS_SERVER
    m_read_pos = 0;
#endif
}

int GDS::LineBuffer::AppendLine(const char* buf, int buf_len, bool isComplete)
{
    if (buf == NULL)
        return -1;
    if (buf_len == 0 && (m_cur_line_completed || !isComplete))
        return 0;

    // buf_len = 0, isComplete = true means to set line complete

    ASSERT(buf_len > 0);

    if (HasEnoughSpace(buf_len) < 0) {
        /*
         * a overload buffer is found, which length is more than m_buf_len;
         */
        if ((0 == m_used_len) || ((false == m_cur_line_completed) && (m_cur_line == m_buf))) {
            /* (0 == m_used_len) means m_buf is clean, buf is larger
             * ((false == m_cur_line_completed) && (m_cur_line == m_buf))), means  current line  partly in m_buf and buf
             * is larger */
#ifndef OBS_SERVER
            if (SendOverloadBuf(m_output, buf, buf_len, isComplete) < 0)
                parser_log(LEVEL_ERROR, "Failed to send overload buffer.");
            /*
             * Limit the maximum line size to 1GB to be consistent with Datanode, or the memory
             * might be exhausted in the case of huge line size.
             */
            if (evbuffer_get_length(m_output) > MaxAllocSize && !isComplete) {
                parser_log(LEVEL_ERROR, "GDS max line size %d is exceeded.", (int)MaxAllocSize);
            }
#else
#ifndef ENABLE_LITE_MODE
            /* for OBS, append  the overload data  in overload buffer */
            SaveOverloadBuf(m_overload_buf, buf, buf_len, isComplete);
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
#endif
            /*
             * Here the curent overload buffer is already done so the actual buf_len can be returned.
             */
            return buf_len;
        }
        return -1;
    }

    errno_t rc = EOK;

    // is prev line completed
    if (m_cur_line_completed) {
        // new line
        m_cur_line = m_buf + m_used_len;
        m_cur_line_len = 0;

        // jump header
        m_used_len += ROW_HEADER_SIZE;
    } else if (m_used_len == 0 && m_cur_line == m_buf && m_cur_line_len == 0) {
        // Here we handle the case where previous line was overloaded and not complete (in 4MB),
        // while this buf complete the previous line but has size < 256K
        m_used_len += ROW_HEADER_SIZE;
    }

    m_cur_line_len += buf_len;
    m_cur_line_completed = isComplete;

    // update line header if line is completed
    if (isComplete) {
        ++m_row_num;
        char row_header[ROW_HEADER_SIZE];
        *(uint32_t*)&row_header[0] = htonl((uint32_t)(m_cur_line_len + 4));  // add size of row_num
        *(uint32_t*)&row_header[4] = htonl(m_row_num);
        ASSERT(m_used_len >= ROW_HEADER_SIZE);
        rc = memcpy_s(m_cur_line, m_buf_len - (m_cur_line - m_buf), row_header, ROW_HEADER_SIZE);
        parser_securec_check(rc);
    }

    // copy data
    char* cur_ptr = m_buf + m_used_len;
    rc = memcpy_s(cur_ptr, (m_buf_len - m_used_len), buf, buf_len);
    parser_securec_check(rc);

    m_used_len += buf_len;

    return buf_len;
}

#ifdef GDS_SERVER
int GDS::LineBuffer::PackData(evbuffer* dest, bool isFlush)
{
    // 1. compute package size
    int package_size = (isFlush || m_cur_line_completed) ? m_used_len : (m_cur_line - m_buf);

    ASSERT(package_size > 0);

    if (0 == package_size)
        parser_log(LEVEL_ERROR, "Failed to send package which size is 0.");

    errno_t rc = EOK;

    if (isFlush && !m_cur_line_completed) {
        char row_header[ROW_HEADER_SIZE];
        *(uint32_t*)&row_header[0] = htonl((uint32_t)(m_cur_line_len + 4));  // add size of row_num
        *(uint32_t*)&row_header[4] = htonl(++m_row_num);

        rc = memcpy_s(m_cur_line, m_buf_len - (m_cur_line - m_buf), row_header, ROW_HEADER_SIZE);
        parser_securec_check(rc);

        m_cur_line_completed = true;
    }

    // 2. package header
    char package_header[GDSCmdHeaderSize];

    package_header[0] = CMD_TYPE_DATA;
    *(uint32_t*)&package_header[1] = htonl((uint32_t)package_size);

    // 3. add to evbffer
    int retval = 0;
    if ((retval = evbuffer_add(dest, package_header, GDSCmdHeaderSize)) != 0)
        return retval;

    if ((retval = evbuffer_add(dest, m_buf, package_size)) != 0)
        return retval;

    if (settings.debug_level == DEBUG_ON)
        gs_elog(LEVEL_LOG, "send package size %u.", (uint32_t)package_size);

    // 4. reset line buffer
    Reset();

    return retval;
}

int GDS::LineBuffer::SendOverloadBuf(evbuffer* dest, const char* buf, int buf_len, bool isComplete)
{
    char row_header[ROW_HEADER_SIZE];

    /*
     * Update row header info.
     */
    if (isComplete)
        ++m_row_num;

    *(uint32_t*)&row_header[0] = htonl((uint32_t)(m_cur_line_len + buf_len + 4));  // add size of row_num
    *(uint32_t*)&row_header[4] = htonl(m_row_num);

    /*
     * Send the buffer;
     */
    char package_header[GDSCmdHeaderSize];

    if (isComplete)
        package_header[0] = CMD_TYPE_DATA;
    else
        package_header[0] = CMD_TYPE_DATA_SEG;

    *(uint32_t*)&package_header[1] = htonl((uint32_t)(m_cur_line_len + buf_len + ROW_HEADER_SIZE));

    int retval = 0;

    /*
     * send gds command header
     */
    if ((retval = evbuffer_add(dest, package_header, GDSCmdHeaderSize)) != 0)
        return retval;

    /*
     * send row header
     */
    if ((retval = evbuffer_add(dest, row_header, ROW_HEADER_SIZE)) != 0)
        return retval;

    /*
     * send actual buffer
     */
    if (m_cur_line_len > 0) {
        if ((retval = evbuffer_add(dest, m_buf + ROW_HEADER_SIZE, m_cur_line_len)) != 0) {
            return retval;
        }
    }

    if ((retval = evbuffer_add(dest, buf, buf_len)) != 0)
        return retval;

    /*
     * reset line buffer
     */
    m_used_len = 0;
    m_cur_line = m_buf;
    m_cur_line_len = 0;

    CmdBase cmd;
    cmd.m_type = package_header[0];
    GDS_Trace_Cmd(&cmd, current_connection, false);
    return 0;
}
#endif

#ifdef OBS_SERVER
#ifndef ENABLE_LITE_MODE
static size_t SourceRead_OBS(Source* self, void* buffer, size_t len)
{
    size_t nread = 0;
    size_t already_read = 0;

    if (self->m_obs_end) {
        OBSReadWriteHandler* handler = self->GetOBSReadWriteHandler();

        if (handler != NULL) {
            DestroyObsReadWriteHandler(handler, false);
            handler = NULL;
        }

        return (size_t)0;
    }

    OBSReadWriteHandler* handler = self->GetOBSReadWriteHandler();

    if (handler == NULL) {
        const char* current_file = self->m_files[self->m_current - 1].c_str();
        handler = CreateObsReadWriteHandler(current_file, OBS_READ, self->m_obs_options);
        self->SetOBSReadWriteHandler(handler);
    }

    /* The handler should have been properly created at this point */
    ASSERT(handler);

    /* Retry to read until getting requred bytes(4MB default) or EOF */
    PROFILING_OBS_START();
    pgstat_report_waitevent(WAIT_EVENT_OBS_READ);
    do {
        nread = read_bucket_object(handler, (char*)buffer + already_read, (len - already_read));
        already_read += nread; /* nread may be < 0 */

        /* Mark we get the end of current object */
        if (nread == 0)
            self->m_obs_end = true;
    } while (nread > 0 && already_read < len);
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_OBS_END_READ(already_read);

    return already_read;
}

static bool SourceNext_OBS(Source* self)
{
    const char* current_file = NULL;

    if ((size_t)(self->m_current) >= self->m_files.size())
        return false;

    while (current_file == NULL && (size_t)(self->m_current) < self->m_files.size()) {
        current_file = self->m_files[self->m_current].c_str();
        self->m_current++;
        if (current_file != NULL && strcmp(current_file, "/dev/null") == 0) {
            current_file = NULL;
            continue;
        }
    }

    /* Clear the cursor */
    self->m_obs_end = false;

    return true;
}

/*
 * Get a line from LineBuffer,
 *
 * Return:
 *	- @True: succeed fetching a new tuple and put it in output_line
 *	- @False: no tuples can be fetch directly in current position, then we should
 *	get next batch of tuples with function GenericReadLines()
 *
 * Important: THIS FOUNCTION JUST USE FOR OBS , DO NOT USE FOR GDS.
 */
bool GDS::LineBuffer::GetNextLine(StringInfo output_line)
{
    /* when have overload data the whole line may in two ways:
     *  a.  m_overload_buf
     *  b.  m_overload_buf + m_buf
     */

    /* check overload stringinfo, get line data from overload stringinfo first*/
    if (m_overload_buf->len != 0) {
        /* copy overload buffer */
        enlargeStringInfo(output_line, m_overload_buf->len);
        appendBinaryStringInfo(output_line, m_overload_buf->data, m_overload_buf->len);

        /* reset overload stinginfo */
        resetStringInfo(m_overload_buf);

        /* if the line not completed will continue read from m_buf */
        if (m_overload_buf_completed)
            return true;
    }

    /* then get line data from m_buf */
    /* Request a tuple before line buffer not filled */
    if (m_used_len == 0)
        return false;

    /* Request tuple when no more data in line buffer */
    if (NoMoreProcessed())
        return false;

    Assert(m_read_pos < m_used_len);

    char* buf = m_buf + m_read_pos;

    /* Parsing the tuple header part */
    char header[ROW_HEADER_SIZE];
    errno_t rc = memcpy_s(header, ROW_HEADER_SIZE, buf, ROW_HEADER_SIZE);
    parser_securec_check(rc);
    int64_t tuplen = ntohl(*(uint32_t*)&(header[0]));
    int64_t nth = ntohl(*(uint32_t*)&(header[4]));

    if (nth < 0 || (unsigned int64_t)nth > PG_UINT32_MAX || tuplen < 0 ||
        (unsigned int64_t)tuplen > MaxAllocSize + GDS_HEADER_LEN) {
        parser_log(
            LEVEL_ERROR, "Linebuffer's content is trashed as tuple's ID %lu and length is not valid %lu", nth, tuplen);
    }

    /* Minus the tuple id in current line buffer to the actual tuplelen */
    tuplen -= 4;

    /* Allocate space and put tuple content to line buffer */
    enlargeStringInfo(output_line, tuplen);
    appendBinaryStringInfo(output_line, buf + ROW_HEADER_SIZE, tuplen);

    /* Move m_read_pos forward pointing to next tuple */
    m_read_pos += (ROW_HEADER_SIZE + tuplen);

    return true;
}

/*
 * Mark the last line completed
 *
 * Note: we commonly use this function after getting a batch of tuples in LineBuffer
 * for OBS parsing
 *
 * Important: THIS FOUNCTION JUST USE FOR OBS , DO NOT USE FOR GDS.
 */
void GDS::LineBuffer::MarkLastLineCompleted()
{
    /* when have overload data the whole line will in two ways.
     *  a.  m_overload_buf
     *  b.  m_overload_buf + m_buf
     *  so then uncompleted line will in m_overload_buf or m_buf
     */

    if (m_used_len != 0) {
        /* uncomplete line in m_buf */
        if (!m_cur_line_completed) {
            /* just like PackData (dest, TRUE) ,  fill the ROW HEADER of the last uncompleted row, and mark it completed
             */
            char row_header[ROW_HEADER_SIZE];
            *(uint32_t*)&row_header[0] = htonl((uint32_t)(m_cur_line_len + 4));  // add size of row_num
            *(uint32_t*)&row_header[4] = htonl(++m_row_num);
            errno_t rc = memcpy_s(m_cur_line, m_buf_len - (m_cur_line - m_buf), row_header, ROW_HEADER_SIZE);
            parser_securec_check(rc);

            m_cur_line_completed = true;
        }
    } else {
        /* uncomplete line in m_overload_buf */
        if (m_overload_buf->len != 0)
            m_overload_buf_completed = true;
    }
}

/*
 * @Description:  copy the read line to stringinfo when the readline can not fill in m_buf
 * @IN/OUT dest: copy dest
 * @IN/OUT buf: src buffer
 * @IN/OUT buf_len: length of the src buffer
 * @IN/OUT isComplete: is the line completed
 * @See also: just like SendOverloadBuf for GDS
 *
 * Note: when have overload data the whole line may in two ways:
 *            a.  m_overload_buf
 *            b.  m_overload_buf + m_buf
 *
 *           the overload line data in stringinfo is without package_header or row_header
 *
 * Important: THIS FOUNCTION JUST USE FOR OBS , DO NOT USE FOR GDS.
 */
void GDS::LineBuffer::SaveOverloadBuf(StringInfo dest, const char* buf, int buf_len, bool isComplete)
{
    Assert((0 == m_used_len) || ((false == m_cur_line_completed) && (m_cur_line == m_buf)));

    Assert(u_sess->cmd_cxt.OBSParserContext);
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->cmd_cxt.OBSParserContext);

    /* add row num */
    if (isComplete)
        ++m_row_num;

    /* copy current uncompleted line from  to m_buf stringinfo */
    if (m_cur_line_len > 0) {
        enlargeStringInfo(dest, m_cur_line_len);
        appendBinaryStringInfo(dest, m_buf + ROW_HEADER_SIZE, m_cur_line_len);
    }

    /* append buf to stringinfo */
    enlargeStringInfo(dest, buf_len);
    appendBinaryStringInfo(dest, buf, buf_len);

    /* set the line is completed */
    m_overload_buf_completed = isComplete;

    /* reset line buffer */
    m_used_len = 0;
    m_cur_line = m_buf;
    m_cur_line_len = 0;

    MemoryContextSwitchTo(oldcontext);
}
#endif
#endif
