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
 *  parser.h
 *
 * IDENTIFICATION
 *        src/bin/gds/parser.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include <stdio.h>
#include <vector>
#include <string>
#include <assert.h>

#ifdef GDS_SERVER
#include "event.h"
#endif
#include "bulkload/utils.h"
#ifdef OBS_SERVER
#include "access/obs/obs_am.h"
#include "commands/copy.h"
#include "lib/stringinfo.h"
#endif

using std::string;
using std::vector;

typedef vector<string> FileList;

// for destructor function use
#define IGNORE_EXCEPTION(A) \
    try {                   \
        A;                  \
    } catch (...) {         \
    }

#define ROW_HEADER_SIZE 8

#ifndef USE_ASSERT_CHECKING
#define ASSERT(condition)
#else
#define ASSERT(condition) assert(condition)
#endif

namespace GDS {
class LineBuffer {
public:
    LineBuffer()
    {
        m_buf = NULL;
        m_buf_len = 0;
        m_used_len = 0;

        m_row_num = 0;
        m_cur_line = NULL;
        m_cur_line_len = 0;
        m_cur_line_completed = true;

        m_output = NULL;
#ifdef OBS_SERVER
        m_read_pos = 0;
        m_overload_buf = NULL;
        m_overload_buf_completed = false;
#endif
    }

    ~LineBuffer()
    {
        if (m_buf != NULL) {
            free(m_buf);
            m_buf = NULL;
        }
    }

#ifdef OBS_SERVER

    /* Get (via copy) next line reffered by m_read_pos */
    bool GetNextLine(StringInfo output_line);

    /* force mark the last line completed */
    void MarkLastLineCompleted();

    /* Check if the m_read_pos has shift to last completed line in LineBuffer */
    inline bool NoMoreProcessed()
    {
        /* check if overload buffer has data to process */
        if (m_overload_buf->len)
            return false;

        /* check if m_buf  has data  to process  */
        int read_end_pos = m_cur_line_completed ? m_used_len : (m_cur_line - m_buf);

        if (m_read_pos >= read_end_pos) {
            return true;
        }

        return false;
    };

    /*
     * Check if data in overload buffer and current line is completed
     * return TRUE for overload buffer has data and the line is completed in overload buffer
     */
    inline bool IsInOverloadBufferAndCompleted()
    {
        if (m_overload_buf->len != 0 && m_overload_buf_completed)
            return true;

        return false;
    }
#endif
    inline int HasEnoughSpace(int buf_len)
    {
        if (m_cur_line_completed)
            return m_buf_len - m_used_len - ROW_HEADER_SIZE - buf_len;
        else
            return m_buf_len - m_used_len - buf_len;
    }

    inline int GetBufferUsedLen()
    {
        return m_used_len;
    }

    inline int GetCompletedRowLen()
    {
        return m_cur_line - m_buf;
    }

    inline int GetCurRowLen()
    {
        return m_cur_line_len;
    }

    inline void ResetRowNum()
    {
        m_row_num = 0;
    }

    inline bool IsCurRowCompleted()
    {
        return m_cur_line_completed;
    }

    /* Set the current output evbuffer using the current connection output evbuffer.
     *
     *	@param output the current connection output evbuffer
     *	@return void
     */
    inline void SetOutput(struct evbuffer* output)
    {
        m_output = output;
    }

    void Init();
    inline void Clean()
    {
        if (m_buf != NULL)
            free(m_buf);

        m_buf = NULL;
    }
    void Reset();
    int AppendLine(const char* buf, int buf_len, bool isComplete);
#ifdef GDS_SERVER

    int PackData(evbuffer* dest, bool isFlush);
    /*Send a overloaded buffer crossing multi packages.
     * For supporting this scenrio some changes occur:
     * (1) If a un-completed segment of line is sent, which uses speficied 0 line number to indicate;
     * (2) If a un-completed segment of line is sent, wich length is the current segment length.
     *
     *	@param dest the current connection output evbuffer
     *	@return int >=0 if success, <0 if fails.
     */
    int SendOverloadBuf(evbuffer* dest, const char* buf, int buf_len, bool isComplete);
#endif
private:
    char* m_buf;
    int m_buf_len;
    int m_used_len;
    unsigned int m_row_num;

    char* m_cur_line;
    int m_cur_line_len;
    bool m_cur_line_completed;

    /*
     * all members about overload buffer
     */
    struct evbuffer* m_output;
#ifdef OBS_SERVER
    StringInfo m_overload_buf;
    int m_read_pos;
    bool m_overload_buf_completed;

    /* save overload buffer to stringinfo for obs */
    void SaveOverloadBuf(StringInfo dest, const char* buf, int buf_len, bool isComplete);
#endif
};

class Source;

typedef size_t (*SourceReadProc)(Source* self, void* buffer, size_t len);

typedef bool (*SourceNextProc)(Source* self);
typedef enum { SOURCE_TYPE_UNKNOWN, SOURCE_TYPE_FILE, SOURCE_TYPE_OBS } SourceType;

class Source {
public:
    const static size_t MAX_BUFFER_SIZE = 1024 * 1024;

    Source()
    {
        m_fd = NULL;
        m_fifo = -1;
        m_current = 0;
        m_woffset = 0;
        m_wused = 0;
        m_writeBuf = NULL;
        SourceRead = NULL;
        SourceNext = NULL;
#ifdef OBS_SERVER
        m_obs_options = NULL;
        m_obs_end = false;
        m_rwhandler = NULL;
#endif
        /*
         * The source type indicator is referred as UNKNOWN first, in constructor
         * we first set its value to unknown and initialize to proper 'source type'
         * at initialization time.
         */
        m_sourcetype = SOURCE_TYPE_UNKNOWN;
    }

    ~Source()
    {
        if (m_fd != NULL) {
            fclose(m_fd);
            m_fd = NULL;
        }

#ifndef WIN32
        if (m_fifo != -1) {
            close(m_fifo);
            m_fifo = -1;
        }
#endif
        if (m_writeBuf != NULL) {
            free(m_writeBuf);
            m_writeBuf = NULL;
        }

        SourceRead = NULL;
        SourceNext = NULL;
    }
#ifdef GDS_SERVER
    void SourceWrite(evbuffer* buffer, size_t len);
#endif
    void SourceWrite(const char* buffer, size_t len);

    SourceReadProc SourceRead;

    SourceNextProc SourceNext;

    inline size_t GetWriteOffset()
    {
        return m_woffset;
    }

    void GenerateNewFile(const char* prefix, const char* suffix = NULL);
    void GenerateNewFileForExport(const char* prefix, const char* suffix);
    void SourceFlush();

    FileList* GetFileList()
    {
        return &m_files;
    }

    void SetFileList(const FileList& files)
    {
        m_files = files;
    }
    inline void SetSourceType(SourceType sourcetype)
    {
        m_sourcetype = sourcetype;
    };
#ifdef OBS_SERVER
    inline OBSReadWriteHandler* GetOBSReadWriteHandler(void)
    {
        return m_rwhandler;
    };

    inline void SetOBSReadWriteHandler(OBSReadWriteHandler* handler)
    {
        m_rwhandler = handler;
    };
#endif
    void SetPath(const string& path)
    {
        m_path = path;
    }

    string GetCurrentFilename()
    {
        return m_filename;
    }

    void SourceInit(bool isWrite);

    void CloseCurrentFile();
    void CloseCurrentFileNoFlush();

    FILE* m_fd;
    int m_fifo;
    FileList m_files;
    int m_current;
    string m_filename;
#ifdef OBS_SERVER
    OBSReadWriteHandler* m_rwhandler;
    bool m_obs_end;
    ObsCopyOptions* m_obs_options;
#endif
private:
    inline size_t SourceWriteInternal(const void* buffer, size_t len);

    size_t m_woffset;
    char* m_writeBuf;
    size_t m_wused;
    string m_path;
    /* new added from DWS */
    SourceType m_sourcetype;
};

typedef enum { RESULT_SUCCESS, RESULT_NEW_ONE, RESULT_EOF, RESULT_BUFFER_FULL } ParserResult;

struct Parser;
/*
 * Parser
 */
typedef void (*ParserInitProc)(Parser* self, CmdBegin* cmd, const FileList* files, SourceType sourcetype);
#ifdef GDS_SERVER
typedef ParserResult (*ParserReadLineProc)(Parser* self, struct evbuffer& buf);
#else
typedef ParserResult (*ParserReadLineProc)(Parser* self);
#endif
typedef ParserResult (*ParserWriteLineProc)(Parser* self, struct evbuffer& buf, int len);
typedef void (*ParserDestroyProc)(Parser* self);

struct Parser {
    ParserInitProc init;          /*initialize*/
    ParserReadLineProc readlines; /* read one line */
    ParserWriteLineProc writelines;
    ParserDestroyProc destroy;
    ParserDestroyProc cleanup;

    Source* source;

    evbuffer* line_buffer;

    bool hasHeader;

    char* prefix;

    LineBuffer line_buffers;

    char* eol;
};

struct ReadableParser : public Parser {

    /**
     * @brief Record Buffer.
     *
     * This buffer stores the data read from the input file.
     */
    char* rec_buf;

    /**
     * @brief Size of the record buffer.
     */
    int buf_len;

    /**
     * @brief Actual size in the record buffer, excluding the trailing NULL.
     */
    int used_len;

    /**
     * @brief Pointer to the current record in the record buffer.
     */
    int cur;

    /**
     * @brief Pointer to the current position of eol string read in the record buffer.
     */
    int eol_cur;

    /**
     * @brief Pointer to the position of eol string read in the last record buffer.
     */
    int eol_cur_saved;

    /**
     * @brief Pointer to the current record in the record buffer, for two situations:
     * If the current parsed lines alreadily flushed to LineBuffer the same as cur;
     * if not, the records between [cur, cur_need_flush] need be flushed to LineBuffer
     * before next parse operation to avoid redundant parse operation.
     */
    int cur_need_flush;

    /**
     * @brief flag to show whether the current record in the record buffer is a
     * completed line or not. If true means completed, false means not.
     */
    bool is_cur_line_completed;

    /**
     * @brief Flag indicating EOF has been encountered in the input file.
     */
    bool eof;

    unsigned int row_num;
};

struct CSVParser : public ReadableParser {
    char quote;  /**< quotation string */
    char escape; /**< escape letter */

    bool in_quote;   /* current position is in quote */
    bool lastWasEsc; /* prev position is esc */
    bool in_cr;      /* current position is after \r */
};

struct FixParser : public ReadableParser {
    int rowSize;
};

struct WritableParser : public Parser {
    char* fileheader;
    int headerSize;
};
};  // namespace GDS
extern GDS::Parser* CreateParser(FileFormat format);

#ifdef WIN32
extern void LinuxPathToWin(char* path);
#endif

#endif
