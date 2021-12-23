/* -------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the openGauss copy command.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/commands/copy.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "executor/exec/execdesc.h"
#include "commands/gds_stream.h"
#include "commands/copypartition.h"
#include "bulkload/utils.h"
#include "gs_ledger/ledger_utils.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/remotecopy.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "access/obs/obs_am.h"
#endif

struct Formatter;

/* CopyStateData is private in commands/copy.c */
struct CopyStateData;
typedef struct CopyStateData* CopyState;

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest {
    COPY_FILE,   /* to/from file */
    COPY_OLD_FE, /* to/from frontend (2.0 protocol) */
    COPY_NEW_FE  /* to/from frontend (3.0 protocol) */
#ifdef PGXC
    ,
    COPY_BUFFER /* Do not send, just prepare */
#endif
    ,
    COPY_GDS,
    COPY_FILE_SEGMENT,
    COPY_ROACH,
    COPY_OBS
} CopyDest;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType { EOL_UNKNOWN, EOL_NL, EOL_CR, EOL_CRNL, EOL_UD } EolType;

typedef enum { MODE_INVALID, MODE_NORMAL, MODE_SHARED, MODE_PRIVATE } ImportMode;

typedef struct BulkLoadFunc {
    void (*initBulkLoad)(CopyState cstate, const char* filename, List* totalTask);
    void (*endBulkLoad)(CopyState cstate);
} BulkLoadFunc;

typedef int (*CopyGetDataFunc)(CopyState cstate, void* databuf, int minread, int maxread);
typedef bool (*CopyReadlineFunc)(CopyState cstate);
typedef void (*CopyWriteLineFunc)(CopyState cstate);
typedef bool (*GetNextCopyFunc)(CopyState cstate);
typedef int (*CopyReadAttrsFunc)(CopyState cstate);

/*
 * This struct is used to record original illegal chars error info;
 * For non-char type field illegal_chars_fault_torlerance doesn't work,
 * so still report illegal chars error.
 */
typedef struct IllegalCharErrInfo {
    int err_offset;   /* original error offset in each bulkload date line */
    int err_field_no; /* original error corresponding to the number of field */
    int err_code;     /* original error code */
    char* err_info;   /* original error info */

    int src_encoding;
    int dest_encoding;
} IllegalCharErrInfo;

/* StringInfo pointer */
struct stringinfo_pointer {
    char* data; /* data buffer */
    int len;    /* data length */
    int cursor; /* cursor to scan data buffer */

    inline void reset(void)
    {
        data = NULL;
        len = 0;
        cursor = 0;
    }

    void set(char* strinfo, int strinfo_len)
    {
        data = strinfo;
        len = strinfo_len;
        cursor = 0;
    }
};

typedef stringinfo_pointer* stringinfo_ptr;

/* custom format about date/time/timestamp/small datetime. */
struct user_time_format {
    /* text about date/time/timestamp/small datetime. */
    char* str;

    /*
     * FormatNode info about date/time/timestamp/small datetime.
     * because FormatNode is defined in CPP file, so use void* type here.
     */
    void* fmt;
};

/* The typmod of assigned typename under transform condition. */
struct AssignTypmod {
    int32 typemod;
    bool assign;
};

/* OBS Manipulation functions */
extern void initOBSModeState(CopyState cstate, const char* filename, List* totalTask);
extern void endOBSModeBulkLoad(CopyState cstate);
extern bool CopyGetNextLineFromOBS(CopyState cstate);
extern bool getNextOBS(CopyState cstate);

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is TRUE
 * when we have to do it the hard way.
 */
typedef struct CopyStateData {
    /* low-level state data */
    CopyDest copy_dest;         /* type of copy source/destination */
    bool is_from;               /* Is this a Copy From or Copy To? */
    FILE* copy_file;            /* used if copy_dest == COPY_FILE */
    StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
                                 * dest == COPY_NEW_FE in COPY FROM */
    bool fe_eof;                /* true if detected end of copy data */
    EolType eol_type;           /* EOL type of input */
    int file_encoding;          /* file or remote side's character encoding */
    bool need_transcoding;      /* file encoding diff from server? */
    bool encoding_embeds_ascii; /* ASCII can be non-first byte? */

    /* parameters from the COPY command */
    Relation rel;           /* relation to copy to or from */
    Relation curPartionRel; /* current partion to copy to*/
    QueryDesc* queryDesc;   /* executable query to copy from */
    List* attnumlist;       /* integer list of attnums to copy */
    char* filename;         /* filename, or NULL for STDIN/STDOUT */
    char* null_print;       /* NULL marker string (server encoding!) */
    char* delim;            /* column delimiter (must be no more than 10 bytes) */
    int null_print_len;     /* length of same */
    int delim_len;
    char* null_print_client;               /* same converted to file encoding */
    char* quote;                           /* CSV quote char (must be 1 byte) */
    char* escape;                          /* CSV escape char (must be 1 byte) */
    char* eol;                             /* user defined EOL string */
    List* force_quote;                     /* list of column names */
    bool* force_quote_flags;               /* per-column CSV FQ flags */
    List* force_notnull;                   /* list of column names */
    bool* force_notnull_flags;             /* per-column CSV FNN flags */
    user_time_format date_format;          /* user-defined date format */
    user_time_format time_format;          /* user-defined time format */
    user_time_format timestamp_format;     /* user-defined timestamp format */
    user_time_format smalldatetime_format; /* user-defined smalldatetime format */
    /* the flag used to control whether illegal characters can be fault-tolerant or not */
    bool compatible_illegal_chars;
    bool oids; /* include OIDs? */
    bool freeze;
    bool header_line;     /* CSV header line? */
    bool force_quote_all; /* FORCE QUOTE *? */
    bool without_escaping;
    List *trans_expr_list;  /* list of information for each transformed column */
    TupleDesc trans_tupledesc;  /* tuple desc after exec transform expression */
    const char *source_query_string;      /* source SQL string for copy */
    const char *transform_query_string;   /* transform expression SQL string */

    /* these are just for error messages, see CopyFromErrorCallback */
    const char* cur_relname; /* table name for error messages */
    uint32 cur_lineno;       /* line number for error messages */
    const char* cur_attname; /* current att for error messages */
    const char* cur_attval;  /* current att value for error messages */

    /*
     * Working state for COPY TO/FROM
     */
    MemoryContext copycontext; /* per-copy execution context */

    /*
     * Working state for COPY TO
     */
    FmgrInfo* out_functions;  /* lookup info for output functions */
    MemoryContext rowcontext; /* per-row evaluation context */

    /*
     * Working state for COPY FROM
     */
    AttrNumber num_defaults;
    bool file_has_oids;
    FmgrInfo oid_in_function;
    Oid oid_typioparam;
    FmgrInfo* in_functions; /* array of input functions for each attrs */
    Oid* typioparams;       /* array of element types for in_functions */
    bool* accept_empty_str; /* is type of each column can accept empty string? */
    int* defmap;            /* array of default att numbers */
    ExprState** defexprs;   /* array of default att expressions */
    bool volatile_defexprs; /* is any of defexprs volatile? */
    List* range_table;
    Datum copy_beginTime;    /* log the query start time*/
    bool log_errors;         /* mark where we tolerate data exceptions and log them into copy_error_log */
    int reject_limit;        /* number of data exceptions we allow per Copy (on coordinator) */
    bool logErrorsData;      /* mark where we tolerate data exceptions and log them into copy_error_log  and make sure
                                Whether to fill the rowrecord field of the copy_error_log*/
    Relation err_table;      /* opened copy_error_log table */
    CopyErrorLogger* logger; /* logger used for copy from error logging*/
    FmgrInfo* err_out_functions; /* lookup info for output functions of copy_error_log*/
    AssignTypmod *as_typemods;   /* array of typmod for each transformed column */
    ExprState** transexprs;     /* array of expr for each transformed column */

    Relation summary_table; /* opened copy_summary table */
    int64 skiprows;
    int64 loadrows;
    int64 errorrows;
    int64 whenrows;
    int64 allnullrows;

    List *sequence_col_list;  /* list of information for each sequence column */
    SqlLoadSequInfo **sequence;
    StringInfoData sequence_buf;

    List *filler_col_list;  /* list of information for each filler column */
    SqlLoadFillerInfo **filler;

    List *constant_col_list;  /* list of information for each sequence column */
    SqlLoadConsInfo **constant;

    /*
     * These variables are used to reduce overhead in textual COPY FROM.
     *
     * attribute_buf holds the separated, de-escaped text for each field of
     * the current line.  The CopyReadAttributes functions return arrays of
     * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
     * the buffer on each cycle.
     */
    StringInfoData attribute_buf;

    /* field raw data pointers found by COPY FROM */

    int max_fields;
    char** raw_fields;

    /*
     * Similarly, line_buf holds the whole input line being processed. The
     * input cycle is first to read the whole line into line_buf, convert it
     * to server encoding there, and then extract the individual attribute
     * fields into attribute_buf.  line_buf is preserved unmodified so that we
     * can display it in error messages if appropriate.
     */
    StringInfoData line_buf;
    bool line_buf_converted; /* converted to server encoding? */

    /*
     * Finally, raw_buf holds raw data read from the data source (file or
     * client connection).	CopyReadLine parses this data sufficiently to
     * locate line boundaries, then transfers the data to line_buf and
     * converts it.  Note: we guarantee that there is a \0 at
     * raw_buf[raw_buf_len].
     */
#define RAW_BUF_SIZE 65536 /* we palloc RAW_BUF_SIZE+1 bytes */
    char* raw_buf;
    int raw_buf_index; /* next byte to process */
    int raw_buf_len;   /* total # of bytes stored */

    PageCompress* pcState;

#ifdef PGXC
    /* Remote COPY state data */
    RemoteCopyData* remoteCopyState;
#endif
    int fill_missing_fields; /* 0 off;1 Compatible with the original copy; -1 trailing nullcols */
    bool ignore_extra_data; /* ignore overflowing fields */

    Formatter* formatter;
    FileFormat fileformat;
    char* headerFilename; /* User define header filename */
    char* out_filename_prefix;
    char* out_fix_alignment;
    StringInfo headerString;

    /* For bulkload*/
    BulkLoadStream* io_stream;
    List* file_list;
    ListCell* file_index;
    ImportMode mode;
    List* taskList;
    ListCell* curTaskPtr;
    long curReadOffset;
    BulkLoadFunc bulkLoadFunc;
    CopyGetDataFunc copyGetDataFunc;
    CopyReadlineFunc readlineFunc;
    CopyReadAttrsFunc readAttrsFunc;
    GetNextCopyFunc getNextCopyFunc;
    stringinfo_pointer inBuffer;

    List *when;

    int64 skip;

    uint32 distSessionKey;
    List* illegal_chars_error; /* used to record every illegal_chars_error for each imported data line. */

    bool isExceptionShutdown; /* To differentiate normal end of a bulkload task or a task abort because of exceptions*/

    // For export
    //
    uint64 distCopyToTotalSize;
    CopyWriteLineFunc writelineFunc;
    bool remoteExport;
    StringInfo outBuffer;

    // For Roach
    void* roach_context;
    Node* roach_routine;

    // For OBS parallel import/export
    ObsCopyOptions obs_copy_options;

    /* adaptive memory assigned for the stmt */
    AdaptMem memUsage;

    LedgerHashState hashstate;
    bool is_load_copy;
    bool is_useeof;
} CopyStateData;

typedef struct InsertCopyLogInfoData {
    Relation rel;
    int insertOpt;
    CommandId mycid;
    EState *estate;
    ResultRelInfo *resultRelInfo;
    TupleTableSlot *myslot;
    BulkInsertState bistate;
}InsertCopyLogInfoData;

typedef struct InsertCopyLogInfoData* LogInsertState;

#define IS_CSV(cstate) ((cstate)->fileformat == FORMAT_CSV)
#define IS_BINARY(cstate) ((cstate)->fileformat == FORMAT_BINARY)
#define IS_FIXED(cstate) ((cstate)->fileformat == FORMAT_FIXED)
#define IS_TEXT(cstate) ((cstate)->fileformat == FORMAT_TEXT)
#define IS_REMOTEWRITE(cstate) ((cstate)->fileformat == FORMAT_WRITABLE)

CopyState BeginCopyTo(
    Relation rel, Node* query, const char* queryString, const char* filename, List* attnamelist, List* options);
void EndCopyTo(CopyState cstate);
uint64 DoCopyTo(CopyState cstate);
extern uint64 DoCopy(CopyStmt* stmt, const char* queryString);
template<bool skipEol>
void CopySendEndOfRow(CopyState cstate);
void CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum* values, const bool* nulls);

extern void ProcessCopyOptions(CopyState cstate, bool is_from, List* options);
extern bool IsTypeAcceptEmptyStr(Oid typeOid);
extern CopyState BeginCopyFrom(Relation rel, const char* filename, List* attnamelist, 
    List* options, void* mem_info, const char* queryString);
extern void EndCopyFrom(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, ExprContext* econtext, Datum* values, bool* nulls, Oid* tupleOid);
extern bool NextCopyFromRawFields(CopyState cstate, char*** fields, int* nfields);
extern void CopyFromErrorCallback(void* arg);
extern void BulkloadErrorCallback(void* arg);

extern DestReceiver* CreateCopyDestReceiver(void);

extern CopyState begin_dist_copy_from(
    Relation rel, const char* filename, List* attnamelist, List* options, List* totalTask);

extern void end_dist_copy(CopyState cstate);

extern List* DeserializeLocations(const char* location);

extern bool is_valid_location(const char* location);

extern bool is_local_location(const char* location);

extern bool is_roach_location(const char* location);

extern void getPathAndPattern(char* url, char** path, char** pattern);

extern char* scan_dir(DIR* dir, const char* dirpath, const char* pattern, long* filesize);

extern CopyState beginExport(
    Relation rel, const char* filename, List* options, bool isRemote, uint32 sessionKey, List* tasklist);
extern void execExport(CopyState cpstate, TupleTableSlot* slot);
extern void endExport(CopyState cstate);

extern uint64 exportGetTotalSize(CopyState cstate);
extern void exportResetTotalSize(CopyState cstate);
extern void exportAllocNewFile(CopyState cstate, const char* newfile);
extern void exportFlushOutBuffer(CopyState cstate);

extern void ProcessFileHeader(CopyState cstate);

extern List* addNullTask(List* taskList);
extern void CopySendString(CopyState cstate, const char* str);
extern void CopySendChar(CopyState cstate, char c);

extern void cstate_fields_buffer_init(CopyState cstate);
extern bool IsCharType(Oid attr_type);
extern int GetDecimalFromHex(char hex);
extern char* limit_printout_length(const char* str);

extern bool StrToInt32(const char* s, int *val);
extern char* TrimStr(const char* str);

extern void UHeapAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy);

extern void HeapAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy);

extern void UHeapAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy);

extern void HeapAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy);
#endif /* COPY_H */

extern void CopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hi_options,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    HeapTuple* bufferedTuples, Partition partition, int2 bucketId);
extern void UHeapCopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    UHeapTuple* bufferedTuples, Partition partition, int2 bucketId);


extern void CheckCopyWhenExprOptions(LoadWhenExpr *when);
extern char* pg_strdup(const char* string);
