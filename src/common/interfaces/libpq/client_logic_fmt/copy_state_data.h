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
 * -------------------------------------------------------------------------
 *
 * copy_state_data.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\copy_state_data.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COPY_STATE_DATA_H
#define COPY_STATE_DATA_H

struct List;

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest {
    COPY_FILE,   /* to/from file */
    COPY_OLD_FE, /* to/from frontend (2.0 protocol) */
    COPY_NEW_FE  /* to/from frontend (3.0 protocol) */
} CopyDest;

/*
 * Represents the end-of-line terminator type of the input
 */
typedef enum EolType {
    EOL_UNKNOWN,
    EOL_NL,
    EOL_CR,
    EOL_CRNL
} EolType;

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
    CopyDest copy_dest = COPY_FILE;      /* type of copy source/destination */
    bool fe_eof;                       /* true if detected end of copy data */
    EolType eol_type;                                  /* EOL type of input */
    int file_encoding;          /* file or remote side's character encoding */
    bool need_transcoding;               /* file encoding diff from server? */
    bool encoding_embeds_ascii;             /* ASCII can be non-first byte? */
    bool is_attlist_null;       /* the attribute list in the copy statement */

    /* parameters from the COPY command */
    bool is_rel;
    bool binary;                                          /* binary format? */
    bool oids;                                             /* include OIDs? */
    bool csv_mode;                         /* Comma Separated Value format? */
    bool header_line;                                   /* CSV header line? */
    const char *null_print;        /* NULL marker string (server encoding!) */
    int null_print_len;                                   /* length of same */
    char *null_print_client;             /* same converted to file encoding */
    char delim;                        /* column delimiter (must be 1 byte) */
    char quote;                          /* CSV quote char (must be 1 byte) */
    char escape;                        /* CSV escape char (must be 1 byte) */
    List *force_quote;                              /* list of column names */
    bool force_quote_all;                                 /* FORCE QUOTE *? */
    bool *force_quote_flags;                     /* per-column CSV FQ flags */
    List *force_notnull;                            /* list of column names */
    bool *force_notnull_flags;                  /* per-column CSV FNN flags */

    /* these are just for error messages, see CopyFromErrorCallback */
    int cur_lineno = 0;                   /* line number for error messages */
    int fieldno = 0;
    bool file_has_oids;
    bool volatile_defexprs;                 /* is any of defexprs volatile? */

#define RAW_BUF_SIZE 65536 /* we palloc RAW_BUF_SIZE+1 bytes */
    const char *cur_eol = "";
} CopyStateData;

void copy_state_data_init(CopyStateData* cstate)
{
    cstate->oids = false;
    cstate->delim = 0;
}

#endif /* COPY_STATE_DATA_H */
