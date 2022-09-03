/* -------------------------------------------------------------------------
 *
 * remotecopy.h
 *		Routines for extension of COPY command for cluster management
 *
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *		src/include/pgxc/remotecopy.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef REMOTECOPY_H
#define REMOTECOPY_H

#include "nodes/parsenodes.h"
#include "commands/formatter.h"

/*
 * This contains the set of data necessary for remote COPY control.
 */
typedef struct RemoteCopyData {
    /* COPY FROM/TO? */
    bool is_from;

    /*
     * On Coordinator we need to rewrite query.
     * While client may submit a copy command dealing with file, Datanodes
     * always send/receive data to/from the Coordinator. So we can not use
     * original statement and should rewrite statement, specifing STDIN/STDOUT
     * as copy source or destination
     */
    StringInfoData query_buf;

    /* Execution nodes for COPY */
    ExecNodes* exec_nodes;

    /* Locator information */
    RelationLocInfo* rel_loc; /* the locator key */
    List* idx_dist_by_col;    /* index of the distributed by column */

    PGXCNodeHandle** connections; /* Involved Datanode connections */
} RemoteCopyData;

/*
 * List of all the options used for query deparse step
 * As CopyStateData stays private in copy.c and in order not to
 * make openGauss code too much intrusive in PostgreSQL code,
 * this intermediate structure is used primarily to generate remote
 * COPY queries based on deparsed options.
 */
typedef struct RemoteCopyOptions {
    bool rco_oids; /* include OIDs? */
    bool rco_freeze;
    bool rco_without_escaping;
    bool rco_ignore_extra_data;
    char* rco_delim;         /* column delimiter (must be 1 byte) */
    char* rco_null_print;    /* NULL marker string (server encoding!) */
    char* rco_quote;         /* CSV quote char (must be 1 byte) */
    char* rco_escape;        /* CSV escape char (must be 1 byte) */
    char* rco_eol;           /* user defined EOL string */
    List* rco_force_quote;   /* list of column names */
    List* rco_force_notnull; /* list of column names */
    Formatter* rco_formatter;
    FileFormat rco_format;
    int rco_eol_type;
    char* rco_date_format;             /* customed date format */
    char* rco_time_format;             /* customed time format */
    char* rco_timestamp_format;        /* customed timestamp format */
    char* rco_smalldatetime_format;    /* customed smalldatetime format */
    bool rco_compatible_illegal_chars; /* compatible illegal chars conversion flag */
    int rco_fill_missing_fields;       /* 0 off;1 Compatible with the original copy; -1 trailing nullcols */
    int rco_fixedEncoding;             /* copy encoding for fixed format */
    const char *transform_query_string;      /* source sql string for copy */
} RemoteCopyOptions;

extern void RemoteCopy_BuildStatement(
    RemoteCopyData* state, Relation rel, RemoteCopyOptions* options, List* attnamelist, List* attnums);
extern void RemoteCopy_GetRelationLoc(RemoteCopyData* state, Relation rel, List* attnums);
extern RemoteCopyOptions* makeRemoteCopyOptions(void);
extern void FreeRemoteCopyData(RemoteCopyData* state);
extern void FreeRemoteCopyOptions(RemoteCopyOptions* options);
#endif
