/*-------------------------------------------------------------------------
 *
 * pgoutput.h
 *		Logical Replication output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pgoutput.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGOUTPUT_H
#define PGOUTPUT_H

#include "replication/logical.h"
#include "utils/palloc.h"

typedef struct PGOutputData {
    PluginTestDecodingData common;

    /* client info */
    uint32 protocol_version;

    List *publication_names;
    List *publications;
    List *deleted_relids;
    bool binary;
} PGOutputData;

#endif /* PGOUTPUT_H */
