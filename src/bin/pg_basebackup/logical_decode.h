/* -------------------------------------------------------------------------
 *
 * logical_decode.h - bounds-checked decoders for parallel logical replication
 *                    streams. Extracted from pg_recvlogical.cpp so the same
 *                    code is exercised by the regression test under test/.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *       src/bin/pg_basebackup/logical_decode.h
 * -------------------------------------------------------------------------
 */
#ifndef PG_RECVLOGICAL_LOGICAL_DECODE_H
#define PG_RECVLOGICAL_LOGICAL_DECODE_H

#include "postgres_fe.h"
#include "libpq/pqexpbuffer.h"

#include <stddef.h>

/*
 * All decoders return true on success and false on any malformed or truncated
 * record. The server-supplied length fields embedded in each chunk MUST be
 * validated against `payload_len` before they are used to advance `*curpos`
 * or to read from the stream buffer.
 *
 * When any decoder returns false, the caller must discard the partially
 * populated `res` buffer rather than emit it -- otherwise the out-of-bounds
 * bytes that triggered the rejection may be leaked through the output path.
 */

bool ResolveTuple(const char* stream, uint32* curpos, size_t payload_len,
                  PQExpBuffer res, bool newTup);

bool DMLToText(const char* stream, uint32* curPos, size_t payload_len,
               PQExpBuffer res);

bool BeginToText(const char* stream, uint32* curPos, size_t payload_len,
                 PQExpBuffer res);

bool CommitToText(const char* stream, uint32* curPos, size_t payload_len,
                  PQExpBuffer res);

bool StreamToText(const char* stream, size_t payload_len, PQExpBuffer res);

bool BatchStreamToText(const char* stream, size_t payload_len, PQExpBuffer res);

#endif /* PG_RECVLOGICAL_LOGICAL_DECODE_H */
