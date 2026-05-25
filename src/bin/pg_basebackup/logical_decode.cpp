/* -------------------------------------------------------------------------
 *
 * logical_decode.cpp - bounds-checked decoders for parallel logical
 *                       replication streams produced by openGauss's
 *                       parallel_decode plugin.
 *
 * These functions are linked into pg_recvlogical and into the regression
 * test under src/bin/pg_basebackup/test/. Every length field embedded in
 * the stream is treated as untrusted input: the decoders validate every
 * read against the payload boundary supplied by the caller and reject
 * malformed records by returning false.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *       src/bin/pg_basebackup/logical_decode.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlog_internal.h"
#include "libpq/pqexpbuffer.h"

#include "logical_decode.h"

static const uint64 upperPart = 32;

/* A 64-bit value (CSN / LSN / xid) is encoded as two big-endian uint32 halves in the stream. */
static const size_t UINT64_PAIR_SIZE = sizeof(uint32) * 2;

/* Begin message fixed header: CSN (uint64) + first_lsn (uint64). */
static const size_t BEGIN_FIXED_HEADER_SIZE = UINT64_PAIR_SIZE * 2;

/*
 * Returns true if at least `need` bytes remain in the payload starting at
 * curpos. Server-supplied length fields must be validated against this
 * before they are used to advance curpos or to read from the stream buffer.
 */
static inline bool HasRemaining(uint32 curpos, size_t payload_len, size_t need)
{
    return curpos <= payload_len && (payload_len - curpos) >= need;
}

/*
 * append comma as seperator
 */
static inline void AppendSeperator(PQExpBuffer res, uint16 attr, uint16 maxAttr)
{
    if (attr < maxAttr - 1) {
        appendPQExpBufferStr(res, ", ");
    }
}

/*
 * decode binary style tuple to text
 *
 * Returns false on any malformed/oversized length field; callers must then
 * discard the entire message rather than emit the partially-decoded buffer.
 */
bool ResolveTuple(const char* stream, uint32* curpos, size_t payload_len,
                  PQExpBuffer res, bool newTup)
{
    if (!HasRemaining(*curpos, payload_len, sizeof(uint16))) {
        return false;
    }
    uint16 attrnum = ntohs(*(uint16 *)(stream + *curpos));
    *curpos += sizeof(attrnum);
    if (newTup) {
        appendPQExpBufferStr(res, "new_tuple: {");
    } else {
        appendPQExpBufferStr(res, "old_value: {");
    }
    for (uint16 i = 0; i < attrnum; i++) {
        if (!HasRemaining(*curpos, payload_len, sizeof(uint16))) {
            return false;
        }
        uint16 colLen = ntohs(*(uint16 *)(stream + *curpos));
        *curpos += sizeof(colLen);
        if (colLen == 0 || !HasRemaining(*curpos, payload_len, colLen)) {
            return false;
        }
        appendBinaryPQExpBuffer(res, stream + *curpos, colLen);
        *curpos += colLen;
        if (!HasRemaining(*curpos, payload_len, sizeof(Oid) + sizeof(uint32))) {
            return false;
        }
        Oid typid = ntohl(*(Oid *)(stream + *curpos));
        *curpos += sizeof(Oid);
        appendPQExpBuffer(res, "[typid = %u]: ", typid);

        uint32 dataLen = ntohl(*(uint32 *)(stream + *curpos));
        *curpos += sizeof(dataLen);
        const uint32 nullTag = 0XFFFFFFFF;
        if (dataLen == nullTag) {
            appendPQExpBufferStr(res, "\"NULL\"");
        } else if (dataLen == 0) {
            appendPQExpBufferStr(res, "\"\"");
        } else {
            if (!HasRemaining(*curpos, payload_len, dataLen)) {
                return false;
            }
            appendPQExpBufferChar(res, '\"');
            appendBinaryPQExpBuffer(res, stream + *curpos, dataLen);
            appendPQExpBufferChar(res, '\"');
            *curpos += dataLen;
        }
        AppendSeperator(res, i, attrnum);
    }
    appendPQExpBufferChar(res, '}');
    return true;
}

/*
 * decode binary style DML to text
 */
bool DMLToText(const char* stream, uint32 *curPos, size_t payload_len, PQExpBuffer res)
{
    if (!HasRemaining(*curPos, payload_len, 1)) {
        return false;
    }
    char dtype = stream[*curPos];
    if (dtype == 'I') {
        appendPQExpBufferStr(res, "INSERT INTO ");
    } else if (dtype == 'U') {
        appendPQExpBufferStr(res, "UPDATE ");
    } else if (dtype == 'D') {
        appendPQExpBufferStr(res, "DELETE FROM ");
    } else {
        return false;
    }
    *curPos += 1;
    if (!HasRemaining(*curPos, payload_len, sizeof(uint16))) {
        return false;
    }
    uint16 schemaLen = ntohs(*(uint16 *)(stream + *curPos));
    *curPos += sizeof(schemaLen);
    if (!HasRemaining(*curPos, payload_len, schemaLen)) {
        return false;
    }
    appendBinaryPQExpBuffer(res, stream + *curPos, schemaLen);
    *curPos += schemaLen;
    appendPQExpBufferChar(res, '.');
    if (!HasRemaining(*curPos, payload_len, sizeof(uint16))) {
        return false;
    }
    uint16 tableLen = ntohs(*(uint16 *)(stream + *curPos));
    *curPos += sizeof(tableLen);
    if (!HasRemaining(*curPos, payload_len, tableLen)) {
        return false;
    }
    appendBinaryPQExpBuffer(res, stream + *curPos, tableLen);
    *curPos += tableLen;

    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'N') {
        *curPos += 1;
        appendPQExpBufferChar(res, ' ');
        if (!ResolveTuple(stream, curPos, payload_len, res, true)) {
            return false;
        }
    }
    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'O') {
        *curPos += 1;
        appendPQExpBufferChar(res, ' ');
        if (!ResolveTuple(stream, curPos, payload_len, res, false)) {
            return false;
        }
    }
    return true;
}

/*
 * decode binary style begin message to text
 */
bool BeginToText(const char* stream, uint32 *curPos, size_t payload_len, PQExpBuffer res)
{
    if (!HasRemaining(*curPos, payload_len, 1)) {
        return false;
    }
    appendPQExpBufferStr(res, "BEGIN ");
    *curPos += 1;
    if (!HasRemaining(*curPos, payload_len, BEGIN_FIXED_HEADER_SIZE)) {
        return false;
    }
    uint32 CSNupper = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(CSNupper);
    uint32 CSNlower = ntohl(*(uint32 *)(&stream[*curPos]));
    uint64 CSN = ((uint64)(CSNupper) << upperPart) + CSNlower;
    *curPos += sizeof(CSNlower);
    appendPQExpBuffer(res, "CSN: %lu ", CSN);

    uint32 LSNupper = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(LSNupper);
    uint32 LSNlower = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(LSNlower);
    appendPQExpBuffer(res, "first_lsn: %X/%X", LSNupper, LSNlower);

    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'X') {
        *curPos += 1;
        if (!HasRemaining(*curPos, payload_len, UINT64_PAIR_SIZE)) {
            return false;
        }
        uint32 xidupper = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidupper);
        uint32 xidlower = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidlower);
        uint64 xid = ((uint64)(xidupper) << upperPart) + xidlower;
        appendPQExpBuffer(res, " xid: %lu", xid);
    }

    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'T') {
        *curPos += 1;
        if (!HasRemaining(*curPos, payload_len, sizeof(uint32))) {
            return false;
        }
        uint32 timeLen = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        if (!HasRemaining(*curPos, payload_len, timeLen)) {
            return false;
        }
        appendPQExpBufferStr(res, " commit_time: ");
        appendBinaryPQExpBuffer(res, &stream[*curPos], timeLen);
        *curPos += timeLen;
    }
    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'O') {
        *curPos += 1;
        if (!HasRemaining(*curPos, payload_len, sizeof(uint32))) {
            return false;
        }
        uint32 originid = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        appendPQExpBuffer(res, " origin_id: %d", originid);
    }
    return true;
}

/*
 * decode binary style commit message to text
 */
bool CommitToText(const char* stream, uint32 *curPos, size_t payload_len, PQExpBuffer res)
{
    if (!HasRemaining(*curPos, payload_len, 1)) {
        return false;
    }
    appendPQExpBufferStr(res, "COMMIT ");
    *curPos += 1;
    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'X') {
        *curPos += 1;
        if (!HasRemaining(*curPos, payload_len, UINT64_PAIR_SIZE)) {
            return false;
        }
        uint32 xidupper = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidupper);
        uint32 xidlower = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidlower);
        uint64 xid = ((uint64)(xidupper) << upperPart) + xidlower;
        appendPQExpBuffer(res, "xid: %lu", xid);
    }
    if (HasRemaining(*curPos, payload_len, 1) && stream[*curPos] == 'T') {
        *curPos += 1;
        if (!HasRemaining(*curPos, payload_len, sizeof(uint32))) {
            return false;
        }
        uint32 timeLen = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        if (!HasRemaining(*curPos, payload_len, timeLen)) {
            return false;
        }
        appendPQExpBufferStr(res, " commit_time: ");
        appendBinaryPQExpBuffer(res, &stream[*curPos], timeLen);
        *curPos += timeLen;
    }
    return true;
}

/*
 * decode binary style log stream to text
 *
 * Returns false on any malformed or truncated record; callers must then
 * abandon the partially-populated `res` buffer.
 */
bool StreamToText(const char* stream, size_t payload_len, PQExpBuffer res)
{
    uint32 pos = 0;
    for (;;) {
        if (!HasRemaining(pos, payload_len, sizeof(uint32))) {
            return false;
        }
        uint32 dmlLen = ntohl(*(uint32 *)(&stream[pos]));
        /* if this is the end of stream, return */
        if (dmlLen == 0) {
            return true;
        }
        pos += sizeof(dmlLen);
        if (!HasRemaining(pos, payload_len, UINT64_PAIR_SIZE)) {
            return false;
        }
        uint32 LSNupper = ntohl(*(uint32 *)(&stream[pos]));
        pos += sizeof(LSNupper);
        uint32 LSNlower = ntohl(*(uint32 *)(&stream[pos]));
        pos += sizeof(LSNlower);
        appendPQExpBuffer(res, "current_lsn: %X/%X ", LSNupper, LSNlower);
        if (!HasRemaining(pos, payload_len, 1)) {
            return false;
        }
        if (stream[pos] == 'B') {
            if (!BeginToText(stream, &pos, payload_len, res)) {
                return false;
            }
        } else if (stream[pos] == 'C') {
            if (!CommitToText(stream, &pos, payload_len, res)) {
                return false;
            }
        } else if (stream[pos] != 'P' && stream[pos] != 'F') {
            if (!DMLToText(stream, &pos, payload_len, res)) {
                return false;
            }
        }
        if (!HasRemaining(pos, payload_len, 1)) {
            return false;
        }
        if (stream[pos] == 'P') {
            pos++;
            appendPQExpBufferChar(res, '\n');
            /* loop and decode the next chunk in this buffer */
        } else if (stream[pos] == 'F') {
            appendPQExpBufferChar(res, '\n');
            return true;
        } else {
            /* Unknown trailer tag => malformed stream */
            return false;
        }
    }
}

/*
 * decode batch sending result stream to text.
 *
 * Returns false on any malformed or truncated record.
 */
bool BatchStreamToText(const char* stream, size_t payload_len, PQExpBuffer res)
{
    size_t pos = 0;
    for (;;) {
        if (!HasRemaining(pos, payload_len, sizeof(uint32))) {
            return false;
        }
        uint32 changeLen = ntohl(*(uint32 *)(&stream[pos]));
        /* if this is the end of stream, return */
        if (changeLen == 0) {
            return true;
        }
        if (changeLen < sizeof(XLogRecPtr)) {
            return false;
        }
        if (!HasRemaining(pos + sizeof(uint32), payload_len, changeLen)) {
            return false;
        }
        size_t dataPos = pos + sizeof(uint32) + sizeof(XLogRecPtr);
        appendBinaryPQExpBuffer(res, stream + dataPos, changeLen - sizeof(XLogRecPtr));
        appendPQExpBufferChar(res, '\n');
        pos += sizeof(uint32) + changeLen;
    }
}
