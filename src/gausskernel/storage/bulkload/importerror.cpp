/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * ---------------------------------------------------------------------------------------
 *
 *  importerror.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/bulkload/importerror.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "bulkload/importerror.h"
#include "utils/memutils.h"
#include "commands/copy.h"
#include "libpq/pqformat.h"
#include "optimizer/streamplan.h"

/* As long as it is bigger than 2*NAMEDATALEN +2 */
#define MAX_NAME_LEN 512
#define UNSIGNED_MINUS_ONE 4294967295

IdGen gt_copyId = {0, 0, false};

/*
 * @Description: generate the unique name prefix of local cache file
 * @IN distSessionKey: session key
 * @IN oid: OID of error table
 * @Return: cache name prefix
 * @See also:
 */
char *generate_unique_cache_name_prefix(Oid oid, uint32 distSessionKey)
{
    char tmpname[MAXPGPATH];
    int rc = snprintf_s(tmpname, sizeof(tmpname), sizeof(tmpname) - 1, "%u.%u.cache", oid, distSessionKey);
    securec_check_ss(rc, "", "");
    tmpname[MAXPGPATH - 1] = '\0';
    return pstrdup(tmpname);
}

char *generate_unique_cache_name_prefix(const char *relname)
{
    char tmpname[MAXPGPATH];
    uint32 copyId = generate_unique_id(&gt_copyId);
    ;
    int rc = snprintf_s(tmpname, sizeof(tmpname), sizeof(tmpname) - 1, "%s.%u.%lu.cache", relname, copyId,
                        t_thrd.proc_cxt.MyProcPid);
    securec_check_ss(rc, "", "");
    tmpname[MAXPGPATH - 1] = '\0';
    return pstrdup(tmpname);
}

/*
 * @Description: generate the unique name of local cache file for each importing thread
 * @IN prefix: name prefix returned by generate_unique_cache_name_prefix()
 * @IN smpId: SMP ID for cache file name suffix
 * @OUT outbuf: local cache file name
 * @Return: local cache file name for this thread
 * @See also:
 */
static inline void generate_local_cache_file(const char *prefix, const uint32 smpId, char *outbuf)
{
    int rc = snprintf_s(outbuf, MAXPGPATH, MAXPGPATH - 1, "%s.%u", prefix, smpId);
    securec_check_ss(rc, "", "");
    outbuf[MAXPGPATH - 1] = '\0';
}

/*
 * @Description: unlink local cache file
 * @IN prefix: name prefix returned by generate_unique_cache_name_prefix()
 * @IN smpId: SMP ID for cache file name suffix
 * @See also: ErrLogInfo struct
 */
void unlink_local_cache_file(const char *prefix, const uint32 smpId)
{
    char localfile[MAXPGPATH];
    generate_local_cache_file(prefix, smpId, localfile);
    UnlinkCacheFile(localfile);
}

void BaseError::Serialize(StringInfo buf)
{
    Form_pg_attribute *attrs = m_desc->attrs;
    int natts = m_desc->natts;

    Assert(m_desc->natts == MaxNumOfValue);
    serializeMaxNumOfValue(buf);

    for (int i = 0; i < natts; ++i) {
        Datum attr = m_values[i];
        if (m_isNull[i]) {
            pq_sendint32(buf, UNSIGNED_MINUS_ONE);
            continue;
        }
        if (attrs[i]->attlen > 0 && attrs[i]->attlen <= 8) {
            pq_sendint32(buf, attrs[i]->attlen);
            pq_sendbytes(buf, (char *)&attr, attrs[i]->attlen);
        } else if (attrs[i]->attlen > 8) {
            pq_sendint32(buf, attrs[i]->attlen);
            pq_sendbytes(buf, DatumGetPointer(attr), attrs[i]->attlen);
        } else {
            pq_sendint32(buf, VARSIZE(attr) - VARHDRSZ);
            pq_sendbytes(buf, VARDATA(attr), VARSIZE(attr) - VARHDRSZ);
        }
    }
}

void BaseError::Deserialize(StringInfo buf)
{
    int natts;
    Form_pg_attribute *attrs = m_desc->attrs;
    natts = pq_getmsgint(buf, 2);
    if (natts != MaxNumOfValue || natts != m_desc->natts) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Found invalid error recored")));
    }
    for (int i = 0; i < natts; ++i) {
        int len = pq_getmsgint(buf, 4);
        if (len == -1) {
            m_isNull[i] = true;
            m_values[i] = (Datum)0;
            continue;
        }

        if (unlikely(len < 0)) {
            ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                     errmsg("Found invalid error recored: negative length that is not -1.")));
        }
        m_isNull[i] = false;
        if (attrs[i]->attlen > 0 && attrs[i]->attlen <= 8) {
            if (unlikely(len != m_desc->attrs[i]->attlen)) {
                ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                         errmsg("Found invalid error recored: length is not the same as the attribute length.")));
            }
            pq_copymsgbytes(buf, (char*)&m_values[i], len);
        } else if (attrs[i]->attlen > 8) {
            if (unlikely(len != m_desc->attrs[i]->attlen)) {
                ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                         errmsg("Found invalid error recored: length is not the same as the attribute length.")));
            }
            m_values[i] = (Datum)palloc(len);
            pq_copymsgbytes(buf, DatumGetPointer(m_values[i]), len);
        } else {
            m_values[i] = (Datum)palloc(VARHDRSZ + len);
            SET_VARSIZE(m_values[i], VARHDRSZ + len);
            pq_copymsgbytes(buf, VARDATA(m_values[i]), len);
        }
    }
}

void BaseError::Reset()
{
    errno_t rc;
    rc = memset_s(m_isNull, sizeof(bool) * MaxNumOfValue, 0, sizeof(bool) * MaxNumOfValue);
    securec_check(rc, "", "");
    rc = memset_s(m_values, sizeof(Datum) * MaxNumOfValue, 0, sizeof(Datum) * MaxNumOfValue);
    securec_check(rc, "", "");
}

void ImportError::serializeMaxNumOfValue(StringInfo buf)
{
    pq_sendint16(buf, MaxNumOfValue);
}

void CopyError::serializeMaxNumOfValue(StringInfo buf)
{
    pq_sendint(buf, MaxNumOfValue, 2);
}

int BaseErrorLogger::FetchError(BaseError *edata)
{
    AutoContextSwitch memGuard(m_memCxt);
    int nread = 0;
    uint32 len = 0;
    Assert(m_buffer != NULL && edata != NULL);
    MemoryContextReset(m_memCxt);
    resetStringInfo(m_buffer);

    nread = FilePRead(m_fd, (char *)&len, 4, m_offset);
    if (nread == 0) {
        return EOF;
    } else if (nread < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fetch error record:%m")));
    } else if (nread < 4) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fetch expected length:%m")));
    }

    m_offset += 4;
    len = ntohl(len);
    enlargeStringInfo(m_buffer, len + 1);

    while ((uint32)m_buffer->len < len) {
        nread = FilePRead(m_fd, m_buffer->data + m_buffer->len, len - m_buffer->len, m_offset);
        if (nread == 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("incomplete error record")));
        } else if (nread < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not fetch error record:%m")));
        }
        m_buffer->len += nread;
        m_offset += nread;
    }

    m_buffer->data[m_buffer->len] = '\0';
    edata->Deserialize(m_buffer);
    return 0;
}

void BaseErrorLogger::SaveError(BaseError *edata)
{
    int nwrite = 0;
    int len = 0;

    Assert(m_buffer != NULL && edata != NULL);
    resetStringInfo(m_buffer);
    appendStringInfoSpaces(m_buffer, 4);
    edata->Serialize(m_buffer);
    *((uint32 *)m_buffer->data) = htonl((uint32)(m_buffer->len - 4));

    while (len < m_buffer->len) {
        nwrite = FilePWrite(m_fd, m_buffer->data + len, m_buffer->len - len, m_offset);
        if (nwrite == 0 || nwrite < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not cache error info:%m")));
        }
        len += nwrite;
        m_offset += nwrite;
    }
}

void ImportErrorLogger::Initialize(const void *output, TupleDesc errDesc, ErrLogInfo &errInfo)
{
    m_memCxt = AllocSetContextCreate(CurrentMemoryContext, "Import Error Context", ALLOCSET_DEFAULT_MINSIZE,
                                     ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_errDesc = errDesc;
    (void)errInfo;
}

void ImportErrorLogger::Destroy()
{
    MemoryContextDelete(m_memCxt);
    m_errDesc = NULL;
}

void GDSErrorLogger::Initialize(const void *output, TupleDesc errDesc, ErrLogInfo &errInfo)
{
    m_output = (GDSStream *)output;
    m_name = pstrdup((char *)errDesc);
    ImportErrorLogger::Initialize(output, NULL, errInfo);
#ifdef USE_ASSERT_CHECKING
    m_counter = 0;
#endif
}

void GDSErrorLogger::Destroy()
{
#ifdef USE_ASSERT_CHECKING
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("Error Log send %d lines", m_counter)));
#endif
    ImportErrorLogger::Destroy();
}

/**
 * GDSErrorLogger doesn't support FetchError method
 */
int GDSErrorLogger::FetchError(BaseError *edata)
{
    return 0;
}

void GDSErrorLogger::SaveError(BaseError *edata)
{
    CmdRemoteLog dat;
    Datum rawdata;

    Assert(edata != NULL);
    if (edata->m_isNull[BaseError::RawDataIdx])
        return;

    rawdata = edata->m_values[BaseError::RawDataIdx];
    dat.m_type = CMD_TYPE_REMOTELOG;
    dat.m_datasize = VARSIZE_ANY_EXHDR(DatumGetPointer(rawdata));
    dat.m_data = VARDATA_ANY(DatumGetPointer(rawdata));
    dat.m_name = pstrdup(m_name);
    SerializeCmd(&dat, m_output->m_outBuf);
    m_output->Flush();
#ifdef USE_ASSERT_CHECKING
    ++m_counter;
#endif
}

void GDSErrorLogger::FormError(CopyState cstate, Datum begintime, ImportError *edata)
{
    Assert(edata != NULL);

    edata->Reset();
    errno_t rc;
    rc = memset_s(edata->m_isNull, sizeof(bool) * ImportError::MaxNumOfValue, 1,
                  sizeof(bool) * ImportError::MaxNumOfValue);
    securec_check(rc, "", "");

    if (cstate->line_buf.len > 0) {
        int len = cstate->line_buf.len + VARHDRSZ + 1;
        edata->m_values[ImportError::RawDataIdx] = (Datum)palloc(len);
        SET_VARSIZE(edata->m_values[ImportError::RawDataIdx], len);
        rc = memcpy_s(((char *)edata->m_values[ImportError::RawDataIdx]) + VARHDRSZ, len - VARHDRSZ,
                      cstate->line_buf.data, cstate->line_buf.len);
        securec_check(rc, "", "");
        ((char *)edata->m_values[ImportError::RawDataIdx])[len - 1] = '\n';
        edata->m_isNull[ImportError::RawDataIdx] = false;
    }
}

void LocalErrorLogger::Initialize(const void *filename, TupleDesc errDesc, ErrLogInfo &errInfo)
{
    char cache_file[MAXPGPATH];
    generate_local_cache_file((const char *)filename, errInfo.smp_id, cache_file);

    m_fd = OpenCacheFile(cache_file, errInfo.unlink_owner);
    m_buffer = makeStringInfo();
    ImportErrorLogger::Initialize(cache_file, errDesc, errInfo);
}

void LocalErrorLogger::Destroy()
{
    ImportErrorLogger::Destroy();
    if (m_buffer != NULL) {
        if (m_buffer->data != NULL)
            pfree(m_buffer->data);
        pfree(m_buffer);
        m_buffer = NULL;
    }

    /*
     * Important:
     *   DON'T delete physical file about local cache file.
     *   INSERT node will read its data and remove it.
     */
    FileClose(m_fd);
}

void LocalErrorLogger::FormError(CopyState cstate, Datum begintime, ImportError *ierror)
{
    ErrorData *edata = NULL;
    int len = 0;
    const char *detail = NULL;
    int sqlerrcode;
    errno_t rc;

    Assert(ierror != NULL);
    edata = CopyErrorData();
    detail = edata->message;
    sqlerrcode = edata->sqlerrcode;
    ierror->Reset();
    ierror->m_desc = m_errDesc;
    ierror->m_values[ImportError::NodeIdIdx] = u_sess->pgxc_cxt.PGXCNodeId;
    ierror->m_values[ImportError::StartTimeIdx] = begintime;
    len = strlen(cstate->filename) + VARHDRSZ;
    ierror->m_values[ImportError::FileNameIdx] = (Datum)palloc(len);
    SET_VARSIZE(ierror->m_values[ImportError::FileNameIdx], len);
    rc = memcpy_s(((char *)ierror->m_values[ImportError::FileNameIdx]) + VARHDRSZ, len - VARHDRSZ, cstate->filename,
                  len - VARHDRSZ);
    securec_check(rc, "", "");
    ierror->m_values[ImportError::LineNOIdx] = cstate->cur_lineno;

    if (cstate->line_buf.len > 0 && sqlerrcode != ERRCODE_CHARACTER_NOT_IN_REPERTOIRE &&
        sqlerrcode != ERRCODE_UNTRANSLATABLE_CHARACTER) {
        char *rawDataVal = NULL;
        int rawDataValLen = 0;
        rawDataVal = limit_printout_length(cstate->line_buf.data);
        rawDataValLen = strlen(rawDataVal);
        len = cstate->line_buf.len + VARHDRSZ;
        ierror->m_values[ImportError::RawDataIdx] = (Datum)palloc(len);
        SET_VARSIZE(ierror->m_values[ImportError::RawDataIdx], len);
        rc = memcpy_s(((char *)ierror->m_values[ImportError::RawDataIdx]) + VARHDRSZ, len - VARHDRSZ,
                      cstate->line_buf.data, rawDataValLen);
        securec_check(rc, "", "");
    } else
        ierror->m_isNull[ImportError::RawDataIdx] = true;
    if (detail != NULL) {
        char *detailVal = limit_printout_length(detail);
        int leng = strlen(detailVal);
        ierror->m_values[ImportError::DetailIdx] = (Datum)palloc(leng + VARHDRSZ);
        SET_VARSIZE(ierror->m_values[ImportError::DetailIdx], leng + VARHDRSZ);
        rc = memcpy_s(((char *)ierror->m_values[ImportError::DetailIdx]) + VARHDRSZ, leng, detail, leng);
        securec_check(rc, "", "");
    } else
        ierror->m_isNull[ImportError::DetailIdx] = true;
}

void CopyErrorLogger::Initialize(CopyState cstate)
{
    /* Get ourselves a cache file */
    char *cache_file = generate_unique_cache_name_prefix(RelationGetRelationName(cstate->rel));

    m_fd = OpenCacheFile(cache_file, true);
    m_buffer = makeStringInfo();

    m_memCxt = AllocSetContextCreate(CurrentMemoryContext, "Copy Import Error Context", ALLOCSET_DEFAULT_MINSIZE,
                                     ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_errDesc = RelationGetDescr(cstate->err_table);
}

void CopyErrorLogger::Reset()
{
    resetStringInfo(m_buffer);
    MemoryContextReset(m_memCxt);
    m_offset = 0;
}

void CopyErrorLogger::Destroy()
{
    MemoryContextDelete(m_memCxt);
    m_errDesc = NULL;
    if (m_buffer != NULL) {
        if (m_buffer->data != NULL)
            pfree(m_buffer->data);
        pfree(m_buffer);
        m_buffer = NULL;
    }

    // This actually close the fd cause we chose "true" upon opening this cache file.
    if (m_fd) {
        FileClose(m_fd);
        m_fd = -1;
    }
}

void CopyErrorLogger::FormError(CopyState cstate, Datum begintime, CopyError *ierror)
{
    ErrorData *edata = NULL;
    int len = 0;
    const char *detail = NULL;
    char *relname = NULL;
    int sqlerrcode;
    errno_t rc;
    char *source = NULL;

    Assert(ierror != NULL);
    edata = CopyErrorData();
    detail = edata->message;
    sqlerrcode = edata->sqlerrcode;
    ierror->Reset();

    ierror->m_desc = m_errDesc;

    relname = (char *)palloc(MAX_NAME_LEN);
    rc = snprintf_s(relname, MAX_NAME_LEN, MAX_NAME_LEN - 1, "%s.%s", cstate->logger->m_namespace,
                    RelationGetRelationName(cstate->rel));
    securec_check_ss(rc, "\0", "\0");

    len = strlen(relname) + VARHDRSZ;
    ierror->m_values[CopyError::RelNameIdx] = (Datum)palloc(len);
    SET_VARSIZE(ierror->m_values[CopyError::RelNameIdx], len);
    rc = memcpy_s(((char *)ierror->m_values[CopyError::RelNameIdx]) + VARHDRSZ, len - VARHDRSZ, relname,
                  len - VARHDRSZ);
    securec_check_c(rc, "\0", "\0");

    ierror->m_values[CopyError::StartTimeIdx] = begintime;

    if (cstate->filename)
        source = cstate->filename;
    else {
        Assert(cstate->copy_dest == COPY_NEW_FE);
        source = "STDIN";
    }

    len = strlen(source) + VARHDRSZ;
    ierror->m_values[CopyError::FileNameIdx] = (Datum)palloc(len);
    SET_VARSIZE(ierror->m_values[CopyError::FileNameIdx], len);
    rc = memcpy_s(((char *)ierror->m_values[CopyError::FileNameIdx]) + VARHDRSZ, len - VARHDRSZ, source,
                  len - VARHDRSZ);
    securec_check_c(rc, "\0", "\0");

    ierror->m_values[CopyError::LineNOIdx] = cstate->cur_lineno;

    /* save the raw data here  */
    if (cstate->line_buf.len > 0 && sqlerrcode != ERRCODE_CHARACTER_NOT_IN_REPERTOIRE &&
        sqlerrcode != ERRCODE_UNTRANSLATABLE_CHARACTER && cstate->logErrorsData) {
        char *rawDataVal = NULL;
        int rawDataValLen = 0;
        rawDataVal = limit_printout_length(cstate->line_buf.data);
        rawDataValLen = strlen(rawDataVal);
        len = rawDataValLen + VARHDRSZ;
        ierror->m_values[CopyError::RawDataIdx] = (Datum)palloc(len);
        SET_VARSIZE(ierror->m_values[CopyError::RawDataIdx], len);
        rc = memcpy_s(((char *)ierror->m_values[CopyError::RawDataIdx]) + VARHDRSZ, len - VARHDRSZ, rawDataVal,
                      rawDataValLen);
        securec_check(rc, "", "");
    } else {
        ierror->m_isNull[CopyError::RawDataIdx] = true;
    }

    if (detail != NULL) {
        char *detailVal = limit_printout_length(detail);
        int leng = strlen(detailVal);
        ierror->m_values[CopyError::DetailIdx] = (Datum)palloc(leng + VARHDRSZ);
        SET_VARSIZE(ierror->m_values[CopyError::DetailIdx], leng + VARHDRSZ);
        rc = memcpy_s(((char *)ierror->m_values[CopyError::DetailIdx]) + VARHDRSZ, leng, detail, leng);
        securec_check_c(rc, "\0", "\0");
    } else
        ierror->m_isNull[CopyError::DetailIdx] = true;

    pfree(relname);
}

void CopyErrorLogger::FormWhenLog(CopyState cstate, Datum begintime, CopyError *ierror)
{
    const char *detail = NULL;
    char *relname = NULL;
    errno_t rc;
    char *source = NULL;

    Assert(ierror != NULL);
    detail = "COPY_WHEN_ROWS";
    ierror->Reset();

    ierror->m_desc = m_errDesc;

    relname = (char *)palloc(MAX_NAME_LEN);
    rc = snprintf_s(relname, MAX_NAME_LEN, MAX_NAME_LEN - 1, "%s.%s", cstate->logger->m_namespace,
                    RelationGetRelationName(cstate->rel));
    securec_check_ss(rc, "\0", "\0");

    ierror->m_values[CopyError::RelNameIdx] = PointerGetDatum(cstring_to_text_with_len(relname, strlen(relname)));
    ierror->m_values[CopyError::StartTimeIdx] = begintime;

    if (cstate->filename)
        source = cstate->filename;
    else {
        Assert(cstate->copy_dest == COPY_NEW_FE);
        source = "STDIN";
    }

    ierror->m_values[CopyError::FileNameIdx] = PointerGetDatum(cstring_to_text_with_len(source, strlen(source)));
    ierror->m_values[CopyError::LineNOIdx] = cstate->cur_lineno;

    /* save the raw data here  */
    if (cstate->line_buf.len > 0 && cstate->logErrorsData) {
        char *rawDataVal = NULL;
        rawDataVal = limit_printout_length(cstate->line_buf.data);
        ierror->m_values[CopyError::RawDataIdx] =
            PointerGetDatum(cstring_to_text_with_len(rawDataVal, strlen(rawDataVal)));
        pfree(rawDataVal);
    } else {
        ierror->m_isNull[CopyError::RawDataIdx] = true;
    }

    if (detail != NULL) {
        ierror->m_values[CopyError::DetailIdx] = PointerGetDatum(cstring_to_text_with_len(detail, strlen(detail)));
    } else {
        ierror->m_isNull[CopyError::DetailIdx] = true;
    }

    pfree(relname);
}
