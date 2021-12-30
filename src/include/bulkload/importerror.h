/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * importerror.h
 *
 * 
 * 
 * IDENTIFICATION
 *        src/include/bulkload/importerror.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMPORTERROR_H
#define IMPORTERROR_H

#include "access/tupdesc.h"
#include "commands/gds_stream.h"
#include "storage/smgr/fd.h"

typedef struct CopyStateData* CopyState;

struct ErrLogInfo {
    /* SMP ID */
    uint32 smp_id;

    /*
     * T, responsibility for unlinking local cache file.
     * see also FD_ERRTBL_LOG_OWNER flag.
     */
    bool unlink_owner;
};

class BaseError : public BaseObject {
public:
    BaseError()
    {
        m_desc = NULL;
        Reset();
    }
    virtual ~BaseError()
    {
    }

    virtual void Serialize(StringInfo buf);

    virtual void Deserialize(StringInfo buf);

    virtual void serializeMaxNumOfValue(StringInfo buf) = 0;

    void Reset();

    static const int StartTimeIdx = 1;
    static const int FileNameIdx = 2;
    static const int LineNOIdx = 3;
    static const int RawDataIdx = 4;
    static const int DetailIdx = 5;
    static const int MaxNumOfValue = 6;

    Datum m_values[MaxNumOfValue];
    bool m_isNull[MaxNumOfValue];
    TupleDesc m_desc;
};

class ImportError : public BaseError {
public:
    ImportError()
    {
    }
    ~ImportError()
    {
    }

    void serializeMaxNumOfValue(StringInfo buf) override;

    static const int NodeIdIdx = 0;
};

/* ImportError for Copy */
class CopyError : public BaseError {
public:
    CopyError()
    {
    }
    ~CopyError()
    {
    }

    void serializeMaxNumOfValue(StringInfo buf) override;

    static const int RelNameIdx = 0;
};

class BaseErrorLogger : public BaseObject {
public:
    BaseErrorLogger() : m_memCxt(NULL), m_errDesc(NULL), m_buffer(NULL), m_fd(-1), m_offset(0)
    {
    }
    virtual ~BaseErrorLogger()
    {
    }

    virtual void Destroy() = 0;
    virtual int FetchError(BaseError* edata);
    virtual void SaveError(BaseError* edata);

    MemoryContext m_memCxt;
    TupleDesc m_errDesc;

protected:
    /* for LocalErrorLogger and CopyErrorLogger */
    StringInfo m_buffer;
    File m_fd;
    Size m_offset;
};

class ImportErrorLogger : public BaseErrorLogger {
public:
    ImportErrorLogger()
    {
    }
    virtual ~ImportErrorLogger()
    {
    }

    virtual void Destroy() override;

    virtual void Initialize(const void* output, TupleDesc errDesc, ErrLogInfo& errInfo);
    virtual void FormError(CopyState cstate, Datum begintime, ImportError* edata){};
};

class GDSErrorLogger : public ImportErrorLogger {
public:
    GDSErrorLogger() : m_output(NULL), m_name(NULL)
    {
    }
    ~GDSErrorLogger()
    {
    }

    void Initialize(const void* output, TupleDesc errDesc, ErrLogInfo& errInfo) override;
    void Destroy() override;
    /* GDSErrorLogger doesn't support FetchError method */
    int FetchError(BaseError* edata) override;
    void SaveError(BaseError* edata) override;
    void FormError(CopyState cstate, Datum begintime, ImportError* edata) override;

private:
    GDSStream* m_output;
    char* m_name;
#ifdef USE_ASSERT_CHECKING
    int m_counter;
#endif
};

class LocalErrorLogger : public ImportErrorLogger {
public:
    LocalErrorLogger()
    {
    }
    ~LocalErrorLogger()
    {
    }

    void Initialize(const void* output, TupleDesc errDesc, ErrLogInfo& errInfo) override;
    void Destroy() override;
    void FormError(CopyState cstate, Datum begintime, ImportError* edata) override;
};

/* Error logger for copy */
class CopyErrorLogger : public BaseErrorLogger {
public:
    CopyErrorLogger() : m_namespace(NULL)
    {
    }
    ~CopyErrorLogger()
    {
    }

    void Destroy() override;

    void Initialize(CopyState cstate);
    void Reset();
    void FormError(CopyState cstate, Datum begintime, CopyError* edata);
    void FormWhenLog(CopyState cstate, Datum begintime, CopyError *edata);
    
    char* m_namespace;
};

extern char* generate_unique_cache_name_prefix(Oid oid, uint32 distSessionKey);
extern char* generate_unique_cache_name_prefix(const char* relname);
extern void unlink_local_cache_file(const char* prefix, const uint32 smpId);

#endif
