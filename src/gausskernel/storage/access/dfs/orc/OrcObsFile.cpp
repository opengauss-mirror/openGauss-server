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
 * OrcObsFile.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/OrcObsFile.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <string>
#include <sstream>
#include <fstream>

/* Becareful: liborc header file must before  openGauss header file */
#include "orc_rw.h"
#ifndef ENABLE_LITE_MODE
#include "orc/Adaptor.hh"
#include "orc/Exceptions.hh"
#endif
#include "OrcObsFile.h"
#include "access/obs/obs_am.h"
#include "storage/cucache_mgr.h"
#include "pgstat.h"
#include "utils/plog.h"

/* Becareful: using throw exception instead of ereport ERROR in this file */
namespace dfs {
class ObsFileInputStream : public GSInputStream {
public:
    explicit ObsFileInputStream(dfs::reader::ReaderState *_readerState)
        : m_filename(""), m_totalLength(0), m_handler(NULL), m_bucket(NULL), m_key(NULL)
    {
        readerState = _readerState;
        m_obsReadCalls = 0;
        m_cacheReadCalls = 0;
    }

    ~ObsFileInputStream()
    {
    }

    const OBSReadWriteHandler *getHandler() const
    {
        return m_handler;
    }

    /*
     * @Description: copy obs file input stream
     * @Return: copy of this object, some of member are share in copy object
     * @See also: ObsFileInputStream(const ObsFileInputStream& other)
     * @Important:  some of member are share in copy object
     */
    DFS_UNIQUE_PTR<GSInputStream> copy()
    {
        return DFS_UNIQUE_PTR<GSInputStream>(new ObsFileInputStream(*this));
    }

    /*
     * @Description: init obs file input stream
     * @IN/OUT handler:obs handler
     * @IN path: object path: bucket + key
     * @IN fileSize: file size
     * @See also:
     */
    void init(OBSReadWriteHandler *handler, const std::string &path, int64_t fileSize)
    {
        // using same handler in same connector
        Assert(handler);
        m_handler = handler;

        // get bucket and key
        FetchUrlPropertiesForQuery(path.c_str(), &m_bucket, &m_key);

        // we need m_filename in above lay function where m_prefix is invisible
        m_filename = m_key;

        // inti handler to read
        // description: think about  in parallel or smp , because the handler is share by same connector
        ObsReadWriteHandlerSetType(handler, OBS_READ);

        // check file length
        Assert(fileSize >= 0);
        if (fileSize == 0) {
            StringInfo err_msg = makeStringInfo();
            appendStringInfo(err_msg, "object size is 0, path = %s", path.c_str());

            orc::orclog(orc::ORC_ERROR, orc::PARSEERROR, err_msg->data);
        }
        m_totalLength = fileSize;

        ereport(DEBUG1, (errmodule(MOD_OBS),
                         errmsg("initialize OBS ORC InputStream, bucket name is %s, prefix is %s, size is %lu",
                                m_bucket, m_key, m_totalLength)));
    }

    /*
     * @Description:  get file size
     * @Return: file size
     * @See also:
     */
    uint64_t getLength() const
    {
        return m_totalLength;
    }

    /*
     * @Description: get natural read size
     * @Return: natural read size
     * @See also:
     */
    uint64_t getNaturalReadSize() const override
    {
        return NATURAL_READ_SIZE;
    }

    /*
     * @Description: read file to buffer
     * @IN buf: dest buffer
     * @IN length: read length
     * @IN offset: read offset
     * @See also:
     */
    void read(void *buf, uint64_t length, uint64_t offset) override
    {
        return readFile(buf, length, offset);
    }

    /*
     * function description: Directly read data file OBS file.
     * Take example, if we want read a OBS file named
     * obsfile1. We use obsfile1 [offset, length] reprensts the range
     * to be read. The three parameter filename\offset\length show
     * the unique coordinate in OBS storage dimension. Use the triple
     * load data and store into buffer.
     * @input buffer, represent the memory to store data
     * @input length, the bytes count to be read
     * @input offset, the file offset from where to read
     * @output NA
     */
    void readFile(void *buf, uint64_t length, uint64_t offset)
    {
        size_t readSize = 0;

        // description: think about  in parallel or smp , because the handler is shared by same connector
        m_handler->properties.get_cond.start_byte = offset;
        m_handler->m_object_info.key = m_key;
        m_handler->m_option.bucket_options.bucket_name = m_bucket;

        PROFILING_OBS_START();
        pgstat_report_waitevent(WAIT_EVENT_OBS_READ);
        readSize = read_bucket_object(m_handler, static_cast<char *>(buf), length);
        pgstat_report_waitevent(WAIT_EVENT_END);
        PROFILING_OBS_END_READ(readSize);

        ereport(DEBUG1,
                (errmodule(MOD_OBS), errmsg("read_bucket_object \"%s\", offset = %lu", m_filename.c_str(), offset)));

        // total counter
        ++m_obsReadCalls;

        if (readSize < length) {
            StringInfo err_msg = makeStringInfo();
            appendStringInfo(err_msg, "read object from obs failed, offset = %lu request = %lu, actual = %lu", offset,
                             length, readSize);

            orc::orclog(orc::ORC_ERROR, orc::PARSEERROR, err_msg->data);
        }

        readerState->orcDataLoadBlockCount++;
        readerState->orcDataLoadBlockSize += length;
    }

    /*
     * @Description: get file name
     * @Return: file name
     * @See also:
     */
    const std::string &getName() const override
    {
        return m_filename;
    }

    /*
     * @Description:  get read stat info
     * @IN localBlock:local block
     * @IN remoteBlock:remote block
     * @IN nnCalls:namenode call
     * @IN dnCalls:datanode call
     * @See also:
     */
    void getStat(uint64_t *localBlock, uint64_t *remoteBlock, uint64_t *nnCalls, uint64_t *dnCalls)
    {
        // obs read for remote read, cache read for local read
        *remoteBlock = m_obsReadCalls;
        *localBlock = m_cacheReadCalls;
    }

    void getLocalRemoteReadCnt(uint64_t *localReadCnt, uint64_t *remoteReadCnt)
    {
        *remoteReadCnt = m_obsReadCalls;

        *localReadCnt = m_cacheReadCalls;
    }

protected:
    ObsFileInputStream(const ObsFileInputStream &other) : GSInputStream(other)
    {
        m_filename = other.m_filename;
        m_totalLength = other.m_totalLength;
        m_handler = other.m_handler;
        m_bucket = other.m_bucket;
        m_key = other.m_key;
        readerState = other.readerState;

        /* performance counter set to zero */
        m_obsReadCalls = 0;
        m_cacheReadCalls = 0;
    }

protected:
    std::string m_filename;
    uint64_t m_totalLength;
    OBSReadWriteHandler *m_handler;
    /* think twice about this default size */
    const static uint64_t NATURAL_READ_SIZE = 1024 * 1024;
    char *m_bucket;
    char *m_key;

    /* performance counter */
    uint64_t m_obsReadCalls;
    uint64_t m_cacheReadCalls;
    dfs::reader::ReaderState *readerState;
};

/*
 * ObsCacheFileInputStream is decoupled from the file format.
 * We split OBS file into pieces and store them into local RAM
 * cache which is implemented in cucache_mgr file. The cache
 * take care our file splits. Take attenton, we said take care
 * our file splits not our orc/parquet/others data.
 */
class ObsCacheFileInputStream : public ObsFileInputStream {
public:
    /* constructor */
    explicit ObsCacheFileInputStream(dfs::reader::ReaderState *_readerState) : ObsFileInputStream(_readerState)
    {
        /* Mark the storage type here for billing service later. */
        readerState->dfsType = DFS_OBS;  // Mark the storage type here for billing service later
    }

    DFS_UNIQUE_PTR<GSInputStream> copy()
    {
        return DFS_UNIQUE_PTR<GSInputStream>(new ObsCacheFileInputStream(*this));
    }

    /* deconstructor */
    ~ObsCacheFileInputStream()
    {
        /* do nothing */
    }

    /*
     * function description: read obs file which represents by
     * m_handler with cache. Take example, if we want read a OBS file named
     * obsfile1. We use obsfile1 [offset, length] reprensts the range
     * to be read. The three parameter filename\offset\length show
     * the unique coordinate in OBS storage dimension. Use the triple
     * search the local cache, if hit read it from the cache otherwise
     * read it from obs and insert triple represented data block
     * into cache.
     * @input buffer, represent the memory to store data
     * @input length, the bytes count to be read
     * @input offset, the file offset from where to read
     * @output NA
     */
    void read(void *buffer, uint64_t length, uint64_t offset) override
    {
        Assert(m_handler != NULL && readerState != NULL);

        m_handler->properties.get_cond.start_byte = offset;

        /* this is useless for OBS foreign table. relId is constantly equals -1. */
        int32 relId = readerState->currentFileID;

        /*
         * Now we only support OBS foreign table whose currentFileID is -1
         */
        if (FOREIGNTABLEFILEID == relId) {
            OrcDataValue *dataBuffer = NULL;
            bool found = false;
            bool err_found = false;
            bool collsion = false;
            bool eTagRenewCollsion = false;
            bool eTagMatch = true;
            int slotId = CACHE_BLOCK_INVALID_IDX;
            RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;

            pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);

            /* (prefix len) + (1 char for '\0')  + (etag len) +  (1 char for '\0') + (data len) */
            uint64 newLength = length + strlen(m_key) + 1 + strlen(readerState->currentSplit->eTag) + 1;

            slotId = OBSCacheAllocBlock(m_handler->m_hostname, m_bucket, m_key, offset, newLength, found, err_found);

            if (err_found) {
                orc::orclog(orc::ORC_ERROR, orc::PARSEERROR, "find an error when allocate data on file %s",
                            m_filename.c_str());
            }
            ereport(DEBUG1,
                    (errmodule(MOD_ORC),
                     errmsg("get data from obs: slotID(%d), filename(%s) ,spcID(%u), dbID(%u), relID(%u), fileID(%d), "
                            "offset(%lu), length(%lu), found(%d)",
                            slotId, m_filename.c_str(), fileNode.spcNode, fileNode.dbNode, fileNode.relNode, relId,
                            offset, length, found)));

            /* dataBuffer: prefix + '\0' + eTag  + '\0' + data */
            dataBuffer = ORCCacheGetBlock(slotId);

            if (found) {
                /* compare dataBuffer prefix head with input prefix */
                size_t prefixLen = strlen(m_key);
                Assert(prefixLen <= OBS_MAX_KEY_SIZE);

                /* probe  the perfix len in data buffer */
                char *perfixInBuf = (char *)dataBuffer->value;
                size_t perfixLenInBuf = strlen(perfixInBuf);
                /* check the prefix len and prefix character */
                if (perfixLenInBuf == prefixLen && !strncmp(perfixInBuf, m_key, prefixLen)) {
                    /* etag len */
                    size_t dataDNALen = strlen(readerState->currentSplit->eTag);
                    /* probe the etag len in data buffer */
                    char *pdataDNAInBuf = (char *)dataBuffer->value + perfixLenInBuf + 1;
                    size_t dataDNALenInBuf = strlen(pdataDNAInBuf);
                    /* check the etag len and prefix character */
                    if (dataDNALen != dataDNALenInBuf ||
                        strncmp(pdataDNAInBuf, readerState->currentSplit->eTag, dataDNALen)) { /* data DNA changed */
                        eTagMatch = false;
                    } else {  // data DNA matched means we could copy data from buffer and don't worry about the change
                        errno_t rc = memcpy_s(buffer, length,
                                              (char *)dataBuffer->value + prefixLen + 1 + dataDNALen + 1, length);
                        securec_check(rc, "\0", "\0");
                        ReleaseORCBlock(slotId);

                        ereport(DEBUG1, (errmodule(MOD_ORC), errmsg("get data from obs cache directly.")));

                        readerState->orcDataCacheBlockCount++;
                        readerState->orcDataCacheBlockSize += length;
                        pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);

                        ++m_cacheReadCalls;
                    }
                } else /* different files have the same key, we must read the data from OBS directly */
                    collsion = true;
            }

            /*
             * Three scenarios must to be handled:
             * 1. collsion: different files have the same key. Read content from OBS directly.
             * 2. eTag changed:
             *	  2.1 No other thread update the cache. Update the cache and store
             *		  new data into cache.
             *	  2.2 There is other thread updating the cache at the same time. Read content
             *		  from OBS directly.
             * 3. Not find data in cache. Read content from OBS directly and store it into cache.
             *
             *
             * OBS file changed at some time. The cache should be refreshed to keep data constant.
             * There may multi threads enter into renew logic which should be avoided.
             */
            if (!eTagMatch && !OBSCacheRenewBlock(slotId))
                eTagRenewCollsion = true;

            /* condition 1 and 2.1, read data from OBS directly. */
            if (collsion || (!eTagMatch && eTagRenewCollsion)) {
                /*
                 * we read data directly from OBS and don't store it into cache.
                 * There is no need continuly pin slot
                 */
                ReleaseORCBlock(slotId);
                readFile(buffer, length, offset);
                readerState->orcDataLoadBlockCount++;
                readerState->orcDataLoadBlockSize += length;
            } else if (!found || (!eTagMatch && !eTagRenewCollsion)) { /* condition 2.2 and 3 */
                ereport(DEBUG1, (errmodule(MOD_ORC), errmsg("get data from obs server")));

                readFile(buffer, length, offset);
                OBSCacheSetBlock(slotId, buffer, length, m_key, readerState->currentSplit->eTag);

                /* update slotId related cache, unpin the slot */
                ReleaseORCBlock(slotId);
                readerState->orcDataLoadBlockCount++;
                readerState->orcDataLoadBlockSize += length;
            }
        } else {
            /* do nothing */
        }
    }

protected:
    /* copy constructor */
    ObsCacheFileInputStream(const ObsCacheFileInputStream &other) : ObsFileInputStream(other)
    {
    }
};  // end of define ObsCacheFileInputStream

DFS_UNIQUE_PTR<GSInputStream> readObsFile(OBSReadWriteHandler *handler, const std::string &path,
                                          dfs::reader::ReaderState *readerState)
{
    ObsFileInputStream *inputstream = new ObsFileInputStream(readerState);
    inputstream->init(handler, path, readerState->currentFileSize);
    return DFS_UNIQUE_PTR<GSInputStream>(inputstream);
}

DFS_UNIQUE_PTR<GSInputStream> readObsFileWithCache(OBSReadWriteHandler *handler, const std::string &path,
                                                   dfs::reader::ReaderState *readerState)
{
    ObsCacheFileInputStream *inputstream = new ObsCacheFileInputStream(readerState);
    inputstream->init(handler, path, readerState->currentFileSize);
    return DFS_UNIQUE_PTR<GSInputStream>(inputstream);
}
}  // namespace dfs
