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
 * orc_writer.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/orc_writer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <arpa/inet.h>
#include "orc_rw.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "access/dfs/dfs_insert.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "dfs_adaptor.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "catalog/dfsstore_ctlg.h"
#include "commands/tablespace.h"
#include "workload/workload.h"

namespace dfs {
#define DEFAULT_ROWS_CHECK 1000
#define DEFAULT_STRIPE_SIZE ((int64)(64 * 1024 * 1024))
#define DFS_BLOCKSIZE_KEY "dfs.blocksize"
#define DFS_GLOBAL_LABEL "dfs.datanode.global.label"
#define DEFAULT_BLOCK_SIZE "134217728"  // 128M
#define BUFFER_CAPACITY                                                                                            \
    ((((uint64)((uint32)maxChunksPerProcess - (uint32)processMemInChunks) << (unsigned int)chunkSizeInBits) / 2) - \
     DEFAULT_STRIPE_SIZE)

namespace writer {
static void parseParsig(Relation rel, const char *partdir, Datum *values, bool *nulls);

template <FieldKind fieldKind, Oid type, bool decimal128>
void appendDatumT(orc::ColumnVectorBatch *colBatch, Datum *val, bool *isNull, uint32 size, int64 eppchOffsetDiff,
                  int64 &bufferSize, int64 maxBufferSize);

inline void checkMemory(int64 &bufferSize, int size, int64 maxBufferSize)
{
    if (!t_thrd.utils_cxt.gs_mp_inited) {
        return;
    }

    bufferSize += size;
    if (bufferSize > maxBufferSize) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY), errmodule(MOD_ORC),
                        errmsg("memory is temporarily unavailable")));
    }
}

ORCColWriter::ORCColWriter(MemoryContext ctx, Relation relation, Relation destRel, DFSConnector *conn,
                           const char *parsig)
    : m_bufferCapacity(0),
      m_epochOffsetDiff(0),
      m_fixedBufferSize(0),
      m_totalBufferSize(0),
      m_maxBufferSize(0),
      m_scales(NULL),
      m_ctx(ctx),
      m_rel(relation),
      m_destRel(destRel),
      m_desc(relation->rd_att),
      m_conn(conn),
      m_appendDatumFunc(NULL),
      m_convertToDatumFunc(NULL),
      m_favorNodes(NULL),
      m_dfsLabel(NULL),
      m_sort(false),
      m_parsig(parsig),
      m_colAttrIndex(NULL),
      m_indexInsertInfo(NULL),
      m_idxBatchRow(NULL),
      m_idxInsertBatchRow(NULL),
      m_tmpValues(NULL),
      m_tmpNulls(NULL),
      m_idxChooseKey(NULL),
      m_indexInfoReady(false)
{
}

ORCColWriter::~ORCColWriter()
{
    m_ctx = NULL;
    m_rel = NULL;
    m_destRel = NULL;
    m_desc = NULL;
    m_conn = NULL;
    m_parsig = NULL;
    m_idxInsertBatchRow = NULL;
    m_tmpNulls = NULL;
    m_favorNodes = NULL;
    m_scales = NULL;
    m_convertToDatumFunc = NULL;
    m_indexInsertInfo = NULL;
    m_colAttrIndex = NULL;
    m_appendDatumFunc = NULL;
    m_tmpValues = NULL;
    m_idxChooseKey = NULL;
    m_idxBatchRow = NULL;
    m_dfsLabel = NULL;
}

int ORCColWriter::AssignRealColAttrIndex()
{
    int realColId = InvalidAttrNumber;
    List *partList = NIL;
    int realnatts = m_desc->natts;

    /*
     * For value partitioned table, the actual columns we are going to store
     * are not including the partition columns
     */
    if (RelationIsValuePartitioned(m_rel)) {
        partList = ((ValuePartitionMap *)m_rel->partMap)->partList;
        realnatts -= list_length(partList);

        int max_part_num = MAX_ACTIVE_PARNUM == 0 ? 1 : MAX_ACTIVE_PARNUM;
        m_maxBufferSize = (int64)(BUFFER_CAPACITY / max_part_num);
    } else {
        m_maxBufferSize = (int64)BUFFER_CAPACITY;
    }

    m_colAttrIndex = (int *)palloc0(sizeof(int) * m_desc->natts);
    for (int i = 0; i < m_desc->natts; i++) {
        if (list_member_int(partList, (i + 1)) || isDroppedCol(i)) {
            m_colAttrIndex[i] = -1;
        } else {
            m_colAttrIndex[i] = realColId;
            realColId++;
        }
    }

    return realnatts;
}

void ORCColWriter::init(IndexInsertInfo *indexInsertInfo)
{
    Form_pg_attribute *attrs = m_desc->attrs;
    int realnatts = 0;

    /* Assign column writer index */
    realnatts = AssignRealColAttrIndex();

    /* Check if we need to set the lable and fetch the lable name if neccessary. */
    initLabelInfo();

    /* Accquire the favorite nodes' list. */
    initFavoriteNodeInfo();

    /* Estimate the max size of buffer. */
    m_bufferCapacity = EstimateBufferRows(m_desc);

    /* Initialize the function pointer. */
    m_convertToDatumFunc = (convertToDatum *)palloc(sizeof(convertToDatum) * realnatts);
    m_appendDatumFunc = (appendDatum *)palloc(sizeof(appendDatum) * realnatts);
    m_scales = (int32_t *)palloc0(sizeof(int32_t) * realnatts);

    /* Initialize m_opts and bind up the function pointer. */
    TupleConstr *constr = m_desc->constr;
    if (constr != NULL && constr->clusterKeyNum > 0) {
        m_sort = true;
    }
    m_opts.stripeSize = DEFAULT_STRIPE_SIZE;
    m_opts.fileSize = 2 * DEFAULT_STRIPE_SIZE;
    m_epochOffsetDiff = (m_opts.getEpochOffset() - PG_EPOCH_OFFSET_DEFAULT);
    setCompressKind();
    setBlockSize();

    for (int i = 0; i < m_desc->natts; i++) {
        /* see comments of m_colAttrIndex in orc_rw.h for more details */
        int realColId = m_colAttrIndex[i];

        /* -1 means value partition column */
        if (-1 == realColId) {
            continue;
        }

        switch (m_desc->attrs[i]->atttypid) {
            case BOOLOID:
                m_opts.schema->addStructField(orc::BOOLEAN, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::BOOLEAN, BOOLOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::BOOLEAN, BOOLOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char), m_maxBufferSize);
                break;
            case INT1OID:
                m_opts.schema->addStructField(orc::BYTE, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::BYTE, INT1OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::BYTE, INT1OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int8), m_maxBufferSize);
                break;
            case INT2OID:
                m_opts.schema->addStructField(orc::SHORT, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::SHORT, INT2OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::SHORT, INT2OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int16), m_maxBufferSize);
                break;
            case INT4OID:
                m_opts.schema->addStructField(orc::INT, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::INT, INT4OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::INT, INT4OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int32), m_maxBufferSize);
                break;
            case INT8OID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, INT8OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, INT8OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case OIDOID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, OIDOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, OIDOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case TIMESTAMPTZOID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, TIMESTAMPTZOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, TIMESTAMPTZOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case TIMEOID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, TIMEOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, TIMEOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case SMALLDATETIMEOID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, SMALLDATETIMEOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, SMALLDATETIMEOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case CASHOID:
                m_opts.schema->addStructField(orc::LONG, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::LONG, CASHOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::LONG, CASHOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                break;
            case FLOAT4OID:
                m_opts.schema->addStructField(orc::FLOAT, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::FLOAT, FLOAT4OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::FLOAT, FLOAT4OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(float4), m_maxBufferSize);
                break;
            case FLOAT8OID:
                m_opts.schema->addStructField(orc::DOUBLE, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::DOUBLE, FLOAT8OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::DOUBLE, FLOAT8OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(float8), m_maxBufferSize);
                break;
            case TEXTOID:
            case CLOBOID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, TEXTOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, TEXTOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            case BPCHAROID: {
                int typmod = m_desc->attrs[i]->atttypmod;
                if (typmod > 0) {
                    m_opts.schema->addStructField(orc::createCharType(orc::CHAR,
                                                                      m_desc->attrs[i]->atttypmod - VARHDRSZ),
                                                  attrs[i]->attname.data);
                    m_convertToDatumFunc[realColId] = &convertToDatumT<orc::CHAR, BPCHAROID, WRITER>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::CHAR, BPCHAROID, false>;
                } else {
                    m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                    m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, BPCHAROID, WRITER>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, BPCHAROID, false>;
                }
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            }
            case VARCHAROID: {
                int typmod = m_desc->attrs[i]->atttypmod;
                if (typmod > 0) {
                    m_opts.schema->addStructField(orc::createCharType(orc::VARCHAR,
                                                                      m_desc->attrs[i]->atttypmod - VARHDRSZ),
                                                  attrs[i]->attname.data);
                    m_convertToDatumFunc[realColId] = &convertToDatumT<orc::VARCHAR, VARCHAROID, WRITER>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::VARCHAR, VARCHAROID, false>;
                } else {
                    m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                    m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, VARCHAROID, WRITER>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, VARCHAROID, false>;
                }
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            }
            case TIMESTAMPOID:
                m_opts.schema->addStructField(orc::TIMESTAMP, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::TIMESTAMP, TIMESTAMPOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::TIMESTAMP, TIMESTAMPOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64) * 2, m_maxBufferSize);
                break;
            case NUMERICOID: {
                uint32 typmod = (uint32)(attrs[i]->atttypmod - VARHDRSZ);
                uint32 precision = uint32((typmod >> 16) & 0xffff);
                uint32 scale = uint32(typmod & 0xffff);

                m_scales[realColId] = scale;

                if (attrs[i]->atttypmod != -1 && precision <= 18) {
                    m_opts.schema->addStructField(orc::createDecimalType(precision, scale), attrs[i]->attname.data);

                    m_convertToDatumFunc[realColId] = &convertDecimalToDatumT<false, false>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::DECIMAL, NUMERICOID, false>;

                    checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(int64), m_maxBufferSize);
                } else if (attrs[i]->atttypmod != -1 && precision <= 38) {
                    m_opts.schema->addStructField(orc::createDecimalType(precision, scale), attrs[i]->attname.data);

                    m_convertToDatumFunc[realColId] = &convertDecimalToDatumT<true, false>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::DECIMAL, NUMERICOID, true>;

                    checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(orc::Int128), m_maxBufferSize);
                } else { /* still save as string on precision > 38 */
                    m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);

                    m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, NUMERICOID, WRITER>;
                    m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, NUMERICOID, false>;

                    checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                }

                break;
            }
            case INTERVALOID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, INTERVALOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, INTERVALOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            case TINTERVALOID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, TINTERVALOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, TINTERVALOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            case TIMETZOID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, TIMETZOID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, TIMETZOID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            case CHAROID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, CHAROID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, CHAROID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            case NVARCHAR2OID:
                m_opts.schema->addStructField(orc::STRING, attrs[i]->attname.data);
                m_convertToDatumFunc[realColId] = &convertToDatumT<orc::STRING, NVARCHAR2OID, WRITER>;
                m_appendDatumFunc[realColId] = &appendDatumT<orc::STRING, NVARCHAR2OID, false>;
                checkMemory(m_fixedBufferSize, m_bufferCapacity * sizeof(char *), m_maxBufferSize);
                break;
            default: {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_ORC),
                                errmsg("Unsupported data type : %u.", m_desc->attrs[i]->atttypid)));
                break;
            }
        }
    }

    m_totalBufferSize = m_fixedBufferSize;

    DFS_TRY()
    {
        m_columnVectorBatch = orc::Writer::createColBatch(m_bufferCapacity, m_opts.getType(), *orc::getDefaultPool());
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHOUTARGS(
        "Error occurs while creating orc column batch because of %s, "
        "detail can be found in dn log of %s.",
        MOD_ORC);

    /* Initialize the variables needed for index if exist. */
    initIndexInsertInfo(indexInsertInfo);
}

void ORCColWriter::Destroy()
{
    if (NULL != m_favorNodes) {
        delete[] m_favorNodes;
        m_favorNodes = NULL;
    }

    if (NULL != m_dfsLabel) {
        pfree_ext(m_dfsLabel->data);
        pfree_ext(m_dfsLabel);
    }

    if (NULL != m_conn) {
        delete (m_conn);
        m_conn = NULL;
    }

    pfree_ext(m_convertToDatumFunc);
    pfree_ext(m_appendDatumFunc);
    pfree_ext(m_colAttrIndex);
}

void ORCColWriter::handleTail()
{
    /* Clean the objects and close the opened relations if there is index. */
    if (m_indexInfoReady) {
        DELETE_EX(m_idxBatchRow);
        bulkload_indexbatch_deinit(m_idxInsertBatchRow);
        pfree_ext(m_tmpValues);
        pfree_ext(m_tmpNulls);
        pfree_ext(m_idxChooseKey);
        m_indexInfoReady = false;
    }
}

/*
 * @Description: Initialize the members for index
 * @See also:
 */
void ORCColWriter::initIndexInsertInfo(IndexInsertInfo *indexInsertInfo)
{
    if (NULL != indexInsertInfo) {
        m_indexInsertInfo = indexInsertInfo;

        /* Initialize the batch row object. */
        m_idxBatchRow = New(CurrentMemoryContext) bulkload_rows(m_desc, m_bufferCapacity);
        m_idxInsertBatchRow = bulkload_indexbatch_init(indexInsertInfo->maxKeyNum, m_bufferCapacity);

        /* Initialize some temp variables. */
        m_tmpValues = (Datum *)palloc(sizeof(Datum) * m_bufferCapacity);
        m_tmpNulls = (bool *)palloc(sizeof(bool) * m_bufferCapacity);
        m_idxChooseKey = (bool *)palloc(sizeof(bool) * m_desc->natts);

        /* Set the ready flag. */
        m_indexInfoReady = true;
    }
}

/* Check if we need to set the lable and fetch the lable name if neccessary. */
void ORCColWriter::initLabelInfo()
{
    m_dfsLabel = makeStringInfo();
    char myhostname[MAXHOSTNAMELEN] = {0};
    (void)gethostname(myhostname, MAXHOSTNAMELEN);
    const char *localHost = pstrdup(myhostname);
    if (NULL != localHost) {
        const char *localLabel = m_conn->getValue(localHost, NULL);
        if (NULL != localLabel) {
            appendStringInfo(m_dfsLabel, "%s", localLabel);
        }
    }

    if (t_thrd.postmaster_cxt.ReplConnArray[1] != NULL) {
        struct in_addr ipv4addr;
        struct hostent hst_ent;
        struct hostent *hp = NULL;
        char buff[1024];
        int error = 0;
        errno_t rcc = 0;
        rcc = memset_s(&ipv4addr, sizeof(ipv4addr), 0, sizeof(ipv4addr));
        securec_check(rcc, "\0", "\0");
        inet_pton(AF_INET, t_thrd.postmaster_cxt.ReplConnArray[1]->remotehost, &ipv4addr);
        int rc = gethostbyaddr_r((char *)&ipv4addr, sizeof(ipv4addr), AF_INET, &hst_ent, buff, 1024, &hp, &error);
        if (0 != rc || 0 != error) {
            ereport(WARNING, (errmodule(MOD_ORC), errmsg("Fail to find the remote host.")));
        } else {
            const char *remoteLabel = m_conn->getValue(hst_ent.h_name, NULL);
            if (NULL != remoteLabel) {
                if (m_dfsLabel->len > 0) {
                    appendStringInfo(m_dfsLabel, ",");
                }
                appendStringInfo(m_dfsLabel, "%s", remoteLabel);
            }
        }
    }

    const char *globalLabel = m_conn->getValue(DFS_GLOBAL_LABEL, NULL);
    if (NULL != globalLabel) {
        if (m_dfsLabel->len > 0) {
            appendStringInfo(m_dfsLabel, ",");
        }
        appendStringInfo(m_dfsLabel, "%s", globalLabel);
    }
}

/* Accquire the favorite nodes' list. */
void ORCColWriter::initFavoriteNodeInfo()
{
    if (t_thrd.postmaster_cxt.ReplConnArray[1] != NULL) {
        m_favorNodes = new std::vector<std::string>[2];
        /* Add the IP of local(primary) node and remote(standy) node. */
        m_favorNodes->emplace_back(t_thrd.postmaster_cxt.ReplConnArray[1]->localhost);
        m_favorNodes->emplace_back(t_thrd.postmaster_cxt.ReplConnArray[1]->remotehost);
        ereport(DEBUG1, (errmodule(MOD_ORC),
                         errmsg("favorite nodes is: %s, %s.", t_thrd.postmaster_cxt.ReplConnArray[1]->localhost,
                                t_thrd.postmaster_cxt.ReplConnArray[1]->remotehost)));
    } else {
        m_favorNodes = NULL;
    }
}

void ORCColWriter::setCompressKind()
{
    const char *compressKind = RelationGetCompression(m_rel);

    /* If we don't set compressionKind in m_opts, it is NONE by default. */
    if (NULL != compressKind) {
        if (0 == pg_strcasecmp(COMPRESSION_NO, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_NONE;
        } else if (0 == pg_strcasecmp(COMPRESSION_YES, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_SNAPPY;
        } else if (0 == pg_strcasecmp(COMPRESSION_LOW, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_SNAPPY;
        } else if (0 == pg_strcasecmp(COMPRESSION_MIDDLE, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_SNAPPY;
            m_opts.encodingStrategy = orc::ES_COMPRESSION;
        } else if (0 == pg_strcasecmp(COMPRESSION_HIGH, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_ZLIB;
            m_opts.encodingStrategy = orc::ES_COMPRESSION;
        } else if (0 == pg_strcasecmp(COMPRESSION_ZLIB, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_ZLIB;
            m_opts.encodingStrategy = orc::ES_COMPRESSION;
        } else if (0 == pg_strcasecmp(COMPRESSION_SNAPPY, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_SNAPPY;
        } else if (0 == pg_strcasecmp(COMPRESSION_LZ4, compressKind)) {
            m_opts.compressionKind = orc::CompressionKind_LZ4;
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_ORC),
                     errmsg("Invalid string for  \"COMPRESSION\" option."),
                     errdetail("Valid string are \"no\", \"yes\", \"low\", \"middle\", \"high\", \"snappy\", \"zlib\", "
                               "\"lz4\" for dfs table.")));
        }
    }
}

void ORCColWriter::setBlockSize()
{
    /* The default size of block is 128M. */
    const char *blockSize = m_conn->getValue(DFS_BLOCKSIZE_KEY, DEFAULT_BLOCK_SIZE);
    if (NULL != blockSize) {
        m_opts.blockSize = pg_strtoint32(const_cast<char *>(blockSize));
        m_opts.fileSize = m_opts.blockSize;
    }
}

void ORCColWriter::openNewFile(const char *filePath)
{
    FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    return;
}

/*
 * @Description: Spill the buffer data into serialize stream.
 * @IN fileID: the current file id
 * @Return: Return 0 means there is no left rows in buffer;
 *		Return non-0 means the current file is full and we need to create a new
 *		file in the next time.
 * @See also:
 */
int ORCColWriter::spill(uint64 fileID)
{
    Assert(NULL != m_orcWriter.get());
    int ret = 0;
    uint64 startOffset = 0;

    if (0 == m_columnVectorBatch->numElements) {
        return ret;
    }

    /*
     * If there is index created on hdfs relation, we need to put all
     * the content of m_columnVectorBatch into batchRow first, and
     * then intert them into index table.
     */
    if (NULL != m_indexInsertInfo) {
        startOffset = m_orcWriter->getSerializeSize();
        insert2Batchrow(startOffset, fileID);
    }

    DFS_TRY()
    {
        ret = m_orcWriter->addColumnBatch(*m_columnVectorBatch.get());
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS(
        "Error occurs while spilling a new file %lu to write because of %s, "
        "detail can be found in dn log of %s.",
        MOD_ORC, fileID);

    /*
     * Read the content of the batchrow and insert some of them, which starts from 0 and
     * size is (newSize - oldSize), into index table.
     */
    if (NULL != m_indexInsertInfo) {
        flush2Index(m_orcWriter->getSerializeSize() - startOffset);
    }

    if (0 == ret) {
        ret = ((m_orcWriter->getSerializeSize() + m_bufferCapacity) > DfsMaxOffset ? 1 : 0);
        m_totalBufferSize = m_fixedBufferSize;
        Assert(0 == m_columnVectorBatch->numElements);
    }

    return ret;
}

void ORCColWriter::appendColDatum(int colID, Datum *vals, bool *isNull, uint32 size)
{
    if (0 != size) {
        Assert(NULL != m_columnVectorBatch.get());

        /*
         * For Value-Partition table, we have to calculate the fieldId to correctly
         * index the fieldId of structVectorBatch
         */
        int fieldId = m_colAttrIndex[colID];
        if (fieldId == -1) {
            return;
        }

        orc::StructVectorBatch *structVectorBatch = (orc::StructVectorBatch *)m_columnVectorBatch.get();
        DFS_TRY()
        {
            m_appendDatumFunc[fieldId](structVectorBatch->fields[fieldId], vals, isNull, size, m_epochOffsetDiff,
                                       m_totalBufferSize, m_maxBufferSize);
        }
        DFS_CATCH();
        DFS_ERRREPORT_WITHARGS(
            "Error occurs while add a column batch on column %d because of %s, "
            "detail can be found in dn log of %s.",
            MOD_ORC, colID);
    }
}

/*
 * @Description: Extract a column's datums from the orc column batch.
 * @IN col: the column id(count from 0)
 * @OUT values: the output datums
 * @OUT nulls the output nulls
 * @See also: extractDatumFromOrcBatchByRow
 */
void ORCColWriter::extractDatumFromOrcBatchByColumn(int col, Datum *values, bool *nulls)
{
    int32_t adjustedColId = m_colAttrIndex[col];
    int32_t rows = m_columnVectorBatch->numElements;
    int32_t atttypmod = m_desc->attrs[col]->atttypmod;

    /*
     * The index columns can not include partition columns for hdfs table, so we do not consider
     * the partition columns.
     */
    if (adjustedColId != -1) {
        orc::StructVectorBatch *struct_batch = (orc::StructVectorBatch *)m_columnVectorBatch.get();
        orc::ColumnVectorBatch *col_batch = struct_batch->fields[adjustedColId];
        int32_t scale = m_scales[adjustedColId];
        for (int rowId = m_columnVectorBatch->startIndex; rowId < rows; rowId++) {
            bool internalNull = false;
            bool meetError = false;
            nulls[rowId] = !col_batch->notNull[rowId];

            if (!nulls[rowId]) {
                values[rowId] = m_convertToDatumFunc[adjustedColId](col_batch, rowId, atttypmod, scale,
                                                                    m_epochOffsetDiff, internalNull, INVALID_ENCODING,
                                                                    0, meetError);

                nulls[rowId] = internalNull;
            }
        }
    }
}

/*
 * @Description: Extract a row's datums from the orc column batch.
 * @IN rowId: the row id(count from 0)
 * @OUT values: the output datums
 * @OUT nulls the output nulls
 * @See also: extractDatumFromOrcBatchByColumn
 */
void ORCColWriter::extractDatumFromOrcBatchByRow(int rowId, Datum *values, bool *nulls)
{
    int attrNum = m_desc->natts;
    int adjustedColId = 0;

    orc::StructVectorBatch *struct_batch = (orc::StructVectorBatch *)m_columnVectorBatch.get();
    orc::ColumnVectorBatch *col_batch = NULL;

    /* prepare tuple data */
    for (int col = 0; col < attrNum; col++) {
        adjustedColId = m_colAttrIndex[col];
        if (adjustedColId != -1) {
            bool internalNull = false;
            bool meetError = false;
            col_batch = struct_batch->fields[adjustedColId];
            nulls[col] = !col_batch->notNull[rowId];

            if (!nulls[col]) {
                int32 atttypmod = m_desc->attrs[col]->atttypmod;
                values[col] = m_convertToDatumFunc[adjustedColId](col_batch, rowId, atttypmod, m_scales[adjustedColId],
                                                                  m_epochOffsetDiff, internalNull, INVALID_ENCODING, 0,
                                                                  meetError);

                nulls[col] = internalNull;
            }
        }
    }
}

/**
 * @Description: Set Null value into dropped columns.
 * @in values, The output datums.
 * @in nulls, The output nulls.
 * @return None.
 */
void ORCColWriter::setNullIntoDroppedCol(Datum *values, bool *nulls)
{
    int attrNum = m_desc->natts;
    for (int col = 0; col < attrNum; col++) {
        if (isDroppedCol(col)) {
            values[col] = 0;
            nulls[col] = true;
        }
    }
}

/*
 * @Description: Insert the datums into the m_idxBatchRow
 * @IN startOffset: the start offset in the orc column batch
 * @IN fileID: the current file ID
 * @See also: extractDatumFromOrcBatchByColumn
 */
void ORCColWriter::insert2Batchrow(uint64 startOffset, uint64 fileID)
{
    errno_t rc = 0;
    int rows = m_columnVectorBatch->numElements;
    Datum *tids = (Datum *)palloc0(sizeof(Datum) * rows);

    rc = memset_s(m_idxChooseKey, m_desc->natts, false, m_desc->natts);
    securec_check(rc, "\0", "\0");

    /* form the ctid data accordint to fileID and offset. */
    for (int i = 0; i < rows; i++) {
        ItemPointer itemPtr = (ItemPointer)&tids[i];
        ++startOffset;
        DfsItemPointerSet(itemPtr, fileID, startOffset);
    }

    /* Fill m_idxBatchRow with tids. */
    m_idxInsertBatchRow->append_one_column(tids, NULL, rows, NULL, SelfItemPointerAttributeNumber);

    /* Clean the content inside the m_idxBatchRow. */
    m_idxBatchRow->reset(true);

    /* Fill m_idxBatchRow with all the index columns. */
    for (int i = 0; i < m_indexInsertInfo->indexNum; ++i) {
        for (int j = 0; j < m_indexInsertInfo->idxKeyNum[i]; ++j) {
            int primitiveCol = m_indexInsertInfo->idxKeyAttr[i][j];
            if (!m_idxChooseKey[primitiveCol]) {
                extractDatumFromOrcBatchByColumn(m_indexInsertInfo->idxKeyAttr[i][j], m_tmpValues, m_tmpNulls);
                m_idxBatchRow->append_one_column(m_tmpValues, m_tmpNulls, rows, m_desc, primitiveCol);
                m_idxChooseKey[primitiveCol] = true;
            }
        }
    }

    pfree_ext(tids);
}

/*
 * @Description: Flush the m_idxBatchRow into index relation.
 * @IN rowCount: the number of rows to be flush
 * @See also:
 */
void ORCColWriter::flush2Index(int32 rowCount)
{
    if (0 == rowCount) {
        return;
    }

    /*
     * Fill m_idxInsertBatchRow with the content of m_idxBatchRow
     * and insert it into index table.
     */
    for (int indice = 0; indice < m_indexInsertInfo->indexNum; ++indice) {
        /* form index-keys data for index relation */
        for (int key = 0; key < m_indexInsertInfo->idxKeyNum[indice]; ++key) {
            bulkload_indexbatch_copy(m_idxInsertBatchRow, key, m_idxBatchRow,
                                     m_indexInsertInfo->idxKeyAttr[indice][key]);
        }

        bulkload_indexbatch_copy_tids(m_idxInsertBatchRow, m_indexInsertInfo->idxKeyNum[indice]);

        /* update the actual number of used attributes and row count. */
        m_idxInsertBatchRow->m_attr_num = m_indexInsertInfo->idxKeyNum[indice] + 1;
        m_idxInsertBatchRow->m_rows_curnum = rowCount;

        if (m_indexInsertInfo->idxInsert[indice] != NULL) {
            /* insert psort index data into index relation */
            m_indexInsertInfo->idxInsert[indice]->BatchInsert(m_idxInsertBatchRow, 0);
        } else {
            /* insert index data into btree index relation */
            Relation indexRel = m_indexInsertInfo->idxRelation[indice];

            Datum *values = (Datum *)palloc(sizeof(Datum) * m_idxInsertBatchRow->m_attr_num);
            bool *isnull = (bool *)palloc(sizeof(bool) * m_idxInsertBatchRow->m_attr_num);

            bulkload_rows_iter iter;
            iter.begin(m_idxInsertBatchRow);
            while (iter.not_end()) {
                iter.next(values, isnull);
                ItemPointer tupleid = (ItemPointer)&values[m_idxInsertBatchRow->m_attr_num - 1];
                index_insert(indexRel,         /* index relation */
                             values,           /* array of index Datums */
                             isnull,           /* null flags */
                             tupleid,          /* tid of heap tuple */
                             m_rel,            /* heap relation */
                             UNIQUE_CHECK_NO); /* type of uniqueness check to do */
            }

            pfree_ext(values);
            pfree_ext(isnull);
        }
    }

    /* Reset the m_idxInsertBatchRow. */
    m_idxInsertBatchRow->reset(true);
}

/*
 * @Description: insert the data left in the buffer into delta table.
 * @IN options: the insert option
 * @See also:
 */
void ORCColWriter::deltaInsert(int options)
{
    int rows = m_columnVectorBatch->numElements;
    int attrNum = m_desc->natts;

    HeapTuple tuple = NULL;
    Relation delta = heap_open(m_rel->rd_rel->reldeltarelid, RowExclusiveLock);
    Assert(NULL != delta);

    Datum *values = (Datum *)palloc(sizeof(Datum) * attrNum);
    bool *nulls = (bool *)palloc(sizeof(bool) * attrNum);

    if (RelationIsValuePartitioned(m_rel)) {
        /* parse partition signature and fill values/nulls */
        parseParsig(m_rel, m_parsig, values, nulls);
    }

    /*
     * Set null values for dropped columns.
     */
    setNullIntoDroppedCol(values, nulls);

    for (int rowId = m_columnVectorBatch->startIndex; rowId < rows; rowId++) {
        /* prepare tuple data */
        extractDatumFromOrcBatchByRow(rowId, values, nulls);
        tuple = heap_form_tuple(m_desc, values, nulls);

        /* We always generate xlog for delta tuple */
        options = (unsigned int)options & (~TABLE_INSERT_SKIP_WAL);

        (void)tableam_tuple_insert(delta, tuple, GetCurrentCommandId(true), options, NULL);
    }

    heap_close(delta, RowExclusiveLock);

    pfree_ext(values);
    pfree_ext(nulls);

    /* Clean the column batch. */
    m_columnVectorBatch->clean();
}

/*
 * @Description: Acquire the min max value of the column. Return false when there is no min/max.
 * @IN colId: the column id
 * @OUT minStr: the string of min value if exist
 * @OUT maxStr: the string of max value if exist
 * @OUT hasMin: whether the min value exist
 * @OUT hasMax: whether the max value exist
 * @Return: false if there is no min or max value of the column, else return true.
 * @See also:
 */
bool ORCColWriter::getMinMax(uint32 colId, char *&minStr, char *&maxStr, bool &hasMin, bool &hasMax)
{
    bool ret = false;
    int orcColId = m_colAttrIndex[colId];

    /* Partition column has no min/max in desc table. */
    if (orcColId == -1 || orcColId == INT_MAX) {
        return false;
    }

    DFS_TRY()
    {
        switch (m_desc->attrs[colId]->atttypid) {
            /* Integer type */
            case INT2OID:
            case INT4OID:
            case INT8OID: {
                int64 min = 0;
                int64 max = 0;
                if (m_orcWriter->getMinAndMax<int64>((int64)orcColId + 1, &min, &max, hasMin, hasMax)) {
                    if (hasMin) {
                        minStr = DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(min)));
                    }
                    if (hasMax) {
                        maxStr = DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(max)));
                    }
                    ret = true;
                }
                break;
            }
            /* float/double type */
            case FLOAT4OID:
            case FLOAT8OID: {
                double min = 0;
                double max = 0;
                if (m_orcWriter->getMinAndMax<double>((int64)orcColId + 1, &min, &max, hasMin, hasMax)) {
                    if (hasMin) {
                        minStr = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(min)));
                    }
                    if (hasMax) {
                        maxStr = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(max)));
                    }
                    ret = true;
                }
                break;
            }
            /* string type */
            case VARCHAROID:
            case BPCHAROID:
            case CLOBOID:
            case TEXTOID: {
                std::string min;
                std::string max;
                errno_t rc;
                if (m_orcWriter->getMinAndMax<std::string>((int64)orcColId + 1, &min, &max, hasMin, hasMax)) {
                    if (hasMin) {
                        int minLen = strlen(min.c_str());
                        minStr = (char *)palloc0(sizeof(char) * (minLen + 1));
                        if (minLen > 0) {
                            rc = memcpy_s(minStr, minLen, min.c_str(), minLen);
                            securec_check(rc, "\0", "\0");
                        }
                        minStr[minLen] = '\0';
                    }
                    if (hasMax) {
                        int maxLen = strlen(max.c_str());
                        maxStr = (char *)palloc0(sizeof(char) * (maxLen + 1));
                        if (maxLen > 0) {
                            rc = memcpy_s(maxStr, maxLen, max.c_str(), maxLen);
                            securec_check(rc, "\0", "\0");
                        }
                        maxStr[maxLen] = '\0';
                    }
                    ret = true;
                }
                break;
            }
            default: {
                /* The other types will not store min/max in desc table. */
                break;
            }
        }
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS(
        "Error occurs while get the min max statistics of column %u because of %s, "
        "detail can be found in dn log of %s.",
        MOD_ORC, colId);

    return ret;
}

void ORCColWriter::closeCurWriter()
{
    if (NULL != m_orcWriter.get()) {
        DFS_TRY()
        {
            m_orcWriter->close();
        }
        DFS_CATCH();
        DFS_ERRREPORT_WITHOUTARGS(
            "Error occurs while close the orc writer because of %s, "
            "detail can be found in dn log of %s.",
            MOD_ORC);
    }
}

int64_t ORCColWriter::verifyFile(char *filePath)
{
    int64 fileSize = m_conn->getFileSize(filePath);
    if (0 == fileSize) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Invalid file size is found on file: %s.", filePath)));
    }

    /* increase used space for user */
    perm_space_increase(m_rel->rd_rel->relowner, fileSize, RelationUsesSpaceType(m_rel->rd_rel->relpersistence));

    return fileSize;
}

template <FieldKind fieldKind, Oid type, bool decimal128>
void appendDatumT(orc::ColumnVectorBatch *colBatch, Datum *vals, bool *isNull, uint32 size, int64 eppchOffsetDiff,
                  int64 &bufferSize, int64 maxBufferSize)
{
    Assert(NULL != isNull);
    uint64 offset = colBatch->getEndIndex();
    errno_t rc = 0;
    if (offset + size > colBatch->capacity) {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC), errmsg("The start index is over the limit.")));
    }

    /* Set the null flag in the column batch. */
    char *notNulData = colBatch->notNull.data();
    for (uint32 i = 0; i < size; i++) {
        if (isNull[i]) {
            colBatch->hasNulls = true;
            notNulData[offset + i] = false;
        } else {
            notNulData[offset + i] = true;
        }
    }

    /* Set the data in the column batch if not null. */
    switch (type) {
        case BOOLOID: {
            bool *data = static_cast<orc::NumberVectorBatch<bool> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetBool(vals[i]);
                }
            }
            break;
        }
        case INT1OID: {
            int8 *data = static_cast<orc::NumberVectorBatch<int8> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetUInt8(vals[i]);
                }
            }
            break;
        }
        case INT2OID: {
            int16 *data = static_cast<orc::NumberVectorBatch<int16> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetInt16(vals[i]);
                }
            }
            break;
        }
        case INT4OID: {
            int32 *data = static_cast<orc::NumberVectorBatch<int32> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetInt32(vals[i]);
                }
            }
            break;
        }
        case DATEOID:
        case INT8OID:
        case OIDOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case SMALLDATETIMEOID:
        case CASHOID: {
            int64 *data = static_cast<orc::NumberVectorBatch<int64> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    switch (type) {
                        case OIDOID:
                            data[offset + i] = DatumGetUInt32(vals[i]);
                            break;
                        case TIMESTAMPTZOID:
                            data[offset + i] = DatumGetTimestampTz(vals[i]);
                            break;
                        case TIMEOID:
                            data[offset + i] = DatumGetTimeADT(vals[i]);
                            break;
                        case SMALLDATETIMEOID:
                            data[offset + i] = DatumGetTimestamp(vals[i]);
                            break;
                        case CASHOID:
                            data[offset + i] = DatumGetCash(vals[i]);
                            break;
                        default:
                            data[offset + i] = DatumGetInt64(vals[i]);
                            break;
                    }
                }
            }
            break;
        }
        case FLOAT4OID: {
            float4 *data = static_cast<orc::NumberVectorBatch<float4> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetFloat4(vals[i]);
                }
            }
            break;
        }
        case FLOAT8OID: {
            float8 *data = static_cast<orc::NumberVectorBatch<float8> *>(colBatch)->data.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    data[offset + i] = DatumGetFloat8(vals[i]);
                }
            }
            break;
        }
        case NVARCHAR2OID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case CLOBOID: {
            char **data = static_cast<orc::StringVectorBatch *>(colBatch)->data.data();
            int64 *strlength = static_cast<orc::StringVectorBatch *>(colBatch)->length.data();

            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    text *src = (text *)vals[i];
                    text *tunpacked = pg_detoast_datum_packed((struct varlena *)src);
                    int len = VARSIZE_ANY_EXHDR(tunpacked);
                    strlength[offset + i] = len;
                    Assert(NULL == data[offset + i]);
                    checkMemory(bufferSize, len + 1, maxBufferSize);
                    data[offset + i] = new char[len + 1];
                    if (len > 0) {
                        rc = memcpy_s(data[offset + i], len, VARDATA_ANY(tunpacked), len);
                        securec_check(rc, "\0", "\0");
                    }
                    data[offset + i][len] = '\0';
                }
            }
            break;
        }
        case TIMESTAMPOID: {
            int64 *seconds = static_cast<orc::TimestampVectorBatch *>(colBatch)->data.data();
            int64 *nanoseconds = static_cast<orc::TimestampVectorBatch *>(colBatch)->nanoseconds.data();
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    int64 microSeconds = DatumGetTimestamp(vals[i]);
                    seconds[offset + i] = (int64)(microSeconds / MICROSECONDS_PER_SECOND) + eppchOffsetDiff;
                    nanoseconds[offset + i] =
                        (int64)((microSeconds % MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND);
                }
            }
            break;
        }
        case NUMERICOID: {
            if (orc::DECIMAL == fieldKind && false == decimal128) {
                int64 *data = static_cast<orc::Decimal64VectorBatch *>(colBatch)->values.data();
                int scale = static_cast<orc::Decimal64VectorBatch *>(colBatch)->scale;
                for (uint32 i = 0; i < size; i++) {
                    if (notNulData[offset + i]) {
                        Numeric n = DatumGetNumeric(vals[i]);
                        if (NUMERIC_IS_BI(n)) {
                            Assert(scale == NUMERIC_BI_SCALE(n));
                            Assert(NUMERIC_IS_BI64(n));
                            data[offset + i] = NUMERIC_64VALUE(n);
                        } else {
                            data[offset + i] = convert_short_numeric_to_int64_byscale(n, scale);
                        }
                    }
                }
            } else if (orc::DECIMAL == fieldKind && true == decimal128) {
                orc::Int128 *data = static_cast<orc::Decimal128VectorBatch *>(colBatch)->values.data();
                int scale = static_cast<orc::Decimal128VectorBatch *>(colBatch)->scale;

                for (uint32 i = 0; i < size; i++) {
                    if (notNulData[offset + i]) {
                        int128 outVal;
                        Numeric n = DatumGetNumeric(vals[i]);
                        if (NUMERIC_IS_BI(n)) {
                            Assert(scale == NUMERIC_BI_SCALE(n));

                            if (NUMERIC_IS_BI128(n)) {
                                rc = memcpy_s(&outVal, sizeof(int128), (n)->choice.n_bi.n_data, sizeof(int128));
                                securec_check(rc, "\0", "\0");
                            } else {
                                outVal = NUMERIC_64VALUE(n);
                            }
                        } else {
                            convert_short_numeric_to_int128_byscale(n, scale, outVal);
                        }

                        orc::Int128 tmpVal((int64)((uint128)outVal >> 64), (uint64)(outVal & 0xFFFFFFFFFFFFFFFF));
                        data[offset + i] = tmpVal;
                    }
                }
            } else {
                char **data = static_cast<orc::StringVectorBatch *>(colBatch)->data.data();
                int64 *strlength = static_cast<orc::StringVectorBatch *>(colBatch)->length.data();
                char *tmpStr = NULL;
                for (uint32 i = 0; i < size; i++) {
                    if (notNulData[offset + i]) {
                        tmpStr = DatumGetCString(DirectFunctionCall1(numeric_out, vals[i]));
                        int len = strlen(tmpStr);
                        strlength[offset + i] = len;
                        Assert(NULL == data[offset + i]);
                        checkMemory(bufferSize, len + 1, maxBufferSize);
                        data[offset + i] = new char[len + 1];
                        if (len > 0) {
                            rc = memcpy_s(data[offset + i], len, tmpStr, len);
                            securec_check(rc, "\0", "\0");
                        }
                        data[offset + i][len] = '\0';
                    }
                }
            }

            break;
        }
        case INTERVALOID:
        case TINTERVALOID:
        case TIMETZOID:
        case CHAROID: {
            char **data = static_cast<orc::StringVectorBatch *>(colBatch)->data.data();
            int64 *strlength = static_cast<orc::StringVectorBatch *>(colBatch)->length.data();
            char *tmpStr = NULL;
            for (uint32 i = 0; i < size; i++) {
                if (notNulData[offset + i]) {
                    switch (type) {
                        case INTERVALOID:
                            tmpStr = DatumGetCString(DirectFunctionCall1(interval_out, vals[i]));
                            break;
                        case TINTERVALOID:
                            tmpStr = DatumGetCString(DirectFunctionCall1(tintervalout, vals[i]));
                            break;
                        case TIMETZOID:
                            tmpStr = DatumGetCString(DirectFunctionCall1(timetz_out, vals[i]));
                            break;
                        case CHAROID:
                            tmpStr = DatumGetCString(DirectFunctionCall1(charout, vals[i]));
                            break;
                        default:
                            break;
                    }

                    int len = strlen(tmpStr);
                    strlength[offset + i] = len;
                    Assert(NULL == data[offset + i]);
                    checkMemory(bufferSize, len + 1, maxBufferSize);
                    data[offset + i] = new char[len + 1];
                    if (len > 0) {
                        rc = memcpy_s(data[offset + i], len, tmpStr, len);
                        securec_check(rc, "\0", "\0");
                    }
                    data[offset + i][len] = '\0';
                }
            }
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_ORC),
                            errmsg("Unsupported data type : %u.", type)));
            break;
        }
    }

    /* Increment the total number of elements in the column batch. */
    colBatch->numElements += size;
}

/*
 * @Description: Create a orc column writer
 * @IN context: memory context
 * @IN rel: the current relation
 * @IN destRel: the dest relation
 * @IN indexInsertInfo: includes index information
 * @IN conn: the connector to dfs
 * @IN parsig: the partition information
 * @Return: a column writer pointer
 * @See also:
 */
ColumnWriter *createORCColWriter(MemoryContext context, Relation rel, Relation destRel,
                                 IndexInsertInfo *indexInsertInfo, DFSConnector *conn, const char *parsig)
{
    if (NULL == conn) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_ORC),
                        errmsg("DFS connector can not be NULL when loading data into dfs table.")));
    }
    ColumnWriter *colWriter = New(context) ORCColWriter(context, rel, destRel, conn, parsig);
    colWriter->init(indexInsertInfo);
    return colWriter;
}

static void parseParsig(Relation rel, const char *partdir, Datum *values, bool *nulls)
{
    List *partList = ((ValuePartitionMap *)rel->partMap)->partList;
    AttrNumber partColId;
    int curPartExprLength = 0;
    char *parsigs = (char *)palloc0(MAX_PARSIGS_LENGTH);
    char *curPartExpr = (char *)palloc0(MAX_PARSIG_LENGTH);
    char *curpos = NULL;

    /* the partdir is like "c2=2/", so we must copy the strlen("c2=2/") - 1. */
    errno_t rc = strncpy_s(parsigs, MAX_PARSIGS_LENGTH, partdir, strlen(partdir) - 1);
    CHECK_PARTITION_SIGNATURE(rc, partdir);
    curpos = parsigs;

    for (int i = 0; i < list_length(partList); i++) {
        partColId = list_nth_int(partList, i);

        if (i == list_length(partList) - 1) {
            Form_pg_attribute att = rel->rd_att->attrs[partColId - 1];
            const char *partColValue = (char *)strchr(curpos, '=') + 1;

            if (strncmp(partColValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH) == 0) {
                nulls[partColId - 1] = true;
                values[partColId - 1] = 0;
            } else {
                nulls[partColId - 1] = false;
                values[partColId - 1] = GetDatumFromString(att->atttypid, att->atttypmod, UriDecode(partColValue));
            }

            break;
        }

        /* Buffer for current partition expression */
        curPartExprLength = strpos(curpos, "/");
        rc = memset_s(curPartExpr, MAX_PARSIG_LENGTH, 0, MAX_PARSIG_LENGTH);
        securec_check(rc, "\0", "\0");
        Assert(curPartExprLength > 0);
        rc = strncpy_s(curPartExpr, MAX_PARSIG_LENGTH, curpos, curPartExprLength);
        CHECK_PARTITION_SIGNATURE(rc, curpos);
        {
            Form_pg_attribute att = rel->rd_att->attrs[partColId - 1];
            const char *partColValue = (char *)strchr(curPartExpr, '=') + 1;

            if (strncmp(partColValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH) == 0) {
                nulls[partColId - 1] = true;
                values[partColId - 1] = 0;
            } else {
                nulls[partColId - 1] = false;
                values[partColId - 1] = GetDatumFromString(att->atttypid, att->atttypmod, UriDecode(partColValue));
            }
        }

        curpos = (char *)curpos + curPartExprLength + 1;
    }

    pfree_ext(parsigs);
    pfree_ext(curPartExpr);
}
}  // namespace writer

uint64 EstimateBufferRows(TupleDesc desc)
{
    return DEFAULT_ROWS_CHECK;
}
}  // namespace dfs
