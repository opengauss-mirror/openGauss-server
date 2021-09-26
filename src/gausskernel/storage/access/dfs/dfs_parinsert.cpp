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
 * ---------------------------------------------------------------------------------------
 *
 *  dfs_parinsert.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_parinsert.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h"
#include "access/cstore_insert.h"
#include "access/cstore_psort.h"
#include "access/dfs/dfs_insert.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/pg_collation.h"
#include "commands/tablespace.h"
#include "dfsdesc.h"
#include "storage/lmgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/heapam.h"

/*
 * Check partition signature creation exception in case of the content exceeding
 * max allowed partition length
 */
#define check_err_msg                                                                    \
    "The length of the partition directory exceeds the current value(%d) of the option " \
    "\"dfs_partition_directory_length\", change the option to the greater value."
#define CHECK_PARTITION_SIGNATURE_CREATE(rc, attname, dirname) do { \
    if ((rc) == -1)                                                                             \
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),                                             \
                        errmsg(check_err_msg, u_sess->attr.attr_storage.dfs_max_parsig_length), \
                        errdetail("the path name is \"%s=%s/\".", attname, dirname)));          \
} while (0)

/* Partition search cache support */
typedef struct PartitionSearchEntry {
    /* key of cache entry */
    char *parsig;

    /*
     * The value of element pointer could be de-referenced into 'DfsInsert *' or
     * 'PartitionStagingFile *'
     */
    void *element;
} PartitionSearchEntry;

/* Routines for partition search cache */
static void BuildPartitionSearchCache(HTAB **cache, const char *cache_name, long max_nelement);
static uint32 PartitionSearchEntryHashFunc(const void *key, Size keysize);
static int PartitionSearchEntryMatchFunc(const void *key1, const void *key2, Size keySize);
static void *FindPartitionEntry(HTAB *cache, const char *key);
static void InsertPartitionEntry(HTAB *cache, const char *key, void *element);

/* Customized HashFunc for partition search cache */
static uint32 PartitionSearchEntryHashFunc(const void *key, Size keysize)
{
    Assert(keysize > 0 && key != NULL);

    const char *pkey = *(char **)key;
    int s_len = strlen(pkey);

    return (DatumGetUInt32(hash_any((const unsigned char *)pkey, s_len)));
}

/* Customized MatchingFunc for partition search cache */
static int PartitionSearchEntryMatchFunc(const void *key1, const void *key2, Size keySize)
{
    Assert(keySize > 0 && key1 != NULL && key2 != NULL);

    /* de-reference char ** to char * to do comparison */
    const char *pkey1 = *(char **)key1;
    const char *pkey2 = *(char **)key2;
    int pkey1_len = strlen(pkey1);
    int pkey2_len = strlen(pkey2);

    return (strncmp(pkey1, pkey2, (pkey1_len > pkey2_len ? pkey1_len : pkey2_len)));
}

/*
 * brief: Create dynamic hash table for partition search
 * input:
 *    @cache: pointer of hashtable that need to be created
 *    @cache_name: name of cache
 *    @max_nelement: the initial size of hashtable at creating time, please note
 *         that for a shared-memory hashtable, the value needs to be a pretty good
 *         estimate, since we can't expand the table on the fly.  But an unshared
 *         hashtable can be expanded on-the-fly, so it's better for nelem to be
 *         on the small side and let the table grow if it's exceeded.  An overly
 *         large nelem will penalize hash_seq_search speed without buying much.
 *
 * output:
 *    @cache: pointer of hashtable that will be created and also the input
 *            parameter
 *
 * return: none
 */
static void BuildPartitionSearchCache(HTAB **cache, const char *cache_name, long max_nelement)
{
    HASHCTL ctl;
    errno_t rc = EOK;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(char *);
    ctl.entrysize = sizeof(PartitionSearchEntry);
    ctl.hcxt = CurrentMemoryContext;
    ctl.match = (HashCompareFunc)PartitionSearchEntryMatchFunc;
    ctl.hash = (HashValueFunc)PartitionSearchEntryHashFunc;

    *cache = hash_create(cache_name, max_nelement, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

    if (!*cache)
        ereport(PANIC, (errmsg("could not initialize partition pruning result cache.")));
}

/*
 * brief: find the element by given key
 * input:
 *    @cache: pointer of hashtable that we want to search
 *    @key: the entry key that we want to search
 *
 * return: pointer of element that we found by given key, return NULL for not found
 */
static void *FindPartitionEntry(HTAB *cache, const char *key)
{
    bool found = false;
    PartitionSearchEntry *entry = NULL;

    /* Find the entry in the hash table. */
    entry = (PartitionSearchEntry *)hash_search(cache, (void *)&key, HASH_FIND, &found);

    if (found) {
        Assert(entry != NULL);
        return entry->element;
    }

    return NULL;
}

/*
 * brief: insert a K,V pair into partition search cache
 * input:
 *    @cache: pointer of hashtable that we want to search
 *    @key: the entry key that we want to insert
 *    @element: the entry that we want to insert
 *
 * return: none
 */
static void InsertPartitionEntry(HTAB *cache, const char *key, void *element)
{
    bool found = true;
    PartitionSearchEntry *entry = NULL;

    entry = (PartitionSearchEntry *)hash_search(cache, (void *)&key, HASH_ENTER, &found);
    if (!found) {
        Assert(entry != NULL);
        entry->element = element;
    }
}

/*
 * Member function implementation
 */

/*
 * brief: convert datum into string for given attribute
 * input:
 *    @typeOid: atttypid of the attribute
 *    @typeMode: atttypmod of the attribute
 *    @value: datum value of the attribute
 *    @attname: attribute name
 *
 * output:
 *    @buf: buffer to hold the converted result
 *
 * return: none
 */
inline void DfsPartitionInsert::GetCStringFromDatum(Oid typeOid, int typeMode, Datum value, const char *attname,
                                                    char *buf) const
{
    int rc = 0;
    char *raw_valuestr = NULL;
    char *encoded_valuestr = NULL;

    switch (typeOid) {
        /* Numeric data type */
        /* 1. Towards to TINYINT */
        case INT1OID: {
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%d/", attname, DatumGetUInt8(value));
            securec_check_ss(rc, "\0", "\0");
            break;
        }

        /* 2. Towards to SMALLINT */
        case INT2OID: {
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%d/", attname, DatumGetInt16(value));
            securec_check_ss(rc, "\0", "\0");
            break;
        }

        /* 3. Towards to INTEGER */
        case INT4OID: {
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%d/", attname, DatumGetInt32(value));
            securec_check_ss(rc, "\0", "\0");
            break;
        }

        /* 4. Towards to BIGINT */
        case INT8OID: {
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%ld/", attname, DatumGetInt64(value));
            securec_check_ss(rc, "\0", "\0");
            break;
        }

        /* 5. Towards to NUMERIC or DECIMAL */
        case NUMERICOID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(numeric_out, value));
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, raw_valuestr);

            break;
        }

        /* 6. Towards to CHAR */
        case CHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(charout, value));
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 7. Towards to CHAR */
        case BPCHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(bpcharout, value));
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 8. Towards to VARCHAR  */
        case VARCHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(varcharout, value));
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 9. Towards to NVARCHAR  */
        case NVARCHAR2OID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(nvarchar2out, value));
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 10. Towards to TEXT */
        case TEXTOID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(textout, value));
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* Temporal related data types */
        /* 11. Towards to DATE */
        case DATEOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(date_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 12. Towards to TIME */
        case TIMEOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(time_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 13. Towards to TIME WITH TIMEZONE */
        case TIMETZOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(timetz_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 14. Towards to TIMEZONE */
        case TIMESTAMPOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(timestamp_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 15. Towards to TIME WITH TIMEZONE */
        case TIMESTAMPTZOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(timestamptz_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 16. Towards to SMALLDATETIME */
        case SMALLDATETIMEOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(smalldatetime_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* 17. Towards to INTERVAL */
        case INTERVALOID: {
            raw_valuestr = (char *)DirectFunctionCall1Coll(interval_out, DEFAULT_COLLATION_OID, value);
            encoded_valuestr = UriEncode(raw_valuestr);
            rc = sprintf_s(buf, MAX_PARSIG_LENGTH, "%s=%s/", attname, encoded_valuestr);

            break;
        }

        /* more data type supported here */
        default: {
            /*
             * As we already blocked any un-supported datatype at table-creation
             * time, so we shouldn't get here, otherwise the catalog information
             * may gets corrupted.
             */
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                            errmsg("Unsupported data type on column:%s when forming partition signature", attname)));
        }
    }

    /* check partition signature creation */
    if (typeOid == NUMERICOID && raw_valuestr != NULL)
        CHECK_PARTITION_SIGNATURE_CREATE(rc, attname, raw_valuestr);

    if (encoded_valuestr != NULL)
        CHECK_PARTITION_SIGNATURE_CREATE(rc, attname, encoded_valuestr);

    /* free raw_valuestr and encoded_valuestr if needed */
    if (raw_valuestr != NULL)
        pfree_ext(raw_valuestr);

    if (encoded_valuestr != NULL)
        pfree_ext(encoded_valuestr);
}

/*
 * brief: construct partition signature by given values/nulls array
 * input:
 *    @values: datum array for given tuple
 *    @nulls: nulls array for given tuple
 *
 * return: constructed partition signature
 */
inline char *DfsPartitionInsert::FormPartitionSignature(Datum *values, const bool *nulls)
{
    errno_t rc = EOK;
    ListCell *cell = NULL;
    char *curpos = m_parsigs;
    TupleDesc tupdesc = m_relation->rd_att;
    List *partList = ((ValuePartitionMap *)m_relation->partMap)->partList;
    int attno = 0;

    /* Clear m_parsig[] buffer */
    rc = memset_s(m_parsigs, MAX_PARSIGS_LENGTH, 0, MAX_PARSIGS_LENGTH);
    securec_check(rc, "\0", "\0");

    foreach (cell, partList) {
        attno = lfirst_int(cell);
        curpos = (char *)m_parsigs + strlen(m_parsigs);

        if (nulls[attno - 1]) {
            rc = sprintf_s(curpos, MAX_PARSIG_LENGTH - strlen(m_parsigs), "%s=__HIVE_DEFAULT_PARTITION__/",
                           tupdesc->attrs[attno - 1]->attname.data);

            CHECK_PARTITION_SIGNATURE_CREATE(rc, tupdesc->attrs[attno - 1]->attname.data, curpos);
        } else {
            /*
             * The formed partition-signature is written to m_parsig[MAX_PARSIG_LEN],
             * the reason we have to do so is to avoid dynamically allocating string
             * buffer to hold the parsig for current partition which is relative
             * expensive than a pre-allocated buffer.
             */
            GetCStringFromDatum(tupdesc->attrs[attno - 1]->atttypid, 0, values[attno - 1],
                                tupdesc->attrs[attno - 1]->attname.data, curpos);
        }
    }

    return (char *)m_parsigs;
}

/* DfsPartitionInsert constructor */
DfsPartitionInsert::DfsPartitionInsert(Relation relation, bool is_update, Plan *plan, MemInfoArg *ArgmemInfo)
    : m_relation(relation),
      m_resultRelInfo(NULL),
      m_delta(NULL),
      m_values(NULL),
      m_nulls(NULL),
      m_pDfsInserts(NULL),
      m_activePartitionWriters(0)
{
    dfsPartMemInfo = NULL;

    /* init memory for insert. */
    InitInsertPartMemArg(plan, ArgmemInfo);

    m_insert_fn = NULL;
    m_tableDirPath = getDfsStorePath(m_relation);
    m_desc = RelationGetDescr(relation);
    m_dataDestRelation = NULL;
    m_end = false;
    m_colMap = NULL;
    m_numPartitionStagingFiles = 0;
    m_partitionStagingFiles = NULL;
    m_partitionCtx = NULL;
    m_type = TUPLE_SORT;

    m_maxPartitionWriters = (dfsPartMemInfo->MemInsert * 1024L) / (128 * 1024 * 1024);
    m_activePartitionWriters = 0;
    m_parsigs = (char *)palloc0(MAX_PARSIGS_LENGTH);
    m_isUpdate = is_update;

    m_SpilledPartitionSearchCache = NULL;
    m_ActivePartitionSearchCache = NULL;
    m_indexInsertInfo = NULL;
    m_indexInfoOuterDestroy = false;
    elog(LOG, "Loading data to relation %s with Max PartionWriter :%d with maxparsig:%d",
         RelationGetRelationName(m_relation), m_maxPartitionWriters, MAX_PARSIGS_LENGTH);
}

void DfsPartitionInsert::InitInsertPartMemArg(Plan *plan, MemInfoArg *ArgmemInfo)
{
    bool hasPck = false;
    int partialClusterRows = 0;

    /* get cluster key */
    partialClusterRows = RelationGetPartialClusterRows(m_relation);
    if (m_relation->rd_rel->relhasclusterkey) {
        hasPck = true;
    }

    dfsPartMemInfo = (MemInfoArg *)palloc0(sizeof(struct MemInfoArg));
    /*
     * plan: Plan is for insert operator to calculate the memory.
     * ArgmemInfo: ArgmemInfo is for update operator to Pass the memory parameters.
     * others: not Memory management will be in.
     * dfs part is value part. the partitionNum is the max partition write number.
     */
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        if (!hasPck) {
            dfsPartMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                    : plan->operatorMemKB[0];
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsPartInsert(init plan) mem is not engough for the basic use: workmem is : %dKB, can spread "
                    "maxMem is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }
            dfsPartMemInfo->MemSort = 0;
        } else {
            /* if has pck, we should first promise the insert memory, operatorMemKB[0] = insert mem + sort mem.
             * At first, Insert memory : must > 128mb to promise the basic use on DFS insert.
             * For part table, insert memory usually is 2G. insert memory must  > 128mb to promise the basic use.
             * Sort memory : must >10mb to promise the basic use on sort.
             */
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsPartInsert(init plan pck) mem is not engough for the basic use: workmem is : %dKB, can spread "
                    "maxMem is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }
            dfsPartMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                    : plan->operatorMemKB[0];
            if (dfsPartMemInfo->MemInsert > PARTITION_MAX_SIZE)
                dfsPartMemInfo->MemInsert = PARTITION_MAX_SIZE;

            dfsPartMemInfo->MemSort = plan->operatorMemKB[0] - dfsPartMemInfo->MemInsert;
            if (dfsPartMemInfo->MemSort < SORT_MIM_MEM) {
                dfsPartMemInfo->MemSort = SORT_MIM_MEM;
                if (dfsPartMemInfo->MemInsert >= DFS_MIN_MEM_SIZE)
                    dfsPartMemInfo->MemInsert = dfsPartMemInfo->MemInsert - SORT_MIM_MEM;
            }
        }
        dfsPartMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
        dfsPartMemInfo->spreadNum = 0;
        dfsPartMemInfo->partitionNum = dfsPartMemInfo->MemInsert / DFS_MIN_MEM_SIZE;
        MEMCTL_LOG(
            DEBUG2,
            "DfsPartInsert(init plan): workmem is : %dKB, all paritition sort workmem: %dKB,can spread maxMem is %dKB.",
            dfsPartMemInfo->MemInsert, dfsPartMemInfo->MemSort, dfsPartMemInfo->canSpreadmaxMem);
    } else if (ArgmemInfo != NULL) {
        dfsPartMemInfo->canSpreadmaxMem = ArgmemInfo->canSpreadmaxMem;
        dfsPartMemInfo->MemInsert = ArgmemInfo->MemInsert;
        dfsPartMemInfo->MemSort = ArgmemInfo->MemSort;
        dfsPartMemInfo->spreadNum = ArgmemInfo->spreadNum;
        dfsPartMemInfo->partitionNum = dfsPartMemInfo->MemInsert / DFS_MIN_MEM_SIZE;
        MEMCTL_LOG(
            DEBUG2,
            "DfsPartInsert(init ArgmemInfo): workmem is : %dKB, all paritition sort workmem: %dKB,can spread maxMem is "
            "%dKB.",
            dfsPartMemInfo->MemInsert, dfsPartMemInfo->MemSort, dfsPartMemInfo->canSpreadmaxMem);
    } else {
        /*
         * For static load, a single partition of sort Mem is 512MB, and there is no need to subdivide sort Mem.
         * So, set the partitionNum is 1 for all partition table.
         */
        dfsPartMemInfo->canSpreadmaxMem = 0;
        dfsPartMemInfo->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        dfsPartMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        dfsPartMemInfo->spreadNum = 0;
        dfsPartMemInfo->partitionNum = 1;
    }
}

void DfsPartitionInsert::BeginBatchInsert(int type, ResultRelInfo *resultRelInfo)
{
    /* Just assert PAX file format and no need setup ORC writer. */
    Assert(RelationIsPAXFormat(m_relation) && RelationIsValuePartitioned(m_relation));

    /* Create empty zeroed partitionwriter desc and initialize it later */
    m_pDfsInserts = (DfsInsert **)palloc0(sizeof(DfsInsert *) * m_maxPartitionWriters);

    for (int i = 0; i < m_maxPartitionWriters; i++)
        m_pDfsInserts[i] = NULL;

    /* Initialize the buffer of tuple during VectorBatch => tuple on insert ... select ... */
    m_delta = heap_open(m_relation->rd_rel->reldeltarelid, RowExclusiveLock);
    m_values = (Datum *)palloc0(sizeof(Datum) * RelationGetNumberOfAttributes(m_relation));
    m_nulls = (bool *)palloc0(sizeof(bool) * RelationGetNumberOfAttributes(m_relation));

    m_type = TUPLE_SORT;
    m_resultRelInfo = resultRelInfo;
    if (m_indexInsertInfo == NULL) {
        m_indexInsertInfo = BuildIndexInsertInfo(m_relation, m_resultRelInfo);
    }

    m_partitionCtx = AllocSetContextCreate(CurrentMemoryContext, "dfs partition insert memory context",
                                           ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT,
                                           dfsPartMemInfo->MemInsert * 1024L);
}

void DfsPartitionInsert::Destroy()
{
    /* clean up Partition Writer */
    if (m_pDfsInserts) {
        for (int i = 0; i < m_maxPartitionWriters; i++) {
            DfsInsert *pInsert = m_pDfsInserts[i];
            if (pInsert != NULL) {
                pInsert->Destroy();
                delete pInsert;
                m_pDfsInserts[i] = NULL;
            }
        }
        pfree_ext(m_pDfsInserts);
    }

    /*
     * Clean up partition buf files, note here using revert order to free partition
     * buf file handlers to improve the searching speed of targert pointer inside
     * memory allocator.
     *
     * Ideally, MemroyAllocator should be further improved to let users do NOT care
     * too much about the palloc()'ed arrary in any order, which will be improved in
     * the future.
     */
    if (m_partitionStagingFiles) {
        for (int i = m_numPartitionStagingFiles - 1; i >= 0; i--) {
            PartitionStagingFile *_psf = m_partitionStagingFiles[i];
            if (_psf != NULL) {
                BufFileClose(_psf->sfile);
                pfree(_psf);
                m_partitionStagingFiles[i] = NULL;
            }
        }
        pfree(m_partitionStagingFiles);
        m_partitionStagingFiles = NULL;
    }

    if (m_parsigs) {
        pfree(m_parsigs);
        m_parsigs = NULL;
    }

    if (m_tableDirPath) {
        pfree(m_tableDirPath->data);
        m_tableDirPath->data = NULL;
        pfree(m_tableDirPath);
        m_tableDirPath = NULL;
    }

    if (m_delta) {
        heap_close(m_delta, RowExclusiveLock);
        m_delta = NULL;
    }

    if (m_values) {
        pfree(m_values);
        m_values = NULL;
    }

    if (m_nulls) {
        pfree(m_nulls);
        m_nulls = NULL;
    }
    if (dfsPartMemInfo) {
        pfree(dfsPartMemInfo);
        dfsPartMemInfo = NULL;
    }

    if (m_ActivePartitionSearchCache) {
        hash_destroy(m_ActivePartitionSearchCache);
        m_ActivePartitionSearchCache = NULL;
    }

    if (m_SpilledPartitionSearchCache) {
        hash_destroy(m_SpilledPartitionSearchCache);
        m_SpilledPartitionSearchCache = NULL;
    }
}

/*
 * brief: Insert a tuple into partition table
 * input:
 *    @values: datum array for given tuple
 *    @nulls: nulls array for given tuple
 *    @option: option that will passed to heap_insert() for tailing part
 *
 * return: none
 */
void DfsPartitionInsert::TupleInsert(Datum *values, bool *nulls, int option)
{
    char *parsig = NULL;

    /* delta table is not partitioned so insert tuple into delta immediately */
    if (u_sess->attr.attr_storage.cstore_insert_mode == TO_DELTA) {
        if (isEnd() || values == NULL || nulls == NULL)
            return;

        /* insert tuple into delta table */
        HeapTuple tuple = (HeapTuple)tableam_tops_form_tuple(m_delta->rd_att, values, nulls, HEAP_TUPLE);
        (void)tableam_tuple_insert(m_delta, tuple, GetCurrentCommandId(true), option, NULL);

        /* free the temp materialized tuple */
        heap_freetuple(tuple);
        tuple = NULL;

        return;
    }

    /* handle tail data for each partition */
    if (m_end) {
        HandleTailData(option);
        return;
    }

    /* Retrieve partition signature */
    parsig = FormPartitionSignature(values, nulls);

    /* Get proper partition Writer */
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(m_partitionCtx);
    DfsInsert *pInsert = GetDfsInsert(parsig);
    if (pInsert == NULL) {
        PartitionStagingFile *psf = GetPartitionStagingFile(parsig);
        size_t nwrites = 0;

        if (psf == NULL) {
            /*
             * We never insert a tuple into this partition (overflow case), so
             * create a new partition staging file to temporally hold it and also
             * add it to m_partitionStagingFile array.
             */
            /* 1). extend PartitionStagingFile array */
            if (m_numPartitionStagingFiles == 0) {
                m_partitionStagingFiles = (PartitionStagingFile **)palloc0(sizeof(PartitionStagingFile *));
                m_partitionStagingFiles[0] = NULL;
            } else {
                m_partitionStagingFiles = (PartitionStagingFile **)repalloc(m_partitionStagingFiles,
                                                                            (m_numPartitionStagingFiles + 1) *
                                                                                sizeof(PartitionStagingFile *));
            }

            /* 2). create new PartitionStagingFile object */
            m_partitionStagingFiles[m_numPartitionStagingFiles] =
                (PartitionStagingFile *)palloc0(sizeof(PartitionStagingFile));

            psf = m_partitionStagingFiles[m_numPartitionStagingFiles];
            psf->parsig = makeStringInfo();
            appendStringInfo(psf->parsig, "%s", parsig);

            /* Obtain file handler, then write it into temp file */
            psf->sfile = BufFileCreateTemp(false);
            if (!psf->sfile)
                ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errcode_for_file_access(),
                                errmsg("could not write to partition staging temporary file while loading partition %s",
                                       parsig)));

            /* create cache entry if first enter here */
            if (!m_SpilledPartitionSearchCache)
                BuildPartitionSearchCache(&m_SpilledPartitionSearchCache, "Spilled Partition Cache",
                                          4096);  // max element is 4096

            /*
             * insert this new partition key, please note we pass the key's
             * pointer rather than key content to avoid key content copy overhead,
             * as in some corner case the key could consist of many bytes.
             */
            InsertPartitionEntry(m_SpilledPartitionSearchCache, psf->parsig->data, (void *)psf);

            /* increase the # of spilled files(partition) */
            m_numPartitionStagingFiles++;
        }
        (void)MemoryContextSwitchTo(oldMemoryContext);

        HeapTuple tuple = (HeapTuple)tableam_tops_form_tuple(m_delta->rd_att, values, nulls, HEAP_TUPLE);

        nwrites = BufFileWrite(psf->sfile, tuple, HEAPTUPLESIZE + tuple->t_len);
        if (nwrites != (size_t)HEAPTUPLESIZE + tuple->t_len) {
            ereport(
                ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR), errcode_for_file_access(),
                 errmsg("error write temp file while loading partitioned DFS table, %lu bytes written but expected %lu",
                        nwrites, HEAPTUPLESIZE + tuple->t_len)));
        }

        /* free the temp materialized tuple */
        heap_freetuple(tuple);
        tuple = NULL;

        /*
         * After dump inserted tuple into buffile, we are done with this tuple and
         * return to caller immediately, the actual flush happens later.
         */
        return;
    }
    (void)MemoryContextSwitchTo(oldMemoryContext);

    pInsert->TupleInsert(values, nulls, option);
}

/* dump each partition writer's data and buffile's writer */
void DfsPartitionInsert::HandleTailData(int options)
{
    /* Handle PartitionWriter's tailing data */
    for (int i = 0; i < m_maxPartitionWriters; i++) {
        DfsInsert *pInsert = m_pDfsInserts[i];
        if (pInsert != NULL) {
            pInsert->SetEndFlag();
            pInsert->TupleInsert(NULL, NULL, options);
            pInsert->Destroy();
            delete pInsert;
            m_pDfsInserts[i] = NULL;
        }
    }

    /* Handle Staging files for swapped partition data */
    elog(LOG, "-> Starting to reload partition staging files into DFS table with total %d spilled partitions",
         m_numPartitionStagingFiles);
    Datum *_values = (Datum *)palloc0(sizeof(Datum) * RelationGetNumberOfAttributes(m_relation));
    bool *_nulls = (bool *)palloc0(sizeof(bool) * RelationGetNumberOfAttributes(m_relation));
    uint32 t_len = 0;
    uint32 t_readlen = 0;
    size_t nread = 0;

    for (int i = 0; i < m_numPartitionStagingFiles; i++) {
        PartitionStagingFile *psf = m_partitionStagingFiles[i];

        Assert(psf && psf->parsig);
        ereport(LOG, (errmsg("    -> Reload [%d/%d] spilled partition Partition:%s", (i + 1),
                             m_numPartitionStagingFiles, psf->parsig->data)));

        DfsInsert *pInsert = New(CurrentMemoryContext) DfsInsert(m_relation, m_isUpdate, psf->parsig->data);
        /* data redistribution for DFS table. */
        pInsert->setDataDestRel(getDataDestRel());
        pInsert->RegisterInsertPendingFunc(m_insert_fn);
        pInsert->setIndexInsertInfo(m_indexInsertInfo);
        pInsert->BeginBatchInsert(m_type, m_resultRelInfo);

        /* rewind buffer file to beginging to start file to table loading */
        if (BufFileSeek(psf->sfile, 0, 0L, SEEK_SET) != 0)
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errcode_for_file_access(),
                            errmsg("could not rewind DFS Partition Loading temporary file: %m")));

        /* Read content of this buffile */
        while (true) {
            nread = BufFileRead(psf->sfile, &t_len, sizeof(uint32));
            if (nread == 0) /* end of file */
                break;

            HeapTuple tuple = (HeapTupleData *)heaptup_alloc(HEAPTUPLESIZE + t_len);
            t_readlen = HEAPTUPLESIZE + t_len - sizeof(uint32);
            tuple->t_len = t_len;

            /* for whole HeapTuple data read but exclude first fields HeapTuple->t_len */
            nread = BufFileRead(psf->sfile, (void *)((char *)tuple + sizeof(uint32)), t_readlen);
            if (nread != t_readlen) {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR), errcode_for_file_access(),
                     errmsg("error read staging temp file for partitioned table loading, expected:%u, actual read %lu",
                            t_readlen, nread)));
            }

            /*
             * We have to reset tuple->t_data as the pointer value read from disk
             * represents the address of loading time which is meaningless here,
             * so adjust it to up-to-date value.
             */
            tuple->t_data = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);

            /* Deforming the tuple into values/nulls array for ORC writer insert */
            heap_deform_tuple(tuple, m_desc, _values, _nulls);
            pInsert->TupleInsert(_values, _nulls, options);

            /* Free the temp materialized tuple */
            heap_freetuple(tuple);
            tuple = NULL;
        }

        /* handle tailing unflushed tuples */
        pInsert->SetEndFlag();
        pInsert->TupleInsert(NULL, NULL, options);
        pInsert->Destroy();
        delete pInsert;
    }

    if (_values != NULL)
        pfree_ext(_values);

    if (_nulls != NULL)
        pfree_ext(_nulls);

    clearIndexInsert();

    return;
}

/*
 * brief: Search a DfsInsert operator by partition signature, create one if max
 *        allowed dfs_insert is not exceeded
 * input:
 *    @parsig: search key of partition
 *
 * return: DfsInsert * for expected parsig, return NULL if not found
 */
inline DfsInsert *DfsPartitionInsert::GetDfsInsert(const char *parsig)
{
    DfsInsert *pInsert = NULL;

    if (!m_ActivePartitionSearchCache) {
        /* Create *active* partition search cache if not exists */
        BuildPartitionSearchCache(&m_ActivePartitionSearchCache, "Active Partition Entry Cache", m_maxPartitionWriters);
    }

    /* Search possible cached dfs_insert from cache */
    pInsert = (DfsInsert *)FindPartitionEntry(m_ActivePartitionSearchCache, parsig);
    /* Return it directly if we find one from cache */
    if (pInsert != NULL) {
        return pInsert;
    }

    /*
     * Return NULL if the required partition dfs_insert is not in cache and we
     * already arrived the max allowed dfs insert writers
     */
    if (m_activePartitionWriters == m_maxPartitionWriters) {
        return NULL;
    }

    /* We have the slot but related writer is not setup */
    pInsert = New(CurrentMemoryContext) DfsInsert(m_relation, m_isUpdate, parsig, NULL, dfsPartMemInfo);

    /* Data redistribution for DFS table. */
    pInsert->setDataDestRel(getDataDestRel());
    pInsert->RegisterInsertPendingFunc(m_insert_fn);
    pInsert->setIndexInsertInfo(m_indexInsertInfo);
    pInsert->BeginBatchInsert(m_type, m_resultRelInfo);
    m_pDfsInserts[m_activePartitionWriters] = pInsert;

    /* Update *active* # of partition writer */
    m_activePartitionWriters++;

    /* Insert this dfs_insert into cache */
    InsertPartitionEntry(m_ActivePartitionSearchCache, parsig, (void *)pInsert);

    return pInsert;
}

/*
 * brief: Get Partition Staging file for overflowed partition writer during data
 *        loading, return NULL if the staging file is not found.
 * input:
 *    @parsig: search key of partition
 *
 * return: PartitionStagingFile * for expected parsig, return NULL if not found
 */
PartitionStagingFile *DfsPartitionInsert::GetPartitionStagingFile(const char *parsig)
{
    /* Get the partition staging file from cache */
    if (m_SpilledPartitionSearchCache) {
        return (PartitionStagingFile *)FindPartitionEntry(m_SpilledPartitionSearchCache, parsig);
    }

    return NULL;
}

/*
 * brief: insert into partitioned HDFS table by given batch
 * input:
 *    @batch: batch that needs to be parsed and inserted
 *    @option: option that will passed to heap_insert() for tailing part
 *
 * return: none
 */
void DfsPartitionInsert::BatchInsert(VectorBatch *batch, int option)
{
    if (m_end || BatchIsNull(batch)) {
        HandleTailData(option);
        return;
    }

    /* batch => tuple conversion */
    for (int row = 0; row < batch->m_rows; row++) {
        for (int col = 0; col < batch->m_cols; col++) {
            ScalarVector *sv = &(batch->m_arr[col]);
            Oid typid = sv->m_desc.typeId;

            m_nulls[col] = IS_NULL(sv->m_flag[row]);
            if (!m_nulls[col])
                m_values[col] = convertScalarToDatum(typid, sv->m_vals[row]);
            else
                m_values[col] = (Datum)0;
        }

        TupleInsert(m_values, m_nulls, option);
    }
}
