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
 * -------------------------------------------------------------------------
 *
 *  subpartitionpruning.cpp
 *        data subpartitionpartition
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/util/subpartitionpruning.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/index.h"
#include "catalog/pg_partition_fn.h"
#include "optimizer/pruning.h"
#include "utils/partitionmap_gs.h"
#include "utils/rel_gs.h"

SubPartitionPruningResult* getSubPartitionFullPruningResult(Relation relation)
{
    RangePartitionMap* rangePartitionMap = NULL;
    ListPartitionMap* listPartitionMap = NULL;
    HashPartitionMap* hashPartitionMap = NULL;
    int i = 0;

    if (!PointerIsValid(relation) || !PointerIsValid(relation->partMap)) {
        return NULL;
    }

    Assert(relation->partMap->type == PART_TYPE_RANGE ||
        relation->partMap->type == PART_TYPE_LIST ||
        relation->partMap->type == PART_TYPE_HASH ||
        relation->partMap->type == PART_TYPE_INTERVAL);

    incre_partmap_refcount(relation->partMap);

    SubPartitionPruningResult* subPartPruningRes = makeNode(SubPartitionPruningResult);
    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        rangePartitionMap = (RangePartitionMap *)relation->partMap;

        /* construct range bitmap */
        for (i = 0; i < rangePartitionMap->rangeElementsNum; i++) {
            int partitionno = rangePartitionMap->rangeElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitionnos =
                lappend_int(subPartPruningRes->ls_selectedSubPartitionnos, partitionno);
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        listPartitionMap = (ListPartitionMap *)relation->partMap;

        for (i = 0; i < listPartitionMap->listElementsNum; i++) {
            int partitionno = listPartitionMap->listElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitionnos =
                lappend_int(subPartPruningRes->ls_selectedSubPartitionnos, partitionno);
        }
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        hashPartitionMap = (HashPartitionMap *)relation->partMap;

        for (i = 0; i < hashPartitionMap->hashElementsNum; i++) {
            int partitionno = hashPartitionMap->hashElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitionnos =
                lappend_int(subPartPruningRes->ls_selectedSubPartitionnos, partitionno);
        }
    }

    decre_partmap_refcount(relation->partMap);
    return subPartPruningRes;
}

SubPartitionPruningResult* PreGetSubPartitionFullPruningResult(Relation relation, Oid partitionid)
{
    Partition part = partitionOpen(relation, partitionid, NoLock);
    Relation partRelation = partitionGetRelation(relation, part);

    SubPartitionPruningResult *subPartPruningRes = getSubPartitionFullPruningResult(partRelation);

    releaseDummyRelation(&partRelation);
    partitionClose(relation, part, NoLock);

    return subPartPruningRes;
}

PruningResult* getFullPruningResult(Relation relation)
{
    /* construct PrunningResult */
    PruningResult* pruningRes = NULL;
    RangePartitionMap* rangePartitionMap = NULL;
    ListPartitionMap* listPartitionMap = NULL;
    HashPartitionMap* hashPartitionMap = NULL;
    int i = 0;

    if (!PointerIsValid(relation) || !PointerIsValid(relation->partMap)) {
        return NULL;
    }

    AssertEreport(relation->partMap->type == PART_TYPE_RANGE ||
        relation->partMap->type == PART_TYPE_LIST ||
        relation->partMap->type == PART_TYPE_HASH ||
        relation->partMap->type == PART_TYPE_INTERVAL,
        MOD_OPT,
        "Unexpected partition map type: expecting RANGE or INTERVAL");
    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_FULL;
    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        rangePartitionMap = (RangePartitionMap*)relation->partMap;

        /* construct range bitmap */
        for (i = 0; i < rangePartitionMap->rangeElementsNum; i++) {
            int partitionno = rangePartitionMap->rangeElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
            pruningRes->ls_selectedPartitionnos = lappend_int(pruningRes->ls_selectedPartitionnos, partitionno);
        }
        if (relation->partMap->type != PART_TYPE_INTERVAL) {
            pruningRes->intervalOffset = 0;
            pruningRes->intervalSelectedPartitions = NULL;
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        listPartitionMap = (ListPartitionMap*)relation->partMap;
        for (i = 0; i < listPartitionMap->listElementsNum; i++) {
            int partitionno = listPartitionMap->listElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
            pruningRes->ls_selectedPartitionnos = lappend_int(pruningRes->ls_selectedPartitionnos, partitionno);
        }
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        hashPartitionMap = (HashPartitionMap*)relation->partMap;
        for (i = 0; i < hashPartitionMap->hashElementsNum; i++) {
            int partitionno = hashPartitionMap->hashElements[i].partitionno;
            if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
                Assert(partitionno > 0);
            }
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
            pruningRes->ls_selectedPartitionnos = lappend_int(pruningRes->ls_selectedPartitionnos, partitionno);
        }
    }

    return pruningRes;
}

/*
 * Support partiton index unusable.
 * check if the partition index is unusable.
 */
bool checkPartitionIndexUnusable(Oid indexOid, int partItrs, PruningResult* pruning_result)
{
    Oid heapRelOid;
    Relation indexRel, heapRel;
    bool partitionIndexUnusable = true;
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    List* part_seqs = pruning_result->ls_rangeSelectedPartitions;
    List* partitionnos = pruning_result->ls_selectedPartitionnos;
    Assert(list_length(part_seqs) == list_length(partitionnos));

    if (pruning_result->expr == NULL) {
        if (PointerIsValid(part_seqs))
            AssertEreport(partItrs == part_seqs->length, MOD_OPT,
                          "The number of partitions does not match that of pruning result.");
    }
    if (!OidIsValid(indexOid)) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
             errmsg("invalid index oid to check for unusability")));
    }

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    heapRelOid = IndexGetRelation(indexOid, false);
    heapRel = relation_open(heapRelOid, NoLock);
    indexRel = relation_open(indexOid, NoLock);
    if (RelationIsGlobalIndex(indexRel)) {
        partitionIndexUnusable = indexRel->rd_index->indisusable;
        relation_close(heapRel, NoLock);
        relation_close(indexRel, NoLock);
        return partitionIndexUnusable;
    }

    if (!RelationIsPartitioned(heapRel) || !RelationIsPartitioned(indexRel) ||
        (heapRel->partMap->type != PART_TYPE_RANGE &&
        heapRel->partMap->type != PART_TYPE_INTERVAL &&
        heapRel->partMap->type != PART_TYPE_LIST &&
        heapRel->partMap->type != PART_TYPE_HASH)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation %s is not partitioned when check partition index", RelationGetRelationName(heapRel))));
    }

    forboth (cell1, part_seqs, cell2, partitionnos) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell1);
        int partitionno = lfirst_int(cell2);
        Relation tablepartrel = NULL;

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq, partitionno);
        tablepart = PartitionOpenWithPartitionno(heapRel, tablepartitionid, partitionno, NoLock);

        /* get index partition and add it to a list for following scan */
        if (RelationIsSubPartitioned(heapRel)) {
            ListCell *lc1 = NULL;
            ListCell *lc2 = NULL;
            tablepartrel = partitionGetRelation(heapRel, tablepart);
            SubPartitionPruningResult *subPartPruning =
                GetSubPartitionPruningResult(pruning_result->ls_selectedSubPartitions, partSeq, partitionno);
            List *subpartList = subPartPruning->ls_selectedSubPartitions;
            List* subpartitionnos = subPartPruning->ls_selectedSubPartitionnos;
            Assert(list_length(subpartList) == list_length(subpartitionnos));

            forboth (lc1, subpartList, lc2, subpartitionnos)
            {
                int subpartSeq = lfirst_int(lc1);
                int subpartitionno = lfirst_int(lc2);
                Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq, subpartitionno);
                Partition subpart =
                    PartitionOpenWithPartitionno(tablepartrel, subpartitionid, subpartitionno, NoLock);

                partitionIndexOidList = PartitionGetPartIndexList(subpart);
                if (!PointerIsValid(partitionIndexOidList)) {
                    ereport(ERROR,
                            (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
                }
                indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
                indexpartition = partitionOpen(indexRel, indexpartitionid, NoLock);

                list_free_ext(partitionIndexOidList);

                // found a unusable index partition
                if (!indexpartition->pd_part->indisusable) {
                    partitionIndexUnusable = false;
                    partitionClose(indexRel, indexpartition, NoLock);
                    partitionClose(tablepartrel, subpart, NoLock);
                    break;
                }

                partitionClose(indexRel, indexpartition, NoLock);
                partitionClose(tablepartrel, subpart, NoLock);
            }

            releaseDummyRelation(&tablepartrel);
            partitionClose(heapRel, tablepart, NoLock);
            if (!partitionIndexUnusable)
                break;
        } else {
            partitionIndexOidList = PartitionGetPartIndexList(tablepart);
            if (!PointerIsValid(partitionIndexOidList)) {
            ereport(ERROR, (errmodule(MOD_OPT),
                            errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
            }
            indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
            list_free_ext(partitionIndexOidList);
            indexpartition = partitionOpen(indexRel, indexpartitionid, NoLock);
            // found a unusable index partition
            if (!indexpartition->pd_part->indisusable) {
                partitionIndexUnusable = false;
                partitionClose(indexRel, indexpartition, NoLock);
                partitionClose(heapRel, tablepart, NoLock);
                break;
            }

            partitionClose(indexRel, indexpartition, NoLock);
            partitionClose(heapRel, tablepart, NoLock);
        }
    }

    relation_close(heapRel, NoLock);
    relation_close(indexRel, NoLock);

    return partitionIndexUnusable;
}

static IndexesUsableType GetIndexesUsableType(int usable_partition_num, int unusable_partition_num, int iterators)
{
    IndexesUsableType ret;

    if (usable_partition_num == iterators) {
        ret = INDEXES_FULL_USABLE;
    } else if (usable_partition_num > 0 && unusable_partition_num > 0) {
        ret = INDEXES_PARTIAL_USABLE;
    } else {
        ret = INDEXES_NONE_USABLE;
    }

    return ret;
}

static IndexesUsableType eliminate_subpartition_index_unusable(Relation heapRel, Relation indexRel,
            PruningResult* inputPruningResult, PruningResult** indexUsablePruningResult,
            PruningResult** indexUnusablePruningResult)
{
    PruningResult* outIndexUsable_pr = NULL;
    PruningResult* outIndexUnusable_pr = NULL;
    Bitmapset* outIndexUsable_bm = NULL;
    Oid indexOid = RelationGetRelid(indexRel);

    List* part_seqs = inputPruningResult->ls_rangeSelectedPartitions;
    List* partitionnos = inputPruningResult->ls_selectedPartitionnos;
    Assert(list_length(part_seqs) == list_length(partitionnos));
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    bool unusable = false;

    // first copy out 2 copies
    outIndexUsable_pr = copyPruningResult(inputPruningResult);
    outIndexUnusable_pr = copyPruningResult(inputPruningResult);

    // get the bm of outIndexUsable_pr, as we want to delete from it.
    outIndexUsable_bm = outIndexUsable_pr->bm_rangeSelectedPartitions;
    // free the bm of outIndexUnusable,
    // as we remove from outIndexUsable and add into outIndexUnusable
    bms_free_ext(outIndexUnusable_pr->bm_rangeSelectedPartitions);
    outIndexUnusable_pr->bm_rangeSelectedPartitions = NULL;

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    // this is the scaning loop for selected partitions
    forboth (cell1, part_seqs, cell2, partitionnos) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        Relation tablepartrel = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell1);
        int partitionno = lfirst_int(cell2);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq, partitionno);
        tablepart = PartitionOpenWithPartitionno(heapRel, tablepartitionid, partitionno, NoLock);
        tablepartrel = partitionGetRelation(heapRel, tablepart);

        /* get index partition and add it to a list for following scan */
        ListCell *lc1 = NULL;
        ListCell *lc2 = NULL;
        SubPartitionPruningResult *subPartPruning =
            GetSubPartitionPruningResult(inputPruningResult->ls_selectedSubPartitions, partSeq, partitionno);
        List *subPartList = subPartPruning->ls_selectedSubPartitions;
        List *subpartitionnos = subPartPruning->ls_selectedSubPartitionnos;
        Assert(list_length(subPartList) == list_length(subpartitionnos));

        forboth (lc1, subPartList, lc2, subpartitionnos)
        {
            int subPartSeq = lfirst_int(lc1);
            int subpartitionno = lfirst_int(lc2);
            Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subPartSeq, subpartitionno);
            Partition subpart =
                PartitionOpenWithPartitionno(tablepartrel, subpartitionid, subpartitionno, NoLock);

            partitionIndexOidList = PartitionGetPartIndexList(subpart);
            if (!PointerIsValid(partitionIndexOidList)) {
                ereport(ERROR,
                        (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
            }
            indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
            indexpartition = partitionOpen(indexRel, indexpartitionid, NoLock);
            // found a unusable index partition
            if (!indexpartition->pd_part->indisusable) {
                unusable = true;
                partitionClose(indexRel, indexpartition, NoLock);
                partitionClose(tablepartrel, subpart, NoLock);
                list_free_ext(partitionIndexOidList);
                break;
            }

            list_free_ext(partitionIndexOidList);
            partitionClose(indexRel, indexpartition, NoLock);
            partitionClose(tablepartrel, subpart, NoLock);
        }

        releaseDummyRelation(&tablepartrel);
        partitionClose(heapRel, tablepart, NoLock);
    }

    relation_close(heapRel, NoLock);
    relation_close(indexRel, NoLock);
    if (unusable) {
        outIndexUnusable_pr = copyPruningResult(inputPruningResult);
        if (indexUnusablePruningResult != NULL) {
            *indexUnusablePruningResult = outIndexUnusable_pr;
        }
        return INDEXES_NONE_USABLE;
    } else {
        outIndexUsable_pr = copyPruningResult(inputPruningResult);
        if (indexUsablePruningResult != NULL) {
            *indexUsablePruningResult = outIndexUsable_pr;
        }
        return INDEXES_FULL_USABLE;
    }
}


/*
 * @@GaussDB@@
 * Brief
 * Description	: wipe out partitions whose local indexes are unusable.
 * return value:  return a pruning result without the wiped, the wiped are output as unusableIndexPruningResult
 */
IndexesUsableType eliminate_partition_index_unusable(Relation heapRel, Relation indexRel,
    PruningResult* inputPruningResult, PruningResult** indexUsablePruningResult,
    PruningResult** indexUnusablePruningResult)
{
    IndexesUsableType ret;
    int usable_partition_num = 0;
    int unusable_partition_num = 0;
    Bitmapset* outIndexUsable_bm = NULL;
    Bitmapset* outIndexUnusable_bm = NULL;
    PruningResult* outIndexUsable_pr = NULL;
    PruningResult* outIndexUnusable_pr = NULL;
    int iterators = bms_num_members(inputPruningResult->bm_rangeSelectedPartitions);
    List* part_seqs = inputPruningResult->ls_rangeSelectedPartitions;
    List* partitionnos = inputPruningResult->ls_selectedPartitionnos;
    Assert(list_length(part_seqs) == list_length(partitionnos));
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    Oid indexOid = RelationGetRelid(indexRel);

    // first copy out 2 copies
    outIndexUsable_pr = copyPruningResult(inputPruningResult);
    outIndexUnusable_pr = copyPruningResult(inputPruningResult);

    // get the bm of outIndexUsable_pr, as we want to delete from it.
    outIndexUsable_bm = outIndexUsable_pr->bm_rangeSelectedPartitions;
    // free the bm of outIndexUnusable,
    // as we remove from outIndexUsable and add into outIndexUnusable
    bms_free_ext(outIndexUnusable_pr->bm_rangeSelectedPartitions);
    outIndexUnusable_pr->bm_rangeSelectedPartitions = NULL;

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    // this is the scaning loop for selected partitions
    forboth (cell1, part_seqs, cell2, partitionnos) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell1);
        int partitionno = lfirst_int(cell2);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq, partitionno);
        tablepart = PartitionOpenWithPartitionno(heapRel, tablepartitionid, partitionno, NoLock);

        /* get index partition and add it to a list for following scan */
        partitionIndexOidList = PartitionGetPartIndexList(tablepart);
        if (!PointerIsValid(partitionIndexOidList)) {
            ereport(ERROR, (errmodule(MOD_OPT),
                            errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
        }
        indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
        list_free_ext(partitionIndexOidList);
        indexpartition = partitionOpen(indexRel, indexpartitionid, NoLock);
        // found a unusable index partition
        if (!indexpartition->pd_part->indisusable) {
            // delete partSeq from usable and add into unusable
            if (!bms_is_member(partSeq, outIndexUsable_bm) || bms_is_member(partSeq, outIndexUnusable_bm)) {
                ereport(ERROR,
                            (errmodule(MOD_OPT),
                             (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                              errmsg("bit map error when searching for unusable index partition"))));
            }
            outIndexUsable_bm = bms_del_member(outIndexUsable_bm, partSeq);
            outIndexUnusable_bm = bms_add_member(outIndexUnusable_bm, partSeq);
        }

        /*
         * Already hold parent table lock, it's safe to release lock.
         */
        partitionClose(indexRel, indexpartition, NoLock);
        partitionClose(heapRel, tablepart, NoLock);
    }

    // result check
    usable_partition_num = bms_num_members(outIndexUsable_bm);
    unusable_partition_num = bms_num_members(outIndexUnusable_bm);
    if (usable_partition_num + unusable_partition_num != iterators ||
        bms_overlap(outIndexUsable_bm, outIndexUnusable_bm)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("bit map error after searching for unusable index partition")));
    }

    // set the return value
    ret = GetIndexesUsableType(usable_partition_num, unusable_partition_num, iterators);

    // set back the bit map
    if (usable_partition_num > 0) {
        outIndexUsable_pr->bm_rangeSelectedPartitions = outIndexUsable_bm;
        generateListFromPruningBM(outIndexUsable_pr, heapRel->partMap);
        // set the output
        if (indexUsablePruningResult != NULL) {
            *indexUsablePruningResult = outIndexUsable_pr;
        }
    }
    // set back the bit map
    if (unusable_partition_num > 0) {
        outIndexUnusable_pr->bm_rangeSelectedPartitions = outIndexUnusable_bm;
        generateListFromPruningBM(outIndexUnusable_pr, heapRel->partMap);
        // set the output
        if (indexUnusablePruningResult != NULL) {
            *indexUnusablePruningResult = outIndexUnusable_pr;
        }
    }

    relation_close(heapRel, NoLock);
    relation_close(indexRel, NoLock);

    return ret;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: wipe out partitions whose local indexes are unusable.
 * return value:  return a pruning result without the wiped, the wiped are output as unusableIndexPruningResult
 */
IndexesUsableType eliminate_partition_index_unusable(Oid indexOid, PruningResult* inputPruningResult,
    PruningResult** indexUsablePruningResult, PruningResult** indexUnusablePruningResult)
{
    IndexesUsableType ret;
    Oid heapRelOid;
    Relation indexRel, heapRel;
    int iterators = bms_num_members(inputPruningResult->bm_rangeSelectedPartitions);
    List* part_seqs = inputPruningResult->ls_rangeSelectedPartitions;

    if (inputPruningResult->expr == NULL) {
        if (PointerIsValid(part_seqs))
            AssertEreport(part_seqs->length == iterators, MOD_OPT, "");
    }
    // sanity check
    if (!OidIsValid(indexOid)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid index oid to check for unusability")));
    }
    heapRelOid = IndexGetRelation(indexOid, false);

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    heapRel = relation_open(heapRelOid, NoLock);
    indexRel = relation_open(indexOid, NoLock);
    /* Global partition index Just return FULL or NONE */
    if (RelationIsGlobalIndex(indexRel)) {
        ret = indexRel->rd_index->indisusable ? INDEXES_FULL_USABLE : INDEXES_NONE_USABLE;
        relation_close(heapRel, NoLock);
        relation_close(indexRel, NoLock);
        return ret;
    }

    if (!RelationIsPartitioned(heapRel) || !RelationIsPartitioned(indexRel)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation %s is not partitioned", RelationGetRelationName(heapRel))));
    }

    if (RelationIsSubPartitioned(heapRel)) {
        return eliminate_subpartition_index_unusable(heapRel, indexRel,
                    inputPruningResult,
                    indexUsablePruningResult,
                    indexUnusablePruningResult);
    } else {
        return eliminate_partition_index_unusable(heapRel, indexRel,
                    inputPruningResult,
                    indexUsablePruningResult,
                    indexUnusablePruningResult);
    }
}
