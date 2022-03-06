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

    SubPartitionPruningResult* subPartPruningRes = makeNode(SubPartitionPruningResult);
    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        rangePartitionMap = (RangePartitionMap *)relation->partMap;

        /* construct range bitmap */
        for (i = 0; i < rangePartitionMap->rangeElementsNum; i++) {
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        listPartitionMap = (ListPartitionMap *)relation->partMap;

        for (i = 0; i < listPartitionMap->listElementsNum; i++) {
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
        }
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        hashPartitionMap = (HashPartitionMap *)relation->partMap;

        for (i = 0; i < hashPartitionMap->hashElementsNum; i++) {
            subPartPruningRes->bm_selectedSubPartitions =
                bms_add_member(subPartPruningRes->bm_selectedSubPartitions, i);
            subPartPruningRes->ls_selectedSubPartitions = lappend_int(subPartPruningRes->ls_selectedSubPartitions, i);
        }
    }

    return subPartPruningRes;
}

SubPartitionPruningResult* PreGetSubPartitionFullPruningResult(Relation relation, Oid partitionid)
{
    Partition part = partitionOpen(relation, partitionid, AccessShareLock);
    Relation partRelation = partitionGetRelation(relation, part);

    SubPartitionPruningResult *subPartPruningRes = getSubPartitionFullPruningResult(partRelation);

    releaseDummyRelation(&partRelation);
    partitionClose(relation, part, AccessShareLock);

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
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
        }
        if (relation->partMap->type != PART_TYPE_INTERVAL) {
            pruningRes->intervalOffset = 0;
            pruningRes->intervalSelectedPartitions = NULL;
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        listPartitionMap = (ListPartitionMap*)relation->partMap;
        for (i = 0; i < listPartitionMap->listElementsNum; i++) {
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
        }
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        hashPartitionMap = (HashPartitionMap*)relation->partMap;
        for (i = 0; i < hashPartitionMap->hashElementsNum; i++) {
            pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
            pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
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
    ListCell* cell = NULL;
    List* part_seqs = pruning_result->ls_rangeSelectedPartitions;

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

    foreach (cell, part_seqs) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell);
        Relation tablepartrel = NULL;

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq);
        tablepart = partitionOpen(heapRel, tablepartitionid, AccessShareLock);

        /* get index partition and add it to a list for following scan */
        if (RelationIsSubPartitioned(heapRel)) {
            ListCell *lc = NULL;
            tablepartrel = partitionGetRelation(heapRel, tablepart);
            SubPartitionPruningResult *subPartPruning =
                GetSubPartitionPruningResult(pruning_result->ls_selectedSubPartitions, partSeq);
            List *subpartList = subPartPruning->ls_selectedSubPartitions;

            foreach (lc, subpartList)
            {
                int subpartSeq = lfirst_int(lc);
                Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq);
                Partition subpart = partitionOpen(tablepartrel, subpartitionid, AccessShareLock);

                partitionIndexOidList = PartitionGetPartIndexList(subpart);
                if (!PointerIsValid(partitionIndexOidList)) {
                    ereport(ERROR,
                            (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
                }
                indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
                indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);

                list_free_ext(partitionIndexOidList);

                // found a unusable index partition
                if (!indexpartition->pd_part->indisusable) {
                    partitionIndexUnusable = false;
                    partitionClose(indexRel, indexpartition, AccessShareLock);
                    partitionClose(tablepartrel, subpart, AccessShareLock);
                    break;
                }

                partitionClose(indexRel, indexpartition, AccessShareLock);
                partitionClose(tablepartrel, subpart, AccessShareLock);
            }

            releaseDummyRelation(&tablepartrel);
            partitionClose(heapRel, tablepart, AccessShareLock);
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
            indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);
            // found a unusable index partition
            if (!indexpartition->pd_part->indisusable) {
                partitionIndexUnusable = false;
                partitionClose(indexRel, indexpartition, AccessShareLock);
                partitionClose(heapRel, tablepart, AccessShareLock);
                break;
            }

            partitionClose(indexRel, indexpartition, AccessShareLock);
            partitionClose(heapRel, tablepart, AccessShareLock);
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
    ListCell* cell = NULL;
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

    // this is the scaning loop for selected partitions
    foreach (cell, part_seqs) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        Relation tablepartrel = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq);
        tablepart = partitionOpen(heapRel, tablepartitionid, AccessShareLock);
        tablepartrel = partitionGetRelation(heapRel, tablepart);

        /* get index partition and add it to a list for following scan */
        ListCell *lc = NULL;
        SubPartitionPruningResult *subPartPruning =
            GetSubPartitionPruningResult(inputPruningResult->ls_selectedSubPartitions, partSeq);
        List *subPartList = subPartPruning->ls_selectedSubPartitions;

        foreach (lc, subPartList)
        {
            int subPartSeq = lfirst_int(lc);
            Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subPartSeq);
            Partition subpart = partitionOpen(tablepartrel, subpartitionid, AccessShareLock);

            partitionIndexOidList = PartitionGetPartIndexList(subpart);
            if (!PointerIsValid(partitionIndexOidList)) {
                ereport(ERROR,
                        (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
            }
            indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
            indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);
            // found a unusable index partition
            if (!indexpartition->pd_part->indisusable) {
                unusable = true;
                partitionClose(indexRel, indexpartition, AccessShareLock);
                partitionClose(tablepartrel, subpart, AccessShareLock);
                list_free_ext(partitionIndexOidList);
                break;
            }

            list_free_ext(partitionIndexOidList);
            partitionClose(indexRel, indexpartition, AccessShareLock);
            partitionClose(tablepartrel, subpart, AccessShareLock);
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
    ListCell* cell = NULL;
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

    // this is the scaning loop for selected partitions
    foreach (cell, part_seqs) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq);
        tablepart = partitionOpen(heapRel, tablepartitionid, AccessShareLock);

        /* get index partition and add it to a list for following scan */
        partitionIndexOidList = PartitionGetPartIndexList(tablepart);
        if (!PointerIsValid(partitionIndexOidList)) {
            ereport(ERROR, (errmodule(MOD_OPT),
                            errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart))));
        }
        indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
        list_free_ext(partitionIndexOidList);
        indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);
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
        partitionClose(indexRel, indexpartition, AccessShareLock);
        partitionClose(heapRel, tablepart, AccessShareLock);
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
        generateListFromPruningBM(outIndexUsable_pr);
        // set the output
        if (indexUsablePruningResult != NULL) {
            *indexUsablePruningResult = outIndexUsable_pr;
        }
    }
    // set back the bit map
    if (unusable_partition_num > 0) {
        outIndexUnusable_pr->bm_rangeSelectedPartitions = outIndexUnusable_bm;
        generateListFromPruningBM(outIndexUnusable_pr);
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
