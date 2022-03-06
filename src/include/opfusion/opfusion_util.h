/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * opfusion_util.h
 *        Declaration of utilities for bypass executor.
 *
 * IDENTIFICATION
 * src/include/opfusion/opfusion_util.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_UTIL_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_UTIL_H_

#include "commands/prepare.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "opfusion/opfusion_scan.h"
#include "pgxc/pgxcnode.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

const int FUSION_EXECUTE = 0;
const int FUSION_DESCRIB = 1;
typedef struct pnFusionObj {
    char portalname[NAMEDATALEN];
    OpFusion *opfusion;
} pnFusionObj;

#define HASH_TBL_LEN 64

enum FusionType {
    NONE_FUSION,

    SELECT_FUSION,
    SELECT_FOR_UPDATE_FUSION,
    INSERT_FUSION,
    UPDATE_FUSION,
    DELETE_FUSION,
    AGG_INDEX_FUSION,
    SORT_INDEX_FUSION,

    MOT_JIT_SELECT_FUSION,
    MOT_JIT_MODIFY_FUSION,

    BYPASS_OK,

    NOBYPASS_NO_CPLAN,
    NOBYPASS_NO_SIMPLE_PLAN,
    NOBYPASS_NO_QUERY_TYPE,
    NOBYPASS_LIMITOFFSET_CONST_LESS_THAN_ZERO,
    NOBYPASS_LIMITCOUNT_CONST_LESS_THAN_ZERO,
    NOBYPASS_LIMIT_NOT_CONST,
    NOBYPASS_INVALID_SELECT_FOR_UPDATE,
    NOBYPASS_INVALID_MODIFYTABLE,
    NOBYPASS_NO_SIMPLE_INSERT,
    NOBYPASS_UPSERT_NOT_SUPPORT,

    NOBYPASS_NO_INDEXSCAN,
    NOBYPASS_ONLY_SUPPORT_BTREE_INDEX,
    NOBYPASS_INDEXSCAN_WITH_ORDERBY,
    NOBYPASS_INDEXSCAN_WITH_QUAL,
    NOBYPASS_INDEXSCAN_CONDITION_INVALID,

    NOBYPASS_INDEXONLYSCAN_WITH_ORDERBY,
    NOBYPASS_INDEXONLYSCAN_WITH_QUAL,
    NOBYPASS_INDEXONLYSCAN_CONDITION_INVALID,

    NOBYPASS_TARGET_WITH_SYS_COL,
    NOBYPASS_TARGET_WITH_NO_TABLE_COL,
    NOBYPASS_NO_TARGETENTRY,
    NOBYPASS_PARAM_TYPE_INVALID,

    NOBYPASS_DML_RELATION_NUM_INVALID,
    NOBYPASS_DML_RELATION_NOT_SUPPORT,
    NOBYPASS_DML_TARGET_TYPE_INVALID,

    NOBYPASS_EXP_NOT_SUPPORT,
    NOBYPASS_STREAM_NOT_SUPPORT,
    NOBYPASS_NULLTEST_TYPE_INVALID,

    NOBYPASS_INVALID_PLAN,

    NOBYPASS_NOT_PLAIN_AGG,
    NOBYPASS_ONE_TARGET_ALLOWED,
    NOBYPASS_AGGREF_TARGET_ALLOWED,
    NOBYPASS_JUST_SUM_ALLOWED,
    NOBYPASS_JUST_VAR_FOR_AGGARGS,

    NOBYPASS_JUST_MERGE_UNSUPPORTED,

    NOBYPASS_JUST_VAR_ALLOWED_IN_SORT,

    NOBYPASS_ZERO_PARTITION,
    NOBYPASS_MULTI_PARTITION,
    NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION,
    NO_BYPASS_PARTITIONKEY_IS_NULL,
    NOBYPASS_NO_UPDATE_PARTITIONKEY,
    NOBYPASS_NO_INCLUDING_PARTITIONKEY,
    NOBYPASS_PARTITION_BYPASS_NOT_OPEN,
    NOBYPASS_VERSION_SCAN_PLAN,
    NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION
};

enum FusionDebug {
    BYPASS_OFF,
    BYPASS_LOG,
};

const int MAX_OP_FUNCTION_NUM = 2;
typedef struct FuncExprInfo {
    AttrNumber resno;
    Oid funcid;
    List *args;
    char *resname;
} FuncExprInfo;

const int OPFUSION_FUNCTION_ID_MAX_HASH_SIZE = 200;

/* length of function_id should not more than OPFUSION_FUNCTION_ID_MAX_HASH_SIZE */
const Oid function_id[] = {
    311,  /* convert float4 to float8 */
    312,  /* convert float8 to float4 */
    313,  /* convert int2 to int4 */
    314,  /* convert int4 to int2 */
    315,  /* int2vectoreq */
    316,  /* convert int4 to float8 */
    317,  /* convert float8 to int4 */
    318,  /* convert int4 to float4 */
    319,  /* convert float4 to int4 */
    401,  /* convert char(n) to text */
    406,  /* convert name to text */
    407,  /* convert text to name */
    408,  /* convert name to char(n) */
    409,  /* convert char(n) to name */
    668,  /* adjust char() to typmod length */
    669,  /* adjust varchar() to typmod length */
    944,  /* convert text to char */
    946,  /* convert char to text */
    1200, /* adjust interval precision */
    1400, /* convert varchar to name */
    1401, /* convert name to varchar */
    1683, /* convert int4 to bitstring */
    1684, /* convert bitstring to int4 */
    1685, /* adjust bit() to typmod length */
    1687, /* adjust varbit() to typmod length */
    1703, /* adjust numeric to typmod precision/scale */
    1705, /* abs */
    1706, /* sign */
    1707, /* round */
    1711, /* ceil */
    1712, /* floor */
    1728, /* mod */
    1740, /* convert int4 to numeric */
    1742, /* convert float4 to numeric */
    1743, /* convert float8 to numeric */
    1744, /* convert numeric to int4 */
    1745, /* convert numeric to float4 */
    1746, /* convert numeric to float8 */
    1768, /* format interval to text */
    1777, /* convert text to numeric */
    1778, /* convert text to timestamp with time zone */
    1780, /* convert text to date */
    1961, /* adjust timestamp precision */
    1967, /* adjust timestamptz precision */
    1968, /* adjust time precision */
    1969, /* adjust time with time zone precision */
    2089, /* convert int4 number to hex */
    2090, /* convert int8 number to hex */
    2617, /* ceiling */
    3180, /* convert int2 to boolen */
    3192, /* convert int4 to bpchar */
    3207, /* convert text to timestamp without time zone */
    3811, /* convert int4 to money */
    3812, /* convert int8 to money */
    3823, /* convert money to numeric */
    3824, /* convert numeric to money */
    3961, /* adjust nvarchar2() to typmod length */
    3961, /* adjust nvarchar2() to typmod length */
    4065, /* convert int1 to varchar */
    4066, /* convert int1 to nvarchar2 */
    4067, /* convert int1 to bpchar */
    4068, /* convert int2 to bpchar */
    4069, /* convert int8 to bpchar */
    4070, /* convert float4 to bpchar */
    4071, /* convert float8 to bpchar */
    4072, /* convert numeric to bpchar */
    4073, /* convert text to timestamp */
    4167, /* convert int4 to text */
    4168, /* convert int8 to text */
    4169, /* convert float4 to text */
    4170, /* convert float8 to text */
    4171, /* convert numeric to text */
    4172, /* convert bpchar to numeric */
    4173, /* convert varchar to numeric */
    4174, /* convert varchar to int4 */
    4175, /* convert bpchar to int4 */
    4176, /* convert varchar to int8 */
    4177, /* convert timestampzone to text */
    4178, /* convert timestamp to text */
    4179, /* convert timestamp to text */
    4180, /* convert int2 to varchar */
    4181, /* convert int4 to varchar */
    4182, /* convert int8 to varchar */
    4183, /* convert numeric to varchar */
    4184, /* convert float4 to varchar */
    4185, /* convert float8 to varchar */
    4186, /* convert varchar to timestamp */
    4187, /* convert bpchar to timestamp */
    4188, /* convert text to int1 */
    4189, /* convert text to int2 */
    4190, /* convert text to int4 */
    4191, /* convert text to int8 */
    4192, /* convert text to float4 */
    4193, /* convert text to float8 */
    4194, /* convert text to numeric */
    4195, /* convert bpchar to int8 */
    4196, /* convert bpchar to float4 */
    4197, /* convert bpchar to float8 */
    4198, /* convert varchar to float4 */
    4199, /* convert varchar to float8 */
    5523, /* convert int1 to int2 */
    5524, /* convert int2 to int1 */
    5525, /* convert int1 to int4 */
    5526, /* convert int4 to int1 */
    5527, /* convert int1 to int8 */
    5528, /* convert int8 to int1 */
    5529, /* convert int1 to float4 */
    5530, /* convert float4 to int1 */
    5531, /* convert int1 to float8 */
    5532, /* convert float8 to int1 */
    5533, /* convert int1 to bool */
    5534  /* convert bool to int1 */
};

extern int namestrcmp(Name name, const char *str);
extern void report_qps_type(CmdType commandType);

void InitOpfusionFunctionId();
Node *JudgePlanIsPartIterator(Plan *plan);
const char *getBypassReason(FusionType result);
void BypassUnsupportedReason(FusionType result);
FusionType getSelectFusionType(List *stmt_list, ParamListInfo params);
FusionType getInsertFusionType(List *stmt_list, ParamListInfo params);
FusionType getUpdateFusionType(List *stmt_list, ParamListInfo params);
FusionType getDeleteFusionType(List *stmt_list, ParamListInfo params);
void tpslot_free_heaptuple(TupleTableSlot *reslot);
void InitPartitionByScanFusion(Relation rel, Relation *fakRel, Partition *part, EState *estate, const ScanFusion *scan);
Relation InitBucketRelation(int2 bucketid, Relation rel, Partition part);
void ExecDoneStepInFusion(EState *estate);
Oid GetRelOidForPartitionTable(Scan scan, const Relation rel, ParamListInfo params);
Relation InitPartitionIndexInFusion(Oid parentIndexOid, Oid partOid, Partition *partIndex, Relation *parentIndex,
    Relation rel);
void InitPartitionRelationInFusion(Oid partOid, Relation parentRel, Partition *partRel, Relation *rel);
void ExeceDoneInIndexFusionConstruct(bool isPartTbl, Relation *parentRel, Partition *part, Relation *index,
    Relation *rel);

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_UTIL_H_ */