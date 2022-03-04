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
 * dfs_query.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_query.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_QUERY_H
#define DFS_QUERY_H

#include "pg_config.h"

#ifndef ENABLE_LITE_MODE
#include "orc/Exceptions.hh"
#endif
#include "access/dfs/dfs_am.h"
#include "catalog/pg_collation.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/subselect.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dfs_vector.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/lsyscache.h"

#define DFS_PRIVATE_ITEM "DfsPrivateItem"
#define DFS_NUMERIC64_MAX_PRECISION 18

#ifdef ENABLE_LLVM_COMPILE
extern bool CodeGenThreadObjectReady();
extern bool ForeignScanExprCodeGen(Expr *expr, PlanState *parent, void **jittedFunc);

/*
 * Declare the function pointers for LLVM machine code address.
 */
typedef bool (*evaPredicateDouble)(double value);
typedef bool (*evaPredicateInt)(int64_t value);
#endif

/*
 * define the strategy numbers for hdfs foriegn scan. -1 is invalid,
 * 0-6 stand for op strategy, 11-12 stand for isnull/is not null.
 */
typedef enum {
    HDFS_QUERY_INVALID = -1,

    /*
     * 0-6 stand for OpExpr strategy,
     * HDFS_QUERY_EQ: '='
     * HDFS_QUERY_LT: '<'
     * HDFS_QUERY_GT: '>'
     * HDFS_QUERY_LTE: '<='
     * HDFS_QUERY_GTE: '>='
     * HDFS_QUERY_NE1: '<>'
     * HDFS_QUERY_NE2: '!='
     */
    HDFS_QUERY_EQ = 0,
    HDFS_QUERY_LT = 1,
    HDFS_QUERY_GT = 2,
    HDFS_QUERY_LTE = 3,
    HDFS_QUERY_GTE = 4,
    HDFS_QUERY_NE1 = 5,
    HDFS_QUERY_NE2 = 6,

    /* 11-12 stand for isnull/is not null */
    HDFS_QUERY_ISNULL = 11,
    HDFS_QUERY_ISNOTNULL = 12,
} HdfsQueryOperator;

typedef enum {
    NO_ENCODING_CHECK = 0,
    LOW_ENCODING_CHECK = 1,
    HIGH_ENCODING_CHECK = 2
} EncodingLevel;

/*
 * A template class which stores the info from the expression. This predicate will be pushed down to orc reader and
 * check it before the conversion to datum.
 */
template <typename T, typename baseType>
class HdfsScanPredicate : public BaseObject {
public:
    AttrNumber m_attno;            // column no
    Oid m_attType;                 // column data type
    int32 m_varTypMod;             // the var type of the value
    HdfsQueryOperator m_strategy;  // the number of strategy
    Oid m_collation;               // collation to use, if needed
    bool m_keepFalse;              // the check result keeps false if it is true
    T *m_argument;                 // store value
#ifdef ENABLE_LLVM_COMPILE
    char *m_predFunc;    /* IR function pointer */
    bool m_isPredJitted; /* whether use LLVM optimization or not. if the m_isPredJitted is true, use it */
    void *m_jittedFunc;  /* machine code address pointer. */
#endif

public:
    /*
     * Construct function of template class HdfsScanPredicate
     *
     * @_in param attNo: the index no of the column in the relation defination.
     * @_in param attType: the data type oid of the current var.
     * @_in param strategy: the HdfsQueryOperator(=,<,>,<=,>=,<>,!=,is null,is not null)
     */
    HdfsScanPredicate(AttrNumber attNo, Oid attType, HdfsQueryOperator strategy, Oid collation, int32 varTypMod)
        : m_attno(attNo),
          m_attType(attType),
          m_varTypMod(varTypMod),
          m_strategy(strategy),
          m_collation(collation),
          m_keepFalse(false)
    {
        m_argument = New(CurrentMemoryContext) T(strategy);
#ifdef ENABLE_LLVM_COMPILE
        m_predFunc = NULL;
        m_isPredJitted = false;
        m_jittedFunc = NULL;
#endif
    }

    ~HdfsScanPredicate()
    {
        delete (m_argument);
        m_argument = NULL;
    }

    /*
     * Initialize the m_argument of HdfsScanPredicate by datumValue if it is a operation predicate.
     *
     * @_in param datumValue: Just as the name says, it is a value of datum.
     * @_in param datumType: THe data type of the datum.
     */
    inline void Init(Datum datumValue, Oid datumType, int32 typeMod)
    {
        if (m_strategy >= HDFS_QUERY_EQ && m_strategy <= HDFS_QUERY_NE2) {
            m_argument->SetValueFromDatum(datumValue, datumType, typeMod);
        }
    }

    /*
     * Set the keepFalse flag.
     *
     * @_in param keepFalse: flag indicates whether the check result keeps false.
     */
    inline void setKeepFalse(bool keepFalse)
    {
        m_keepFalse = keepFalse;
    }

    /*
     * Build HdfsScanPredicate which will be pushed down to orc_reader according to the filtered and-clauses, we
     * can not push down the or-clauses now.
     *
     * @_in param expr: Expression which has been filtered by the optimizer.
     * @_in param state: PlanState is used when the expr is extern_param or exec_param to afford information.
     * @_in param scanstate: ScanState is used when init rumtime predicate.
     * @_in param varNoPos: An order number of var in the hdfsScanPredicateArr.
     * @_in param predicateArrPos: The parameter predicate position in *hdfsScanPredicateArr.
     * @return True: has init predicate; False, do not init predicate, need init dynamicly on running time phase.
     */
    bool BuildHdfsScanPredicateFromClause(Expr *expr, PlanState *ps, ScanState *scanstate, AttrNumber varNoPos,
                                          int predicateArrPos);

    /*
     * Check the value by the predicate.
     *
     * @_in param value: The value to be checked, it is of basic type like int64 or char*.
     * @return True: match and not filtered; False: filtered and set isSelected to false.
     */
    inline bool HdfsPredicateCheckOne(baseType value)
    {
        return m_argument->CheckPredicate(value, m_collation);
    }
};

#ifdef ENABLE_LLVM_COMPILE
/*
 * Brief        : Function to check if a value of basic type can match the clauses list pushed
 *                down. Here we do not check the length of scanClauses, and the caller need ensure it.
 * Description  :
 * Input        : value, The value to be checked, can not be NULL (it will handled by HdfsPredicateCheckNull).
 *                scanClauses, Clauses which can be pushed down to orc reader.
 * Output       : None.
 * Return Value : True: means the value match the predicate pushed down, so we can not prunning it,
 *                False: means the value does not match the predicate pushed down, so skip it.
 * Notes        : If the predicate->m_isPredJitted is true, we choose LLVM optimization to check scanclauses.
 */
template <typename wrapper, typename baseType>
bool HdfsPredicateCheckValueIntForLlvm(baseType &value, List *&scanClauses)
{
    ListCell *lc = NULL;
    HdfsScanPredicate<wrapper, baseType> *predicate = NULL;

    foreach (lc, scanClauses) {
        predicate = (HdfsScanPredicate<wrapper, baseType> *)lfirst(lc);

        if (NULL != predicate->m_jittedFunc) {
            if (!((evaPredicateInt)(predicate->m_jittedFunc))(value)) {
                return false;
            }

        } else {
            if (HDFS_QUERY_ISNULL == predicate->m_strategy || true == predicate->m_keepFalse) {
                return false;
            } else if (HDFS_QUERY_ISNOTNULL == predicate->m_strategy) {
                continue;
            } else if (!predicate->HdfsPredicateCheckOne(value)) {
                return false;
            }
        }
    }

    return true;
}

/*
 * Brief        : Function to check if a value of basic type can match the clauses list pushed
 *                down. Here we do not check the length of scanClauses, and the caller need ensure it.
 * Description  :
 * Input        : value, The value to be checked, can not be NULL (it will handled by HdfsPredicateCheckNull).
 *                scanClauses, Clauses which can be pushed down to orc reader.
 * Output       : None.
 * Return Value : True: means the value match the predicate pushed down, so we can not prunning it,
 *                False: means the value does not match the predicate pushed down, so skip it.
 * Notes        : If the predicate->m_isPredJitted is true, we choose LLVM optimization to check scanclauses.
 */
template <typename wrapper, typename baseType>
bool HdfsPredicateCheckValueDoubleForLlvm(baseType &value, List *&scanClauses)
{
    ListCell *lc = NULL;
    HdfsScanPredicate<wrapper, baseType> *predicate = NULL;

    foreach (lc, scanClauses) {
        predicate = (HdfsScanPredicate<wrapper, baseType> *)lfirst(lc);

        if (NULL != predicate->m_jittedFunc) {
            if (!((evaPredicateDouble)(predicate->m_jittedFunc))((double)value)) {
                return false;
            }

        } else {
            if (HDFS_QUERY_ISNULL == predicate->m_strategy || true == predicate->m_keepFalse) {
                return false;
            } else if (HDFS_QUERY_ISNOTNULL == predicate->m_strategy) {
                continue;
            } else if (!predicate->HdfsPredicateCheckOne(value)) {
                return false;
            }
        }
    }

    return true;
}
#endif


/**
 * @Description: Identify the qual which could be pushed down to
 * file level(e.g. ORC reader level).
 * @out hdfsPushDownQual: List to store all the clauses to push down.
 * @in/out primitiveQual: As a input parameter, it represents all expression
 * restrictions. As a output parameter, it represents actual expression
 * restrictions for bottom scan(.e.g. dfs scan, foreign scan).
 * @return Return the list of the column no which maps the pushdown qual.
 */
List *fix_pushdown_qual(List **hdfsPushDownQual, List **primitiveQual, const List *part_list);

/*
 * Convert a char* to Datum according to the data type oid.
 *
 * @_in param typeOid: The oid of the type in pg_type catalog.
 * @_in param typeMod: The mod of data type.
 * @_in param value: The string value which need to be converted to datum.
 * @return Return the datum converted from String.
 */
Datum GetDatumFromString(Oid typeOid, int4 typeMod, char *value);

void GetStringFromDatum(Oid typeOid, int4 typeMod, Datum data, StringInfo string);

/*
 * Initialize a folder split by the folder name, filename(both folder name and filename comprise the absolute file
 * path), columnNo list of partition and column value list of partition.
 *
 * @_in param fileName: name of the file.
 * @_in param partContentList: the list of value in partition column.
 * @_in param objectSize: the size of the current object
 * @return Return the constructed split.
 */
SplitInfo *InitFolderSplit(char *fileName, List *partContentList, int64 objectSize);

/*
 * Initialize a file split by the file path, columnNo list of partition and column value list of partition. This is
 * different from InitFolderSplit for it use the absolute file path as the param and has no file name which is used for
 * partition only.
 *
 * @_in param filePath: the absolute path of the split file.
 * @_in param partContentList: the list of value in partition column.
 * @_in param fileSize: the size of the current object
 * @return Return the constructed split.
 */
SplitInfo *InitFileSplit(char *filePath, List *partContentList, int64 fileSize);

/*
 * Destroy all the structors in the split.
 * @_in param split: The split need to to clean and is not used any more.
 */
void DestroySplit(SplitInfo *split);

/*
 * Build a basic Operator expression.
 * @_in param variable: The var according to whose type we build the base constraint.
 * @return Return and OpExpression.
 */
Node *MakeBaseConstraint(Var *variable, Datum minValue, Datum maxValue, bool hasMinimum, bool hasMaximum);

Node *MakeBaseConstraintWithExpr(OpExpr *lessThanExpr, OpExpr *greaterThanExpr, Datum minValue, Datum maxValue,
                                 bool hasMinimum, bool hasMaximum);

OpExpr *MakeOperatorExpression(Var *variable, int16 strategyNumber);

/*
 * Build a new null test expr with the given type.
 *
 * @_in param variable: The value based on which we build a NullTest expression.
 * @_in param type: is null or is not null.
 * @return Return the NullTest expression we build here.
 */
Node *BuildNullTestConstraint(Var *variable, NullTestType type);

/*
 * Fill the expression with the const value.
 * @_in param equalExpr: the expression of the constraint.
 * @_in param value: Value to be filled for the right op.
 * @_in param isNull: Whether the value is null.
 * @return Return the constraint.
 */
void BuildConstraintConst(Expr *equalExpr, Datum value, bool isNull);

/*
 * Fetch the Var according to the column no from the columnList, used in partition filter.
 *
 * @_in param columnList: The column list from which we find the var.
 * @_in param colNo: The column index by which we find the var.
 * @return Return null: we don't find the var in the list with the colno; not null: we find the var and return it.
 */
Var *GetVarFromColumnList(List *columnList, int colNo);

/*
 * Return the digits of a int value. like 0 return 1, 10 return 2.
 */
uint64 GetDigitOfInt(uint64 x);

/*
 * Encode the uri char. Like "#" -> "%23", used in partition filter.
 *
 * @_in param pSrc: The uri format string to be encoded.
 * @return Return the encoded string.
 */
char *UriEncode(const char *pSrc);

/*
 * Decode the uri char. Like "%23" -> "#", used in partition filter.
 *
 * @_in param pSrc: The uri format string to be decoded.
 * @return Return the decoded string.
 */
char *UriDecode(const char *pSrc);

/*
 * Construct a node of DfsPrivateItem.
 */
DfsPrivateItem *MakeDfsPrivateItem(List *columnList, List *targetList, List *restrictColList, List *opExpressionList,
                                   List *dnTask, List *hdfsQual, double *selectivity, int colNum, List *partList);

/*
 * Parse the fileNames string from the split List.
 * @_in_out param splitList: point to the original split List, which may contain multiple files.
 * @_in param currentFileName: point to the first file or the only file '/user/demai/file1.orc' (new buffer).
 * @return Return the split parsed from the list.
 */
SplitInfo *ParseFileSplitList(List **splitList, char **currentFileName);

/*
 * @Description: find a split from the split list by file ID
 * @IN splitList: the list of all splits
 * @IN fileID: the file id
 * @Return: the split if found, or return NULL
 * @See also:
 */
SplitInfo *FindFileSplitByID(List *splitList, int fileID);

/**
 * @Description: Set all the rows of current ScalarVector with the same value transfered.
 * The value can be null.
 * @in vec, The scalar vector to which we set the value.
 * @in rowsToRead, The number of rows to set.
 * @in value: The fixed value to set all the rows.
 * @in isNull, Whether the column value is null.
 * @return None.
 */
void SetAllValue(ScalarVector *vec, int rows, Datum value, bool isNull);

/* Brief: Acquires column information needed for this foreign table from the restriction.
 *        Returns them in a new list.
 * input param @rel: relation information struct pointer.
 */
List *GetRestrictColumns(List *neededList, int count);

/*
 * Merge two list and kick out the repeated one.
 * _in_param targetList: The list of the target columns.
 * _in_param restrictList: The list of the restriction columns.
 * _in_param count: The length of the result list.
 * @return the merged and de-duplicated list.
 */
List *MergeList(List *targetList, List *restrictList, int count);

/*
 * @Description: build the columns list for dfsscan, which comes from reltargetlist
 *	 	and exclude the index columns.
 * @IN reltargetlist: the primitive target list
 * @IN excludedColList: the columns which should be abandoned.
 * @Return: the final reader list for dfs scan
 * @See also:
 */
List *build_dfs_reader_tlist(List *reltargetlist, List *excludedColList);

/*
 * Calculate the predicate selectivity of each column.
 */
void CalculateWeightByColumns(PlannerInfo *root, List *hdfsQualColumn, List *hdfsQual, double *selectivity, int colNum);

/*
 *brief: Check column data type for orc table.
 *input param @ColType: Oid of column data type;
 *input param @ColName: column name.
 */
void DFSCheckDataType(TypeName *typName, char* ColName, int format);

/*
 * brief: Check foldername, filenames, hdfscfgpath format. The rules are the followings:
 *		  1. The space do not allow appear in the entrie OptStr, but '\ ' is needed
 *		  2. The comma do not appera in beignning and ending of OptStr
 *		  3. The comma as a separator exists if OptStr have many filenames path
 *		  4. Only a foldername path exists for foldername option and hdfscfgpath
 *		  5. The OptStr do not be empty
 * input param @OptStr: the foldername path or file names or hdfscfgpath string
 * input param @OptType: a foldername, a filenames or a hdfscfgpath type
 */
void CheckFoldernameOrFilenamesOrCfgPtah(const char *OptStr, char *OptType);

/*
 * brief: Check or get port and ip of foreign server(Only support ipv4 format),
 *        the rules are the followings:
 *		  1. The space do not allow appear in the entrie Address
 *		  2. The comma do not appera in beignning and ending of Address
 *		  3. The comma as a separator exists if OptStr have many address
 *		  4. The Address do not be empty
 * input param @Address: address option of foreign server
 * output param @AddrList: return List included port and ip
 * input param @IsCheck: if IsCheck is true, only check, else get port and ip
 */
void CheckGetServerIpAndPort(const char *Address, List **AddrList, bool IsCheck, int real_addr_max);

/*
 * brief: Build internal executor parameter predicate for pushdown of dfs scan
 *        when excute plan.
 * input param @readerState: Includes all the params which are used during reading.
 */
void BuildRunTimePredicates(dfs::reader::ReaderState *readerState);

/**
 * @Description: Get the int64 data according to the column type.
 * @in datumValue, the datum value.
 * @in colVar, The Var struct. It keeps data type. As for decimal data, the
 * precision store here.
 * @return return the transformed data.
 */
int64_t datumGetInt64ByVar(Datum datumValue, Var *colVar);

/**
 * @Description: Get the int128 data according to the column type. Currently,
 * only the decimal may be converted to int128 data.
 * @in datumValue, the datum value.
 * @in colVar, The Var struct. It keeps data type. As for decimal data, the
 * precision store here.
 * @return return the transformed data.
 */
__int128 datumGetInt128ByVar(Datum datumValue, Var *colVar);

/**
 * @Description: Get the float8 data according to the column type.
 * @in datumValue, the datum value.
 * @in colVar, The Var struct. It keeps data type.
 * @return return the transformed data.
 */
double datumGetFloat8ByVar(Datum datumValue, Var *colVar);

/**
 * @Description: Get the string data according to the column type.
 * @in datumValue, the datum value.
 * @in colVar, The Var struct. It keeps data type.
 * @return return the transformed data.
 */
char *datumGetStringByVar(Datum datumValue, Var *colVar);

/*
 *
 * brief: Generate column list.
 * input param @attributes: information of pg_attribure;
 * input param @columnCount: the count of column.
 * input param @colIdx: The column idx array.
 */
List *CreateColList(Form_pg_attribute *attributes, const int columnCount, const int16 *colIdx = NULL);

/*
 * @Description: remove the elements of the 'del' list in the 'src' list
 */
void list_delete_list(List **src, List *del);

/**
 * @Description: Check the given file encoding.
 * @in optionDef: The envoding string.
 * @return None.
 */
void checkEncoding(DefElem *optionDef);

int FindLastCharPos(char seperator, const char *srcString);

bool IsVarNode(Node *node);

/**
 * @Description: The default column value satisfies whether predicates.
 * @in isNull, The default column whether is null.
 * @in value, The default column value.
 * @in colVar, The Var sturct of default column.
 * @in predicateList, The default column predicates.
 * @return If the default column value satisfies the default column predicates,
 * return true, otherwise return false.
 * @Notes: Only the column of the pushed down data type can reach here.
 */
bool defColSatisfyPredicates(bool isNull, Datum value, Var *col_var, List *predicate_list);
bool IsParamConst(Node *node);

/**
 * @Description: Convert the string according to the database encoding and original encoding.
 * @in src: the input string.
 * @in length: the length of the input string.
 * @in encoding: the original encoding.
 * @return the converted string.
 */
char *pg_to_server_withfailure(char *src, int64 length, int32 encoding, int32 checkEncodingLevel, bool &meetError);

BlockNumber getPageCountForFt(void *additionalData);

#define QUERY_NOT_SUPPORT(foreignTableId, msg) do { \
    Relation relation = RelationIdGetRelation(foreignTableId);                           \
    if (RelationIsValid(relation))                                                       \
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),      \
                        errmsg(msg, RelationGetRelationName(relation))));                \
    else                                                                                 \
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),      \
                        errmsg("could not open relation with OID  %u", foreignTableId))); \
} while (0)

#endif /* DFS_QUERY_H */
