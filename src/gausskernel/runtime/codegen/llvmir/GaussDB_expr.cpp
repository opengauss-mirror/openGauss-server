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
 * GaussDB_expr.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/runtime/codegen/llvmir/GaussDB_expr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "vecexecutor/vecexpression.h"
#include "vecexecutor/vectorbatch.h"
#include "nodes/execnodes.h"
#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "catalog/pg_type.h"
#include "commands/typecmds.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeSubplan.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "utils/numeric.h"
#include "utils/biginteger.h"
#include "utils/bloom_filter.h"
#include "vecexecutor/vecfunc.h"
#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonicarray.h"
#include "vectorsonic/vsonichashagg.h"
#include "vectorsonic/vsonicencodingchar.h"
#include "fmgr.h"

#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vecsortagg.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vechashtable.h"

struct NumData {
    union NumChoice {
        uint16 n_header;
        struct NumShort {
            uint16 n_header;
            int16 n_data[1];
        } n_short;
    } choice;
} ;
typedef struct NumData *MyNumeric;

typedef struct {
    int16 scale;
    int64 value;
} numeric_type;


/**
 * @Description: A simple c-function used to show the basic structure of LLVM
 * module and LLVM IR function
 * @in fcache: FuncExprState
 * @in econtext: ExprContext
 * @in p_selection: mark if this element is choosed or not
 * @in p_vector: temporay ScalarVector
 * @return: the result vector
 */
extern "C"
ScalarVector* llvmJittedFunc(FuncExprState *fcache, HashJoinTbl *HJT, struct NumericData *num,
                             ExprContext *econtext, bool *p_selection, ScalarVector *p_vector)
{
    VectorBatch *batch = NULL;
    ScalarVector *tmp_vector = NULL;
    ScalarVector *result = NULL;
    ScalarValue* parg1;
    ScalarValue* presult;
    uint8* pflags = NULL;
    AttrNumber attnum = 26;
    int32 nvalues;
    Datum r;
    int i;
    uint32 mask = 0;

    batch = econtext->ecxt_scanbatch;
    tmp_vector = &batch->m_arr[attnum - 1];
    nvalues = tmp_vector->m_rows;
    parg1 = tmp_vector->m_vals;
    result = p_vector;
    presult = result->m_vals;
    pflags = (uint8*)(result->m_flag);
    for (i = 0; i < nvalues; i++) {
        r = (parg1[i] + 1) * 2 + 3;
        presult[i] = (long long)r < 10;
        SET_NOTNULL(pflags[i]);
    }
    
    result->m_rows = nvalues;
    result->m_desc.typeId = INT8OID;

    return result;
}

/*
 * @Description: simple case of memcmp, which is used to generate the LLVM IR
 * function in LLVM module.
 * @in data1: the first source string
 * @in data2: the second source string
 * @in len: the length of the string we want to compare
 */
extern "C"
Datum LLVMIRmemcmp(char* data1, char* data2, int len)
{
    bool result;
    result = true;
    do {
        if (*data1++ != *data2++) {
            result = false;
            break;
        }
    } while (--len != 0);
    return (Datum)result;
}


typedef struct {
    uint32_t len;
    char* data;
} varchar_type;

/*
 * @Description: function of rtrim1, which is used to generate the LLVM IR
 * function in LLVM module.
 * @in arg1: the source string
 * @return: the string result after rtrim operation with set fixed as ' '
 */
extern "C"
varchar_type LLVMIRrtrim1(Datum arg1)
{
    char *data1 = NULL;
    int i;
    int len1;
    varchar_type result;

    if ((((varattrib_1b*)arg1)->va_header & 0x01) == 0x01) {
        data1 = ((varattrib_1b*)arg1)->va_data;
        len1 = ((((varattrib_1b*)arg1)->va_header >> 1) & 0x7F) - 1;
    } else {
        data1 = ((varattrib_4b*)arg1)->va_4byte.va_data;
        len1 = ((((varattrib_4b*)arg1)->va_4byte.va_header >> 2) & 0x3FFFFFFF) - 4;
    }

    for (i = len1 - 1; i >= 0; i--) {
        if (data1[i] != ' ') {
            break;
        }      
    }

    result.len = i + 1;
    result.data = data1;
    return result;
}

/*
 * @Description: function of btrim1, which is used to generate the LLVM IR
 * function in LLVM module.
 * @in arg1: the source string in Datum representation
 * @return: the string result after btrim operation with set fixed as ' '
 */
extern "C"
varchar_type LLVMIRbtrim1(Datum arg1)
{
    char* data1 = NULL;
    int i;
    int len1;
    varchar_type result;

    if ((((varattrib_1b*)arg1)->va_header & 0x01) == 0x01) {
        data1 = ((varattrib_1b*)arg1)->va_data;
        len1 = ((((varattrib_1b*)arg1)->va_header >> 1) & 0x7F) - 1;
    } else {
        data1 = ((varattrib_4b*)arg1)->va_4byte.va_data;
        len1 = ((((varattrib_4b*)arg1)->va_4byte.va_header >> 2) & 0x3FFFFFFF) - 4;
    }

    for (i = len1 - 1; i >= 0; i--) {
        if (data1[i] != ' ') {
            break;
        }            
    }
    len1 = i + 1;

    for (i = 0; i < len1; i++) {
        if (data1[i] != ' ') {
            break;
        }     
    }

    result.len = len1 - i;
    result.data = data1 + i;
    return result;
}

/*
 * @Description: simple case of bpchareq function
 * @in arg1: left argument of the bpchareq function.
 * @in arg2: right argument of the bpchareq function.
 * @in collid: Collation of BPCHAREQOID
 * @return: the result of bpchareq with arguments arg1 and arg2.
 * @Notes: The second argument varchar_type is a constant and its length 
 * is the true length.
 */
extern "C"
Datum LLVMIRbpchareq(uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data)
{
    char* data1 = NULL;
    char* data2 = NULL;
    int i, len1, len2;
    bool result = true;

    len1 = arg1_len;
    len2 = arg2_len;
    data1 = arg1_data;
    data2 = arg2_data;

    for (i = len1 - 1; i >= 0; i--) {
        if (data1[i] != ' ') {
            break;
        }     
    }
    len1 = i + 1;

    for (i = len2 - 1; i >= 0; i--) {
        if (data2[i] != ' ') {
            break;
        }     
    }
    len2 = i + 1;

    if (len1 != len2) {
        result = false;
    } else if (len1 != 0) {
        do {
            if (*data1++ != *data2++) {
                result = false;
                break;
            }
        } while (--len1 != 0);
    }
    return (Datum)result;
}

/*
 * @Description: simple case of bpcharne function
 * @in arg1: left argument of the bpcharne function.
 * @in arg2: right argument of the bpcharne function.
 * @in collid: Collation of BPCHARNEOID
 * @return: the result of bpchareq with arguments arg1 and arg2.
 * @Notes: The second argument varchar_type is a constant and its length 
 * is the true length.
 */
extern "C"
Datum LLVMIRbpcharne(uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data)
{
    char* data1 = NULL;
    char* data2 = NULL;
    int i, len1, len2;
    bool result = false;

    len1 = arg1_len;
    len2 = arg2_len;
    data1 = arg1_data;
    data2 = arg2_data;

    for (i = len1 - 1; i >= 0; i--) {
        if (data1[i] != ' ') {
            break;
        }     
    }
    len1 = i + 1;

    for (i = len2 - 1; i >= 0; i--) {
        if (data2[i] != ' ') {
            break;
        }      
    }
    len2 = i + 1;

    if (len1 != len2) {
        result = true;
    } else if (len1 != 0) {
        do {
            if (*data1++ != *data2++) {
                result = true;
                break;
            }
        } while (--len1 != 0);
    }
    return (Datum)result;
}

/*
 * @Description: simple case of texteq function
 * @in arg1: left argument of the texteq function.
 * @in arg2: right argument of the texteq function.
 * @in collid: Collation of TEXTEQOID
 * @return: the result of texteq with arguments arg1 and arg2.
 */
extern "C"
Datum LLVMIRtexteq (uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data)
{
    bool result;
    int len1, len2;
    char* data1 = NULL;
    char* data2 = NULL;

    len1 = arg1_len;
    len2 = arg2_len;
    result = true;
    if (len1 != len2) {
        result = false;
    } else if (len1 != 0) {
        data1 = arg1_data;
        data2 = arg2_data;
        do {
            if (*data1++ != *data2++) {
                result = false;
                break;
            }
        } while (--len1 != 0);
    }
    return (Datum)result;
}

/*
 * @Description: simple case of textneq function
 * @in arg1: left argument of the textneq function.
 * @in arg2: right argument of the textneq function.
 * @in collid: Collation of TEXTNEQOID
 * @return: the result of textneq with arguments arg1 and arg2.
 */
extern "C"
Datum LLVMIRtextneq (uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data)
{
    bool result;
    int len1, len2;
    char* data1 = NULL;
    char* data2 = NULL;

    len1 = arg1_len;
    len2 = arg2_len;
    result = false;
    if (len1 != len2) {
        result = true;
    } else if (len1 != 0) {
        data1 = arg1_data;
        data2 = arg2_data;
        do {
            /* just compare the bit */
            if (*data1++ != *data2++) {
                result = true;
                break;
            }
        } while (--len1 != 0);
    }
    return (Datum)result;
}


/*
 * @Description: simple case of substr function with function oid equals to 877
 * @in str: string
 * @in s: start position
 * @in L: length of the str we want to check.
 * @Notes: 1. support utf-8 & ASCII encoding
 * 2. assume L > 0 and s > 0
 */
extern "C"
varchar_type LLVMIRsubstring_ASCII(uint32_t str_len, char* str_data, int s, int l)
{
    int e = s + l;
    int strlen, strwchars, len, i;
    char* p = NULL;
    char* p1 = NULL;
    char* p2 = NULL;
    char* p_end = NULL;

    if (str_len == 0) {
        return {str_len, str_data};
    }   
    strlen = str_len;
    p = str_data;
    p_end = p + strlen;

    p2 = str_data;

    /* s starts from 1, if s = 1, the loop body is not executed. */
    for (i = 1; i < s && p2 < p_end; i += len) {
        if ((*p2 & 0x80) == 0) {
            len = 1;
        } else if ((*p2 & 0xe0) == 0xc0) {
            len = 2;
        } else if ((*p2 & 0xf0) == 0xe0) {
            len = 3;
        } else if ((*p2 & 0xf8) == 0xf0) {
            len = 4;
        } else {
            len = 1;
        }

        if (i + len >= e) {
            p2 += (e - i);
        } else if (i + len >= s) {
            p2 += (s - i);
        } else {
            p2 += len;
        }    
    }

    /*
     * If the start position is beyond the string, return 0-length string
     */
    if (p2 >= p_end) {
        str_len = 0;
        return {str_len, str_data};
    }
    
    p1 = p2;
    for (i = s; i < e && p2 < p_end; i += len) {
        if ((*p2 & 0x80) == 0) {
            len = 1;
        } else if ((*p2 & 0xe0) == 0xc0) {
            len = 2;
        } else if ((*p2 & 0xf0) == 0xe0) {
            len = 3;
        } else if ((*p2 & 0xf8) == 0xf0) {
            len = 4;
        } else {
            len = 1;
        }
            
        if (i + len >= e) {
            p2 += (e - i);
        } else {
            p2 += len;
        }   
    }
    str_len = p2 - p1;
    str_data = p1;
    return {str_len, str_data};
}


/*
 * @Description: simple case of substr function with function oid equals to 877
 * @in str: string
 * @in s: start position
 * @in l: length of the str we want to check.
 * @Notes: 1. support utf-8 & ASCII encoding
 * 2. assume l > 0 and s > 0
 */
extern "C"
varchar_type LLVMIRsubstring_UTF8(uint32_t str_len, char* str_data, int s, int l)
{
    int e = s + l;
    int strlen, strwchars, len, i;
    char* p = NULL;
    char* p1 = NULL;
    char* p2 = NULL;
    char* p_end = NULL;

    if (str_len == 0) {
        return {str_len, str_data};
    }
        
    strlen = str_len;
    p = str_data;
    p_end = p + strlen;
    p2 = str_data;

    /* s starts from 1, if s = 1, the loop body is not executed. */
    for (i = 1; i < s && p2 < p_end; i++) {
        if ((*p2 & 0x80) == 0) {
            len = 1;
        } else if ((*p2 & 0xe0) == 0xc0) {
            len = 2;
        } else if ((*p2 & 0xf0) == 0xe0) {
            len = 3;
        } else if ((*p2 & 0xf8) == 0xf0) {
            len = 4;
        } else {
            len = 1;
        }            
        p2 += len;
    }

    /* If the start position is beyond the string, return 0-length string */
    if (p2 >= p_end) {
        str_len = 0;
        return {str_len, str_data};
    }
    
    p1 = p2;
    for (i = s; i < e && p2 < p_end; i++) {
        if ((*p2 & 0x80) == 0) {
            len = 1;
        } else if ((*p2 & 0xe0) == 0xc0) {
            len = 2;
        } else if ((*p2 & 0xf0) == 0xe0) {
            len = 3;
        } else if ((*p2 & 0xf8) == 0xf0) {
            len = 4;
        } else {
            len = 1;
        }
        p2 += len;
    }

    str_len = p2 - p1;
    if (p2 > p_end) {
        str_len--;
    }
    
    str_data = p1;
    return {str_len, str_data};
}

/**
 * @Description: simple textgt function, which is used to create LLVM IR function
 * @in arg1: left argument of the textgt function.
 * @in arg2: right argument of the textgt function.
 * @in collid: Collation of TEXTGT
 * @return: the result of textgt with arguments arg1 and arg2.
 */
extern "C"
Datum LLVMIRtextgt(uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data, Oid collid)
{
    char* data1 = NULL;
    char* data2 = NULL;
    int i, len1, len2, len;
    bool result;

    len1 = arg1_len;
    len2 = arg2_len;
    data1 = arg1_data;
    data2 = arg2_data;

    len = (len1 < len2) ? len1 : len2;

    for (i = 0; i < len; i++) {
        /* Only for comparison on digits 0 - 9 */
        if (data1[i] >= 48 && data1[i] <= 57) {
            if (data1[i] < data2[i]) {
                return (Datum)0;
            } else if (data1[i] > data2[i]) {
                return (Datum)1;
            }        
        } else {
            int res = varstr_cmp(data1, len1, data2, len2, collid); 
            if (res <= 0) {
                return (Datum)0;
            } else {
                return (Datum)1;
            }     
        }
    }

    if (len1 <= len2) {
        return (Datum)0;
    } else {
        return (Datum)1;
    }     
}

/**
 * @Description: simple textlt function, which is used to create LLVM IR function
 * @in arg1: left argument of the textlt function.
 * @in arg2: right argument of the textlt function.
 * @in collid: Collation of TEXTLTOID
 * @return: the result of textlt with arguments arg1 and arg2.
 */
extern "C"
Datum LLVMIRtextlt(uint32_t arg1_len, char* arg1_data, uint32_t arg2_len, char* arg2_data, Oid collid)
{
    char* data1 = NULL;
    char* data2 = NULL;
    int i, len1, len2, len;
    bool result;

    len1 = arg1_len;
    len2 = arg2_len;
    data1 = arg1_data;
    data2 = arg2_data;

    len = (len1 < len2) ? len1 : len2;

    for (i = 0; i < len; i++) {
        /* Only for comparison on digits 0 - 9 */
        if (data1[i] >= 48 && data1[i] <= 57) {
            if (data1[i] < data2[i]) {
                return (Datum)1;
            } else if (data1[i] > data2[i]) {
                return (Datum)0;
            }
        } else {
            /* use the original strcmp function */
            int res = varstr_cmp(data1, len1, data2, len2, collid); 
            if (res >= 0) {
                return (Datum)0;
            } else {
                return (Datum)1;
            }     
        }
    }

    if (len1 >= len2) {
        return (Datum)0;
    } else {
        return (Datum)1;
    }   
}

/*
 * @Description: Clang this function and get the struct of PG_FUNCTION_ARGS in
 * LLVM module.
 * @in PG_FUNCTION_ARGS: function information.
 * @return: the result of function int4_bool
 */
extern "C"
Datum int4_bool2(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_INT32(0) == 0) {
        PG_RETURN_BOOL(false);
    } else {
        PG_RETURN_BOOL(true);
    }   
}

/*
 * @Description: Clang this function to get the LLVM IR function of text like
 * @in str: the source string
 * @in ptn: pattern string
 */
extern "C"
Datum LLVMIRtextlike(uint32_t str_len, char* str_data, uint32_t ptn_len, char* ptn_data)
{ 
    /*
     * str for source string , ptn for pattern string
     * only implement the case 'pattern%' , '%pattern', 'pattern' in which 'pattern' is not empty string
     * pattern string length should be greate than 1
     */
    int slen = str_len;
    int plen = ptn_len;
    char* str1 = str_data;
    char* ptr2 = ptn_data;

    if (ptr2[plen - 1] == '%') {
    /* pattern string is like 'pattern%' */
        plen--;
        if (slen < plen) {
            PG_RETURN_BOOL(false);
        }    
    } else if (ptr2[0] == '%') {
    /* pattern string is like '%pattern' */
        plen--;
        if (slen < plen) {
            PG_RETURN_BOOL(false);
        }   
        ptr2++;
        str1 += slen - plen;
    } else {
    /* pattern string is like 'pattern' */
        if (slen != plen) {
            PG_RETURN_BOOL(false);
        }    
    }
    
    while (plen) {
        if (*str1 != *ptr2) {
            PG_RETURN_BOOL(false);
        }  
        str1++;
        ptr2++;
        plen--;
    }
    
    PG_RETURN_BOOL(true);
}

/*
 * @Description: Clang this function to get the LLVM IR function of text not like
 * @in *t: the source string
 * @in tlen: the length of source string
 * @in *p: the pattern string
 * @in plen: the length of pattern string
 */
extern "C"
Datum LLVMIRtextnotlike(char *t, int tlen, char *p, int plen)
{
    while (tlen > 0 && plen > 0) {
        if (*p == '%') {
            char firstpat;
            p++;
            plen--;
        
            while (plen > 0) {
                if (*p == '%') {
                    p++;
                    plen--;
                } else {
                    break;
                } 
            }
        
            if (plen <= 0) {
                return 1;
            }

            firstpat = *p;
            while (tlen > 0) {
                if (*t == firstpat) {
                    int matched = LLVMIRtextnotlike(t, tlen, p, plen);
                    if (matched != 0) {
                        return matched;
                    }
                }
                /* corresponding to nextchar(t, tlen) */
                t++;
                tlen--;
            }
            return -1;
        } else if (*p != *t) {
            return 0;
        }
        
        /* correspoding to next(t, tlen), nextbyte(p, plen) */
        t++;
        tlen--;
        p++; 
        plen--;
    }
    
    if (tlen > 0) {
        return 0;
    }

    while (plen > 0 && *p == '%') {
        p++;
        plen--;
    }
    if (plen <= 0) {
        return 1;
    }

    return -1;
}


/*
 * @Description: Convert the BI Numeric to {i16, i64} format with the first element be the scale 
 * and the second element be the integer
 * @in num: The input numeric data with BI Numeric format
 * @return: The {i16, i64} representation of numeric data.
 */
extern "C"
numeric_type LLVMIRBINum2int8(Datum num)
{
    numeric_type result;
    uint8 scale;
    Datum res;
    Numeric arg = (Numeric)num;

    scale = arg->choice.n_header & 0x00FF;
    res = *(int64 *)(arg->choice.n_bi.n_data);

    result.scale = scale;
    result.value = res;
    return result;
}

/*
 * @Description: Get the status from HashAggRunner to check is the agg is finished or not
 * @in tbl: The data with HashAggRunner structure
 * @return: The status of current aggregation
 */
extern "C"
bool GetStatusofHashAggRunner(HashAggRunner *tbl)
{
    return tbl->m_finish;
}

/*
 * @Description: Get number of keys from SortAggRunner
 * @in tbl: The data with SortAggRunner structure  
 * @return: The number of keys of SortAggRunner
 */
extern "C"
int GetmkeyofSortAggRunner(SortAggRunner *tbl)
{
    return tbl->m_key;
}

/*
 * @Description: Get the pos from bictl. 
 * @in ctl: The bictl data
 * @return: The stored value of the special operation.
 * @Notes: We call this function to load the bictl data structure.
 */
extern "C"
Datum BigI64pos(bictl *ctl)
{    
    return ctl->store_pos;
}

/*
 * @Description: Call global variables to load the variables in LLVM module.
 * @in val: The input integer value.
 * @in delta_scale: Input parameters of Int64MultiOutOfBound and ScaleMultipler
 * @return: The result after evaluation.
 */
extern "C"
Datum Bi64LoadGlobalVar(Datum val, int delta_scale)
{
    Datum res, res1;
    if (val > Int64MultiOutOfBound[delta_scale]) {
        res = val * 1.0;
    } else if (val > ScaleMultipler[delta_scale]) {
        res = val * 2.0;
    }

    return res;
}

/*
 * @Description: By calling bi64addbi64 function to load the getScaleMultipler
 * instruction in LLVM module.
 * @in overflow: The flag of the overflow status.
 * @in larg: Input left numeric argument.
 * @in rarg: Input right numeric argument.
 * @return: The summeration of the arguments.
 */
extern "C"
Datum Simplebi64add64CodeGen(BiAdjustScale overflow, Numeric larg, Numeric rarg)
{
    uint8 lval_scale = NUMERIC_BI_SCALE(larg);
    uint8 rval_scale = NUMERIC_BI_SCALE(rarg);
    int64 left_val = NUMERIC_64VALUE(larg);
    int64 right_val = NUMERIC_64VALUE(rarg);
    int64 res = 0;
    int128 big_result = 0;
    int res_scale = 0;;

    if (lval_scale > rval_scale) {
        res_scale = lval_scale;
    } else {
        res_scale = rval_scale;
    }
        
    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, add directly */
        case BI_AJUST_TRUE:
            big_result = (int128)left_val + (int128)right_val;
            break;

        /* right_scaled is out of bound */
        case BI_RIGHT_OUT_OF_BOUND:
            big_result = (int128)left_val + (int128)right_val * getScaleMultiplier(res_scale - rval_scale);
            break;

        /* leftval_scaled is out of bound */
        default:
            big_result = (int128)left_val * getScaleMultiplier(res_scale - lval_scale) + (int128)right_val;
            break;
    }
    
    /* return big_result in Datum format */
    res = (int64)big_result;
    return res;
}

/*
 * @Description: Specifilize match key part of HashInnerJoin to get a good performance.
 * @in batch: Batch data information.
 * @in batch_idx: The row index of this batch.
 * @in cell: Hash cell data information.
 * @in numkeys: Number of keys.
 * @return: return true if all the keys are matched. 
 */
extern "C"
bool match_key(VectorBatch* batch, int batch_idx, hashCell* cell, int numkeys)
{
    bool match = true;
    int i;
    ScalarVector* p_vector = NULL;
    hashVal* hashval = NULL;
    FunctionCallInfoData fcinfo;
    Datum args[2];

    fcinfo.arg = &args[0];

    for (i = 0; i < numkeys; i++) {
        p_vector = &batch->m_arr[i];
        hashval = &cell->m_val[i];

        if (BOTH_NOT_NULL(p_vector->m_flag[batch_idx], hashval->flag)) {
            if (p_vector->m_desc.encoded == false) {
                if (p_vector->m_vals[batch_idx] == hashval->val) {
                    continue;
                } else {
                    return false;
                }     
            } else {
                fcinfo.arg[0] = ScalarVector::Decode(p_vector->m_vals[batch_idx]);
                fcinfo.arg[1] = ScalarVector::Decode(hashval->val);
                match = (fcinfo.arg[0] == fcinfo.arg[1]);
                if (match == false) {
                    return false;
                }
            }
        } else if (IS_NULL(p_vector->m_flag[batch_idx]) && IS_NULL(hashval->flag)) { // both null is equal
            continue;
        } else if (IS_NULL(p_vector->m_flag[batch_idx]) || IS_NULL(hashval->flag)) {
            return false;
        }   
    }

    return true;
}

/*
 * @Description: Get filter.class.BloomFilterImpl data structure by calling 
 * one of its parameter.
 * @in bloom_filter_impl: BloomFilterImpl structure
 * @return: return one of its member numBits.
 */
extern "C"
int64 getnumBitsBFImpl(filter::BloomFilterImpl<int64> *bloom_filter_impl)
{
    return bloom_filter_impl->numBits;
}

/*
 * @Description: Get sonic memory context from sonicdatumarray and so on.
 * @in data: sonic encoding datum array structure
 * @return: return mcxt.
 */
extern "C"
MemoryContext getSonicContext(SonicEncodingDatumArray *data)
{
    return data->m_cxt;
}

/*
 * @Description: Get hash context of sonic hashagg.
 * @in sonic_hagg: sonic hashagg data structure
 * @return: return hash context.
 */
extern "C"
MemoryContext getHashContext(SonicHashAgg *sonic_hagg)
{
    return sonic_hagg->m_memControl.hashContext;
}

/*
 * @Description: Get sonic datum array of sonic hashagg.
 * @in sonichagg: sonic hashagg data structure.
 * @return: return sonic datum array.
 */
extern "C"
SonicDatumArray** getSonicDataArray(SonicHashAgg *sonic_hagg)
{
    return sonic_hagg->m_data;
}

/*
 * @Description: Get sonic datumflag accordint to location.
 * @in loc: location of current sonic datum array value.
 * @in data: sonic datum array.
 * @return: return flag of data with location loc.
 */
extern "C"
uint8 getDataNthNullFlag(uint32 loc, SonicEncodingDatumArray *data)
{
    int arr_idx = getArrayIndx(loc, data->m_nbit);
    int atom_idx = getArrayLoc(loc, data->m_atomSize - 1);
    return data->getNthNullFlag(arr_idx, atom_idx);
}
