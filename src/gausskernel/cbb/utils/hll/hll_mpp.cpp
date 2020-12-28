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
 * hll_mpp.cpp
 *    distribute hll aggregation
 *
 * Description: distribute hll aggregation
 *
 * The design of HLL distribute agg
 * ---------------------------------
 *
 *            client
 *             |
 *             CN <---(2) collect phase (union hll_trans_type) (3) final phase (hll_trans_type to hll_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2 <---(1) trans phase: iterate every tuple on DN and apply transfunc
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- tuples
 *      | 7 |     | 8 |
 *      -----     -----
 *
 * Thanks to the nature of HyperLogLog algorithm we can use three phased aggregation
 * for the sake of ultimate performance in distribute computing.
 * In Three-Phased-Agg each DN can apply the thrans phase function
 * on the hashval/hll available at the Datanode. The result of transition
 * phase is then transferred to the Coordinator node for collect & final
 * phase processing.
 *
 * We create a temporary variable of data type namely hll_trans_type
 * to hold the current internal state of the trans phase and collection phase
 * For every input from the datanode(result of transition phase on that node),
 * the collection function is invoked with the current collection state
 * value and the new transition value(obtained from the datanode) to
 * caculate a new internal collection state value.
 *
 * After all the transition values from the data nodes have been processed.
 * In the third or finalization phase the final function is invoked once
 * to covert the internal state type(hll_trans_type) to the hll type.
 *
 *
 * A word about the hll_trans_type
 * ---------------------------------
 * we have two different data structure to repersent HyperLogLog
 * namely the hll_type(packed) and hll_trans_type(unpakced). The packed type
 * is small in size(1.2k typical) and good for storing the hll data in a table
 * and pass hll around through network channel. However it not well suited
 * for computing(add/union of hll). So we have a hll_trans_type(unpakced)
 * to do the computing and for internal state passing in distribute aggeration.
 *
 * Compared to greenplumn implementation of distribute hll, we avoid
 * the high cost of pack-unpack. Therefore we have a performance boost
 * under distribute aggeration. Roughly, 1x-10x performance gain compared with
 * sort/hash agg implementation of distinct algorithm.
 *
 * hll_add_agg
 * ------------
 * trans phase function  : hll_trans_type hll_add_trans0(hll_trans_type, hll_hash_val)
 * collect phase function: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 * final phase function  : hll hll_pack(hll_trans_type)
 *
 *            client
 *             |
 *             CN <---(2) collect phase: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 *                <---(3) final phase  : hll hll_pack(hll_trans_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- (1) trans phase: hll_trans_type hll_add_trans0(hll_trans_type, hll_hash_val)
 *      | 7 |     | 8 |
 *      -----     -----
 *
 * hll_union_agg
 * --------------
 * trans phase function  : hll_trans_type hll_union_trans(hll_trans_type, hll)
 * collect phase function: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 * final phase function  : hll hll_pack(hll_trans_type)
 *
 *            client
 *             |
 *             CN <---(2) collect phase: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 *                <---(3) final phase  : hll hll_pack(hll_trans_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- (1) trans phase:  hll_trans_type hll_union_trans(hll_trans_type, hll)
 *      | 7 |     | 8 |
 *      -----     -----
 * IDENTIFICATION
 *    src/gausskernel/cbb/utils/hll/hll_mpp.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <postgres.h>  // Needs to be first.
#include <inttypes.h>
#include "catalog/pg_type.h"
#include "catalog/pg_aggregate.h"
#include "nodes/nodeFuncs.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/var.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "hll.h"
#include "utils/int8.h"
#include "utils/memutils.h"

/*
 * @Description: alloc a multiset and wrap it in a varlen bytea
 *
 *      multiset_t is a fixed length type, somewhere around 128k
 *      at first we to use PASSBYVALUE, however pg_type.typlen field
 *      is type of int2 which can only hold max 256. So we have to
 *      wrap multiset_t in a varlen type(bytea) and the length in
 *      the varlen header in order the datumCopy() works well on it
 *
 * @param[IN] rcontext: the memory context to alloc mem
 * @return the wrapped multiset.
 */
bytea* setup_multiset(MemoryContext rcontext)
{
    MemoryContext oldcontext;
    multiset_t* msp = NULL;
    bytea* b = NULL;

    oldcontext = MemoryContextSwitchTo(rcontext);

    b = (bytea*)palloc0(sizeof(multiset_t) + VARHDRSZ);

    SET_VARSIZE(b, sizeof(multiset_t) + VARHDRSZ);

    msp = (multiset_t*)VARDATA(b);

    msp->ms_type = MST_UNINIT;

    MemoryContextSwitchTo(oldcontext);

    return b;
}

/* function for check parameters of hll */
void check_hll_para_range(int32 log2m, int32 regwidth, int64 expthresh, int32 sparseon)
{
    if (log2m < HLL_MIN_LOG2M || log2m > HLL_MAX_LOG2M)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("log2m = %d is out of range, it should be in range %d to %d",
                    log2m,
                    HLL_MIN_LOG2M,
                    HLL_MAX_LOG2M)));

    if (regwidth < HLL_MIN_REGWIDTH || regwidth > HLL_MAX_REGWIDTH)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("regwidth = %d is out of range, it should be in range %d to %d",
                    regwidth,
                    HLL_MIN_REGWIDTH,
                    HLL_MAX_REGWIDTH)));

    if (expthresh < HLL_MIN_EXPTHRESH || expthresh > HLL_MAX_EXPTHRESH)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expthresh = %ld is out of range, it should be in range %d to %d",
                    expthresh,
                    HLL_MIN_EXPTHRESH,
                    HLL_MAX_EXPTHRESH)));

    if (sparseon != HLL_MIN_SPARSEON && sparseon != HLL_MAX_SPARSEON)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("sparseon should be %d or %d", HLL_MIN_SPARSEON, HLL_MAX_SPARSEON)));
}

/*
 * @Description: transfunc for agg func hll_add_agg()
 *
 *      these function works on the first phase of aggeration.
 *      namely the transfunc. And they are *not* strict regrading
 *      of NULL handling upon the first invoke of the function in
 *      the aggretation.
 *
 *      we provide hll_add_trans function with different number
 *      of parameters which maximum to four and minimum to zero,
 *      and we also give two mode NORMAL and COMPRESSED which
 *      influce memory cost. And they are controlled by u_sess->attr.attr_sql.enable_compress_hll
 */
Datum hll_add_trans0(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans1(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans2(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans3(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans4(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

void handleParameter(int32* log2m, int32* regwidth, int64* expthresh, int32* sparseon, FunctionCallInfo fcinfo)
{
    switch (fcinfo->nargs) {
        case HLL_PARAMETER0:
            *log2m = u_sess->attr.attr_sql.g_default_log2m;
            *regwidth = u_sess->attr.attr_sql.g_default_regwidth;
            *expthresh = u_sess->attr.attr_sql.g_default_expthresh;
            *sparseon = u_sess->attr.attr_sql.g_default_sparseon;
            break;
        case HLL_PARAMETER1:
            *log2m = PG_ARGISNULL(2) ? u_sess->attr.attr_sql.g_default_log2m : PG_GETARG_INT32(2);
            *regwidth = u_sess->attr.attr_sql.g_default_regwidth;
            *expthresh = u_sess->attr.attr_sql.g_default_expthresh;
            *sparseon = u_sess->attr.attr_sql.g_default_sparseon;
            break;
        case HLL_PARAMETER2:
            *log2m = PG_ARGISNULL(2) ? u_sess->attr.attr_sql.g_default_log2m : PG_GETARG_INT32(2);
            *regwidth = PG_ARGISNULL(3) ? u_sess->attr.attr_sql.g_default_regwidth : PG_GETARG_INT32(3);
            *expthresh = u_sess->attr.attr_sql.g_default_expthresh;
            *sparseon = u_sess->attr.attr_sql.g_default_sparseon;
            break;
        case HLL_PARAMETER3:
            *log2m = PG_ARGISNULL(2) ? u_sess->attr.attr_sql.g_default_log2m : PG_GETARG_INT32(2);
            *regwidth = PG_ARGISNULL(3) ? u_sess->attr.attr_sql.g_default_regwidth : PG_GETARG_INT32(3);
            *expthresh = PG_ARGISNULL(4) ? u_sess->attr.attr_sql.g_default_expthresh : PG_GETARG_INT64(4);
            *sparseon = u_sess->attr.attr_sql.g_default_sparseon;
            break;
        case HLL_PARAMETER4:
            *log2m = PG_ARGISNULL(2) ? u_sess->attr.attr_sql.g_default_log2m : PG_GETARG_INT32(2);
            *regwidth = PG_ARGISNULL(3) ? u_sess->attr.attr_sql.g_default_regwidth : PG_GETARG_INT32(3);
            *expthresh = PG_ARGISNULL(4) ? u_sess->attr.attr_sql.g_default_expthresh : PG_GETARG_INT64(4);
            *sparseon = PG_ARGISNULL(5) ? u_sess->attr.attr_sql.g_default_sparseon : PG_GETARG_INT32(5);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("no such parameters of input")));
    }
}
/*
 * @Description:
 *      this function will be called when u_sess->attr.attr_sql.enable_compress_hll is OFF
 *      a hashval will be added to multiset_t type and just return it.
 *
 * @param[IN] multiset_t: NULL indicates the first call, multiset_t
                           passed as input.
 * @param[IN] hashval: the hashval need to be add to multiset_t.
 * @return multiset_t.
 */
Datum hll_add_trans_normal(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;
    multiset_t* msap = NULL;
    bytea* msab = NULL;
    int32 log2m;
    int32 regwidth;
    int64 expthresh;
    int32 sparseon;

    /* We must be called as a transition routine or we fail. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_add_trans_normal outside transition context")));

    /* handle numbers of parameter we need to take */
    handleParameter(&log2m, &regwidth, &expthresh, &sparseon, fcinfo);
    /* check all parameters of hll */
    check_hll_para_range(log2m, regwidth, expthresh, sparseon);
    expthresh = integer_exp2(expthresh);

    /* If the first argument is a NULL on first call, init an hll_empty */
    if (PG_ARGISNULL(0)) {
        msab = setup_multiset(aggctx);
        msap = (multiset_t*)VARDATA(msab);

        check_modifiers(log2m, regwidth, expthresh, sparseon);

        errno_t rc = memset_s(msap, sizeof(multiset_t), '\0', sizeof(multiset_t));
        securec_check(rc, "\0", "\0");

        msap->ms_type = MST_EMPTY;
        msap->ms_nbits = regwidth;
        msap->ms_nregs = 1 << (unsigned int)log2m;
        msap->ms_log2nregs = (unsigned int) log2m;
        msap->ms_expthresh = expthresh;
        msap->ms_sparseon = sparseon;
    } else {
        msab = (bytea*)PG_GETARG_POINTER(0);
        msap = (multiset_t*)VARDATA(msab);
    }

    // Is the second argument non-null?
    if (!PG_ARGISNULL(1)) {
        int64 hashval = PG_GETARG_INT64(1);
        multiset_add(msap, hashval);
    }

    PG_RETURN_BYTEA_P(msab);
}

/*
 * @Description:
 *      this function will be called when u_sess->attr.attr_sql.enable_compress_hll is ON
 *      first the input is a hll (compressed multiset_t) then unpack it
 *      to multiset_t and add hashval to it. finally we need pack mutiset_t
 *      to hll and return it.
 *
 * @param[IN] hll: NULL indicates the first call, hll passed as input.
 * @param[IN] hashval: the hashval need to be add to multiset_t.
 * @return hll.
 */
Datum hll_add_trans_compressed(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;
    multiset_t msa;
    bytea* cb = NULL;
    size_t csz;
    int32 log2m;
    int32 regwidth;
    int64 expthresh;
    int32 sparseon;

    /* We must be called as a transition routine or we fail. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(
            ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_add_trans_compressed outside transition context")));

    /* handle numbers of parameter we need to take */
    handleParameter(&log2m, &regwidth, &expthresh, &sparseon, fcinfo);
    /* check all parameters of hll */
    check_hll_para_range(log2m, regwidth, expthresh, sparseon);
    expthresh = integer_exp2(expthresh);

    /*
     If the first argument is a NULL on first call, init an hll_empty
     If it's NON-NULL, unpacked it in compressed mode
     */
    if (PG_ARGISNULL(0)) {
        check_modifiers(log2m, regwidth, expthresh, sparseon);

        (&msa)->ms_type = MST_EMPTY;
        (&msa)->ms_nbits = regwidth;
        (&msa)->ms_nregs = 1 << (unsigned int)log2m;
        (&msa)->ms_log2nregs = (unsigned int)log2m;
        (&msa)->ms_expthresh = expthresh;
        (&msa)->ms_sparseon = sparseon;
    } else {
        bytea* ab = NULL;
        size_t asz;

        ab = PG_GETARG_BYTEA_P(0);
        asz = VARSIZE(ab) - VARHDRSZ;
        // unpack hll to multiset_t
        multiset_unpack(&msa, (uint8_t*)VARDATA(ab), asz, NULL);
    }

    // Is the second argument non-null?
    if (!PG_ARGISNULL(1)) {
        int64 hashval = PG_GETARG_INT64(1);
        multiset_add(&msa, hashval);
    }

    // packed multiset_t to hll then return it
    csz = multiset_packed_size(&msa);

    cb = (bytea*)palloc0(VARHDRSZ + csz);
    SET_VARSIZE(cb, VARHDRSZ + csz);
    multiset_pack(&msa, (uint8_t*)VARDATA(cb), csz);

    PG_RETURN_BYTEA_P(cb);
}

/*
* call NORMAL or COMPRESSED function by u_sess->attr.attr_sql.enable_compress_hll
*/
Datum hll_add_trans_n(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_add_trans_compressed(fcinfo);
    } else {
        return hll_add_trans_normal(fcinfo);
    }
}

/*
 * @Description: collectfunc for agg func hll_add_agg() and hll_union_agg()
 *
 *		we also need unpacked input hll to multiset_t and union them like in
 *		hll_union_collect_normal, then packed it as output
 *
 * @param[IN] hll: NULL indicates the first call, passed in the
 *                        transvalue in the successive call
 * @param[IN] hll: the other multiset_t to be unioned.
 * @return the unioned hll as the transvalue.
 */
Datum hll_union_collect_compressed(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;

    bytea* cb = NULL;
    multiset_t msa;
    size_t csz;

    /* now inputs are all hll, we need unpack them, calculate and return hll. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_collect_compressed outside transition context")));

    if (PG_ARGISNULL(0)) {
        (&msa)->ms_type = MST_UNINIT;
    } else {
        bytea* ab = PG_GETARG_BYTEA_P(0);
        size_t asz = VARSIZE(ab) - VARHDRSZ;

        multiset_unpack(&msa, (uint8_t*)VARDATA(ab), asz, NULL);
    }

    if (!PG_ARGISNULL(1)) {
        multiset_t msc;
        bytea* bb = (bytea*)PG_GETARG_BYTEA_P(1);
        size_t bsz = VARSIZE(bb) - VARHDRSZ;

        multiset_unpack(&msc, (uint8_t*)VARDATA(bb), bsz, NULL);
        /* Was the first argument uninitialized? */
        if ((&msa)->ms_type == MST_UNINIT) {
            /* Yes, clone the metadata from the second arg. */
            copy_metadata(&msa, &msc);
            (&msa)->ms_type = MST_EMPTY;
        } else {
            /* Nope, make sure the metadata is compatible. */
            check_metadata(&msa, &msc);
        }

        multiset_union(&msa, &msc);
    }

    csz = multiset_packed_size(&msa);

    cb = (bytea*)palloc0(VARHDRSZ + csz);
    SET_VARSIZE(cb, VARHDRSZ + csz);

    multiset_pack(&msa, (uint8_t*)VARDATA(cb), csz);

    PG_RETURN_BYTEA_P(cb);
}

/*
 * @Description: collectfunc for agg func hll_add_agg() and hll_union_agg()
 *
 *      this function works on the second stage for both hll_add_agg()
 *      and hll_union_agg(). it union two varlen multiset_t and the unioned
 *      varlen multiset_t is returned.
 *
 * @param[IN] multiset_t: NULL indicates the first call, passed in the
 *                        transvalue in the successive call
 * @param[IN] multiset_t: the other multiset_t to be unioned.
 * @return the unioned multiset_t as the transvalue.
 */
Datum hll_union_collect_normal(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;

    bytea* msab = NULL;
    bytea* msbb = NULL;

    multiset_t* msap = NULL;
    multiset_t* msbp = NULL;

    /* now inputs are all hll, we need unpack them, calculate and return hll. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(
            ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_collect_normal outside transition context")));

    // Is the first argument a NULL?
    if (PG_ARGISNULL(0)) {
        msab = setup_multiset(aggctx);
        msap = (multiset_t*)VARDATA(msab);
    } else {
        msab = (bytea*)PG_GETARG_POINTER(0);
        msap = (multiset_t*)VARDATA(msab);
    }

    /* Is the second argument non-null? */
    if (!PG_ARGISNULL(1)) {
        msbb = (bytea*)PG_GETARG_POINTER(1);
        msbp = (multiset_t*)VARDATA(msbb);

        /* Was the first argument uninitialized? */
        if (msap->ms_type == MST_UNINIT) {
            /* Yes, clone the metadata from the second arg. */
            copy_metadata(msap, msbp);
            msap->ms_type = MST_EMPTY;
        } else {
            /* Nope, make sure the metadata is compatible. */
            check_metadata(msap, msbp);
        }

        multiset_union(msap, msbp);
    }

    PG_RETURN_BYTEA_P(msab);
}

/*
* @ Description
*
*  here we have NORMAL and COMPRESSED mode for
*  hll_union_collect functions, same like hll_add_trans_n
*  and hll_union_trans.
*/
Datum hll_union_collect(PG_FUNCTION_ARGS)
{
    /*
        If both arguments are null then we return null, because we shouldn't
        return any hll with UNINIT type state.
    */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }

    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_union_collect_compressed(fcinfo);
    } else {
        return hll_union_collect_normal(fcinfo);
    }
}

/*
 * @Description: finalfunc for agg func hll_add_agg() and hll_union_agg()
 *
 *      this function works on the final stage for hll agg funcs
.*      if u_sess->attr.attr_sql.enable_compress_hll is one we call hll_pack_compressed
.*      return input directly.
 */
Datum hll_pack_compressed(PG_FUNCTION_ARGS)
{
    bytea* msab = NULL;
    size_t asz;
    multiset_t msa;

    // unpack transtype first
    msab = PG_GETARG_BYTEA_P(0);
    asz = VARSIZE(msab) - VARHDRSZ;

    multiset_unpack(&msa, (uint8_t*)VARDATA(msab), asz, NULL);

    /* Was the aggregation uninitialized? */
    if (msa.ms_type == MST_UNINIT) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_BYTEA_P(msab);
    }
}

/*
 * @Description: finalfunc for agg func hll_add_agg() and hll_union_agg()
 *
 *      this function works on the final stage for hll agg funcs
 *      it *pack* the big 128k multiset_t into a tider type *hll*
 *      which is 1.2k in size. the packed value is returned to caller
 *      as the aggeration result.
 *
 * @param[IN] multiset_t: the varlen multiset_t to be packed
 * @return the packed hll type is returned.
 */
Datum hll_pack_normal(PG_FUNCTION_ARGS)
{
    bytea* cb = NULL;
    size_t csz;

    multiset_t* msap = NULL;
    bytea* msab = NULL;

    msab = (bytea*)PG_GETARG_POINTER(0);
    msap = (multiset_t*)VARDATA(msab);

    /* Was the aggregation uninitialized? */
    if (msap->ms_type == MST_UNINIT) {
        PG_RETURN_NULL();
    } else {
        csz = multiset_packed_size(msap);
        cb = (bytea*)palloc0(VARHDRSZ + csz);
        SET_VARSIZE(cb, VARHDRSZ + csz);

        multiset_pack(msap, (uint8_t*)VARDATA(cb), csz);

        /* We don't need to pfree the msap memory because it is zone
         * allocated inside postgres.
         *
         * Furthermore, sometimes final functions are called multiple
         * times so deallocating it the first time leads to badness.
         */
        PG_RETURN_BYTEA_P(cb);
    }
}

/*
 * call hll_pack_compressed or hll_pack_normal based on u_sess->attr.attr_sql.enable_compress_hll
 */
Datum hll_pack(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_pack_compressed(fcinfo);
    } else {
        return hll_pack_normal(fcinfo);
    }
}

/*
 * @Description: transfunc for agg func hll_union_agg()
 *
 *      this function works on the first stage for agg func
 *      hll_union_agg() which union a transvalue(multiset_t)
 *      and a packed(hll) together and a valen multiset_t
 *      is returned.
 *
 * @param[IN] multiset_t: the transvalue for aggeration
 * @param[IN] hll: passed column(type is hll) for hll_union_agg()
 * @return the varlen multiset_t
 */
Datum hll_union_trans_normal(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;

    bytea* msab = NULL;
    multiset_t* msap = NULL;
    multiset_t msb;

    /* We must be called as a transition routine or we fail. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_trans_normal outside transition context")));

    /* Is the first argument a NULL? */
    if (PG_ARGISNULL(0)) {
        msab = setup_multiset(aggctx);
        msap = (multiset_t*)VARDATA(msab);
    } else {
        msab = (bytea*)PG_GETARG_POINTER(0);
        msap = (multiset_t*)VARDATA(msab);
    }

    /* Is the second argument non-null? */
    if (!PG_ARGISNULL(1)) {
        /* This is the packed "argument" vector. */
        bytea* bb = PG_GETARG_BYTEA_P(1);
        size_t bsz = VARSIZE(bb) - VARHDRSZ;

        multiset_unpack(&msb, (uint8_t*)VARDATA(bb), bsz, NULL);

        /* Was the first argument uninitialized? */
        if (msap->ms_type == MST_UNINIT) {
            /* Yes, clone the metadata from the second arg. */
            copy_metadata(msap, &msb);
            msap->ms_type = MST_EMPTY;
        } else {
            /* Nope, make sure the metadata is compatible. */
            check_metadata(msap, &msb);
        }

        multiset_union(msap, &msb);
    }

    PG_RETURN_BYTEA_P(msab);
}

/*
 * @Description: transfunc for agg func hll_union_agg()
 *
.*	As same as hll_union_trans_normal, but we also need
.*	unpacked input hll and packed multiset_t to hll as output
 *
 * @param[IN] hll: the transvalue for aggeration
 * @param[IN] hll: passed column(type is hll) for hll_union_agg()
 * @return hll
 */
Datum hll_union_trans_compressed(PG_FUNCTION_ARGS)
{
    MemoryContext aggctx;

    multiset_t msa;
    multiset_t msb;
    bytea* cb = NULL;
    size_t csz;

    /* We must be called as a transition routine or we fail. */
    if (!AggCheckCallContext(fcinfo, &aggctx))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_trans_compressed context")));

    /* Is the first argument a NULL? */
    if (PG_ARGISNULL(0)) {
        msa.ms_type = MST_UNINIT;
    } else {
        bytea* ab = PG_GETARG_BYTEA_P(0);
        size_t asz = VARSIZE(ab) - VARHDRSZ;

        multiset_unpack(&msa, (uint8_t*)VARDATA(ab), asz, NULL);
    }

    /* Is the second argument non-null? */
    if (!PG_ARGISNULL(1)) {
        /* This is the packed "argument" vector. */
        bytea* bb = PG_GETARG_BYTEA_P(1);
        size_t bsz = VARSIZE(bb) - VARHDRSZ;

        multiset_unpack(&msb, (uint8_t*)VARDATA(bb), bsz, NULL);

        /* Was the first argument uninitialized? */
        if (msa.ms_type == MST_UNINIT) {
            /* Yes, clone the metadata from the second arg. */
            copy_metadata(&msa, &msb);
            msa.ms_type = MST_EMPTY;
        } else {
            /* Nope, make sure the metadata is compatible. */
            check_metadata(&msa, &msb);
        }

        multiset_union(&msa, &msb);
    }

    csz = multiset_packed_size(&msa);

    cb = (bytea*)palloc0(VARHDRSZ + csz);
    SET_VARSIZE(cb, VARHDRSZ + csz);
    multiset_pack(&msa, (uint8_t*)VARDATA(cb), csz);

    PG_RETURN_BYTEA_P(cb);
}

Datum hll_union_trans(PG_FUNCTION_ARGS)
{
    /*
      here if two arguments are both null, we just return null,
      because we shouldn't return uninit hll to cn.
    */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }

    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_union_trans_compressed(fcinfo);
    } else {
        return hll_union_trans_normal(fcinfo);
    }
}

/*
 * @Description:
 *      we don;t need packed it in hll_trans_recv_compressed
 *      and just return hll directly
 */
Datum hll_trans_recv_compressed(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));
    return dd;
}

/*
 * @Description: recv func for hll_trans_type(multiset_t) in MORMAL
 *
 *      this function accept a binary format and do
 *      (1) convert the binary format to bytea format using bytearecv
 *      (2) alloc a varlen multiset_t
 *      (3) unpack the bytea into the multiset_t
 *
 */
Datum hll_trans_recv_normal(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));
    bytea* ab = DatumGetByteaP(dd);
    size_t asz = VARSIZE(ab) - VARHDRSZ;

    bytea* msab = setup_multiset(CurrentMemoryContext);
    multiset_t* msap = (multiset_t*)VARDATA(msab);

    multiset_unpack(msap, (uint8_t*)VARDATA(ab), asz, NULL);

    return PointerGetDatum(msab);
}

/*
 * @Description:
 *  split to hll_trans_recv_normal and hll_trans_recv_compressed
 *  controlled by u_sess->attr.attr_sql.enable_compress_hll.
 */
Datum hll_trans_recv(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_trans_recv_compressed(fcinfo);
    } else {
        return hll_trans_recv_normal(fcinfo);
    }
}

Datum hll_trans_send_compressed(PG_FUNCTION_ARGS)
{
    bytea* bp = (bytea*)PG_GETARG_POINTER(0);

    StringInfoData buf;
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA(bp), VARSIZE(bp) - VARHDRSZ);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * @Description: send func for hll_trans_type(or multiset_t)
 *
 *      this function accept a varlen multiset_t do
 *      (1) convert the multiset_t into packed format
 *      (2) send the packed data
 *
 *      by pack the varlen multiset_t we save some network traffic
 */
Datum hll_trans_send_normal(PG_FUNCTION_ARGS)
{
    bytea* msab = (bytea*)PG_GETARG_POINTER(0);
    multiset_t* msap = (multiset_t*)VARDATA(msab);
    size_t csz = multiset_packed_size(msap);
    bytea* bp = (bytea*)palloc0(VARHDRSZ + csz);

    SET_VARSIZE(bp, VARHDRSZ + csz);

    multiset_pack(msap, (uint8_t*)VARDATA(bp), csz);

    StringInfoData buf;
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA(bp), VARSIZE(bp) - VARHDRSZ);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum hll_trans_send(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_trans_send_compressed(fcinfo);
    } else {
        return hll_trans_send_normal(fcinfo);
    }
}

Datum hll_trans_in_compressed(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));
    return dd;
}

/*
 * @Description: input func for hll_trans_type(or multiset_t)
 *
 *      this function accept a cstring format and do
 *      (1) convert the cstring format to bytea format using byteain
 *      (2) alloc a varlen multiset_t
 *      (3) unpack the bytea into the multiset_t
 *
 *      by pack the varlen multiset_t we save some network traffic
 *
 */
Datum hll_trans_in_normal(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));
    bytea* bp = DatumGetByteaP(dd);
    size_t sz = VARSIZE(bp) - VARHDRSZ;

    bytea* msb = setup_multiset(CurrentMemoryContext);
    multiset_t* msp = (multiset_t*)VARDATA(msb);
    multiset_unpack(msp, (uint8_t*)VARDATA(bp), sz, NULL);

    PG_RETURN_BYTEA_P(msb);
}

Datum hll_trans_in(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_trans_in_compressed(fcinfo);
    } else {
        return hll_trans_in_normal(fcinfo);
    }
}

Datum hll_trans_out_compressed(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));
    return dd;
}

/*
 * @Description: output func for hll_trans_type(or multiset_t)
 *
 *      this function accept a varlen multiset_t and do
 *      (1) convert the multiset_t into packed format
 *      (2) send the packed data
 *
 *      by pack the varlen multiset_t we save some network traffic
 *
 */
Datum hll_trans_out_normal(PG_FUNCTION_ARGS)
{
    bytea* msab = (bytea*)PG_GETARG_POINTER(0);
    multiset_t* msap = (multiset_t*)VARDATA(msab);

    size_t csz = multiset_packed_size(msap);
    bytea* cb = (bytea*)palloc0(VARHDRSZ + csz);

    SET_VARSIZE(cb, VARHDRSZ + csz);
    multiset_pack(msap, (uint8_t*)VARDATA(cb), csz);

    Datum dd = DirectFunctionCall1(byteaout, PointerGetDatum(cb));

    return dd;
}

Datum hll_trans_out(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.enable_compress_hll) {
        return hll_trans_out_compressed(fcinfo);
    } else {
        return hll_trans_out_normal(fcinfo);
    }
}

static bool contain_hll_agg_walker(Node* node, int* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Aggref)) {
        Aggref* arf = (Aggref*)node;
        if (arf->aggfnoid == HLL_ADD_TRANS0_OID || arf->aggfnoid == HLL_ADD_TRANS1_OID ||
            arf->aggfnoid == HLL_ADD_TRANS2_OID || arf->aggfnoid == HLL_ADD_TRANS3_OID ||
            arf->aggfnoid == HLL_ADD_TRANS4_OID || arf->aggfnoid == HLL_UNION_TRANS_OID)
            (*context)++;
        return false;
    }

    return expression_tree_walker(node, (bool (*)())contain_hll_agg_walker, context);
}

static int contain_hll_agg(Node* node)
{
    int count = 0;

    (void)contain_hll_agg_walker(node, &count);
    return count;
}

/*
 * @Description: the function is uesd for estimate used memory in hll mode.
 *
 *      we use function contain_hll_agg and contain_hll_agg_walker to parse query node
 *      to get numbers of hll_agg functions.
 *
 * @return: the size of hash_table_size in hll mode which is used for estimate e-memory.
 */
double estimate_hllagg_size(double numGroups, List* tleList)
{
    int tmp = contain_hll_agg((Node*)tleList);
    double hash_table_size;

    if (u_sess->attr.attr_sql.enable_compress_hll) {
        // a HLL takes regwidth * 2^log2m bits to store
        int64 hll_size = (int64)(
            (u_sess->attr.attr_sql.g_default_regwidth * ((int64)pow(2, u_sess->attr.attr_sql.g_default_log2m))) / 8);
        hash_table_size = ((numGroups * tmp * hll_size) / 1024L);
    } else {
        hash_table_size = ((numGroups * tmp * ((int64)(131 * 1024))) / 1024L);
    }

    return hash_table_size * 2;
}
