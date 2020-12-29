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
 * int8.inl
 *	  template implementation of 64-bit integers.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/vecexecutor/vecprimitive/int8.inl
 *
 *-------------------------------------------------------------------------
 */

#ifndef INT8_INL
#define INT8_INL

#include "utils/int8.h"
#include "utils/biginteger.h"
#include "catalog/pg_type.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vechashagg.h"
#include "vectorsonic/vsonichashagg.h"
#include "vectorsonic/vsonicarray.h"

#define SAMESIGN(a,b)	(((a) < 0) == ((b) < 0))

template <SimpleOp sop,typename Datatype1,typename Datatype2>
ScalarVector*
vint8_sop(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	ScalarValue* parg2 = PG_GETARG_VECVAL(1);
	int32		   nvalues = PG_GETARG_INT32(2);
	ScalarValue* presult = PG_GETARG_VECVAL(3);
	uint8*	pflag = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	bool*		   pselection = PG_GETARG_SELECTION(4);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
	int          i;


    if(likely(pselection == NULL))
    {
    	for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				presult[i] = eval_simple_op<sop, int64>((Datatype1)parg1[i], (Datatype2)parg2[i]);
				SET_NOTNULL(pflag[i]);
			}
			else
				SET_NULL(pflag[i]);
		}
    }
	else
	{
		for (i = 0; i < nvalues; i++)
		{
			if(pselection[i])
			{
				if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
				{
					presult[i] = eval_simple_op<sop, int64>((Datatype1)parg1[i], (Datatype2)parg2[i]);
					SET_NOTNULL(pflag[i]);
				}
				else
					SET_NULL(pflag[i]);
			}
		}
	}

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = BOOLOID;
    return PG_GETARG_VECTOR(3);
}

template <typename datatype,SimpleOp sop>
ScalarVector*
vint_min_max(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	hashCell*	  cell = NULL;
	int			  i;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;
	Datum 		  args[2];
	Datum		  result;

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(IS_NULL(cell->m_val[idx].flag))
			{
				cell->m_val[idx].val = pVal[i];
				SET_NOTNULL(cell->m_val[idx].flag);
			}
			else
			{
				args[0] = cell->m_val[idx].val;
				args[1] = pVal[i];

				if (sop == SOP_GT)
					result = (((datatype)args[0] > (datatype)args[1]) ? args[0] : args[1]);
				else
					result = (((datatype)args[0] < (datatype)args[1]) ? args[0] : args[1]);

				cell->m_val[idx].val = result;
			}
		}
	}

	return NULL;
}

/*
 * @Description	: min or max(int)'s agg function that supprot int2, int4, int8.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.   
 * @out m_data[idx]	: value of min or max(int) for each group.
 */
template <typename datatype,SimpleOp sop>
ScalarVector*
vsint_min_max(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			idx = (int)PG_GETARG_DATUM(1);
	uint32*		loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicEncodingDatumArray* data = (SonicEncodingDatumArray*) sdata[idx];

	int				i;
	ScalarValue* 	pVal = pVector->m_vals;
	uint8*			flag = pVector->m_flag;
	int				nrows = pVector->m_rows;
	datatype*		leftdata = NULL;
	uint8			leftflag;
	int				arrIndx, atomIndx;

	for (i = 0 ; i < nrows; i++)
	{
		 /* only do when not null */
		if (loc[i] != 0 && NOT_NULL(flag[i]))
		{
			/* previous result(leftdata) */
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);
			
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);
			leftdata = &((datatype *)data->m_arr[arrIndx]->data)[atomIndx];

			if (IS_NULL(leftflag))
			{
				*leftdata = pVal[i];
				data->setNthNullFlag(arrIndx, atomIndx, false);
			}
			else
			{
				if (sop == SOP_GT)
					*leftdata = (((datatype)leftdata[0] > (datatype)pVal[i]) ? leftdata[0] : pVal[i]);
				else
					*leftdata = (((datatype)leftdata[0] < (datatype)pVal[i]) ? leftdata[0] : pVal[i]);
			}
		}
	}

	return NULL;
}

template<bool isTransition>
ScalarVector*
vint8_avg(PG_FUNCTION_ARGS)
{

	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);

	MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);

	hashCell*	  cell = NULL;
	int			  arg1, arg2, i;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;
	Datum	      *datumarray = NULL;
	int			  ndatum;
	bictl		  ctl;
	Numeric		  leftarg, rightarg;	// left-hand and right-hand operand of addition
	ctl.context = context;


	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(NOT_NULL(cell->m_val[idx].flag))
			{
				if (isTransition)
				{
					/* leftarg is bi64 or bi128 */
					leftarg = (Numeric)(cell->m_val[idx].val);
					/* rightarg is bi64 */
					rightarg = (Numeric)(makeNumeric64(pVal[i], 0));
					ctl.store_pos = cell->m_val[idx].val;
					arg1 = NUMERIC_IS_BI128(leftarg);
					/* call big integer fast add function: bi128 add bi64 */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][0])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					cell->m_val[idx].val = ctl.store_pos;
					cell->m_val[idx + 1].val++; //count++
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);
					/* leftarg is bi64 or bi128 */
					leftarg = (Numeric)(cell->m_val[idx].val);
					/* rightarg is bi64 or bi128 */
					rightarg = (Numeric)(datumarray[1]);
					ctl.store_pos = cell->m_val[idx].val;
					arg1 = NUMERIC_IS_BI128(leftarg);
					arg2 = NUMERIC_IS_BI128(rightarg);
					/* call big integer fast add function: bi128 add bi128 */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					cell->m_val[idx].val = ctl.store_pos;

					/* calculate count: count = oldcount + count */
					/* the count num can be stored by int64, call bi64add64 directly */
					ctl.store_pos = cell->m_val[idx + 1].val;
					BiAggFunMatrix[BI_AGG_ADD][0][0]((Numeric)(ctl.store_pos), (Numeric)(datumarray[0]), &ctl);
				}
			}
			else
			{
				if(isTransition)
				{
					/* store the first value to bi64 */
					cell->m_val[idx].val = addVariable(context, makeNumeric64(pVal[i], 0));
					cell->m_val[idx + 1].val = 1; //count set 1
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
									  NUMERICOID, -1, false, 'i',
									  &datumarray, NULL, &ndatum);

					cell->m_val[idx].val = addVariable(context, datumarray[1]);
					cell->m_val[idx + 1].val = addVariable(context, datumarray[0]);
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				SET_NULL(cell->m_val[idx + 2].flag);
			}
		}
	}
	return NULL;
}

/*
 * @Description	: avg(int8)'s transition function.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.   
 * @out m_data[idx]	: value of sum(int) for each group.
 * @out m_data[idx + 1]	: number of sum(int8) for each group.
 */
template<bool isTransition>
ScalarVector*
vsint8_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int idx = (int)PG_GETARG_DATUM(1);
	uint32*	  loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicEncodingDatumArray* data = (SonicEncodingDatumArray*)sdata[idx];

	SonicDatumArray* count = sdata[idx + 1];
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		flag = pVector->m_flag;
	int			nrows = pVector->m_rows;

	/* left-hand and right-hand operand of addition  */
	Datum		args[2];
	Datum		*leftdata = NULL;
	Datum		*countdata = NULL;
	uint8		leftflag;
	/* left-hand and right-hand operand of addition */
	Numeric		leftarg, rightarg;	
	int			arg1, arg2, i;
	Datum		*datumarray = NULL;
	int			arrIndx,atomIndx;
	int			ndatum;
	
	FunctionCallInfoData finfo;
	bictl		  ctl;
	finfo.arg = &args[0];
	ctl.context = data->m_cxt;
	
	for (i = 0 ; i < nrows; i++)
	{
		/* only do when not null */
		if (loc[i] != 0 && NOT_NULL(flag[i])) 
		{
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

			/* get previous datum flag */
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);

			/* previous count result(leftdata) */
			countdata = &((Datum *)count->m_arr[arrIndx]->data)[atomIndx];

			if (NOT_NULL(leftflag))
			{
				/* get data value */
				leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];

				/* updata previous sum result based on the given pVal[i] */
				if (isTransition)
				{
					/* leftarg is bi64 or bi128 */
					leftarg = (Numeric)(leftdata[0]);
					/* rightarg is bi64 */
					rightarg = (Numeric)(makeNumeric64(pVal[i], 0));
					ctl.store_pos = leftdata[0];
					arg1 = NUMERIC_IS_BI128(leftarg);
					/* call big integer fast add function: bi128 add bi64 */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][0])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					leftdata[0] = ctl.store_pos;
					countdata[0]++;
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);
					/* leftarg is bi64 or bi128 */
					leftarg = (Numeric)(leftdata[0]);
					/* rightarg is bi64 or bi128 */
					rightarg = (Numeric)(datumarray[1]);
					ctl.store_pos = leftdata[0];
					arg1 = NUMERIC_IS_BI128(leftarg);
					arg2 = NUMERIC_IS_BI128(rightarg);
					/* call big integer fast add function: bi128 add bi128 */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					leftdata[0] = ctl.store_pos;

					/* calculate count: count = oldcount + count */
					*countdata = *countdata + NUMERIC_64VALUE((Numeric)datumarray[0]);
				}
			}
			else
			{
				/* store the first data pVal[i] in data and m_count */
				if (isTransition)
				{
					/* store the first value to bi64 */
					data->setValue(makeNumeric64(pVal[i], 0), false, arrIndx, atomIndx);
					countdata[0] = 1;
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
									  NUMERICOID, -1, false, 'i',
									  &datumarray, NULL, &ndatum);

					data->setValue(datumarray[1], false, arrIndx, atomIndx);
					*countdata = NUMERIC_64VALUE((Numeric)datumarray[0]);
				}
			}
		}
	}
	return NULL;
}

template <PGFunction absfun>
ScalarVector*
vector_abs(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	int32		   nvalues = PG_GETARG_INT32(1);
	ScalarValue* presult = PG_GETARG_VECVAL(2);
	bool*		   pselection = PG_GETARG_SELECTION(3);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8* 	pflagsRes = (uint8*)(PG_GETARG_VECTOR(2)->m_flag);
	int i;
	Datum args;
	FunctionCallInfoData finfo;

	finfo.arg = &args;

	if (likely(pselection == NULL))
	{   
		for (i = 0 ; i < nvalues; i++)
		{
			if (NOT_NULL(pflags1[i]))
			{
				args = parg1[i];
				presult[i] =  absfun(&finfo);
				SET_NOTNULL(pflagsRes[i]);
			}
			else
				SET_NULL(pflagsRes[i]);
		}
	}
	else
	{
		for (i = 0 ; i < nvalues; i++)
		{
			if(pselection[i])
			{
				if (NOT_NULL(pflags1[i]))
				{	
					args = parg1[i];
					presult[i] =  absfun(&finfo);
					SET_NOTNULL(pflagsRes[i]);	
				}
				else
					SET_NULL(pflagsRes[i]);
			}
		}
	}

	PG_GETARG_VECTOR(2)->m_rows = nvalues;	

	return PG_GETARG_VECTOR(2);

}

template <typename Datatype1,typename Datatype2>
ScalarVector*
vint8mul(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32		nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool*		pselection = PG_GETARG_SELECTION(4);
    uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    uint32   	mask = 0;
    int         i;
	Datatype1	arg1;
	Datatype2	arg2;
    int64 		result;

    if(likely(pselection == NULL))
	{
		for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				arg1 = (Datatype1)parg1[i];
				arg2 = (Datatype2)parg2[i];

				result = arg1 * arg2;
				mask |= (arg1 != (int64) ((int32) arg1) || arg2 != (int64) ((int32) arg2))
						&& (arg2 != 0 &&
							(result / arg2 != arg1 || (arg2 == -1 && arg1 < 0 && result < 0)));
				presult[i] = result;
				SET_NOTNULL(pflagsRes[i]);
			}
			else
				SET_NULL(pflagsRes[i]);
		}
	}
	else
	{
		for (i = 0; i < nvalues; i++)
		{
			if(pselection[i])
			{
				if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
				{
					arg1 = (Datatype1)parg1[i];
					arg2 = (Datatype2)parg2[i];

					result = arg1 * arg2;
					mask |= (unsigned int)((arg1 != (int64) ((int32) arg1) || arg2 != (int64) ((int32) arg2))
							&& (arg2 != 0 &&
								(result / arg2 != arg1 || (arg2 == -1 && arg1 < 0 && result < 0))));
					presult[i] = result;
					SET_NOTNULL(pflagsRes[i]);
				}
				else
					SET_NULL(pflagsRes[i]);
			}
		}
	}

    if (mask != 0)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("bigint out of range")));

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT8OID;
    return PG_GETARG_VECTOR(3);
}

template <typename Datatype1,typename Datatype2>
ScalarVector*
vint8mi(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32		   nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool*		pselection = PG_GETARG_SELECTION(4);
    uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    uint32		mask = 0;
    int         i;
	Datatype1	arg1;
	Datatype2	arg2;
    int64 		result;

    if(likely(pselection == NULL))
   	{
   		for (i = 0; i < nvalues; i++)
   		{
   			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
   			{
   				arg1 = (Datatype1)parg1[i];
   				arg2 = (Datatype2)parg2[i];

   				result = arg1 - arg2;
   				mask |= !SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
   				presult[i] = result;
				SET_NOTNULL(pflagsRes[i]);
   			}
   			else
   				SET_NULL(pflagsRes[i]);
   		}
   	}
   	else
   	{
   		for (i = 0; i < nvalues; i++)
   		{
   			if(pselection[i])
   			{
   				if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
   				{
   					arg1 = (Datatype1)parg1[i];
   					arg2 = (Datatype2)parg2[i];

   					result = arg1 - arg2;
   					mask |= (unsigned int)(!SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1));
   					presult[i] = result;
					SET_NOTNULL(pflagsRes[i]);
   				}
   				else
   					SET_NULL(pflagsRes[i]);
   			}
   		}
   	}

    if (mask != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("bigint out of range")));
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT8OID;
    return PG_GETARG_VECTOR(3);
}

template <typename Datatype1,typename Datatype2>
ScalarVector*
vint8pl(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32		   nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool*		pselection = PG_GETARG_SELECTION(4);
    uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    uint32    	 mask = 0;
    int          i;
	Datatype1	arg1;
	Datatype2	arg2;
    int64 		result;

	if(likely(pselection == NULL))
	{
		for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				arg1 = (Datatype1)parg1[i];
				arg2 = (Datatype2)parg2[i];

				result = arg1 + arg2;
				mask |= SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
				presult[i] = result;
				SET_NOTNULL(pflagsRes[i]);
			}
			else
				SET_NULL(pflagsRes[i]);
		}
	}
	else
	{
		for (i = 0; i < nvalues; i++)
		{
			if(pselection[i])
			{
				if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
				{
					arg1 = (Datatype1)parg1[i];
					arg2 = (Datatype2)parg2[i];

					result = arg1 + arg2;
					mask |= (unsigned int)(SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1));
					presult[i] = result;
					SET_NOTNULL(pflagsRes[i]);
				}
				else
					SET_NULL(pflagsRes[i]);
			}
		}
	}


    if (mask != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("bigint out of range")));
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT8OID;
    return PG_GETARG_VECTOR(3);
}

/*
* @Description: calculate agg funtion stddev_sam's trans value for int2/int4/int8/numeric
* @in type_size -	for diffent data type
* @in isTransition - is the trans value
* @return - ScalarVector
*/
template<int type_size, bool isTransition>
ScalarVector*
vint_stddev_samp(PG_FUNCTION_ARGS)
{

	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);
	hashCell*	  cell = NULL;
	int			  i;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;
	Datum 		  args[2];
	Datum		  result[2];
	Datum		  bi_result;
	Datum		*datumarray = NULL;
	int			ndatum;
	Datum		newval;
	FunctionCallInfoData finfo;

	bictl		  ctl;
	Numeric		  leftarg, rightarg;	/* left-hand and right-hand operand of addition */
	uint16		  num1Flags, num2Flags;	/* numeric flags of num1 and num2  */
	int			  arg1, arg2;

	finfo.arg = &args[0];
	ctl.context = context;

	for (i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if (cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if (IS_NULL(cell->m_val[idx].flag))
			{
				if (isTransition)
				{
					if (type_size == 4)
					{
						newval = DirectFunctionCall1(int4_numeric_bi, pVal[i]);
					}
					else if (type_size == 8)
					{
						newval = DirectFunctionCall1(int8_numeric_bi, pVal[i]);
					}
					else if (type_size == 2)
					{
						newval = DirectFunctionCall1(int2_numeric_bi, pVal[i]);
					}
					else
					{
						newval = pVal[i];
					}

					leftarg = DatumGetBINumeric(newval);

					cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));
					cell->m_val[idx + 1].val = 1; /* first time set count to 1 */
					cell->m_val[idx + 2].val = addVariable(context, DirectFunctionCall2(numeric_mul, newval, newval));
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);

					cell->m_val[idx].val = addVariable(context, datumarray[1]);
					cell->m_val[idx + 1].val = addVariable(context, datumarray[0]);	
					cell->m_val[idx + 2].val = addVariable(context, datumarray[2]);
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				SET_NOTNULL(cell->m_val[idx + 2].flag);
			}
			else
			{
				if (isTransition)
				{
					args[0] = cell->m_val[idx].val;

					if (type_size == 4)
					{
						newval = DirectFunctionCall1(int4_numeric_bi, pVal[i]);
					}
					else if (type_size == 2)
					{
						newval = DirectFunctionCall1(int2_numeric_bi, pVal[i]);
					}
					else if (type_size == 8)
					{
						newval = DirectFunctionCall1(int8_numeric_bi, pVal[i]);
					}
					else
					{
						newval = pVal[i];
					}

					args[1] = newval;

					leftarg =  (Numeric)(args[0]);
					rightarg = DatumGetBINumeric(newval);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);

					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx].val;
						/* call big integer fast add function */
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						/* ctl.store_pos may be pointed to new address. */
						cell->m_val[idx].val = ctl.store_pos;
					} 
					else /* call numeric_add */
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						
						result[0] = DirectFunctionCall2(numeric_add, args[0], args[1]);
						cell->m_val[idx].val = addVariable(context, result[0]);
					}

					leftarg =  (Numeric)(cell->m_val[idx + 2].val);
					rightarg = DatumGetBINumeric(DirectFunctionCall2(numeric_mul, newval, newval));
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					
					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx + 2].val;
						/* call big integer fast add function */
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						/* ctl.store_pos may be pointed to new address. */
						cell->m_val[idx + 2].val = ctl.store_pos;
					} 
					else /* call numeric_add */
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						
						result[1] = DirectFunctionCall2(numeric_add, cell->m_val[idx + 2].val, DirectFunctionCall2(numeric_mul, newval, newval));
						cell->m_val[idx + 2].val = addVariable(context, result[1]);
					}

					cell->m_val[idx + 1].val++;
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);

					/* calculate sum: sum = sum + num */
					leftarg = (Numeric)(cell->m_val[idx].val);
					rightarg = DatumGetBINumeric(datumarray[1]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx].val;
						/* call big integer fast add function */
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						/* ctl.store_pos may be pointed to new address. */
						cell->m_val[idx].val = ctl.store_pos;
					} 
					else  /* call numeric_add */
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						bi_result = numeric_add(&finfo);
						cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, bi_result);
					}

					leftarg = (Numeric)(cell->m_val[idx + 2].val);
					rightarg = DatumGetBINumeric(datumarray[2]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx + 2].val;
						/* call big integer fast add function */
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						/* ctl.store_pos may be pointed to new address. */
						cell->m_val[idx + 2].val = ctl.store_pos;
					} 
					else  /* call numeric_add */
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						bi_result = numeric_add(&finfo);
						cell->m_val[idx + 2].val = replaceVariable(context, cell->m_val[idx + 2].val, bi_result);
					}
					
					/* 
					 * calculate count: count = oldcount + count 
					 * the count num can be stored by int64, call bi64add64 directly.
					 */
					ctl.store_pos = cell->m_val[idx + 1].val;
					BiAggFunMatrix[BI_AGG_ADD][0][0]((Numeric)(ctl.store_pos), (Numeric)(datumarray[0]), &ctl);
				}
			}
		}
	}

	return NULL;
}


/*
* @Description: calculate agg funtion stddev_sam's trans value for int2/int4/int8/numeric
* @in isTransition - is the trans value
* @in singlenode - only in datanode
* @return - ScalarVector
*/
template<bool isTransition, bool singlenode>
Datum
vnumeric_stddev_samp_final(PG_FUNCTION_ARGS)
{
	hashCell*	cell = (hashCell*)PG_GETARG_DATUM(0);
	int			idx  = PG_GETARG_DATUM(1);
	ScalarValue*	m_vals = (ScalarValue*)PG_GETARG_DATUM(2);
	uint8  *m_flag = (uint8*)PG_GETARG_DATUM(3);
	
	Datum	  args;
	Datum	  arg[3];
	FunctionCallInfoData finfo;

	finfo.arg = &args;
	Numeric tmp_arg;
	
	if (singlenode)
	{
		if (IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}

		if (isTransition)
		{
			tmp_arg = DatumGetNumeric(DirectFunctionCall1(int8_numeric, cell->m_val[idx + 1].val));
		}
		else
		{
			tmp_arg = bitonumeric(cell->m_val[idx + 1].val);
		}

		arg[0] = NumericGetDatum(tmp_arg);
		arg[1] = NumericGetDatum(bitonumeric(cell->m_val[idx].val));
		arg[2] = NumericGetDatum(bitonumeric(cell->m_val[idx + 2].val));
		
		args = PointerGetDatum(construct_array(arg, 3, NUMERICOID, -1, false, 'i'));

		finfo.isnull = false;
		*m_vals = numeric_stddev_samp(&finfo);
		if (finfo.isnull)
		{
			SET_NULL(*m_flag);
		}
		else
		{
			SET_NOTNULL(*m_flag);
		}
	}
	else
	{
		if (IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}

		arg[1] = cell->m_val[idx].val;
		arg[2] = cell->m_val[idx + 2].val;

		if (isTransition)
		{
			arg[0] = makeNumeric64(cell->m_val[idx + 1].val, 0);
		}
		else
		{
			arg[0] = cell->m_val[idx + 1].val;
		}

		*m_vals = PointerGetDatum(construct_array(arg, 3, NUMERICOID, -1, false, 'i'));
		SET_NOTNULL(*m_flag);
	}

	return (Datum) 0;
}

#endif
