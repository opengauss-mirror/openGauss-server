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
 * int4.inl
 *	  template implementation of 32-bit integers.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/vecexecutor/vecprimitive/int4.inl
 *
 *-------------------------------------------------------------------------
 */

#ifndef INT4_INL
#define INT4_INL

#include "catalog/pg_type.h"
#include <ctype.h>
#include <limits.h>
#include "vecexecutor/vechashtable.h"
#include "utils/array.h"
#include "utils/biginteger.h"
#include "vectorsonic/vsonichashagg.h"

#define SAMESIGN(a,b)	(((a) < 0) == ((b) < 0))

typedef struct vecInt8TransTypeData
{
	int64		count;
	int64		sum;
} VecInt8TransTypeData;

template <SimpleOp sop,typename Datatype>
ScalarVector*
vint_sop(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	ScalarValue* parg2 = PG_GETARG_VECVAL(1);
	int32		 nvalues = PG_GETARG_INT32(2);
	ScalarValue* presult = PG_GETARG_VECVAL(3);
	uint8*	pflag = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	bool*		pselection = PG_GETARG_SELECTION(4);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
	int          i;


    if(likely(pselection == NULL))
    {
    	for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				presult[i] = eval_simple_op<sop, Datatype>((Datatype)parg1[i], (Datatype)parg2[i]);
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
					presult[i] = eval_simple_op<sop, Datatype>((Datatype)parg1[i], (Datatype)parg2[i]);
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

/* 
 * vint_avg_final: For each level of avg, a final operation is needed.
 *
 * singlenode: is the last stage of avg. If so, output is numeric type, or array type.
 */
template<bool singlenode>
Datum
vint_avg_final(PG_FUNCTION_ARGS)
{
	hashCell* cell = (hashCell*)PG_GETARG_DATUM(0);
	int		  idx  = PG_GETARG_DATUM(1);
	ScalarValue*  pvals = (ScalarValue*)PG_GETARG_DATUM(2);
	uint8  *pflag = (uint8*)PG_GETARG_DATUM(3);
	Datum	  args[2];
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];
	if(singlenode)
	{
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}

		/* call int8_numeric_bi and numeric_div, the avg result can be big integer type */
		args[0] = DirectFunctionCall1(int8_numeric_bi, cell->m_val[idx].val);
		args[1] = DirectFunctionCall1(int8_numeric_bi, cell->m_val[idx+1].val);

		*pvals = numeric_div(&finfo);
		SET_NOTNULL(*pflag);
	}
	else
	{
		ArrayType  *transarray = NULL;
		VecInt8TransTypeData *transdata = NULL;
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}

		args[0] = cell->m_val[idx].val;
		args[1] = cell->m_val[idx+1].val;
		
		transarray = construct_array(args, 2, INT8OID, sizeof(int64), true, 'd');

		transdata = (VecInt8TransTypeData *) ARR_DATA_PTR(transarray);
		transdata->count = args[1];
		transdata->sum = args[0];
		*pvals = PointerGetDatum(transarray);
		SET_NOTNULL(*pflag);
	}
	
	return (Datum) 0;
}

/*
 * @Description	: avg(int)'s final function, support int4 and int2.
 *
 * @in m_data[idx]	: value of sum(int) for each group.
 * @in m_data[idx + 1]: number of sum(int) for each group.
 * @out pVal		: avg(int)'s result for nrows-th row. 
 */
template<bool singlenode>
Datum
vsint_avg_final(PG_FUNCTION_ARGS)
{
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(0);
	int idx = (int)PG_GETARG_DATUM(1);
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(2);
	int dataIdx = (int)PG_GETARG_DATUM(3);
	
	SonicDatumArray* data = sdata[idx];
	SonicDatumArray* count = sdata[idx + 1];
	
	int		nrows = pVector->m_rows;
	uint8*	pflag = &pVector->m_flag[nrows];
	ScalarValue* pvals = &pVector->m_vals[nrows];
	Datum	args[2];
	Datum	*leftdata = NULL;
	Datum	*countdata = NULL;
	uint8	leftflag;
	FunctionCallInfoData finfo;
	int		arrIndx, atomIndx;

	finfo.arg = &args[0];
	nrows = nrows + 1;

	/* the same index are used for data and m_count */
	arrIndx = getArrayIndx(dataIdx, data->m_nbit);
	atomIndx = getArrayLoc(dataIdx, data->m_atomSize - 1);

	/* get sum (leftdata) amd count (countdata) */
	leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];
	leftflag = data->getNthNullFlag(arrIndx, atomIndx);
	countdata = &((Datum *)count->m_arr[arrIndx]->data)[atomIndx];

	if (singlenode)
	{
		if (IS_NULL(leftflag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}
		
		/* call int8_numeric_bi and numeric_div, the avg result can be big integer type */
		args[0] = DirectFunctionCall1(int8_numeric_bi, *leftdata);
		args[1] = DirectFunctionCall1(int8_numeric_bi, *countdata);

		*pvals = numeric_div(&finfo);
		SET_NOTNULL(*pflag);
	}
	else
	{
		ArrayType  *transarray = NULL;
		VecInt8TransTypeData *transdata = NULL;
		if (IS_NULL(leftflag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}

		args[0] = *leftdata;
		args[1] = *countdata;
	
		transarray = construct_array(args, 2, INT8OID, sizeof(int64), true, 'd');
		transdata = (VecInt8TransTypeData *) ARR_DATA_PTR(transarray);
		transdata->count = args[1];
		transdata->sum = args[0];
		*pvals = PointerGetDatum(transarray);
		SET_NOTNULL(*pflag);
	}

	return (Datum) 0;
}

/* 
 * vint_avg: For each level of avg, a final operation is needed.
 *
 * isTransition: is the first stage of avg. If so, input is simple type, or array type.
 */
template<int int_size, bool isTransition>
ScalarVector*
vint_avg(PG_FUNCTION_ARGS)
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
	FunctionCallInfoData finfo;
	ArrayType  *transarray = NULL;
	VecInt8TransTypeData *transdata = NULL;
	
	finfo.arg = &args[0];

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(IS_NULL(cell->m_val[idx].flag))
			{
				if(isTransition)
				{
					if(int_size == 4)
						cell->m_val[idx].val = (int64)DatumGetInt32(pVal[i]);
					else if(int_size == 2)
						cell->m_val[idx].val = (int64)DatumGetInt16(pVal[i]);
					else if(int_size == 1)
						cell->m_val[idx].val = (int64)DatumGetUInt8(pVal[i]);

					cell->m_val[idx + 1].val = 1; //count set 1
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transdata = (VecInt8TransTypeData *) ARR_DATA_PTR(transarray);
					cell->m_val[idx].val = transdata->sum;
					cell->m_val[idx + 1].val = transdata->count;
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				SET_NULL(cell->m_val[idx + 2].flag);
			}
			else
			{
				
				if(isTransition)
				{
					args[0] = cell->m_val[idx].val;
					args[1] = pVal[i];

					if(int_size == 4)
						result = int84pl(&finfo);
					else if(int_size == 2)
						result = int82pl(&finfo);
					else if(int_size == 1)
					{
						args[1] = Int64GetDatum((int64)DatumGetUInt8(pVal[i]));
						result = int8pl(&finfo);
					}
					
					cell->m_val[idx].val = result;
					cell->m_val[idx + 1].val++; //count++
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transdata = (VecInt8TransTypeData *) ARR_DATA_PTR(transarray);

					cell->m_val[idx].val = DirectFunctionCall2(int8pl,cell->m_val[idx].val, Int64GetDatum(transdata->sum));
					cell->m_val[idx+1].val += transdata->count;
				}
			}
		}
	}

	return NULL;
}

/*
 * @Description	: avg(int)'s transition function, support int2 and int4.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.   
 * @out m_data[idx]	: value of sum(int) for each group.
 * @out m_data[idx + 1]	: number of sum(int) for each group.
 */
template<int int_size, bool isTransition>
ScalarVector*
vsint_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int idx = (int)PG_GETARG_DATUM(1);
	uint32*	  loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicDatumArray* data = sdata[idx];
	SonicDatumArray* count = sdata[idx + 1];
	
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		flag = pVector->m_flag;
	int			nrows = pVector->m_rows;
	int			arrIndx, atomIndx;
	
	Datum		args[2];
	Datum		*leftdata = NULL;
	Datum		*countdata = NULL;
	uint8		leftflag;
	int			i;
	
	ArrayType  *transarray = NULL;
	VecInt8TransTypeData *transdata = NULL;
	FunctionCallInfoData finfo;
	
	finfo.arg = &args[0];

	for (i = 0; i < nrows; i++)
	{
		if (loc[i] != 0 && NOT_NULL(flag[i]))
		{
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

			/* previous sum result(leftdata) */
			leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);

			/* previous count result(countdata) */
			countdata = &((Datum *)count->m_arr[arrIndx]->data)[atomIndx];

			if (IS_NULL(leftflag))
			{
				/* store the first data pVal[i] in data and count */
				if (isTransition)
				{
					/* convert to int64 and then convert to uint64 */
					if (int_size == 4)
						*leftdata = (int64)DatumGetInt32(pVal[i]);
					else if (int_size == 2)
						*leftdata = (int64)DatumGetInt16(pVal[i]);
					else if (int_size == 1)
						*leftdata = (int64)DatumGetUInt8(pVal[i]);
					data->setNthNullFlag(arrIndx, atomIndx, false);

					*countdata = 1;
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transdata = (VecInt8TransTypeData *) ARR_DATA_PTR(transarray);

					*leftdata = transdata->sum;
					data->setNthNullFlag(arrIndx, atomIndx, false);

					*countdata = transdata->count;
				}
			}
			else
			{
				/* updata previous sum result based on the given pVal[i] */
				if (isTransition)
				{
					args[0] = *leftdata;
					args[1] = pVal[i];

					if (int_size == 4)
						*leftdata  = int84pl(&finfo);
					else if (int_size == 2)
						*leftdata  = int82pl(&finfo);
					else if (int_size == 1)
					{
						args[1] = Int64GetDatum((int64)DatumGetUInt8(pVal[i]));
						*leftdata  = int8pl(&finfo);
					}
					(*countdata)++;
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transdata = (VecInt8TransTypeData *)ARR_DATA_PTR(transarray);

					*leftdata = DirectFunctionCall2(int8pl, *leftdata, Int64GetDatum(transdata->sum));
					*countdata += transdata->count;
				}
			}
		}
	}
	return NULL;
}

template<bool isInt32, bool isTransition>
ScalarVector*
vint_sum(PG_FUNCTION_ARGS)
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
				if(isTransition)
				{
					if(isInt32)
						cell->m_val[idx].val = (int64)DatumGetInt32(pVal[i]);
					else
						cell->m_val[idx].val = (int64)DatumGetInt16(pVal[i]);
				}
				else
					cell->m_val[idx].val = pVal[i];
				
				SET_NOTNULL(cell->m_val[idx].flag);
			}
			else
			{
				args[0] = cell->m_val[idx].val;
				args[1] = pVal[i];
				if(isTransition)
				{
					if(isInt32)
						result = DirectFunctionCall2(int84pl,args[0],args[1]);
					else
						result = DirectFunctionCall2(int82pl,args[0],args[1]);
				}
				else
					result = DirectFunctionCall2(int8pl,args[0],args[1]);
				
				cell->m_val[idx].val = result;
			}
		}
	}
	return NULL;
}

/*
 * @Description	: sum(int)'s agg function, support int2 and int4.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.   
 * @out m_data[idx]	: value of sum(int) for each group.
 */
template<bool isInt32, bool isTransition>
ScalarVector*
vsint_sum(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int idx = (int)PG_GETARG_DATUM(1);
	uint32*	  loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicDatumArray* data = sdata[idx];

	ScalarValue*	pVal = pVector->m_vals;
	uint8*		flag = pVector->m_flag;
	int			nrows = pVector->m_rows;
	Datum		args[2];

	Datum		*leftdata = NULL;
	uint8		leftflag;
	int			i;
	int			arrIndx, atomIndx;

	for(i = 0 ; i < nrows; i++)
	{
		if (loc[i] != 0 && NOT_NULL(flag[i]))
		{
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

			/* get sum (leftdata) */
			leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);

			if (IS_NULL(leftflag))
			{
				/* store the first data pVal[i] in data */
				if (isTransition)
				{
					if (isInt32)
						*leftdata = DatumGetInt32(pVal[i]);
					else
						*leftdata = DatumGetInt16(pVal[i]);
				}
				else
					*leftdata = pVal[i];    

				data->setNthNullFlag(arrIndx, atomIndx, false);
			}
			else
			{
				/* updata previous sum result based on the given pVal[i] */
				args[0] = *leftdata;
				args[1] = pVal[i];
				if (isTransition)
				{
					if (isInt32)
						*leftdata = DirectFunctionCall2(int84pl, args[0], args[1]);
					else
						*leftdata = DirectFunctionCall2(int82pl, args[0], args[1]);
				}
				else
					*leftdata = DirectFunctionCall2(int8pl, args[0], args[1]);
			}
		}
	}
	return NULL;
}
	
template <PGFunction timetzFun>
ScalarVector*
vtimetz_min_max(PG_FUNCTION_ARGS)
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
	Datum		  result;
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(IS_NULL(cell->m_val[idx].flag))
			{
				cell->m_val[idx].val = addVariable(context, pVal[i]);
				SET_NOTNULL(cell->m_val[idx].flag);
			}
			else
			{
				args[0] = PointerGetDatum((char*)ScalarVector::Decode(cell->m_val[idx].val) + VARHDRSZ_SHORT);
				args[1] = PointerGetDatum((char*)ScalarVector::Decode(pVal[i]) + VARHDRSZ_SHORT);
				result = timetzFun(&finfo);
				if(result != args[0])
				{
					cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, pVal[i]);
				}
			}
		}
	}

	return NULL;
}

/* 
 * vinterval_avg: For each level of avg, a final operation is needed.
 *
 * isTransition: is the first stage of avg. If so, input is interval type, or array type.
 */
template<bool isTransition>
ScalarVector*
vinterval_avg(PG_FUNCTION_ARGS)
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
	Datum		  result;
	FunctionCallInfoData finfo;
	Datum		  sum;
	Datum			*transdatums = NULL;
	int				ndatums;
	Interval		*N = NULL;
	
	finfo.arg = &args[0];

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(IS_NULL(cell->m_val[idx].flag))
			{
				if(isTransition)
				{
					cell->m_val[idx].val = addVariable(context, pVal[i]);
					cell->m_val[idx + 1].val = 1; //count set 1
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
									  INTERVALOID, sizeof(Interval), false, 'd',
									  &transdatums, NULL, &ndatums);
					
					N = DatumGetIntervalP(transdatums[1]);
					
					cell->m_val[idx].val = addVariable(context, ScalarVector::DatumToScalar(transdatums[0],INTERVALOID,false));

					cell->m_val[idx + 1].val = N->time;
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				SET_NULL(cell->m_val[idx + 2].flag);
			}
			else
			{
				args[0] = PointerGetDatum((char*)cell->m_val[idx].val + VARHDRSZ_SHORT);
				args[1] = PointerGetDatum((char*)pVal[i] + VARHDRSZ_SHORT);
				
				if(isTransition)
				{
					result = interval_pl(&finfo);
					ScalarValue     val = ScalarVector::DatumToScalar(result,INTERVALOID,false);
					cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, val);
					cell->m_val[idx + 1].val++; //count++
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  INTERVALOID, sizeof(Interval), false, 'd',
										  &transdatums, NULL, &ndatums);
					
					N = DatumGetIntervalP(transdatums[1]);

					sum = DirectFunctionCall2(interval_pl,  transdatums[0], args[0]);
					cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, ScalarVector::DatumToScalar(sum,INTERVALOID,false));

					cell->m_val[idx + 1].val += N->time;
				}
			}
		}
	}
	return NULL;
}

/* 
 * vinterval_avg_final: For each level of avg, a final operation is needed.
 *
 * singlenode: is the last stage of avg. If so, output is interval type, or array type.
 */
template<bool singlenode>
Datum
vinterval_avg_final(PG_FUNCTION_ARGS)
{
	hashCell* cell = (hashCell*)PG_GETARG_DATUM(0);
	int		  idx  = PG_GETARG_DATUM(1);
	ScalarValue*  m_vals = (ScalarValue*)PG_GETARG_DATUM(2);
	uint8  *m_flag = (uint8*)PG_GETARG_DATUM(3);
	
	Datum	  args[2];
	FunctionCallInfoData finfo;
	Datum	   transdatums[2];
	Interval	N;

	finfo.arg = &args[0];
	if(singlenode)
	{
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}
		
		args[0] = PointerGetDatum((char*)cell->m_val[idx].val + VARHDRSZ_SHORT);
		args[1] = DirectFunctionCall1(i8tod,cell->m_val[idx+1].val);
		
		*m_vals = ScalarVector::DatumToScalar(interval_div(&finfo),INTERVALOID,false);
		SET_NOTNULL(*m_flag);
	}
	else
	{
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}

		args[0] = cell->m_val[idx].val;
		args[1] = cell->m_val[idx+1].val;
		
		N.time = args[1];
		
		transdatums[0] = PointerGetDatum((char*)args[0] + VARHDRSZ_SHORT);
		transdatums[1] = IntervalPGetDatum(&N);

		*m_vals = PointerGetDatum(construct_array(transdatums, 2, INTERVALOID, sizeof(Interval), false, 'd'));
		SET_NOTNULL(*m_flag);
	}
	
	return (Datum) 0;
}

template <PGFunction ctidFun>
ScalarVector*
vctid_sop(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	ScalarValue* parg2 = PG_GETARG_VECVAL(1);
	int32		 nvalues = PG_GETARG_INT32(2);
	ScalarValue* presult = PG_GETARG_VECVAL(3);
	uint8*	pflag = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	bool*		pselection = PG_GETARG_SELECTION(4);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
	Oid     typeId1 = PG_GETARG_VECTOR(0)->m_desc.typeId;
	Oid     typeId2 = PG_GETARG_VECTOR(1)->m_desc.typeId;
	int          i;
	Datum 		  args[2];
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];

	if(likely(pselection == NULL))
	{
		for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				args[0] = typeId1 == TIDOID?parg1[i]:PointerGetDatum(parg1+i);
				args[1] = typeId2 == TIDOID?parg2[i]:PointerGetDatum(parg2+i);

				presult[i] = ctidFun(&finfo);
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
					args[0] = typeId1 == TIDOID?parg1[i]:PointerGetDatum(parg1+i);
					args[1] = typeId2 == TIDOID?parg2[i]:PointerGetDatum(parg2+i);

					presult[i] = ctidFun(&finfo);
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

#endif
