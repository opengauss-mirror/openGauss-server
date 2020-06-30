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
 * numeric.inl
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/vecexecutor/vecprimitive/numeric.inl
 *
 * -------------------------------------------------------------------------
 */

#ifndef NUMERIC_INL_
#define NUMERIC_INL_

#include <math.h>
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/biginteger.h"
#include "utils/numeric.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vechashagg.h"
#include "vectorsonic/vsonichashagg.h"
#include "vectorsonic/vsonicarray.h"

/* 
 * vnumeric_avg_final: For each level of avg, a final operation is needed.
 *
 * isTransition: is the first stage of avg. If so, input is int8 type, or array type.
 * singlenode: is the last stage of avg. If so, output is numeric type, or array type.
 */
template<bool isTransition, bool singlenode>
Datum
vnumeric_avg_final(PG_FUNCTION_ARGS)
{
	hashCell* cell = (hashCell*)PG_GETARG_DATUM(0);
	int		  idx  = PG_GETARG_DATUM(1);
	ScalarValue*  m_vals = (ScalarValue*)PG_GETARG_DATUM(2);
	uint8  *m_flag = (uint8*)PG_GETARG_DATUM(3);
	
	Datum	  args[2];
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];
	if(singlenode)
	{
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}
		
		args[0] = cell->m_val[idx].val;
		if (isTransition)
			args[1] = makeNumeric64(cell->m_val[idx + 1].val, 0);
		else
			args[1] = cell->m_val[idx+1].val;
		
		*m_vals = numeric_div(&finfo);
		SET_NOTNULL(*m_flag);
	}
	else
	{
		if(IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}

		args[1] = cell->m_val[idx].val;
		if (isTransition)
			args[0] = makeNumeric64(cell->m_val[idx + 1].val, 0);
		else
			args[0] = cell->m_val[idx+1].val;
		*m_vals = PointerGetDatum(construct_array(args, 2, NUMERICOID, -1, false, 'i'));
		SET_NOTNULL(*m_flag);
	}

	return (Datum) 0;
}

template<bool isTransition>
ScalarVector*
vnumeric_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);

	MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);

	hashCell*	  cell = NULL;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;
	Datum	      *datumarray = NULL;
	int			   ndatum;
	bictl		  ctl;
	Numeric		  leftarg, rightarg;	// left-hand and right-hand operand of addition	
	uint16		  num1Flags, num2Flags;		// numeric flags of num1 and num2 
	int			  arg1, arg2, i;

	finfo.arg = &args[0];
	ctl.context = context;

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && NOT_NULL(flag[i])) //only do when not null
		{
			if(NOT_NULL(cell->m_val[idx].flag))
			{
				if(isTransition)
				{
					leftarg =  (Numeric)(cell->m_val[idx].val);
					rightarg = DatumGetBINumeric(pVal[i]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
				
					if(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx].val;
						// call big integer fast add function
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						// ctl.store_pos may be pointed to new address.
						cell->m_val[idx].val = ctl.store_pos;
					} 
					else // call numeric_add
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						result = numeric_add(&finfo);
						cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, result);
					}
					cell->m_val[idx + 1].val++; //count++
				}
				else
				{
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);
					// calculate sum: sum = sum + num
					leftarg = (Numeric)(cell->m_val[idx].val);
					rightarg = DatumGetBINumeric(datumarray[1]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					if(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = cell->m_val[idx].val;
						// call big integer fast add function
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						// ctl.store_pos may be pointed to new address.
						cell->m_val[idx].val = ctl.store_pos;
					} 
					else // call numeric_add
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						result = numeric_add(&finfo);
						cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, result);
					}
					
					// calculate count: count = oldcount + count
					// the count num can be stored by int64, call bi64add64 directly.
					ctl.store_pos = cell->m_val[idx + 1].val;
					BiAggFunMatrix[BI_AGG_ADD][0][0]((Numeric)(ctl.store_pos), (Numeric)(datumarray[0]), &ctl);
				}
			}
			else
			{
				if(isTransition)
				{
					leftarg = DatumGetBINumeric(pVal[i]);
					cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));
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
 * @Description	: avg(numeric)'s final function.
 *
 * @in m_data[idx]	 : value of sum(numeric) for each group.
 * @in m_data[idx + 1]: number of sum(numeric) for each group.
 * @out pVal		: avg(numeric)'s result for nrows-th row. 
 */
template<bool isTransition, bool singlenode>
Datum
vsnumeric_avg_final(PG_FUNCTION_ARGS)
{
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(0);
	int				idx = (int)PG_GETARG_DATUM(1);
	ScalarVector*	pVector = (ScalarVector*)PG_GETARG_DATUM(2);
	uint32			dataIdx = (uint32)PG_GETARG_DATUM(3);
	
	SonicEncodingDatumArray*	data = (SonicEncodingDatumArray*)sdata[idx];
	SonicDatumArray*		scount = sdata[idx + 1];
	int 			nrows = pVector->m_rows;
	uint8*			pflag = &pVector->m_flag[nrows];
	ScalarValue*	pvals = &pVector->m_vals[nrows];
	Datum			args[2];
	Datum			*countdata = NULL;
	Datum			*leftdata = NULL;
	
	uint8			leftflag;
	int				arrIndx,atomIndx;

	FunctionCallInfoData finfo;
	finfo.arg = &args[0];
	nrows = nrows + 1;

	arrIndx = getArrayIndx(dataIdx, data->m_nbit);
	atomIndx = getArrayLoc(dataIdx, data->m_atomSize - 1);
	
	/* sum result(leftdata) */
	leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];
	leftflag = data->getNthNullFlag(arrIndx, atomIndx);
	
	/* count result(leftdata) */
	countdata = &((Datum *)scount->m_arr[arrIndx]->data)[atomIndx];
			
	if (singlenode)
	{
		if (IS_NULL(leftflag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}

		args[0] = *leftdata;
		args[1] = makeNumeric64(*countdata, 0);

		/* avg result(m_vals) */
		*pvals = numeric_div(&finfo);
		SET_NOTNULL(*pflag);
	}
	else
	{
		if (IS_NULL(leftflag))
		{
			SET_NULL(*pflag);
			return (Datum) 0;
		}

		args[1] = *leftdata;
		args[0] = makeNumeric64(*countdata, 0);
		
		*pvals = PointerGetDatum(construct_array(args, 2, NUMERICOID, -1, false, 'i'));
		SET_NOTNULL(*pflag);
	}
	return (Datum) 0;
}

/*
 * @Description	: avg(numeric)'s transition function.
 *
 * @in m_loc	: location in hash table.
 * @in pVal	: vector to be calculated.
 * @out m_data[idx]		: value of sum(numeric) for each group.
 * @out m_data[idx + 1]	: number of sum(numeric) for each group.
 */
template<bool isTransition>
ScalarVector*
vsnumeric_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			idx = (int)PG_GETARG_DATUM(1);
	uint32*		loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);

	SonicEncodingDatumArray* data = (SonicEncodingDatumArray*)sdata[idx];
	SonicDatumArray* scount = sdata[idx + 1];
	ScalarValue*	pVal = pVector->m_vals;
	uint8*			flag = pVector->m_flag;
	int				nrows = pVector->m_rows;
	Datum			args[2];
	Datum			result;
	Datum			*leftdata = NULL;
	Datum			*countdata = NULL;
	uint8			leftflag;
	FunctionCallInfoData finfo;
	bictl			ctl;
	/* left-hand and right-hand operand of addition */
	Numeric			leftarg, rightarg;	
	/* numeric flags of num1 and num2 */
	uint16			num1Flags, num2Flags;	
	int				arg1, arg2, i;
	Datum			*datumarray = NULL;
	int				ndatum;
	int				arrIndx, atomIndx;
	
	finfo.arg = &args[0];
	ctl.context = data->m_cxt;

	for(i = 0 ; i < nrows; i++)
	{
		if ((loc[i] != 0) && NOT_NULL(flag[i]))
		{
			/* previous sum result(leftdata) */
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

			/* first get data flag */
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);

			/* previous count result(leftdata) */
			countdata = &((Datum *)scount->m_arr[arrIndx]->data)[atomIndx];

			if (NOT_NULL(leftflag))
			{
				/* get data value */
				leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];

				/* updata previous sum and count result based on the given pVal[i] */
				if (isTransition)
				{
					leftarg = (Numeric)(leftdata[0]);
					rightarg = DatumGetBINumeric(pVal[i]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);

					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						ctl.store_pos = leftdata[0];
						/* call big integer fast add function */
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						leftdata[0] = ctl.store_pos;
					} 
					else
					{
						 /* call numeric_add */
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						result = numeric_add(&finfo);
						leftdata[0] = data -> replaceVariable(leftdata[0], result);
					}
					/* add count */
					countdata[0]++;
				}
				else
				{
					/* We assume the input is array of numeric */ 
					deconstruct_array(DatumGetArrayTypeP(pVal[i]),
										  NUMERICOID, -1, false, 'i',
										  &datumarray, NULL, &ndatum);
					/* calculate sum: sum = sum + num */
					leftarg = (Numeric)(leftdata[0]);
					rightarg = DatumGetBINumeric(datumarray[1]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						/* call big integer fast add function */
						ctl.store_pos = leftdata[0];
						(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
						leftdata[0] = ctl.store_pos;
					} 
					else
					{
						/* call numeric_add */
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						result = numeric_add(&finfo);
						leftdata[0] = data -> replaceVariable(leftdata[0], result);
					}
					
					/* calculate count: count = oldcount + count */
					*countdata = *countdata + NUMERIC_64VALUE((Numeric)datumarray[0]);
				}
			}
			else
			{
				/* store the first data pVal[i] in data and m_count */
				if (isTransition)
				{
					leftarg = DatumGetBINumeric(pVal[i]);
					data->setValue(NumericGetDatum(leftarg), false, arrIndx, atomIndx);
					*countdata = 1;
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


template <PGFunction numericFun, biop op>
ScalarVector*
vnumeric_op(PG_FUNCTION_ARGS)
{
	ScalarValue *parg1 = PG_GETARG_VECVAL(0);
	ScalarValue *parg2 = PG_GETARG_VECVAL(1);
	ScalarVector* pvector1 = PG_GETARG_VECTOR(0);
	ScalarVector* pvector2 = PG_GETARG_VECTOR(1);
	uint8*		 flag1 = pvector1->m_flag;
	uint8*		 flag2 = pvector2->m_flag;

	int32        nvalues = PG_GETARG_INT32(2);
	ScalarValue *presult = PG_GETARG_VECVAL(3);
	ScalarVector* presultVector = PG_GETARG_VECTOR(3);
	uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	bool        *pselection = PG_GETARG_SELECTION(4);
	int 		arg1, arg2, i;
	Numeric		leftarg, rightarg;	// left-hand and right-hand operand of addition	
	uint16		num1Flags, num2Flags;		// numeric flags of num1 and num2 
	Datum       args[2];
	FunctionCallInfoData finfo;
	finfo.arg = &args[0];

	if(likely(pselection == NULL))
	{
		for(i = 0 ; i < nvalues; i++)
		{
			if(BOTH_NOT_NULL(flag1[i], flag2[i]))
			{
				leftarg =  DatumGetBINumeric(parg1[i]);
				rightarg = DatumGetBINumeric(parg2[i]);
				num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
				num2Flags = NUMERIC_NB_FLAGBITS(rightarg);	
				
				if(likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)))
				{
					arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
					arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
					// call big integer fast calculate function
					presult[i] = (BiFunMatrix[op][arg1][arg2])(leftarg, rightarg, NULL);
				} 
				else // numeric_funcs
				{
					args[0] = NumericGetDatum(leftarg);
					args[1] = NumericGetDatum(rightarg);
					presult[i] = numericFun(&finfo);
				}
				SET_NOTNULL(pflagsRes[i]);
			}
			else
			{
				SET_NULL(pflagsRes[i]);
			}

		}
	}
	else
	{
		for(i = 0 ; i < nvalues; i++)
		{
			if(pselection[i] == true)
			{
				if(BOTH_NOT_NULL(flag1[i], flag2[i]))
				{
					leftarg =  DatumGetBINumeric(parg1[i]);
					rightarg = DatumGetBINumeric(parg2[i]);
					num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
					num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
					
					if(likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)))
					{
						arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
						arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
						// call big integer fast calculate function
						presult[i] = (BiFunMatrix[op][arg1][arg2])(leftarg, rightarg, NULL);
					} 
					else // numeric_funcs
					{
						args[0] = NumericGetDatum(leftarg);
						args[1] = NumericGetDatum(rightarg);
						presult[i] = numericFun(&finfo);
					}
					SET_NOTNULL(pflagsRes[i]);
				}
				else
				{
					SET_NULL(pflagsRes[i]);
				}
			}
		}

	}
    presultVector->m_rows = PG_GETARG_INT32(2);

    return presultVector;
}

template <PGFunction numericFun, bool get_smaller>
ScalarVector*
vnumeric_min_max(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);
	hashCell*	  cell = NULL;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;
	Numeric		  leftarg, rightarg;	// left-hand and right-hand operand of addition	
	uint16		  num1Flags, num2Flags;		// numeric flags of num1 and num2 
	int			  arg1, arg2, i;
	bictl		  ctl;
	finfo.arg = &args[0];

	ctl.context = context;

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if(cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if(IS_NULL(cell->m_val[idx].flag))
			{
				leftarg = DatumGetBINumeric(pVal[i]);
				cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));
				SET_NOTNULL(cell->m_val[idx].flag);
			}
			else
			{
				leftarg = (Numeric)(cell->m_val[idx].val);
				rightarg = DatumGetBINumeric(pVal[i]);
				num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
				num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
				// try to use big integer fast compare function
				if(likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)))
				{
					arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
					arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
					ctl.store_pos = cell->m_val[idx].val;
					// call big integer fast compare function
					if(get_smaller)
						(BiAggFunMatrix[BI_AGG_SMALLER][arg1][arg2])(leftarg, rightarg, &ctl);
					else
						(BiAggFunMatrix[BI_AGG_LARGER][arg1][arg2])(leftarg, rightarg, &ctl);
					// ctl.store_pos may be pointed to new address.
					cell->m_val[idx].val = ctl.store_pos;
				}
				else
				{
					args[0] = NumericGetDatum(leftarg);
					args[1] = NumericGetDatum(rightarg);
					result = numericFun(&finfo);
					if(result != cell->m_val[idx].val)
						cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, result);
				}
			}
		}
	}

	return NULL;
}

/*
 * @Description	: min or max(numeric)'s agg function.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.   
 * @out m_data[idx]	: value of min or max(numeric) for each group.
 */
template <PGFunction numericFun, bool get_smaller>
ScalarVector*
vsnumeric_min_max(PG_FUNCTION_ARGS)
{
	ScalarVector*	pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int				idx = (int)PG_GETARG_DATUM(1);
	uint32*			loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicEncodingDatumArray* data = (SonicEncodingDatumArray*) sdata[idx];

	ScalarValue*	pVal = pVector->m_vals;
	uint8*			flag = pVector->m_flag;
	int				nrows = pVector->m_rows;
	Datum			args[2];
	Datum			result;
	Datum			*leftdata = NULL;
	FunctionCallInfoData finfo;
	/* left-hand and right-hand operand of addition */
	Numeric			leftarg, rightarg;	
	/* numeric flags of num1 and num2 */
	uint16			num1Flags, num2Flags;
	int				arg1, arg2, i;
	bictl			ctl;
	uint8			leftflag;
	int				arrIndx, atomIndx;

	finfo.arg = &args[0];
	ctl.context = data->m_cxt;

	for (i = 0; i < nrows; i++)
	{
		/*only do when not null */
		if ((loc[i] != 0) && NOT_NULL(flag[i]))
		{
			/* previous result(leftdata) */
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

			/* check data flag */
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);

			if (IS_NULL(leftflag))
			{
				leftarg = DatumGetBINumeric(pVal[i]);
				data->setValue(NumericGetDatum(leftarg), false, arrIndx, atomIndx);
			}
			else
			{
				leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];

				leftarg = (Numeric)(leftdata[0]);
				rightarg = DatumGetBINumeric(pVal[i]);
				num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
				num2Flags = NUMERIC_NB_FLAGBITS(rightarg);
				/* try to use big integer fast compare function */
				if (likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)))
				{
					arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
					arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
					ctl.store_pos = leftdata[0];
					/* call big integer fast compare function */
					if (get_smaller)
						(BiAggFunMatrix[BI_AGG_SMALLER][arg1][arg2])(leftarg, rightarg, &ctl);
					else
						(BiAggFunMatrix[BI_AGG_LARGER][arg1][arg2])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address. */
					leftdata[0] = ctl.store_pos;
				}
				else
				{
					args[0] = NumericGetDatum(leftarg);
					args[1] = NumericGetDatum(rightarg);
					result = numericFun(&finfo);
					if(result != leftdata[0])
						leftdata[0]  = data->replaceVariable(leftdata[0], result);
				}
			}
		}
	}

	return NULL;
}

template<bool isTransition, bool allcount>
ScalarVector*
vector_count(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	hashCell*	  cell = NULL;
	int			  i;
	ScalarValue*  pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int			  nrows = pVector->m_rows;

	for(i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if (cell != NULL)
		{
			if(IS_NULL(flag[i]) == false || allcount)
			{
				if(IS_NULL(cell->m_val[idx].flag))
				{
					if(isTransition)
						cell->m_val[idx].val = 1;
					else
						cell->m_val[idx].val = pVal[i];
					
					SET_NOTNULL(cell->m_val[idx].flag);
				}
				else
				{
					if(isTransition)
						cell->m_val[idx].val++;
					else
						cell->m_val[idx].val += pVal[i];
				}
			}
			else
				SET_NOTNULL(cell->m_val[idx].flag);
		}
	}
	
	return NULL;
}

/*
 * @Description	: Calculate total numbers of input data with respect to different group.
 * @in isTransition	: Mark if need to transfer or not
 * @in allcount	: Mark count null value or not.
 * @return		: NULL, actual result has been inserted into m_data.
 */
template<bool isTransition, bool allcount>
ScalarVector*
vsonic_count(PG_FUNCTION_ARGS)
{
	ScalarVector*	pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int				idx = PG_GETARG_DATUM(1);
	uint32*			loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray**	sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicDatumArray*	data = sdata[idx];

	/* information about input scalarvector */
	ScalarValue*	pVal = pVector->m_vals;
	uint8*			flag = pVector->m_flag;
	int				nrows = pVector->m_rows;
	int				arrIdx, atomIdx;
	Datum			*aggval = NULL;

	Assert(data->m_atomTypeSize == sizeof(Datum));

	for(int i = 0 ; i < nrows; i++)
	{
		if (0 != loc[i])
		{
			if(IS_NULL(flag[i]) == false || allcount)
			{
				/* get n-th data's actual position in hash table, including arrIdx and atomIdx */
				arrIdx = getArrayIndx(loc[i], data->m_nbit);
				atomIdx = getArrayLoc(loc[i], data->m_atomSize - 1);
				aggval = &((Datum*)(data->m_arr[arrIdx])->data)[atomIdx];

				if(IS_NULL(data->getNthNullFlag(arrIdx, atomIdx)))
				{
					/* initialize count number */
					aggval[0] = isTransition ? 1 : pVal[i];
					
					data->setNthNullFlag(loc[i], false);
				}
				else
				{
					/*  count if not null in same group */
					aggval[0] = isTransition ? (aggval[0] + 1) : (aggval[0] + pVal[i]);
				}
			}
			else
				data->setNthNullFlag(loc[i], false);
		}
	}
	
	return NULL;
}

template<bool isTransition>
ScalarVector*
vint8_sum(PG_FUNCTION_ARGS)
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
				if(isTransition)
				{
					/* leftarg maybe bi64 or bi128 */
					leftarg = (Numeric)(cell->m_val[idx].val);
					/* store pVal[i](int8) to bi64 */
					rightarg = (Numeric)(makeNumeric64(pVal[i], 0));
					ctl.store_pos = cell->m_val[idx].val;
					arg1 = NUMERIC_IS_BI128(leftarg);
					/* call big integer fast add function */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][0])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					cell->m_val[idx].val = ctl.store_pos;
				}
				else
				{
					/* leftarg maybe bi64 or bi128 */
					leftarg = (Numeric)(cell->m_val[idx].val);
					/* rightarg maybe bi64 or bi128 */
					rightarg = (Numeric)(pVal[i]);
					ctl.store_pos = cell->m_val[idx].val;
					arg1 = NUMERIC_IS_BI128(leftarg);
					arg2 = NUMERIC_IS_BI128(rightarg);
					/* call big integer fast add function */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					cell->m_val[idx].val = ctl.store_pos;
				}
			}
			else
			{
				if(isTransition) //copy the first value
				{
					/* store the first value to bi64 */
					cell->m_val[idx].val = addVariable(context, makeNumeric64(pVal[i], 0));
				}
				else
				{
					/* copy the sum data to hash table */
					cell->m_val[idx].val = addVariable(context, pVal[i]);
				}
				SET_NOTNULL(cell->m_val[idx].flag);
			}
		}
	}
	return NULL;
}

/*
 * @Description	: sum(int8)'s agg function.
 *
 * @in m_loc	: location in hash table.
 * @in pVal	: vector to be calculated.
 * @out m_data[idx]		: value of sum(int8) for each group.
 */
template<bool isTransition>
ScalarVector*
vsint8_sum(PG_FUNCTION_ARGS)
{
	ScalarVector*	pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int				idx = (int)PG_GETARG_DATUM(1);
	uint32*			loc = (uint32*)PG_GETARG_DATUM(2);
	SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);
	SonicEncodingDatumArray* data = (SonicEncodingDatumArray*)sdata[idx];

	Datum			*leftdata = NULL;
	uint8			leftflag;
	Numeric			leftarg, rightarg;
	int				arg1, arg2, i;
	int				arrIndx, atomIndx;
	bictl			ctl;

	ScalarValue*	pVal = pVector->m_vals;
	uint8*			flag = pVector->m_flag;
	int				nrows = pVector->m_rows;
	ctl.context = data->m_cxt;

	for (i = 0; i < nrows; i++)
	{
		if (loc[i] != 0 && NOT_NULL(flag[i]))
		{
			/* get sum (leftdata) */
			arrIndx = getArrayIndx(loc[i], data->m_nbit);
			atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);
			/* first check flag */
			leftflag = data->getNthNullFlag(arrIndx, atomIndx);	

			if (NOT_NULL(leftflag))
			{
				/* get data value */
				leftdata = &((Datum *)data->m_arr[arrIndx]->data)[atomIndx];

				if (isTransition)
				{
					/* leftarg maybe bi64 or bi128 */
					leftarg = (Numeric)(leftdata[0]);
					/* store pVal[i](int8) to bi64 */
					rightarg = (Numeric)(makeNumeric64(pVal[i], 0));
					ctl.store_pos = leftdata[0];
					arg1 = NUMERIC_IS_BI128(leftarg);
					/* call big integer fast add function */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][0])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					leftdata[0]= ctl.store_pos;
				}
				else
				{
					/* leftarg maybe bi64 or bi128 */
					leftarg =  (Numeric)(leftdata[0]);
					/* rightarg maybe bi64 or bi128 */
					rightarg = (Numeric)(pVal[i]);
					ctl.store_pos = leftdata[0];
					arg1 = NUMERIC_IS_BI128(leftarg);
					arg2 = NUMERIC_IS_BI128(rightarg);
					/* call big integer fast add function */
					(BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
					/* ctl.store_pos may be pointed to new address */
					leftdata[0] = ctl.store_pos;
				}
			}
			else
			{
				/* copy the first value */
				if (isTransition)
				{
					/* store the first value to bi64 */
					data->setValue(makeNumeric64(pVal[i], 0), false, arrIndx, atomIndx);
				}
				else
				{
					/* copy the sum data to hash table */
					data->setValue(pVal[i], false, arrIndx, atomIndx);
				}
			}
		}
	}
	return NULL;
}

#endif /* NUMERIC_INL_ */
