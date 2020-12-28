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
 * float.inl
 *	  template implementation of float.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/vecexecutor/vecprimitive/float.inl
 *
 *-------------------------------------------------------------------------
 */

#ifndef FLOAT_INL
#define FLOAT_INL

#include "vecexecutor/vechashtable.h"
#include "utils/array.h"

template <PGFunction floatFun>
ScalarVector*
vfloat4_sop(PG_FUNCTION_ARGS)
{
	ScalarValue*	parg1 = PG_GETARG_VECVAL(0);
	ScalarValue*	parg2 = PG_GETARG_VECVAL(1);
	int32       	nvalues = PG_GETARG_INT32(2);
	ScalarValue*	presult = PG_GETARG_VECVAL(3);
	uint8*  pflag = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	bool*        	pselection = PG_GETARG_SELECTION(4);
	uint8*			pflags1 = (PG_GETARG_VECTOR(0)->m_flag);
	uint8*			pflags2 = (PG_GETARG_VECTOR(1)->m_flag);
	int            	i;

    if(likely(pselection == NULL))
    {
    	for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				presult[i] = DatumGetBool(DirectFunctionCall2(floatFun,parg1[i], parg2[i]));
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
					presult[i] = DatumGetBool(DirectFunctionCall2(floatFun,parg1[i], parg2[i]));
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
* @Description: For each level of avg/stddev_samp, a final operation is needed
* @in isTransition -  is the first stage of avg/stddev_samp. If so, input is int8 type, or array type.
* @in singlenode - is the last stage of avg/stddev_samp. If so, output is float8 type, or array type.
* @in is_samp- is avg or stddev_samp
* @return - ScalarVector
*/
template<bool isTransition, bool singlenode, bool is_samp>
Datum
vfloat_avg_final(PG_FUNCTION_ARGS)
{
	hashCell* cell = (hashCell*)PG_GETARG_DATUM(0);
	int		  idx  = PG_GETARG_DATUM(1);
	ScalarValue*  m_vals = (ScalarValue*)PG_GETARG_DATUM(2);
	uint8  *m_flag = (uint8*)PG_GETARG_DATUM(3);
	ArrayType  *transarray = NULL;
	float8	   *transvalues = NULL;

	Datum	  args[3];
	Datum arg;
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
			args[1] = DirectFunctionCall1(i8tod,cell->m_val[idx + 1].val);
		else
			args[1] = cell->m_val[idx + 1].val;

		if (is_samp)
		{
			args[2] =  cell->m_val[idx + 2].val;
		}
		else
		{
			args[2] =  DatumGetFloat8(args[0]) *  DatumGetFloat8(args[0]);
		}

		if (false == is_samp)
		{
			*m_vals = float8div(&finfo);
			SET_NOTNULL(*m_flag);
		}
		else
		{
			transarray = construct_array(args, 3, FLOAT8OID, sizeof(float8), true, 'd');
			transvalues = check_float8_array(transarray, "float8_accum", 3);

			transvalues[0] = DatumGetFloat8(args[1]);
			transvalues[1] = DatumGetFloat8(args[0]);
			transvalues[2] =  DatumGetFloat8(args[2]);

			finfo.arg = &arg;
			arg = PointerGetDatum(transarray);

			finfo.isnull = false;
			*m_vals = float8_stddev_samp(&finfo);

			if (finfo.isnull)
				SET_NULL(*m_flag);
			else
				SET_NOTNULL(*m_flag);
		}
	}
	else //construct a array type.
	{
		if (IS_NULL(cell->m_val[idx].flag))
		{
			SET_NULL(*m_flag);
			return (Datum) 0;
		}

		args[0] = cell->m_val[idx].val;
		if (isTransition)
			args[1] = DirectFunctionCall1(i8tod,cell->m_val[idx + 1].val);
		else
			args[1] = cell->m_val[idx + 1].val;

		if (is_samp)
		{
			args[2] =  cell->m_val[idx + 2].val;
		}
		else
		{
			args[2] =  DatumGetFloat8(args[0]) *  DatumGetFloat8(args[0]);
		}

		transarray = construct_array(args, 3, FLOAT8OID, sizeof(float8), true, 'd');
		transvalues = check_float8_array(transarray, "float8_accum", 3);
		
		transvalues[0] = DatumGetFloat8(args[1]);
		transvalues[1] = DatumGetFloat8(args[0]);
		transvalues[2] = DatumGetFloat8(args[2]);

		*m_vals = PointerGetDatum(transarray);
		SET_NOTNULL(*m_flag);
	}

	return (Datum) 0;
}


/*
* @Description: For each level of avg/stddev_samp, a trans operation is needed
* @in isTransition -  is the first stage of avg/stddev_samp. 
* @in is_samp- is avg or stddev_samp
* @return - ScalarVector
*/
template<bool isTransition, bool is_samp>
ScalarVector*
vfloat8_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector1 = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	hashCell*	  cell = NULL;
	int			  i;
	ScalarValue*  pVal = pVector1->m_vals;
	uint8*		  flag = pVector1->m_flag;
	int			  nrows = pVector1->m_rows;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;
	ArrayType  *transarray = NULL;
	float8	   *transvalues = NULL;

	finfo.arg = &args[0];

	for (i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if (cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if (IS_NULL(cell->m_val[idx].flag))
			{
				if (isTransition)
				{
					cell->m_val[idx].val = pVal[i];
					cell->m_val[idx + 1].val = 1; //count set 1
					if (is_samp)
					{
						cell->m_val[idx + 2].val = DirectFunctionCall2(float8mul, pVal[i], pVal[i]);
					}
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transvalues =  check_float8_array(transarray, "float8_avg", 3);
					cell->m_val[idx].val = Float8GetDatum(transvalues[1]);
					cell->m_val[idx + 1].val = Float8GetDatum(transvalues[0]);
					if (is_samp)
					{
						cell->m_val[idx + 2].val = Float8GetDatum(transvalues[2]);
					}
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				if (is_samp)
				{
					SET_NOTNULL(cell->m_val[idx + 2].flag);
				}
				else
				{
					SET_NULL(cell->m_val[idx + 2].flag);
				}
			}
			else
			{
				args[0] = cell->m_val[idx].val;
				args[1] = pVal[i];
				
				if (isTransition)
				{
					result = float8pl(&finfo);
					cell->m_val[idx].val = result;
					cell->m_val[idx + 1].val++; //count++
					if (is_samp)
					{
						args[0] = cell->m_val[idx + 2].val;
						args[1] = DirectFunctionCall2(float8mul, pVal[i], pVal[i]);	
						result = float8pl(&finfo);
						cell->m_val[idx + 2].val = result;
					}
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transvalues =  check_float8_array(transarray, "float8_avg", 3);

					cell->m_val[idx].val = DirectFunctionCall2(float8pl,cell->m_val[idx].val, Float8GetDatum(transvalues[1]));
					cell->m_val[idx + 1].val = DirectFunctionCall2(float8pl,cell->m_val[idx + 1].val,Float8GetDatum(transvalues[0]));
					if (is_samp)
					{
						cell->m_val[idx + 2].val = DirectFunctionCall2(float8pl, cell->m_val[idx + 2].val, Float8GetDatum(transvalues[2]));
					}
				}
			}
		}
	}

	return NULL;
}

/*
* @Description: For each level of avg/stddev_samp, a trans operation is needed
* @in isTransition -  is the first stage of avg/stddev_samp.
* @in is_samp- is avg or stddev_samp
* @return - ScalarVector
*/
template<bool isTransition, bool is_samp>
ScalarVector*
vfloat4_avg(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector1 = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	hashCell*	  cell = NULL;
	int			  i;
	ScalarValue*  pVal = pVector1->m_vals;
	uint8*		  flag = pVector1->m_flag;
	int			  nrows = pVector1->m_rows;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;
	ArrayType  *transarray = NULL;
	float8	   *transvalues = NULL;
	Datum		  pvalue;

	finfo.arg = &args[0];

	for (i = 0 ; i < nrows; i++)
	{
		cell = loc[i];
		if (cell && IS_NULL(flag[i]) == false) //only do when not null
		{
			if (IS_NULL(cell->m_val[idx].flag))
			{
				if (isTransition)
				{
					pvalue = DirectFunctionCall1(ftod,pVal[i]);
					cell->m_val[idx].val =  pvalue;
					cell->m_val[idx + 1].val = 1; //count set 1
					if (is_samp)
					{
						cell->m_val[idx + 2].val = DirectFunctionCall2(float8mul, pvalue, pvalue);
					}
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transvalues =  check_float8_array(transarray, "float8_avg", 3);

					cell->m_val[idx].val = Float8GetDatum(transvalues[1]);
					cell->m_val[idx + 1].val = Float8GetDatum(transvalues[0]);
					if (is_samp)
					{
						cell->m_val[idx + 2].val = Float8GetDatum(transvalues[2]);
					}
				}

				SET_NOTNULL(cell->m_val[idx].flag);
				SET_NOTNULL(cell->m_val[idx + 1].flag);
				if (is_samp)
				{
					SET_NOTNULL(cell->m_val[idx + 2].flag);
				}
				else
				{
					SET_NULL(cell->m_val[idx + 2].flag);
				}
			}
			else
			{
				if (isTransition)
				{
					args[0] = cell->m_val[idx].val;
					args[1] = pVal[i];
					result = float84pl(&finfo);
					cell->m_val[idx].val = result;
					cell->m_val[idx + 1].val++; //count++
					if (is_samp)
					{
						args[0] = cell->m_val[idx + 2].val;
						pvalue = DirectFunctionCall1(ftod,pVal[i]);
						args[1] = DirectFunctionCall2(float8mul, pvalue, pvalue);
						result = float8pl(&finfo);
						cell->m_val[idx + 2].val = result;
					}
				}
				else
				{
					transarray = DatumGetArrayTypeP(pVal[i]);
					transvalues =  check_float8_array(transarray, "float8_avg", 3);


					cell->m_val[idx].val = DirectFunctionCall2(float8pl,cell->m_val[idx].val, Float8GetDatum(transvalues[1]));
					cell->m_val[idx + 1].val = DirectFunctionCall2(float8pl,cell->m_val[idx + 1].val,Float8GetDatum(transvalues[0]));
					if (is_samp)
					{
						cell->m_val[idx + 2].val = DirectFunctionCall2(float8pl,cell->m_val[idx + 2].val,Float8GetDatum(transvalues[2]));
					}
				}
			}
		}
	}

	return NULL;
}

template <PGFunction floatFun>
ScalarVector*
vfloat_min_max(PG_FUNCTION_ARGS)
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

	finfo.arg = &args[0];

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

				result = floatFun(&finfo);

				cell->m_val[idx].val = result;
			}
		}
	}

	return NULL;
}

template<PGFunction floatFun>
ScalarVector*
vfloat_sum(PG_FUNCTION_ARGS)
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

	finfo.arg = &args[0];

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

				result = floatFun(&finfo);
				cell->m_val[idx].val = result;
			}
		}
	}

	return NULL;
}

template <PGFunction floatFun>
ScalarVector*
vfloat4_mop(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	ScalarValue* parg2 = PG_GETARG_VECVAL(1);
	int32		   nvalues = PG_GETARG_INT32(2);
	ScalarValue* presult = PG_GETARG_VECVAL(3);
	bool*		pselection = PG_GETARG_SELECTION(4);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
	uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	int          i;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];

	if(likely(pselection == NULL))
	{
		for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				args[0] = parg1[i];
				args[1] = parg2[i];

				result = floatFun(&finfo);
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
					args[0] = parg1[i];
					args[1] = parg2[i];

					result = floatFun(&finfo);
					presult[i] = result;
					SET_NOTNULL(pflagsRes[i]);
				}
				else
					SET_NULL(pflagsRes[i]);
			}
		}
	}

	PG_GETARG_VECTOR(3)->m_rows = nvalues;
	PG_GETARG_VECTOR(3)->m_desc.typeId = FLOAT4OID;
	return PG_GETARG_VECTOR(3);
}


template <PGFunction floatFun>
ScalarVector*
vfloat8_mop(PG_FUNCTION_ARGS)
{
	ScalarValue* parg1 = PG_GETARG_VECVAL(0);
	ScalarValue* parg2 = PG_GETARG_VECVAL(1);
	int32		   nvalues = PG_GETARG_INT32(2);
	ScalarValue* presult = PG_GETARG_VECVAL(3);
	bool*		pselection = PG_GETARG_SELECTION(4);
	uint8*		pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
	uint8*		pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
	uint8* 		pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
	int          i;
	Datum 		  args[2];
	Datum		  result;
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];

	if(likely(pselection == NULL))
	{
		for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				args[0] = parg1[i];
				args[1] = parg2[i];

				result = floatFun(&finfo);
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
					args[0] = parg1[i];
					args[1] = parg2[i];

					result = floatFun(&finfo);
					presult[i] = result;
					SET_NOTNULL(pflagsRes[i]);
				}
				else
					SET_NULL(pflagsRes[i]);
			}
		}
	}

	PG_GETARG_VECTOR(3)->m_rows = nvalues;
	PG_GETARG_VECTOR(3)->m_desc.typeId = FLOAT8OID;
	return PG_GETARG_VECTOR(3);
}

#endif

