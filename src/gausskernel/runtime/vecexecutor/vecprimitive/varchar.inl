/* --------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2002, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * varchar.inl
 *	  template implementation of char/varchar routines.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/vecexecutor/vecprimitive/varchar.inl
 *
 * -------------------------------------------------------------------------
 */

#ifndef VARCHAR_INL
#define VARCHAR_INL

#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <limits.h>
#include "utils/formatting.h"
#include "utils/builtins.h"
#include "vecexecutor/vechashtable.h"
#include "mb/pg_wchar.h"
#include "fmgr.h"
#include "vecexecutor/vecfunc.h"
#include "access/tuptoaster.h"


/* Comparison Functions used for bpchar type
 * vectorize function
 */
template<SimpleOp sop>
static FORCE_INLINE bool
bpchar_sop(Datum arg1, Datum arg2, Oid collation)
{
	if (sop == SOP_EQ)
	{
		return 	DatumGetBool(DirectFunctionCall2(bpchareq, arg1, arg2));
	}	
	else if (sop == SOP_NEQ)
	{
		return DatumGetBool(DirectFunctionCall2(bpcharne, arg1, arg2));
	}
	else if (sop == SOP_LE)
	{
		return DatumGetBool(DirectFunctionCall2Coll(bpcharle, collation, arg1, arg2));
	}
	else if (sop == SOP_LT)
	{
		return DatumGetBool(DirectFunctionCall2Coll(bpcharlt, collation, arg1, arg2));
	}
	else if (sop == SOP_GE)
	{
		return DatumGetBool(DirectFunctionCall2Coll(bpcharge, collation, arg1, arg2));
	}
	else if (sop == SOP_GT)
	{
		return DatumGetBool(DirectFunctionCall2Coll(bpchargt, collation, arg1, arg2));
	}
}


/* Comparison Functions used for text type
 * vectorize function
 */
template<SimpleOp sop>
static FORCE_INLINE bool
text_sop(Datum arg1, Datum arg2, Oid collation)
{
	if (sop == SOP_EQ)
	{
		return DatumGetBool(DirectFunctionCall2(texteq, arg1, arg2));
	}
	else if (sop == SOP_NEQ)
	{
		return DatumGetBool(DirectFunctionCall2(textne, arg1, arg2));
	}
	else if (sop == SOP_LE)
	{
		return DatumGetBool(DirectFunctionCall2Coll(text_le, collation, arg1, arg2));
	}
	else if (sop == SOP_LT)
	{
		return DatumGetBool(DirectFunctionCall2Coll(text_lt, collation, arg1, arg2));
	}
	else if (sop == SOP_GE)
	{
		 return DatumGetBool(DirectFunctionCall2Coll(text_ge, collation, arg1, arg2));
	}
	else if (sop == SOP_GT)
	{
		return DatumGetBool(DirectFunctionCall2Coll(text_gt, collation, arg1, arg2));
	}
}

/* Comparison Functions used for bpchar
 * vectorize function
 */

template <SimpleOp sop>
ScalarVector *
vbpchar_sop(PG_FUNCTION_ARGS)
{
	ScalarVector	*VecParg1 = PG_GETARG_VECTOR(0);
	ScalarVector	*VecParg2 = PG_GETARG_VECTOR(1);
	int32			nvalues = PG_GETARG_INT32(2);
	ScalarVector	*VecResult = PG_GETARG_VECTOR(3);
	uint8*  pflag = (uint8*)(VecResult->m_flag);
	bool			*pselection = PG_GETARG_SELECTION(4);
	ScalarValue 	*presult = VecResult->m_vals;
	ScalarValue		*parg1 = VecParg1->m_vals;
	ScalarValue		*parg2 = VecParg2->m_vals;
	uint8*			pflags1 = VecParg1->m_flag;
	uint8*			pflags2 = VecParg2->m_flag;
	Datum			value1;
	Datum			value2;
	Oid				collation = PG_GET_COLLATION();
	int				i;
	bool			result = false;
	

    if(likely(pselection == NULL))
    {
    	for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				value1= ScalarVector::Decode(parg1[i]);
				value2 = ScalarVector::Decode(parg2[i]);
				result = bpchar_sop<sop>(value1, value2, collation);
				presult[i] = result;
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
					value1= ScalarVector::Decode(parg1[i]);
					value2 = ScalarVector::Decode(parg2[i]);
					result = bpchar_sop<sop>(value1, value2, collation);
					presult[i] = result;
					SET_NOTNULL(pflag[i]);
				}
				else
					SET_NULL(pflag[i]);
			}
		}
    }

	VecResult->m_rows = nvalues;

	return VecResult;
}


template <SimpleOp sop>
ScalarVector *
vtext_sop(PG_FUNCTION_ARGS)
{
	ScalarVector	*VecParg1 = PG_GETARG_VECTOR(0);
	ScalarVector	*VecParg2 = PG_GETARG_VECTOR(1);
	int32			nvalues = PG_GETARG_INT32(2);
	ScalarVector	*VecResult = PG_GETARG_VECTOR(3);
	uint8*  pflag = (uint8*)(VecResult->m_flag);
	bool			*pselection = PG_GETARG_SELECTION(4);
	ScalarValue		*parg1 = VecParg1->m_vals;
	ScalarValue		*parg2 = VecParg2->m_vals;
	ScalarValue 	*presult = VecResult->m_vals;
	uint8*			pflags1 = VecParg1->m_flag;
	uint8*			pflags2 = VecParg2->m_flag;
	Datum			value1;
	Datum			value2;
	Oid          	collation = PG_GET_COLLATION();
	int				i;
	bool			result = false;

    if(likely(pselection == NULL))
    {
    	for (i = 0; i < nvalues; i++)
		{
			if (BOTH_NOT_NULL(pflags1[i], pflags2[i]))
			{
				value1= ScalarVector::Decode(parg1[i]);
				value2 = ScalarVector::Decode(parg2[i]);
				result = text_sop<sop>(value1, value2, collation);
				presult[i] = result;
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
					value1= ScalarVector::Decode(parg1[i]);
					value2 = ScalarVector::Decode(parg2[i]);
					result = text_sop<sop>(value1, value2, collation);
					presult[i] = result;
					SET_NOTNULL(pflag[i]);
				}
				else
					SET_NULL(pflag[i]);
			}
		}
	}

	VecResult->m_rows = nvalues;

	return VecResult;
}

// if bigerOrSmaller equals 1, means find the larger, otherwise find the smaller one
//
template <PGFunction bpcharFun, int bigerOrSmaller>
ScalarVector*
vbpchar_min_max(PG_FUNCTION_ARGS)
{
	ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
	int			  idx = PG_GETARG_DATUM(1);

	MemoryContext	context = (MemoryContext)PG_GETARG_DATUM(3);
	hashCell**	  loc = (hashCell**)PG_GETARG_DATUM(2);
	hashCell*	  cell = NULL;
	int		  i;
	ScalarValue*      pVal = pVector->m_vals;
	uint8*		  flag = pVector->m_flag;
	int		  nrows = pVector->m_rows;
	Datum 		  args1;
	Datum 		  args2;
	int		  result = 0;
	Oid 		  collation = PG_GET_COLLATION();


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
				args1 = ScalarVector::Decode(cell->m_val[idx].val);
				args2 = ScalarVector::Decode(pVal[i]);
				result = DirectFunctionCall2Coll(bpcharFun, collation, args1, args2);
				if(bigerOrSmaller * result < 0)
					cell->m_val[idx].val = replaceVariable(context, args1, args2);
			}
		}
	}

	return NULL;
}

template<int eml>
inline int32
vec_text_len(Datum str, mblen_converter fun_mblen)
{
	int len = 0;

	/* fastpath when max encoding length is one */
	if (eml == 1)
		len = toast_raw_datum_size(str) - VARHDRSZ;
	else
	{
		text	   *t = DatumGetTextPP(str);
		const char *mbstr;
		int limit;

		mbstr = VARDATA_ANY(t);
		limit = VARSIZE_ANY_EXHDR(t);
		while (limit > 0 && *mbstr)
		{
			int			l = fun_mblen((const unsigned char *)mbstr);

			limit -= l;
			mbstr += l;
			len++;
		}
		if (limit < 0)   //Prevent string length from exceeding the limit.
			len--;
	}

	return len;
}


inline static Datum
null_return(bool *is_null)
{
	text *result	= NULL;
	if(u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
	{
		*is_null = true;
		return (Datum)0;
	}
	else
	{
		result = cstring_to_text("");
		PG_RETURN_TEXT_P(result);
	}
}
template<bool orclcompat,bool withlen, int  eml, bool is_compress>
Datum 
vec_text_substr(Datum str, int32 start, int32 length, bool *is_null, mblen_converter fun_mblen)
{
	int32	S			= start;		/* start position */
	int32	S1;						/* adjusted start position */
	int32	L1;						/* adjusted substring length */
	text	*result	= NULL;
	text	*slice = NULL;
	int32	i;
	char	*p = NULL;
	char	*s = NULL;
	int		E;					/* end position */
	int32	slice_size;
	int32	slice_strlen;
	int32	E1;
	int		total;
	errno_t rc;


	total = vec_text_len<eml>(str, fun_mblen);

	if(orclcompat)
	{
		if(withlen)
		{
			if ((length <= 0) || (start > total) || (start + total < 0))
			{
				return null_return(is_null);
			}
		}
		else
		{
			if ((start > total) || (start + total < 0))
			{
				return null_return(is_null);
			}
		}
		/*
		 * amend the start position. when S < 0,
		 * amend the sartPosition to abs(start) from last char,
		 * when s==0, the start position is set 1.
		 */
		if (S < 0)
		{
			S = total + S + 1;
		}
		else if (0 == S)
		{
			S = 1;
		}	
	}

	S1 = Max(S, 1);
	
	if (withlen == false)
	{
		slice_size = L1 = -1;
		E = total + 1;
	}
	else
	{
		E = S + length;
		/*
		 * A negative value for L is the only way for the end position to
		 * be before the start. SQL99 says to throw an error.
		 */
		if (E < S)
		{

			if (length < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SUBSTRING_ERROR),
						 errmsg("negative substring length not allowed")));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SUBSTRING_ERROR),
						errmsg("the giving length is too long, it lets the end postion integer out of range")));
			}
		}
		/*
		 * A zero or negative value for the end position can happen if the
		 * start was negative or one. SQL99 says to return a zero-length
		 * string.
		 */
		if (E < 1)
		{
			return null_return(is_null);
		}

		L1 = E - S1;

		if(eml > 1)
			slice_size = E * eml;
	}


	E = Min(E,total + 1);

	if(eml == 1)
	{
		result = DatumGetTextPSlice(str, S1 - 1, L1);

		/* null return for A db format */
		if (result == NULL || 0 == VARSIZE_ANY_EXHDR(result))
			return null_return(is_null);
		else
			PG_RETURN_TEXT_P(result);
	}
	else
	{
		/*
		 * If we're working with an untoasted source, no need to do an extra
		 * copying step.
		 */
		if (is_compress)
		{
			slice = DatumGetTextPSlice(str, 0, slice_size);

			/* Now we can get the actual length of the slice in MB characters */
			slice_strlen = pg_mbstrlen_with_len(VARDATA_ANY(slice), VARSIZE_ANY_EXHDR(slice));

			/*
			 * Check that the start position wasn't > slice_strlen. If so, SQL99
			 * says to return a zero-length string.
			 */
			if (S1 > slice_strlen)
			{
				if (slice != (text *) DatumGetPointer(str))
					pfree_ext(slice);
				return null_return(is_null);

			}

			/*
			 * Adjust L1 and E1 now that we know the slice string length. Again
			 * remember that S1 is one based, and slice_start is zero based.
			 */
			if (L1 > -1)
				E1 = Min(S1 + L1, 1 + slice_strlen);
			else
				E1 = 1 + slice_strlen;
		}
		else
		{
			slice = (text *) DatumGetPointer(str);
			E1 = E ;
		}
		
		if (S1 >= E1)
		{
			return null_return(is_null);
		}
		
		p = VARDATA_ANY(slice);
		for (i = 0; i < S1 - 1; i++)
		{
			p += fun_mblen((const unsigned char *)p);
		}

		/* hang onto a pointer to our start position */
		s = p;

		/*
		 * Count the actual bytes used by the substring of the requested
		 * length.
		 */
		for (i = S1; i < E1; i++)
		{
			p += fun_mblen((const unsigned char *)p);
		}

		if(0 == (p - s))
		{
			return null_return(is_null);
		}
		
		result = (text *) palloc(VARHDRSZ + (p - s));
		SET_VARSIZE(result, VARHDRSZ + (p - s));
		rc = memcpy_s(VARDATA(result), (p - s), s, (p - s));
		securec_check(rc,"\0","\0");
		PG_RETURN_TEXT_P(result);
	}
}

/*
 * vtext_substr() vectorize function
 *
 * Return a substring starting at the specified position.
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length
 */
template<bool orclcompat, bool withlen>
ScalarVector *
vtext_substr (PG_FUNCTION_ARGS)
{
	ScalarVector	*VecParg1 = PG_GETARG_VECTOR(0);
	ScalarValue		*parg2 = PG_GETARG_VECVAL(1);
	uint8			*flag2 = PG_GETARG_VECTOR(1)->m_flag;
	
	uint8 			*flag3 = NULL;
	ScalarValue		*parg3 = NULL;
	int32			nvalues = 0;
	ScalarVector	*VecResult = NULL;
	bool			*pselection = NULL;
	Datum			result;
	int				i = 0;
	int32 			eml;
	int				start;
	int				length = 0;
	bool			isnull = false;
	bool			is_compress = false;
	int				baseIdx = 0;
	
	eml	= pg_database_encoding_max_length();
	mblen_converter	fun_mblen;
	fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

	if(withlen)
	{
		parg3 = PG_GETARG_VECVAL(2);
		flag3 = PG_GETARG_VECTOR(2)->m_flag;
		nvalues = PG_GETARG_INT32(3);
		VecResult = PG_GETARG_VECTOR(4);
		pselection = PG_GETARG_SELECTION(5);
	}
	else
	{
		nvalues = PG_GETARG_INT32(2);
		VecResult = PG_GETARG_VECTOR(3);
		pselection = PG_GETARG_SELECTION(4);
	}

	ScalarValue		*parg1 = VecParg1->m_vals;
	uint8           *flag1 = VecParg1->m_flag;
	uint8			*ResultFlag = VecResult->m_flag;
	if (pselection != NULL)
	{
		for (i = 0; i < nvalues; i++)
		{
			isnull = false;
			if (pselection[i])
			{
				if (NOT_NULL(flag1[i]) && NOT_NULL(flag2[i]) && (withlen ? NOT_NULL(flag3[i]) : true))
				{
					start = parg2[i];
					if(withlen)
						length = parg3[i];
					
					is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(parg1[i])) || VARATT_IS_EXTERNAL(DatumGetPointer(parg1[i])));
					baseIdx = orclcompat * 4 + withlen * 2 + is_compress + (eml - 1) * 8;

					result = (*substr_Array[baseIdx])(parg1[i],start,length,&isnull,fun_mblen);

					if(isnull == true)
						SET_NULL(ResultFlag[i]);
					else
					{
						VecResult->m_vals[i] = result;
						SET_NOTNULL(ResultFlag[i]);
					}
				}
				else
				{
					SET_NULL(ResultFlag[i]);
				}
			}
		}
	}
	else
	{
		for (i = 0; i < nvalues; i++)
		{
			isnull = false;
			if (NOT_NULL(flag1[i]) && NOT_NULL(flag2[i]) && (withlen ? NOT_NULL(flag3[i]) : true))
			{
				start = parg2[i];
				if(withlen)
					length = parg3[i];

				is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(parg1[i])) || VARATT_IS_EXTERNAL(DatumGetPointer(parg1[i])));
				
				baseIdx = orclcompat * 4 + withlen * 2 + is_compress + (eml - 1) * 8;

				result = (*substr_Array[baseIdx])(parg1[i],start,length,&isnull,fun_mblen);

				if(isnull == true)
					SET_NULL(ResultFlag[i]);
				else
				{
					VecResult->m_vals[i] = result;
					SET_NOTNULL(ResultFlag[i]);
				}
			}
			else
			{
				SET_NULL(ResultFlag[i]);
			}
		}

	}

	VecResult->m_rows = nvalues;

	return VecResult;
}

template <PGFunction bpcharFunc>
ScalarVector *
vbpchar(PG_FUNCTION_ARGS)
{
	ScalarVector	*VecParg1 = PG_GETARG_VECTOR(0);
	ScalarValue		*parg2 = NULL;
	ScalarValue		*parg3 = NULL;
	int32			nvalues = 0;
	ScalarVector	*VecResult = NULL;
	bool			*pselection = NULL;
	Datum			result;
	int				i;
	Datum 			args[3];
	FunctionCallInfoData finfo;

	if (bpcharFunc == name_bpchar)
	{
		nvalues = PG_GETARG_INT32(1);
		VecResult = PG_GETARG_VECTOR(2);
		pselection = PG_GETARG_SELECTION(3);
	}
	else if (bpcharFunc == bpchar)
	{
		parg2 = PG_GETARG_VECVAL(1);
		parg3 = PG_GETARG_VECVAL(2);
		nvalues = PG_GETARG_INT32(3);
		VecResult = PG_GETARG_VECTOR(4);
		pselection = PG_GETARG_SELECTION(5);
	}
	else if (bpcharFunc == char_bpchar)
	{
		nvalues = PG_GETARG_INT32(1);
		VecResult = PG_GETARG_VECTOR(2);
		pselection = PG_GETARG_SELECTION(3);
	}
	else
	{
		nvalues = PG_GETARG_INT32(1);
		VecResult = PG_GETARG_VECTOR(2);
		pselection = PG_GETARG_SELECTION(3);
	}

	ScalarValue		*parg1 = VecParg1->m_vals;
	uint8           *flag1 = VecParg1->m_flag;
	uint8			*ResultFlag = VecResult->m_flag;
	finfo.arg = &args[0];

    if (pselection != NULL)
    {
		for (i = 0; i < nvalues; i++)
		{
			if (pselection[i])
			{
				if (NOT_NULL(flag1[i]))
				{
					args[0] = ScalarVector::Decode(parg1[i]);
					
					if(bpcharFunc == bpchar)
					{
						args[1] = parg2[i];
						args[2] = parg3[i];
					}
					finfo.isnull = false;
					result = bpcharFunc(&finfo);
					if(finfo.isnull == true)
						SET_NULL(ResultFlag[i]);
					else
					{
						VecResult->m_vals[i] = result;
						SET_NOTNULL(ResultFlag[i]);
					}
				}
				else
				{
					SET_NULL(ResultFlag[i]);
				}
			}
		}
    }
    else
    {
		for (i = 0; i < nvalues; i++)
		{
			if (NOT_NULL(flag1[i]))
			{
				args[0] = ScalarVector::Decode(parg1[i]);
				
				if(bpcharFunc == bpchar)
				{
					args[1] = parg2[i];
					args[2] = parg3[i];
				}
				finfo.isnull = false;
				result = bpcharFunc(&finfo);
				if(finfo.isnull == true)
					SET_NULL(ResultFlag[i]);
				else
				{
					VecResult->m_vals[i] = result;
					SET_NOTNULL(ResultFlag[i]);
				}
			}
			else
			{
				SET_NULL(ResultFlag[i]);
			}
		}

    }

    VecResult->m_rows = nvalues;

    return VecResult;
}

template <PGFunction trim1Func>
ScalarVector *
vtrim1(PG_FUNCTION_ARGS)
{
	ScalarVector	*VecParg1 = PG_GETARG_VECTOR(0);
	int32			nvalues = PG_GETARG_INT32(1);
	ScalarVector	*VecResult = PG_GETARG_VECTOR(2);
	bool			*pselection = PG_GETARG_SELECTION(3);

	ScalarValue		*parg1 = VecParg1->m_vals;
	uint8			*flag = VecParg1->m_flag;
	uint8			*ResultFlag = VecResult->m_flag;
	Datum			result;
	int				i;
	Datum 			args[1];
	FunctionCallInfoData finfo;

	finfo.arg = &args[0];


	if (pselection != NULL)
	{
		for (i = 0; i < nvalues; i++)
		{
			if (pselection[i])
			{
				if (IS_NULL(flag[i]))
				{
					SET_NULL(ResultFlag[i]);
				}
				else
				{
					args[0] = ScalarVector::Decode(parg1[i]);
					finfo.isnull = false;
					result = trim1Func(&finfo);
					if(unlikely(finfo.isnull == true))
						SET_NULL(ResultFlag[i]);
					else
					{
						VecResult->m_vals[i] = result;
						SET_NOTNULL(ResultFlag[i]);
					}
				}
			}
		}
	}
	else
	{
		for (i = 0; i < nvalues; i++)
		{
			if (IS_NULL(flag[i]))
			{
				SET_NULL(ResultFlag[i]);
			}
			else
			{
				args[0] = ScalarVector::Decode(parg1[i]);
				finfo.isnull = false;
				result = trim1Func(&finfo);
				if(unlikely(finfo.isnull == true))
					SET_NULL(ResultFlag[i]);
				else
				{
					VecResult->m_vals[i] = result;
					SET_NOTNULL(ResultFlag[i]);
				}
			}
		}
	}

	PG_GETARG_VECTOR(2)->m_rows = nvalues;
    return PG_GETARG_VECTOR(2);
}

#endif // VARCHAR_INL
