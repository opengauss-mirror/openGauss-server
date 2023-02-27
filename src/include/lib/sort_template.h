/*-------------------------------------------------------------------------
 *
 * sort_template.h
 *
 *	  A template for a sort algorithm that supports varying degrees of
 *	  specialization.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1992-1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *		src/include/lib/sort_template.h
 *
 *-------------------------------------------------------------------------
 */

#define ST_MAKE_PREFIX(a) CppConcat(a,_)
#define ST_MAKE_NAME(a,b) ST_MAKE_NAME_(ST_MAKE_PREFIX(a),b)
#define ST_MAKE_NAME_(a,b) CppConcat(a,b)

#ifdef ST_ELEMENT_TYPE_VOID
#define ST_ELEMENT_TYPE void
#define ST_SORT_PROTO_ELEMENT_SIZE , size_t element_size
#define ST_SORT_INVOKE_ELEMENT_SIZE , element_size
#else
#define ST_SORT_PROTO_ELEMENT_SIZE
#define ST_SORT_INVOKE_ELEMENT_SIZE
#endif

#ifdef ST_COMPARE_RUNTIME_POINTER

#ifndef ST_COMPARATOR_TYPE_NAME
#define ST_COMPARATOR_TYPE_NAME ST_MAKE_NAME(ST_SORT, compare_function)
#endif
#define ST_COMPARE compare
#ifndef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#else
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#endif
#else
#define ST_SORT_PROTO_COMPARE
#define ST_SORT_INVOKE_COMPARE
#endif

#ifdef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_ARG , ST_COMPARE_ARG_TYPE *arg
#define ST_SORT_INVOKE_ARG , arg
#else
#define ST_SORT_PROTO_ARG
#define ST_SORT_INVOKE_ARG
#endif

#ifdef ST_DECLARE

#ifdef ST_COMPARE_RUNTIME_POINTER
typedef int (*ST_COMPARATOR_TYPE_NAME) (const ST_ELEMENT_TYPE *,
										const ST_ELEMENT_TYPE *ST_SORT_PROTO_ARG);
#endif

ST_SCOPE void ST_SORT(ST_ELEMENT_TYPE *first, size_t n
					  ST_SORT_PROTO_ELEMENT_SIZE
					  ST_SORT_PROTO_COMPARE
					  ST_SORT_PROTO_ARG);

#endif

#ifdef ST_DEFINE

#define ST_MED3 ST_MAKE_NAME(ST_SORT, med3)
#define ST_SWAP ST_MAKE_NAME(ST_SORT, swap)
#define ST_SWAPN ST_MAKE_NAME(ST_SORT, swapn)

#ifdef ST_CHECK_FOR_INTERRUPTS
#define DO_CHECK_FOR_INTERRUPTS() CHECK_FOR_INTERRUPTS()
#else
#define DO_CHECK_FOR_INTERRUPTS()
#endif

#ifdef ST_COMPARE_RUNTIME_POINTER
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_) ST_SORT_INVOKE_ARG)
#elif defined(ST_COMPARE_ARG_TYPE)
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_), arg)
#else
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_))
#endif
#define DO_MED3(a_, b_, c_)												\
	(ST_POINTER_TYPE *)ST_MED3((a_), (b_), (c_)											\
			ST_SORT_INVOKE_COMPARE										\
			ST_SORT_INVOKE_ARG)
#define DO_SORT(a_, n_)													\
	ST_SORT((a_), (n_)													\
			ST_SORT_INVOKE_ELEMENT_SIZE									\
			ST_SORT_INVOKE_COMPARE										\
			ST_SORT_INVOKE_ARG)

#ifndef ST_ELEMENT_TYPE_VOID
#define ST_POINTER_TYPE ST_ELEMENT_TYPE
#define ST_POINTER_STEP (unsigned int)1
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) ST_SWAP((a_), (b_))
#else
#define ST_POINTER_TYPE uint8
#define ST_POINTER_STEP (unsigned int)element_size
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) DO_SWAPN((a_), (b_), element_size)
#endif

static pg_noinline ST_ELEMENT_TYPE *
ST_MED3(ST_ELEMENT_TYPE *a,
		ST_ELEMENT_TYPE *b,
		ST_ELEMENT_TYPE *c
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	return DO_COMPARE(a, b) < 0 ?
		(DO_COMPARE(b, c) < 0 ? b : (DO_COMPARE(a, c) < 0 ? c : a))
		: (DO_COMPARE(b, c) > 0 ? b : (DO_COMPARE(a, c) < 0 ? a : c));
}

static inline void
ST_SWAP(ST_POINTER_TYPE *a, ST_POINTER_TYPE *b)
{
	ST_POINTER_TYPE tmp = *a;

	*a = *b;
	*b = tmp;
}

static inline void
ST_SWAPN(ST_POINTER_TYPE *a, ST_POINTER_TYPE *b, size_t n)
{
	for (size_t i = 0; i < n; ++i)
		ST_SWAP(&a[i], &b[i]);
}

ST_SCOPE void
ST_SORT(ST_ELEMENT_TYPE *data, size_t n
		ST_SORT_PROTO_ELEMENT_SIZE
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	ST_POINTER_TYPE *a = (ST_POINTER_TYPE *) data,
			   *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				presorted;

loop:
	DO_CHECK_FOR_INTERRUPTS();
	if (n < 7)
	{
		for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
			 pm += ST_POINTER_STEP)
			for (pl = pm; pl > a && DO_COMPARE(pl - ST_POINTER_STEP, pl) > 0;
				 pl -= ST_POINTER_STEP)
				DO_SWAP(pl, pl - ST_POINTER_STEP);
		return;
	}
	presorted = 1;
	for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
		 pm += ST_POINTER_STEP)
	{
		DO_CHECK_FOR_INTERRUPTS();
		if (DO_COMPARE(pm - ST_POINTER_STEP, pm) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = a + (n / 2) * ST_POINTER_STEP;
	if (n > 7)
	{
		pl = a;
		pn = a + (n - 1) * ST_POINTER_STEP;
		if (n > 40)
		{
			size_t		d = (n / 8) * ST_POINTER_STEP;

			pl = DO_MED3(pl, pl + d, pl + 2 * d);
			pm = DO_MED3(pm - d, pm, pm + d);
			pn = DO_MED3(pn - 2 * d, pn - d, pn);
		}
		pm = DO_MED3(pl, pm, pn);
	}
	DO_SWAP(a, pm);
	pa = pb = a + ST_POINTER_STEP;
	pc = pd = a + (n - 1) * ST_POINTER_STEP;
	for (;;)
	{
		while (pb <= pc && (r = DO_COMPARE(pb, a)) <= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pa, pb);
				pa += ST_POINTER_STEP;
			}
			pb += ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = DO_COMPARE(pc, a)) >= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pc, pd);
				pd -= ST_POINTER_STEP;
			}
			pc -= ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		DO_SWAP(pb, pc);
		pb += ST_POINTER_STEP;
		pc -= ST_POINTER_STEP;
	}
	pn = a + n * ST_POINTER_STEP;
	d1 = Min(pa - a, pb - pa);
	DO_SWAPN(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - ST_POINTER_STEP);
	DO_SWAPN(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		if (d1 > ST_POINTER_STEP)
			DO_SORT(a, d1 / ST_POINTER_STEP);
		if (d2 > ST_POINTER_STEP)
		{
			a = pn - d2;
			n = d2 / ST_POINTER_STEP;
			goto loop;
		}
	}
	else
	{
		if (d2 > ST_POINTER_STEP)
			DO_SORT(pn - d2, d2 / ST_POINTER_STEP);
		if (d1 > ST_POINTER_STEP)
		{
			n = d1 / ST_POINTER_STEP;
			goto loop;
		}
	}
}
#endif

#undef DO_CHECK_FOR_INTERRUPTS
#undef DO_COMPARE
#undef DO_MED3
#undef DO_SORT
#undef DO_SWAP
#undef DO_SWAPN
#undef ST_COMPARATOR_TYPE_NAME
#undef ST_COMPARE
#undef ST_COMPARE_ARG_TYPE
#undef ST_COMPARE_RUNTIME_POINTER
#undef ST_ELEMENT_TYPE
#undef ST_ELEMENT_TYPE_VOID
#undef ST_MAKE_NAME
#undef ST_MAKE_NAME_
#undef ST_MAKE_PREFIX
#undef ST_MED3
#undef ST_POINTER_STEP
#undef ST_POINTER_TYPE
#undef ST_SCOPE
#undef ST_SORT
#undef ST_SORT_INVOKE_ARG
#undef ST_SORT_INVOKE_COMPARE
#undef ST_SORT_INVOKE_ELEMENT_SIZE
#undef ST_SORT_PROTO_ARG
#undef ST_SORT_PROTO_COMPARE
#undef ST_SORT_PROTO_ELEMENT_SIZE
#undef ST_SWAP
#undef ST_SWAPN