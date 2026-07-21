#include "tsdb.h"
#include "catalog/objectaccess.h"



BackgroundWorker *MyBgworkerEntry;

bool row_security =true;
PGDLLIMPORT int NamedLWLockTrancheRequests;


long swaptype;
long es;


double		parallel_setup_cost ;
double		parallel_tuple_cost ;


Datum
make_interval(PG_FUNCTION_ARGS)
{
	int32		years = PG_GETARG_INT32(0);
	int32		months = PG_GETARG_INT32(1);
	int32		weeks = PG_GETARG_INT32(2);
	int32		days = PG_GETARG_INT32(3);
	int32		hours = PG_GETARG_INT32(4);
	int32		mins = PG_GETARG_INT32(5);
	double		secs = PG_GETARG_FLOAT8(6);
	Interval   *result;

	/*
	 * Reject out-of-range inputs.  We really ought to check the integer
	 * inputs as well, but it's not entirely clear what limits to apply.
	 */
	if (isinf(secs) || isnan(secs))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result = (Interval *) palloc(sizeof(Interval));
	result->month = years * MONTHS_PER_YEAR + months;
	result->day = weeks * 7 + days;

#ifdef HAVE_INT64_TIMESTAMP
	secs = rint(secs * USECS_PER_SEC);
	result->time = hours * ((int64) SECS_PER_HOUR * USECS_PER_SEC) +
		mins * ((int64) SECS_PER_MINUTE * USECS_PER_SEC) +
		(int64) secs;
#else
	result->time = hours * (double) SECS_PER_HOUR +
		mins * (double) SECS_PER_MINUTE +
		secs;
#endif

	PG_RETURN_INTERVAL_P(result);
} 


void
RunObjectPostAlterHook(Oid classId, Oid objectId, int subId,
					   Oid auxiliaryId, bool is_internal)
{
	ObjectAccessPostAlter pa_arg;

	/* caller should check, but just in case... */
	Assert(object_access_hook != NULL);

	memset(&pa_arg, 0, sizeof(ObjectAccessPostAlter));
	pa_arg.auxiliary_id = auxiliaryId;
	pa_arg.is_internal = is_internal;
	(*object_access_hook) ((ObjectAccessType)2,
						   classId, objectId, subId,
						   (void *) &pa_arg);
} 