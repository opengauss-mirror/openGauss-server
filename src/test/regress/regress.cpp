/*
 * src/test/regress/regress.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <float.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "c.h"

#include "fmgr.h"
#include "libpq/pqformat.h"
#include "stdio.h"

#include "nodes/parsenodes.h"
#include "utils/date.h"
#include "utils/datetime.h"

#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/typcache.h"
#include "utils/memutils.h"

#define P_MAXDIG 12
#define LDELIM '('
#define RDELIM ')'
#define DELIM ','

#define IsNull -1
#define IsNotNull 0
#define Multiplier_J 10000
#define LastDay(y, m) (((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) ? LastDaysL[m] : LastDays[m])
extern "C" Datum regress_dist_ptpath(PG_FUNCTION_ARGS);
extern "C" Datum regress_path_dist(PG_FUNCTION_ARGS);
extern "C" PATH* poly2path(POLYGON* poly);
extern "C" Datum interpt_pp(PG_FUNCTION_ARGS);
extern "C" void regress_lseg_construct(LSEG* lseg, Point* pt1, Point* pt2);
extern "C" Datum overpaid(PG_FUNCTION_ARGS);
extern "C" Datum boxarea(PG_FUNCTION_ARGS);
extern "C" Datum reverse_name(PG_FUNCTION_ARGS);
extern "C" Datum oldstyle_length(PG_FUNCTION_ARGS);
extern "C" Datum int44in(PG_FUNCTION_ARGS);
extern "C" Datum int44out(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_0(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_7(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_8(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_9(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_10(PG_FUNCTION_ARGS);
extern "C" Datum vec_int4add_11(PG_FUNCTION_ARGS);
extern Datum make_tuple_indirect(PG_FUNCTION_ARGS);

/************c function overload and v0&v1 support***********/
extern "C" Datum funcA(PG_FUNCTION_ARGS);
extern "C" Datum funcB(PG_FUNCTION_ARGS);
extern "C" Datum funcC(PG_FUNCTION_ARGS);

/**************create_type check*****************************/
extern "C" Datum complex_in(PG_FUNCTION_ARGS);
extern "C" Datum complex_out(PG_FUNCTION_ARGS);
extern "C" Datum complex_recv(PG_FUNCTION_ARGS);
extern "C" Datum complex_send(PG_FUNCTION_ARGS);

/***************************UDF CREM**************************/

extern "C" {
Datum truncInt1(PG_FUNCTION_ARGS);
Datum truncInt(PG_FUNCTION_ARGS);
Datum truncDec81(PG_FUNCTION_ARGS);
Datum truncDec8(PG_FUNCTION_ARGS);
Datum truncFloat(PG_FUNCTION_ARGS);
Datum TransTimestamp(PG_FUNCTION_ARGS);
Datum TransDate(PG_FUNCTION_ARGS);
Datum signi(PG_FUNCTION_ARGS);
Datum signf(PG_FUNCTION_ARGS);
Datum RoundInt(PG_FUNCTION_ARGS);
Datum RoundFloat(PG_FUNCTION_ARGS);
Datum RoundDec8(PG_FUNCTION_ARGS);
Datum lpad_f(PG_FUNCTION_ARGS);
Datum rpad_f(PG_FUNCTION_ARGS);
Datum normsdist(PG_FUNCTION_ARGS);
Datum months_between_dd(PG_FUNCTION_ARGS);
Datum months_between_dt(PG_FUNCTION_ARGS);
Datum months_between_td(PG_FUNCTION_ARGS);
Datum months_between_tt(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_ACC(PG_FUNCTION_ARGS);
//	Datum FUNC_GREAST_MOB24(PG_FUNCTION_ARGS);
Datum last_day_d(PG_FUNCTION_ARGS);
Datum last_day_t(PG_FUNCTION_ARGS);
Datum last_day_tz(PG_FUNCTION_ARGS);
}

/*
extern "C"
{
    Datum FUNC_II_DIV_CL_MOB6(PG_FUNCTION_ARGS);
    Datum oadd_months_d(PG_FUNCTION_ARGS);
    Datum oadd_months_t(PG_FUNCTION_ARGS);
    Datum oadd_months_tz(PG_FUNCTION_ARGS);
    Datum ceilDec8(PG_FUNCTION_ARGS);
    Datum ceilFloat(PG_FUNCTION_ARGS);
    Datum ceilInt(PG_FUNCTION_ARGS);
    Datum FUNC_AGE_JUDGE(PG_FUNCTION_ARGS);
    Datum FUNC_CAP(PG_FUNCTION_ARGS);
    Datum FUNC_COST_SERVICE(PG_FUNCTION_ARGS);
    Datum FUNC_DIV_CIIS_DATA_100(PG_FUNCTION_ARGS);
    Datum FUNC_DIV_CIIS_DATA_ZERO(PG_FUNCTION_ARGS);
    Datum FUNC_DIV_DATA(PG_FUNCTION_ARGS);
    Datum FUNC_DIV_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_DIV_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_DIV_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_DIV_MOB12(PG_FUNCTION_ARGS);
//	Datum FUNC_DPT_MOB12(PG_FUNCTION_ARGS);
    Datum FUNC_DPTADD_MOB6(PG_FUNCTION_ARGS);
    Datum FUNC_FOUR_SEG(PG_FUNCTION_ARGS);
    Datum FUNC_GREAST_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_GREAST_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_GREAST_MOB12(PG_FUNCTION_ARGS);
    Datum FUNC_II_CL_DPM_CAST(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_II_DIV_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_II_DIV_MOB12(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_NULL_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_NULL_MOB6(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_NULL_MOB9(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_NULL_MOB12(PG_FUNCTION_ARGS);
    Datum FUNC_II_CL_SUM_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_CL_SUM_MOB6(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_CL_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_MOB6(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_MOB9(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_MOB12(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_VAR_MOB3(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_VAR_MOB6(PG_FUNCTION_ARGS);
    Datum FUNC_II_GREAST_VAR_MOB9(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_CEIL_DATA(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_DATA(PG_FUNCTION_ARGS);
    Datum FUNC_II_DIV_DATA_NULL(PG_FUNCTION_ARGS);
    Datum FUNC_II_DPM_QC_MIN1(PG_FUNCTION_ARGS);
    Datum FUNC_II_FLOOR_ZERO(PG_FUNCTION_ARGS);
    Datum FUNC_III_CS0507_2(PG_FUNCTION_ARGS);
}
*/

extern "C" {
//	Datum FUNC_II_GREAST_VAR_MOB12(PG_FUNCTION_ARGS);
Datum FUNC_III_CS0507_3(PG_FUNCTION_ARGS);
Datum FUNC_III_CS0507(PG_FUNCTION_ARGS);
Datum FUNC_II_JUDGE_DF_AGE(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_CL_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_MOB12(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_MOB3(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_II_LEAST_VAR_MOB3(PG_FUNCTION_ARGS);
//	Datum FUNC_II_SUM_MOB12(PG_FUNCTION_ARGS);
//	Datum FUNC_II_SUM_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_II_SUM_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_II_SUM_MOB3(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_ACC_3(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_ACC_6(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_ACC_9(PG_FUNCTION_ARGS);
////	Datum FUNC_JUDGE_ACC_12(PG_FUNCTION_ARGS);
////	Datum FUNC_JUDGE_ACC_15(PG_FUNCTION_ARGS);
////	Datum FUNC_JUDGE_ACC_18(PG_FUNCTION_ARGS);
////	Datum FUNC_JUDGE_ACC_21(PG_FUNCTION_ARGS);
////	Datum FUNC_JUDGE_ACC_24(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_EAD(PG_FUNCTION_ARGS);
//	Datum FUNC_JUDGE_NUMBER(PG_FUNCTION_ARGS);
//	Datum FUNC_LEAST_MOB3(PG_FUNCTION_ARGS);
//	Datum FUNC_LEAST_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_LEAST_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_MINROC_AMOUNT(PG_FUNCTION_ARGS);
//	Datum FUNC_MINROC_NUM(PG_FUNCTION_ARGS);
//	Datum FUNC_MONTH_FLAG_BANNIAN(PG_FUNCTION_ARGS);
//	Datum FUNC_MONTH_FLAG_JI(PG_FUNCTION_ARGS);
//	Datum FUNC_OVE_MONTH_CODE(PG_FUNCTION_ARGS);
//	Datum FUNC_PRICE_CODE(PG_FUNCTION_ARGS);
//	Datum FUNC_SUM_MOB3(PG_FUNCTION_ARGS);
//	Datum FUNC_SUM_MOB6(PG_FUNCTION_ARGS);
//	Datum FUNC_SUM_MOB9(PG_FUNCTION_ARGS);
//	Datum FUNC_SUM_MOB12(PG_FUNCTION_ARGS);
Datum FUNC_TRANS_MOBCODE(PG_FUNCTION_ARGS);
Datum FUNC_TRANS_RISKCODE(PG_FUNCTION_ARGS);
Datum FUNC_XW_FIX_DPD(PG_FUNCTION_ARGS);
Datum FUNC_ZERO_NULL(PG_FUNCTION_ARGS);
Datum greatestcc(PG_FUNCTION_ARGS);
Datum greatestcd(PG_FUNCTION_ARGS);
Datum greatestci(PG_FUNCTION_ARGS);
Datum greatestdc(PG_FUNCTION_ARGS);
Datum greatestdd(PG_FUNCTION_ARGS);
Datum greatestdi(PG_FUNCTION_ARGS);
Datum greatestic(PG_FUNCTION_ARGS);
Datum greatestid(PG_FUNCTION_ARGS);
}

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(funcA);
PG_FUNCTION_INFO_V1(funcB);

/***************************UDF CREM**************************/

PG_FUNCTION_INFO_V1(truncInt1);
PG_FUNCTION_INFO_V1(truncInt);
PG_FUNCTION_INFO_V1(truncDec81);
PG_FUNCTION_INFO_V1(truncDec8);
PG_FUNCTION_INFO_V1(truncFloat);
PG_FUNCTION_INFO_V1(TransTimestamp);
PG_FUNCTION_INFO_V1(TransDate);
PG_FUNCTION_INFO_V1(signi);
PG_FUNCTION_INFO_V1(signf);
PG_FUNCTION_INFO_V1(RoundInt);
PG_FUNCTION_INFO_V1(RoundFloat);
PG_FUNCTION_INFO_V1(RoundDec8);
PG_FUNCTION_INFO_V1(lpad_f);
PG_FUNCTION_INFO_V1(rpad_f);
PG_FUNCTION_INFO_V1(normsdist);
PG_FUNCTION_INFO_V1(months_between_dd);
PG_FUNCTION_INFO_V1(months_between_dt);
PG_FUNCTION_INFO_V1(months_between_td);
PG_FUNCTION_INFO_V1(months_between_tt);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC);
// PG_FUNCTION_INFO_V1(FUNC_GREAST_MOB24);
PG_FUNCTION_INFO_V1(last_day_d);
PG_FUNCTION_INFO_V1(last_day_t);
PG_FUNCTION_INFO_V1(last_day_tz);

// PG_FUNCTION_INFO_V1(FUNC_II_DIV_CL_MOB6);
// PG_FUNCTION_INFO_V1(oadd_months_d);
// PG_FUNCTION_INFO_V1(oadd_months_t);
// PG_FUNCTION_INFO_V1(oadd_months_tz);
// PG_FUNCTION_INFO_V1(ceilDec8);
// PG_FUNCTION_INFO_V1(ceilFloat);
// PG_FUNCTION_INFO_V1(ceilInt);
// PG_FUNCTION_INFO_V1(FUNC_AGE_JUDGE);
// PG_FUNCTION_INFO_V1(FUNC_CAP);
// PG_FUNCTION_INFO_V1(FUNC_COST_SERVICE);
// PG_FUNCTION_INFO_V1(FUNC_DIV_CIIS_DATA_100);
// PG_FUNCTION_INFO_V1(FUNC_DIV_CIIS_DATA_ZERO);
// PG_FUNCTION_INFO_V1(FUNC_DIV_DATA);
// PG_FUNCTION_INFO_V1(FUNC_DIV_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_DIV_MOB6);
////PG_FUNCTION_INFO_V1(FUNC_DIV_MOB9);
////PG_FUNCTION_INFO_V1(FUNC_DIV_MOB12);
////PG_FUNCTION_INFO_V1(FUNC_DPT_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_DPTADD_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_FOUR_SEG);
// PG_FUNCTION_INFO_V1(FUNC_GREAST_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_GREAST_MOB6);
////PG_FUNCTION_INFO_V1(FUNC_GREAST_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_CL_DPM_CAST);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_MOB6);
////PG_FUNCTION_INFO_V1(FUNC_II_DIV_MOB9);
////PG_FUNCTION_INFO_V1(FUNC_II_DIV_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_NULL_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_NULL_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_NULL_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_NULL_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_CL_SUM_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_CL_SUM_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_CL_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_VAR_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_VAR_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_VAR_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_CEIL_DATA);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_DATA);
// PG_FUNCTION_INFO_V1(FUNC_II_DIV_DATA_NULL);
// PG_FUNCTION_INFO_V1(FUNC_II_DPM_QC_MIN1);
// PG_FUNCTION_INFO_V1(FUNC_II_FLOOR_ZERO);
// PG_FUNCTION_INFO_V1(FUNC_III_CS0507_2);

// PG_FUNCTION_INFO_V1(FUNC_II_GREAST_VAR_MOB12);
PG_FUNCTION_INFO_V1(FUNC_III_CS0507_3);
PG_FUNCTION_INFO_V1(FUNC_III_CS0507);
PG_FUNCTION_INFO_V1(FUNC_II_JUDGE_DF_AGE);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_CL_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_II_LEAST_VAR_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_II_SUM_MOB12);
// PG_FUNCTION_INFO_V1(FUNC_II_SUM_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_II_SUM_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_II_SUM_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_3);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_6);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_9);
////PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_12);
////PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_15);
////PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_18);
////PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_21);
////PG_FUNCTION_INFO_V1(FUNC_JUDGE_ACC_24);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_EAD);
// PG_FUNCTION_INFO_V1(FUNC_JUDGE_NUMBER);
// PG_FUNCTION_INFO_V1(FUNC_LEAST_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_LEAST_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_LEAST_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_MINROC_AMOUNT);
// PG_FUNCTION_INFO_V1(FUNC_MINROC_NUM);
// PG_FUNCTION_INFO_V1(FUNC_MONTH_FLAG_BANNIAN);
// PG_FUNCTION_INFO_V1(FUNC_MONTH_FLAG_JI);
// PG_FUNCTION_INFO_V1(FUNC_OVE_MONTH_CODE);
// PG_FUNCTION_INFO_V1(FUNC_PRICE_CODE);
// PG_FUNCTION_INFO_V1(FUNC_SUM_MOB3);
// PG_FUNCTION_INFO_V1(FUNC_SUM_MOB6);
// PG_FUNCTION_INFO_V1(FUNC_SUM_MOB9);
// PG_FUNCTION_INFO_V1(FUNC_SUM_MOB12);
PG_FUNCTION_INFO_V1(FUNC_TRANS_MOBCODE);
PG_FUNCTION_INFO_V1(FUNC_TRANS_RISKCODE);
PG_FUNCTION_INFO_V1(FUNC_XW_FIX_DPD);
PG_FUNCTION_INFO_V1(FUNC_ZERO_NULL);
PG_FUNCTION_INFO_V1(greatestcc);
PG_FUNCTION_INFO_V1(greatestcd);
PG_FUNCTION_INFO_V1(greatestci);
PG_FUNCTION_INFO_V1(greatestdc);
PG_FUNCTION_INFO_V1(greatestdd);
PG_FUNCTION_INFO_V1(greatestdi);
PG_FUNCTION_INFO_V1(greatestic);
PG_FUNCTION_INFO_V1(greatestid);

/***************create_type check*****************/
PG_FUNCTION_INFO_V1(complex_in);
PG_FUNCTION_INFO_V1(complex_out);
PG_FUNCTION_INFO_V1(complex_recv);
PG_FUNCTION_INFO_V1(complex_send);

/*
 * Distance from a point to a path
 */
PG_FUNCTION_INFO_V1(regress_dist_ptpath);

Datum regress_dist_ptpath(PG_FUNCTION_ARGS)
{
    Point* pt = PG_GETARG_POINT_P(0);
    PATH* path = PG_GETARG_PATH_P(1);
    float8 result = 0.0; /* keep compiler quiet */
    float8 tmp;
    int i;
    LSEG lseg;

    switch (path->npts) {
        case 0:
            PG_RETURN_NULL();
        case 1:
            result = point_dt(pt, &path->p[0]);
            break;
        default:

            /*
             * the distance from a point to a path is the smallest distance
             * from the point to any of its constituent segments.
             */
            Assert(path->npts > 1);
            for (i = 0; i < path->npts - 1; ++i) {
                regress_lseg_construct(&lseg, &path->p[i], &path->p[i + 1]);
                tmp = DatumGetFloat8(DirectFunctionCall2(dist_ps, PointPGetDatum(pt), LsegPGetDatum(&lseg)));
                if (i == 0 || tmp < result)
                    result = tmp;
            }
            break;
    }
    PG_RETURN_FLOAT8(result);
}

/*
 * this essentially does a cartesian product of the lsegs in the
 * two paths, and finds the min distance between any two lsegs
 */
PG_FUNCTION_INFO_V1(regress_path_dist);

Datum regress_path_dist(PG_FUNCTION_ARGS)
{
    PATH* p1 = PG_GETARG_PATH_P(0);
    PATH* p2 = PG_GETARG_PATH_P(1);
    bool have_min = false;
    float8 min = 0.0; /* initialize to keep compiler quiet */
    float8 tmp;
    int i, j;
    LSEG seg1, seg2;

    for (i = 0; i < p1->npts - 1; i++) {
        for (j = 0; j < p2->npts - 1; j++) {
            regress_lseg_construct(&seg1, &p1->p[i], &p1->p[i + 1]);
            regress_lseg_construct(&seg2, &p2->p[j], &p2->p[j + 1]);

            tmp = DatumGetFloat8(DirectFunctionCall2(lseg_distance, LsegPGetDatum(&seg1), LsegPGetDatum(&seg2)));
            if (!have_min || tmp < min) {
                min = tmp;
                have_min = true;
            }
        }
    }

    if (!have_min)
        PG_RETURN_NULL();

    PG_RETURN_FLOAT8(min);
}

PATH* poly2path(POLYGON* poly)
{
    int i;
    char* output = (char*)palloc(2 * (P_MAXDIG + 1) * poly->npts + 64);
    char buf[2 * (P_MAXDIG) + 20];

    sprintf(output, "(1, %*d", P_MAXDIG, poly->npts);

    for (i = 0; i < poly->npts; i++) {
        snprintf(buf, sizeof(buf), ",%*g,%*g", P_MAXDIG, poly->p[i].x, P_MAXDIG, poly->p[i].y);
        strcat(output, buf);
    }

    snprintf(buf, sizeof(buf), "%c", RDELIM);
    strcat(output, buf);
    return DatumGetPathP(DirectFunctionCall1(path_in, CStringGetDatum(output)));
}

/* return the point where two paths intersect, or NULL if no intersection. */
PG_FUNCTION_INFO_V1(interpt_pp);

Datum interpt_pp(PG_FUNCTION_ARGS)
{
    PATH* p1 = PG_GETARG_PATH_P(0);
    PATH* p2 = PG_GETARG_PATH_P(1);
    int i, j;
    LSEG seg1, seg2;
    bool found = false; /* We've found the intersection */

    found = false; /* Haven't found it yet */

    for (i = 0; i < p1->npts - 1 && !found; i++) {
        regress_lseg_construct(&seg1, &p1->p[i], &p1->p[i + 1]);
        for (j = 0; j < p2->npts - 1 && !found; j++) {
            regress_lseg_construct(&seg2, &p2->p[j], &p2->p[j + 1]);
            if (DatumGetBool(DirectFunctionCall2(lseg_intersect, LsegPGetDatum(&seg1), LsegPGetDatum(&seg2))))
                found = true;
        }
    }

    if (!found)
        PG_RETURN_NULL();

    /*
     * Note: DirectFunctionCall2 will kick out an error if lseg_interpt()
     * returns NULL, but that should be impossible since we know the two
     * segments intersect.
     */
    PG_RETURN_DATUM(DirectFunctionCall2(lseg_interpt, LsegPGetDatum(&seg1), LsegPGetDatum(&seg2)));
}

/* like lseg_construct, but assume space already allocated */
void regress_lseg_construct(LSEG* lseg, Point* pt1, Point* pt2)
{
    lseg->p[0].x = pt1->x;
    lseg->p[0].y = pt1->y;
    lseg->p[1].x = pt2->x;
    lseg->p[1].y = pt2->y;
    lseg->m = point_sl(pt1, pt2);
}

PG_FUNCTION_INFO_V1(overpaid);

Datum overpaid(PG_FUNCTION_ARGS)
{
    HeapTupleHeader tuple = PG_GETARG_HEAPTUPLEHEADER(0);
    bool isnull = false;
    int32 salary;

    salary = DatumGetInt32(GetAttributeByName(tuple, "salary", &isnull));
    if (isnull)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(salary > 699);
}

/* New type "widget"
 * This used to be "circle", but I added circle to builtins,
 *	so needed to make sure the names do not collide. - tgl 97/04/21
 */

typedef struct {
    Point center;
    double radius;
} WIDGET;

extern "C" Datum widget_in(PG_FUNCTION_ARGS);
extern "C" Datum widget_out(PG_FUNCTION_ARGS);
extern "C" Datum pt_in_widget(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(widget_in);
PG_FUNCTION_INFO_V1(widget_out);

#define NARGS 3

Datum widget_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    char *p, *coord[NARGS], buf2[1000];
    int i;
    WIDGET* result = NULL;

    if (str == NULL)
        PG_RETURN_NULL();
    for (i = 0, p = str; *p && i < NARGS && *p != RDELIM; p++)
        if (*p == ',' || (*p == LDELIM && !i))
            coord[i++] = p + 1;
    if (i < NARGS - 1)
        PG_RETURN_NULL();
    result = (WIDGET*)palloc(sizeof(WIDGET));
    result->center.x = atof(coord[0]);
    result->center.y = atof(coord[1]);
    result->radius = atof(coord[2]);

    snprintf(buf2, sizeof(buf2), "widget_in: read (%f, %f, %f)\n", result->center.x, result->center.y, result->radius);
    PG_RETURN_POINTER(result);
}

Datum widget_out(PG_FUNCTION_ARGS)
{
    WIDGET* widget = (WIDGET*)PG_GETARG_POINTER(0);
    char* result = NULL;

    if (widget == NULL)
        PG_RETURN_NULL();

    result = (char*)palloc(60);
    sprintf(result, "(%g,%g,%g)", widget->center.x, widget->center.y, widget->radius);
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(pt_in_widget);

Datum pt_in_widget(PG_FUNCTION_ARGS)
{
    Point* point = PG_GETARG_POINT_P(0);
    WIDGET* widget = (WIDGET*)PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(point_dt(point, &widget->center) < widget->radius);
}

PG_FUNCTION_INFO_V1(boxarea);

Datum boxarea(PG_FUNCTION_ARGS)
{
    BOX* box = PG_GETARG_BOX_P(0);
    double width, height;

    width = Abs(box->high.x - box->low.x);
    height = Abs(box->high.y - box->low.y);
    PG_RETURN_FLOAT8(width * height);
}

PG_FUNCTION_INFO_V1(reverse_name);

Datum reverse_name(PG_FUNCTION_ARGS)
{
    char* string = PG_GETARG_CSTRING(0);
    int i;
    int len;
    char* new_string = NULL;

    new_string = (char*)palloc0(NAMEDATALEN);
    for (i = 0; i < NAMEDATALEN && string[i]; ++i)
        ;
    if (i == NAMEDATALEN || !string[i])
        --i;
    len = i;
    for (; i >= 0; --i)
        new_string[len - i] = string[i];
    PG_RETURN_CSTRING(new_string);
}

PG_FUNCTION_INFO_V1(oldstyle_length);

/*
 * This rather silly function is just to test that oldstyle functions
 * work correctly on toast-able inputs.
 */
Datum oldstyle_length(PG_FUNCTION_ARGS)
{
    int n = PG_GETARG_INT32(0);
    text* t = (text*)PG_GETARG_DATUM(1);
    int len = 0;

    if (!PG_ARGISNULL(1))
        len = VARSIZE(t) - VARHDRSZ;
    else
        PG_RETURN_NULL();

    PG_RETURN_INT32(n + len);
}

static TransactionId fd17b_xid = InvalidTransactionId;
static TransactionId fd17a_xid = InvalidTransactionId;
static int fd17b_level = 0;
static int fd17a_level = 0;
static bool fd17b_recursion = true;
static bool fd17a_recursion = true;
extern "C" Datum funny_dup17(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(funny_dup17);

Datum funny_dup17(PG_FUNCTION_ARGS)
{
    TriggerData* trigdata = (TriggerData*)fcinfo->context;
    TransactionId* xid = NULL;
    int* level = NULL;
    bool* recursion = NULL;
    Relation rel;
    TupleDesc tupdesc;
    HeapTuple tuple;
    char *query, *fieldval, *fieldtype;
    char* when = NULL;
    int inserted;
    int selected = 0;
    int ret;

    if (!CALLED_AS_TRIGGER(fcinfo))
        (elog(ERROR, "funny_dup17: not fired by trigger manager"));

    tuple = trigdata->tg_trigtuple;
    rel = trigdata->tg_relation;
    tupdesc = rel->rd_att;
    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
        xid = &fd17b_xid;
        level = &fd17b_level;
        recursion = &fd17b_recursion;
        when = "BEFORE";
    } else {
        xid = &fd17a_xid;
        level = &fd17a_level;
        recursion = &fd17a_recursion;
        when = "AFTER ";
    }

    if (!TransactionIdIsCurrentTransactionId(*xid)) {
        *xid = GetCurrentTransactionId();
        *level = 0;
        *recursion = true;
    }

    if (*level == 17) {
        *recursion = false;
        return PointerGetDatum(tuple);
    }

    if (!(*recursion))
        return PointerGetDatum(tuple);

    (*level)++;

    SPI_connect();

    fieldval = SPI_getvalue(tuple, tupdesc, 1);
    fieldtype = SPI_gettype(tupdesc, 1);

    query = (char*)palloc(100 + NAMEDATALEN * 3 + strlen(fieldval) + strlen(fieldtype));

    sprintf(query,
        "insert into %s select * from %s where %s = '%s'::%s",
        SPI_getrelname(rel),
        SPI_getrelname(rel),
        SPI_fname(tupdesc, 1),
        fieldval,
        fieldtype);

    if ((ret = SPI_exec(query, 0)) < 0)
        (elog(ERROR, "funny_dup17 (fired %s) on level %3d: SPI_exec (insert ...) returned %d", when, *level, ret));

    inserted = SPI_processed;

    sprintf(query,
        "select count (*) from %s where %s = '%s'::%s",
        SPI_getrelname(rel),
        SPI_fname(tupdesc, 1),
        fieldval,
        fieldtype);

    if ((ret = SPI_exec(query, 0)) < 0)
        (elog(ERROR, "funny_dup17 (fired %s) on level %3d: SPI_exec (select ...) returned %d", when, *level, ret));

    if (SPI_processed > 0) {
        selected = DatumGetInt32(DirectFunctionCall1(
            int4in, CStringGetDatum(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
    }

    elog(DEBUG4,
        "funny_dup17 (fired %s) on level %3d: %d/%d tuples inserted/selected",
        when,
        *level,
        inserted,
        selected);

    SPI_finish();

    (*level)--;

    if (*level == 0)
        *xid = InvalidTransactionId;

    return PointerGetDatum(tuple);
}

extern "C" Datum ttdummy(PG_FUNCTION_ARGS);
extern "C" Datum set_ttdummy(PG_FUNCTION_ARGS);

#define TTDUMMY_INFINITY 999999

static SPIPlanPtr splan = NULL;
static bool ttoff = false;

PG_FUNCTION_INFO_V1(ttdummy);

Datum ttdummy(PG_FUNCTION_ARGS)
{
    TriggerData* trigdata = (TriggerData*)fcinfo->context;
    Trigger* trigger = NULL; /* to get trigger name */
    char** args;             /* arguments */
    int attnum[2];           /* fnumbers of start/stop columns */
    Datum oldon, oldoff;
    Datum newon, newoff;
    Datum* cvals = NULL;  /* column values */
    char* cnulls = NULL;  /* column nulls */
    char* relname = NULL; /* triggered relation name */
    Relation rel;         /* triggered relation */
    HeapTuple trigtuple;
    HeapTuple newtuple = NULL;
    HeapTuple rettuple;
    TupleDesc tupdesc;   /* tuple description */
    int natts;           /* # of attributes */
    bool isnull = false; /* to know is some column NULL or not */
    int ret;
    int i;

    if (!CALLED_AS_TRIGGER(fcinfo))
        (elog(ERROR, "ttdummy: not fired by trigger manager"));
    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        (elog(ERROR, "ttdummy: must be fired for row"));
    if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        (elog(ERROR, "ttdummy: must be fired before event"));
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        (elog(ERROR, "ttdummy: cannot process INSERT event"));
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        newtuple = trigdata->tg_newtuple;

    trigtuple = trigdata->tg_trigtuple;

    rel = trigdata->tg_relation;
    relname = SPI_getrelname(rel);

    /* check if TT is OFF for this relation */
    if (ttoff) /* OFF - nothing to do */
    {
        pfree(relname);
        return PointerGetDatum((newtuple != NULL) ? newtuple : trigtuple);
    }

    trigger = trigdata->tg_trigger;

    if (trigger->tgnargs != 2)
        (elog(ERROR, "ttdummy (%s): invalid (!= 2) number of arguments %d", relname, trigger->tgnargs));

    args = trigger->tgargs;
    tupdesc = rel->rd_att;
    natts = tupdesc->natts;

    for (i = 0; i < 2; i++) {
        attnum[i] = SPI_fnumber(tupdesc, args[i]);
        if (attnum[i] < 0)
            (elog(ERROR, "ttdummy (%s): there is no attribute %s", relname, args[i]));
        if (SPI_gettypeid(tupdesc, attnum[i]) != INT4OID)
            (elog(ERROR, "ttdummy (%s): attributes %s and %s must be of abstime type", relname, args[0], args[1]));
    }

    oldon = SPI_getbinval(trigtuple, tupdesc, attnum[0], &isnull);
    if (isnull)
        (elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[0]));

    oldoff = SPI_getbinval(trigtuple, tupdesc, attnum[1], &isnull);
    if (isnull)
        (elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[1]));

    if (newtuple != NULL) /* UPDATE */
    {
        newon = SPI_getbinval(newtuple, tupdesc, attnum[0], &isnull);
        if (isnull)
            (elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[0]));
        newoff = SPI_getbinval(newtuple, tupdesc, attnum[1], &isnull);
        if (isnull)
            (elog(ERROR, "ttdummy (%s): %s must be NOT NULL", relname, args[1]));

        if (oldon != newon || oldoff != newoff)
            (elog(ERROR,
                "ttdummy (%s): you cannot change %s and/or %s columns (use set_ttdummy)",
                relname,
                args[0],
                args[1]));

        if (newoff != TTDUMMY_INFINITY) {
            pfree(relname); /* allocated in upper executor context */
            return PointerGetDatum(NULL);
        }
    } else if (oldoff != TTDUMMY_INFINITY) /* DELETE */
    {
        pfree(relname);
        return PointerGetDatum(NULL);
    }

    newoff = DirectFunctionCall1(nextval, CStringGetTextDatum("ttdummy_seq"));
    /* nextval now returns int64; coerce down to int32 */
    newoff = Int32GetDatum((int32)DatumGetInt64(newoff));

    /* Connect to SPI manager */
    if ((ret = SPI_connect()) < 0)
        (elog(ERROR, "ttdummy (%s): SPI_connect returned %d", relname, ret));

    /* Fetch tuple values and nulls */
    cvals = (Datum*)palloc(natts * sizeof(Datum));
    cnulls = (char*)palloc(natts * sizeof(char));
    for (i = 0; i < natts; i++) {
        cvals[i] = SPI_getbinval((newtuple != NULL) ? newtuple : trigtuple, tupdesc, i + 1, &isnull);
        cnulls[i] = (isnull) ? 'n' : ' ';
    }

    /* change date column(s) */
    if (newtuple) /* UPDATE */
    {
        cvals[attnum[0] - 1] = newoff; /* start_date eq current date */
        cnulls[attnum[0] - 1] = ' ';
        cvals[attnum[1] - 1] = TTDUMMY_INFINITY; /* stop_date eq INFINITY */
        cnulls[attnum[1] - 1] = ' ';
    } else
    /* DELETE */
    {
        cvals[attnum[1] - 1] = newoff; /* stop_date eq current date */
        cnulls[attnum[1] - 1] = ' ';
    }

    /* if there is no plan ... */
    if (splan == NULL) {
        SPIPlanPtr pplan;
        Oid* ctypes = NULL;
        char* query = NULL;

        /* allocate space in preparation */
        ctypes = (Oid*)palloc(natts * sizeof(Oid));
        query = (char*)palloc(100 + 16 * natts);

        /*
         * Construct query: INSERT INTO _relation_ VALUES ($1, ...)
         */
        sprintf(query, "INSERT INTO %s VALUES (", relname);
        for (i = 1; i <= natts; i++) {
            sprintf(query + strlen(query), "$%d%s", i, (i < natts) ? ", " : ")");
            ctypes[i - 1] = SPI_gettypeid(tupdesc, i);
        }

        /* Prepare plan for query */
        pplan = SPI_prepare(query, natts, ctypes);
        if (pplan == NULL)
            (elog(ERROR, "ttdummy (%s): SPI_prepare returned %d", relname, SPI_result));

        if (SPI_keepplan(pplan))
            (elog(ERROR, "ttdummy (%s): SPI_keepplan failed", relname));

        splan = pplan;
    }

    ret = SPI_execp(splan, cvals, cnulls, 0);

    if (ret < 0)
        (elog(ERROR, "ttdummy (%s): SPI_execp returned %d", relname, ret));

    /* Tuple to return to upper Executor ... */
    if (newtuple) /* UPDATE */
    {
        HeapTuple tmptuple;

        tmptuple = SPI_copytuple(trigtuple);
        rettuple = SPI_modifytuple(rel, tmptuple, 1, &(attnum[1]), &newoff, NULL);
        SPI_freetuple(tmptuple);
    } else
        /* DELETE */
        rettuple = trigtuple;

    SPI_finish(); /* don't forget say Bye to SPI mgr */

    pfree(relname);

    return PointerGetDatum(rettuple);
}

PG_FUNCTION_INFO_V1(set_ttdummy);

Datum set_ttdummy(PG_FUNCTION_ARGS)
{
    int32 on = PG_GETARG_INT32(0);

    if (ttoff) /* OFF currently */
    {
        if (on == 0)
            PG_RETURN_INT32(0);

        /* turn ON */
        ttoff = false;
        PG_RETURN_INT32(0);
    }

    /* ON currently */
    if (on != 0)
        PG_RETURN_INT32(1);

    /* turn OFF */
    ttoff = true;

    PG_RETURN_INT32(1);
}

/*
 * Type int44 has no real-world use, but the regression tests use it.
 * It's a four-element vector of int4's.
 */

/*
 *		int44in			- converts "num num ..." to internal form
 *
 *		Note: Fills any missing positions with zeroes.
 */
PG_FUNCTION_INFO_V1(int44in);

Datum int44in(PG_FUNCTION_ARGS)
{
    char* input_string = PG_GETARG_CSTRING(0);
    int32* result = (int32*)palloc(4 * sizeof(int32));
    int i;

    i = sscanf(input_string, "%d, %d, %d, %d", &result[0], &result[1], &result[2], &result[3]);
    while (i < 4)
        result[i++] = 0;

    PG_RETURN_POINTER(result);
}

/*
 *		int44out		- converts internal form to "num num ..."
 */
PG_FUNCTION_INFO_V1(int44out);

Datum int44out(PG_FUNCTION_ARGS)
{
    int32* an_array = (int32*)PG_GETARG_POINTER(0);
    char* result = (char*)palloc(16 * 4); /* Allow 14 digits +
                                           * sign */
    int i;
    char* walk = NULL;

    walk = result;
    for (i = 0; i < 4; i++) {
        pg_ltoa(an_array[i], walk);
        while (*++walk != '\0')
            ;
        *walk++ = ' ';
    }
    *--walk = '\0';
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(make_tuple_indirect);
Datum make_tuple_indirect(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleData tuple;
    int ncolumns;
    Datum* values = NULL;
    bool* nulls = NULL;

    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;

    HeapTuple newtup;

    int i;

    MemoryContext old_context;

    /* Extract type info from the tuple itself */
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = tupdesc->natts;

    /* Build a temporary HeapTuple control structure */
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    values = (Datum*)palloc(ncolumns * sizeof(Datum));
    nulls = (bool*)palloc(ncolumns * sizeof(bool));

    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    old_context = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

    for (i = 0; i < ncolumns; i++) {
        struct varlena* attr;
        struct varlena* new_attr;
        struct varatt_indirect redirect_pointer;

        /* only work on existing, not-null varlenas */
        if (tupdesc->attrs[i]->attisdropped || nulls[i] || tupdesc->attrs[i]->attlen != -1)
            continue;

        attr = (struct varlena*)DatumGetPointer(values[i]);

        /* don't recursively indirect */
        if (VARATT_IS_EXTERNAL_INDIRECT(attr))
            continue;

        /* copy datum, so it still lives later */
        if (VARATT_IS_EXTERNAL_ONDISK(attr))
            attr = heap_tuple_fetch_attr(attr);
        else {
            struct varlena* oldattr = attr;
            attr = (struct varlena*)palloc0(VARSIZE_ANY(oldattr));
            memcpy(attr, oldattr, VARSIZE_ANY(oldattr));
        }

        /* build indirection Datum */
        new_attr = (struct varlena*)palloc0(INDIRECT_POINTER_SIZE);
        redirect_pointer.pointer = attr;
        SET_VARTAG_EXTERNAL(new_attr, VARTAG_INDIRECT);
        memcpy(VARDATA_EXTERNAL(new_attr), &redirect_pointer, sizeof(redirect_pointer));

        values[i] = PointerGetDatum(new_attr);
    }

    newtup = heap_form_tuple(tupdesc, values, nulls);
    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);

    MemoryContextSwitchTo(old_context);

    PG_RETURN_HEAPTUPLEHEADER(newtup->t_data);
}

/*
 *		vec_int4add_0 	- add int4 with 0 args
 */

PG_FUNCTION_INFO_V1(vec_int4add_0);
Datum vec_int4add_0(PG_FUNCTION_ARGS)
{
    int32 result;

    result = 1;

    PG_RETURN_INT32(result);
}
/*
 *		vec_int4add_7		- add int4 with 7 args
 */

PG_FUNCTION_INFO_V1(vec_int4add_7);
Datum vec_int4add_7(PG_FUNCTION_ARGS)
{
    int32 p1 = PG_GETARG_INT32(0);
    int32 p2 = PG_GETARG_INT32(1);
    int32 p3 = PG_GETARG_INT32(2);
    int32 p4 = PG_GETARG_INT32(3);
    int32 p5 = PG_GETARG_INT32(4);
    int32 p6 = PG_GETARG_INT32(5);
    int32 p7 = PG_GETARG_INT32(6);
    int32 result;

    result = p1 + p2 + p3 + p4 + p5 + p6 + p7;

    PG_RETURN_INT32(result);
}

/*
 *		vec_int4add_8		- add int4 with 8 args
 */

PG_FUNCTION_INFO_V1(vec_int4add_8);
Datum vec_int4add_8(PG_FUNCTION_ARGS)
{
    int32 p1 = PG_GETARG_INT32(0);
    int32 p2 = PG_GETARG_INT32(1);
    int32 p3 = PG_GETARG_INT32(2);
    int32 p4 = PG_GETARG_INT32(3);
    int32 p5 = PG_GETARG_INT32(4);
    int32 p6 = PG_GETARG_INT32(5);
    int32 p7 = PG_GETARG_INT32(6);
    int32 p8 = PG_GETARG_INT32(7);
    int32 result;

    result = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8;

    PG_RETURN_INT32(result);
}

/*
 *		vec_int4add_9	- add int4 with 9 args
 */

PG_FUNCTION_INFO_V1(vec_int4add_9);
Datum vec_int4add_9(PG_FUNCTION_ARGS)
{
    int32 p1 = PG_GETARG_INT32(0);
    int32 p2 = PG_GETARG_INT32(1);
    int32 p3 = PG_GETARG_INT32(2);
    int32 p4 = PG_GETARG_INT32(3);
    int32 p5 = PG_GETARG_INT32(4);
    int32 p6 = PG_GETARG_INT32(5);
    int32 p7 = PG_GETARG_INT32(6);
    int32 p8 = PG_GETARG_INT32(7);
    int32 p9 = PG_GETARG_INT32(8);
    int32 result;

    result = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9;

    PG_RETURN_INT32(result);
}

PG_FUNCTION_INFO_V1(vec_int4add_10);
Datum vec_int4add_10(PG_FUNCTION_ARGS)
{
    int32 p1 = PG_GETARG_INT32(0);
    int32 p2 = PG_GETARG_INT32(1);
    int32 p3 = PG_GETARG_INT32(2);
    int32 p4 = PG_GETARG_INT32(3);
    int32 p5 = PG_GETARG_INT32(4);
    int32 p6 = PG_GETARG_INT32(5);
    int32 p7 = PG_GETARG_INT32(6);
    int32 p8 = PG_GETARG_INT32(7);
    int32 p9 = PG_GETARG_INT32(8);
    int32 p10 = PG_GETARG_INT32(9);
    int32 result;

    result = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10;

    PG_RETURN_INT32(result);
}

PG_FUNCTION_INFO_V1(vec_int4add_11);
Datum vec_int4add_11(PG_FUNCTION_ARGS)
{
    int32 p1 = PG_GETARG_INT32(0);
    int32 p2 = PG_GETARG_INT32(1);
    int32 p3 = PG_GETARG_INT32(2);
    int32 p4 = PG_GETARG_INT32(3);
    int32 p5 = PG_GETARG_INT32(4);
    int32 p6 = PG_GETARG_INT32(5);
    int32 p7 = PG_GETARG_INT32(6);
    int32 p8 = PG_GETARG_INT32(7);
    int32 p9 = PG_GETARG_INT32(8);
    int32 p10 = PG_GETARG_INT32(9);
    int32 result;

    result = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10;

    PG_RETURN_INT32(result);
}

static void test_atomic_uint32(void)
{
    uint32 var;
    uint32 expected;
    int i;

    pg_atomic_init_u32(&var, 0);

    if (pg_atomic_read_u32(&var) != 0)
        (elog(ERROR, "atomic_read_u32() #1 wrong"));

    pg_atomic_write_u32(&var, 3);

    if (pg_atomic_read_u32(&var) != 3)
        (elog(ERROR, "atomic_read_u32() #2 wrong"));

    if (pg_atomic_fetch_add_u32(&var, 1) != 3)
        (elog(ERROR, "atomic_fetch_add_u32() #1 wrong"));

    if (pg_atomic_fetch_sub_u32(&var, 1) != 4)
        (elog(ERROR, "atomic_fetch_sub_u32() #1 wrong"));

    if (pg_atomic_sub_fetch_u32(&var, 3) != 0)
        (elog(ERROR, "atomic_sub_fetch_u32() #1 wrong"));

    if (pg_atomic_add_fetch_u32(&var, 10) != 10)
        (elog(ERROR, "atomic_add_fetch_u32() #1 wrong"));

    if (pg_atomic_exchange_u32(&var, 5) != 10)
        (elog(ERROR, "pg_atomic_exchange_u32() #1 wrong"));

    if (pg_atomic_exchange_u32(&var, 0) != 5)
        (elog(ERROR, "pg_atomic_exchange_u32() #0 wrong"));

    /* test around numerical limits */
    if (pg_atomic_fetch_add_u32(&var, INT_MAX) != 0)
        (elog(ERROR, "pg_atomic_fetch_add_u32() #2 wrong"));

    if (pg_atomic_fetch_add_u32(&var, INT_MAX) != INT_MAX)
        (elog(ERROR, "pg_atomic_add_fetch_u32() #3 wrong"));

    pg_atomic_fetch_add_u32(&var, 1); /* top up to UINT_MAX */

    if (pg_atomic_read_u32(&var) != UINT_MAX)
        (elog(ERROR, "atomic_read_u32() #2 wrong"));

    if (pg_atomic_fetch_sub_u32(&var, INT_MAX) != UINT_MAX)
        (elog(ERROR, "pg_atomic_fetch_sub_u32() #2 wrong"));

    if (pg_atomic_read_u32(&var) != (uint32)INT_MAX + 1)
        (elog(ERROR, "atomic_read_u32() #3 wrong: %u", pg_atomic_read_u32(&var)));

    expected = pg_atomic_sub_fetch_u32(&var, INT_MAX);
    if (expected != 1)
        (elog(ERROR, "pg_atomic_sub_fetch_u32() #3 wrong: %u", expected));

    pg_atomic_sub_fetch_u32(&var, 1);

    /* fail exchange because of old expected */
    expected = 10;
    if (pg_atomic_compare_exchange_u32(&var, &expected, 1))
        (elog(ERROR, "atomic_compare_exchange_u32() changed value spuriously"));

    /* CAS is allowed to fail due to interrupts, try a couple of times */
    for (i = 0; i < 1000; i++) {
        expected = 0;
        if (!pg_atomic_compare_exchange_u32(&var, &expected, 1))
            break;
    }
    if (i == 1000)
        (elog(ERROR, "atomic_compare_exchange_u32() never succeeded"));
    if (pg_atomic_read_u32(&var) != 1)
        (elog(ERROR, "atomic_compare_exchange_u32() didn't set value properly"));

    pg_atomic_write_u32(&var, 0);

    /* try setting flagbits */
    if (pg_atomic_fetch_or_u32(&var, 1) & 1)
        (elog(ERROR, "pg_atomic_fetch_or_u32() #1 wrong"));

    if (!(pg_atomic_fetch_or_u32(&var, 2) & 1))
        (elog(ERROR, "pg_atomic_fetch_or_u32() #2 wrong"));

    if (pg_atomic_read_u32(&var) != 3)
        (elog(ERROR, "invalid result after pg_atomic_fetch_or_u32()"));

    /* try clearing flagbits */
    if ((pg_atomic_fetch_and_u32(&var, ~2) & 3) != 3)
        (elog(ERROR, "pg_atomic_fetch_and_u32() #1 wrong"));

    if (pg_atomic_fetch_and_u32(&var, ~1) != 1)
        (elog(ERROR, "pg_atomic_fetch_and_u32() #2 wrong: is %u", pg_atomic_read_u32(&var)));
    /* no bits set anymore */
    if (pg_atomic_fetch_and_u32(&var, ~0) != 0)
        (elog(ERROR, "pg_atomic_fetch_and_u32() #3 wrong"));
}

static void test_atomic_uint64(void)
{
    uint64 var;
    uint64 expected;
    int i;

    pg_atomic_init_u64(&var, 0);

    if (pg_atomic_read_u64(&var) != 0)
        (elog(ERROR, "atomic_read_u64() #1 wrong"));

    pg_atomic_write_u64(&var, 3);

    if (pg_atomic_read_u64(&var) != 3)
        (elog(ERROR, "atomic_read_u64() #2 wrong"));

    if (pg_atomic_fetch_add_u64(&var, 1) != 3)
        (elog(ERROR, "atomic_fetch_add_u64() #1 wrong"));

    if (pg_atomic_fetch_sub_u64(&var, 1) != 4)
        (elog(ERROR, "atomic_fetch_sub_u64() #1 wrong"));

    if (pg_atomic_sub_fetch_u64(&var, 3) != 0)
        (elog(ERROR, "atomic_sub_fetch_u64() #1 wrong"));

    if (pg_atomic_add_fetch_u64(&var, 10) != 10)
        (elog(ERROR, "atomic_add_fetch_u64() #1 wrong"));

    if (pg_atomic_exchange_u64(&var, 5) != 10)
        (elog(ERROR, "pg_atomic_exchange_u64() #1 wrong"));

    if (pg_atomic_exchange_u64(&var, 0) != 5)
        (elog(ERROR, "pg_atomic_exchange_u64() #0 wrong"));

    /* fail exchange because of old expected */
    expected = 10;
    if (pg_atomic_compare_exchange_u64(&var, &expected, 1))
        (elog(ERROR, "atomic_compare_exchange_u64() changed value spuriously"));

    /* CAS is allowed to fail due to interrupts, try a couple of times */
    for (i = 0; i < 100; i++) {
        expected = 0;
        if (!pg_atomic_compare_exchange_u64(&var, &expected, 1))
            break;
    }
    if (i == 100)
        (elog(ERROR, "atomic_compare_exchange_u64() never succeeded"));
    if (pg_atomic_read_u64(&var) != 1)
        (elog(ERROR, "atomic_compare_exchange_u64() didn't set value properly"));

    pg_atomic_write_u64(&var, 0);

    /* try setting flagbits */
    if (pg_atomic_fetch_or_u64(&var, 1) & 1)
        (elog(ERROR, "pg_atomic_fetch_or_u64() #1 wrong"));

    if (!(pg_atomic_fetch_or_u64(&var, 2) & 1))
        (elog(ERROR, "pg_atomic_fetch_or_u64() #2 wrong"));

    if (pg_atomic_read_u64(&var) != 3)
        (elog(ERROR, "invalid result after pg_atomic_fetch_or_u64()"));

    /* try clearing flagbits */
    if ((pg_atomic_fetch_and_u64(&var, ~2) & 3) != 3)
        (elog(ERROR, "pg_atomic_fetch_and_u64() #1 wrong"));

    if (pg_atomic_fetch_and_u64(&var, ~1) != 1)
        (elog(ERROR, "pg_atomic_fetch_and_u64() #2 wrong: is " UINT64_FORMAT, pg_atomic_read_u64(&var)));
    /* no bits set anymore */
    if (pg_atomic_fetch_and_u64(&var, ~0) != 0)
        (elog(ERROR, "pg_atomic_fetch_and_u64() #3 wrong"));
}

extern "C" Datum test_atomic_ops(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(test_atomic_ops);
Datum test_atomic_ops(PG_FUNCTION_ARGS)
{

    test_atomic_uint32();

    test_atomic_uint64();

    PG_RETURN_BOOL(true);
}

Datum funcA(PG_FUNCTION_ARGS)
{
    StringInfoData si;
    initStringInfo(&si);

    appendStringInfo(&si, "funcA");

    PG_RETURN_TEXT_P(cstring_to_text(pstrdup(si.data)));
}

Datum funcB(PG_FUNCTION_ARGS)
{
    StringInfoData si;
    initStringInfo(&si);

    appendStringInfo(&si, "funcB");

    PG_RETURN_TEXT_P(cstring_to_text(pstrdup(si.data)));
}

Datum funcC(PG_FUNCTION_ARGS)
{
    StringInfoData si;
    initStringInfo(&si);

    appendStringInfo(&si, "funcC");

    PG_RETURN_TEXT_P(cstring_to_text(pstrdup(si.data)));
}

/***************************UDF CREM**************************/

/*½«½Å±¾ÖÐÖØ¸´µÄÓï¾ä¶¨ÒåÎªºê */
#define if_judeg()                                                                               \
    if (((PRODUCT[0] == 'P' && PRODUCT[1] == 'M') && p > 3 * Multiplier_J && length == 2) ||     \
        ((PRODUCT[0] == 'P' && (PRODUCT[1] == 'O' || PRODUCT[1] == 'A' || PRODUCT[1] == 'C')) && \
            p > 2 * Multiplier_J && length == 2))

static int64 to_int64(Datum dm, int64 ml)
{
    Datum ml_n = DirectFunctionCall1(int8_numeric, Int64GetDatum(ml));
    Datum cj_n = DirectFunctionCall2(numeric_mul, ml_n, dm);
    Datum int_n = DirectFunctionCall1(numeric_int8, cj_n);

    return DatumGetInt64(int_n);
}

#define TmpVar -999999999
/* logic here is the same as C, only used for decimal(18,4) */

Datum truncInt1(PG_FUNCTION_ARGS)
{
    int inputInteger = PG_GETARG_INT32(0);

    PG_RETURN_INT32(inputInteger);
}

Datum truncInt(PG_FUNCTION_ARGS)
{
    int64 V0 = 0;
    int64 VR = 0;
    int64 P0 = 0;
    int64 M = 0;

    // check for NULL Value
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    V0 = (int64)PG_GETARG_INT32(0);
    // if Places is zero or NULL, then nothing to do, exit
    if (PG_ARGISNULL(1) || (M = (int64)PG_GETARG_INT32(1)) > 0)
        PG_RETURN_INT32((int32)V0);

    // calculate the truncating
    P0 = pow(10, -M);
    VR = (V0 / P0) * P0;

    PG_RETURN_INT32((int32)VR);
}

Datum truncFloat(PG_FUNCTION_ARGS)
{
    float arg1 = 0;
    double V0 = 0;
    double V1 = 0;
    double V2 = 0;
    double V3 = 0;
    double VR = 0;
    long P0 = 0;
    long M = 0;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    arg1 = PG_GETARG_FLOAT4(0);
    V0 = (double)arg1;
    if (!PG_ARGISNULL(1))
        M = PG_GETARG_INT32(1);
    if (M >= 0)  // Positive POWER Case
    {
        // calculate the truncating
        P0 = pow(10, M);
        V3 = V0 * P0;
        V1 = modf(V3, &V2);
        VR = V2 / P0;
    } else  // Negative POWER case
    {
        // calculate the truncating
        P0 = pow(10, -M);
        V2 = modf((V0 / P0), &V1);
        VR = V1 * P0;
    }

    PG_RETURN_FLOAT8(VR);
}

Datum truncDec81(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall2(numeric_trunc, PG_GETARG_DATUM(0), Int32GetDatum(0));
}

Datum truncDec8(PG_FUNCTION_ARGS)
{
    int scale = 0;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (!PG_ARGISNULL(1))
        scale = PG_GETARG_INT32(1);

    if (scale >= 5)
        return PG_GETARG_DATUM(0);  //Èç¹û½ØÈ¡µÄÊýÖµ´óÓÚ5£¬Ö±½Ó¾ÍÔ­Öµµ¼³ö

    return DirectFunctionCall2(numeric_trunc, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
}

int is_separator(char a, const int sepaNums, const char* sepa_char)
{
    int i = 0;

    for (; i < sepaNums; i++) {
        if (a == sepa_char[i])
            return 1;
    }

    return 0;
}

/**
 * ´Ó×Ö·û´®ÖÐ»ñµÃÊý×Ö
 */
int get_int_from_str(char* source, int len)
{
    int i = 0;
    int value = 0;

    for (i = 0; i < len; i++) {
        if (source[i] < '0' || source[i] > '9')
            return -1;
        value = value * 10 + (source[i] - '0');
    }

    return value;
}

Datum TransTimestamp(PG_FUNCTION_ARGS)
{
    VarChar* source_v = NULL;
    uint32 len;
    char* source = NULL;
    char* result = NULL;
    Datum result_v;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int min = 0;
    int sec = 0;
    const int sepaNums = 5;
    const char sepa_char[] = {'.', '/', '-', ':', ' '};

#define IS_SEPA(a) is_separator(a, sepaNums, sepa_char)

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    source_v = PG_GETARG_BYTEA_PP(0);
    source = VARDATA_ANY(source_v);
    len = PG_GETARG_INT32(1);

    if (len > VARSIZE_ANY_EXHDR(source_v)) {
        PG_FREE_IF_COPY(source_v, 0);
        return PG_GETARG_DATUM(2);
    }

    if (len == 19) {
        //'2010-01-02 23:59:59'
        //ÅÐ¶Ï·Ö¸ô·ûÊÇ·ñÕýÈ·
        if (IS_SEPA(source[4]) && IS_SEPA(source[7]) && IS_SEPA(source[10]) && IS_SEPA(source[13]) &&
            IS_SEPA(source[16])) {
            year = get_int_from_str(source, 4);
            month = get_int_from_str(source + 5, 2);
            day = get_int_from_str(source + 8, 2);
            hour = get_int_from_str(source + 11, 2);
            min = get_int_from_str(source + 14, 2);
            sec = get_int_from_str(source + 17, 2);
        }
    } else if (len == 14) {
        //'20100102235959'
        year = get_int_from_str(source, 4);
        month = get_int_from_str(source + 4, 2);
        day = get_int_from_str(source + 6, 2);
        hour = get_int_from_str(source + 8, 2);
        min = get_int_from_str(source + 10, 2);
        sec = get_int_from_str(source + 12, 2);
    }

    //²¹È«Äê·Ý
    if (year >= 0 && year <= 20)
        year += 2000;
    else if (year > 20 && year <= 99)
        year += 1900;

    //ÅÐ¶ÏÈÕÆÚµÄºÏ·¨ÐÔ
    int valid = 1;
    if (year <= 0)
        valid = -1;
    if (month < 1 || month > 12)
        valid = -1;
    if (day < 1 || day > 31)
        valid = -1;
    if (hour < 0 || hour > 23)
        valid = -1;
    if (min < 0 || min > 59)
        valid = -1;
    if (sec < 0 || sec > 59)
        valid = -1;

    //Ð¡ÔÂ
    if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30)
        valid = -1;

    //¶þÔÂ
    if (month == 2) {
        if (((year % 4 != 0) || (year % 4 == 0 && year % 100 == 0 && year % 400 != 0)) && day > 28)
            valid = -1;
        else if (day > 29)
            valid = -1;
    }

    if (valid == -1) {
        PG_FREE_IF_COPY(source_v, 0);
        return PG_GETARG_DATUM(2);
    }

    result = (char*)palloc0(21);
    sprintf(result, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec);
    result_v = DirectFunctionCall3(varcharin, PointerGetDatum(result), UInt32GetDatum(0), Int32GetDatum(-1));

    PG_FREE_IF_COPY(source_v, 0);
    pfree(result);

    return result_v;
}

//×Ö·û´®¸ñÊ½ÀàÐÍ
#define DATE_STR_ERR -1    //´íÎóµÄÈÕÆÚÀàÐÍ
#define DATE_STR_TYPE_1 1  //'yyyy-mm-dd' 'yyyy/mm/dd' 'yyyy.mm.dd'
#define DATE_STR_TYPE_2 2  //'yyyymmdd'
#define DATE_STR_TYPE_3 3  //'yy-mm-dd' »ò 'yy/mm/dd' »ò 'yy.mm.dd'

/**
 * ½«Ö¸¶¨¸ñÊ½µÄÈÕÆÚ×Ö·û´®×ª»»ÎªÕûÊý£¬Èç¹ûÈÕÆÚ·Ç·¨£¬·µ»Ø-1
 */
int convert_to_int(char* source, int date_str_type)
{
    int year = 0;
    int month = 0;
    int day = 0;
    int i = 0;
    if (date_str_type == DATE_STR_TYPE_1) {
        //'2010-01-02'
        for (i = 0; i < 4; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            year = year * 10 + (source[i] - '0');
        }

        for (i = 5; i < 7; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            month = month * 10 + (source[i] - '0');
        }

        for (i = 8; i < 10; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            day = day * 10 + (source[i] - '0');
        }
    } else if (date_str_type == DATE_STR_TYPE_2) {
        //'yyyymmdd'
        for (i = 0; i < 4; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            year = year * 10 + (source[i] - '0');
        }
        for (i = 4; i < 6; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            month = month * 10 + (source[i] - '0');
        }
        for (i = 6; i < 8; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            day = day * 10 + (source[i] - '0');
        }
    } else if (date_str_type == DATE_STR_TYPE_3) {
        //'yy-mm-dd'
        for (i = 0; i < 2; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            year = year * 10 + (source[i] - '0');
        }
        for (i = 3; i < 5; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            month = month * 10 + (source[i] - '0');
        }
        for (i = 6; i < 8; i++) {
            if (source[i] < '0' || source[i] > '9')
                return -1;
            day = day * 10 + (source[i] - '0');
        }
    } else {
        return -1;
    }

    //²¹È«Äê·Ý
    if (year >= 0 && year <= 20)
        year += 2000;
    else if (year > 20 && year <= 99)
        year += 1900;
    // if(year < 100 && date_str_type == DATE_STR_TYPE_3) year += 1900;
    if (year == 0)
        return -1;

    // if(year < 1949) return -1;
    // if(year > 2049) return -1;
    if (month < 1 || month > 12)
        return -1;
    if (day < 1 || day > 31)
        return -1;

    //Ð¡ÔÂ
    if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30)
        return -1;

    //¶þÔÂ
    if (month == 2) {
        if (((year % 4 != 0) || (year % 4 == 0 && year % 100 == 0 && year % 400 != 0)) && day > 28)
            return -1;
        else if (day > 29)
            return -1;
    }

    return year * 10000 + month * 100 + day;
}

/**
 * ÅÐ¶ÏÊäÈë´®µÄ¸ñÊ½ÀàÐÍ£¬Ö»Í¨¹ý·Ö¸ô·ûÅÐ¶Ï
 */
int check_date_string(char* source, int len)
{
    //·Ö¸ô·û
    const int sepNums = 3;
    const char separator_char[] = {'.', '/', '-'};

#define IS_SEPA_C(a) is_separator(a, sepNums, separator_char)

    if (len == 8) {
        if (IS_SEPA_C(source[2]) && IS_SEPA_C(source[5]))
            return DATE_STR_TYPE_3;
        else
            return DATE_STR_TYPE_2;
    }

    if (len == 10) {
        if (IS_SEPA_C(source[4]) && IS_SEPA_C(source[7]))
            return DATE_STR_TYPE_1;
        else
            return DATE_STR_ERR;
    }

    return DATE_STR_ERR;
}

Datum TransDate(PG_FUNCTION_ARGS)
{
    VarChar* source_v = NULL;
    uint32 len;
    char* source = NULL;
    char* result = NULL;
    Datum result_v;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    source_v = PG_GETARG_BYTEA_PP(0);
    source = VARDATA_ANY(source_v);
    len = PG_GETARG_INT32(1);

    if (len > VARSIZE_ANY_EXHDR(source_v)) {
        PG_FREE_IF_COPY(source_v, 0);
        return PG_GETARG_DATUM(2);
    }

    //»ñÈ¡×Ö·û´®µÄ¸ñÊ½ÀàÐÍ
    int date_str_type = check_date_string(source, len);
    if (date_str_type < 0) {
        PG_FREE_IF_COPY(source_v, 0);
        return PG_GETARG_DATUM(2);
    }

    int date_int = convert_to_int(source, date_str_type);
    if (date_int < 0) {
        PG_FREE_IF_COPY(source_v, 0);
        return PG_GETARG_DATUM(2);
    }

    result = (char*)palloc0(21);
    sprintf(result, "%08d", date_int);
    result_v = DirectFunctionCall3(varcharin, PointerGetDatum(result), UInt32GetDatum(0), Int32GetDatum(-1));
    PG_FREE_IF_COPY(source_v, 0);
    pfree(result);

    return result_v;
}

Datum signf(PG_FUNCTION_ARGS)
{
    float4 inputFlt;
    int result;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    inputFlt = PG_GETARG_FLOAT4(0);
    result = (inputFlt > 0 ? 1 : (inputFlt < 0 ? -1 : 0));

    PG_RETURN_INT32(result);
}

Datum signi(PG_FUNCTION_ARGS)
{
    float4 inputInt;
    int result;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    inputInt = PG_GETARG_INT32(0);
    result = (inputInt > 0 ? 1 : (inputInt < 0 ? -1 : 0));

    PG_RETURN_INT32(result);
}

Datum lpad_f(PG_FUNCTION_ARGS)
{
    Datum num;

    num = DirectFunctionCall2(numeric_trunc, PG_GETARG_DATUM(1), Int32GetDatum(0));
    num = DirectFunctionCall1(numeric_int4, num);

    return DirectFunctionCall3(lpad, PG_GETARG_DATUM(0), num, PG_GETARG_DATUM(2));
}

Datum rpad_f(PG_FUNCTION_ARGS)
{
    Datum num;

    num = DirectFunctionCall2(numeric_trunc, PG_GETARG_DATUM(1), Int32GetDatum(0));
    num = DirectFunctionCall1(numeric_int4, num);

    return DirectFunctionCall3(rpad, PG_GETARG_DATUM(0), num, PG_GETARG_DATUM(2));
}

Datum RoundInt(PG_FUNCTION_ARGS)
{
    int64 V0 = 0;
    int64 V2 = 0;
    int64 V3 = 0;
    int64 VR = 0;
    int64 P0 = 0;
    int64 M = 0;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    V0 = PG_GETARG_INT32(0);
    if (PG_ARGISNULL(1) || (M = PG_GETARG_INT32(1)) > 0)
        PG_RETURN_INT32((int32)V0);

    P0 = pow(10, -M);
    V2 = (V0 / P0) * P0;
    V3 = V0 - V2;

    VR = V2;
    if (labs(V3) >= (0.5 * P0)) {
        if (V2 < 0)
            P0 = -P0;
        VR = V2 + P0;
    }

    PG_RETURN_INT32((int32)VR);
}

Datum RoundFloat(PG_FUNCTION_ARGS)
{
    float8 V0;
    float8 V1;
    float8 V2;
    float8 V3;
    float8 VR;
    int64 P0;
    int64 M = 0;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    V0 = PG_GETARG_FLOAT4(0);
    if (!PG_ARGISNULL(1))
        M = PG_GETARG_INT32(1);

    if (M >= 0)  // Positive POWER Case
    {
        // calculate the rounding
        P0 = pow(10, M);
        V3 = V0 * P0;
        V1 = modf(V3, &V2);
        V2 = V2 / P0;
        V3 = V3 / P0;
        V3 = V0 - V2;
        VR = V2;
        // here we round up if >= 0.5 of that scale
        if (fabs(V3) >= (0.499999999999 / P0)) {
            if (V2 < 0)
                P0 = -P0;
            VR = V2 + ((float)1 / P0);
        }
    } else  // Negative POWER case
    {
        // calculate the rounding
        P0 = pow(10, -M);
        V2 = modf((V0 / P0), &V1);
        V2 = V1 * P0;
        V3 = V0 - V2;
        VR = V2;
        // here we round up if >= 0.5 of that scale
        if (fabs(V3) >= (0.5 * P0)) {
            if (V2 < 0)
                P0 = -P0;
            VR = V2 + P0;
        }
    }

    PG_RETURN_FLOAT8(VR);
}

Datum normsdist(PG_FUNCTION_ARGS)
{
    int64 I0;
    int64 I0_abs;
    int64 mid;
    int64 base_value;
    int64 min_x_value;
    int64 max_pro_value;
    int64 min_pro_value;

    float8 normvalue;
    float8 gamma;
    float8 mid_value;
    float8 a1;
    float8 a2;
    float8 a3;
    float8 a4;
    float8 a5;
    float8 k;
    float8 n;
    float8 result;

    int64 Multiplier = 1000000000000000;

    gamma = 0.2316419;
    a1 = 0.31938153;
    a2 = -0.356563782;
    a3 = 1.781477973;
    a4 = -1.821255978;
    a5 = 1.330274429;
    k = 0;
    n = 0;
    base_value = 0.5 * Multiplier;
    min_x_value = 10 * Multiplier;
    max_pro_value = 0.999999999 * Multiplier;
    min_pro_value = 0.000000001 * Multiplier;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    I0 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    // È¡´úÔ­ÏÈµÄabs(I0)
    I0_abs = I0 < 0 ? -I0 : I0;
    if (I0 == 0) {
        result = (float8)base_value / Multiplier;
        return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
    };

    if (I0 >= min_x_value) {
        result = (float8)max_pro_value / Multiplier;
        return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
    }

    if (I0_abs >= min_x_value) {
        result = (float8)min_pro_value / Multiplier;
        return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
    }

    k = (float8)Multiplier / (Multiplier + I0_abs * gamma);
    n = (float8)k * (a1 + k * (a2 + k * (a3 + k * (a4 + k * a5))));

    mid_value = (float8)I0 / Multiplier;  // modify 20130506
    mid_value = mid_value * mid_value;
    mid_value = exp(-mid_value / 2);
    normvalue = (1 / sqrt(2 * 3.1415926)) * mid_value;
    n = 1 - normvalue * n;

    // for test start
    // mid = n*Multiplier;
    // memcpy((BYTE *) result, &mid, 8);
    // r/eturn;
    // for test end

    if (I0 < 0)
        mid = (1 - n) * Multiplier;
    else
        mid = n * Multiplier;
    result = (float8)mid / Multiplier;
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
}

#define LastDay(y, m) (((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) ? LastDaysL[m] : LastDays[m])

Datum months_between_dd(PG_FUNCTION_ARGS)
{
    DateADT date1 = PG_GETARG_DATEADT(0);
    DateADT date2 = PG_GETARG_DATEADT(1);
    struct pg_tm tm1;
    struct pg_tm tm2;
    short LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    short LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    float result;

    j2date(date1 + POSTGRES_EPOCH_JDATE, &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday);
    j2date(date2 + POSTGRES_EPOCH_JDATE, &tm2.tm_year, &tm2.tm_mon, &tm2.tm_mday);
    tm1.tm_mon = tm1.tm_mon - 1;
    tm2.tm_mon = tm2.tm_mon - 1;

    if (tm1.tm_mday > 27 && tm2.tm_mday > 27 && tm1.tm_mday == LastDay(tm1.tm_year, tm1.tm_mon) &&
        tm2.tm_mday == LastDay(tm2.tm_year, tm2.tm_mon))
        result = (tm1.tm_year * 12 + tm1.tm_mon) - (tm2.tm_year * 12 + tm2.tm_mon);
    else
        result =
            (((tm1.tm_year * 12 + tm1.tm_mon) - (tm2.tm_year * 12 + tm2.tm_mon)) * 31 + tm1.tm_mday - tm2.tm_mday) /
            31.0;

    PG_RETURN_FLOAT4(result);
}

Datum months_between_dt(PG_FUNCTION_ARGS)
{
    DateADT date1 = PG_GETARG_DATEADT(0);
    Timestamp date2 = PG_GETARG_TIMESTAMP(1);
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int Year1, Month1, Months1, Day1;
    int Year2, Month2, Months2, Day2;
    float result;

    j2date(date1 + POSTGRES_EPOCH_JDATE, &Year1, &Month1, &Day1);
    Month1 = Month1 - 1;

    timestamp2tm(date2, NULL, tm, &fsec, NULL, NULL);
    Year2 = tm->tm_year;
    Month2 = tm->tm_mon - 1;
    Day2 = tm->tm_mday;

    Months1 = Year1 * 12 + Month1;
    Months2 = Year2 * 12 + Month2;

    if (Day1 > 27 && Day2 > 27 && Day1 == LastDay(Year1, Month1) && Day2 == LastDay(Year2, Month2))
        result = Months1 - Months2;
    else
        result = ((Months1 - Months2) * 31 + Day1 - Day2) / 31.0;

    PG_RETURN_FLOAT4(result);
}

Datum months_between_td(PG_FUNCTION_ARGS)
{
    Timestamp date1 = PG_GETARG_TIMESTAMP(0);
    DateADT date2 = PG_GETARG_DATEADT(1);
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int Year1, Month1, Months1, Day1;
    int Year2, Month2, Months2, Day2;
    float result;

    timestamp2tm(date1, NULL, tm, &fsec, NULL, NULL);
    Year1 = tm->tm_year;
    Month1 = tm->tm_mon - 1;
    Day1 = tm->tm_mday;
    j2date(date2 + POSTGRES_EPOCH_JDATE, &Year2, &Month2, &Day2);
    Month2 = Month2 - 1;

    Months1 = Year1 * 12 + Month1;
    Months2 = Year2 * 12 + Month2;

    if (Day1 > 27 && Day2 > 27 && Day1 == LastDay(Year1, Month1) && Day2 == LastDay(Year2, Month2))
        result = Months1 - Months2;
    else
        result = ((Months1 - Months2) * 31 + Day1 - Day2) / 31.0;

    PG_RETURN_FLOAT4(result);
}

Datum months_between_tt(PG_FUNCTION_ARGS)
{
    Timestamp date1 = PG_GETARG_TIMESTAMP(0);
    Timestamp date2 = PG_GETARG_TIMESTAMP(1);
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int Year1, Month1, Months1, Day1;
    int Year2, Month2, Months2, Day2;
    float result;

    timestamp2tm(date1, NULL, tm, &fsec, NULL, NULL);
    Year1 = tm->tm_year;
    Month1 = tm->tm_mon - 1;
    Day1 = tm->tm_mday;
    timestamp2tm(date2, NULL, tm, &fsec, NULL, NULL);
    Year2 = tm->tm_year;
    Month2 = tm->tm_mon - 1;
    Day2 = tm->tm_mday;

    Months1 = Year1 * 12 + Month1;
    Months2 = Year2 * 12 + Month2;

    if (Day1 > 27 && Day2 > 27 && Day1 == LastDay(Year1, Month1) && Day2 == LastDay(Year2, Month2))
        result = Months1 - Months2;
    else
        result = ((Months1 - Months2) * 31 + Day1 - Day2) / 31.0;

    PG_RETURN_FLOAT4(result);
}

Datum last_day_d(PG_FUNCTION_ARGS)
{
    DateADT inDate = PG_GETARG_DATEADT(0);
    short LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    short LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int syear;
    int smonth;
    int sday;
    j2date(inDate + POSTGRES_EPOCH_JDATE, &syear, &smonth, &sday);
    sday = LastDay(syear, smonth - 1);

    PG_RETURN_DATEADT(date2j(syear, smonth, sday) - POSTGRES_EPOCH_JDATE);
}

Datum last_day_t(PG_FUNCTION_ARGS)
{
    Timestamp inTimestamp = PG_GETARG_TIMESTAMP(0);
    Timestamp result;
    int tz = 0;
    short LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    short LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;

    if (timestamp2tm(inTimestamp, NULL, tm, &fsec, NULL, NULL) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    tm->tm_mday = LastDay(tm->tm_year, tm->tm_mon - 1);

    if (tm2timestamp(tm, fsec, &tz, &result) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

Datum last_day_tz(PG_FUNCTION_ARGS)
{
    TimestampTz inTimestampTz = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz result;
    int tz;
    short LastDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    short LastDaysL[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;

    if (timestamp2tm(inTimestampTz, &tz, tm, &fsec, NULL, NULL) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
    tm->tm_mday = LastDay(tm->tm_year, tm->tm_mon - 1);

    if (tm2timestamp(tm, fsec, &tz, &result) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

/* FUNC_III_CS0507_3 */
Datum FUNC_III_CS0507_3(PG_FUNCTION_ARGS)
{
    text* result = NULL;
    StringInfo buf = NULL;
    int64 p1;
    int64 Multiplier = 10000;

    buf = makeStringInfo();

    if (PG_ARGISNULL(0)) {
        appendBinaryStringInfo(buf, "030", strlen("030"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else {
        p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    }

    if (p1 > 0 && p1 <= 30 * Multiplier) {
        appendBinaryStringInfo(buf, "021", strlen("021"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else if (p1 > 30 * Multiplier && p1 <= 50 * Multiplier) {
        appendBinaryStringInfo(buf, "022", strlen("022"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else if (p1 > 50 * Multiplier && p1 <= 60 * Multiplier) {
        appendBinaryStringInfo(buf, "023", strlen("023"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }

    else if (p1 > 60 * Multiplier && p1 <= 70 * Multiplier) {
        appendBinaryStringInfo(buf, "024", strlen("024"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }

    else if (p1 > 70 * Multiplier && p1 <= 80 * Multiplier) {
        appendBinaryStringInfo(buf, "025", strlen("025"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }

    else if (p1 > 80 * Multiplier && p1 <= 90 * Multiplier) {
        appendBinaryStringInfo(buf, "026", strlen("026"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }

    else {
        appendBinaryStringInfo(buf, "027", strlen("027"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }
}

/* FUNC_III_CS0507 */
Datum FUNC_III_CS0507(PG_FUNCTION_ARGS)
{
    text* result = NULL;
    StringInfo buf = NULL;

    int64 p1;
    int64 Multiplier = 10000;

    buf = makeStringInfo();
    if (PG_ARGISNULL(0)) {
        appendBinaryStringInfo(buf, "004", strlen("004"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else {
        p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    }

    if (p1 < 12 * Multiplier) {
        appendBinaryStringInfo(buf, "001", strlen("001"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else if (p1 >= 12 * Multiplier && p1 <= 35 * Multiplier) {
        appendBinaryStringInfo(buf, "002", strlen("002"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else if (p1 >= 36 * Multiplier && p1 <= 59 * Multiplier) {
        appendBinaryStringInfo(buf, "003", strlen("003"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else {
        appendBinaryStringInfo(buf, "004", strlen("004"));
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    }
}

/* FUNC_II_JUDGE_DF_AGE */
Datum FUNC_II_JUDGE_DF_AGE(PG_FUNCTION_ARGS)
{
    int64 p1;
    int64 p2;
    int64 p3;
    int64 p4;
    int64 p5;
    int64 p6;
    int64 p7;
    int64 p8;
    int64 p9;
    int64 p10;
    int64 p11;
    int64 p12;
    int64 Multiplier = 10000;

    int p;

    // ¡ä|¨¤¨ª¨¨?2??aNULl¦Ì??¨¦??
    if (PG_ARGISNULL(0)) {
        p1 = -1 * Multiplier;
    } else
        p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);

    if (PG_ARGISNULL(1)) {
        p2 = -1 * Multiplier;
    } else
        p2 = to_int64(PG_GETARG_DATUM(1), Multiplier);

    if (PG_ARGISNULL(2)) {
        p3 = -1 * Multiplier;
    } else
        p3 = to_int64(PG_GETARG_DATUM(2), Multiplier);

    if (PG_ARGISNULL(3)) {
        p4 = -1 * Multiplier;
    } else
        p4 = to_int64(PG_GETARG_DATUM(3), Multiplier);

    if (PG_ARGISNULL(4)) {
        p5 = -1 * Multiplier;
    } else
        p5 = to_int64(PG_GETARG_DATUM(4), Multiplier);

    if (PG_ARGISNULL(5)) {
        p6 = -1 * Multiplier;
    } else
        p6 = to_int64(PG_GETARG_DATUM(5), Multiplier);

    if (PG_ARGISNULL(6)) {
        p7 = -1 * Multiplier;
    } else
        p7 = to_int64(PG_GETARG_DATUM(6), Multiplier);

    if (PG_ARGISNULL(7)) {
        p8 = -1 * Multiplier;
    } else
        p8 = to_int64(PG_GETARG_DATUM(7), Multiplier);

    if (PG_ARGISNULL(8)) {
        p9 = -1 * Multiplier;
    } else
        p9 = to_int64(PG_GETARG_DATUM(8), Multiplier);

    if (PG_ARGISNULL(9)) {
        p10 = -1 * Multiplier;
    } else
        p10 = to_int64(PG_GETARG_DATUM(9), Multiplier);

    if (PG_ARGISNULL(10)) {
        p11 = -1 * Multiplier;
    } else
        p11 = to_int64(PG_GETARG_DATUM(10), Multiplier);

    if (PG_ARGISNULL(11)) {
        p12 = -1 * Multiplier;
    } else
        p12 = to_int64(PG_GETARG_DATUM(11), Multiplier);

    if (p1 == 0) {
        p = 0;
        PG_RETURN_INT32(p);
    } else if (p2 == 0 && p1 == 1 * Multiplier) {
        p = 1;
        PG_RETURN_INT32(p);
    } else if (p3 == 0 && p2 == 1 * Multiplier) {
        p = 2;
        PG_RETURN_INT32(p);
    } else if (p4 == 0 && p3 == 1 * Multiplier) {
        p = 3;
        PG_RETURN_INT32(p);
    } else if (p5 == 0 && p4 == 1 * Multiplier) {
        p = 4;
        PG_RETURN_INT32(p);
    } else if (p6 == 0 && p5 == 1 * Multiplier) {
        p = 5;
        PG_RETURN_INT32(p);
    } else if (p7 == 0 && p6 == 1 * Multiplier) {
        p = 6;
        PG_RETURN_INT32(p);
    } else if (p8 == 0 && p7 == 1 * Multiplier) {
        p = 7;
        PG_RETURN_INT32(p);
    } else if (p9 == 0 && p8 == 1 * Multiplier) {
        p = 8;
        PG_RETURN_INT32(p);
    } else if (p10 == 0 && p9 == 1 * Multiplier) {
        p = 9;
        PG_RETURN_INT32(p);
    } else if (p11 == 0 && p10 == 1 * Multiplier) {
        p = 10;
        PG_RETURN_INT32(p);
    } else if (p12 == 0 && p11 == 1 * Multiplier) {
        p = 11;
        PG_RETURN_INT32(p);
    } else {
        p = 99;
        PG_RETURN_INT32(p);
    }
}

/* FUNC_TRANS_MOBCODE */
Datum FUNC_TRANS_MOBCODE(PG_FUNCTION_ARGS)
{
    int64 p1;
    text* result = NULL;
    StringInfo buf = NULL;
    int64 Multiplier = 10000;

    buf = makeStringInfo();
    if (PG_ARGISNULL(0)) {
        appendBinaryStringInfo(buf, "001", 3);
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else
        p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);

    if (p1 == Multiplier) {
        appendBinaryStringInfo(buf, "001", 3);
    } else if (p1 == 2 * Multiplier) {
        appendBinaryStringInfo(buf, "002", 3);
    } else if (p1 == 3 * Multiplier) {
        appendBinaryStringInfo(buf, "003", 3);
    } else if (p1 == 4 * Multiplier) {
        appendBinaryStringInfo(buf, "004", 3);
    } else if (p1 == 5 * Multiplier) {
        appendBinaryStringInfo(buf, "005", 3);
    } else if (p1 == 6 * Multiplier) {
        appendBinaryStringInfo(buf, "006", 3);
    } else if (p1 == 7 * Multiplier) {
        appendBinaryStringInfo(buf, "007", 3);
    } else if (p1 == 8 * Multiplier) {
        appendBinaryStringInfo(buf, "008", 3);
    } else if (p1 == 9 * Multiplier) {
        appendBinaryStringInfo(buf, "009", 3);
    } else if (p1 == 10 * Multiplier) {
        appendBinaryStringInfo(buf, "010", 3);
    } else if (p1 == 11 * Multiplier) {
        appendBinaryStringInfo(buf, "011", 3);
    } else if (p1 == 12 * Multiplier) {
        appendBinaryStringInfo(buf, "012", 3);
    } else {
        appendBinaryStringInfo(buf, "013", 3);
    }

    result = cstring_to_text(buf->data);
    pfree(buf->data);
    pfree(buf);
    PG_RETURN_TEXT_P(result);
}

/* FUNC_TRANS_RISKCODE */
Datum FUNC_TRANS_RISKCODE(PG_FUNCTION_ARGS)
{
    int64 p1;
    StringInfo buf = NULL;
    text* result = NULL;
    char* p_i_cardtype = NULL;
    int64 Multiplier = 10000;

    buf = makeStringInfo();
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        appendBinaryStringInfo(buf, "014", 3);
        result = cstring_to_text(buf->data);
        pfree(buf->data);
        pfree(buf);
        PG_RETURN_TEXT_P(result);
    } else {
        p_i_cardtype = text_to_cstring(PG_GETARG_TEXT_PP(0));
        p1 = to_int64(PG_GETARG_DATUM(1), Multiplier);
    }

    // if( ( p_i_cardtype[0] == 'C' || p_i_cardtype[0] == 'I' ) && p_i_cardtype[1] == 'C' )
    if (strcmp((const char*)p_i_cardtype, (const char*)"CC") == 0 ||
        strcmp((const char*)p_i_cardtype, (const char*)"IC") == 0) {
        if (p1 == 0) {
            appendBinaryStringInfo(buf, "015", 3);
        } else if (p1 == Multiplier) {
            appendBinaryStringInfo(buf, "001", 3);
        } else if (p1 == 2 * Multiplier) {
            appendBinaryStringInfo(buf, "002", 3);
        } else if (p1 == 3 * Multiplier) {
            appendBinaryStringInfo(buf, "003", 3);
        } else if (p1 == 4 * Multiplier) {
            appendBinaryStringInfo(buf, "004", 3);
        } else if (p1 == 5 * Multiplier) {
            appendBinaryStringInfo(buf, "005", 3);
        } else if (p1 == 6 * Multiplier) {
            appendBinaryStringInfo(buf, "006", 3);
        } else if (p1 == 7 * Multiplier) {
            appendBinaryStringInfo(buf, "007", 3);
        } else if (p1 == 8 * Multiplier) {
            appendBinaryStringInfo(buf, "008", 3);
        } else if (p1 == 9 * Multiplier) {
            appendBinaryStringInfo(buf, "009", 3);
        } else if (p1 == 10 * Multiplier) {
            appendBinaryStringInfo(buf, "010", 3);
        } else if (p1 == 11 * Multiplier) {
            appendBinaryStringInfo(buf, "011", 3);
        } else if (p1 == 12 * Multiplier) {
            appendBinaryStringInfo(buf, "012", 3);
        } else {
            appendBinaryStringInfo(buf, "013", 3);
        }
    } else {
        if (p1 == 0) {
            appendBinaryStringInfo(buf, "015", 3);
        } else if (p1 == Multiplier) {
            appendBinaryStringInfo(buf, "016", 3);
        } else if (p1 == 2 * Multiplier) {
            appendBinaryStringInfo(buf, "001", 3);
        } else if (p1 == 3 * Multiplier) {
            appendBinaryStringInfo(buf, "002", 3);
        } else if (p1 == 4 * Multiplier) {
            appendBinaryStringInfo(buf, "003", 3);
        } else if (p1 == 5 * Multiplier) {
            appendBinaryStringInfo(buf, "004", 3);
        } else if (p1 == 6 * Multiplier) {
            appendBinaryStringInfo(buf, "005", 3);
        } else if (p1 == 7 * Multiplier) {
            appendBinaryStringInfo(buf, "006", 3);
        } else if (p1 == 8 * Multiplier) {
            appendBinaryStringInfo(buf, "007", 3);
        } else if (p1 == 9 * Multiplier) {
            appendBinaryStringInfo(buf, "008", 3);
        } else if (p1 == 10 * Multiplier) {
            appendBinaryStringInfo(buf, "009", 3);
        } else if (p1 == 11 * Multiplier) {
            appendBinaryStringInfo(buf, "010", 3);
        } else if (p1 == 12 * Multiplier) {
            appendBinaryStringInfo(buf, "011", 3);
        } else {
            appendBinaryStringInfo(buf, "013", 3);
        }
    }

    result = cstring_to_text(buf->data);
    pfree(buf->data);
    pfree(buf);
    pfree(p_i_cardtype);
    PG_RETURN_TEXT_P(result);
}

/* FUNC_XW_FIX_DPD */
Datum FUNC_XW_FIX_DPD(PG_FUNCTION_ARGS)
{

    int64 p1;
    char* var1 = NULL;
    int result;
    int64 Multiplier = 10000;

    if (PG_ARGISNULL(0)) {
        result = 2;
        PG_RETURN_INT32(result);
    } else
        var1 = text_to_cstring(PG_GETARG_TEXT_PP(0));

    if (strcmp(var1, "002") == 0 || strcmp(var1, "003") == 0 || strcmp(var1, "004") == 0) {
        if (PG_ARGISNULL(1)) {
            result = 2;
            PG_RETURN_INT32(result);
        } else
            p1 = to_int64(PG_GETARG_DATUM(1), Multiplier);

        if (p1 == 2 * Multiplier) {
            result = 8;
        } else if (p1 == 3 * Multiplier) {
            result = 7;
        } else if (p1 == 4 * Multiplier) {
            result = 6;
        } else if (p1 == 5 * Multiplier) {
            result = 5;
        } else if (p1 == 6 * Multiplier) {
            result = 4;
        } else if (p1 == 7 * Multiplier) {
            result = 3;
        } else {
            result = 2;
        }
    } else {
        if (PG_ARGISNULL(1)) {
            result = 2;
            PG_RETURN_INT32(result);
        } else
            p1 = to_int64(PG_GETARG_DATUM(1), Multiplier);

        if (p1 == 4 * Multiplier) {
            result = 8;
        } else if (p1 == 5 * Multiplier) {
            result = 7;
        } else if (p1 == 6 * Multiplier) {
            result = 6;
        } else if (p1 == 7 * Multiplier) {
            result = 5;
        } else if (p1 == 8 * Multiplier) {
            result = 4;
        } else if (p1 == 9 * Multiplier) {
            result = 3;
        } else {
            result = 2;
        }
    }

    pfree(var1);
    PG_RETURN_INT32(result);
}

/* FUNC_ZERO_NULL */
Datum FUNC_ZERO_NULL(PG_FUNCTION_ARGS)
{
    int64 p1;
    float8 result;
    int64 Multiplier = 10000;

    p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);

    if (p1 == 0) {
        PG_RETURN_NULL();
    } else {
        result = (float8)p1 / Multiplier;
        return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
    }
}

/* greatestcc (text, text):
 * 	compare text with text
 */
Datum greatestcc(PG_FUNCTION_ARGS)
{
    text* result = NULL;
    char* str1 = NULL;
    char* str2 = NULL;

    str1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    str2 = text_to_cstring(PG_GETARG_TEXT_PP(1));

    if (strcmp((const char*)str2, (const char*)str1) > 0) {
        result = cstring_to_text(str2);
    } else {
        result = cstring_to_text(str1);
    }

    pfree(str1);
    pfree(str2);
    PG_RETURN_TEXT_P(result);
}

/* greatestcd (text, float8):
 * 	compare text with float8_to_text
 * Note: input float8 will be convert to numeric with 4 decimal digits
 */
Datum greatestcd(PG_FUNCTION_ARGS)
{
    int64 p2;
    int64 Multiplier = 10000;
    int length;
    int i, j;

    char inputStr2[30] = "";
    char* inputStr1 = NULL;
    char char_result[32] = "";
    text* result = NULL;

    inputStr1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    p2 = to_int64(PG_GETARG_DATUM(1), Multiplier);

    /* assume that: input data is decimal(18,4) */
    sprintf(inputStr2, "%lld", (long long)p2);

    length = strlen(inputStr2);

    j = 0;
    for (i = 0; i <= length; i++) {
        char_result[i] = inputStr2[j];
        j = j + 1;

        if (i == length - 4 - 1) {  // ?D??¦Ì¡À?a¦Ì1¨ºy¦Ì¨²4??¡Á?¡¤?¨º¡À, ?1?-D?¨ºy¦Ì?
            i = i + 1;
            char_result[i] = '.';
        }
    }
    char_result[i] = '\0';  // ¨¬¨ª?¨®¡Á?¡¤?¡ä??¨¢¨º?¡¤?

    if (strcmp((const char*)char_result, (const char*)inputStr1) > 0)
        result = cstring_to_text(char_result);
    else
        result = cstring_to_text(inputStr1);

    pfree(inputStr1);
    PG_RETURN_TEXT_P(result);
}

/* greatestci (text, int):
 * 	compare text with int_to_text
 */
Datum greatestci(PG_FUNCTION_ARGS)
{
    int32 p2;

    char inputStr2[20] = "";
    char* inputStr1 = NULL;
    text* result = NULL;

    inputStr1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    p2 = PG_GETARG_INT32(1);

    sprintf(inputStr2, "%d", p2);

    if (strcmp((const char*)inputStr2, (const char*)inputStr1) > 0)
        result = cstring_to_text(inputStr2);
    else
        result = cstring_to_text(inputStr1);

    pfree(inputStr1);
    PG_RETURN_TEXT_P(result);
}

/* greatestdc (float8, text):
 * 	compare float8 with text_to_float8
 */
Datum greatestdc(PG_FUNCTION_ARGS)
{

    int64 p1;
    float8 p2;
    int64 p = 0;
    char* str2 = NULL;
    int64 Multiplier = 10000;
    float8 result;
    Datum q2;
    int64 i2;

    p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    str2 = text_to_cstring(PG_GETARG_TEXT_PP(1));

    p2 = strtod((const char*)str2, (char**)NULL);
    q2 = DirectFunctionCall1(float8_numeric, Float8GetDatum(p2));
    i2 = to_int64(q2, Multiplier);
    p = i2 > p1 ? i2 : p1;

    pfree(str2);
    result = (float8)p / Multiplier;
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
}

/* greatestdd (float8, float8):
 * 	compare float8 with float8
 */
Datum greatestdd(PG_FUNCTION_ARGS)
{
    int64 p1;
    int64 p2;
    int64 p;
    int64 Multiplier = 10000;
    float8 result;

    p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    p2 = to_int64(PG_GETARG_DATUM(1), Multiplier);

    p = (p2 > p1 ? p2 : p1);
    result = (float8)p / Multiplier;
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
}

/* greatestdi (float8, int):
 * 	compare float8 with int
 */
Datum greatestdi(PG_FUNCTION_ARGS)
{

    int64 p1;
    int32 p2;
    int64 p;
    int64 Multiplier = 10000;
    float8 result;

    p1 = to_int64(PG_GETARG_DATUM(0), Multiplier);
    p2 = PG_GETARG_INT32(1);
    p = (p2 * Multiplier) > p1 ? (p2 * Multiplier) : p1;

    result = (float8)p / Multiplier;
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
}

/* greatestic (int, text):
 * 	compare int with text_to_int
 */
Datum greatestic(PG_FUNCTION_ARGS)
{
    int32 p1;
    int32 p2 = 0;
    char* str2 = NULL;
    int32 p;

    p1 = PG_GETARG_INT32(0);
    str2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    p2 = (int32)strtol((const char*)str2, (char**)NULL, 10);

    p = p2 > p1 ? p2 : p1;
    pfree(str2);
    PG_RETURN_INT32(p);
}

/* greatestid (int, float8):
 * 	compare int with float8
 */
Datum greatestid(PG_FUNCTION_ARGS)
{
    int32 p1;
    int64 p2;
    int64 p;
    int64 Multiplier = 10000;
    float8 result;

    p1 = PG_GETARG_INT32(0);
    p2 = to_int64(PG_GETARG_DATUM(1), Multiplier);

    p = (p1 * Multiplier) > p2 ? (p1 * Multiplier) : p2;
    result = (float8)p / Multiplier;
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(result));
}

/***************create_type check*****************************/
typedef struct Complex {
    double x;
    double y;
} Complex;

Datum complex_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    double x, y;
    Complex* result = NULL;

    if (sscanf(str, " ( %lf , %lf )", &x, &y) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for complex: \"%s\"", str)));

    result = (Complex*)palloc(sizeof(Complex));
    result->x = x;
    result->y = y;
    PG_RETURN_POINTER(result);
}

Datum complex_out(PG_FUNCTION_ARGS)
{
    Complex* complex = (Complex*)PG_GETARG_POINTER(0);
    char* result = NULL;

    result = (char*)palloc(100);
    snprintf(result, 100, "(%g,%g)", complex->x, complex->y);
    PG_RETURN_CSTRING(result);
}

Datum complex_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    Complex* result = NULL;

    result = (Complex*)palloc(sizeof(Complex));
    result->x = pq_getmsgfloat8(buf);
    result->y = pq_getmsgfloat8(buf);
    PG_RETURN_POINTER(result);
}

Datum complex_send(PG_FUNCTION_ARGS)
{
    Complex* complex = (Complex*)PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendfloat8(&buf, complex->x);
    pq_sendfloat8(&buf, complex->y);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
