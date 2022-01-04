/* -------------------------------------------------------------------------
 *
 * pg_aggregate.h
 *	  definition of the system "aggregate" relation (pg_aggregate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_aggregate.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_AGGREGATE_H
#define PG_AGGREGATE_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------------------------------------------------------
 *		pg_aggregate definition.
 *
 *		cpp turns this into typedef struct FormData_pg_aggregate
 *
#ifdef PGXC
 * 		Derived from pg_aggregate, added collection function, collection data
 * type and collection initial value.
#endif
 *
 *	aggfnoid			pg_proc OID of the aggregate itself
 *	aggtransfn			transition function
#ifdef PGXC
 *	aggcollectfn		collectition function
#endif
 *	aggfinalfn			final function (0 if none)
 *	aggsortop			associated sort operator (0 if none)
 *	aggtranstype		type of aggregate's transition (state) data
 *	agginitval			initial value for transition state (can be NULL)
#ifdef PGXC
 *	agginitcollect		initial value for collection state (can be NULL)
#endif
 * ----------------------------------------------------------------
 */
#define AggregateRelationId  2600
#define AggregateRelation_Rowtype_Id 11326

CATALOG(pg_aggregate,2600) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
	regproc		aggfnoid;
	regproc		aggtransfn;
	regproc		aggcollectfn; /* PGXC */
	regproc		aggfinalfn;
	Oid			aggsortop;
	Oid			aggtranstype;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		agginitval;
	text		agginitcollect;	/* PGXC, VARIABLE LENGTH FIELD */
#endif
	char		aggkind;
	int2		aggnumdirectargs;
} FormData_pg_aggregate;

/* ----------------
 *		Form_pg_aggregate corresponds to a pointer to a tuple with
 *		the format of pg_aggregate relation.
 * ----------------
 */
typedef FormData_pg_aggregate *Form_pg_aggregate;

/* ----------------
 *		compiler constants for pg_aggregate
 * ----------------
 */

#ifdef PGXC
#define Natts_pg_aggregate                 10
#define Anum_pg_aggregate_aggfnoid         1
#define Anum_pg_aggregate_aggtransfn       2
#define Anum_pg_aggregate_aggcollectfn     3
#define Anum_pg_aggregate_aggfinalfn       4
#define Anum_pg_aggregate_aggsortop        5
#define Anum_pg_aggregate_aggtranstype     6
#define Anum_pg_aggregate_agginitval	   7
#define Anum_pg_aggregate_agginitcollect   8
#define Anum_pg_aggregate_aggkind          9
#define Anum_pg_aggregate_aggnumdirectargs 10
#endif

/*
 * Symbolic values for aggkind column. We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments).
 */
#define AGGKIND_NORMAL				'n'
#define AGGKIND_DEFAULT				'n'
#define AGGKIND_ORDERED_SET			'o'
#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) == AGGKIND_ORDERED_SET)
#define AGGNUMDIRECTARGS_DEFAULT	 0
/* ----------------
 * initial contents of pg_aggregate
 * ---------------
 */

/* avg */
#ifdef PGXC
DATA(insert ( 2100	int8_avg_accum	numeric_avg_collect	numeric_avg		0	1231	"{0,0}" "{0,0}" 	n	0));
#define INT8AVGFUNCOID 2100
DATA(insert ( 2101	int4_avg_accum	int8_avg_collect	int8_avg		0	1016	"{0,0}" "{0,0}" 	n	0));
#define INT4AVGFUNCOID 2101
DATA(insert ( 2102	int2_avg_accum	int8_avg_collect	int8_avg		0	1016	"{0,0}" "{0,0}" 	n	0));
#define INT2AVGFUNCOID 2102
DATA(insert ( 5537	int1_avg_accum	int8_avg_collect	int8_avg		0	1016	"{0,0}" "{0,0}" 	n	0));
#define INT1AVGFUNCOID 5537
DATA(insert ( 2103	numeric_avg_accum	numeric_avg_collect	numeric_avg		0	1231	"{0,0}" "{0,0}" 	n	0));
#define NUMERICAVGFUNCOID 2103
DATA(insert ( 2104	float4_accum	float8_collect	float8_avg		0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
#define FLOAT4AVGFUNCOID 2104
DATA(insert ( 2105	float8_accum	float8_collect	float8_avg		0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
#define FLOAT8AVGFUNCOID 2105
DATA(insert ( 2106	interval_accum	interval_collect	interval_avg	0	1187	"{0 second,0 second}" "{0 second,0 second}" 	n	0));
#define INTERVALAGGAVGFUNCOID 2106
#endif

/* sum */
#ifdef PGXC
DATA(insert ( 2107	int8_sum		numeric_add		-				0	1700	_null_ _null_ 	n	0));
#define INT8SUMFUNCOID 2107
DATA(insert ( 2108	int4_sum		int8_sum_to_int8		-				0	20		_null_ _null_ 	n	0));
#define INT4SUMFUNCOID 2108
DATA(insert ( 2109	int2_sum		int8_sum_to_int8		-				0	20		_null_ _null_ 	n	0));
#define INT2SUMFUNCOID 2109
DATA(insert ( 2110	float4pl		float4pl		-				0	700		_null_ _null_ 	n	0));
DATA(insert ( 2111	float8pl		float8pl		-				0	701		_null_ _null_ 	n	0));
DATA(insert ( 2112	cash_pl			cash_pl			-				0	790		_null_ _null_ 	n	0));
DATA(insert ( 2113	interval_pl		interval_pl		-				0	1186	_null_ _null_ 	n	0));
DATA(insert ( 2114	numeric_add		numeric_add		-				0	1700	_null_ _null_ 	n	0));
#define NUMERICSUMFUNCOID 2114
#endif

/* max */
#ifdef PGXC
DATA(insert ( 2115	int8larger		int8larger		-				413		20		_null_ _null_ 	n	0));
#define INT8LARGERFUNCOID 2115
DATA(insert ( 2116	int4larger		int4larger		-				521		23		_null_ _null_ 	n	0));
#define INT4LARGERFUNCOID 2116
DATA(insert ( 2117	int2larger		int2larger		-				520		21		_null_ _null_ 	n	0));
#define INT2LARGERFUNCOID 2117
DATA(insert ( 5538	int1larger		int1larger		-				5517		5545		_null_ _null_ 	n	0));
DATA(insert ( 2118	oidlarger		oidlarger		-				610		26		_null_ _null_ 	n	0));
DATA(insert ( 2119	float4larger	float4larger	-				623		700		_null_ _null_ 	n	0));
DATA(insert ( 2120	float8larger	float8larger	-				674		701		_null_ _null_ 	n	0));
DATA(insert ( 2121	int4larger		int4larger		-				563		702		_null_ _null_ 	n	0));
DATA(insert ( 2122	date_larger		date_larger		-				1097	1082	_null_ _null_ 	n	0));
DATA(insert ( 2123	time_larger		time_larger		-				1112	1083	_null_ _null_ 	n	0));
DATA(insert ( 2124	timetz_larger	timetz_larger	-				1554	1266	_null_ _null_ 	n	0));
DATA(insert ( 2125	cashlarger		cashlarger		-				903		790		_null_ _null_ 	n	0));
DATA(insert ( 2126	timestamp_larger	timestamp_larger	-		2064	1114	_null_ _null_ 	n	0));
DATA(insert ( 2127	timestamptz_larger	timestamptz_larger	-		1324	1184	_null_ _null_ 	n	0));
DATA(insert ( 2128	interval_larger interval_larger -				1334	1186	_null_ _null_ 	n	0));
DATA(insert ( 2129	text_larger		text_larger		-				666		25		_null_ _null_ 	n	0));
DATA(insert ( 2130	numeric_larger	numeric_larger	-				1756	1700	_null_ _null_ 	n	0));
DATA(insert ( 6668	network_larger		network_larger		-		1205	869	   _null_ _null_ 	n	0));
#define NUMERICLARGERFUNCOID 2130
DATA(insert ( 2050	array_larger	array_larger	-				1073	2277	_null_ _null_ 	n	0));
DATA(insert ( 2244	bpchar_larger	bpchar_larger	-				1060	1042	_null_ _null_ 	n	0));
DATA(insert ( 2797	tidlarger		tidlarger		-				2800	27		_null_ _null_ 	n	0));
DATA(insert ( 3526	enum_larger		enum_larger		-				3519	3500	_null_ _null_ 	n	0));
DATA(insert ( 9010 	smalldatetime_larger		smalldatetime_larger		-                       5554    9003    _null_ _null_ 	n	0));
DATA(insert ( 9009	smalldatetime_smaller		smalldatetime_smaller		-			5552	9003	_null_ _null_ 	n	0));
#endif

/* min */
#ifdef PGXC
DATA(insert ( 2131	int8smaller		int8smaller		-				412		20		_null_ _null_ 	n	0));
#define INT8SMALLERFUNCOID 2131
DATA(insert ( 2132	int4smaller		int4smaller		-				97		23		_null_ _null_ 	n	0));
#define INT4SMALLERFUNCOID 2132
DATA(insert ( 2133	int2smaller		int2smaller		-				95		21		_null_ _null_ 	n	0));
#define INT2SMALLERFUNCOID 2133
DATA(insert ( 2134	oidsmaller		oidsmaller		-				609		26		_null_ _null_ 	n	0));
DATA(insert ( 2135	float4smaller	float4smaller	-				622		700		_null_ _null_ 	n	0));
DATA(insert ( 2136	float8smaller	float8smaller	-				672		701		_null_ _null_ 	n	0));
DATA(insert ( 2137	int4smaller		int4smaller		-				562		702		_null_ _null_ 	n	0));
DATA(insert ( 2138	date_smaller	date_smaller	-				1095	1082	_null_ _null_ 	n	0));
DATA(insert ( 2139	time_smaller	time_smaller	-				1110	1083	_null_ _null_ 	n	0));
DATA(insert ( 2140	timetz_smaller	timetz_smaller	-				1552	1266	_null_ _null_ 	n	0));
DATA(insert ( 2141	cashsmaller		cashsmaller		-				902		790		_null_ _null_ 	n	0));
DATA(insert ( 2142	timestamp_smaller	timestamp_smaller	-		2062	1114	_null_ _null_ 	n	0));
DATA(insert ( 2143	timestamptz_smaller timestamptz_smaller -		1322	1184	_null_ _null_ 	n	0));
DATA(insert ( 2144	interval_smaller	interval_smaller	-		1332	1186	_null_ _null_ 	n	0));
DATA(insert ( 2145	text_smaller	text_smaller	-				664		25		_null_ _null_ 	n	0));
DATA(insert ( 2146	numeric_smaller numeric_smaller -				1754	1700	_null_ _null_ 	n	0));
DATA(insert ( 6669	network_smaller		network_smaller		-		1203	869	    _null_ _null_ 	n	0));
#define NUMERICSMALLERFUNCOID 2146
DATA(insert ( 2051	array_smaller	array_smaller	-				1072	2277	_null_ _null_ 	n	0));
DATA(insert ( 2245	bpchar_smaller	bpchar_smaller	-				1058	1042	_null_ _null_ 	n	0));
DATA(insert ( 2798	tidsmaller		tidsmaller		-				2799	27		_null_ _null_ 	n	0));
DATA(insert ( 3527	enum_smaller	enum_smaller	-				3518	3500	_null_ _null_ 	n	0));
#endif

/* count */
/* Final function is data type conversion function numeric_int8 is referenced by OID because of ambiguous definition in pg_proc */
#ifdef PGXC
DATA(insert ( 2147	int8inc_any		int8_sum_to_int8 -				0		20		"0" "0" 	n	0));
DATA(insert ( 2803	int8inc			int8_sum_to_int8 -				0		20		"0" "0" 	n	0));
#endif

/* var_pop */
#ifdef PGXC
DATA(insert ( 2718	int8_accum		numeric_collect	numeric_var_pop	0		1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2719	int4_accum		numeric_collect	numeric_var_pop	0		1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2720	int2_accum		numeric_collect	numeric_var_pop	0		1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2721	float4_accum	float8_collect	float8_var_pop	0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2722	float8_accum	float8_collect	float8_var_pop	0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2723	numeric_accum	numeric_collect	numeric_var_pop	0		1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* var_samp */
#ifdef PGXC
DATA(insert ( 2641	int8_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2642	int4_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2643	int2_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2644	float4_accum	float8_collect	float8_var_samp 0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2645	float8_accum	float8_collect	float8_var_samp 0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2646	numeric_accum	numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* variance: historical Postgres syntax for var_samp */
#ifdef PGXC
DATA(insert ( 2148	int8_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2149	int4_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2150	int2_accum		numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2151	float4_accum	float8_collect	float8_var_samp 0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2152	float8_accum	float8_collect	float8_var_samp 0		1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2153	numeric_accum	numeric_collect	numeric_var_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* stddev_pop */
#ifdef PGXC
DATA(insert ( 2724	int8_accum		numeric_collect	numeric_stddev_pop	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2725	int4_accum		numeric_collect	numeric_stddev_pop	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2726	int2_accum		numeric_collect	numeric_stddev_pop	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2727	float4_accum	float8_collect	float8_stddev_pop	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2728	float8_accum	float8_collect	float8_stddev_pop	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2729	numeric_accum	numeric_collect	numeric_stddev_pop	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* stddev_samp */
#ifdef PGXC
DATA(insert ( 2712	int8_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2713	int4_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2714	int2_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2715	float4_accum	float8_collect	float8_stddev_samp	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2716	float8_accum	float8_collect	float8_stddev_samp	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2717	numeric_accum	numeric_collect	numeric_stddev_samp 0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* stddev: historical Postgres syntax for stddev_samp */
#ifdef PGXC
DATA(insert ( 2154	int8_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2155	int4_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2156	int2_accum		numeric_collect	numeric_stddev_samp	0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2157	float4_accum	float8_collect	float8_stddev_samp	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2158	float8_accum	float8_collect	float8_stddev_samp	0	1022	"{0,0,0}" "{0,0,0}" 	n	0));
DATA(insert ( 2159	numeric_accum	numeric_collect	numeric_stddev_samp 0	1231	"{0,0,0}" "{0,0,0}" 	n	0));
#endif

/* SQL2003 binary regression aggregates */
#ifdef PGXC
DATA(insert ( 2818	int8inc_float8_float8	int8_sum_to_int8			-					0	20		"0" _null_ 	n	0));
DATA(insert ( 2819	float8_regr_accum	float8_regr_collect	float8_regr_sxx			0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2820	float8_regr_accum	float8_regr_collect	float8_regr_syy			0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2821	float8_regr_accum	float8_regr_collect	float8_regr_sxy			0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2822	float8_regr_accum	float8_regr_collect	float8_regr_avgx		0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2823	float8_regr_accum	float8_regr_collect	float8_regr_avgy		0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2824	float8_regr_accum	float8_regr_collect	float8_regr_r2			0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2825	float8_regr_accum	float8_regr_collect	float8_regr_slope		0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2826	float8_regr_accum	float8_regr_collect	float8_regr_intercept	0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2827	float8_regr_accum	float8_regr_collect	float8_covar_pop		0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2828	float8_regr_accum	float8_regr_collect	float8_covar_samp		0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
DATA(insert ( 2829	float8_regr_accum	float8_regr_collect	float8_corr				0	1022	"{0,0,0,0,0,0}" "{0,0,0,0,0,0}" 	n	0));
#endif

/* boolean-and and boolean-or */
#ifdef PGXC
DATA(insert ( 2517	booland_statefunc	booland_statefunc	-		58	16		_null_ _null_ 	n	0));
DATA(insert ( 2518	boolor_statefunc	boolor_statefunc	-		59	16		_null_ _null_ 	n	0));
DATA(insert ( 2519	booland_statefunc	booland_statefunc	-		58	16		_null_ _null_ 	n	0));
#endif

/* bitwise integer */
#ifdef PGXC
DATA(insert ( 5539	int1and		  int1and		  -					0	5545		_null_ _null_	n	0));
DATA(insert ( 5540	int1or		  int1or		  -					0	5545		_null_ _null_	n	0));
DATA(insert ( 2236	int2and		  int2and		  -					0	21		_null_ _null_ 	n	0));
DATA(insert ( 2237	int2or		  int2or		  -					0	21		_null_ _null_ 	n	0));
DATA(insert ( 2238	int4and		  int4and		  -					0	23		_null_ _null_ 	n	0));
DATA(insert ( 2239	int4or		  int4or		  -					0	23		_null_ _null_ 	n	0));
DATA(insert ( 2240	int8and		  int8and		  -					0	20		_null_ _null_ 	n	0));
DATA(insert ( 2241	int8or		  int8or		  -					0	20		_null_ _null_ 	n	0));
DATA(insert ( 2242	bitand		  bitand		  -					0	1560	_null_ _null_ 	n	0));
DATA(insert ( 2243	bitor		  bitor			  -					0	1560	_null_ _null_ 	n	0));
#endif

/* xml */
#ifdef PGXC
DATA(insert ( 2901	xmlconcat2	  xmlconcat2	  -					0	142		_null_ _null_ 	n	0));
#endif

/* array */
#ifdef PGXC
DATA(insert ( 2335	array_agg_transfn	-	array_agg_finalfn		0	2281	_null_ _null_ 	n	0));
#endif

/* text */
#ifdef PGXC
DATA(insert ( 3538	string_agg_transfn			-	string_agg_finalfn	0	2281	_null_ _null_ 	n	0));
#endif

/* checksum */
#ifdef PGXC
DATA(insert ( 4600	checksumtext_agg_transfn		  numeric_add		  -				0	1700	_null_ _null_ 	n	0));
#endif

/* bytea */
#ifdef PGXC
DATA(insert ( 3545	bytea_string_agg_transfn	-	bytea_string_agg_finalfn		0	2281	_null_ _null_ 	n	0));
#endif

/* json */
DATA(insert ( 3124	json_agg_transfn	-	json_agg_finalfn	0	2281	_null_ _null_ 	n	0));
DATA(insert ( 3403 	json_object_agg_transfn	-	json_object_agg_finalfn 0	2281	_null_ _null_ 	n	0));

/* first last */
DATA(insert ( 6560	first_transition	-	-	0	2283	_null_ _null_ 	n	0));
DATA(insert ( 6561	last_transition 	-	-	0	2283	_null_ _null_ 	n	0));

/* hll distribute agg */
DATA(insert ( 4366		hll_add_trans0 hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_ADD_TRANS0_OID 4366
DATA(insert ( 4380		hll_add_trans1 hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_ADD_TRANS1_OID 4380
DATA(insert ( 4381		hll_add_trans2 hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_ADD_TRANS2_OID 4381
DATA(insert ( 4382		hll_add_trans3 hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_ADD_TRANS3_OID 4382
DATA(insert ( 4383		hll_add_trans4 hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_ADD_TRANS4_OID 4383
DATA(insert ( 4367		hll_union_trans hll_union_collect hll_pack 0 4370 _null_ _null_ 	n	0));
#define HLL_UNION_TRANS_OID 4367

/* list */
#ifdef PGXC
DATA(insert ( 3552	list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter */
#ifdef PGXC
DATA(insert ( 3554	list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (int2) */
#ifdef PGXC
DATA(insert ( 3556	int2_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (int2) */
#ifdef PGXC
DATA(insert ( 3558	int2_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list  (int4) */
#ifdef PGXC
DATA(insert ( 3560	int4_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (int4) */
#ifdef PGXC
DATA(insert ( 3562	int4_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (int8) */
#ifdef PGXC
DATA(insert ( 3564	int8_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (int8) */
#ifdef PGXC
DATA(insert ( 3566	int8_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (float4) */
#ifdef PGXC
DATA(insert ( 3568	float4_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (float4) */
#ifdef PGXC
DATA(insert ( 3570	float4_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (float8) */
#ifdef PGXC
DATA(insert ( 3572	float8_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (float8) */
#ifdef PGXC
DATA(insert ( 3574	float8_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (numeric) */
#ifdef PGXC
DATA(insert ( 3576	numeric_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (numeric) */
#ifdef PGXC
DATA(insert ( 3578	numeric_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (date) */
#ifdef PGXC
DATA(insert ( 3580	date_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (date) */
#ifdef PGXC
DATA(insert ( 3582	date_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (timestamp) */
#ifdef PGXC
DATA(insert ( 3584	timestamp_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (timestamptz) */
#ifdef PGXC
DATA(insert ( 3586	timestamp_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (timestamptz) */
#ifdef PGXC
DATA(insert ( 3588	timestamptz_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (timestamptz) */
#ifdef PGXC
DATA(insert ( 3590	timestamptz_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list (interval) */
#ifdef PGXC
DATA(insert ( 4506	interval_list_agg_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* list without delimiter (interval) */
#ifdef PGXC
DATA(insert ( 4508	interval_list_agg_noarg2_transfn			-	list_agg_finalfn			0	2281	_null_ _null_	n	0));
#endif

/* ordered-set aggregates XXX shall we add collect funcs? */
DATA(insert ( 4452 ordered_set_transition      -    percentile_cont_float8_final            0   2281    _null_   _null_ 	 o 1));
DATA(insert ( 4454 ordered_set_transition      -    percentile_cont_interval_final          0   2281    _null_   _null_ 	 o 1));
DATA(insert (4461 ordered_set_transition - mode_final 0 2281 _null_ _null_ o 0));

DATA(insert (5555 median_transfn      -    median_float8_finalfn            0   2281    _null_   _null_ 	 n 0));
DATA(insert (5556 median_transfn      -    median_interval_finalfn          0   2281    _null_   _null_ 	 n 0));

/* percentile */
DATA(insert ( 9990	tdigest_merge		tdigest_merge_to_one		calculate_quantile_of				0	4406	_null_ _null_ 	n	0));
#define ADDTDIGESTMERGEOID 9990

DATA(insert ( 9986	tdigest_mergep		tdigest_merge_to_one		calculate_value_at				0	4406	_null_ _null_ 	n	0));
#define ADDTDIGESTMERGEPOID 9986

/*
 * prototypes for functions in pg_aggregate.c
 */
extern void AggregateCreate(const char *aggName,
				Oid aggNamespace,
				char aggKind,
				Oid *aggArgTypes,
				int numArgs,
				List *aggtransfnName,
#ifdef PGXC
				List *aggcollectfnName,
#endif
				List *aggfinalfnName,
				List *aggsortopName,
				Oid aggTransType,
#ifdef PGXC
				const char *agginitval,
				const char *agginitcollect);
#else
				const char *agginitval);
#endif

#endif   /* PG_AGGREGATE_H */
