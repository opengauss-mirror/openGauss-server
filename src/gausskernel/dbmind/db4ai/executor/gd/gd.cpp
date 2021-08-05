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
 *---------------------------------------------------------------------------------------
 *
 *  gd.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/gd.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"

#include "db4ai/gd.h"
#include "nodes/primnodes.h"
#include "utils/array.h"

const char *gd_get_optimizer_name(OptimizerML optimizer)
{
    static const char* names[] = { "gd", "ngd" };
    if (optimizer > OPTIMIZER_NGD)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid optimizer %d", optimizer)));

    return names[optimizer];
}

const char *gd_get_expr_name(GradientDescentExprField field)
{
    const char* names[] = {
        "algorithm",
        "optimizer",
        "result_type",
        "num_iterations",
        "exec_time_msecs",
        "processed",
        "discarded",
        "weights",
        "categories",
    };

    if ((field & GD_EXPR_SCORE) != 0)
        return gd_get_metric_name(field & ~GD_EXPR_SCORE);

    if (field > GD_EXPR_CATEGORIES)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid GD expression field %d", field)));

    return names[field];
}

Datum gd_float_get_datum(Oid type, gd_float value)
{
    Datum datum = 0;
    switch (type) {
        case BOOLOID:
            datum = BoolGetDatum(value != 0.0);
            break;
        case INT1OID:
            datum = Int8GetDatum(value);
            break;
        case INT2OID:
            datum = Int16GetDatum(value);
            break;
        case INT4OID:
            datum = Int32GetDatum(value);
            break;
        case INT8OID:
            datum = Int64GetDatum(value);
            break;
        case FLOAT4OID:
            datum = Float4GetDatum(value);
            break;
        case FLOAT8OID:
            datum = Float8GetDatum(value);
            break;
        case NUMERICOID:
            datum = DirectFunctionCall1(float4_numeric, Float4GetDatum(value));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Oid type %d not yet supported", type)));
            break;
    }
    return datum;
}

gd_float gd_datum_get_float(Oid type, Datum datum)
{
    gd_float value = 0;
    switch (type) {
        case BOOLOID:
            value = DatumGetBool(datum) ? 1.0 : 0.0;
            break;
        case INT1OID:
            value = DatumGetInt8(datum);
            break;
        case INT2OID:
            value = DatumGetInt16(datum);
            break;
        case INT4OID:
            value = DatumGetInt32(datum);
            break;
        case INT8OID:
            value = DatumGetInt64(datum);
            break;
        case FLOAT4OID:
            value = DatumGetFloat4(datum);
            break;
        case FLOAT8OID:
            value = DatumGetFloat8(datum);
            break;
        case NUMERICOID:
            value = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, datum));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Oid type %d not yet supported", type)));
            break;
    }
    return value;
}

char *gd_get_metric_name(int metric)
{
    switch (metric) {
        case METRIC_ACCURACY:
            return "accuracy";
        case METRIC_F1:
            return "f1";
        case METRIC_PRECISION:
            return "precision";
        case METRIC_RECALL:
            return "recall";
        case METRIC_LOSS:
            return "loss";
        case METRIC_MSE:
            return "mse";
        default:
            Assert(false);
    }
    return nullptr;
}

extern GradientDescentAlgorithm gd_logistic_regression;
extern GradientDescentAlgorithm gd_svm_classification;
extern GradientDescentAlgorithm gd_linear_regression;

GradientDescentAlgorithm *gd_get_algorithm(AlgorithmML algorithm)
{
    GradientDescentAlgorithm *gd_algorithm = nullptr;
    switch (algorithm) {
        case LOGISTIC_REGRESSION:
            gd_algorithm = &gd_logistic_regression;
            break;
        case SVM_CLASSIFICATION:
            gd_algorithm = &gd_svm_classification;
            break;
        case LINEAR_REGRESSION:
            gd_algorithm = &gd_linear_regression;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid algorithm %d", algorithm)));
            break;
    }
    return gd_algorithm;
}

// ////////////////////////////////////////////////////////////////////////////
// expressions for projections

static struct {
    GradientDescentExprField field;
    Oid fieldtype;
    char *name;
} GradientDescentExpr_fields[] = {
    { GD_EXPR_ALGORITHM,        INT4OID,        "algorithm"},
    { GD_EXPR_OPTIMIZER,        INT4OID,        "optimizer"},
    { GD_EXPR_RESULT_TYPE,      OIDOID,         "result_type"},
    { GD_EXPR_NUM_ITERATIONS,   INT4OID,        "num_iterations"},
    { GD_EXPR_EXEC_TIME_MSECS,  FLOAT4OID,      "exec_time_msecs"},
    { GD_EXPR_PROCESSED_TUPLES, INT4OID,        "processed_tuples"},
    { GD_EXPR_DISCARDED_TUPLES, INT4OID,        "discarded_tuples"},
    { GD_EXPR_WEIGHTS,          FLOAT4ARRAYOID, "weights"},
};

static GradientDescentExpr *makeGradientDescentExpr(GradientDescentExprField field, Oid fieldtype)
{
    GradientDescentExpr *xpr = makeNode(GradientDescentExpr);
    xpr->field = field;
    xpr->fieldtype = fieldtype;
    return xpr;
}

List *makeGradientDescentExpr(AlgorithmML algorithm, List *list, int field)
{
    Expr *expr;
    for (size_t i = 0; i < sizeof(GradientDescentExpr_fields) / sizeof(GradientDescentExpr_fields[0]); i++) {
        expr = (Expr *)makeGradientDescentExpr(GradientDescentExpr_fields[i].field,
            GradientDescentExpr_fields[i].fieldtype);
        list = lappend(list, makeTargetEntry(expr, field++, GradientDescentExpr_fields[i].name, false));
    }

    // add metrics
    GradientDescentAlgorithm *palgo = gd_get_algorithm(algorithm);
    int metrics = palgo->metrics;
    int metric = 1;
    while (metrics != 0) {
        if (metrics & metric) {
            expr = (Expr *)makeGradientDescentExpr(makeGradientDescentExprFieldScore(metric), FLOAT4OID);
            list = lappend(list, makeTargetEntry(expr, field++, gd_get_metric_name(metric), false));
            metrics &= ~metric;
        }
        metric <<= 1;
    }

    // binary value mappings
    if (dep_var_is_binary(palgo)) {
        expr = (Expr *)makeGradientDescentExpr(GD_EXPR_CATEGORIES, TEXTARRAYOID);
        list = lappend(list, makeTargetEntry(expr, field++, "categories", false));
    }

    return list;
}

Datum ExecGDExprScore(GradientDescentExprState *mlstate, bool *isNull)
{
    Datum dt = 0;
    bool hasp, hasr;
    double precision, recall;
    GradientDescentState *gd_state = (GradientDescentState *)mlstate->ps;

    switch (mlstate->xpr->field & ~GD_EXPR_SCORE) {
        case METRIC_LOSS:
            dt = Float4GetDatum(gd_state->loss);
            break;
        case METRIC_ACCURACY:
            dt = Float4GetDatum(get_accuracy(&gd_state->scores));
            break;
        case METRIC_F1: // 2 * (precision * recall) / (precision + recall)
            precision = get_precision(&gd_state->scores, &hasp);
            recall = get_recall(&gd_state->scores, &hasr);
            if ((hasp && precision > 0) || (hasr && recall > 0)) {
                dt = Float4GetDatum(2.0 * precision * recall / (precision + recall));
            } else
                *isNull = true;
            break;
        case METRIC_PRECISION:
            precision = get_precision(&gd_state->scores, &hasp);
            if (hasp) {
                dt = Float4GetDatum(precision);
            } else
                *isNull = true;
            break;
        case METRIC_RECALL:
            recall = get_recall(&gd_state->scores, &hasr);
            if (hasr) {
                dt = Float4GetDatum(recall);
            } else
                *isNull = true;
            break;
        case METRIC_MSE:
            dt = Float4GetDatum(gd_state->scores.mse);
            break;
        default:
            *isNull = true;
            break;
    }

    return dt;
}

Datum ExecNonGDExprScore(GradientDescentExprState *mlstate, ExprContext *econtext, bool *isNull)
{
    Datum dt = 0;
    Oid typoutput;
    GradientDescentState *gd_state = (GradientDescentState *)mlstate->ps;
    GradientDescent *gd_node = (GradientDescent *)gd_state->ss.ps.plan;
    ArrayBuildState *astate = NULL;

    switch (mlstate->xpr->field) {
        case GD_EXPR_ALGORITHM:
            dt = Int32GetDatum(gd_node->algorithm);
            break;
        case GD_EXPR_OPTIMIZER:
            dt = Int32GetDatum(gd_node->optimizer);
            break;
        case GD_EXPR_RESULT_TYPE:
            typoutput = get_atttypid(gd_state, gd_node->targetcol);
            dt = UInt32GetDatum(typoutput);
            break;
        case GD_EXPR_NUM_ITERATIONS:
            dt = Int32GetDatum(gd_state->n_iterations);
            break;
        case GD_EXPR_EXEC_TIME_MSECS:
            dt = Float4GetDatum(gd_state->usecs / 1000.0);
            break;
        case GD_EXPR_PROCESSED_TUPLES:
            dt = Int32GetDatum(gd_state->processed);
            break;
        case GD_EXPR_DISCARDED_TUPLES:
            dt = Int32GetDatum(gd_state->discarded);
            break;
        case GD_EXPR_WEIGHTS: {
            gd_float *pw = gd_state->weights.data;
            for (int i = 0; i < gd_state->weights.rows; i++)
                astate = accumArrayResult(astate, Float4GetDatum(*pw++), false, FLOAT4OID, CurrentMemoryContext);
            dt = makeArrayResult(astate, econtext->ecxt_per_query_memory);
        } break;
        case GD_EXPR_CATEGORIES:
            typoutput = get_atttypid(gd_state, gd_node->targetcol);
            for (int i = 0; i < gd_state->num_classes; i++)
                astate =
                    accumArrayResult(astate, gd_state->binary_classes[i], false, typoutput, CurrentMemoryContext);
            dt = makeArrayResult(astate, econtext->ecxt_per_query_memory);
            break;
        default:
            *isNull = true;
            break;
    }

    return dt;
}

Datum ExecEvalGradientDescent(GradientDescentExprState *mlstate, ExprContext *econtext, bool *isNull,
    ExprDoneCond *isDone)
{
    Datum dt = 0;

    if (isDone != NULL)
        *isDone = ExprSingleResult;

    *isNull = false;
    if ((mlstate->xpr->field & GD_EXPR_SCORE) != 0) {
        dt = ExecGDExprScore(mlstate, isNull);
    } else {
        dt = ExecNonGDExprScore(mlstate, econtext, isNull);
    }

    return dt;
}

