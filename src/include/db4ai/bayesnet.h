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
 *  bayesnet.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/bayesnet.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _BAYESNET_H_
#define _BAYESNET_H_

#include <cmath>
#include <float.h>
#include "db4ai/db4ai_api.h"
#include "db4ai/bayesnet_vector.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_map.h"
#include "gs_policy/gs_string.h"
#include "gs_policy/gs_set.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "instruments/generate_report.h"

#define BITMAP_LEN 128

class ContinDescriptionNet {
    int numslots;
    double mean;
    double variance;
    int numrows;
    int numnulls;
    double min;
    double max;
    int num;  // only for mean calculate
    int est_numdistincts;
    uint128 hash_card_est;  // estimate the num_distinct dynamically

public:
    ContinDescriptionNet()
        : numslots(5),
          mean(0.0),
          variance(0.0),
          numrows(0),
          numnulls(0),
          min(DBL_MAX),
          max(DBL_MIN),
          num(0),
          est_numdistincts(0),
          hash_card_est(0){};
    void setMeanMinMax(double element)
    {
        mean += (element - mean) / ++num;
        if (element > max) {
            max = element;
        }
        if (element < min) {
            min = element;
        }
    };

    // Second round func
    void setCard(double element)
    {
        int pos = 0;
        if (min < max) {
            pos = (element - min) / (max - min) * 127;
        }
        uint128 x = 1;
        hash_card_est |= (x << pos);
    };

    // Second round func
    void setVariance(double element)
    {
        variance += pow(element - mean, 2);
    };

    double getMean() const
    {
        return mean;
    };
    double getNumrows() const
    {
        return numrows;
    };
    double getVariance() const
    {
        return numrows > 0 ? variance / numrows : 0.0;
    };
    double getStandardDeviation() const
    {
        return sqrt(getVariance());
    };
    int getCategory() const
    {
        return numslots;
    };
    double getDelta() const
    {
        return est_numdistincts > 0 ? (max - min) / est_numdistincts : 0.0;
    };

    // Second round func
    void estimate_numdistincts()
    {
        int c = 0;
        uint128 n = hash_card_est;
        for (c = 0; n; ++c) {
            n &= (n - 1);
        }
        if (c >= BITMAP_LEN - 1) {
            est_numdistincts = ((BITMAP_LEN - 1) + numrows - numnulls) / 2;
        } else {
            double coeff = 1.0;
            double sum = 0.0;
            for (int i = c; i < numrows + 1; i++) {
                if (coeff < 1e-3) {
                    break;
                } else {
                    sum += i * coeff;
                    coeff *= ((double)c / BITMAP_LEN);
                }
            }
            est_numdistincts = (int)sum;
            if (est_numdistincts < 1) {
                est_numdistincts = 1;
            } else if (est_numdistincts > numrows) {
                est_numdistincts = numrows;
            }
        }
        return;
    };

    void addNumnulls()
    {
        numnulls++;
    };
    int getNumnulls()
    {
        return numnulls;
    };

    void addNumrows()
    {
        numrows++;
    };
    int getNumrows()
    {
        return numrows;
    };
};

template <typename T>
int cmp_value_bayesnet(const void *a, const void *b)
{
    T *left = (T *)a;
    T *right = (T *)b;
    Assert(left->type == right->type);
    if (!left->isnull and !right->isnull) {
        double res;
        Oid type = left->type;
        switch (type) {
            case FLOAT4OID:
            case FLOAT8OID:
                res = DatumGetFloat8(left->data) - DatumGetFloat8(right->data);
                break;
            case INT4OID:
                res = DatumGetInt32(left->data) - DatumGetInt32(right->data);
                break;
            case INT8OID:
                res = DatumGetInt64(left->data) - DatumGetInt64(right->data);
                break;
            case INT16OID:
                res = DatumGetInt128(left->data) - DatumGetInt128(right->data);
                break;
            case NUMERICOID:
                res = cmp_numerics(DatumGetNumeric(left->data), DatumGetNumeric(right->data));
                break;
            default:
                res = (double)left->hashval - (double)right->hashval;
        }
        if (abs(res) < 1e-6) {
            return 0;
        } else if (res < 0) {
            return -1;
        } else {
            return +1;
        }
    } else {
        if (left->isnull and right->isnull)
            return 0;
        else if (left->isnull)
            return +1;
        else
            return -1;
    }
}

typedef struct ValueInTuple {
    Datum data;
    Oid type;
    bool isnull;
    uint32_t hashval;
} ValueInTuple;

typedef struct PredictProb {
    char *value;
    double prob;
} PredictProb;

typedef struct ProbVector {
    double prob;
    int numProb = 0;
    double *probs;
    int binWidth;
} ProbVector;

#define BAYES_NOTHING 0
#define BAYES_MCV 1
#define BAYES_BIN 2
#define MAX_DISTINCT_NO_BIN_MCV 1024
#define MAX_DISTINCT_SUPPORT 1024 * 1024

using bayesContinNetList = Vector<ContinDescriptionNet>;
using bayesDatumList = Vector<ValueInTuple>;

using tupleMatrix = int **;

typedef struct Bayes {
    AlgorithmAPI algo;
} Bayes;

typedef struct SerializedModelBayesNode {
    Bayes *algorithm;
    bool returnprob;
    Oid return_type;
    uint32_t features_cols;
    uint32_t categories_size;
    uint32_t prob_offset = 0U;

    uint32_t *category_num;
    Oid *attrtype;
    double *prob;
    bayesDatumList **featuresmatrix_fornet;
} SerializedModelBayesNode;

extern Bayes naive_bayes;

typedef struct BayesNet {
    AlgorithmAPI algo;
} BayesNet;

typedef struct SerializedModelBayesNet {
    BayesNet *algorithm;
    Oid return_type;
    int num_nodes;
    int *sizes;
    bool *parents;
    int *data_process;
    double **targets_singlecol_prob;
    int *bin_widths;
    SerializedModelBayesNode *nodes;
    bool *iseliminated = NULL;
    bool *has_children;
} SerializedModelBayesNet;

typedef struct ValueMapEntry {
    ValueInTuple value;
    uint32_t cnt;
} ValueMapEntry;

extern uint32 value_hash(const void* key, Size keysize);
extern int value_compare(const void *a, const void *b, Size keysize);

extern BayesNet bayes_net_internal;
#define IsContinuous(type) (type == FLOAT4OID or type == FLOAT8OID)
extern ModelPredictor bayes_net_predict_prepare(AlgorithmAPI *self, SerializedModel const *model, Oid returntype);
extern double probabilityDensityRange(double x1, double x2, double mean, double stdDiv);
#endif /* BAYESNET_H */
