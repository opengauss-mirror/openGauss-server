/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------
 *
 * direct_algos.h
 *        Declaration of the currently implemented DB4AI algorithms using the direct API
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/direct_algos.h
 *
 * ---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_DIRECT_ALGOS_H
#define DB4AI_DIRECT_ALGOS_H

#include "mock.h"

/*
 * template for your next wonderful algorithm
 *
 * class AmazingDB4AIAlgo final : public ModelCRTP<AmazingDB4AIAlgo> {
 * private:
 *     friend ModelCRTP<AmazingDB4AIAlgo>;
 *
 *     using ModelCRTP<AmazingDB4AIAlgo>::ModelCRTP;
 *
 *     void do_init(const char *datapath);
 *     void do_end();
 *     bool do_train(char const *model_name, Hyperparameters const *hp);
 *     bool do_predict();
 * };
 *
 */

/*
 * logistic regression
 */
class LogisticRegressionDirectAPI final : public ModelCRTP<LogisticRegressionDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<LogisticRegressionDirectAPI>;

    // inheriting constructors of the base class
    using ModelCRTP<LogisticRegressionDirectAPI>::ModelCRTP;

    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};

/*
 * svm classifiaction
 */
class SvmcDirectAPI final : public ModelCRTP<SvmcDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<SvmcDirectAPI>;

    // inheriting constructors of the base class
    using ModelCRTP<SvmcDirectAPI>::ModelCRTP;

    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};

/*
 * multiclass
 */
class MulticlassDirectAPI final : public ModelCRTP<MulticlassDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<MulticlassDirectAPI>;

    // inheriting constructors of the base class
    using ModelCRTP<MulticlassDirectAPI>::ModelCRTP;

    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};

/*
 * principal components
 */
class PrincipalComponentsDirectAPI final : public ModelCRTP<PrincipalComponentsDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<PrincipalComponentsDirectAPI>;

    // inheriting constructors of the base class
    using ModelCRTP<PrincipalComponentsDirectAPI>::ModelCRTP;

    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};

/*
 * k-means
 */
class KMeansDirectAPI final : public ModelCRTP<KMeansDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<KMeansDirectAPI>;
    
    // inheriting constructors of the base class
    using ModelCRTP<KMeansDirectAPI>::ModelCRTP;
    
    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};


/*
 * xgboost
 */
class XGBoostDirectAPI final : public ModelCRTP<XGBoostDirectAPI> {
private:
    // this friendship is enough for our purpose
    friend ModelCRTP<XGBoostDirectAPI>;
    
    // inheriting constructors of the base class
    using ModelCRTP<XGBoostDirectAPI>::ModelCRTP;
    
    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader);
    bool do_predict(Reader *reader);
};

#endif //DB4AI_DIRECT_ALGOS_H
