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
*  pca.cpp
*        SGD specialization to compute principal components
*
* IDENTIFICATION
*        src/gausskernel/dbmind/db4ai/executor/gd/pca.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "db4ai/gd.h"
#include "db4ai/db4ai_cpu.h"
#include "db4ai/fp_ops.h"

// we reuse this function defined in kmeans.cpp
extern ArrayType *construct_empty_md_array(uint32_t const num_components, uint32_t const dimension);

static void pca_gradients(GradientsConfig *cfg)
{
    Assert(cfg->features->rows > 0);
    
    auto cfg_pca = reinterpret_cast<GradientsConfigPCA *>(cfg);
    Matrix const *data = cfg->features;
    Matrix *eigenvectors = cfg->weights;
    Matrix *gradients = cfg->gradients;
    Matrix *dot_products = cfg_pca->dot_products;
    
    /*
     * eigenvectors is a matrix of (d x (k + 2))
     * d: dimension of the features
     * k: number of components to be computed
     * +2: proportions and standard deviations (column) vectors
     *
     * gradients is also a (clean) matrix of (d x (k + 2))
     *
     * eigenvalues is a matrix of (k x 1) and it is used as output
     */
    int32_t const dimension = eigenvectors->rows;
    int32_t const num_eigenvectors = eigenvectors->columns - 2;
    int32_t const batch_size = data->rows;
    
    // this is the sum of all eigenvectors
    double total_std_dev = 0.;
    double total_std_dev_correction = 0.;
    double local_error = 0.;
    double proportion = 0.;
    // this helps us update the running statistics of each eigenvalue
    IncrementalStatistics running_stats;
    
    /*
     * the gradient of the loss function w.r.t. the current approximation matrix W
     * (row vectors approximating the leading eigenvectors) is M * W, where M is an stochastic approximation to the
     * empirical covariance matrix (via x^t * x - observe that data vectors are row vector by default
     * and thus x^t is a column vector).
     *
     * observe that we do not produce the whole (empirical) covariance matrix, but rather we compute it
     * progressively (in a matrix-free manner using only vector-vector and scalar-vector operations)
     */
    
    /*
     * these are only stubs to represent every eigenvector, the gradient it moves in,
     * its proportion, and its standard deviation, each as a matrix (d x 1)
     * this is a temporary solution while we implement efficient high-order BLAS
     */
    Matrix eigenvector;
    Matrix eigenvector_gradient;
    Matrix eigenvector_proportion;
    Matrix eigenvector_std_dev;
    eigenvector.transposed = eigenvector_gradient.transposed =
    eigenvector_proportion.transposed = eigenvector_std_dev.transposed = false;
    
    eigenvector.rows = eigenvector_gradient.rows = eigenvector_proportion.rows =
    eigenvector_std_dev.rows = dimension;
    
    eigenvector.columns = eigenvector_gradient.columns =
    eigenvector_proportion.columns = eigenvector_std_dev.columns = 1;
    
    eigenvector.allocated = eigenvector_gradient.allocated =
    eigenvector_proportion.allocated = eigenvector_std_dev.allocated = dimension;
    
    // the proportion vector is the second-to-last column vector of the eigenvectors matrix
    eigenvector_proportion.data = eigenvectors->data + (num_eigenvectors * dimension);
    // the standard deviation vector is the very last column vector of the weights matrix
    eigenvector_std_dev.data = eigenvector_proportion.data + dimension;
    
    /*
     * this is also a stub for every dot product (n x 1) of the dot_products matrix
     */
    Matrix dot_xr_w;
    dot_xr_w.transposed = false;
    dot_xr_w.rows = batch_size;
    dot_xr_w.columns = 1;
    dot_xr_w.allocated = batch_size;
    
    /*
     * a (column) stub for every data vector (row vector of) from the data matrix (of dimensions n x d)
     */
    Matrix x_r;
    x_r.transposed = false;
    x_r.rows = dimension;
    x_r.columns = 1;
    x_r.allocated = dimension;
    
    /*
     * the empirical covariance matrix M = X^t * X is not produced explicitly. thus, in order to produce the product
     * M * W = ((X^t * X) * W), we do it as M * W = (X^t * (X * W)) to first produce a matrix of dimensions (n x k)
     * this first matrix is produced with the following loop
     *
     * at this point we also compute the current approximation to the corresponding eigenvalues since:
     * M * w_i = lambda_i * w_i -> w_i^t * M * w_i = w_i^t * lambda_i * w_i -> ||X * w_i||^2 = lambda_i * ||w_i||^2 ->
     * lambda_i = ||X * w_i||^2 / ||w_i||^2 -> lambda_i = ||X * w_i||^2 since w_i is a unit vector
     *
     * we thus piggy-back on this computation :)
     */
    for (int32_t e = 0; e < num_eigenvectors; ++e) {
        eigenvector.data = eigenvectors->data + (e * dimension);
        dot_xr_w.data = dot_products->data + (e * batch_size);
        /*
         * this is a column vector in which each entry represents the dot product <x_i, e_j>
         * where x_i is the i-th data (row) vector of the batch and e_j is the j-th (column) eigenvector
         */
        matrix_mult_vector(data, &eigenvector, &dot_xr_w);
        
        // this is eigenvalue * batch_size
        eigenvector_proportion.data[e] = matrix_dot(&dot_xr_w, &dot_xr_w);
        // we aggregate all eigenvalues (in a robust manner) to be able to compute each proportion
        // later on below
        twoSum(total_std_dev, eigenvector_proportion.data[e], &total_std_dev, &local_error);
        total_std_dev_correction += local_error;
    }
    total_std_dev += total_std_dev_correction;
    
    /*
     * at this point we have produced matrix W' = (X * W) of dimensions n x k which is stored in dot_products
     */
    
    /*
     * let us now factor in the current changes to the empirical covariance matrix w.r.t. the current batch
     * we assume that the gradients (d x k) matrix has been reset
     *
     * we will now produce the product X^t * W' to finally obtain M * W of dimension (d x k)
     * which correspond to the gradients of the different components
     *
     * observe that, technically, we have to transpose X to a matrix of dimension d x n, but since we produce
     * the product component-wise, the transposition can be simulated
     *
     * the error of the batch is computed here as well
     */
    float8 factor = 0.;
    double error = 0.;
    double error_correction = 0.;
    for (int32_t e = 0; e < num_eigenvectors; ++e) {
        dot_xr_w.data = dot_products->data + (e * batch_size);
        eigenvector_gradient.data = gradients->data + (e * dimension);
        for (int32_t r = 0; r < batch_size; ++r) {
            x_r.data = data->data + (r * dimension);
            factor = dot_xr_w.data[r];
            matrix_mult_scalar_add(&eigenvector_gradient, &x_r, factor);
        }
        // at this point the current proportion of each eigenvalue can be computed
        twoDiv(eigenvector_proportion.data[e], total_std_dev, &proportion, &local_error);
        // truncation
        eigenvector_proportion.data[e] = proportion + local_error;
        
        // we update the current running statistics of the proportion of each eigenvalue
        running_stats.setTotal(eigenvector_proportion.data[e]);
        cfg_pca->eigenvalues_stats[e] += running_stats;
        
        // this is the current standard deviation of the proportion of each eigenvalue
        eigenvector_std_dev.data[e] = cfg_pca->eigenvalues_stats[e].getEmpiricalStdDev();
        // we aggregate the standard deviations to produce the error of the batch
        twoSum(error, eigenvector_std_dev.data[e], &error, &local_error);
        error_correction += local_error;
    }
    // we take care of the error when doing arithmetic
    cfg_pca->batch_error = error + error_correction;
}

force_inline static double pca_test(const GradientDescentState* gd_state, const Matrix *features,
                                    const Matrix *dep_var, const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);
    
    const TrainModel *gd_node = gd_state->tms.config;

    // DB4AI_API
    Assert(gd_node->configurations == 1);
    HyperparametersGD *hyperp = (HyperparametersGD *)gd_node->hyperparameters[0];
    return hyperp->lambda;
}

static Datum pca_predict(const Matrix *features, const Matrix *weights,
                         Oid return_type, void *extra_data, bool max_binary, bool *categorize)
{
    Assert(!max_binary);

    /*
     * the projection of an n-dimensional tuple onto a lower k-dimensional space is given by the
     * inner product features^T * weights. assuming that features is a column vector
     * of dimensions n x 1 and weights is a matrix of dimensions n x k
     */
    int32_t const dimension = weights->rows;
    // remember that the last two columns of the matrix contain statistics about the eigenvectors
    // and thus they are not relevant for prediction
    int32_t const num_eigenvectors = weights->columns - 2;
    int32_t const total_bytes = (sizeof(float8) * num_eigenvectors) + ARR_OVERHEAD_NONULLS(1);
    errno_t errorno = EOK;
    int32_t dims[1] = {num_eigenvectors};
    int32_t lbs[1] = {1};
    int32_t coordinate_offset = 0;
    auto projected_coordinates = reinterpret_cast<ArrayType *>(palloc0(total_bytes));
    
    SET_VARSIZE(projected_coordinates, total_bytes);
    projected_coordinates->ndim = 1;
    projected_coordinates->dataoffset = 0;
    projected_coordinates->elemtype = FLOAT8OID;
    errorno = memcpy_s(ARR_DIMS(projected_coordinates), sizeof(int32_t), dims, sizeof(int32_t));
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(ARR_LBOUND(projected_coordinates), sizeof(int32_t), lbs, sizeof(int32_t));
    securec_check(errorno, "\0", "\0");
    
    auto projected_coordinates_array = reinterpret_cast<float8 *>(ARR_DATA_PTR(projected_coordinates));
    
    Matrix eigenvector;
    eigenvector.columns = 1;
    eigenvector.rows = eigenvector.allocated = dimension;
    eigenvector.transposed = false;
    
    // this loop computes the inner product between the input point and the
    // generating vectors
    for (int32_t e = 0; e < num_eigenvectors; ++e) {
        eigenvector.data = weights->data + coordinate_offset;
        projected_coordinates_array[e] = matrix_dot(features, &eigenvector);
        coordinate_offset += dimension;
    }
    
    *categorize = false;
    return PointerGetDatum(projected_coordinates);
}

//////////////////////////////////////////////////////////////////////////////////

static HyperparameterDefinition pca_hyperparameter_definitions[] = {
    HYPERPARAMETER_INT4("number_components", 1, 1, true, INT32_MAX, true, HyperparametersGD, number_dimensions,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("batch_size", 1000, 1, true, MAX_BATCH_SIZE, true, HyperparametersGD, batch_size,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("max_iterations", 100, 1, true, ITER_MAX, true, HyperparametersGD, max_iterations,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("max_seconds", 0, 0, true, INT32_MAX, true, HyperparametersGD, max_seconds, HP_NO_AUTOML()),
    HYPERPARAMETER_FLOAT8("tolerance", 0.0005, 0.0, true, DBL_MAX, true, HyperparametersGD, tolerance, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true, HyperparametersGD, seed, HP_NO_AUTOML()),
    HYPERPARAMETER_BOOL("verbose", false, HyperparametersGD, verbose, HP_NO_AUTOML()),
};

static const HyperparameterDefinition *gd_get_hyperparameters_pca(AlgorithmAPI *self, int *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(pca_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return pca_hyperparameter_definitions;
}

/*
 * We specialized PCA's explain
 */
List *pca_explain(AlgorithmAPI *self, SerializedModel const *model, Oid const return_type)
{
    List *model_info = nullptr;
    
    // extract serialized model
    SerializedModelGD gds;
    gd_deserialize((GradientDescent *)self, model, return_type, &gds);
    
    /*
     * weights is a matrix of dimensions n x k (n rows, k columns)
     */
    int32_t const dimension = gds.weights.rows;
    // remember that the last two columns of the matrix contain statistics about the eigenvectors
    // and thus we handle them in a different manner (not as the coordinates of eigenvectors)
    int32_t const num_eigenvectors = gds.weights.columns - 2;
    
    // output array of coordinates
    ArrayType *eigenvector_coordinates_array = nullptr;
    float8 *eigenvector_coordinates_array_data = nullptr;
    // raw data
    float8 *pw = gds.weights.data;
    // the proportion vector is the second-to-last column vector of the eigenvectors matrix
    float8 *proportions = pw + (num_eigenvectors * dimension);
    // the standard deviations vector is the very last column vector of the weights matrix
    float8 *std_devs = proportions + dimension;
    errno_t errorno = EOK;
    uint32_t const size_component_bytes = dimension * sizeof(float8);
    // this loop produces the output of every principal component
    for (int32_t e = 0; e < num_eigenvectors; ++e) {
        /*
         * opening and closing the eigenvector group
         * if both open_group and close_group are set, they will be ignored)
         */
        TrainingInfo *eigenvector_open_group = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        eigenvector_open_group->open_group = true;
        TrainingInfo *eigenvector_close_group = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        eigenvector_close_group->close_group = true;
        
        // the properties of an eigenvector
        TrainingInfo *eigenvector_id = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *eigenvector_coordinates = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *eigenvector_proportion = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *eigenvector_standard_deviation = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        
        // opening and closing a group must have exactly the same name (otherwise fire!)
        eigenvector_open_group->name = "Principal Component";
        eigenvector_close_group->name = "Principal Component";
        
        // the name of each property
        eigenvector_id->name = "ID";
        eigenvector_coordinates->name = "Coordinates";
        eigenvector_proportion->name = "Relative proportion";
        eigenvector_standard_deviation->name = "Standard deviation of proportion";
        
        // the type of each property
        eigenvector_id->type = INT4OID;
        eigenvector_coordinates->type = FLOAT8ARRAYOID;
        eigenvector_proportion->type = FLOAT8OID;
        eigenvector_standard_deviation->type = FLOAT8OID;
        
        // the coordinates of the component
        eigenvector_coordinates_array = construct_empty_md_array(1, dimension);
        eigenvector_coordinates_array_data = reinterpret_cast<double *>(ARR_DATA_PTR(eigenvector_coordinates_array));
        
        // filling in the data
        eigenvector_id->value = Int32GetDatum(e + 1);
        eigenvector_coordinates->value = PointerGetDatum(eigenvector_coordinates_array);
        eigenvector_proportion->value = Float8GetDatumFast(proportions[e]);
        eigenvector_standard_deviation->value = Float8GetDatumFast(std_devs[e]);
        
        // filling in the coordinates
        errorno = memcpy_s(eigenvector_coordinates_array_data, size_component_bytes, pw, size_component_bytes);
        securec_check(errorno, "\0", "\0");
        
        /*
         * appending the properties to the list of properties
         * OBSERVE that open and close elements must be well positioned! (at the beginning and end of the information)
         */
        model_info = lappend(model_info, eigenvector_open_group);
        model_info = lappend(model_info, eigenvector_id);
        model_info = lappend(model_info, eigenvector_coordinates);
        model_info = lappend(model_info, eigenvector_proportion);
        model_info = lappend(model_info, eigenvector_standard_deviation);
        model_info = lappend(model_info, eigenvector_close_group);
        
        pw += dimension;
    }
    
    return model_info;
}

ModelHyperparameters *gd_make_hyperparameters_pca(AlgorithmAPI *self)
{
    ModelHyperparameters *hyperp = gd_make_hyperparameters(self);
    ((HyperparametersGD *)hyperp)->optimizer = INVALID_OPTIMIZER;
    return hyperp;
}

GradientDescent gd_pca = {
    {PCA, "pca", ALGORITHM_ML_UNSUPERVISED | ALGORITHM_ML_RESCANS_DATA, gd_metrics_loss, gd_get_hyperparameters_pca,
     gd_make_hyperparameters_pca, gd_update_hyperparameters, gd_create, gd_run, gd_end, gd_predict_prepare, gd_predict,
     pca_explain},
    false,
    FLOAT8ARRAYOID,  // default return type
    0.,              // default feature
    0.,
    0.,
    nullptr,
    gd_init_optimizer_pca,
    nullptr,
    nullptr,
    nullptr,
    pca_gradients,
    pca_test,
    pca_predict,
    nullptr,
};