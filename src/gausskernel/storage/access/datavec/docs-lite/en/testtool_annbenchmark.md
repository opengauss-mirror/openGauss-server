# Vector Analysis Performance Testing with Ann-Benchmarks

## 1. Preparation

### Test Environment

- Install Python >= 3.8.6.
- Download the Ann-Benchmarks performance testing tool adapted for the openGauss database. Download: [ann-benchmarks-openGauss](https://github.com/lauraty123/ann-benchmarks-openGauss)
- Install the dependencies required by the testing tool: `pip3 install -r requirements.txt`.
- Deploy an openGauss-DataVec container instance. For details, see [Installing the openGauss-DataVec Container Image](https://docs.opengauss.org/en/docs/latest/installation_guide/installing_the_container_image.html).

### Test Data

| Dataset | Dimensions | Train size | Test size | Neighbors | Distance | Download |
| ------- | ---------: | ---------: | --------: | --------: | -------- | -------- |
| [DEEP1B](http://sites.skoltech.ru/compvision/noimi/) | 96 | 9,990,000 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/deep-image-96-angular.hdf5) (3.6 GB) |
| [Fashion-MNIST](https://github.com/zalandoresearch/fashion-mnist) | 784 | 60,000 | 10,000 | 100 | Euclidean | [HDF5](http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5) (217 MB) |
| [GIST](http://corpus-texmex.irisa.fr/) | 960 | 1,000,000 | 1,000 | 100 | Euclidean | [HDF5](http://ann-benchmarks.com/gist-960-euclidean.hdf5) (3.6 GB) |
| [GloVe](http://nlp.stanford.edu/projects/glove/) | 25 | 1,183,514 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/glove-25-angular.hdf5) (121 MB) |
| GloVe | 50 | 1,183,514 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/glove-50-angular.hdf5) (235 MB) |
| GloVe | 100 | 1,183,514 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/glove-100-angular.hdf5) (463 MB) |
| GloVe | 200 | 1,183,514 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/glove-200-angular.hdf5) (918 MB) |
| [Kosarak](https://fimi.uantwerpen.be/data/) | 27,983 | 74,962 | 500 | 100 | Jaccard | [HDF5](http://ann-benchmarks.com/kosarak-jaccard.hdf5) (33 MB) |
| [MNIST](http://yann.lecun.com/exdb/mnist/) | 784 | 60,000 | 10,000 | 100 | Euclidean | [HDF5](http://ann-benchmarks.com/mnist-784-euclidean.hdf5) (217 MB) |
| [MovieLens-10M](https://grouplens.org/datasets/movielens/10m/) | 65,134 | 69,363 | 500 | 100 | Jaccard | [HDF5](http://ann-benchmarks.com/movielens10m-jaccard.hdf5) (63 MB) |
| [NYTimes](https://archive.ics.uci.edu/ml/datasets/bag+of+words) | 256 | 290,000 | 10,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/nytimes-256-angular.hdf5) (301 MB) |
| [SIFT](http://corpus-texmex.irisa.fr/) | 128 | 1,000,000 | 10,000 | 100 | Euclidean | [HDF5](http://ann-benchmarks.com/sift-128-euclidean.hdf5) (501 MB) |
| [Last.fm](https://github.com/erikbern/ann-benchmarks/pull/91) | 65 | 292,385 | 50,000 | 100 | Angular | [HDF5](http://ann-benchmarks.com/lastfm-64-dot.hdf5) (135 MB) |
| [COCO-I2I](https://cocodataset.org/) | 512 | 113,287 | 10,000 | 100 | Angular | [HDF5](https://github.com/fabiocarrara/str-encoders/releases/download/v0.1.3/coco-i2i-512-angular.hdf5) (136 MB) |
| [COCO-T2I](https://cocodataset.org/) | 512 | 113,287 | 10,000 | 100 | Angular | [HDF5](https://github.com/fabiocarrara/str-encoders/releases/download/v0.1.3/coco-t2i-512-angular.hdf5) (136 MB) |

> **Note:**
> - Dataset location: Place the datasets in `/ann-benchmarks-openGauss/data`. Create the directory first by running `mkdir data`.
> - Dataset download: You can download the datasets directly using `wget`. For example: `wget http://ann-benchmarks.com/glove-50-angular.hdf5 --no-check-certificate`.

## 2. Test Procedure

### Database Configuration

The configuration file is located at the following path inside the container. Note that you must **restart the container** for changes to database parameters to take effect.

```text
/var/lib/opengauss/data/postgresql.conf
```

Recommended configuration parameters:

```text
shared_buffers=50GB # Recommended to be greater than the total size of the database and indexes.
maintenance_work_mem=4GB
password_encryption_type=1
max_connections=1000 # Maximum number of connections.
```

For details about modifying these parameters, see [GUC Parameter Usage](https://docs.opengauss.org/en/docs/latest/database_reference/guc_parameter_usage.html).

### Ann-Benchmarks Configuration

Modify the database connection settings in `go_opgs.sh`.

```bash
export ANN_BENCHMARKS_OG_USER='YourUserName'
export ANN_BENCHMARKS_OG_PASSWORD='YourPassword'
export ANN_BENCHMARKS_OG_DBNAME='YourDBName'
export ANN_BENCHMARKS_OG_HOST='YourHost'
export ANN_BENCHMARKS_OG_PORT=YourPort
```

Modify the index construction and index query parameters in `ann-benchmarks-openGauss/ann_benchmarks/algorithms/openGauss/config.yml` as required.

```bash
  - base_args: ['@metric']
    constructor: openGaussHNSW
    disabled: false
    docker_tag: ann-benchmarks-openGauss
    module: ann_benchmarks.algorithms.openGauss
    name: openGauss-hnsw
    run_groups:
       M-16:
         arg_groups: [{M: 16, efConstruction: 200, concurrents: 80}]
         args: {}
         query_args: [[10, 20, 40, 80, 120, 200, 400, 800]]
       M-24:
         arg_groups: [{M: 24, efConstruction: 200, concurrents: 80}]
         args: {}
         query_args: [[10, 20, 40, 80, 120, 200, 400, 800]]
```

- `name`: name of the approximate search algorithm.
- `run_groups`: index construction and index query parameter settings. `arg_groups` specifies the HNSW index construction parameters `M` and `efConstruction`. `concurrents` specifies the number of concurrent threads. The recommended value is the number of CPU cores. `query_args` specifies the HNSW index query parameter `ef_search`.

### Running the Test

Modify the startup command in `go_opgs.sh`.

```bash
python3 run.py --algorithm openGauss-hnsw --dataset fashion-mnist-784-euclidean --local --runs 1 -k 10 --batch 
```

- `--algorithm`: algorithm name. The algorithm name is specified by the `name` field in `ann-benchmarks-openGauss/ann_benchmarks/algorithms/openGauss/config.yml`. The currently supported algorithms are `openGauss-hnsw`, `openGauss-hnswpq`, and `openGauss-ivfflat`.
- `--dataset`: dataset name. For supported datasets, see the **Test Data** section in **Preparation**.
- `--runs`: number of times to run the test set.
- `--k`: number of top-K results.
- `--batch`: enables concurrent vector queries when this parameter is specified.

Start the test.

```bash
sh go_opgs.sh
```

> **Note:**<br>
> If the same test group has been run previously, rename or delete the corresponding files under `ann-benchmarks-openGauss/results/<dataset>/<k>/<algorithm>`. Otherwise, the test group will be skipped directly.

## 3. Test Results

### Generating an Interactive HTML Web Page

```bash
python3 create_website.py --outputdir <YOUR_RESULT_PATH> --scatter --recompute
```

### Exporting Test Results

```bash
python3 data_export.py --out <result_file_name>.csv
```

Test result metrics:

- `Recall`: recall
- `qps`: throughput
- `p99`: P99 latency
- `build`: index construction time
