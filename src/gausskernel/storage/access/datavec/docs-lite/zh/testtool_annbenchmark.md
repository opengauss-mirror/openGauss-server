# 向量分析性能测试-AnnBenchmark

## 1. 准备工作

### 测试环境

- 安装python>=3.8.6
- 下载适配openGauss数据库的ann-benchmark性能测试工具。下载链接：[ann-benchmarks-openGauss](https://github.com/lauraty123/ann-benchmarks-openGauss)
- 安装测试工具所需的依赖包`pip3 install -r requirements.txt`
- 部署openGauss-DataVec容器实例，教程可参考[openGauss-DataVec容器镜像安装](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)

### 测试数据

| Dataset                                                           | Dimensions | Train size | Test size | Neighbors | Distance  | Download                                                                   |
| ----------------------------------------------------------------- | ---------: | ---------: | --------: | --------: | --------- | -------------------------------------------------------------------------- |
| [DEEP1B](http://sites.skoltech.ru/compvision/noimi/)              |         96 |  9,990,000 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/deep-image-96-angular.hdf5) (3.6GB)
| [Fashion-MNIST](https://github.com/zalandoresearch/fashion-mnist) |        784 |     60,000 |    10,000 |       100 | Euclidean | [HDF5](http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5) (217MB) |
| [GIST](http://corpus-texmex.irisa.fr/)                            |        960 |  1,000,000 |     1,000 |       100 | Euclidean | [HDF5](http://ann-benchmarks.com/gist-960-euclidean.hdf5) (3.6GB)          |
| [GloVe](http://nlp.stanford.edu/projects/glove/)                  |         25 |  1,183,514 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/glove-25-angular.hdf5) (121MB)            |
| GloVe                                                             |         50 |  1,183,514 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/glove-50-angular.hdf5) (235MB)            |
| GloVe                                                             |        100 |  1,183,514 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/glove-100-angular.hdf5) (463MB)           |
| GloVe                                                             |        200 |  1,183,514 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/glove-200-angular.hdf5) (918MB)           |
| [Kosarak](https://fimi.uantwerpen.be/data/)                        |      27,983 |     74,962 |       500 |       100 | Jaccard   | [HDF5](http://ann-benchmarks.com/kosarak-jaccard.hdf5) (33MB)             |
| [MNIST](http://yann.lecun.com/exdb/mnist/)                        |        784 |     60,000 |    10,000 |       100 | Euclidean | [HDF5](http://ann-benchmarks.com/mnist-784-euclidean.hdf5) (217MB)         |
| [MovieLens-10M](https://grouplens.org/datasets/movielens/10m/)  |      65,134 |     69,363 |       500 |       100 | Jaccard   | [HDF5](http://ann-benchmarks.com/movielens10m-jaccard.hdf5) (63MB)             |
| [NYTimes](https://archive.ics.uci.edu/ml/datasets/bag+of+words)   |        256 |    290,000 |    10,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/nytimes-256-angular.hdf5) (301MB)         |
| [SIFT](http://corpus-texmex.irisa.fr/)                           |        128 |  1,000,000 |    10,000 |       100 | Euclidean | [HDF5](http://ann-benchmarks.com/sift-128-euclidean.hdf5) (501MB)          |
| [Last.fm](https://github.com/erikbern/ann-benchmarks/pull/91)     |         65 |    292,385 |    50,000 |       100 | Angular   | [HDF5](http://ann-benchmarks.com/lastfm-64-dot.hdf5) (135MB)               |
| [COCO-I2I](https://cocodataset.org/)                              |        512 |    113,287 |    10,000 |       100 | Angular   | [HDF5](https://github.com/fabiocarrara/str-encoders/releases/download/v0.1.3/coco-i2i-512-angular.hdf5) (136MB) |
| [COCO-T2I](https://cocodataset.org/)                              |        512 |    113,287 |    10,000 |       100 | Angular   | [HDF5](https://github.com/fabiocarrara/str-encoders/releases/download/v0.1.3/coco-t2i-512-angular.hdf5) (136MB) |

> 注：<br>
> 数据集位置：置放路径为/ann-benchmarks-openGauss/data，需要先`mkdir data`<br>
> 数据集下载：可以直接通过`wget`下载，例如：`wget http://ann-benchmarks.com/glove-50-angular.hdf5 --no-check-certificate`

## 2. 测试流程

### 数据库配置

容器内的配置文件路径(注意数据库参数修改后需要`重启容器`才能生效)：

```
/var/lib/opengauss/data/postgresql.conf
```

推荐配置参数：

```
shared_buffers=50GB #推荐大于数据库和索引的总和
maintenance_work_mem=4GB
password_encryption_type=1
max_connections=1000 #最大连接数
```

有关具体参数的修改，请参考 **[GUC参数说明](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/guc_parameter_usage.html)** 

### ann-benchmark配置

修改`go_opgs.sh`中的数据库连接配置

```bash
export ANN_BENCHMARKS_OG_USER='YourUserName'
export ANN_BENCHMARKS_OG_PASSWORD='YourPassword'
export ANN_BENCHMARKS_OG_DBNAME='YourDBName'
export ANN_BENCHMARKS_OG_HOST='YourHost'
export ANN_BENCHMARKS_OG_PORT=YourPort
```

修改ann-benchmarks-openGauss/ann_benchmarks/algorithms/openGauss/config.yml中构建索引、索引查询相关参数(按需)

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

- name: 近似搜索算法名
- run_groups: 索引构建和索引查询参数设置，这里的arg_groups设置的是hnsw索引构建参数m和efconstruction，concurrents为并发线程数，推荐值为cpu核数，query_args则是hnsw索引查询参数ef_search。

### 执行测试

修改`go_opgs.sh`中的启动命令

```bash
python3 run.py --algorithm openGauss-hnsw --dataset fashion-mnist-784-euclidean --local --runs 1 -k 10 --batch 
```

- --algorithm 算法名，这里的算法名来源于ann-benchmarks-openGauss/ann_benchmarks/algorithms/openGauss/config.yml的name字段，目前支持openGauss-hnsw、openGauss-hnswpq、openGauss-ivfflat。
- --dataset 数据集名称，支持的数据集见准备工作的测试数据章节。
- -- runs 执行测试集次数
- --k topk数目
- --batch 如果加入该参数，可以开启向量数据的并发查询

开启测试

```bash
sh go_opgs.sh
```

> 注：<br>
> 若已经执行过同组测试，需要重命名或删除`ann-benchmarks-openGauss/results/<dataset>/<k>/<algorithm>`下对应的文件，否则会直接跳过改组测试。

## 3. 测试结果

### 输出html可互动网页

```bash
python3 create_website.py --outputdir <YOUR_RESULT_PATH> --scatter --recompute
```

### 输出数据表格

```bash
python3 data_export.py --out <结果文件名>.csv
```

测试结果说明：

- k-nn:准确率
- qps:吞吐量
- p99:p99时延
- build:构建索引耗时
