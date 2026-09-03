# Performance Testing Using the VectorDBBench Tool

VectorDBBench is an open-source vector database benchmarking tool that measures key metrics to evaluate vector database performance.  
This document describes how to use VectorDBBench to perform performance testing on the DataVec vector engine in openGauss.

## Installing Python 3

Only Python 3.11 or later is supported.

```bash
wget --no-check-certificate https://www.python.org/ftp/python/3.11.0/Python-3.11.0.tar.xz
tar -xvf Python-3.11.0.tar.xz
cd Python-3.11.0

# <YOUR_PYTHON_INSTALL_PATH> is the user-defined Python installation path.
./configure --prefix=<YOUR_PYTHON_INSTALL_PATH> --enable-optimizations
make -j; make install
```

> [!NOTE]
>
>An error indicating that some dependency packages are missing may occur when compiling and installing Python 3. Install the required dependency packages and run the preceding commands again to compile and install Python 3.

Set the environment variables for Python 3:

```bash
vim ~/.bashrc
export PATH=<YOUR_PYTHON_INSTALL_PATH>/bin:$PATH
source ~/.bashrc
```

## Installing vectordb-bench

```bash
pip3 install vectordb-bench[all]

# If the installation fails with an error such as "mariadb_config not found ...", try installing a specific version: pip3 install vectordb-bench[all]==0.0.22
```

Replace the `vectordb-bench` folder with the version adapted for openGauss. Before doing so, delete the original `vectordb_bench` folder from the Python installation directory or move it to another directory as a backup. Then run the following copy command:

```bash
# Download the vectordb-bench version adapted for openGauss: https://github.com/wlff123/VectorDBBench.git
cp -r vectordb_bench <YOUR_PYTHON_INSTALL_PATH>/lib/python3.11/site-packages/
```

## Downloading Datasets

When VectorDBBench runs a test, it automatically downloads the selected dataset from the network. You can also download the dataset manually.

```bash
# Example of downloading the cohere1m dataset
wget https://assets.zilliz.com/benchmark/cohere_medium_1m/test.parquet --no-check-certificate
wget https://assets.zilliz.com/benchmark/cohere_medium_1m/neighbors.parquet --no-check-certificate
wget https://assets.zilliz.com/benchmark/cohere_medium_1m/shuffle_train.parquet --no-check-certificate

# Place the downloaded dataset files in the "<YOUR_DATASET_PATH>/cohere/cohere_medium_1m/" directory.
```

## Configuring openGauss Database Parameters

Configure the key parameters in the `postgresql.conf` file in the database node directory according to the test requirements:

```bash
max_connections = 1000          # Set this value greater than the number of concurrent connections for concurrent tests.
shared_buffers = 16GB           # If the machine has sufficient memory, it is recommended to set this value greater than the dataset size.
enable_indexscan = on           # Enable index scans.
enable_seqscan = off            # Disable full-table scans.
password_encryption_type = 1    # Use SHA-256 and MD5 to encrypt passwords.
```

## Performance Testing

The `vectordbbench` command-line interface is recommended for performance testing. You can flexibly adjust the test parameters as needed.

```bash
# Change the dataset path to the absolute path of the directory containing the dataset.
# vi <YOUR_PYTHON_INSTALL_PATH>/lib/python3.11/site-packages/vectordb_bench/__init__.py
DATASET_LOCAL_DIR = env.path("DATASET_LOCAL_DIR", "<YOUR_DATASET_PATH>")

# If the local machine cannot download the dataset from the network, the test may fail. In this case, you can comment out the code related to dataset downloading.
# vi <YOUR_PYTHON_INSTALL_PATH>/lib/python3.11/site-packages/vectordb_bench/backend/data_source.py
class AwsS3Reader(DatasetReader):
    # ...
    def read(self, dataset: str, files: list[str], local_ds_root: pathlib.Path):
        downloads = []
        # ...
        else:
            for file in files:
                remote_file = pathlib.PurePosixPath(self.remote_root, dataset, file)
                local_file = local_ds_root.joinpath(file)
                # Comment out the code for downloading the dataset here.
                # if (not local_file.exists()) or (not self.validate_file(remote_file, local_file)):
                #     log.info(f"local file: {local_file} not match with remote: {remote_file}; add to downloading list")
                #     downloads.append(remote_file)
```

Example test commands:

```bash
# vectordbbench opengausshnsw --case-type <DATASET> --k <TOPK> --concurrency-duration <DURATION> --num-concurrency <CONCURRENCY_NUM> --user-name <USERNAME> --password <PASSWORD> --host <HOST> --port <PORT> --db-name <DB_NAME> --m <M> --ef-construction <EF_CONSTRUCTION> --ef-search <EF_SEARCH>
# CASE-TYPE: Test case type.
# DATASET: Test dataset.
# TOPK: Number of nearest-neighbor results to query.
# DURATION: Query duration (in seconds).
# CONCURRENCY_NUM: Number of concurrent queries.
# USERNAME: Database username.
# PASSWORD: Database password.
# HOST: Database IP address.
# PORT: Database port.
# M: hnsw index construction parameter.
# EF_CONSTRUCTION: hnsw index construction parameter.
# EF_SEARCH: hnsw index search parameter.

# For details about the parameters, run the following commands to view the help information.
vectordbbench opengausshnsw --help
vectordbbench opengausshnswpq --help

# hnsw index test command
vectordbbench opengausshnsw --case-type Performance768D1M --k 10 --concurrency-duration 60 --num-concurrency 1 --user-name gaussdb --password YourPassword --host 127.0.0.1 --port 5432 --db-name postgres --m 16 --ef-construction 200 --ef-search 200
# hnswpq index test command
vectordbbench opengausshnswpq --pq_m 96 --hnsw_earlystop_threshold 160 --case-type Performance768D1M --k 10 --concurrency-duration 60 --num-concurrency 1 --user-name gaussdb --password YourPassword --host 127.0.0.1 --port 5432 --db-name postgres --m 16 --ef-construction 200 --ef-search 200
```

> [!NOTE]
>
> 1. Before running the hnswpq index test command, configure the PQ retrieval acceleration package. The PQ feature currently supports only the ARM architecture. For details, see [PQ](./pq.md).
> 2. You can view the supported dataset names in the description of the `--case-type` field displayed by running `vectordbbench opengausshnsw --help`.
> 3. During performance testing, VectorDBBench uses the user specified in the test command to perform operations such as creating tables, inserting data, creating indexes, executing queries, deleting indexes, and deleting tables in the database. Make sure that the user has the required permissions for these operations in advance.

After you run the test command, the test progress and results are printed in the current terminal.

```bash
# Example test result
... INFO: Performance case got result: Metric(max_load_count=0, load_duration=xxx, qps=xxx, serial_latency_p99=xxx, recall=xxx, ndcg=xxx, conc_num_list=xxx, conc_qps_list=xxx, conc_latency_p99_list=xxx, conc_latency_avg_list=xxx)...
# load_duration: Total time required to import the data and build the index.
# qps: Throughput.
# serial_latency_p99: P99 query latency with a single concurrent query.
# recall: Recall rate.
# conc_latency_p99_list: P99 latency for concurrent queries.
```
