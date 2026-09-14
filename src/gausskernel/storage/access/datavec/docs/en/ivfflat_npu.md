# IVFFLAT-NPU

## 1. Introduction

This chapter mainly describes the installation and usage steps of the IVFFLAT-NPU feature of the DataVec vector engine in the openGauss database, to guide users through the operations. This feature uses the NPU to accelerate the construction and query performance of IVFFLAT indexes.

>[!NOTE] Description
>
>The IVFFLAT-NPU feature currently supports only IVFFLAT indexes.<br>
>The IVFFLAT-NPU feature currently supports only the vector data type. Using other vector data types will cause execution failures.<br>
>The IVFFLAT-NPU feature currently supports only the 910B series NPU.<br>
>The IVFFLAT-NPU feature can run only in a container environment where the CANN architecture is installed.

## 2. Installation Preparation

### Obtain the openGauss Image and Start the Container

- Image acquisition
For details, see [openGauss Container Installation and Deployment](https://docs.opengauss.org/en/docs/latest/installation_guide/installation_overview.html).
- Container startup
Startup command:

  ```bash
  docker run -d -it --name <Your_Container_Name>\
    --network=host  --ipc=host \
    --device=/dev/davinci0 \
    --device=/dev/davinci_manager --device=/dev/devmm_svm --device=/dev/hisi_hdc --hostname ascend_docker  \
    -e ASCEND_RUNTIME_OPTIONS=NODRV \
    -v /usr/local/Ascend/driver:/usr/local/Ascend/driver \
    -v /etc/ascend_install.info:/etc/ascend_install.info  \
    -v /usr/local/Ascend/add-ons/:/usr/local/Ascend/add-ons/ \
    -v /usr/local/sbin/npu-smi:/usr/local/sbin/npu-smi \
    -v  /var/log/npu/conf/slog:/var/log/npu/conf/slog  \
    -v /var/log/npu/slog:/var/log/npu/slog -v /var/log/npu/profiling:/var/log/npu/profiling \
    -v /var/log/npu/dump:/var/log/npu/dump -v /var/log/npu:/usr/slog  \
    -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
    -e GS_PASSWORD=<Your_GS_PASSWORD> -e GS_USERNAME=<Your_GS_USERNAME> -e GS_DB=<Your_GS_DB> opengauss:7.0.0-RC2
  ```

  Ensure that the host NPU driver is installed in `/usr/local/` Ascend.

  After starting the container, first switch to the omm user and run the npu-smi info command to check whether the NPU can display relevant information normally (it is recommended to configure NPU-related environment variables in `~/.bashrc`; see the next subsection for the environment variables). If the information is displayed normally, you can proceed to the next step. If it cannot be displayed normally, you can try to modify the container startup command based on the error reported on the NPU side.
  >Description:<br>
  >1) If the error `dcmi module initialize failed.ret is -8005` is reported when running npu-smi info as the omm user, you can manually modify the file permissions under /dev. The specific commands are as follows      (**need to be placed in the entrypoint.sh file to take effect**):
>
  >```
  >chown omm:omm /dev/davinci* 
  >chown omm:omm /dev/devmm_svm
  >chown omm:omm /dev/hisi_hdc
  >```
>
  >2) If npu-smi info reports "device is used", this is because openGauss runs under the ordinary user omm. If the container is started in privileged mode (in privileged mode, the container automatically mounts all NPU cards) and the NPU cards are occupied by other containers, this problem may occur. It is recommended to disable privileged mode and ensure that the NPU cards are idle.

### Install the CANN Framework

- See the attachment of this document for the CANN framework installation script in the container.
- Run the installation script as the root user in the container.

  ```bash
  chmod +x install_cann.sh
  ./install_cann.sh
  ```

### Configure the NPU Acceleration Installation Package

- The NPU acceleration library source code is located in the subdirectory openGauss-server/contrib/nputurbo under the openGauss-server source code. Compilation method: In the CMakeLists.txt file under the nputurbo directory, set the parameter SOC_VERSION to the corresponding NPU model (the default is Ascend910B4, and the options are Ascend910B4/Ascend910B3); create a build directory under the nputurbo directory, enter that directory, run 'cmake ../', and then run 'make'. After successful compilation, the libnputurbo.so dynamic acceleration library is generated.
- Configure environment variables
  Modify the environment configuration in the container

 ```bash
 cd /
 vi entrypoint.sh
 ```

 ```bash
 #Add NPU-related environment variables.
export ASCEND_TOOLKIT_HOME="/usr/local/Ascend/ascend-toolkit/latest"
export LD_LIBRARY_PATH="/usr/local/Ascend/driver/lib64/common/:/usr/local/Ascend/driver/lib64/driver/:${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe/op_tiling:${LD_LIBRARY_PATH}"
export PYTHONPATH="${ASCEND_TOOLKIT_HOME}/python/site-packages:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe:${PYTHONPATH}"
export PATH="${ASCEND_TOOLKIT_HOME}/bin:${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin:${ASCEND_TOOLKIT_HOME}/tools/ccec_compiler/bin:${PATH}"
export ASCEND_HOME_PATH="${ASCEND_TOOLKIT_HOME}"


#Add the so package environment variable.
export DATAVEC_NPU_LIB_PATH=<YOUR_SO_PATH>
  ```

 `YOUR_SO_PATH` is the path where libnputurbo.so is located.

### Configure GUC Parameters

Refer to Section 4 to configure the NPU-related GUC parameters in the `postgresql.conf` file at the path `/var/lib/opengauss/data` inside the container.

- After the above configuration is complete, restart the container.

```bash
docker restart <CONTAINER_ID>
```

> Description:<br>
> 1) If `bisheng:command not found` appears when manually compiling the NPU acceleration package, run `source /usr/local/Ascend/ascend-toolkit/latest/bin/setenv.bash`.

## 3. Environment Requirements

The IVFFLAT-NPU feature supports the ARM architecture and the openEuler22.03 operating system.

## 4. Installation and Uninstallation

### Enabling the IVFFLAT-NPU Feature

- Mandatory parameter:
Set the GUC parameter `enable_ivfflat_npu = on` to enable the IVFFLAT-NPU feature.

- Optional parameters:\
The GUC parameter `ivfflat_npubind_info = '1-2'` sets the NPU card number to use. The default value is 0, which means NPU card 0 is used.\
The GUC parameter `cache_data_on_npu` = (on|off): indicates whether to cache the original vector data in NPU memory during retrieval. Setting it to "on" improves retrieval performance, while setting it to "off" saves NPU memory. The default value is "off".

### Disable the IVFFLAT-NPU Feature

Set the GUC parameter `enable_ivfflat_npu = off` to disable the IVFFLAT-NPU feature.

For details about the GUC parameters related to the IVFFLAT-NPU feature, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

## 5. Using IVFFLAT-NPU

### IVFFLAT-NPU

``` 
openGauss=# set enable_npu = on;
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING ivfflat (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (lists = <LISTS>);

```

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - vector data column name

#### IVFFLAT-NPU Index Operator

The IVFFLAT index operator `[TYPE]_[DISTANCE_FUN]_ops` format:

- `TYPE` - vector type
    - vector

IVFPQ index supports the following vector data dimensions:

Name | Dimension limit 
--- | --- 
vector | 2,000

- `DISTANCE_FUN` - distance function
    - l2
    - ip
    - cosine

#### Vector Index Operator

Index operator | operator | Description
--- |--- |---
vector_l2_ops | <-> |L2 distance
vector_ip_ops | <#> |Inner product
vector_cosine_ops | <=> |Cosine distance

#### Index Options

- `lists` - Number of cluster centers (cells) in the inverted list (default is 100)
- `parallel_workers` - Parallelism for index construction, 1 to 32 (default is 1, concurrent construction)

**Example:** Create an IVFPQ index using L2 distance calculation with residual and set `lists = 200`.

```
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
```

#### Building a Vector Index in Parallel

Speed up vector index creation by enabling parallel build:

```
ALTER TABLE [TABLE_NAME] 
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**Example:** Set the parallel build degree of the index to 8.

```
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

#### Query Options

- `ivfflat_probe` - The size of the candidate set during query (defaults to 1).

```
openGauss=# SET ivfflat_probes = 10;
```

- `enable_seqscan` - Use a non-vector index during query (default on).

```
openGauss=# SET enable_seqscan = off;
```

#### Querying with an Index

```
openGauss=# set enable_npu = on;
openGauss=# SELECT * FROM [TABLE_NAME] ORDER BY [COLUMN_NAME] [operator] [VALUE];
```

- `TABLE_NAME` - table name
- `COLUMN_NAME` - column name
- `operator` - distance calculation operator, which must be the same as the distance calculation method used when creating the index
- `VALUE` - the query vector
**Example:** Sort by L2 distance in ascending order to query all vectors in the `embedding` column of the `items` table that are similar to the vector [1,2,3,4].

```
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';
```

#### NPU Cache Release

- Command for periodically checking NPU video memory

```bash
watch -n 0.1 npu-smi info
```

You can use the command above to check the video memory size used by the gaussdb process.

- NPU cache release scenarios
When the NPU performs an index query, it automatically caches the corresponding lists computation results so that subsequent queries can use them directly. However, when insert, update, or delete operations occur, the clustering results change, so the system automatically releases the cache. You can use the `npu-smi info` command to check the NPU video memory size before and after cache release.
Example:

```bash
openGauss=# set enable_npu = on;
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';

openGauss=# INSERT INTO items values(1, '[2,3,4,5]');
openGauss=# DELETE FROM items WHERE id = 1;
openGauss=# UPDATE items SET id = 0 WHERE id = 1;
```

## 6. Constraints

- Vector indexes support only ordinary row-store tables, temporary tables, Toast tables, Unlogged tables, and segment-page tables. For other tables, only btree and ubtree indexes can be created on vector data.
- If REINDEX is not executed after ALTER INDEX, subsequently inserted data is indexed according to the new index options, while existing data in the index remains unchanged.
- IVFNPU indexes do not support ustore storage.
- When building a table with vector columns, the INDEX clause can be used to build default btree and ubtree indexes, but a vector index cannot be specified.
- When the vector column dimension is not specified, a vector index cannot be built; only btree and ubtree indexes are supported.

## Appendix

Installation script (ensure the environment is connected to the network)

```bash
#!/bin/bash
set -e

# Install system dependencies
packages=(
    gcc gcc-c++ make cmake curl zlib-devel bzip2-devel openssl-devel
    ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel
    libpcap-devel xz-devel libev-devel expat-devel libffi-devel
    systemtap-sdt-devel unzip pciutils net-tools lapack-devel gcc-gfortran
    util-linux findutils wget
)

to_install=()
for pkg in "${packages[@]}"; do
    if ! rpm -q $pkg &>/dev/null; then
        to_install+=("$pkg")
    fi
done

if [ ${#to_install[@]} -gt 0 ]; then
    echo "1-Install the following packages: ${to_install[*]}"
    yum update -y
    yum install -y "${to_install[@]}"
    yum clean all
    rm -rf /var/cache/yum
    rm -rf /tmp/*
else
    echo "1-All packages are already installed. Skip the installation step."
fi

# Install Python 3.10.17
PYTHON_VERSION="3.10.17"
PYTHON_PREFIX="/usr/local/python${PYTHON_VERSION}"

export PATH="${PYTHON_PREFIX}/bin:${PATH}"

if ! command -v python3 &> /dev/null || \
   [[ $(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))") != "${PYTHON_VERSION%.*}" ]]; then
   
    echo "Python ${PYTHON_VERSION} not found, installing..."
    
    mkdir -p "${PYTHON_PREFIX}/lib"
    curl -fsSL "https://repo.huaweicloud.com/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" -o "/tmp/Python-${PYTHON_VERSION}.tgz"
    tar -xf "/tmp/Python-${PYTHON_VERSION}.tgz" -C /tmp
    cd "/tmp/Python-${PYTHON_VERSION}"
    
    ./configure --enable-shared --enable-optimizations LDFLAGS="-Wl,-rpath ${PYTHON_PREFIX}/lib" --prefix="${PYTHON_PREFIX}"
    
    make -j $(nproc)
    make altinstall
    
    ln -sf "${PYTHON_PREFIX}/bin/python${PYTHON_VERSION%.*}" "${PYTHON_PREFIX}/bin/python3"
    ln -sf "${PYTHON_PREFIX}/bin/pip${PYTHON_VERSION%.*}" "${PYTHON_PREFIX}/bin/pip3"
    ln -sf "${PYTHON_PREFIX}/bin/python3" "${PYTHON_PREFIX}/bin/python"
    ln -sf "${PYTHON_PREFIX}/bin/pip3" "${PYTHON_PREFIX}/bin/pip"
    
    echo "2-Python ${PYTHON_VERSION} installed successfully. Installation path: ${PYTHON_PREFIX}"
else
    echo "2-Python ${PYTHON_VERSION%.*} is already installed at $(which python3)"
fi

export PATH="${PYTHON_PREFIX}/bin:${PATH}"

# Install Python dependencies
packages=(
    attrs cython numpy==1.24.0 decorator sympy cffi pyyaml pathlib2
    psutils protobuf==3.20 scipy requests absl-py
)
to_install=()
for pkg in "${packages[@]}"; do
    if ! pip show $(echo "$pkg" | cut -d= -f1) &>/dev/null; then
        to_install+=("$pkg")
    fi
done
if [ ${#to_install[@]} -gt 0 ]; then
    echo "3-Install the following Python dependencies: ${to_install[*]}"
    pip install --no-cache-dir --upgrade pip
    pip install --no-cache-dir "${to_install[@]}"
else
    echo "3-All dependencies are installed. Skip the installation step."
fi

# Determine the CANN version based on the architecture.
ARCH=$(uname -m)
case "${ARCH}" in
     "x86_64") CANN_ARCH="x86_64" ;;
     "aarch64") CANN_ARCH="aarch64" ;;
     *) echo "Unsupported architecture: ${ARCH}" && exit 1 ;;
esac

CANN_TOOLKIT_URL="https://ascend-repo.obs.cn-east-2.myhuaweicloud.com/Milan-ASL/Milan-ASL%20V100R001C22B800TP013/Ascend-cann-toolkit_8.2.RC1.alpha001_linux-${CANN_ARCH}.run"
CANN_KERNELS_URL="https://ascend-repo.obs.cn-east-2.myhuaweicloud.com/Milan-ASL/Milan-ASL%20V100R001C22B800TP013/Ascend-cann-kernels-910b_8.2.RC1.alpha001_linux-${CANN_ARCH}.run"
CANN_INSTALL_DIR="/usr/local/Ascend"

## Download the CANN installation package.
if [ ! -f ~/Ascend-cann-toolkit.run ]; then
    echo "4-Downloading CANN Toolkit..."
    wget "${CANN_TOOLKIT_URL}" -O ~/Ascend-cann-toolkit.run --no-check
else
    echo "4-CANN_Toolkit downloaded."
fi

if [ ! -f ~/Ascend-cann-kernels.run ]; then
    echo "4-Downloading CANN KERNELS..."
    wget "${CANN_KERNELS_URL}" -O ~/Ascend-cann-kernels.run --no-check
else
    echo "4-CANN_KERNELS downloaded."
fi

## Install CANN Toolkit.
if [ ! -d "/usr/local/Ascend/ascend-toolkit/latest" ]; then
    echo "5-CANN Toolkit is being installed..."
    chmod +x ~/Ascend-cann-toolkit.run
    ~/Ascend-cann-toolkit.run --quiet --install --install-for-all
    #rm -f ~/Ascend-cann-toolkit.run

    chmod +x ~/Ascend-cann-kernels.run
    ~/Ascend-cann-kernels.run --quiet --install --install-for-all
    #rm -f ~/Ascend-cann-kernels.run
    
else
    echo "5-CANN Toolkit is already installed. Skip the installation step..."
fi

# Set the CANN environment variables.
export ASCEND_TOOLKIT_HOME="${CANN_INSTALL_DIR}/ascend-toolkit/latest"
export LD_LIBRARY_PATH="/usr/local/Ascend/driver/lib64/common/:/usr/local/Ascend/driver/lib64/driver/:${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe/op_tiling:${LD_LIBRARY_PATH}"
export PYTHONPATH="${ASCEND_TOOLKIT_HOME}/python/site-packages:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe:${PYTHONPATH}"
export PATH="${ASCEND_TOOLKIT_HOME}/bin:${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin:${ASCEND_TOOLKIT_HOME}/tools/ccec_compiler/bin:${PATH}"
export ASCEND_HOME_PATH="${ASCEND_TOOLKIT_HOME}"

# Verify the installation.
echo "===Installation complete==="
echo "Python version: $(python --version)"
echo "Pip version: $(pip --version)"
echo "CANN toolkit path: ${ASCEND_TOOLKIT_HOME}"

rm -rf /tmp/*
```
