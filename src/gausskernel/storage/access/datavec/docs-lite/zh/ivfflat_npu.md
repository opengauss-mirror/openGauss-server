# IVFFLAT-NPU

## 1.介绍

本章节主要介绍openGauss数据库DataVec向量引擎IVFFLAT-NPU特性的安装使用步骤，以指导用户顺利完成操作。本特性结合NPU加速ivfflat索引的构建与查询性能。

>[!NOTE]说明
>
>IVFFLAT-NPU特性暂时只支持IVFFLAT索引。<br>
>IVFFLAT-NPU特性暂时只支持vector数据类型，在其他向量数据类型会导致执行失败。<br>
>IVFFLAT-NPU特性暂时只支持910B系列NPU。<br>
>IVFFLAT-NPU 特性仅支持在已安装 CANN 架构的容器环境中运行。

## 2.安装准备

### 获取openGauss镜像并启动容器

- 镜像获取
详见[openGauss容器安装部署](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)。
- 容器启动
启动命令：

  ```bash
  docker run -d -it --name <Your_Container_Name> \
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

  这里需要保证宿主机NPU驱动程序安装在/usr/local/Ascend上。

  在启动容器后请先切换到omm用户下通过npu-smi info命令查看npu是否可以正常显示相关信息（建议在~/.bashrc中配置npu相关环境变量，环境变量见下一小节，如果正常显示可以继续下一步骤，如果不能正常显示可以根据npu侧报错尝试修改容器启动命令
  >说明：<br>
  >1）如果进入omm使用npu-smi info时报`dcmi module initialize failed.ret is -8005`的错误，可以手动修改/dev下的文件权限，具体命令如下      (**需要放到entrypoint.sh文件中生效**)：
>
  >```
  >chown omm:omm /dev/davinci* 
  >chown omm:omm /dev/devmm_svm
  >chown omm:omm /dev/hisi_hdc
  >```
>
  >2）如果npu-smi info报"device is used"，这是因为openGauss在普通用户omm下运行，如果容器以特权模式启动（特权模式下容器会自动挂载所有npu卡）并且npu卡有其他容器占用，就可能存在这种问题，建议关闭特权模式，并且保证npu卡空闲。
  
### 安装CANN框架

- 详见本文附件获取容器内cann框架安装脚本
- 容器root下运行安装脚本

  ```bash
  chmod +x install_cann.sh
  ./install_cann.sh
  ```

### 配置NPU加速安装包

- NPU加速库源码位于openGauss-server源码下的子目录openGauss-server/contrib/nputurbo。编译方法：在nputurbo目录下CMakeLists.txt中设置参数SOC_VERSION为对应NPU型号(默认为Ascend910B4，可选Ascend910B4/Ascend910B3)；在nputurbo目录下创建一个build目录，并进入该目录，执行‘cmake ../’，再执行‘make’，编译成功后，会生成libnputurbo.so动态加速库。
- 配置环境变量
 在容器内修改环境配置

 ```bash
 cd /
 vi entrypoint.sh
 ```
 
 ```bash
 #添加NPU相关环境变量
export ASCEND_TOOLKIT_HOME="/usr/local/Ascend/ascend-toolkit/latest"
export LD_LIBRARY_PATH="/usr/local/Ascend/driver/lib64/common/:/usr/local/Ascend/driver/lib64/driver/:${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe/op_tiling:${LD_LIBRARY_PATH}"
export PYTHONPATH="${ASCEND_TOOLKIT_HOME}/python/site-packages:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe:${PYTHONPATH}"
export PATH="${ASCEND_TOOLKIT_HOME}/bin:${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin:${ASCEND_TOOLKIT_HOME}/tools/ccec_compiler/bin:${PATH}"
export ASCEND_HOME_PATH="${ASCEND_TOOLKIT_HOME}"


#添加so包环境变量
export DATAVEC_NPU_LIB_PATH=<YOUR_SO_PATH>
  ```

 `YOUR_SO_PATH`为libnputurbo.so所在路径。

### 配置GUC参数

参考第4节在容器内路径`/var/lib/opengauss/data/postgresql.conf`文件中配置NPU相关的GUC参数。

- 以上配置完成后重启容器。

```bash
docker restart <CONTAINER_ID>
```

>说明：<br>
>1）如果用户手动编译NPU加速包时出现`bisheng:command not found`，需要执行`source /usr/local/Ascend/ascend-toolkit/latest/bin/setenv.bash`。

## 3.环境要求

IVFFLAT-NPU特性支持ARM架构以及openEuler22.03操作系统。

## 4.安装与卸载

### 启用IVFFLAT-NPU特性

- 必选参数：
设置GUC参数`enable_ivfflat_npu = on`启用IVFFLAT-NPU特性。

- 可选参数：\
GUC参数`ivfflat_npubind_info = '1-2'`设置使用的NPU卡编号。默认值为0，表示使用第0号NPU卡。\
GUC参数`cache_data_on_npu`on|off）：表示检索时是否在 NPU 内存中缓存原始向量数据。将其设置为"on"可提升检索性能，而将其设置为"off "则可节省 NPU 内存。默认值为"off"。

### 关闭IVFFLAT-NPU特性

设置GUC参数`enable_ivfflat_npu = off`关闭IVFFLAT-NPU特性。

IVFFLAT-NPU特性相关GUC参数详情请参见[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/datavec_vector_engine_parameters.html)。

## 5.使用IVFFLAT-NPU

### IVFFLAT-NPU

``` 
openGauss=# set enable_npu = on;
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING ivfflat (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (lists = <LISTS>);

```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

#### IVFFLAT-NPU索引操作符

IVFFLAT索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector

IVFPQ索引支持向量数据维度：

名称 | 维度限制 
--- | --- 
vector | 2,000

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine

#### vector 索引操作符

索引操作符 | operator | 描述 
--- |--- |--- 
vector_l2_ops | <-> |L2距离
vector_ip_ops | <#> |内积
vector_cosine_ops | <=> |余弦距离

#### 索引选项

- `lists` - 倒排表（单元格）聚类聚类中心数量（默认为100））
- `parallel_workers` - 构建索引并行度 1~32（默认为1并发构建）

**示例：** 使用带残差的L2距离计算创建IVFPQ索引并设置`lists = 200`。

```
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
```

#### 并行构建向量索引

通过开启并行构建功能来加速向量索引的创建：

```
ALTER TABLE [TABLE_NAME] 
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**示例：** 构建索引并行数设置为8。

```
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

#### 查询选项

- `ivfflat_probe` - 查询时候选集的大小（默认为1）。

```
openGauss=# SET ivfflat_probes = 10;
```

- `enable_seqscan` - 查询时使用非向量索引(默认on)

```
openGauss=# SET enable_seqscan = off;
```

#### 使用索引查询

```
openGauss=# set enable_npu = on;
openGauss=# SELECT * FROM [TABLE_NAME] ORDER BY [COLUMN_NAME] [operator] [VALUE];
```

- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 列名
- `operator` - 距离计算操作符，需要与创建索引时使用距离计算方法相同
- `VALUE` - 查询的向量
**示例：** 通过l2距离从小到大排序，查询items表中embedding这一列中与[1,2,3,4]向量相似的全部向量。

```
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';
```

#### NPU释放缓存

- NPU显存定时查看命令

```bash
watch -n 0.1 npu-smi info
```

可以通过以上命令查看gaussdb进程使用的显存大小。

- NPU缓存释放场景
当 NPU 进行索引查询时，会自动缓存相应的 lists 计算结果，以便后续查询时直接使用。然而，在增删改操作发生时，聚类结果会变化，因此系统会自动释放缓存。可以使用`npu-smi info`命令查看缓存释放前后的 NPU 显存大小。
示例：

```bash
openGauss=# set enable_npu = on;
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';

openGauss=# INSERT INTO items values(1, '[2,3,4,5]');
openGauss=# DELETE FROM items WHERE id = 1;
openGauss=# UPDATE items SET id = 0 WHERE id = 1;
```

## 6.约束

- 向量索引仅支持普通行存表，临时表，Toast表，Unlogged表，段页式表等，其他表仅支持对向量数据创建btree和ubtree索引。
- 若ALTER INDEX后不执行REINDEX，后插入的数据会根据新的索引选项构建索引，而索引中已存在的数据不会因此改变。
- IVFNPU索引不支持ustore存储。
- 构建带有向量表时可以使用INDEX子句构建默认btree、ubtree索引，无法指定向量索引。
- 未指定向量列维度时无法构建向量索引，只支持构建btree、ubtree索引。

## 附件

安装脚本（需要保证环境是联网状态）

```bash
#!/bin/bash
set -e

# 安装系统依赖
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
    echo "1-安装以下软件包: ${to_install[*]}"
    yum update -y
    yum install -y "${to_install[@]}"
    yum clean all
    rm -rf /var/cache/yum
    rm -rf /tmp/*
else
    echo "1-所有软件包已安装，跳过安装步骤。"
fi

# 安装Python 3.10.17
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
    
    
    echo "2-Python ${PYTHON_VERSION} 安装成功，安装路径为： ${PYTHON_PREFIX}"
else
    echo "2-Python ${PYTHON_VERSION%.*} 已安装在 $(which python3)"
fi

export PATH="${PYTHON_PREFIX}/bin:${PATH}"

# 安装Python依赖
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
    echo "3-安装以下Python依赖: ${to_install[*]}"
    pip install --no-cache-dir --upgrade pip
    pip install --no-cache-dir "${to_install[@]}"
else
    echo "3-所有依赖已安装，跳过安装步骤。"
fi

# 根据架构确定CANN版本
ARCH=$(uname -m)
case "${ARCH}" in
     "x86_64") CANN_ARCH="x86_64" ;;
     "aarch64") CANN_ARCH="aarch64" ;;
     *) echo "Unsupported architecture: ${ARCH}" && exit 1 ;;
esac

CANN_TOOLKIT_URL="https://ascend-repo.obs.cn-east-2.myhuaweicloud.com/Milan-ASL/Milan-ASL%20V100R001C22B800TP013/Ascend-cann-toolkit_8.2.RC1.alpha001_linux-${CANN_ARCH}.run"
CANN_KERNELS_URL="https://ascend-repo.obs.cn-east-2.myhuaweicloud.com/Milan-ASL/Milan-ASL%20V100R001C22B800TP013/Ascend-cann-kernels-910b_8.2.RC1.alpha001_linux-${CANN_ARCH}.run"
CANN_INSTALL_DIR="/usr/local/Ascend"

## 下载CANN安装包
if [ ! -f ~/Ascend-cann-toolkit.run ]; then
    echo "4-下载CANN Toolkit..."
    wget "${CANN_TOOLKIT_URL}" -O ~/Ascend-cann-toolkit.run --no-check
else
    echo "4-CANN_Toolkit已下载"
fi

if [ ! -f ~/Ascend-cann-kernels.run ]; then
    echo "4-下载CANN KERNELS..."
    wget "${CANN_KERNELS_URL}" -O ~/Ascend-cann-kernels.run --no-check
else
    echo "4-CANN_KERNELS已下载"
fi

## 安装CANN Toolkit
if [ ! -d "/usr/local/Ascend/ascend-toolkit/latest" ]; then
    echo "5-CANN Toolkit正在安装..."
    chmod +x ~/Ascend-cann-toolkit.run
    ~/Ascend-cann-toolkit.run --quiet --install --install-for-all
    #rm -f ~/Ascend-cann-toolkit.run

    chmod +x ~/Ascend-cann-kernels.run
    ~/Ascend-cann-kernels.run --quiet --install --install-for-all
    #rm -f ~/Ascend-cann-kernels.run
    
else
    echo "5-CANN Toolkit 已经安装，跳过安装步骤..."
fi

# 设置CANN环境变量
export ASCEND_TOOLKIT_HOME="${CANN_INSTALL_DIR}/ascend-toolkit/latest"
export LD_LIBRARY_PATH="/usr/local/Ascend/driver/lib64/common/:/usr/local/Ascend/driver/lib64/driver/:${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe/op_tiling:${LD_LIBRARY_PATH}"
export PYTHONPATH="${ASCEND_TOOLKIT_HOME}/python/site-packages:${ASCEND_TOOLKIT_HOME}/opp/built-in/op_impl/ai_core/tbe:${PYTHONPATH}"
export PATH="${ASCEND_TOOLKIT_HOME}/bin:${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin:${ASCEND_TOOLKIT_HOME}/tools/ccec_compiler/bin:${PATH}"
export ASCEND_HOME_PATH="${ASCEND_TOOLKIT_HOME}"

# 验证安装
echo "===安装完成==="
echo "Python版本：$(python --version)"
echo "Pip版本：$(pip --version)"
echo "CANN工具包路径：${ASCEND_TOOLKIT_HOME}"

rm -rf /tmp/*
```
