# 数据分支快速安装

数据分支基于 Neon 存储计算分离架构，并适配 openGauss 计算节点。源码方式便于本地调试；Docker 方式更适合快速启动一套可连接、可写入、可验证分支隔离的环境。

## 一、源码编译部署

### 1. 准备工作

#### 环境要求

建议使用 openEuler 22.03

#### 安装基础依赖

以 openEuler 22.03 为例，安装编译 openGauss 与 Neon 所需依赖：

```bash
yum install -y \
  libtool readline-devel zlib-devel flex bison libseccomp-devel openssl-devel \
  clang pkgconfig  postgresql-devel  postgresql cmake protobuf-compiler \
  protobuf-devel  libcurl-devel openssl python3 python3-pip lsof  libicu-devel
```

安装 Rust。项目会读取源码中的 `rust-toolchain.toml`，自动使用对应的 Rust 工具链版本。

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.bashrc
```

确认 `protoc` 版本不低于 3.15：

```bash
protoc --version
```

#### 准备 openGauss binarylibs

openGauss 编译需要第三方依赖 binarylibs。以 openEuler 22.03 aarch64 环境为例：

```bash
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz
tar -zxvf openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz
mv openGauss-third_party_binarylibs_openEuler_2203_arm 3rd_og
```

如果使用其他 CPU 架构，请下载与 openEuler 22.03 和当前架构匹配的 openGauss binarylibs，并将解压目录命名为 `3rd_og`。

### 2. 拉取源码

拉取 `neon_release_9129` 分支源码及子模块：

```bash
git clone --recursive -b neon_release_9129 https://gitcode.com/opengauss/neon.git
cd neon
git submodule update --init --recursive
```

### 3. 编译安装
配置环境变量
```bash
export NEON_BASE= # data branching的路径
export CODE_BASE=$NEON_BASE/vendor/openGauss
export BINARYLIBS=  # openGauss三方包binarylibs的路径
export THIRD_BIN_PATH=$BINARYLIBS
export GAUSSHOME=$NEON_BASE/og_install/V702
export GCC_PATH=$BINARYLIBS/buildtools/gcc10.3
export CC=$GCC_PATH/gcc/bin/gcc
export CXX=$GCC_PATH/gcc/bin/g++
export THIRD__PART=$BINARYLIBS/dependency/
export LD_LIBRARY_PATH=$THIRD_PART/kerberos/comm/lib:$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib/:$GCC_PATH/mpfr/lib/:$GCC_PATH/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH
export OPENGAUSS_BINARYLIBS_DIR=$BINARYLIBS
```

执行如下命令编译 Neon 组件、openGauss V702 计算节点和 `neon` 扩展：

```bash
BUILD_TYPE=release make -j64
```

编译完成后，关键产物如下：

- `target/release/neon_local`：本地数据分支控制工具。
- `target/release/pageserver`、`target/release/safekeeper`、`target/release/storage_broker`：存储侧组件。
- `og_install/V702`：带 Neon 适配的 openGauss 安装目录。
- `og_install/V702/lib/postgresql/neon.so`：openGauss 侧 Neon 扩展。

### 4. 启动本地数据分支环境

初始化本地环境：

```bash
cd neon

./target/release/neon_local init (数据路径在neon/.neon)
./target/release/neon_local start
```

创建默认 tenant、main 分支和计算节点：

```bash
./target/release/neon_local tenant create --set-default
./target/release/neon_local endpoint create main
./target/release/neon_local endpoint start main
./target/release/neon_local endpoint list
```

默认情况下，main 计算节点监听 `127.0.0.1:55432`，用户为 `cloud_admin`，数据库为 `postgres`。

### 5. 验证数据分支

连接 main 分支并写入数据：

```bash
gsql -d postgres -U cloud_admin -p 55432 -h 127.0.0.1
```

```sql
DROP TABLE IF EXISTS branch_demo;
CREATE TABLE branch_demo(id int primary key, note text);
INSERT INTO branch_demo VALUES (1, 'main');
SELECT * FROM branch_demo ORDER BY id;
```

创建一个新分支，并为该分支启动独立计算节点：

```bash
./target/release/neon_local timeline branch --branch-name branch1
./target/release/neon_local endpoint create branch1 --branch-name branch1
./target/release/neon_local endpoint start branch1
./target/release/neon_local endpoint list
```

连接新分支，验证 main 分支已有数据会被继承：

```bash
gsql -d postgres -U cloud_admin -p 55435 -h 127.0.0.1
```

```sql
SELECT * FROM branch_demo ORDER BY id;
INSERT INTO branch_demo VALUES (2, 'branch1');
SELECT * FROM branch_demo ORDER BY id;
```

重新连接 main 分支，验证新分支写入的数据不会影响 main 分支：

```bash
gsql -d postgres -U cloud_admin -p 55432 -h 127.0.0.1
```

```sql
SELECT * FROM branch_demo ORDER BY id;
```

main 分支应只包含 `id = 1` 的数据，新分支应包含 `id = 1` 和 `id = 2` 的数据。

### 6. 停止环境

停止本地数据分支服务：

```bash
./target/release/neon_local endpoint stop main
./target/release/neon_local stop
```

如果需要重新初始化，可在确认不再需要本地数据后删除 `.neon` 目录：

```bash
rm -rf .neon
```

## 二、Docker 部署

### 1. 准备工作

Docker 部署默认使用以下镜像：

| 镜像 | 说明 |
| --- | --- |
| `neon:latest_opgs` | 存储侧和控制侧服务镜像，包含 `storage_broker`、`pageserver`、`safekeeper`、`endpoint_storage` 等组件 |
| `compute-node-opengauss-v702:latest` | openGauss 计算节点镜像，包含 compute 启动脚本、`compute_ctl`、openGauss V702 和 Neon 扩展 |


### 2. 启动全部组件

进入 Docker Compose 目录并启动服务：

```bash
cd docker-compose
OG_VERSION=V702 \
NEON_IMAGE=neon:latest_opgs \
COMPUTE_IMAGE=compute-node-opengauss-v702:latest \
docker compose -f docker-compose.yml up -d
```

等待计算节点就绪：

```bash
docker compose -f docker-compose.yml logs -f compute_is_ready
```

看到如下日志表示 compute 已可连接：

```text
All computes are started
```


### 3. 查看状态和日志

```bash
docker compose -f docker-compose.yml ps
docker compose -f docker-compose.yml logs -f pageserver
docker compose -f docker-compose.yml logs -f safekeeper1
docker compose -f docker-compose.yml logs -f compute1
```

### 4. 停止和清理

停止并删除容器，保留 `.neon` 数据目录：

```bash
docker compose -f docker-compose.yml down
```

## 三、常见问题

### 1. `protoc` 版本过低

Neon 编译要求 `protoc` 版本不低于 3.15。如果系统仓库版本过低，请安装更新版本的 protobuf 编译器后重新执行 `make`。

### 2. 源码编译过程中Cargo 拉 crates.io 依赖超时

用国内镜像

### 3. 端口冲突

源码部署默认使用 `55432` 等端口；Docker 部署默认映射 `55433`、`9898`、`50051` 等端口。如果端口已被占用，请停止占用进程，或在创建 endpoint / 修改 `docker-compose.yml` 时调整端口。
