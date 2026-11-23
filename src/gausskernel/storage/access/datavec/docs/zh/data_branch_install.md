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
git clone --recursive -b neon_release_9129 https://atomgit.com/opengauss/neon.git
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

# 数据默认保存在 neon/.neon
./target/release/neon_local init
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

先从 `endpoint list` 确认 branch1 的数据库端口，将下面的 `<branch1_pg_port>` 替换为实际值，再连接并验证继承的数据：

```bash
gsql -d postgres -U cloud_admin -p <branch1_pg_port> -h 127.0.0.1
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

### 1. 选择架构和镜像

发布镜像仓库为 `swr.cn-north-4.myhuaweicloud.com/kunpeng-ai`，按宿主机架构选择同一批次的存储、计算镜像：

| 架构 | 存储及控制侧镜像 | 计算侧镜像 |
| --- | --- | --- |
| ARM64 / aarch64 | `og_storage:arm` | `og_compute:arm` |
| x86_64 / amd64 | `og_storage:x86` | `og_compute:x86` |

`og_storage` 包含 control plane、pageserver、safekeeper 等服务；`og_compute` 包含 openGauss、`compute_ctl` 和 Neon 扩展。以下部署脚本会拉取所选架构的镜像，并分别设置本地运行别名 `og_storage:latest`、`og_compute:latest`，供当前部署配置使用。

### 2. 安装并启动

宿主机需安装 Docker、Docker Compose v2、`curl` 和 `python3`。全新环境通过安装脚本获取部署文件，再由部署脚本初始化 control plane、启动存储服务并创建默认 main endpoint：

```bash
curl -fsSL https://raw.gitcode.com/opengauss/neon/raw/neon_release_9129/deployment/deploy_docker/install.sh -o install.sh
bash install.sh

# ARM64 / aarch64
bash openGauss-branching-deploy/deploy.sh arm
```

x86_64 环境将最后一条命令替换为：

```bash
bash openGauss-branching-deploy/deploy.sh x86
```

已有匹配的本地运行别名镜像时，可增加 `--skip-pull` 跳过拉取。已有环境的 1.0→1.1 升级按下一节操作，不重新执行清空状态的初始化流程。对象存储、镜像交付及部署配置详见 Neon 仓库 `deployment/deploy_docker/README.md`。

### 3. 日常管理与连接

日常管理使用部署目录内的 `bin/docker_local`，由它调用 control plane 管理动态 endpoint：

```bash
cd openGauss-branching-deploy/docker-compose
bin/docker_local status
bin/docker_local endpoint list
bin/docker_local logs pageserver --tail 100
```

默认端口需按部署模式区分：

| 场景或服务 | 默认端口及含义 |
| --- | --- |
| 本地源码部署 main | `55432`，数据库连接端口 |
| Docker 一键部署 main | 宿主机 `55433`，数据库连接端口；`3080`，compute HTTP 端口 |
| Docker control plane | 宿主机 `127.0.0.1:8080`，管理 API |
| Docker pageserver | `9898`，HTTP 管理端口 |
| storage broker | `50051`，broker gRPC 端口 |

endpoint 的端口可由配置或创建参数指定，端口占用时也可能调整，连接时以 `endpoint list/status` 的实际映射为准。实验环境中的 main `55432`、dev1 `55435`、main2 `55436` 等是具体环境的 endpoint 端口，不能替代所有 Docker 部署的默认值。

默认 Docker main 的宿主机连接示例：

```bash
gsql -d postgres -U cloud_admin -p 55433 -h 127.0.0.1
```

停止时保留已有配置和数据：

```bash
bin/docker_local stop --include-control-plane
```

完整的启停、扩容及 endpoint 管理命令见 Neon 仓库 `docker-compose/README.md`。

## 三、neon 扩展从 1.0 升级到 1.1

本节仅说明数据库内 neon 扩展从 `1.0` 到 `1.1` 的 SQL 升级。前置条件是运行环境已确认兼容，并已提供匹配的新版扩展库和升级脚本；计算节点、存储组件、控制工具应使用兼容的交付版本。

容器镜像更新与扩展 SQL 升级是两项操作：仅执行 `docker pull` 不会更新运行中的容器，也不会自动执行 `ALTER EXTENSION`。更换镜像通常需要停止并重新创建相应容器、保留持久化数据，但目前尚未完成整套旧镜像到新镜像环境的跨版本兼容与升级验证，本节不提供镜像原地升级或滚动升级流程。`arm`、`x86` tag 表示镜像架构，不能据此判断版本升级兼容性。

`deploy.sh` 包含初始化及默认 tenant/endpoint 创建流程，不能直接作为已有环境的升级工具。需要更新程序或镜像时，先按对应交付版本已确认的兼容流程准备好运行环境，再执行以下扩展升级步骤。

### 1. 升级前准备

1. 安排维护窗口，暂停相关分支写入和新的增量合并。已有 `blocked` 合并先完成冲突决议并 continue，或通过 abort 终止，具体命令见[增量分支合并](./data_branch_delta_merge.md#八-人工冲突处理继续与终止)。
2. 记录 tenant、timeline、endpoint、worker 维护的 database 和当前扩展版本，备份配置及数据。Docker 环境保留部署目录、`deploy.env`、`.neon` 和相关持久化存储；源码环境保留 `.neon` 与 endpoint 配置。
3. 确认计算节点实际使用的安装目录同时提供匹配的 `neon.so`、`neon--1.1.sql` 和 `neon--1.0--1.1.sql`。更新扩展库后需要重启计算节点加载新库；镜像或程序环境的更新须先完成兼容性确认，不能只修改 SQL 文件中的版本号。

升级期间不执行 `init --force=remove-all-contents`，也不删除 `.neon`、业务数据或 oggit 元数据。

### 2. 执行扩展升级

新版本计算节点可连接后，以有扩展升级权限的管理员连接每个安装了 neon 的数据库；增量合并使用的数据库应与该 endpoint 的 `--oggit-database` 一致。先检查已安装版本及可用版本：

```sql
SELECT extname, extversion
FROM pg_extension
WHERE extname = 'neon';

SELECT version, installed
FROM pg_available_extension_versions
WHERE name = 'neon'
ORDER BY version;
```

当前已安装版本为 `1.0`，且可用版本中包含 `1.1` 时执行：

```sql
ALTER EXTENSION neon UPDATE TO '1.1';

SELECT extname, extversion
FROM pg_extension
WHERE extname = 'neon';
-- 预期 extversion 为 1.1
```

已是 `1.1` 的数据库无需重复升级。若找不到升级路径或脚本，先检查计算节点实际使用的安装目录、镜像及交付文件，不使用 DROP/CREATE EXTENSION 代替升级。

### 3. 升级后检查

按原有部署方式重新启用 Oggit，保持原来的 database 选择；对参与增量合并的两端分别确认扩展版本及 worker 状态：

```sql
SELECT timeline_id, status, decode_lsn, scanned_lsn, last_error
FROM oggit.state;
```

确认分支身份正确、worker 为 `active`，并等待扫描进度追齐，再执行一次增量 diff 核对结果后恢复合并。保留升级前备份和日志；升级脚本若失败，先根据数据库错误修正原因，再确认事务和扩展版本后重试。

## 四、常见问题

### 1. `protoc` 版本过低

Neon 编译要求 `protoc` 版本不低于 3.15。如果系统仓库版本过低，请安装更新版本的 protobuf 编译器后重新执行 `make`。

### 2. 源码编译过程中Cargo 拉 crates.io 依赖超时

用国内镜像

### 3. 端口冲突

先按本节端口表确认连接的是数据库端口还是管理端口，再使用 `ss -ltnp` 检查监听进程，并用 `ss -tanop` 检查 TIME_WAIT 等连接状态。若服务端口落在系统 `ip_local_port_range` 内，出站连接也可能临时占用该端口；可由管理员将服务端口加入 `ip_local_reserved_ports`，保留已有保留端口配置，或在创建 endpoint 时选择临时端口范围外的端口。实际数据库端口以 endpoint 返回信息为准。
