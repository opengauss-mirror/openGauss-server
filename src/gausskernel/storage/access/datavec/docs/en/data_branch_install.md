# Data Branch Quick Installation

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:09.552Z pushedAt=2026-07-30T12:23:58.587Z -->

The data branch capability is built upon the Neon storage-compute disaggregated architecture, with adaptations for the openGauss compute node. The source-code method is ideal for local debugging, whereas the Docker method is more suitable for rapidly provisioning a fully functional environment that supports connections, writes, and branch isolation validation.

## Compilation and Deployment via Source Code

### Preparations

#### Environment Requirements

openEuler 22.03 is recommended.

#### Basic Dependencies

Install the dependencies required for compiling openGauss and Neon (openEuler 22.03 as an example):

```bash
yum install -y \
  libtool readline-devel zlib-devel flex bison libseccomp-devel openssl-devel \
  clang pkgconfig  postgresql-devel  postgresql cmake protobuf-compiler \
  protobuf-devel  libcurl-devel openssl python3 python3-pip lsof  libicu-devel
```

Install Rust. The project reads the `rust-toolchain.toml` file from the source code and automatically uses the corresponding Rust version.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.bashrc
```

Verify that the `protoc` version is not lower than 3.15:

```bash
protoc --version
```

#### openGauss binarylibs

Compiling openGauss requires the third-party dependency binarylibs. The following uses openEuler 22.03 (AArch64) as an example.

```bash
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz
tar -zxvf openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz
mv openGauss-third_party_binarylibs_openEuler_2203_arm 3rd_og
```

If you are using a different CPU architecture, download the openGauss binarylibs that match openEuler 22.03 and your current architecture, and name the extracted directory as `3rd_og`.

### Pull Source Code

Pull the source code and submodules of the `neon_release_9129` branch:

```bash
git clone --recursive -b neon_release_9129 https://gitcode.com/opengauss/neon.git
cd neon
git submodule update --init --recursive
```

### Compilation and Installation

Configure environment variables:

```bash
export NEON_BASE= # Path to data branching
export CODE_BASE=$NEON_BASE/vendor/openGauss
export BINARYLIBS=  # Path to openGauss third-party binarylibs
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

Compile the Neon components, the openGauss V702 compute node, and the Neon extension:

```bash
BUILD_TYPE=release make -j64
```

After the compilation is complete, the key artifacts are as follows:

- `target/release/neon_local`: local data branch control tool.
- `target/release/pageserver`, `target/release/safekeeper`, `target/release/storage_broker`: storage-side components.
- `og_install/V702`: openGauss installation directory with Neon adaptation.
- `og_install/V702/lib/postgresql/neon.so`: Neon extension on the openGauss side.

### Start Local Data Branch Environment

Initialize the local environment:

```bash
cd neon

./target/release/neon_local init (data path located at neon/.neon)
./target/release/neon_local start
```

Create the default `tenant`, `main` branch, and compute node:

```bash
./target/release/neon_local tenant create --set-default
./target/release/neon_local endpoint create main
./target/release/neon_local endpoint start main
./target/release/neon_local endpoint list
```

By default, `main` listens on `127.0.0.1:55432`, with the user `cloud_admin` and the database `postgres`.

### Verify the Data Branch

Connect to the `main` branch and write data:

```bash
gsql -d postgres -U cloud_admin -p 55432 -h 127.0.0.1
```

```sql
DROP TABLE IF EXISTS branch_demo;
CREATE TABLE branch_demo(id int primary key, note text);
INSERT INTO branch_demo VALUES (1, 'main');
SELECT * FROM branch_demo ORDER BY id;
```

Create a new branch and start an independent compute node for it:

```bash
./target/release/neon_local timeline branch1 --branch-name branch1
./target/release/neon_local endpoint create branch1 --branch-name branch1
./target/release/neon_local endpoint start branch1
./target/release/neon_local endpoint list
```

Connect to the new branch and verify that the existing data from the `main` branch is inherited:

```bash
gsql -d postgres -U cloud_admin -p 55435 -h 127.0.0.1
```

```sql
SELECT * FROM branch_demo ORDER BY id;
INSERT INTO branch_demo VALUES (2, 'branch1');
SELECT * FROM branch_demo ORDER BY id;
```

Reconnect to the `main` branch and verify that the data written in the new branch does not affect the `main` branch:

```bash
gsql -d postgres -U cloud_admin -p 55432 -h 127.0.0.1
```

```sql
SELECT * FROM branch_demo ORDER BY id;
```

The `main` branch should contain only the data with `id = 1`, while the new branch should contain the data with `id = 1` and `id = 2`.

### Stop the Environment

Stop the local data branch service:

```bash
./target/release/neon_local endpoint stop main
./target/release/neon_local stop
```

If re-initialization is needed, you can delete the `.neon` directory after confirming that the local data is no longer required:

```bash
rm -rf .neon
```

## Deployment via Docker

### Preparations

Docker deployment uses the following images by default:

| Image | Description |
| --- | --- |
| `neon:latest_opgs` | Storage-side and control-side service image, containing `storage_broker`, `pageserver`, `safekeeper`, and `endpoint_storage`. |
| `compute-node-opengauss-v702:latest` | openGauss compute node image, containing the compute startup script, `compute_ctl`, openGauss V702, and the Neon extension. |

### Start All Components

Enter the `Docker Compose` directory and start the service:

```bash
cd docker-compose
OG_VERSION=V702 \
NEON_IMAGE=neon:latest_opgs \
COMPUTE_IMAGE=compute-node-opengauss-v702:latest \
docker compose -f docker-compose.yml up -d
```

Wait for the compute node to be ready:

```bash
docker compose -f docker-compose.yml logs -f compute_is_ready
```

The following log indicates that compute is ready for connection:

```text
All computes are started
```

### View Status and Logs

```bash
docker compose -f docker-compose.yml ps
docker compose -f docker-compose.yml logs -f pageserver
docker compose -f docker-compose.yml logs -f safekeeper1
docker compose -f docker-compose.yml logs -f compute1
```

### Stop and Clean Up

Stop and remove the container while preserving the `.neon` data directory:

```bash
docker compose -f docker-compose.yml down
```

## FAQs

### `protoc` Version Is Too Low

Neon compilation requires `protoc` version no lower than 3.15. If the system repository version is too low, install a newer version of the protobuf compiler and then re-execute `make`.

### Cargo crates.io Timeout During Source Compilation

Use a domestic mirror.

### 3. Port Conflict

The deployment via source uses port `55432` by default; Docker deployment maps ports `55433`, `9898`, and `50051`. If a port is already occupied, stop the occupying process, or adjust the port when creating an endpoint or modifying `docker-compose.yml`.
