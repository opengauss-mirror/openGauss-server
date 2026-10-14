# Resource Pooling Installation and Usage

## Manual Installation Example<a name="section188071153201818"></a>

Compared to traditional `gs_initdb` database creation, resource pooling divides directories into three types: **instance-exclusive non-shared**, **instance-exclusive shared**, and **cluster-wide shared**. Directories requiring sharing must reside on storage array devices, while non-shared directories must reside on local disks. When building a standby node, only directories belonging to that instance need to be created; there is no need to recreate the cluster-wide shared directory structure. Resource pooling introduces new GUC parameters and switches the system table storage format from heap to segment-page.

> [!NOTE]
> The manual steps below are intended for engineering verification after compilation. For standard openGauss deployment, refer to the *Installation Guide*.

### Prerequisites

- The project has been compiled successfully. For compilation instructions, see [Compiling the Version](https://docs.opengauss.org/en/docs/latest/compilation_guide/compiling_the_version.html).
- The host has mounted the storage array LUNs, the Ultrapath multipathing software is installed, and the storage devices are available.

### Procedure

1. <a name="li0988195961613"></a>Create symbolic links for the storage array LUNs and assign appropriate user permissions (assuming the LUNs are `/dev/sde` and `/dev/sdf`).

   ```
   sudo ln -s /dev/sde /dev/tpcc_data
   sudo ln -s /dev/sdf /dev/tpcc_log
   sudo chmod 777 /dev/tpcc_data
   sudo chmod 777 /dev/tpcc_log
   ```

2. Grant raw I/O capabilities to the executable requiring storage access.

   ```
   sudo setcap CAP_SYS_RAWIO+ep /absolute/path/to/perctrl
   ```

   > **perctrl**: A utility used to grant read/write permissions to DSS-related tools and processes.

3. Create the configuration files required for the DSS server process and database initialization.

   Assuming the test directory is `/data/test`:

   ```
   └─dss_home/
       ├── cfg
       │   ├── dss_inst.ini
       │   └── dss_vg_conf.ini
       └── log  // The log directory must exist before startup
   ```

   **dss_inst.ini configuration:**

   ```
   INST_ID=0
   _LOG_LEVEL=55
   _LOG_BACKUP_FILE_COUNT=128
   _LOG_MAX_FILE_SIZE=20M
   LSNR_PATH=/data/test/dss_home
   STORAGE_MODE=SHARE_DISK
   _SHM_KEY=12
   ```

   **Parameter Descriptions:**

   - **INST_ID**: Instance ID. Range: [0, 63]. Must be unique for each `dssserver` process in the cluster.
   - **_LOG_LEVEL**: Log verbosity level.
   - **_LOG_BACKUP_FILE_COUNT**: Maximum number of backup log files retained.
   - **_LOG_MAX_FILE_SIZE**: Maximum size of a single log file.
   - **LSNR_PATH**: Directory for Unix Domain Sockets used for client-server communication. Usually set to the DSS home directory.
   - **STORAGE_MODE**: Storage type. Set to `SHARE_DISK` for storage arrays.
   - **_SHM_KEY**: Shared memory key. Must be unique for each DSS instance.

   **dss_vg_conf.ini configuration:**

   ```
   data:/dev/tpcc_data
   log:/dev/tpcc_log
   ```

   This maps the `+data` directory to `/dev/tpcc_data` and `+log` to `/dev/tpcc_log`. Note that a leading `+` prefix distinguishes DSS volumes from standard file system paths. DSS functions similarly to a distributed file system.

4. Initialize the Volume Groups (VGs) on the storage array using the `dsscmd` utility (similar to initializing a file system on a raw disk).

   ```
   # Clear the beginning of the LUNs
   dd if=/dev/zero bs=2048 count=100000 of=/dev/tpcc_data
   dd if=/dev/zero bs=2048 count=100000 of=/dev/tpcc_log

   # Create VGs
   dsscmd cv -g data -v /dev/tpcc_data -s 2048 -D /data/ss_test/dss_home
   dsscmd cv -g log  -v /dev/tpcc_log -s 65536 -D /data/ss_test/dss_home

   # Start the DSS server
   dssserver -D /data/ss_test/dss_home &
   ```

5. <a name="li23296624419"></a>Initialize the resource pooling database using `gs_initdb`.

   ```
   gs_initdb -D /data/ss_test/dn_primary --nodename=single_node -w ****** \
     --vgname="+data,+log" \
     --enable-dss \
     --dms_url="0:127.0.0.1:1611,1:127.0.0.1:1711" \
     -I 0 \
     --socketpath="UDS:/data/ss_test/dss_home/.dss_unix_d_socket"
   ```

   **New Parameters:**

   - **--vgname**: Specifies the volume groups for the database. Names must match `dss_vg_conf.ini` and be prefixed with `+`.
   - **--enable-dss**: Indicates that the database will be created within DSS.
   - **--dms_url**: DMS communication URLs. Format: `instance_id:ip:port`.
   - **-I**: Current instance ID. Range: [0, 63].
   - **--socketpath**: Path to the Unix Domain Socket for DSS client-server communication.

6. Upon successful creation, start the database process using `gs_ctl`.

   ```
   gs_ctl start -D /data/ss_test/dn_primary
   ```

7. Repeat the above steps to deploy the standby node(s).

---

## RDMA Usage Example<a name="ZH-CN_TOPIC_0000002148262710"></a>

The resource pooling feature provides real-time consistent read capabilities on standby nodes. Page exchanges between the primary and standby can be accelerated via RDMA. This section describes how to enable RDMA communication in a resource pooling environment.

### Installation Preparation<a name="ZH-CN_TOPIC_0000002183621657"></a>

#### Obtaining the Installation Package<a name="zh-cn_topic_0000002082250312_section186941117112519"></a>

- Obtain the HCOM installation package from the openGauss community.
- The `libhcom4db.so` library is included in the standard openGauss installation package. To compile it manually, obtain the source code from [Gitee](https://gitcode.com/opengauss/hcom4db).

#### Environment Requirements<a name="zh-cn_topic_0000002082250312_section14726153115253"></a>

**Table 1** Environment Requirements

| Item | Configuration | Description |
| :--- | :--- | :--- |
| **NIC** | Determined by actual hardware (e.g., Mellanox CX4/CX5 series). | Must support RDMA and Ethernet. RDMA protocols must be enabled. For configuration details, see the [*RDMA Network Guide*](https://opengauss.org/en/blogs/RDMA/RDMA_Network_Guide.html). |
| **Switch** | - | RDMA protocol is used by default; a lossless network configuration is required. |
| **Server** | - | RDMA protocol is used by default; a lossless network configuration is required. |
| **OS** | Arm: openEuler 22.03 LTS | Due to minor kernel version differences, incompatibility errors may occur during component installation. You may need to modify the OS `weak-modules` script. See [*Modification_of_the_weak-modules_Script_for_OS_Compatibility*](https://opengauss.org/en/blogs/weak-modules/Modification_of_the_weak-modules_Script_for_OS_Compatibility.html). |
| **Software** | MLNX_OFED_LINUX | Required for RDMA NIC drivers. Refer to Table 2 for OS/OFED compatibility. |
| | openGauss-All-{version}-openEuler22.03-aarch64.tar.gz | openGauss installation package. |
| | BoostKit-hcom_1.0.0_openeuler2003_aarch64.tar.gz | HCOM installation package. |

**Table 2** OS and OFED Compatibility

| OS Version | OFED Version |
| :--- | :--- |
| openEuler 22.03 LTS | MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-aarch64.tgz |

### Installation and Uninstallation<a name="ZH-CN_TOPIC_0000002148420794"></a>

Place the required software in the specified paths, set the environment variables, deploy HCOM4DB, update the openGauss configuration, and restart the database to enable RDMA.

#### Prerequisites<a name="zh-cn_topic_0000002082095562_section1544415522710"></a>

- RDMA requires CX4/CX5 NICs and the corresponding drivers.
- openGauss is deployed. The default database administrator is `omm`.
- The HCOM package matching the OS and architecture has been obtained (e.g., `BoostKit-hcom_1.0.0_openeuler2003_aarch64.tar.gz`).
- The `${GAUSSHOME}/lib` directory exists on all nodes and contains `libhcom4db.so`.

#### Procedure<a name="zh-cn_topic_0000002082095562_section16893155672411"></a>

1. Log in as the `omm` user and upload the HCOM package to a directory writable only by `omm`.

2. Extract the package:

   ```bash
   tar -xzvf BoostKit-hcom_1.0.0_openeuler2003_aarch64.tar.gz --no-same-owner
   ```

3. Copy `libhcom.so` to `${GAUSSHOME}/lib`.

   > [!NOTE]
   > It is recommended to minimize the permissions of the `.so` file based on business requirements.

   ```bash
   cp -f BoostKit-hcom_1.0.0_openeuler2003_aarch64/hcom/lib/libhcom.so ${GAUSSHOME}/lib/
   ```

4. Set the environment variables for the RPC library and HCOM4DB:

   ```bash
   export OCK_RPC_LIB_PATH=${GAUSSHOME}/lib
   export HCOM4DB_LIB_PATH=${GAUSSHOME}/lib
   ```

5. Modify `postgresql.conf`:

   a. Open the file:

   ```bash
   vim ${PGDATA}/postgresql.conf
   ```

   b. Press `i` to enter insert mode. Set `ss_interconnect_type` to `RDMA` and add the `ss_rdma_work_config` parameter:

   ```ini
   ss_interconnect_type=RDMA
   ss_rdma_work_config='6 10'  # OCK RDMA uses user-space poll mode, binding to CPUs [6, 10]
   ```

   c. Press `Esc`, type `:wq!`, and press `Enter` to save and exit.

6. (Optional) Stop the `om_monitor` process if it is running:

   ```bash
   gs_om -t killmonitor
   ```

7. Restart the database:

   ```bash
   cm_ctl stop
   cm_ctl start
   ```

   > [!NOTE]
   > If startup fails, check the logs at `postgresql-YYYY-MM-DD_HHMMSS.log` in the openGauss log directory.

---

## SCRLock Usage Example

### Introduction

SCRLock (Smart Cached Remote Lock) is a distributed locking SDK featuring a local lock cache. This section describes how to install and use the SCRLock feature in openGauss to enhance distributed lock performance in resource pooling scenarios.

### Installation Preparation

#### Obtaining the Installation Package

Obtain the SCRLock installation package from the openGauss community.

#### Environment Requirements

**Table 1** Environment Requirements

| Item | Configuration | Description |
| :--- | :--- | :--- |
| **NIC** | Mellanox CX4/CX5 series. | Must support RDMA and Ethernet. See [RDMA_Network_Guide](https://opengauss.org/en/blogs/RDMA/RDMA_Network_Guide.html). |
| **OS** | **Arm:** openEuler 22.03 LTS, 20.03 LTS, 20.03 LTS SP1, 20.03 LTS SP3.<br>**x86:** openEuler 22.03 LTS, 20.03 LTS, 20.03 LTS SP1, 20.03 LTS SP3. | Minor kernel version differences may cause incompatibility errors during component installation. You may need to modify the OS `weak-modules` script. See [Modification_of_the_weak-modules_Script_for_OS_Compatibility](https://opengauss.org/en/blogs/weak-modules/Modification_of_the_weak-modules_Script_for_OS_Compatibility.html). |
| **Software** | MLNX_OFED_LINUX | Required for RDMA NIC drivers. See <a href="#zh-cn_topic_0000002082250312_table364115286293">Table 2</a> for compatibility. |
| | SCRLock Packages (Examples):<br>- OCK_scrlock_openEuler-22.03-LTS-aarch64.tar.gz<br>- OCK_scrlock_openEuler-20.03-LTS-x86_64.tar.gz | Select the package matching your OS and architecture. |

**Table 2** OS and OFED Compatibility
<a name="zh-cn_topic_0000002082250312_table364115286293"></a>

| OS Version | OFED Version (x86) | OFED Version (Arm) |
| :--- | :--- | :--- |
| openEuler 20.03 LTS | MLNX_OFED_LINUX-5.4-3.6.8.1-openeuler20.03-x86_64.tgz | MLNX_OFED_LINUX-5.4-3.1.0.0-openeuler20.03-aarch64.tgz |
| openEuler 20.03 LTS SP1 | MLNX_OFED_LINUX-5.4-3.6.8.1-openeuler20.03sp1-x86_64.tgz | MLNX_OFED_LINUX-5.4-3.1.0.0-openeuler20.03sp1-aarch64.tgz |
| openEuler 20.03 LTS SP3 | MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler20.03sp3-x86_64.tgz | MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler20.03sp3-aarch64.tgz |
| openEuler 22.03 LTS | MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-x86_64.tgz | MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-aarch64.tgz |

### Installation and Uninstallation

#### One-Click SCRLock Deployment

SCRLock provides a simplified deployment script. By inputting the installation path, user, and node information, you can deploy the feature with a single command.

##### Prerequisites

- RDMA requires CX4/CX5 NICs.
- openGauss is deployed.
- The SCRLock package matching the OS and architecture has been obtained.
- The `${GAUSSHOME}/lib` directory exists on all nodes.
- All servers must share the same username and password, and the user must have privileges to execute `rmmod`, `rpm`, `depmod`, and `modprobe`.

##### Procedure

1. Log in as the `<SCRLock-install-user>`, upload the package, and extract it:

   ```bash
   tar -xzvf OCK_scrlock_openEuler-20.03-LTS-SP1-aarch64.tar.gz
   ```

   The directory contents are as follows:

   | File | Description |
   | :--- | :--- |
   | OCK_scrlock_openEuler_aarch64.tar.gz | Source archive |
   | OCK_scrlock_openEuler_aarch64.tar.gz.txt | Signature file |
   | OCK_scrlock_openEuler_aarch64.tar.gz.cms | Manifest file |

2. Extract the source archive:

   ```bash
   tar -xzvf OCK_scrlock_openEuler_aarch64.tar.gz
   ```

   The extracted contents are as follows:

   | File/Directory | Description |
   | :--- | :--- |
   | scrlock_lib/ | Shared object files (.so) |
   | scripts/ | Deployment scripts |
   | umdk_rpm/ | RPM dependencies |
   | bin/ | Verification binaries |

3. Verify the package signature using the `verification` binary:

   ```bash
   ./bin/verification OCK_scrlock_openEuler_aarch64.tar.gz \
     OCK_scrlock_openEuler_aarch64.tar.gz.cms \
     OCK_scrlock_openEuler_aarch64.tar.gz.txt
   ```

   > [!NOTE]
   > Arguments: Source file, Signature file, Manifest file.

   **Success:**

   ```
   Starting to verify OCK_scrlock_openEuler_aarch64.tar.gz...
   Verify the source file passed.
   Verify the sha file passed.
   ```

   **Failure:**

   ```
   Starting to verify OCK_scrlock_openEuler_aarch64.tar.gz...
   Verify the source file failed.
   ```

   > [!NOTE]
   > If verification fails, the package may have been tampered with. Obtain a new package and retry.

4. Navigate to the `scripts` directory:

   ```bash
   cd scripts
   ```

5. Execute the deployment script. Enter the `<SCRLock-install-user>` password when prompted:

   ```bash
   sh scrlock_install.sh -H '192.168.4.164 192.168.4.165 192.168.4.166' \
     -U omm -G dbgrp -D /home/omm/lib
   ```

   > [!NOTE]
   > - `-H`: Cluster IP addresses (e.g., `'192.168.4.164 192.168.4.165 192.168.4.166'`).
   > - `-U`: Database administrator user (e.g., `omm`).
   > - `-G`: Database administrator group (e.g., `dbgrp`).
   > - `-D`: Path to `${GAUSSHOME}/lib` (e.g., `/home/omm/lib`).
   > - `-h`: Display help information.
   >
   > For security, disable SSH remote login for privileged users after deployment.

6. <a name="li9627181442313"></a>Switch to the database administrator user:

   ```
   su - omm
   ```

7. (Optional) Kill the `om_monitor` process:

   ```
   gs_om -t killmonitor
   ```

8. <a name="li153702610143"></a>Load environment variables:

   ```
   source ~/.bashrc
   ```

   > [!TIP]
   > Steps [6](#li9627181442313) to [8](#li153702610143) must also be performed on standby nodes.

#### Enabling the SCRLock Feature

Enable SCRLock by modifying the configuration file and restarting the database.

##### Prerequisites

- openGauss with SCRLock support is installed.
- The one-click SCRLock deployment has been completed.

##### Procedure

1. Log in as the database administrator.

2. Edit `postgresql.conf`:

   a. Open the file:

   ```
   vim postgresql.conf
   ```

   b. Press `i` to enter insert mode. Modify the parameters according to **Table 1**:

   ```
   ss_enable_scrlock = off
   ss_enable_srclock_sleep_mode = off
   ss_scrlock_server_port = 8000
   ss_scrlock_worker_count = 2
   ss_scrlock_worker_bind_core = ''
   ss_scrlock_server_bind_core = ''
   ```

   **Table 1** SCRLock Configuration Parameters

   | Parameter | Type | Description | Range | Default |
   | :--- | :--- | :--- | :--- | :--- |
   | **ss_enable_scrlock** | Boolean | Enables or disables SCRLock. | `on`, `off` | `off` |
   | **ss_enable_scrlock_sleep_mode** | Boolean | Enables or disables sleep mode. | `on`, `off` | `on` |
   | **ss_scrlock_server_port** | Integer | SCRLock server listening port. | 1024–65535 | 8000 |
   | **ss_scrlock_worker_count** | Integer | Number of SCRLock client workers. | 2–16 | 2 |
   | **ss_scrlock_worker_bind_core** | String | CPU affinity for workers (Start End). | e.g., `"10 15"` | `""` |
   | **ss_scrlock_server_bind_core** | String | CPU affinity for the server. | e.g., `"10 15"` | `""` |

   c. Press `Esc`, type `:wq!`, and press `Enter` to save and exit.

3. Restart the database:

   ```
   cm_ctl stop
   cm_ctl start
   ```

   > [!NOTE]
   > If startup fails, check `postgresql-YYYY-MM-DD_HHMMSS.log` in the openGauss log directory.

4. Verify the configuration:

   ```
   gsql -d postgres -p 16600 -c "show ss_enable_scrlock"
   ```

   **Expected Output:**

   ```
    ss_enable_scrlock
   -------------------
    on
   (1 row)
   ```

   > [!NOTE]
   > - Adjust the port (`-p`) as needed.
   > - View SCRLock logs: `tail -fn20 ${GAUSSLOG}/pg_log/scrlock*.log`

#### Disabling the SCRLock Feature

Disable SCRLock by updating the configuration and restarting the database.

##### Procedure

1. Edit `postgresql.conf`:

   a. Open the file:

   ```
   vim postgresql.conf
   ```

   b. Press `i` to enter insert mode. Set `ss_enable_scrlock` to `off`:

   ```
   ss_enable_scrlock = off
   ```

   c. Press `Esc`, type `:wq!`, and press `Enter` to save and exit.

2. Stop the database:

   ```
   cm_ctl stop
   ```

3. (Optional) Uninstall SCRLock by removing the library and environment variable:

   ```
   rm -f /home/omm/lib/libscrlock.so
   unset OCK_SCRLOCK_LIB_PATH
   ```

   > Replace `/home/omm/lib/` with the actual directory containing `libscrlock.so`.

4. Restart the database:

   ```
   cm_ctl start
   ```
