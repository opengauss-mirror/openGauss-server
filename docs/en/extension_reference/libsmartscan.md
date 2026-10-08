# libsmartscan

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:38.891Z pushedAt=2026-09-21T03:28:40.537Z -->

## Installation

1. Obtain Libsmartscan_5.1.0_openEuler_aarch64.tar.gz.

2. Extract the tar package and create the log directory.

```
tar -zxvf Libsmartscan_5.1.0_openEuler_aarch64.tar.gz
cd Libsmartscan_5.1.0_openEuler_aarch64
mkdir log
```

3. Add the following environment variables:

```
export UCX_NET_DEVCES=enp132s0 #enp132s0 is the network interface corresponding to the libsmartscan listening IP
export UCX_TLS=tcp
export UCX_IB_REG_METHODS=rcache,odp,direct
export LD_LIBRARY_PATH=/path/to/Libsmartscan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty
/rpc/openEuler_2003_armlib:$LD_LIBRARY_PATH
```

4. Configure parameters and start libsmartscan

```
./libsmartscan
```

## Configuration Parameter Description

### logPath

**Parameter Description**: The parameter value is a string, specifying the log file write path.

**Value Range**: String

**Default Value**: ./log

### logLevel

**Parameter Description**: The parameter value is an enumerated string, which specifies the log printing level.

**Value Range**: ERROR | DEBUG | WARNING | INFO

**Default Value**: ERROR

### dataPath

**Parameter Description**: The parameter value is a string. This parameter is used for DEBUG debugging in a single-machine development environment.

**Value Range**: string

**Default value**: None

### ip

**Parameter Description**: The parameter value is a string. This parameter specifies the listening IP for libsmartscan.

**Value Range**: string

**Default Value**: 127.0.0.1

### port

**Parameter Description**: The parameter value is an integer. This parameter specifies the listening port of libsmartscan.

**Value Range**: [0, 65535]

**Default Value**: 6060

### threadNum

**Parameter Description**: The parameter value is an integer, indicating the number of worker threads for libsmartscan.

**Value Range**: [1, 64]

**Default value**: 4

### cephConfPath

**Parameter Description**: The parameter value is a string. This parameter specifies the path to the ceph cluster configuration file ceph.conf. The default installation path of ceph.conf is "/etc/ceph/ceph.conf".

**Value Range**: string

**Default Value**: None

### shareBuffers

**Parameter Description**: The parameter value is an integer. This parameter is reserved and has no actual meaning.

### certPath

**Parameter Description**: The parameter value is a string. This parameter takes effect only when SSL is enabled, and specifies the CA certificate path.

**Value Range**: String

**Default Value**: None

### privateKeyPath

**Parameter Description**: The parameter value is a string. This parameter is valid only when SSL is enabled, and specifies the private key path.

**Value Range**: string

**Default Value**: none

### keypass

**Parameter Description**: The parameter value is a string. This parameter takes effect only when SSL is enabled, and specifies the keypass path.

**Value Range**: String

**Default Value**: None