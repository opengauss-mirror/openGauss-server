# libsmartscan 

## 安装

1.获取Libsmartscan_5.1.0_openEuler_aarch64.tar.gz。

2.解压tar包，创建log目录。

```
tar -zxvf Libsmartscan_5.1.0_openEuler_aarch64.tar.gz
cd Libsmartscan_5.1.0_openEuler_aarch64
mkdir log
```

3.添加如下环境变量：

```
export UCX_NET_DEVCES=enp132s0 #enp132s0为libsmartscan监听ip对应的网口
export UCX_TLS=tcp
export UCX_IB_REG_METHODS=rcache,odp,direct
export LD_LIBRARY_PATH=/path/to/Libsmartscan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty
/rpc/openEuler_2003_armlib:$LD_LIBRARY_PATH
```

4.配置参数，启动libsmartscan

```
./libsmartscan
```

## 配置参数说明

### logPath

**参数说明**：参数值为字符串，该参数为日志文件写入路径。

**取值范围**：字符串

**默认值**：./log

### logLevel

**参数说明**：参数值为枚举字符串，该参数为日志打印级别。

**取值范围**：ERROR | DEBUG | WARNING | INFO

**默认值**：ERROR

### dataPath

**参数说明**：参数值为字符串，该参数为开发人员单机环境DEBUG调试使用。

**取值范围**：字符串

**默认值**：无

### ip

**参数说明**：参数值为字符串，该参数为libsmartscan监听ip。

**取值范围**：字符串

**默认值**：127.0.0.1

### port

**参数说明**：参数值为整数，该参数为libsmartscan监听端口。

**取值范围**：[0, 65535]

**默认值**：6060

### threadNum

**参数说明**：参数值为整数，该参数为libsmartscan工作线程个数。

**取值范围**：[1, 64]

**默认值**：4

### cephConfPath

**参数说明**：参数值为字符串，该参数为ceph集群配置文件ceph.conf路径，ceph.conf默认安装路径为"/etc/ceph/ceph.conf"。

**取值范围**：字符串

**默认值**：无

### shareBuffers

**参数说明**：参数值为整数，该参数为预留参数，无实意。

### certPath

**参数说明**：参数值为字符串，该参数仅在开启SSL时有效，指定CA证书路径。

**取值范围**：字符串

**默认值**：无

### privateKeyPath

**参数说明**：参数值为字符串，该参数仅在开启SSL时有效，指定private key路径。

**取值范围**：字符串

**默认值**：无

### keypass

**参数说明**：参数值为字符串，该参数仅在开启SSL时有效，指定keypass路径。

**取值范围**：字符串

**默认值**：无
