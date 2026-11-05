# Pre-installation Failure Caused by Missing or Incorrect XML Parameters

## Symptom

The following errors may appear during pre-installation:

**Invalid parameter name:**

```shell
[GAUSS-51258] : The parameter [dss_ssl_enabl] in the XML file is an incorrect parameter.
```

**Missing parameter:**

```shell
[GAUSS-50012] : The parameter 'dss_home' value can't be empty.
```

**Invalid parameter value:**

```shell
[GAUSS-50026] : Failed to check dss_ssl_enable parameters in the XML file. It's must be on or off
```

```shell
[GAUSS-50419] : Failed to obtain the public volume 'dat' in 'data:/dev/disk/by-id/scsi-36382028100ed96ac1ec2dd6c000000d2,p0:/dev/disk/by-id/scsi-36382028100ed96ac1ec2dd6c000000d3'.
```

## Troubleshooting Method

Inspect the `cluster_config.xml` file based on the error message to verify the correctness of the corresponding parameters.

## Root Cause

The XML configuration contains errors. The following describe the mandatory parameters required for a resource pool deployment.

Global Configuration

```shell
<!-- Global information -->
<CLUSTER>
<!-- Database name -->
<PARAM name="clusterName" value="Cluster_template" />  
<!-- Database node hostnames -->
<PARAM name="nodeNames" value="node1_hostname,node2_hostname" />
<!-- Database installation directory -->
<PARAM name="gaussdbAppPath" value="/opt/huawei/install/app" />
<!-- Database core file directory -->
<PARAM name="corePath" value="/opt/huawei/corefile"/>
<!-- Node IPs (must correspond to nodeNames; all IPs must be of the same type: IPv4 or IPv6) -->
<PARAM name="backIp1s" value="192.168.0.1,192.168.0.2"/>
<!-- Required for Resource Pooling -->
<!-- Enable resource pooling mode -->
<PARAM name="enable_dss" value="on"/>
<!-- DSS instance directory -->
<PARAM name="dss_home" value="/opt/huawei/install/data/dss"/>
<!-- DSS shared volume group name -->
<PARAM name="ss_dss_vg_name" value="data"/>
<!-- DSS volume group mapping (includes shared volumes) -->
<PARAM name="dss_vg_info" value="data:/dev/sdb,p0:/dev/sdc,p1:/dev/sdd"/>
<!-- CM voting disk -->
<PARAM name="votingDiskPath" value="/dev/sde"/>
<!-- CM shared disk directory -->
<PARAM name="shareDiskDir" value="/dev/sdf"/>
```

Primary Node Configuration

```shell
<DEVICELIST>
<!-- Deployment on node 1 -->
<DEVICE sn="node1_hostname">
<!-- Node 1 hostname -->
<PARAM name="name" value="node1_hostname" />
<!-- AZ name and priority -->
<PARAM name="azName" value="AZ1"/>
<PARAM name="azPriority" value="1"/>

<!-- Node IP (if only one NIC is available, set backIp1 and sshIP1 to the same value) -->
<PARAM name="backIp1" value="192.168.0.1"/>

<!-- cm (if available) -->
<PARAM name="cmsNum" value="1"/>
<PARAM name="cmServerPortBase" value="15000"/>
<PARAM name="cmServerLevel" value="1"/>
<PARAM name="cmServerRelation" value="node1_hostname,node2_hostname"/>
<PARAM name="cmDir" value="/opt/huawei/data/cmserver"/>

<!-- dn -->
<PARAM name="dataNum" value="1"/>
<PARAM name="dataNode1" value="/opt/huawei/install/data/dn,node2_hostname,/opt/huawei/install/data/dn"/>
```

Standby Node Configuration

```shell
<DEVICE sn="node2_hostname">
<PARAM name="name" value="node2_hostname"/>
<PARAM name="azName" value="AZ1"/>
<PARAM name="azPriority" value="1"/>
<PARAM name="backIp1" value="192.168.0.2"/>
<!-- cm -->
<PARAM name="cmDir" value="/opt/huawei/install/cm"/> 
```

## Solution

Correct the parameters in the XML configuration file, then re-run the pre-installation process.
