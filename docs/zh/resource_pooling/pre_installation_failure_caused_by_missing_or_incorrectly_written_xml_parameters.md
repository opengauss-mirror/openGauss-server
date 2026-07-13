# 因xml参数缺失或写入错误导致的预安装失败问题

## 一、问题现象

执行预安装出现以下几类报错信息：

参数错误。

```shell
[GAUSS-51258] : The parameter [dss_ssl_enabl] in the XML file is an incorrect parameter.
```

参数缺失。

```shell
[GAUSS-50012] : The parameter 'dss_home' value can't be empty.
```

参数值错误。

```shell
[GAUSS-50026] : Failed to check dss_ssl_enable parameters in the XML file. It's must be on or off
```

```shell
[GAUSS-50419] : Failed to obtain the public volume 'dat' in 'data:/dev/disk/by-id/scsi-36382028100ed96ac1ec2dd6c000000d2,p0:/dev/disk/by-id/scsi-36382028100ed96ac1ec2dd6c000000d3'.
```

## 二、定位方法

根据报错信息，查看xml文件对应内容是否有问题。

## 三、问题根因

xml文件参数有误。xml必填参数说明如下。

整体信息必须配置以下内容。

```shell
<!-- 整体信息 -->
<CLUSTER>
<!-- 数据库名称 -->
<PARAM name="clusterName" value="Cluster_template" />  
<!-- 数据库节点名称(hostname) -->
<PARAM name="nodeNames" value="node1_hostname,node2_hostname" />
<!-- 数据库安装目录-->
<PARAM name="gaussdbAppPath" value="/opt/huawei/install/app" />
<!--数据库core文件目录-->
<PARAM name="corePath" value="/opt/huawei/corefile"/>
<!-- 节点IP，与nodeNames一一对应，所有节点ip类型要一致（ipv4或ipv6）-->
<PARAM name="backIp1s" value="192.168.0.1,192.168.0.2"/>
<!-- 资源池化下必须配置 -->
<!-- 资源池化模式开关 -->
<PARAM name="enable_dss" value="on"/>
<!-- dss实例目录 -->
<PARAM name="dss_home" value="/opt/huawei/install/data/dss"/>
<!-- dss共享卷名 -->
<PARAM name="ss_dss_vg_name" value="data"/>
<!-- dss挂载卷组名和卷组信息，包含共享卷 -->
<PARAM name="dss_vg_info" value="data:/dev/sdb,p0:/dev/sdc,p1:/dev/sdd"/>
<!-- cm投票卷 -->
<PARAM name="votingDiskPath" value="/dev/sde"/>
<!-- cm共享卷 -->
<PARAM name="shareDiskDir" value="/dev/sdf"/>
```

主节点上必须配置以下信息。

```shell
<DEVICELIST>
<!-- 节点1上的部署信息 -->
<DEVICE sn="node1_hostname">
<!-- 节点1的主机名称 -->
<PARAM name="name" value="node1_hostname" />
<!-- 节点1所在的AZ及AZ优先级 -->
<PARAM name="azName" value="AZ1"/>
<PARAM name="azPriority" value="1"/>
<!-- 节点1的IP，如果服务器只有一个网卡可用，将backIP1和sshIP1配置成同一个IP -->
<PARAM name="backIp1" value="192.168.0.1"/>

<!-- 如果cm存在 -->
<PARAM name="cmsNum" value="1"/> 
<PARAM name="cmServerPortBase" value="15000"/>  
<PARAM name="cmServerLevel" value="1"/> 
<PARAM name="cmServerRelation" value="node1_hostname,node2_hostname"/> 
<PARAM name="cmDir" value="/opt/huawei/data/cmserver"/>

<!-- dn -->
<PARAM name="dataNum" value="1"/>
<PARAM name="dataNode1" value="/opt/huawei/install/data/dn,node2_hostname,/opt/huawei/install/data/dn"/> 
```

备节点上必须配置以下内容。

```shell
<DEVICE sn="node2_hostname">
<PARAM name="name" value="node2_hostname"/>
<PARAM name="azName" value="AZ1"/>
<PARAM name="azPriority" value="1"/>
<PARAM name="backIp1" value="192.168.0.2"/>
<!-- cm -->
<PARAM name="cmDir" value="/opt/huawei/install/cm"/> 
```

## 四、解决方法

修改xml文件，重新执行预安装。
