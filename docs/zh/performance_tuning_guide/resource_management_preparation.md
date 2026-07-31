# 资源管理准备

## 资源规划

完成资源负载管理功能配置前，需要先根据业务模型完成租户资源的规划。业务运行一段时间后，可以根据资源的使用情况再进行配置调整。

本章节我们假设某大型企业内的两个部门共用同一套集群，openGauss通过将同一个部门需要使用的系统资源集合划分为系统的一个租户，以此来实现不同部门间的资源隔离，其资源规划如[表1](#table65031957184315)所示。

**表 1**  租户资源规划

<a name="table65031957184315"></a>
<table><thead align="left"><tr id="row115181157114318"><th class="cellrowborder" valign="top" width="22%" id="mcps1.2.4.1.1"><p id="p0518165716436"><a name="p0518165716436"></a><a name="p0518165716436"></a>租户名称</p>
</th>
<th class="cellrowborder" valign="top" width="36%" id="mcps1.2.4.1.2"><p id="p165181857124313"><a name="p165181857124313"></a><a name="p165181857124313"></a>参数名称</p>
</th>
<th class="cellrowborder" valign="top" width="42%" id="mcps1.2.4.1.3"><p id="p185187579437"><a name="p185187579437"></a><a name="p185187579437"></a>取值样例</p>
</th>
</tr>
</thead>
<tbody><tr id="row195181457114317"><td class="cellrowborder" rowspan="6" valign="top" width="22%" headers="mcps1.2.4.1.1 "><p id="p1799971812487"><a name="p1799971812487"></a><a name="p1799971812487"></a>租户A</p>
</td>
<td class="cellrowborder" valign="top" width="36%" headers="mcps1.2.4.1.2 "><p id="p175181057144310"><a name="p175181057144310"></a><a name="p175181057144310"></a>子Class控制组</p>
</td>
<td class="cellrowborder" valign="top" width="42%" headers="mcps1.2.4.1.3 "><p id="p165181571431"><a name="p165181571431"></a><a name="p165181571431"></a>class_a</p>
</td>
</tr>
<tr id="row103291178311"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p0329571434"><a name="p0329571434"></a><a name="p0329571434"></a>Workload控制组</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul89077212245"></a><a name="ul89077212245"></a><ul id="ul89077212245"><li>workload_a1</li><li>workload_a2</li></ul>
</td>
</tr>
<tr id="row55751516115619"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p6575111695613"><a name="p6575111695613"></a><a name="p6575111695613"></a>组资源池</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="p157514169567"><a name="p157514169567"></a><a name="p157514169567"></a>resource_pool_a</p>
</td>
</tr>
<tr id="row1751820572434"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p165186571434"><a name="p165186571434"></a><a name="p165186571434"></a>业务资源池</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul7142870243"></a><a name="ul7142870243"></a><ul id="ul7142870243"><li>resource_pool_a1</li><li>resource_pool_a2</li></ul>
</td>
</tr>
<tr id="row201731314587"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p101734141386"><a name="p101734141386"></a><a name="p101734141386"></a>组用户</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="p121730140817"><a name="p121730140817"></a><a name="p121730140817"></a>tenant_a</p>
</td>
</tr>
<tr id="row115161431174810"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p145461425164511"><a name="p145461425164511"></a><a name="p145461425164511"></a>业务用户</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul1898617116247"></a><a name="ul1898617116247"></a><ul id="ul1898617116247"><li>tenant_a1</li><li>tenant_a2</li></ul>
</td>
</tr>
<tr id="row118451473485"><td class="cellrowborder" rowspan="6" valign="top" width="22%" headers="mcps1.2.4.1.1 "><p id="p17466925610"><a name="p17466925610"></a><a name="p17466925610"></a>租户B</p>
</td>
<td class="cellrowborder" valign="top" width="36%" headers="mcps1.2.4.1.2 "><p id="p1564210501334"><a name="p1564210501334"></a><a name="p1564210501334"></a>子Class控制组</p>
</td>
<td class="cellrowborder" valign="top" width="42%" headers="mcps1.2.4.1.3 "><p id="p1664225010310"><a name="p1664225010310"></a><a name="p1664225010310"></a>class_b</p>
</td>
</tr>
<tr id="row78015432319"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p1864295012316"><a name="p1864295012316"></a><a name="p1864295012316"></a>Workload控制组</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul136891815172411"></a><a name="ul136891815172411"></a><ul id="ul136891815172411"><li>workload_b1</li><li>workload_b2</li></ul>
</td>
</tr>
<tr id="row19513104514565"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p35132045135615"><a name="p35132045135615"></a><a name="p35132045135615"></a>组资源池</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="p1551310459564"><a name="p1551310459564"></a><a name="p1551310459564"></a>resource_pool_b</p>
</td>
</tr>
<tr id="row5845875486"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p13530171144812"><a name="p13530171144812"></a><a name="p13530171144812"></a>业务资源池</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul1067320214242"></a><a name="ul1067320214242"></a><ul id="ul1067320214242"><li>resource_pool_b1</li><li>resource_pool_b2</li></ul>
</td>
</tr>
<tr id="row38278915911"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p148270916912"><a name="p148270916912"></a><a name="p148270916912"></a>组用户</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="p198271694919"><a name="p198271694919"></a><a name="p198271694919"></a>tenant_b</p>
</td>
</tr>
<tr id="row6296155612482"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="p11765165913485"><a name="p11765165913485"></a><a name="p11765165913485"></a>业务用户</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><a name="ul1957912265246"></a><a name="ul1957912265246"></a><ul id="ul1957912265246"><li>tenant_b1</li><li>tenant_b2</li></ul>
</td>
</tr>
</tbody>
</table>

## 启动资源负载管理功能

### 背景信息<a name="section4704103619115"></a>

使用资源负载管理功能前，需要参考本节完成参数配置。

### 前提条件<a name="section102673333318"></a>

- 在openGauss中，如果需要对系统资源进行管理，用户需要拥有DBA权限。通过执行如下语法查询哪些用户拥有该权限：

    ```
    openGauss=# SELECT rolname FROM pg_roles WHERE rolsystemadmin = 't';
     rolname
    ---------
     omm
     Jack
    (2 rows)
    ```

- 如果想要将一个用户纳入资源负载管理的范围，则此用户必须具有login权限。通过执行如下语法查询哪些用户拥有该权限：

    ```
    openGauss=# SELECT rolname FROM pg_roles WHERE rolcanlogin = 't';
     rolname
    ---------
     omm
    (1 row)
    ```

>[!TIP]须知
>如果一个用户的login权限被取消，那么他的resource pool将会自动修改为default\_pool。default\_pool的详细介绍请参见[表2](#创建资源池#zh-cn_topic_0066854608_table57723085173126)。

### 操作步骤<a name="section344124715313"></a>

DBA权限用户可以通过如下步骤启动基于资源池的资源负载管理。此处以omm用户为例进行描述。

1. 以操作系统用户omm登录openGauss主节点。
2. 开启基于资源池的资源负载管理功能。

    ```
    gs_guc set -N all -I all -c "use_workload_manager=on"
    ```

3. 重启数据库使参数设置生效。

    ```
    gs_om -t stop && gs_om -t start
    ```

## 设置控制组

### 背景信息<a name="section4704103619115"></a>

openGauss资源负载管理的核心是资源池，而配置资源池首先要在环境中实现控制组Cgroups的设置。更多Cgroups的原理介绍，请查看相关操作系统的产品手册。openGauss的控制组请参考[查看控制组的信息](#zh-cn_topic_0066854607_s66a16734a4e54c00abaaa1cc44c82c89)。

Class控制组为数据库业务运行所在的顶层控制组，集群部署时会自动生成默认子Class控制组“DefaultClass”。DefaultClass的Medium控制组会含有系统触发的作业在运行，该控制组不允许进行资源修改，且运行在该控制组上的作业不受资源管理的控制，所以推荐创建新的子Class及其Workload控制组来设置资源比例。

### 前提条件<a name="section1034014512269"></a>

已熟悉《工具与命令参考》中“服务端工具 \> gs\_cgroup”章节和“服务端工具 \> gs\_ssh”章节的使用。

### 操作步骤<a name="zh-cn_topic_0066854607_section5658359019124"></a>

>[!NOTE]说明
>
>- 在openGauss中，需要在每个集群节点上执行控制组的创建、更新、删除操作，才能实现对整个集群资源的控制，所以下述步骤中都使用《工具与命令参考》中“服务端工具 \> gs\_ssh”命令执行。
>
>- 控制组的命名要求如下：
>
>   - 无论是子Class控制组还是Workload控制组，都不允许在名称中包含字符“：”。
>   - 不可以创建同名的控制组。

**创建子Class控制组和Workload控制组**

1. 以操作系统用户omm登录openGauss主节点。
2. 创建名称为“class\_a”和“class\_b”的子Class控制组，CPU资源配额分别为Class的40%和20%。

    ```
    gs_ssh -c "gs_cgroup -c -S class_a -s 40"
    ```

    ```
    gs_ssh -c "gs_cgroup -c -S class_b -s 20"
    ```

3. 创建子Class控制组“class\_a”下名称为“workload\_a1”和“workload\_a2”的Workload控制组，CPU资源配额分别为“class\_a”控制组的20%和60%。

    ```
    gs_ssh -c "gs_cgroup -c -S class_a -G workload_a1 -g 20 "
    ```

    ```
    gs_ssh -c "gs_cgroup -c -S class_a -G workload_a2 -g 60 "
    ```

4. 创建子Class控制组“class\_b”下名称为“workload\_b1”和“workload\_b2”的Workload控制组，CPU资源配额分别为“class\_b”控制组的50%和40%。

    ```
    gs_ssh -c "gs_cgroup -c -S class_b -G workload_b1 -g 50 "
    ```

    ```
    gs_ssh -c "gs_cgroup -c -S class_b -G workload_b2 -g 40 "
    ```

**更新控制组的资源配额**

1. 更新“class\_a”控制组的CPU资源配额为30%。

    ```
    gs_ssh -c "gs_cgroup -u -S class_a -s 30"
    ```

2. 更新“class\_a”下的“workload\_a1”的CPU资源配额为“class\_a”的30%。

    ```
    gs_ssh -c "gs_cgroup -u -S class_a -G workload_a1 -g 30"
    ```

    >[!TIP]须知
    >调整后的Workload控制组“workload\_a1”占有的CPU资源不应大于其对应的子Class控制组“class\_a”。并且，此名称不能是Timeshare Cgroup的默认名称，如“Low”、“Medium”、“High”或“Rush”。

**删除控制组**

1. 删除控制组“class\_a”。

    ```
    gs_ssh -c "gs_cgroup -d  -S class_a"
    ```

    以上操作可以删除控制组“class\_a”。

    >[!TIP]须知
    >root用户或者具有root访问权限的用户指定“-d” 和“-U username”删除普通用户“username”可访问的默认Cgroups。普通用户指定“-d”和“-S classname”可以删除已有的Class Cgroups。

### 查看控制组的信息<a name="zh-cn_topic_0066854607_s66a16734a4e54c00abaaa1cc44c82c89"></a>

1. 查看配置文件中控制组信息。

    ```
    gs_cgroup -p 
    ```

    控制组配置信息

    ```
    gs_cgroup -p
    
    Top Group information is listed:
    GID:   0 Type: Top    Percent(%): 1000( 50) Name: Root                  Cores: 0-47
    GID:   1 Type: Top    Percent(%):  833( 83) Name: Gaussdb:omm           Cores: 0-20
    GID:   2 Type: Top    Percent(%):  333( 40) Name: Backend               Cores: 0-20
    GID:   3 Type: Top    Percent(%):  499( 60) Name: Class                 Cores: 0-20
    
    Backend Group information is listed:
    GID:   4 Type: BAKWD  Name: DefaultBackend   TopGID:   2 Percent(%): 266(80) Cores: 0-20
    GID:   5 Type: BAKWD  Name: Vacuum           TopGID:   2 Percent(%):  66(20) Cores: 0-20
    
    Class Group information is listed:
    GID:  20 Type: CLASS  Name: DefaultClass     TopGID:   3 Percent(%): 166(20) MaxLevel: 1 RemPCT: 100 Cores: 0-20
    GID:  21 Type: CLASS  Name: class1           TopGID:   3 Percent(%): 332(40) MaxLevel: 2 RemPCT:  70 Cores: 0-20
    
    Workload Group information is listed:
    GID:  86 Type: DEFWD  Name: grp1:2           ClsGID:  21 Percent(%):  99(30) WDLevel:  2 Quota(%): 30 Cores: 0-5
    
    Timeshare Group information is listed:
    GID: 724 Type: TSWD   Name: Low              Rate: 1
    GID: 725 Type: TSWD   Name: Medium           Rate: 2
    GID: 726 Type: TSWD   Name: High             Rate: 4
    GID: 727 Type: TSWD   Name: Rush             Rate: 8
    
    Group Exception information is listed:
    GID:  20 Type: EXCEPTION Class: DefaultClass
    PENALTY: QualificationTime=1800 CPUSkewPercent=30
    
    GID:  21 Type: EXCEPTION Class: class1
    PENALTY: AllCpuTime=100 QualificationTime=2400 CPUSkewPercent=90
    
    GID:  86 Type: EXCEPTION Group: class1:grp1:2
    ABORT: BlockTime=1200 ElapsedTime=2400
    ```

    上述示例查看到的控制组配置信息如[表1](#zh-cn_topic_0085032167_zh-cn_topic_0059777958_t6ef2f8b1d69342eda1f26e57003015c2)所示。

    **表 1**  控制组配置信息

    <a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_t6ef2f8b1d69342eda1f26e57003015c2"></a>
    <table><thead align="left"><tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_raf32468133ec42a98fa0a24a84f6e542"><th class="cellrowborder" valign="top" width="12.42%" id="mcps1.2.6.1.1"><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35afb8adcfcc44caab1a15a95bc460f3"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35afb8adcfcc44caab1a15a95bc460f3"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35afb8adcfcc44caab1a15a95bc460f3"></a>GID</p>
    </th>
    <th class="cellrowborder" valign="top" width="13.900000000000002%" id="mcps1.2.6.1.2"><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5e63574953494fda87d121cc98444458"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5e63574953494fda87d121cc98444458"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5e63574953494fda87d121cc98444458"></a>类型</p>
    </th>
    <th class="cellrowborder" valign="top" width="15.61%" id="mcps1.2.6.1.3"><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a64f986ec452e42c284a6f32d6156dfb8"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a64f986ec452e42c284a6f32d6156dfb8"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a64f986ec452e42c284a6f32d6156dfb8"></a>名称</p>
    </th>
    <th class="cellrowborder" valign="top" width="31.55%" id="mcps1.2.6.1.4"><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae59345dfad974f2981d49561fde6edde"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae59345dfad974f2981d49561fde6edde"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae59345dfad974f2981d49561fde6edde"></a>Percent（%）信息</p>
    </th>
    <th class="cellrowborder" valign="top" width="26.52%" id="mcps1.2.6.1.5"><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_af84180bce2c64a849829b13fdb1e21d5"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_af84180bce2c64a849829b13fdb1e21d5"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_af84180bce2c64a849829b13fdb1e21d5"></a>特定信息</p>
    </th>
    </tr>
    </thead>
    <tbody><tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r40c9836246fc434cb849097be80f4238"><td class="cellrowborder" valign="top" width="12.42%" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2917dc8c27254a51a345ee36e67a1720"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2917dc8c27254a51a345ee36e67a1720"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2917dc8c27254a51a345ee36e67a1720"></a>0</p>
    </td>
    <td class="cellrowborder" rowspan="4" valign="top" width="13.900000000000002%" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a06d02d08bbc2479ab2f65b40bd7b1aa2"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a06d02d08bbc2479ab2f65b40bd7b1aa2"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a06d02d08bbc2479ab2f65b40bd7b1aa2"></a>Top控制组</p>
    </td>
    <td class="cellrowborder" valign="top" width="15.61%" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5aa339132fd84fffb152ea53482ffcad"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5aa339132fd84fffb152ea53482ffcad"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5aa339132fd84fffb152ea53482ffcad"></a>Root</p>
    </td>
    <td class="cellrowborder" valign="top" width="31.55%" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a338af691b8b349658412db97f3db8076"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a338af691b8b349658412db97f3db8076"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a338af691b8b349658412db97f3db8076"></a>1000代表总的系统资源为1000份。</p>
    <p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1581165ca9ae4dd080b5f9b82f5de2e7"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1581165ca9ae4dd080b5f9b82f5de2e7"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1581165ca9ae4dd080b5f9b82f5de2e7"></a>括号中的50代表IO资源的50%。</p>
    <p id="zh-cn_topic_0085032167_p7162175943818"><a name="zh-cn_topic_0085032167_p7162175943818"></a><a name="zh-cn_topic_0085032167_p7162175943818"></a><span id="text72654133610"><a name="text72654133610"></a><a name="text72654133610"></a>openGauss</span>不通过控制组对IO资源做控制，因此下面其他控制组信息中仅涉及CPU配额情况。</p>
    </td>
    <td class="cellrowborder" valign="top" width="26.52%" headers="mcps1.2.6.1.5 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a6eb5d42ab11f40ef961f8058258bd179"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a6eb5d42ab11f40ef961f8058258bd179"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a6eb5d42ab11f40ef961f8058258bd179"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r983cfc45212e4992b1950009f0e56504"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1eaf0bdb85924deab2806570de44f3af"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1eaf0bdb85924deab2806570de44f3af"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1eaf0bdb85924deab2806570de44f3af"></a>1</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a90c7540a2a9f46af829f8337a21fcbe7"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a90c7540a2a9f46af829f8337a21fcbe7"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a90c7540a2a9f46af829f8337a21fcbe7"></a>Gaussdb:<span id="text1785391015013"><a name="text1785391015013"></a><a name="text1785391015013"></a>omm</span></p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_abcad05a44a894c50ade6a6054e936ddf"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_abcad05a44a894c50ade6a6054e936ddf"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_abcad05a44a894c50ade6a6054e936ddf"></a>系统中只运行一套数据库程序，Gaussdb:<span id="text4458181275015"><a name="text4458181275015"></a><a name="text4458181275015"></a>omm</span>控制组默认配额为833，数据库程序和非数据库程序的比值为（833:167=5:1）。</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ab57c2123aa8a4f648fcaf14225f6c74a"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ab57c2123aa8a4f648fcaf14225f6c74a"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ab57c2123aa8a4f648fcaf14225f6c74a"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rb51a7c2bd35249f58e7520595cfb74f4"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7f152f6bf6484613a26adc92a992a612"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7f152f6bf6484613a26adc92a992a612"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7f152f6bf6484613a26adc92a992a612"></a>2</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae34f31263140431ab5b0eb6800bbe56a"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae34f31263140431ab5b0eb6800bbe56a"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ae34f31263140431ab5b0eb6800bbe56a"></a>Backend</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adcc253590a304f1eba6dbc3f56a42b31"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adcc253590a304f1eba6dbc3f56a42b31"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adcc253590a304f1eba6dbc3f56a42b31"></a>Backend和Class括号中的40和60，代表Backend占用Gaussdb:dbuser控制组40%的资源，Class占用Gaussdb:dbuser控制组60%的资源。</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a55cc1dc9b6d8417996044cd8757ef808"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a55cc1dc9b6d8417996044cd8757ef808"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a55cc1dc9b6d8417996044cd8757ef808"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rc5b04760cc9443d7894575b28d6f82bc"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a073225e9f51c4d45afb1ccfcb9c98f62"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a073225e9f51c4d45afb1ccfcb9c98f62"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a073225e9f51c4d45afb1ccfcb9c98f62"></a>3</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a94cd7a0e32c84ee9a996e6f8c9db099a"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a94cd7a0e32c84ee9a996e6f8c9db099a"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a94cd7a0e32c84ee9a996e6f8c9db099a"></a>Class</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a56886ba6fcde430f9a6eb0f257b4f3bf"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a56886ba6fcde430f9a6eb0f257b4f3bf"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a56886ba6fcde430f9a6eb0f257b4f3bf"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rebb21435963c46d69a04d0ab35e0caf8"><td class="cellrowborder" valign="top" width="12.42%" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5078655f17a24de5839b2be2076ccba1"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5078655f17a24de5839b2be2076ccba1"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a5078655f17a24de5839b2be2076ccba1"></a>4</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="13.900000000000002%" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a139683e2a3e843ea93915c9d37de3cf8"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a139683e2a3e843ea93915c9d37de3cf8"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a139683e2a3e843ea93915c9d37de3cf8"></a>Backend控制组</p>
    </td>
    <td class="cellrowborder" valign="top" width="15.61%" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a89fe583c176e448da9f169b3f01e5e27"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a89fe583c176e448da9f169b3f01e5e27"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a89fe583c176e448da9f169b3f01e5e27"></a>DefaultBackend</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="31.55%" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a632dadada0c2425298fa5621a11ca772"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a632dadada0c2425298fa5621a11ca772"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a632dadada0c2425298fa5621a11ca772"></a>括号中的80和20代表DefaultBackend和Vacuum占用Backend控制组80%和20%的资源。</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="26.52%" headers="mcps1.2.6.1.5 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac7d60f8f0b3742d19ef61e5b17b8201f"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac7d60f8f0b3742d19ef61e5b17b8201f"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac7d60f8f0b3742d19ef61e5b17b8201f"></a>TopGID：代表Top类型控制组中Backend组的GID，即2。</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r3bbdbf32c9a54aaeb216b0c132d62439"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7eae4871ce5c4b2b8ab519f7dbc3f0e8"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7eae4871ce5c4b2b8ab519f7dbc3f0e8"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7eae4871ce5c4b2b8ab519f7dbc3f0e8"></a>5</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2656aced855847baa02b9208adcfabd9"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2656aced855847baa02b9208adcfabd9"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a2656aced855847baa02b9208adcfabd9"></a>Vacuum</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r4bdc9c26155048b7b6fef177826bb6f9"><td class="cellrowborder" valign="top" width="12.42%" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aad0efd2996714e8fbad3d9d970f10017"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aad0efd2996714e8fbad3d9d970f10017"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aad0efd2996714e8fbad3d9d970f10017"></a>20</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="13.900000000000002%" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad7825e742b514ec2871344b0bc037279"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad7825e742b514ec2871344b0bc037279"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad7825e742b514ec2871344b0bc037279"></a>Class控制组</p>
    </td>
    <td class="cellrowborder" valign="top" width="15.61%" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7cf846331fb24b13b663a961e3e2905c"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7cf846331fb24b13b663a961e3e2905c"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a7cf846331fb24b13b663a961e3e2905c"></a>DefaultClass</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="31.55%" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac93ff437c8ba41ea9d7e35368d3ab5bb"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac93ff437c8ba41ea9d7e35368d3ab5bb"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ac93ff437c8ba41ea9d7e35368d3ab5bb"></a>DefaultClass和class1的20和40代表占Class控制组20%和40%的资源。因为当前只有两个Class组，所有它们按照20:40的比例分配Class控制组499的系统配额，则分别为166和332。</p>
    </td>
    <td class="cellrowborder" rowspan="2" valign="top" width="26.52%" headers="mcps1.2.6.1.5 "><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u01f01475a56e48468034a2f15ebcd156"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u01f01475a56e48468034a2f15ebcd156"></a><ul id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u01f01475a56e48468034a2f15ebcd156"><li>TopGID：代表DefaultClass和class1所属的上层控制（Top控制组中的Class组）的GID，即3。</li><li>MaxLevel：Class组当前含有的Workload组的最大层次，DefaultClass没有Workload Cgroup，其数值为1。</li><li>RemPCT:代表Class组分配Workload组后剩余的资源百分比。如class1中剩余的百分比为70。</li></ul>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rb09775a1dc284a5badceb435d1fa0deb"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a47e5ba42370049b0a39138e3b7028243"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a47e5ba42370049b0a39138e3b7028243"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a47e5ba42370049b0a39138e3b7028243"></a>21</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a952d4b454c754614961bd0acc1d8eb14"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a952d4b454c754614961bd0acc1d8eb14"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a952d4b454c754614961bd0acc1d8eb14"></a>class1</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r69f1f4dcc43042d49dbf46ac0cc7fd5a"><td class="cellrowborder" valign="top" width="12.42%" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a3d0e978aa70947b7bf8ee28f7f69ef41"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a3d0e978aa70947b7bf8ee28f7f69ef41"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a3d0e978aa70947b7bf8ee28f7f69ef41"></a>86</p>
    </td>
    <td class="cellrowborder" valign="top" width="13.900000000000002%" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a00b2084dc9164cdbb7c2152fb45144ac"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a00b2084dc9164cdbb7c2152fb45144ac"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a00b2084dc9164cdbb7c2152fb45144ac"></a>Workload控制组</p>
    </td>
    <td class="cellrowborder" valign="top" width="15.61%" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_p1643572385820"><a name="zh-cn_topic_0085032167_p1643572385820"></a><a name="zh-cn_topic_0085032167_p1643572385820"></a>grp1:2</p>
    <p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35c42c0dbaf341eda30f77c6dfe3206a"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35c42c0dbaf341eda30f77c6dfe3206a"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a35c42c0dbaf341eda30f77c6dfe3206a"></a>（该名称由Workload Cgroup Name和其在class中的层级组成，它是class1的第一个Workload组，层级为2，每个Class组最多10层Workload Cgroup。）</p>
    </td>
    <td class="cellrowborder" valign="top" width="31.55%" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa56d91049b224ed2a92027036762be85"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa56d91049b224ed2a92027036762be85"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa56d91049b224ed2a92027036762be85"></a>根据设置，其占class1的百分比为30，则为332*30%=99。</p>
    </td>
    <td class="cellrowborder" valign="top" width="26.52%" headers="mcps1.2.6.1.5 "><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u37d2117f9f64408ea81e8167d73d9153"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u37d2117f9f64408ea81e8167d73d9153"></a><ul id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_u37d2117f9f64408ea81e8167d73d9153"><li>ClsGID：代表Workload控制组所属的上层控制组（class1控制组）的GID。</li><li>WDLevel：代表当前Workload Cgroup在对应的Class组所在的层次。</li></ul>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ra51ec99f046248e4a80f1357d7cbbbf6"><td class="cellrowborder" valign="top" width="12.42%" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac66021fdd084e699cf47892c7aac50f"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac66021fdd084e699cf47892c7aac50f"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac66021fdd084e699cf47892c7aac50f"></a>724</p>
    </td>
    <td class="cellrowborder" rowspan="4" valign="top" width="13.900000000000002%" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a31622eb38f454fe4bb0e201ca2bf7af7"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a31622eb38f454fe4bb0e201ca2bf7af7"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a31622eb38f454fe4bb0e201ca2bf7af7"></a>Timeshare控制组</p>
    </td>
    <td class="cellrowborder" valign="top" width="15.61%" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aebc6436beb654da299f46f73c7c73c86"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aebc6436beb654da299f46f73c7c73c86"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aebc6436beb654da299f46f73c7c73c86"></a>Low</p>
    </td>
    <td class="cellrowborder" valign="top" width="31.55%" headers="mcps1.2.6.1.4 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac1ccc37de00462f869d63432b3ea2ed"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac1ccc37de00462f869d63432b3ea2ed"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aac1ccc37de00462f869d63432b3ea2ed"></a>-</p>
    </td>
    <td class="cellrowborder" rowspan="4" valign="top" width="26.52%" headers="mcps1.2.6.1.5 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aacc9155fa98446588808649ce29fc559"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aacc9155fa98446588808649ce29fc559"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aacc9155fa98446588808649ce29fc559"></a>Rate：代表Timeshare中的分配比例，Low最少为1，Rush最高为8。这四个Timeshare组的资源配比为Rush:High:Medium:Low=8:4:2:1</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rc218d5326a2744f3aea8ed9b5854b8ea"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ace62508ac2424abb8a994e84175e63c2"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ace62508ac2424abb8a994e84175e63c2"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ace62508ac2424abb8a994e84175e63c2"></a>725</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adf5d2ad6919d4242a0314c0d5893c4c7"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adf5d2ad6919d4242a0314c0d5893c4c7"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_adf5d2ad6919d4242a0314c0d5893c4c7"></a>Medium</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a899b0db2dbf34c108c267729c1aaa715"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a899b0db2dbf34c108c267729c1aaa715"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a899b0db2dbf34c108c267729c1aaa715"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_rc8fa48c94125496f99288554c61d6b0f"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a73d01fb5ca31424492c153ae6313011b"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a73d01fb5ca31424492c153ae6313011b"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a73d01fb5ca31424492c153ae6313011b"></a>726</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad5ed1e4abafc46a7888901c64ae77fb0"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad5ed1e4abafc46a7888901c64ae77fb0"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_ad5ed1e4abafc46a7888901c64ae77fb0"></a>High</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a931e04a7e35645719108993544a8de7b"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a931e04a7e35645719108993544a8de7b"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a931e04a7e35645719108993544a8de7b"></a>-</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_r6f7fb17cc6f7454c8a73990b1439ec2b"><td class="cellrowborder" valign="top" headers="mcps1.2.6.1.1 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aeaa5db9e07664c90994f3cc96133eedd"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aeaa5db9e07664c90994f3cc96133eedd"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aeaa5db9e07664c90994f3cc96133eedd"></a>727</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.2 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1f2edf02225d433cb2209eaaf68d3815"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1f2edf02225d433cb2209eaaf68d3815"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_a1f2edf02225d433cb2209eaaf68d3815"></a>Rush</p>
    </td>
    <td class="cellrowborder" valign="top" headers="mcps1.2.6.1.3 "><p id="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa5ee50649fe74d3d938f201dd5cdfbf3"><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa5ee50649fe74d3d938f201dd5cdfbf3"></a><a name="zh-cn_topic_0085032167_zh-cn_topic_0059777958_aa5ee50649fe74d3d938f201dd5cdfbf3"></a>-</p>
    </td>
    </tr>
    </tbody>
    </table>

2. 查看操作系统中树形结构的控制组信息。

    执行如下命令可以查询控制组树形结构信息。

    ```
    gs_cgroup -P
    ```

    返回信息如下，其中shares代表操作系统中CPU资源的动态资源配额“cpu.shares”的数值，cpus代表操作系统中CPUSET资源的动态资源限额“cpuset.cpus”的数值，指的是该控制组能够使用的核数范围。

    ```
    Mount Information:
    cpu:/dev/cgroup/cpu
    blkio:/dev/cgroup/blkio
    cpuset:/dev/cgroup/cpuset
    cpuacct:/dev/cgroup/cpuacct
    
    Group Tree Information:
    - Gaussdb:wangrui (shares: 5120, cpus: 0-20, weight: 1000)
            - Backend (shares: 4096, cpus: 0-20, weight: 400)
                    - Vacuum (shares: 2048, cpus: 0-20, weight: 200)
                    - DefaultBackend (shares: 8192, cpus: 0-20, weight: 800)
            - Class (shares: 6144, cpus: 0-20, weight: 600)
                    - class1 (shares: 4096, cpus: 0-20, weight: 400)
                            - RemainWD:1 (shares: 1000, cpus: 0-20, weight: 100)
                                    - RemainWD:2 (shares: 7000, cpus: 0-20, weight: 700)
                                            - Timeshare (shares: 1024, cpus: 0-20, weight: 500)
                                                    - Rush (shares: 8192, cpus: 0-20, weight: 800)
                                                    - High (shares: 4096, cpus: 0-20, weight: 400)
                                                    - Medium (shares: 2048, cpus: 0-20, weight: 200)
                                                    - Low (shares: 1024, cpus: 0-20, weight: 100)
                                    - grp1:2 (shares: 3000, cpus: 0-5, weight: 300)
                            - TopWD:1 (shares: 9000, cpus: 0-20, weight: 900)
                    - DefaultClass (shares: 2048, cpus: 0-20, weight: 200)
                            - RemainWD:1 (shares: 1000, cpus: 0-20, weight: 100)
                                    - Timeshare (shares: 1024, cpus: 0-20, weight: 500)
                                            - Rush (shares: 8192, cpus: 0-20, weight: 800)
                                            - High (shares: 4096, cpus: 0-20, weight: 400)
                                            - Medium (shares: 2048, cpus: 0-20, weight: 200)
                                            - Low (shares: 1024, cpus: 0-20, weight: 100)
                            - TopWD:1 (shares: 9000, cpus: 0-20, weight: 900)
    ```

3. 通过系统视图获取控制组配置信息。

    a.[使用gsql访问openGauss](https://docs.opengauss.org/zh/docs/latest/getting_started/gsql_connection_and_usage.html)数据库。
    
    b.获取系统中所有控制组的配置信息。
    
    ```
    openGauss=# SELECT * FROM gs_all_control_group_info;
    ```

## 创建资源池

### 背景信息<a name="section4704103619115"></a>

openGauss支持通过创建资源池对主机资源进行划分。开启资源负载管理之后，仅使用默认资源池并不能满足业务对资源负载管理的诉求，必须根据需要创建新的资源池，对系统资源进行重分配，来满足实际业务对系统资源精细管理的需要。普通资源池的特点见[表1](#table1223985366)。

**表 1**  普通资源池的特点

<a name="table1223985366"></a>
<table><thead align="left"><tr id="row1423917515618"><th class="cellrowborder" valign="top" width="30%" id="mcps1.2.3.1.1"><p id="p62391954610"><a name="p62391954610"></a><a name="p62391954610"></a>资源池分类</p>
</th>
<th class="cellrowborder" valign="top" width="70%" id="mcps1.2.3.1.2"><p id="p1823935866"><a name="p1823935866"></a><a name="p1823935866"></a>特点</p>
</th>
</tr>
</thead>
<tbody><tr id="row8239651361"><td class="cellrowborder" valign="top" width="30%" headers="mcps1.2.3.1.1 "><p id="p1142495621714"><a name="p1142495621714"></a><a name="p1142495621714"></a>普通资源池（普通场景）</p>
</td>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.1.2 "><a name="ul735611411475"></a><a name="ul735611411475"></a><ul id="ul735611411475"><li>关联Workload控制组。且必须没有组资源池关联该控制组对应的子class。即如果创建业务资源池关联控制组‘class1:wd’，那么必须没有组资源池关联‘class1’。</li><li>mem_percent默认为0%，没有mem_percent相加小于100%的限制。</li></ul>
</td>
</tr>
</tbody>
</table>

在开启了资源负载管理功能之后，系统会自动创建default\_pool，当一个会话或者用户没有指定关联的资源池时，都会被默认关联到default\_pool。default\_pool默认绑定DefaultClass:Medium控制组，并且不限制所关联的业务的并发数。default\_pool的详细属性如[表2](#zh-cn_topic_0066854608_table57723085173126)所示。

**表 2**  default\_pool属性

<a name="zh-cn_topic_0066854608_table57723085173126"></a>
<table><thead align="left"><tr id="zh-cn_topic_0066854608_row49357524173126"><th class="cellrowborder" valign="top" width="26.37736226377362%" id="mcps1.2.4.1.1"><p id="zh-cn_topic_0066854608_p38536494173126"><a name="zh-cn_topic_0066854608_p38536494173126"></a><a name="zh-cn_topic_0066854608_p38536494173126"></a>属性</p>
</th>
<th class="cellrowborder" valign="top" width="28.44715528447155%" id="mcps1.2.4.1.2"><p id="zh-cn_topic_0066854608_p34448329173126"><a name="zh-cn_topic_0066854608_p34448329173126"></a><a name="zh-cn_topic_0066854608_p34448329173126"></a>属性值</p>
</th>
<th class="cellrowborder" valign="top" width="45.175482451754824%" id="mcps1.2.4.1.3"><p id="zh-cn_topic_0066854608_p38851277173126"><a name="zh-cn_topic_0066854608_p38851277173126"></a><a name="zh-cn_topic_0066854608_p38851277173126"></a>说明</p>
</th>
</tr>
</thead>
<tbody><tr id="zh-cn_topic_0066854608_row59945696173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p23763245173126"><a name="zh-cn_topic_0066854608_p23763245173126"></a><a name="zh-cn_topic_0066854608_p23763245173126"></a>respool_name</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p45774682173126"><a name="zh-cn_topic_0066854608_p45774682173126"></a><a name="zh-cn_topic_0066854608_p45774682173126"></a>default_pool</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p16761743173126"><a name="zh-cn_topic_0066854608_p16761743173126"></a><a name="zh-cn_topic_0066854608_p16761743173126"></a>资源池名称。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row16637965173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p5497890173126"><a name="zh-cn_topic_0066854608_p5497890173126"></a><a name="zh-cn_topic_0066854608_p5497890173126"></a>mem_percent</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p42675923173126"><a name="zh-cn_topic_0066854608_p42675923173126"></a><a name="zh-cn_topic_0066854608_p42675923173126"></a>100</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p34197770173126"><a name="zh-cn_topic_0066854608_p34197770173126"></a><a name="zh-cn_topic_0066854608_p34197770173126"></a>最大占用内存百分比。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row39344474173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p32785810173126"><a name="zh-cn_topic_0066854608_p32785810173126"></a><a name="zh-cn_topic_0066854608_p32785810173126"></a>cpu_affinity</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p38404930173126"><a name="zh-cn_topic_0066854608_p38404930173126"></a><a name="zh-cn_topic_0066854608_p38404930173126"></a>-1</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p23791635173126"><a name="zh-cn_topic_0066854608_p23791635173126"></a><a name="zh-cn_topic_0066854608_p23791635173126"></a>CPU亲和性，保留参数。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row12798127173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p30015380173126"><a name="zh-cn_topic_0066854608_p30015380173126"></a><a name="zh-cn_topic_0066854608_p30015380173126"></a>control_group</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p15326701173126"><a name="zh-cn_topic_0066854608_p15326701173126"></a><a name="zh-cn_topic_0066854608_p15326701173126"></a>DefaultClass:Medium</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p33503267173126"><a name="zh-cn_topic_0066854608_p33503267173126"></a><a name="zh-cn_topic_0066854608_p33503267173126"></a>资源池关联的控制组。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row33093948173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p63364103173126"><a name="zh-cn_topic_0066854608_p63364103173126"></a><a name="zh-cn_topic_0066854608_p63364103173126"></a>active_statements</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p32218723173126"><a name="zh-cn_topic_0066854608_p32218723173126"></a><a name="zh-cn_topic_0066854608_p32218723173126"></a>-1</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p59579767173126"><a name="zh-cn_topic_0066854608_p59579767173126"></a><a name="zh-cn_topic_0066854608_p59579767173126"></a>资源池允许的最大并发数。-1为不限制并发数量，最大值不超过INT_MAX。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row66455862173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p14215762173126"><a name="zh-cn_topic_0066854608_p14215762173126"></a><a name="zh-cn_topic_0066854608_p14215762173126"></a>max_dop</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p10626107173126"><a name="zh-cn_topic_0066854608_p10626107173126"></a><a name="zh-cn_topic_0066854608_p10626107173126"></a>1</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p55408344173126"><a name="zh-cn_topic_0066854608_p55408344173126"></a><a name="zh-cn_topic_0066854608_p55408344173126"></a>开启SMP后，算子执行的并发度，保留参数。</p>
</td>
</tr>
<tr id="zh-cn_topic_0066854608_row28913054173126"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0066854608_p60256037173126"><a name="zh-cn_topic_0066854608_p60256037173126"></a><a name="zh-cn_topic_0066854608_p60256037173126"></a>memory_limit</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0066854608_p48900820173126"><a name="zh-cn_topic_0066854608_p48900820173126"></a><a name="zh-cn_topic_0066854608_p48900820173126"></a>8GB</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p1543508173126"><a name="zh-cn_topic_0066854608_p1543508173126"></a><a name="zh-cn_topic_0066854608_p1543508173126"></a>内存使用上限，保留参数。</p>
</td>
</tr>
<tr id="row2051142017312"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p7512142013117"><a name="p7512142013117"></a><a name="p7512142013117"></a>parentid</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p551212207317"><a name="p551212207317"></a><a name="p551212207317"></a>0</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p651217201315"><a name="p651217201315"></a><a name="p651217201315"></a>父资源池OID。</p>
</td>
</tr>
<tr id="row688518003219"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p788516019324"><a name="p788516019324"></a><a name="p788516019324"></a>io_limits</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p17885150133211"><a name="p17885150133211"></a><a name="p17885150133211"></a>0</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p2268117153212"><a name="p2268117153212"></a><a name="p2268117153212"></a>每秒触发IO的次数上限。行存单位是万次/s，列存是次/s。0表示不控制，最大值不超过INT_MAX。</p>
</td>
</tr>
<tr id="row112023317329"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p41211633153214"><a name="p41211633153214"></a><a name="p41211633153214"></a>io_priority</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p3121733103218"><a name="p3121733103218"></a><a name="p3121733103218"></a>None</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p127130562321"><a name="p127130562321"></a><a name="p127130562321"></a>IO利用率高达90%时，重消耗IO作业进行IO资源管控时关联的优先级等级。None表示不控制。</p>
</td>
</tr>
<tr id="row1982461353019"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p18253131302"><a name="p18253131302"></a><a name="p18253131302"></a>nodegroup</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p882521311305"><a name="p882521311305"></a><a name="p882521311305"></a>InstallationGuide</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p1582641383018"><a name="p1582641383018"></a><a name="p1582641383018"></a>资源池所在的逻辑集群的名称(单机下不生效)。</p>
</td>
</tr>
<tr id="row490512216308"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p79052022113010"><a name="p79052022113010"></a><a name="p79052022113010"></a>is_foreign</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p7905102214309"><a name="p7905102214309"></a><a name="p7905102214309"></a>f</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p15905022123018"><a name="p15905022123018"></a><a name="p15905022123018"></a>资源池不用于逻辑集群之外的用户(单机下不生效)。</p>
</td>
</tr>
</tbody>
</table>

>[!TIP]须知
>openGauss不允许对default\_pool参数进行修改。

### 前提条件<a name="section17352165513395"></a>

已熟悉[CREATE RESOURCE POOL](https://docs.opengauss.org/zh/docs/latest/sql_reference/create_resource_pool.html)、[ALTER RESOURCE POOL](https://docs.opengauss.org/zh/docs/latest/sql_reference/alter_resource_pool.html)和[DROP RESOURCE POOL](https://docs.opengauss.org/zh/docs/latest/sql_reference/drop_resource_pool.html)语法的使用。

### 操作过程<a name="zh-cn_topic_0066854608_section16606579202019"></a>

**创建资源池**

1. [使使用gsql访问openGauss](https://docs.opengauss.org/zh/docs/latest/getting_started/gsql_connection_and_usage.html)数据库。

2. 创建组资源池关联到指定的子Class控制组。例如下面：名称为“resource\_pool\_a”的组资源池关联到了“class\_a”控制组。

    ```
    openGauss=# CREATE RESOURCE POOL resource_pool_a WITH (control_group='class_a');
    openGauss=# CREATE RESOURCE POOL resource_pool_b WITH (control_group='class_b');
    CREATE RESOURCE POOL
    ```

3. 创建业务资源池关联到指定的Workload控制组。例如下面：名称为“resource\_pool\_a1”的业务资源池关联到了“workload\_a1”控制组。

    ```
    openGauss=# CREATE RESOURCE POOL resource_pool_a1 WITH (control_group='class_a:workload_a1');
    openGauss=# CREATE RESOURCE POOL resource_pool_a2 WITH (control_group='class_a:workload_a2');
    openGauss=# CREATE RESOURCE POOL resource_pool_b1 WITH (control_group='class_b:workload_b1');
    openGauss=# CREATE RESOURCE POOL resource_pool_b2 WITH (control_group='class_b:workload_b2');
    CREATE RESOURCE POOL
    ```

    >[!NOTE]说明
    >
    >- 如果在创建资源池的时候不指定所关联的控制组，则该资源池会被关联到默认控制组（DefaultClass控制组下的“Medium” Timeshare控制组）。
    >
    >- control\_group取值区分大小写，指定时要使用单引号或双引号。
    >
    >- 若数据库用户指定Timeshare控制组代表的字符串，即“Rush”、“High”、“Medium”或“Low”其中一种，如control\_group的字符串为“High”，代表资源池指定到DefaultClass控制组下的“High” Timeshare控制组。
    >
    >- control\_group可以指定用户创建Workload控制组，即'class1:wd'，也可以带有控制组的级别，例如：'class1:wd:2'，这个级别范围一定要在1-10的范围内，但这个级别将不做任何区分作用。在旧版本中，允许创建同名Workload控制组，以级别进行区分。但新版本升级后，不允许创建同名控制组，用户如在旧版本中已创建同名Workload控制组，使用过程中其级别将不进行区分，由此可能造成的控制组不明确使用的问题，需要用户自行把旧的同名控制组删除以明确控制组使用。

**管理资源池**

修改资源池的属性。例如下面：修改资源池“resource\_pool\_a2”关联的控制组为“class\_a:workload\_a1”（假设class\_a:workload\_a1未被其他资源池关联）。

```
openGauss=# ALTER RESOURCE POOL resource_pool_a2 WITH (control_group="class_a:workload_a1");
ALTER RESOURCE POOL
```

**删除资源池**

删除资源池。例如下面删除资源池“resource\_pool\_a2”。

```
openGauss=# DROP RESOURCE POOL resource_pool_a2;
DROP RESOURCE POOL
```

>[!NOTE]说明  
>
>- 如果某个角色已关联到该资源池，无法删除。  
>- 多租户场景下，如果删除组资源池，其业务资源池都将被删除。只有不关联用户时，资源池才能被删除。

### 查看资源池的信息<a name="zh-cn_topic_0066854608_section63579270173658"></a>

>[!TIP]须知  
>
>- 不允许使用INSERT、UPDATE、DELETE、TRUNCATE操作资源负载管理的系统表pg\_resource\_pool。  
>- 不允许修改资源池的memory\_limit和cpu\_affinity属性。

- 查看当前集群中所有的资源池信息。

    ```
    openGauss=# SELECT * FROM PG_RESOURCE_POOL;
    ```

    ```
       respool_name   | mem_percent | cpu_affinity |    control_group    | active_statements | max_dop | memory_limit | parentid | io_limits | io_priority |  nodegroup   | is_foreign  | max_worker
    ------------------+-------------+--------------+---------------------+-------------------+---------+--------------+----------+-----------+--------------+--------------+------------
     default_pool     |         100 |           -1 | DefaultClass:Medium |                -1 |       1 | 8GB          |        0 |         0 | None        | InstallationGuide | f  |
     resource_pool_a  |          20 |           -1 | class_a             |                10 |       1 | 8GB          |        0 |         0 | None        | InstallationGuide | f  |
     resource_pool_b  |          20 |           -1 | class_b             |                10 |       1 | 8GB          |        0 |         0 | None        | InstallationGuide | f  |
     resource_pool_a1 |          20 |           -1 | class_a:workload_a1 |                10 |       1 | 8GB          |    16970 |         0 | None        | InstallationGuide | f  |
     resource_pool_a2 |          20 |           -1 | class_a:workload_a2 |                10 |       1 | 8GB          |    16970 |         0 | None        | InstallationGuide | f  |
     resource_pool_b1 |          20 |           -1 | class_b:workload_b1 |                10 |       1 | 8GB          |    16971 |         0 | None        | InstallationGuide | f  |
     resource_pool_b2 |          20 |           -1 | class_b:workload_b2 |                10 |       1 | 8GB          |    16971 |         0 | None        | InstallationGuide | f  |
    (7 rows)
    ```

- 查看某个资源池关联的控制组信息，具体内容可以参考[统计信息函数](https://docs.opengauss.org/zh/docs/latest/sql_reference/statistics_information_functions.html)章节的gs\_control\_group\_info\(pool text\)函数。

    如下命令中“resource\_pool\_a1”为资源池名称。

    ```
    openGauss=# SELECT * FROM gs_control_group_info('resource_pool_a1');
    ```

    ```
            name         |  class  |  workload   | type  | gid | shares | limits | rate | cpucores
    ---------------------+---------+-------------+-------+-----+--------+--------+------+----------
     class_a:workload_a1 | class_a | workload_a1 | DEFWD |  87 |     30 |      0 |    0 | 0-3
    (1 row)
    ```

    **表 3**  gs\_control\_group\_info属性

    <a name="table1560939125613"></a>
    <table><thead align="left"><tr id="row260919925618"><th class="cellrowborder" valign="top" width="26.37736226377362%" id="mcps1.2.4.1.1"><p id="p17610179175619"><a name="p17610179175619"></a><a name="p17610179175619"></a>属性</p>
    </th>
    <th class="cellrowborder" valign="top" width="28.44715528447155%" id="mcps1.2.4.1.2"><p id="p361013935612"><a name="p361013935612"></a><a name="p361013935612"></a>属性值</p>
    </th>
    <th class="cellrowborder" valign="top" width="45.175482451754824%" id="mcps1.2.4.1.3"><p id="p461011975616"><a name="p461011975616"></a><a name="p461011975616"></a>说明</p>
    </th>
    </tr>
    </thead>
    <tbody><tr id="row116109912569"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p461079145611"><a name="p461079145611"></a><a name="p461079145611"></a>name</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p161020985611"><a name="p161020985611"></a><a name="p161020985611"></a>class_a:workload_a1</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p196108914566"><a name="p196108914566"></a><a name="p196108914566"></a>class和workload名称</p>
    </td>
    </tr>
    <tr id="row06106985619"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p7610149125612"><a name="p7610149125612"></a><a name="p7610149125612"></a>class</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p2610139185611"><a name="p2610139185611"></a><a name="p2610139185611"></a>class_a</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p4610196562"><a name="p4610196562"></a><a name="p4610196562"></a>Class控制组名称</p>
    </td>
    </tr>
    <tr id="row1399384455713"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p209946441573"><a name="p209946441573"></a><a name="p209946441573"></a>workload</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p159941447578"><a name="p159941447578"></a><a name="p159941447578"></a>workload_a1</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p20994544125713"><a name="p20994544125713"></a><a name="p20994544125713"></a>Workload控制组名称</p>
    </td>
    </tr>
    <tr id="row8632178185817"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p17633882582"><a name="p17633882582"></a><a name="p17633882582"></a>type</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p863315865812"><a name="p863315865812"></a><a name="p863315865812"></a>DEFWD</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p46338815818"><a name="p46338815818"></a><a name="p46338815818"></a>控制组类型（Top、CLASS、BAKWD、DEFWD、TSWD）</p>
    </td>
    </tr>
    <tr id="row19207812135811"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p2208812105815"><a name="p2208812105815"></a><a name="p2208812105815"></a>gid</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p11208161265812"><a name="p11208161265812"></a><a name="p11208161265812"></a>87</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p1120816121589"><a name="p1120816121589"></a><a name="p1120816121589"></a>控制组id</p>
    </td>
    </tr>
    <tr id="row01967412582"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p15196164105812"><a name="p15196164105812"></a><a name="p15196164105812"></a>shares</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p619610455819"><a name="p619610455819"></a><a name="p619610455819"></a>30</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p01966445815"><a name="p01966445815"></a><a name="p01966445815"></a>占父节点CPU资源的百分比</p>
    </td>
    </tr>
    <tr id="row1931985165714"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p183203516578"><a name="p183203516578"></a><a name="p183203516578"></a>limits</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p7320125111571"><a name="p7320125111571"></a><a name="p7320125111571"></a>0</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p43209514578"><a name="p43209514578"></a><a name="p43209514578"></a>占父节点CPU核数的百分比</p>
    </td>
    </tr>
    <tr id="row148871119165810"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p6887201917580"><a name="p6887201917580"></a><a name="p6887201917580"></a>rate</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p188871519125814"><a name="p188871519125814"></a><a name="p188871519125814"></a>0</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p1988821914586"><a name="p1988821914586"></a><a name="p1988821914586"></a>Timeshare中的分配比例</p>
    </td>
    </tr>
    <tr id="row1970717163583"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p47079162588"><a name="p47079162588"></a><a name="p47079162588"></a>cpucores</p>
    </td>
    <td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p37074160581"><a name="p37074160581"></a><a name="p37074160581"></a>0-3</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p17078161586"><a name="p17078161586"></a><a name="p17078161586"></a>CPU核心数</p>
    </td>
    </tr>
    </tbody>
    </table>
