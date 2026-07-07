# 资源管理准备<a name="ZH-CN_TOPIC_0000001193874453"></a>

## 资源规划<a name="ZH-CN_TOPIC_0000001147834736"></a>

完成资源负载管理功能配置前，需要先根据业务模型完成租户资源的规划。业务运行一段时间后，可以根据资源的使用情况再进行配置调整。

本章节我们假设某大型企业内的两个部门共用同一套数据库实例，openGauss通过将同一个部门需要使用的系统资源集合划分为系统的一个租户，以此来实现不同部门间的资源隔离，其资源规划如[表1](#table65031957184315)所示。

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

## 启动资源负载管理功能<a name="ZH-CN_TOPIC_0000001193794331"></a>

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
>
>如果一个用户的login权限被取消，那么他的resource pool将会自动修改为default\_pool。default\_pool的详细介绍请参见[表2](#创建资源池)。

### 操作步骤<a name="section344124715313"></a>

DBA权限用户可以通过如下步骤启动基于资源池的资源负载管理。此处以omm用户为例进行描述。

1. 以操作系统用户omm登录openGauss主节点。
2. 开启基于资源池的资源负载管理功能。

    ```
     gs_guc set -Z datanode -D /gaussdb/data/datanode -c "use_workload_manager=on"
    ```

3. 重启数据库使参数设置生效。

    ```
    gs_ctl restart -D /gaussdb/data/datanode
    ```

## 创建资源池<a name="ZH-CN_TOPIC_0000001193874455"></a>

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
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.1.2 "><a name="ul735611411475"></a><a name="ul735611411475"></a><ul id="ul735611411475"><li>关联Workload控制组。 且必须没有组资源池关联该控制组对应的子class。即如果创建业务资源池关联控制组‘class1:wd’，那么必须没有组资源池关联‘class1’。</li><li>mem_percent默认为0%，没有mem_percent相加小于100%的限制。</li></ul>
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
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0066854608_p59579767173126"><a name="zh-cn_topic_0066854608_p59579767173126"></a><a name="zh-cn_topic_0066854608_p59579767173126"></a>资源池允许的最大并发数。-1为不限制并发数量。</p>
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
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p2268117153212"><a name="p2268117153212"></a><a name="p2268117153212"></a>每秒触发IO的次数上限。行存单位是万次/s，列存是次/s。0表示不控制。</p>
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
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p882521311305"><a name="p882521311305"></a><a name="p882521311305"></a>installation</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p1582641383018"><a name="p1582641383018"></a><a name="p1582641383018"></a>资源池所在的逻辑数据库实例的名称。</p>
</td>
</tr>
<tr id="row490512216308"><td class="cellrowborder" valign="top" width="26.37736226377362%" headers="mcps1.2.4.1.1 "><p id="p79052022113010"><a name="p79052022113010"></a><a name="p79052022113010"></a>is_foreign</p>
</td>
<td class="cellrowborder" valign="top" width="28.44715528447155%" headers="mcps1.2.4.1.2 "><p id="p7905102214309"><a name="p7905102214309"></a><a name="p7905102214309"></a>f</p>
</td>
<td class="cellrowborder" valign="top" width="45.175482451754824%" headers="mcps1.2.4.1.3 "><p id="p15905022123018"><a name="p15905022123018"></a><a name="p15905022123018"></a>资源池不用于逻辑数据库实例之外的用户。</p>
</td>
</tr>
</tbody>
</table>

>[!TIP]须知
>
>openGauss不允许对default\_pool参数进行修改。

### 前提条件<a name="section17352165513395"></a>

已熟悉[CREATE RESOURCE POOL](../sql_reference/create_resource_pool.md)、[ALTER RESOURCE POOL](../sql_reference/alter_resource_pool.md)和[DROP RESOURCE POOL](../sql_reference/drop_resource_pool.md)语法的使用。

### 操作过程<a name="zh-cn_topic_0066854608_section16606579202019"></a>

**创建资源池**

1. [使用gsql连接](../getting_started/gsql_connection_and_usage.md)数据库。

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
    >- 如果在创建资源池的时候不指定所关联的控制组，则该资源池会被关联到默认控制组（DefaultClass控制组下的"Medium" Timeshare控制组）。
    >- control\_group取值区分大小写，指定时要使用单引号。
    >- 若数据库用户指定Timeshare控制组代表的字符串，即"Rush"、"High"、"Medium"或"Low"其中一种，如control\_group的字符串为"High"，代表资源池指定到DefaultClass控制组下的"High" Timeshare控制组。
    >- control\_group可以指定用户创建Workload控制组，即'class1:wd'，也可以带有控制组的级别，例如：'class1:wd:2'，这个级别范围一定要在1-10的范围内，但这个级别将不做任何区分作用。在旧版本中，允许创建同名Workload控制组，以级别进行区分。但新版本升级后，不允许创建同名控制组，用户如在旧版本中已创建同名Workload控制组，使用过程中其级别将不进行区分，由此可能造成的控制组不明确使用的问题，需要用户自行把旧的同名控制组删除以明确控制组使用。

**管理资源池**

修改资源池的属性。例如下面：修改资源池“resource\_pool\_a2”关联的控制组为“class\_a:workload\_a1”（假设class\_a:workload\_a1未被其他资源池关联）。

```
openGauss=# ALTER RESOURCE POOL resource_pool_a2 WITH (control_group="class_a:workload_a1");
ALTER RESOURCE POOL
```

**删除资源池**

删除资源池。例如下面删除资源池“resource\_pool\_a2”

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

- 查看当前数据库实例中所有的资源池信息。

    ```
    openGauss=# SELECT * FROM PG_RESOURCE_POOL;
    ```

    ```
       respool_name   | mem_percent | cpu_affinity |    control_group    | active_statements | max_dop | memory_limit | parentid | io_limits | io_priority |  nodegroup   | is_foreign  | max_worker
    ------------------+-------------+--------------+---------------------+-------------------+---------+--------------+----------+-----------+--------------+--------------+------------
     default_pool     |         100 |           -1 | DefaultClass:Medium |                -1 |       1 | 8GB          |        0 |         0 | None        | installation | f  |
     resource_pool_a  |          20 |           -1 | class_a             |                10 |       1 | 8GB          |        0 |         0 | None        | installation | f  |
     resource_pool_b  |          20 |           -1 | class_b             |                10 |       1 | 8GB          |        0 |         0 | None        | installation | f  |
     resource_pool_a1 |          20 |           -1 | class_a:workload_a1 |                10 |       1 | 8GB          |    16970 |         0 | None        | installation | f  |
     resource_pool_a2 |          20 |           -1 | class_a:workload_a2 |                10 |       1 | 8GB          |    16970 |         0 | None        | installation | f  |
     resource_pool_b1 |          20 |           -1 | class_b:workload_b1 |                10 |       1 | 8GB          |    16971 |         0 | None        | installation | f  |
     resource_pool_b2 |          20 |           -1 | class_b:workload_b2 |                10 |       1 | 8GB          |    16971 |         0 | None        | installation | f  |
    (7 rows)
    ```

- 查看某个资源池关联的控制组信息，具体内容可以参考[•gs\_control\_group\_info\(p...](../sql_reference/statistics_information_functions.md#zh-cn_topic_0283136951_zh-cn_topic_0237121998_table1560939125613)。

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
