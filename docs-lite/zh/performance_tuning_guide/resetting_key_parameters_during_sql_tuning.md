# SQL调优关键参数调整<a name="ZH-CN_TOPIC_0289900358"></a>

本节将介绍影响openGauss SQL调优性能的关键数据库主节点配置参数，配置方法参见[参数配置](https://docs.opengauss.org/zh/docs/latest-lite/database_administration_guide/parameter_configuration.html)。

**表 1**  数据库主节点配置参数

<a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_table6114302"></a>
<table><thead align="left"><tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row21522166"><th class="cellrowborder" valign="top" width="26.5%" id="mcps1.2.3.1.1"><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65573909"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65573909"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65573909"></a>参数/参考值</p>
</th>
<th class="cellrowborder" valign="top" width="73.5%" id="mcps1.2.3.1.2"><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p9886408"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p9886408"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p9886408"></a>描述</p>
</th>
</tr>
</thead>
<tbody><tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row59628243"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65158399"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65158399"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p65158399"></a>enable_nestloop=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p43339000"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p43339000"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p43339000"></a>控制查询优化器对嵌套循环连接（Nest Loop Join）类型的使用。当设置为“on”后，优化器优先使用Nest Loop Join；当设置为“off”后，优化器在存在其他方法时将优先选择其他方法。</p>
<div class="note" id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1238574948"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1238574948"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1238574948"></a><span class="notetitle"> 说明： </span><div class="notebody"><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p19810311241"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p19810311241"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p19810311241"></a>如果只需要在当前数据库连接（即当前Session）中临时更改该参数值，则只需要在SQL语句中执行如下命令：</p>
<a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen181041115417"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen181041115417"></a><pre class="screen" codetype="Sql" id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen181041115417">SET enable_nestloop to off;</pre>
</div></div>
<p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p33568521162216"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p33568521162216"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p33568521162216"></a>此参数默认设置为“on”，但实际调优中应根据情况选择是否关闭。一般情况下，在三种join方式（Nested Loop、Merge Join和Hash Join）里，Nested Loop性能较差，实际调优中可以选择关闭。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row24129853"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p8361080"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p8361080"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p8361080"></a>enable_bitmapscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6158855"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6158855"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6158855"></a>控制查询优化器对位图扫描规划类型的使用。设置为“on”，表示使用；设置为“off”，表示不使用。</p>
<div class="note" id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1657011214411"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1657011214411"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_note1657011214411"></a><span class="notetitle"> 说明： </span><div class="notebody"><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p747814301147"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p747814301147"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p747814301147"></a>如果只需要在当前数据库连接（即当前Session）中临时更改该参数值，则只需要在SQL语句中执行命令如下命令：</p>
<a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen124788309416"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen124788309416"></a><pre class="screen" codetype="Sql" id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_screen124788309416">SET enable_bitmapscan to off;</pre>
</div></div>
<p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p36534824162516"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p36534824162516"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p36534824162516"></a>bitmapscan扫描方式适用于“where a &gt; 1 and b &gt; 1”且a列和b列都有索引这种查询条件，但有时其性能不如indexscan。因此，现场调优如发现查询性能较差且计划中有bitmapscan算子，可以关闭bitmapscan，看性能是否有提升。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row3177297143544"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66890776143554"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66890776143554"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66890776143554"></a>enable_hashagg=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p34548229143544"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p34548229143544"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p34548229143544"></a>控制优化器对Hash聚集规划类型的使用。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row21449639145156"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4800916314528"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4800916314528"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4800916314528"></a>enable_hashjoin=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p3794196145156"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p3794196145156"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p3794196145156"></a>控制优化器对Hash连接规划类型的使用。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row31678976115536"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p15860301115536"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p15860301115536"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p15860301115536"></a>enable_mergejoin=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p31236364115637"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p31236364115637"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p31236364115637"></a>控制优化器对融合连接规划类型的使用。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row65339861145225"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p41108231145313"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p41108231145313"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p41108231145313"></a>enable_indexscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p52574139145225"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p52574139145225"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p52574139145225"></a>控制优化器对索引扫描规划类型的使用。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row25784757145225"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4524365214542"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4524365214542"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p4524365214542"></a>enable_indexonlyscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6606196145225"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6606196145225"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p6606196145225"></a>控制优化器对仅索引扫描规划类型的使用。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row50364799145216"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p18607282145410"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p18607282145410"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p18607282145410"></a>enable_seqscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66511650145216"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66511650145216"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p66511650145216"></a>控制优化器对顺序扫描规划类型的使用。完全消除顺序扫描是不可能的，但是关闭这个变量会让优化器在存在其他方法的时候优先选择其他方法。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_row36952817145219"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p5455969145417"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p5455969145417"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p5455969145417"></a>enable_sort=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p50220297145219"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p50220297145219"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_zh-cn_topic_0073253807_zh-cn_topic_0062520027_p50220297145219"></a>控制优化器使用的排序步骤。该设置不可能完全消除明确的排序，但是关闭这个变量可以让优化器在存在其他方法的时候优先选择其他方法。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_row91254119407"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p5125511194014"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p5125511194014"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p5125511194014"></a>rewrite_rule</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p3125411124017"><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p3125411124017"></a><a name="zh-cn_topic_0283136922_zh-cn_topic_0237121530_p3125411124017"></a>控制优化器是否启用LAZY_AGG和MAGIC_SET重写规则。</p>
</td>
</tr>
<tr id="row15768191461612"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p076951417163"><a name="p076951417163"></a><a name="p076951417163"></a>sql_beta_feature</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p1676941418160"><a name="p1676941418160"></a><a name="p1676941418160"></a>控制优化器是否启用。SEL_SEMI_POISSON/SEL_EXPR_INSTR/PARAM_PATH_GEN/RAND_COST_OPT/PAGE_EST_OPT/PARAM_PATH_OPT/NO_UNIQUE_INDEX_FIRST/JOIN_SEL_WITH_CAST_FUNC/CANONICAL_PATHKEY/INDEX_COST_WITH_LEAF_PAGES_ONLY/PARTITION_OPFUSION/A_STYLE_COERCE/PLPGSQL_STREAM_FETCHALL/PREDPUSH_SAME_LEVEL/PARTITION_FDW_ON/DISABLE_BITMAP_COST_WITH_LOSSY_PAGES/EXTRACT_PUSHDOWN_OR_CLAUSE测试功能。</p>
</td>
</tr>
<tr id="row372215341299"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p187235341897"><a name="p187235341897"></a><a name="p187235341897"></a>var_eq_const_selectivity</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p1072313341893"><a name="p1072313341893"></a><a name="p1072313341893"></a>控制优化器是否使用直方图计算整型常量的选择率。</p>
</td>
</tr>
<tr id="row873820011569"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p1173820025617"><a name="p1173820025617"></a><a name="p1173820025617"></a>partition_page_estimation</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p4739404567"><a name="p4739404567"></a><a name="p4739404567"></a>控制分区表页面是否通过剪枝结果进行页面估算优化，只包括分区表和local索引页面，不包括全局索引页面。估算公式为：</p>
<p id="p151458205116"><a name="p151458205116"></a><a name="p151458205116"></a>估算后页面 = 分区表总页面 * （剪枝后分区数 / 总分区数）。</p>
</td>
</tr>
<tr id="row1497103195719"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p749717385715"><a name="p749717385715"></a><a name="p749717385715"></a>partition_iterator_elimination</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p74971636576"><a name="p74971636576"></a><a name="p74971636576"></a>控制分区表在分区剪枝结果为一个分区时，是否通过消除分区迭代算子，来提升执行效率。</p>
</td>
</tr>
<tr id="row1493324894612"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p5933948154619"><a name="p5933948154619"></a><a name="p5933948154619"></a>enable_functional_dependency</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p16780635134810"><a name="p16780635134810"></a><a name="p16780635134810"></a>控制函数依赖统计信息的使用。</p>
<p id="p06781238164817"><a name="p06781238164817"></a><a name="p06781238164817"></a>设置为“on”，开启两个功能：</p>
<a name="ol93818213497"></a><a name="ol93818213497"></a><ol id="ol93818213497"><li>执行ANALYZE生成的多列统计信息包含函数依赖统计信息。</li><li>计算选择率会使用函数依赖统计信息。</li></ol>
<p id="p17836164534818"><a name="p17836164534818"></a><a name="p17836164534818"></a>设置为“off”，此两个功能不生效：</p>
<a name="ol958481011494"></a><a name="ol958481011494"></a><ol id="ol958481011494"><li>执行ANALYZE生成的多列统计信息不包含函数依赖统计信息。</li><li>计算选择率不会使用函数依赖统计信息。</li></ol>
<div class="note" id="note1145635281215"><a name="note1145635281215"></a><a name="note1145635281215"></a><span class="notetitle"> 说明： </span><div class="notebody"><p id="p345735251216"><a name="p345735251216"></a><a name="p345735251216"></a>函数依赖（Functional Dependency）的概念来自于关系数据库范式（Normal Form），表示属性间的函数关系。函数依赖统计信息，对此概念进行了扩展，表示满足函数关系的数据量占总数据量的比例。函数依赖统计信息是多列统计信息的一种，可以用于提升选择率估算的准确率。</p>
</div></div>
<p id="p11457135251219"><a name="p11457135251219"></a><a name="p11457135251219"></a>函数依赖统计信息适用于形如“where a = 1 and b = 1”的格式，要求a和b均是同一个表的属性，约束条件为等式约束，约束条件用AND连接，约束条件至少为两个。</p>
</td>
</tr>
<tr id="row11393164818220"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p43935488219"><a name="p43935488219"></a><a name="p43935488219"></a>enable_seqscan_fusion</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p14394348921"><a name="p14394348921"></a><a name="p14394348921"></a>控制seqscan底噪消除是否打开。</p>
</td>
</tr>
</tbody>
</table>
