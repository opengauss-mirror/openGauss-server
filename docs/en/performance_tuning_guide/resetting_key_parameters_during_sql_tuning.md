# Resetting Key Parameters During SQL Tuning<a name="EN-US_TOPIC_0245374565"></a>

This section introduces key parameters of the primary database node that affect optimization of SQL statements in openGauss. For details about the parameter configurations, see  [Configuring Running Parameters](../database_administration_guide/viewing_parameter_values.md).

**Table  1**  Parameters of the primary database node

<a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_table6114302"></a>
<table><thead align="left"><tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row21522166"><th class="cellrowborder" valign="top" width="26.5%" id="mcps1.2.3.1.1"><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65573909"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65573909"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65573909"></a>Parameter/Reference Value</p>
</th>
<th class="cellrowborder" valign="top" width="73.5%" id="mcps1.2.3.1.2"><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p9886408"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p9886408"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p9886408"></a>Description</p>
</th>
</tr>
</thead>
<tbody><tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row59628243"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65158399"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65158399"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p65158399"></a>enable_nestloop=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p43339000"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p43339000"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p43339000"></a>Specifies how the optimizer uses <strong id="b126812138114"><a name="b126812138114"></a><a name="b126812138114"></a>Nest Loop Join</strong>. If this parameter is set to <strong id="b422513212110"><a name="b422513212110"></a><a name="b422513212110"></a>on</strong>, the optimizer preferentially uses <strong id="b1923062117111"><a name="b1923062117111"></a><a name="b1923062117111"></a>Nest Loop Join</strong>. If it is set to <strong id="b132312021181112"><a name="b132312021181112"></a><a name="b132312021181112"></a>off</strong>, the optimizer preferentially uses other methods, if any.</p>
<div class="note" id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1238574948"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1238574948"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1238574948"></a><span class="notetitle"> NOTE: </span><div class="notebody"><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p19810311241"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p19810311241"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p19810311241"></a>If you only want to temporarily change the value of this parameter during the current database connection (that is, the current session), execute the following SQL statement:</p>
<a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen181041115417"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen181041115417"></a><pre class="screen" codetype="Sql" id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen181041115417">SET enable_nestloop to off;</pre>
</div></div>
<p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p33568521162216"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p33568521162216"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p33568521162216"></a>By default, this parameter is set to <strong id="b19986249181113"><a name="b19986249181113"></a><a name="b19986249181113"></a>on</strong>. Change the value as required. Generally, nested loop join has the poorest performance among the three <strong id="b42241357181113"><a name="b42241357181113"></a><a name="b42241357181113"></a>JOIN</strong> methods (nested loop join, merge join, and hash join). You are advised to set this parameter to <strong id="b62311579110"><a name="b62311579110"></a><a name="b62311579110"></a>off</strong>.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row24129853"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p8361080"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p8361080"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p8361080"></a>enable_bitmapscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6158855"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6158855"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6158855"></a>Specifies whether the optimizer uses bitmap scanning. If the value is <strong id="b2091162112126"><a name="b2091162112126"></a><a name="b2091162112126"></a>on</strong>, bitmap scanning is used. If the value is <strong id="b498102116126"><a name="b498102116126"></a><a name="b498102116126"></a>off</strong>, it is not used.</p>
<div class="note" id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1657011214411"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1657011214411"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_note1657011214411"></a><span class="notetitle"> NOTE: </span><div class="notebody"><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p747814301147"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p747814301147"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p747814301147"></a>If you only want to temporarily change the value of this parameter during the current database connection (that is, the current session), execute the following SQL statement:</p>
<a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen124788309416"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen124788309416"></a><pre class="screen" codetype="Sql" id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_screen124788309416">SET enable_bitmapscan to off;</pre>
</div></div>
<p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p36534824162516"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p36534824162516"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p36534824162516"></a>The bitmap scanning applies only in the query condition where <strong id="b1729183412123"><a name="b1729183412123"></a><a name="b1729183412123"></a>a &gt; 1 and b &gt; 1</strong> and indexes are created on columns <strong id="b973783419122"><a name="b973783419122"></a><a name="b973783419122"></a>a</strong> and <strong id="b274043412123"><a name="b274043412123"></a><a name="b274043412123"></a>b</strong>. During performance tuning, if the query performance is poor and bitmapscan operators are in the execution plan, set this parameter to <strong id="b763414781315"><a name="b763414781315"></a><a name="b763414781315"></a>off</strong> and check whether the performance is improved.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row3177297143544"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66890776143554"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66890776143554"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66890776143554"></a>enable_hashagg=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p34548229143544"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p34548229143544"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p34548229143544"></a>Specifies whether to enable the optimizer's use of Hash-aggregation plan types.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row21449639145156"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4800916314528"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4800916314528"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4800916314528"></a>enable_hashjoin=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p3794196145156"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p3794196145156"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p3794196145156"></a>Specifies whether to enable the optimizer's use of Hash-join plan types.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row31678976115536"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p15860301115536"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p15860301115536"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p15860301115536"></a>enable_mergejoin=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p31236364115637"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p31236364115637"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p31236364115637"></a>Specifies whether to enable the optimizer's use of Hash-merge plan types.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row65339861145225"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p41108231145313"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p41108231145313"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p41108231145313"></a>enable_indexscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p52574139145225"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p52574139145225"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p52574139145225"></a>Specifies whether to enable the optimizer's use of index-scan plan types.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row25784757145225"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4524365214542"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4524365214542"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p4524365214542"></a>sql_beta_feature</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6606196145225"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6606196145225"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p6606196145225"></a>Specifies whether to enable the optimizer's use of index-only-scan plan types.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row50364799145216"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p18607282145410"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p18607282145410"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p18607282145410"></a>enable_seqscan=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66511650145216"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66511650145216"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p66511650145216"></a>Specifies whether the optimizer uses bitmap scanning. It is impossible to suppress sequential scans entirely, but setting this variable to <strong id="b1779952613149"><a name="b1779952613149"></a><a name="b1779952613149"></a>off</strong> encourages the optimizer to choose other methods if available.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_row36952817145219"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p5455969145417"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p5455969145417"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p5455969145417"></a>enable_sort=on</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p50220297145219"><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p50220297145219"></a><a name="en-us_topic_0237121530_en-us_topic_0073253807_en-us_topic_0062520027_p50220297145219"></a>Specifies the optimizer sorts. It is impossible to fully suppress explicit sorts, but setting this variable to <strong id="b204698497147"><a name="b204698497147"></a><a name="b204698497147"></a>off</strong> allows the optimizer to preferentially choose other methods if available.</p>
</td>
</tr>
<tr id="en-us_topic_0237121530_row91254119407"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_p5125511194014"><a name="en-us_topic_0237121530_p5125511194014"></a><a name="en-us_topic_0237121530_p5125511194014"></a>rewrite_rule</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0237121530_p3125411124017"><a name="en-us_topic_0237121530_p3125411124017"></a><a name="en-us_topic_0237121530_p3125411124017"></a>Specifies whether the optimizer enables the <strong id="b11177335152"><a name="b11177335152"></a><a name="b11177335152"></a>LAZY_AGG</strong> and <strong id="b1118763171517"><a name="b1118763171517"></a><a name="b1118763171517"></a>MAGIC_SET</strong> rewriting rules.</p>
</td>
</tr>
    <tr id="en-us_topic_0237121530_row91254119407"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0237121530_p5125511194014"><a name="en-us_topic_0237121530_p5125511194014"></a><a name="en-us_topic_0237121530_p5125511194014"></a>sql_beta_feature</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p1676941418160"><a name="p1676941418160"></a><a name="p1676941418160"></a>Determines whether the optimizer enables the SEL_SEMI_POISSON, SEL_EXPR_INSTR, PARAM_PATH_GEN, RAND_COST_OPT, PARAM_PATH_OPT, PAGE_EST_OPT, CANONICAL_PATHKEY, PARTITION_OPFUSION, PREDPUSH_SAME_LEVEL, PARTITION_FDW_ON, DISABLE_BITMAP_COST_WITH_LOSSY_PAGES beta features.</p>
</td>
</tr>
<tr id="row109293122216"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p169298129219"><a name="p169298129219"></a><a name="p169298129219"></a>var_eq_const_selectivity</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p119296122217"><a name="p119296122217"></a><a name="p119296122217"></a>Determines whether the optimizer uses histograms to calculate the integer constant selection rate.</p>
</td>
</tr>
<tr id="row18675165718502"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p1667514571505"><a name="p1667514571505"></a><a name="p1667514571505"></a>partition_page_estimation</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p18675757195010"><a name="p18675757195010"></a><a name="p18675757195010"></a>Determines whether to optimize the estimation of partitioned table page based on the pruning result. Only the partitioned table page and local index page are included, and the global index page is not included. The estimation formula is: </p>
<p id="p151458205116"><a name="p151458205116"></a><a name="p151458205116"></a>Number of pages after estimation = Number of pages in the partitioned table x (Number of partitions after pruning/Number of partitions)</p>
</td>
</tr>
<tr id="row17146036175314"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p6147113617536"><a name="p6147113617536"></a><a name="p6147113617536"></a>partition_iterator_elimination</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p814716366533"><a name="p814716366533"></a><a name="p814716366533"></a>Determines whether to eliminate the partition iteration operator to improve execution efficiency when the partition pruning result of a partitioned table is a partition.</p>
</td>
</tr>
<tr id="row1266817517495"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p12120152231113"><a name="p12120152231113"></a><a name="p12120152231113"></a>enable_functional_dependency</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p4198164193511"><a name="p4198164193511"></a><a name="p4198164193511"></a>Determines whether to use the functional dependency statistics.</p>
<p id="p10544133318355"><a name="p10544133318355"></a><a name="p10544133318355"></a>If the value is **on**:</p>
<a name="ol15490140123616"></a><a name="ol15490140123616"></a><ol id="ol15490140123616"><li>The statistics about multiple columns generated by ANALYZE contain functional dependency statistics. </li><li>Functional dependency statistics are used to calculate the selection rate.</li></ol>
<p id="p1638144993519"><a name="p1638144993519"></a><a name="p1638144993519"></a>If the value is **off**:</p>
<a name="ol17501953619"></a><a name="ol17501953619"></a><ol id="ol17501953619"><li>The statistics about multiple columns generated by ANALYZE do not contain functional dependency statistics. </li><li>Functional dependency statistics are not used to calculate the selection rate.</li></ol>
<div class="note" id="note1145635281215"><a name="note1145635281215"></a><a name="note1145635281215"></a><span class="notetitle"> description: </span><div class="notebody"><p id="p345735251216"><a name="p345735251216"></a><a name="p345735251216"></a>The concept of functional dependency comes from the relational database normal form, indicating the functional relationship between attributes. The concept of functional dependency statistics extends the preceding concept. It indicates the ratio of the data volume that meets the functional relationship to the total data volume. Functional dependency statistics are a type of multi-column statistics, which can be used to improve the accuracy of selection rate estimation.</p>
</div></div>
<p id="p11457135251219"><a name="p11457135251219"></a><a name="p11457135251219"></a>Functional dependency statistics are applicable to the "where a = 1 and b = 1" format. The a and b must be attributes of the same table. The constraint is an equation constraint. The constraint is connected by AND. There are at least two constraints.</p>
</td>
</tr>
<tr id="row782611401047"><td class="cellrowborder" valign="top" width="26.5%" headers="mcps1.2.3.1.1 "><p id="p18241124419412"><a name="p18241124419412"></a><a name="p18241124419412"></a>enable_seqscan_fusion</p>
</td>
<td class="cellrowborder" valign="top" width="73.5%" headers="mcps1.2.3.1.2 "><p id="p14394348921"><a name="p14394348921"></a><a name="p14394348921"></a>Determines whether to enable seqscan background noise elimination.</p>
</td>
</tr>
</tbody>
</table>
