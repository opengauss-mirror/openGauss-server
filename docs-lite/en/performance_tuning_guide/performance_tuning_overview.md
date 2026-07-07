# Overview<a name="EN-US_TOPIC_0289900313"></a>

To fine-tune openGauss performance, you need to identify performance bottlenecks, adjust key parameters, and optimize SQL statements. During performance tuning, locate and analyze performance issues based on performance elements, such as system resources, throughput, and loads to achieve required system performance.

Various factors must be considered during openGauss performance tuning. Therefore, optimization personnel must know well about knowledge, such as system software architecture, hardware and software configuration, database parameter configuration, concurrency control, query processing, and database applications.

>[!TIP]NOTICE 
>Performance tuning sometimes require openGauss restart, which may interrupt current services. Therefore, after the service goes live and when openGauss needs to be restarted, you must send the request to related management department about the operation window time for approval.

## Tuning Process<a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_section7336753113553"></a>

[Figure 1](#en-us_topic_0283137244_fig36820421398)  shows the procedure of performance tuning.

**Figure  1**  openGauss performance tuning<a name="en-us_topic_0283137244_fig36820421398"></a>  
![](figures/opengauss-performance-tuning.png)

[Table 1](#en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_table18747316113544)  lists the details about each phase.

**Table  1**  openGauss performance tuning

<a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_table18747316113544"></a>
<table><thead align="left"><tr id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_row57564514113544"><th class="cellrowborder" valign="top" width="26%" id="mcps1.2.3.1.1"><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p32214083113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p32214083113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p32214083113544"></a>Phase</p>
</th>
<th class="cellrowborder" valign="top" width="74%" id="mcps1.2.3.1.2"><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p59203969113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p59203969113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p59203969113544"></a>Description</p>
</th>
</tr>
</thead>
<tbody><tr id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_row30792195113544"><td class="cellrowborder" valign="top" width="26%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p11139890113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p11139890113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p11139890113544"></a><a href="determining_the_scope_of_performance_tuning.md">Determining the Scope of Performance Tuning</a></p>
</td>
<td class="cellrowborder" valign="top" width="74%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_p6652358711738"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_p6652358711738"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_p6652358711738"></a>The phase where the CPU, memory, I/O, and network resource usage of each node in <span id="text991519461216"><a name="text991519461216"></a><a name="text991519461216"></a>openGauss</span> are obtained to check whether these resources are fully used and whether any bottleneck exists</p>
</td>
</tr>
<tr id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_row7268277113544"><td class="cellrowborder" valign="top" width="26%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p51859545113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p51859545113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p51859545113544"></a><a href="optimizing_os_parameters.md">System Optimization</a></p>
</td>
<td class="cellrowborder" valign="top" width="74%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p39873610113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p39873610113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p39873610113544"></a>The phase where OS and database system level optimization are performed to fully use the CPU, memory, I/O, and network resources, prevent resource conflicts, and improve the query throughput in the system</p>
</td>
</tr>
<tr id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_row23318170113544"><td class="cellrowborder" valign="top" width="26%" headers="mcps1.2.3.1.1 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p9723624113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p9723624113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p9723624113544"></a><a href="sql_optimization.md">SQL Optimization</a></p>
</td>
<td class="cellrowborder" valign="top" width="74%" headers="mcps1.2.3.1.2 "><p id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p49416119113544"><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p49416119113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_p49416119113544"></a>The phase where the SQL statements used are analyzed to determine whether any optimization can be performed. Analysis of SQL statements comprises:</p>
<a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_ul42091895113544"></a><a name="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_ul42091895113544"></a><ul id="en-us_topic_0283137244_en-us_topic_0237121483_en-us_topic_0073253541_en-us_topic_0040046511_ul42091895113544"><li>Generating table statistics using <strong id="b361953564512"><a name="b361953564512"></a><a name="b361953564512"></a>ANALYZE</strong>: The <strong id="b361923534517"><a name="b361923534517"></a><a name="b361923534517"></a>ANALYZE</strong> statement collects statistics about the database table content. Statistical results are stored in the system catalog <strong id="b1662018358452"><a name="b1662018358452"></a><a name="b1662018358452"></a>PG_STATISTIC</strong>. The execution plan generator uses these statistics to determine which one is the most effective execution plan.</li><li>Analyzing the execution plan: The <strong id="b898034720482"><a name="b898034720482"></a><a name="b898034720482"></a>EXPLAIN</strong> statement displays the execution plan of SQL statements, and the <strong id="b298724715488"><a name="b298724715488"></a><a name="b298724715488"></a>EXPLAIN PERFORMANCE</strong> statement displays the execution time of each operator in SQL statements.</li><li>Identifying the root causes of issues: Identifies possible causes by analyzing the execution plan and performs specific optimization by modifying database-level SQL optimization parameters.</li><li>Compiling better SQL statements: Compiles better SQL statements in the scenarios, such as cache of intermediate and temporary data for complex queries, result set cache, and result set combination.</li></ul>
</td>
</tr>
</tbody>
</table>
