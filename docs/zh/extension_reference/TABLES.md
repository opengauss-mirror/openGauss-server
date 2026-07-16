# TABLES <a name="ZH-CN_TOPIC_0310260671"></a>

存储表相关的信息。

**表 1**  TABLES相比于PGXC/PG新增字段

<a name="table1011513101687"></a>
<table><tbody><tr id="row201685101086"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p7168210483"><a name="p7168210483"></a><a name="p7168210483"></a><strong id="b1316817109817"><a name="b1316817109817"></a><a name="b1316817109817"></a>名称</strong></p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1816817101585"><a name="p1816817101585"></a><a name="p1816817101585"></a><strong id="b1016820101589"><a name="b1016820101589"></a><a name="b1016820101589"></a>类型</strong></p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p111687101286"><a name="p111687101286"></a><a name="p111687101286"></a><strong id="b1716911015819"><a name="b1716911015819"></a><a name="b1716911015819"></a>描述</strong></p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>table_collation</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的字符序。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>engine</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的存储引擎。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>version</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>int</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的版本。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>row_format</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的行存储格式。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>table_rows</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的行数估算。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>avg_row_length</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的平均每行长度估算。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>data_length</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表数据的总大小。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>max_data_length</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表最大可存储数据量。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>index_length</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引总大小。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>data_free</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的剩余空间大小。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>auto_increment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>int128</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>自增列的下一个值。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>create_time</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>timestamp</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的创建时间。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>update_time</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>timestamp</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的修改时间。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>check_time</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>timestamp</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的最后一次检查时间。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>checksum</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>表的校验和。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>create_options</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>建表时的额外信息。</p>
</td>
</tr>
</tbody>
</table>
