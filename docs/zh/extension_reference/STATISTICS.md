# STATISTICS <a name="ZH-CN_TOPIC_0310260671"></a>

存储索引相关的信息。

**表 1**  STATISTICS字段

<a name="table1011513101687"></a>
<table><tbody><tr id="row201685101086"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p7168210483"><a name="p7168210483"></a><a name="p7168210483"></a><strong id="b1316817109817"><a name="b1316817109817"></a><a name="b1316817109817"></a>名称</strong></p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1816817101585"><a name="p1816817101585"></a><a name="p1816817101585"></a><strong id="b1016820101589"><a name="b1016820101589"></a><a name="b1016820101589"></a>类型</strong></p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p111687101286"><a name="p111687101286"></a><a name="p111687101286"></a><strong id="b1716911015819"><a name="b1716911015819"></a><a name="b1716911015819"></a>描述</strong></p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>table_catalog</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的物理表所在的数据库名。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>table_schema</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的物理表所在的schema名。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>table_name</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引所在的物理表名。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>non_unique</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>int</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>是否是唯一索引。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>index_schema</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引所在的schema名。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>index_name</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引名称。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>seq_in_index</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>int</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引所在的位置。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>column_name</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引所在的列名。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>collation</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(1)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>是升序还是降序索引。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>cardinality</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的行数估算。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>sub_part</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的字符长度。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>packed</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>是否为压缩索引。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>nullable</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(3)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引是否允许为空。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>index_type</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(11)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引类型。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>comment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(8)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的注释信息。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>index_comment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(2048)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引的注释信息。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>is_visible</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(3)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引是否可见。</p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>expression</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>text</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>索引表达式。</p>
</td>
</tr>
</tbody>
</table>
