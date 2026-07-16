# COLLATIONS <a name="ZH-CN_TOPIC_0310260671"></a>

存储字符序相关的信息。

**表 1**  COLLATIONS相比于PGXC/PG新增字段

<a name="table1011513101687"></a>
<table><tbody><tr id="row201685101086"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p7168210483"><a name="p7168210483"></a><a name="p7168210483"></a><strong id="b1316817109817"><a name="b1316817109817"></a><a name="b1316817109817"></a>名称</strong></p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1816817101585"><a name="p1816817101585"></a><a name="p1816817101585"></a><strong id="b1016820101589"><a name="b1016820101589"></a><a name="b1016820101589"></a>类型</strong></p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p111687101286"><a name="p111687101286"></a><a name="p111687101286"></a><strong id="b1716911015819"><a name="b1716911015819"></a><a name="b1716911015819"></a>描述</strong></p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>character_set_name</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>varchar(64)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>字符序对应的字符集名称。</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>id</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>bigint</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>字符序ID。</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>is_default</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>varchar(3)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>是否为对应字符集的默认字符序。</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>is_compiled</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>varchar(3)</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>是否为内置字符序。</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>sortlen</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>int</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>字符序的比较长度。</p>
</td>
</tr>
</tbody>
</table>
