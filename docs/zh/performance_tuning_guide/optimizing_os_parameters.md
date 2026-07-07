# 操作系统参数调优

在性能调优过程中，可以根据实际业务情况修改关键操作系统（OS）配置参数，以提升openGauss数据库的性能。

## 前提条件<a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_section24680968154821"></a>

需要用户使用gs\_check检查操作系统参数结果是否和建议值保持一致，如果不一致，用户可根据实际业务情况去手动修改。

## 内存相关参数设置<a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_section562061531741"></a>

配置“sysctl.conf”文件，修改内存相关参数vm.extfrag\_threshold为1000（参考值），如果文件中没有内存相关参数，可以手动添加。

```
vim /etc/sysctl.conf
```

修改完成后，请执行如下命令，使参数生效。

```
sysctl -p 
```

## 网络相关参数设置<a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_section710274519189"></a>

- 配置“sysctl.conf”文件，修改网络相关参数，如果文件中没有网络相关参数，可以手动添加。详细说明请参见[表1](#zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_table5597039720233)。

    ```
    vim /etc/sysctl.conf
    ```

    在修改完成后，请执行如下命令，使参数生效。

    ```
    sysctl -p 
    ```

    **表 1**  网络相关参数

    <a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_table5597039720233"></a>
    <table><thead align="left"><tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row3622161620233"><th class="cellrowborder" valign="top" width="31.269999999999996%" id="mcps1.2.4.1.1"><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p166140422031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p166140422031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p166140422031"></a>参数名</p>
    </th>
    <th class="cellrowborder" valign="top" width="23.18%" id="mcps1.2.4.1.2"><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p35601502031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p35601502031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p35601502031"></a>参考值</p>
    </th>
    <th class="cellrowborder" valign="top" width="45.550000000000004%" id="mcps1.2.4.1.3"><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p199366942031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p199366942031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p199366942031"></a>说明</p>
    </th>
    </tr>
    </thead>
    <tbody><tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row1666292620233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p179029862031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p179029862031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p179029862031"></a>net.ipv4.tcp_timestamps</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p408557272031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p408557272031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p408557272031"></a>1</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p209795682031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p209795682031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p209795682031"></a>表示开启TCP连接中TIME-WAIT sockets的快速回收，默认为0，表示关闭，1表示打开。</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row4096249820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p603935832031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p603935832031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p603935832031"></a>net.ipv4.tcp_mem</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p600420392031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p600420392031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p600420392031"></a>94500000 915000000 927000000</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p315669672031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p315669672031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p315669672031"></a>第一个数字表示，当tcp使用的page少于 94500000 时，kernel不对其进行任何的干预。</p>
    <p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p156672522031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p156672522031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p156672522031"></a>第二个数字表示，当tcp使用的page超过 915000000 时，kernel会进入“memory pressure”压力模式。</p>
    <p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p67875442031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p67875442031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p67875442031"></a>第三个数字表示，当tcp使用的pages超过 927000000 时，就会报：Out of socket memory。</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row4494163820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p491728812031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p491728812031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p491728812031"></a>net.ipv4.tcp_max_orphans</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p235804102031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p235804102031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p235804102031"></a>3276800</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p309650562031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p309650562031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p309650562031"></a>最大孤儿套接字（orphan sockets）数。</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row6109143520233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p96946902031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p96946902031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p96946902031"></a>net.ipv4.tcp_fin_timeout</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p470724342031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p470724342031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p470724342031"></a>60</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p547707902031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p547707902031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p547707902031"></a>表示系統默认的TIMEOUT时间。</p>
    </td>
    </tr>
    <tr id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_row6159800820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p618024542031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p618024542031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p618024542031"></a>net.ipv4.ip_local_port_range</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p399429002031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p399429002031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p399429002031"></a>26000 65535</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p141494422031"><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p141494422031"></a><a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_p141494422031"></a>TCP和UDP能够使用的port段。</p>
    </td>
    </tr>
    </tbody>
    </table>

- 设置10GE网卡最大传输单元（MTU），使用ifconfig命令设置。10GE网卡推荐设置为8192，可提升网络带宽利用率。

    示例：
    
    ```
    #ifconfig ethx mtu 8192
    #ifconfig ethx
    ethx   Link encap:Ethernet  HWaddr XX:XX:XX:XX:XX:XX
    inet addr:xxx.xxx.xxx.xxx  Bcast:xxx.xxx.xxx.xxx  Mask:xxx.xxx.xxx.0
    inet6 addr: fxxx::9xxx:bxxx:xxxa:1d18/64 Scope:Link
    UP BROADCAST RUNNING MULTICAST  MTU:8192  Metric:1
    RX packets:179849803 errors:0 dropped:0 overruns:0 frame:0
    TX packets:40492292 errors:0 dropped:0 overruns:0 carrier:0
    collisions:0 txqueuelen:1000
    RX bytes:17952090386 (17120.4 Mb)  TX bytes:171359670290 (163421.3 Mb)
    ```
    
    >[!NOTE]说明 
    >
    >- ethx为10GE数据库内部使用的业务网卡。  
    >- 第一条命令设置MTU，第二条命令验证是否设置成功，MTU的值为“MTU:8192”。  
    >- 需使用root用户设置。  

- 设置10GE网卡接收（rx）、发送队列（tx）长度，使用ethtool工具设置。10GE网卡推荐设置为4096，可提升网络带宽利用率。

    示例：

    ```
    # ethtool -G ethx rx 4096 tx 4096
    # ethtool -g ethx
    Ring parameters for ethx:
    Pre-set maximums:
    RX:             4096
    RX Mini:        0
    RX Jumbo:       0
    TX:             4096
    Current hardware settings:
    RX:             4096
    RX Mini:        0
    RX Jumbo:       0
    TX:             4096
    ```

    >[!NOTE]说明
    > 
    >- ethx为10GE数据库内部使用的业务网卡。  
    >- 第一条命令设置网卡接收、发送队列长度，第二条命令验证是否设置成功，示例的输出表示设置成功。  
    >- 需使用root用户设置。  

## I/O相关参数设置<a name="zh-cn_topic_0237121493_zh-cn_topic_0073253550_zh-cn_topic_0040046482_section43084899192126"></a>

设置hugepage属性。通过如下命令，关闭透明大页。

```
echo never >
/sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
```

修改完成后，请执行如下命令，使参数生效。

```
reboot
```
