# Optimizing OS Parameters<a name="EN-US_TOPIC_0289900831"></a>

You can improve the openGauss performance by modifying key parameters of the OS based on actual service requirements during the performance optimization.

## Prerequisites<a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_section24680968154821"></a>

You have checked whether the OS parameters are set to the suggested values using  **gs\_check**. If not, modify them as required.

## Memory Parameters<a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_section562061531741"></a>

In the  **sysctl.conf**  file, change the value of  **vm.extfrag\_threshold**  to  **1000**  \(reference value\). If the file does not contain memory parameters, add them manually.

```
vim /etc/sysctl.conf
```

Run the following command to make the modification take effect:

```
sysctl -p 
```

## Network Parameters<a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_section710274519189"></a>

- In the  **sysctl.conf**  file, modify network parameters. If the file does not contain such parameters, add them manually. For details, see  [Table 1](#en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_table5597039720233).

    ```
    vim /etc/sysctl.conf
    ```

    Run the following command to make the modification take effect:

    ```
    sysctl -p 
    ```

    **Table  1**  Network parameters

    <a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_table5597039720233"></a>
    <table><thead align="left"><tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row3622161620233"><th class="cellrowborder" valign="top" width="31.269999999999996%" id="mcps1.2.4.1.1"><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p166140422031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p166140422031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p166140422031"></a>Parameter</p>
    </th>
    <th class="cellrowborder" valign="top" width="23.18%" id="mcps1.2.4.1.2"><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p35601502031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p35601502031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p35601502031"></a>Reference Value</p>
    </th>
    <th class="cellrowborder" valign="top" width="45.550000000000004%" id="mcps1.2.4.1.3"><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p199366942031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p199366942031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p199366942031"></a>Description</p>
    </th>
    </tr>
    </thead>
    <tbody><tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row1666292620233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p179029862031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p179029862031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p179029862031"></a>net.ipv4.tcp_timestamps</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p408557272031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p408557272031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p408557272031"></a>1</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p209795682031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p209795682031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p209795682031"></a>Specifies whether to enable quick reclamation of the sockets in TIME-WAIT state during TCP connection establishment. The default value <strong id="b95718813115"><a name="b95718813115"></a><a name="b95718813115"></a>0</strong> indicates that quick reclamation is disabled, and the value <strong id="b16511013133110"><a name="b16511013133110"></a><a name="b16511013133110"></a>1</strong> indicates that quick reclamation is enabled.</p>
    </td>
    </tr>
    <tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row4096249820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p603935832031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p603935832031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p603935832031"></a>net.ipv4.tcp_mem</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p600420392031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p600420392031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p600420392031"></a>94500000 915000000 927000000</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p315669672031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p315669672031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p315669672031"></a><strong id="b19974111713114"><a name="b19974111713114"></a><a name="b19974111713114"></a>94500000</strong>: If less than 94,500,000 pages are used by the TCP, the kernel is not affected.</p>
    <p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p156672522031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p156672522031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p156672522031"></a><strong id="b2430153413314"><a name="b2430153413314"></a><a name="b2430153413314"></a>915000000</strong>: If more than 915,000,000 pages are used by the TCP, the kernel enters the <strong id="b14351334153117"><a name="b14351334153117"></a><a name="b14351334153117"></a>memory pressure</strong> mode.</p>
    <p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p67875442031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p67875442031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p67875442031"></a><strong id="b15193194113114"><a name="b15193194113114"></a><a name="b15193194113114"></a>927000000</strong>: If more than 927,000,000 pages are used by the TCP, the "Out of socket memory." message is displayed.</p>
    </td>
    </tr>
    <tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row4494163820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p491728812031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p491728812031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p491728812031"></a>net.ipv4.tcp_max_orphans</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p235804102031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p235804102031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p235804102031"></a>3276800</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p309650562031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p309650562031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p309650562031"></a>Maximum number of the orphan sockets</p>
    </td>
    </tr>
    <tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row6109143520233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p96946902031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p96946902031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p96946902031"></a>net.ipv4.tcp_fin_timeout</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p470724342031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p470724342031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p470724342031"></a>60</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p547707902031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p547707902031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p547707902031"></a>Default timeout period</p>
    </td>
    </tr>
    <tr id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_row6159800820233"><td class="cellrowborder" valign="top" width="31.269999999999996%" headers="mcps1.2.4.1.1 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p618024542031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p618024542031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p618024542031"></a>net.ipv4.ip_local_port_range</p>
    </td>
    <td class="cellrowborder" valign="top" width="23.18%" headers="mcps1.2.4.1.2 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p399429002031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p399429002031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p399429002031"></a>26000 65535</p>
    </td>
    <td class="cellrowborder" valign="top" width="45.550000000000004%" headers="mcps1.2.4.1.3 "><p id="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p141494422031"><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p141494422031"></a><a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_p141494422031"></a>Port range that can be used by TCP or UDP</p>
    </td>
    </tr>
    </tbody>
    </table>

- Use the  **ifconfig**  command to set the maximum transmission unit \(MTU\) of 10 GE NICs. The value  **8192**  is recommended because this setting improves the network bandwidth usage.

    Example:

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

    >[!NOTE]NOTE 
    >- **ethx**  indicates the 10 GE service NIC used in the database.
    >- The first command is used to set the MTU. The second command is used to verify that the MTU has been successfully set. The texts in bold indicate the value of the MTU.
    >- Set the MTU as user  **root**.

- Use  **ethtool**  to set the length of the receiving \(**RX**\) queue and that of the sending \(**TX**\) queue for 10 GE NICs. The value  **4096**  is recommended because this setting improves the network bandwidth usage.

    Example:

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

    >[!NOTE]NOTE 
    >- **ethx**  indicates the 10 GE service NIC used in the database.
    >- The first command is used to set the lengths of the receiving and sending queues. The second command is to verify whether the lengths have been successfully set. If information similar to the example is displayed, the setting is successful.
    >- Set the lengths of the receiving and sending queues as user  **root**.

## I/O Parameters<a name="en-us_topic_0283136559_en-us_topic_0237121493_en-us_topic_0073253550_en-us_topic_0040046482_section43084899192126"></a>

Set the  **hugepage**  attribute. Run the following commands to disable the transparent huge page function:

```
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
```

Run the following command to make the modification take effect:

```
reboot
```
