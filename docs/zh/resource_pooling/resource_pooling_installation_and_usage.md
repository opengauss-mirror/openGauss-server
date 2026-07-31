# 资源池化安装与使用

## 手动安装示例<a name="section188071153201818"></a>

与传统gs\_initdb建库相比，资源池化建库将目录分为三种类型，每实例独占且不共享、每实例独占且共享、所有实例共享。其中需要共享的目录均需存放到磁阵设备上，而不共享的目录存放在本地盘上。另外备机建库只需要建隶属于自己的目录，不需要再次创建所有实例共享的目录结构。资源池化新增了相关GUC参数，以及将系统表存储方式从页式切换到段页式。

>[!NOTE]说明
>此处的单步手动安装是指工程编译之后，可单步执行相关命令进行资源池化建库。openGauss常规安装请参照《安装指南》。

- 前置条件
    - 工程已完成代码编译，编译请参见[软件安装编译](https://docs.opengauss.org/zh/docs/latest/compilation_guide/compiling_the_version.html)。
    - 主机已经挂载磁阵LUN设备，并且已经安装ultrapath多路径软件，磁阵设备可用。

- 操作步骤
    1. <a name="li0988195961613"></a>为磁阵LUN盘符创建软链接，并赋予相应的用户权限（假设磁阵LUN对应的盘符为/dev/sde, /dev/sdf）。

        ```
        sudo ln -s /dev/sde /dev/tpcc_data
        sudo ln -s /dev/sdf /dev/tpcc_log
        sudo chmod 777 /dev/tpcc_data
        sudo chmod 777 /dev/tpcc_log
        ```

    2. 为需要磁阵RAWIO权限的可执行文件赋权。

        ```
        sudo -i setcap CAP_SYS_RAWIO+ep 绝对路径/perctrl
        ```

        perctrl：用于对dss相关工具和进程赋予读写权限的可执行工具。

    3. 创建DSS服务端进程及建资源池化库需要的配置文件。

        测试目录（假设为/data/test）

        ```
        └─dss_home/
        ├── cfg
        │   ├── dss_inst.ini
        │   └── dss_vg_conf.ini
        └── log // 启动前需存在log目录
        ```

        dss\_init.ini配置内容如下：

        ```
        INST_ID=0
        _LOG_LEVEL=55
        _LOG_BACKUP_FILE_COUNT=128
        _LOG_MAX_FILE_SIZE =20M
        LSNR_PATH=/data/test/dss_home
        STORAGE_MODE=SHARE_DISK
        _SHM_KEY=12
        ```

        上述配置中参数说明如下：

        - INST\_ID配置实例号，取值范围\[0, 63\]，每个主备下的dssserver进程各不相同。
        - \_LOG\_LEVEL日志级别。
        - \_LOG\_BACKUP\_FILE\_COUNT日志文件最多保留的个数。
        - \_LOG\_MAX\_FILE\_SIZE为单个日志文件最大大小。
        - LSNR\_PATH为DSS客户端与服务端之间通信使用的domain socket保存的目录，一般设为DSS服务端进程的家目录。
        - STORAGE\_MODE为DSS对应的存储设备类型，磁阵的话配置为SHARE_DISK。
        - \_SHM\_KEY共享内存KEY，需要保证每个DSS各不相同。

        dss\_vg\_conf.ini配置内容如下，

        ```
         data:/dev/tpcc_data
         log: /dev/tpcc_log
        ```

        表示+data目录的内容存放在/dev/tpcc\_data设备上，+log目录内容存放在/dev/tpcc\_log设备上。这里要注意下，约定通过根目录名是否有+字符区分是文件系统中的文件，还是DSS中的文件。用户可以把DSS当做类似分布式文件系统来看待。

    4. 使用DSS客户端工具（dsscmd）在磁阵设备上初始化VG（类似于在裸盘上初始化文件系统的操作）。

        ```
        # 清空磁阵LUN开头数据
        dd if=/dev/zero bs=2048 count=100000 of=/dev/tpcc_data
        dd if=/dev/zero bs=2048 count=100000 of=/dev/tpcc_log
        # 创建VG
        dsscmd cv -g data -v /dev/tpcc_data -s 2048 -D /data/ss_test/dss_home
        dsscmd cv -g log  -v /dev/tpcc_log -s 65536 -D /data/ss_test/dss_home
        # 拉起dssserver
        dssserver -D /data/ss_test/dss_home &
        ```

    5. <a name="li23296624419"></a>通过gs\_initdb建立资源池化库。

        ```
        gs_initdb -D /data/ss_test/dn_primary --nodename=single_node -w ****** --vgname="+data,+log" --enable-dss --dms_url="0:127.0.0.1:1611,1:127.0.0.1:1711" -I 0 --socketpath="UDS:/data/ss_test/dss_home/.dss_unix_d_socket"
        ```

        其中新增5个相关参数：

        - --vgname 卷组名，指定资源池化库建在哪个卷组下，这个名字与dss\_vg\_conf.ini文件中的配置项相关，卷组名需要出现在配置文件中，并且以‘+’字符开头。
        - --enable-dss 表示资源池化库要建到DSS里。
        - --dms\_url "0:127.0.0.1:1611,1:127.0.0.1:1711"，格式为instance\_id:ip:port。
        - -I  指定当前节点的实例号，取值范围\[0,63\]。
        - --socketpath 指定DSS客户端（这里指集成到数据库相关可执行文件中的DSS客户端动态库）与服务端通信使用的unix domain socket存放的位置。

    6. 建资源池化库成功，通过gs\_ctl start命令拉取数据库进程。

        ```
        gs_ctl start -D /data/ss_test/dn_primary
        ```

    7. 按照上述步骤再重新执行安装备机。

## RDMA使用示例<a name="ZH-CN_TOPIC_0000002148262710"></a>

资源池化特性提供备机实时一致性读功能，主备之间页面交换可选择通过RDMA加速。此章节简要描述如何在资源池化中开启RDMA通信功能。

### 安装准备<a name="ZH-CN_TOPIC_0000002183621657"></a>

**获取安装包<a name="zh-cn_topic_0000002082250312_section186941117112519"></a>**

- 请通过openGauss社区获取HCOM安装包。
- HCOM4DB的so已包含在openGauss安装包中。如果需要自行编译，可以从[Gitee](https://gitcode.com/opengauss/hcom4db)获取源码。

**环境要求<a name="zh-cn_topic_0000002082250312_section14726153115253"></a>**

**表 1**  环境要求

<a name="zh-cn_topic_0000002082250312_table81711048122510"></a>
<table><thead align="left"><tr id="zh-cn_topic_0000002082250312_row2172104812518"><th class="cellrowborder" valign="top" width="12.121212121212121%" id="mcps1.2.4.1.1"><p id="zh-cn_topic_0000002082250312_p9172104802514"><a name="zh-cn_topic_0000002082250312_p9172104802514"></a><a name="zh-cn_topic_0000002082250312_p9172104802514"></a>项目</p>
</th>
<th class="cellrowborder" valign="top" width="42.76427642764276%" id="mcps1.2.4.1.2"><p id="zh-cn_topic_0000002082250312_p11721948162514"><a name="zh-cn_topic_0000002082250312_p11721948162514"></a><a name="zh-cn_topic_0000002082250312_p11721948162514"></a>配置描述</p>
</th>
<th class="cellrowborder" valign="top" width="45.11451145114511%" id="mcps1.2.4.1.3"><p id="zh-cn_topic_0000002082250312_p1217244811251"><a name="zh-cn_topic_0000002082250312_p1217244811251"></a><a name="zh-cn_topic_0000002082250312_p1217244811251"></a>说明</p>
</th>
</tr>
</thead>
<tbody><tr id="zh-cn_topic_0000002082250312_row31724485250"><td class="cellrowborder" valign="top" width="12.121212121212121%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p9764141514918"><a name="zh-cn_topic_0000002082250312_p9764141514918"></a><a name="zh-cn_topic_0000002082250312_p9764141514918"></a>网卡</p>
</td>
<td class="cellrowborder" valign="top" width="42.76427642764276%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p1543012207413"><a name="zh-cn_topic_0000002082250312_p1543012207413"></a><a name="zh-cn_topic_0000002082250312_p1543012207413"></a>具体型号根据实际情况确定，例如Mellanox的CX4/CX5系列网卡。</p>
</td>
<td class="cellrowborder" valign="top" width="45.11451145114511%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0000002082250312_p66743541395"><a name="zh-cn_topic_0000002082250312_p66743541395"></a><a name="zh-cn_topic_0000002082250312_p66743541395"></a>需要支持RDMA和以太网并开启RDMA协议，开启RDMA协议的具体步骤请参见<a href="https://opengauss.org/zh/blogs/RDMA/RDMA%E7%BD%91%E7%BB%9C%E6%8C%87%E5%AF%BC_2024.html" target="_blank" rel="noopener noreferrer">《RDMA网络指导》</a>。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row125972322310"><td class="cellrowborder" valign="top" width="12.121212121212121%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p152468415531"><a name="zh-cn_topic_0000002082250312_p152468415531"></a><a name="zh-cn_topic_0000002082250312_p152468415531"></a>交换机</p>
</td>
<td class="cellrowborder" valign="top" width="42.76427642764276%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p14246134145318"><a name="zh-cn_topic_0000002082250312_p14246134145318"></a><a name="zh-cn_topic_0000002082250312_p14246134145318"></a>-</p>
</td>
<td class="cellrowborder" valign="top" width="45.11451145114511%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0000002082250312_p19402145392318"><a name="zh-cn_topic_0000002082250312_p19402145392318"></a><a name="zh-cn_topic_0000002082250312_p19402145392318"></a>默认使用RDMA协议，需要配置无损网络。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row16845115919221"><td class="cellrowborder" valign="top" width="12.121212121212121%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p14875172275315"><a name="zh-cn_topic_0000002082250312_p14875172275315"></a><a name="zh-cn_topic_0000002082250312_p14875172275315"></a>服务器</p>
</td>
<td class="cellrowborder" valign="top" width="42.76427642764276%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p4875172214532"><a name="zh-cn_topic_0000002082250312_p4875172214532"></a><a name="zh-cn_topic_0000002082250312_p4875172214532"></a>-</p>
</td>
<td class="cellrowborder" valign="top" width="45.11451145114511%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0000002082250312_p166212862416"><a name="zh-cn_topic_0000002082250312_p166212862416"></a><a name="zh-cn_topic_0000002082250312_p166212862416"></a>默认使用RDMA协议，需要配置无损网络。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row17172144822512"><td class="cellrowborder" valign="top" width="12.121212121212121%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p151721148152510"><a name="zh-cn_topic_0000002082250312_p151721148152510"></a><a name="zh-cn_topic_0000002082250312_p151721148152510"></a>操作系统</p>
</td>
<td class="cellrowborder" valign="top" width="42.76427642764276%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p15172174812251"><a name="zh-cn_topic_0000002082250312_p15172174812251"></a><a name="zh-cn_topic_0000002082250312_p15172174812251"></a>Arm：openEuler 22.03 LTS</p>
</td>
<td class="cellrowborder" valign="top" width="45.11451145114511%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0000002082250312_p417374815256"><a name="zh-cn_topic_0000002082250312_p417374815256"></a><a name="zh-cn_topic_0000002082250312_p417374815256"></a>同一版本，因内核小版本不一致，或存在内核组件安装时报不兼容错误，需要修改OS weak-modules脚本，具体修改方法请参见<a href="https://opengauss.org/zh/blogs/weak-modules/OS兼容性weak-modules脚本修改方法.html" target="_blank" rel="noopener noreferrer">《OS兼容性weak-modules脚本修改方法》</a>。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row14173164819251"><td class="cellrowborder" rowspan="3" valign="top" width="12.121212121212121%" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p12173548192516"><a name="zh-cn_topic_0000002082250312_p12173548192516"></a><a name="zh-cn_topic_0000002082250312_p12173548192516"></a>软件</p>
</td>
<td class="cellrowborder" valign="top" width="42.76427642764276%" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p148779365287"><a name="zh-cn_topic_0000002082250312_p148779365287"></a><a name="zh-cn_topic_0000002082250312_p148779365287"></a>MLNX_OFED_LINUX</p>
</td>
<td class="cellrowborder" valign="top" width="45.11451145114511%" headers="mcps1.2.4.1.3 "><p id="zh-cn_topic_0000002082250312_p17177324296"><a name="zh-cn_topic_0000002082250312_p17177324296"></a><a name="zh-cn_topic_0000002082250312_p17177324296"></a>开启RDMA协议，需要安装RDMA网卡驱动。</p>
<p id="zh-cn_topic_0000002082250312_p387703619288"><a name="zh-cn_topic_0000002082250312_p387703619288"></a><a name="zh-cn_topic_0000002082250312_p387703619288"></a>OS和OFED的配套关系请参见<a href="#zh-cn_topic_0000002082250312_table364115286293">表2</a>。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row4173848152519"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p148785361288"><a name="zh-cn_topic_0000002082250312_p148785361288"></a><a name="zh-cn_topic_0000002082250312_p148785361288"></a>openGauss-All-{version}-openEuler22.03-aarch64.tar.gz</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p2901951202814"><a name="zh-cn_topic_0000002082250312_p2901951202814"></a><a name="zh-cn_topic_0000002082250312_p2901951202814"></a>openGauss安装包。</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002082250312_row617317481251"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><p id="zh-cn_topic_0000002082250312_p12878143682814"><a name="zh-cn_topic_0000002082250312_p12878143682814"></a><a name="zh-cn_topic_0000002082250312_p12878143682814"></a>BoostKit-hcom_1.0.0_openeuler2003_aarch64.tar.gz</p>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="zh-cn_topic_0000002082250312_p158919512282"><a name="zh-cn_topic_0000002082250312_p158919512282"></a><a name="zh-cn_topic_0000002082250312_p158919512282"></a>HCOM安装包。</p>
</td>
</tr>
</tbody>
</table>

**表 2**  OS和OFED配套关系

<a name="zh-cn_topic_0000002082250312_table364115286293"></a>
<table><thead align="left"><tr id="zh-cn_topic_0000002082250312_row10641102852910"><th class="cellrowborder" valign="top" width="50%" id="mcps1.2.3.1.1"><p id="zh-cn_topic_0000002082250312_p1352842916304"><a name="zh-cn_topic_0000002082250312_p1352842916304"></a><a name="zh-cn_topic_0000002082250312_p1352842916304"></a>OS版本</p>
</th>
<th class="cellrowborder" valign="top" width="50%" id="mcps1.2.3.1.2"><p id="zh-cn_topic_0000002082250312_p9528929153019"><a name="zh-cn_topic_0000002082250312_p9528929153019"></a><a name="zh-cn_topic_0000002082250312_p9528929153019"></a>OFED版本</p>
</th>
</tr>
</thead>
<tbody><tr id="zh-cn_topic_0000002082250312_row164132818292"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0000002082250312_p45281529123017"><a name="zh-cn_topic_0000002082250312_p45281529123017"></a><a name="zh-cn_topic_0000002082250312_p45281529123017"></a>openEuler 22.03 LTS</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.2.3.1.2 "><p id="zh-cn_topic_0000002082250312_p155291729133014"><a name="zh-cn_topic_0000002082250312_p155291729133014"></a><a name="zh-cn_topic_0000002082250312_p155291729133014"></a>MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-aarch64.tgz</p>
</td>
</tr>
</tbody>
</table>

### 安装卸载<a name="ZH-CN_TOPIC_0000002148420794"></a>

将相关软件放在指定路径，并设置环境变量，部署HCOM4DB，后设置openGauss数据库配置项，重启数据库，即可启用RDMA特性。

**前置条件<a name="zh-cn_topic_0000002082095562_section1544415522710"></a>**

- 开启RDMA功能依赖CX4/CX5网卡，安装RDMA网卡驱动。
- openGauss已部署完成，默认数据库管理用户为omm。
- 获取对应操作系统和CPU架构的HCOM安装包，例如：BoostKit-hcom\_1.0.0\_openeuler2003\_aarch64.tar.gz。
- 主备节点已存在“$\{GAUSSHOME\}/lib“目录，该目录默认包含HCOM4DB的so文件libhcom4db.so。

**操作步骤<a name="zh-cn_topic_0000002082095562_section16893155672411"></a>**

1. 以数据库管理用户omm登录节点，并上传HCOM安装包，上传目录仅omm用户有写权限。
2. 进入安装包上传目录，解压缩HCOM安装包。

        ```
        tar -xzvf BoostKit-hcom_1.0.0_openeuler2003_aarch64.tar.gz --no-same-owner
        ```
3. 将libhcom.so拷贝至指定路径“$\{GAUSSHOME\}/lib/“。

    >[!NOTE]说明
    >建议用户根据业务使用情况，设置so文件权限最小化。

        ```
        cp -f BoostKit-hcom_1.0.0_openeuler2003_aarch64/hcom/lib/libhcom.so ${GAUSSHOME}/lib/
        ```

4. 设置环境变量OCK\_RPC\_LIB\_PATH（libhcom4db.so的路径）和HCOM4DB\_LIB\_PATH（libhcom.so的路径）。

    ```
    export OCK_RPC_LIB_PATH=${GAUSSHOME}/lib
    export HCOM4DB_LIB_PATH=${GAUSSHOME}/lib
    ```

5. 修改postgresql.conf配置文件。
    1. 打开$\{PGDATA\}/postgresql.conf文件。

        ```
        vim ${PGDATA}/postgresql.conf
        ```

    2. 按“i”进入编辑模式。将参数“ss\_interconnect\_type“的值改为RDMA，添加配置项ss\_rdma\_work\_config。

        ```
        ss_interconnect_type=RDMA
        ss_rdma_work_config ='6 10'     # OCK RDMA使用用户态poll方式，并绑定cpu [6 10]，空格分开
        ```

    3. 按“Esc”键，输入**:wq!**，按“Enter”保存并退出编辑。

6. （可选）退出om\_monitor进程。

    如果om\_monitor进程存在，则需要执行此操作。

    ```
    gs_om -t killmonitor
    ```

7. 重启数据库使RDMA生效。
    1. 停止openGauss。

        ```
        cm_ctl stop
        ```

    2. 启动openGauss。

        ```
        cm_ctl start
        ```

        >[!NOTE]说明
        >如果启动失败请根据openGauss日志目录下的“postgresql-_YYYY-MM-DD\_HHMMSS_.log”日志信息排查错误。

## SCRLock使用示例

### 介绍

SCRLock，全称smart cached remote lock，是一个带有本地锁缓存的分布式锁SDK。本章节主要介绍openGauss数据库SCRLock特性的安装使用，指导用户顺利完成操作。在资源池化场景下使用SCRLock提供分布式锁能力，提高分布式锁性能。

### 安装准备

#### 获取安装包

请通过openGauss社区获取SCRLock安装包。

#### 环境要求

**表 1**  环境要求

<a name="table4957735203620"></a>
<table><thead align="left"><tr id="row129571235133614"><th class="cellrowborder" valign="top" width="33.33333333333333%" id="mcps1.2.4.1.1"><p id="p16314156113820"><a name="p16314156113820"></a><a name="p16314156113820"></a>项目</p>
</th>
<th class="cellrowborder" valign="top" width="36.86368636863686%" id="mcps1.2.4.1.2"><p id="p3314269388"><a name="p3314269388"></a><a name="p3314269388"></a>配置描述</p>
</th>
<th class="cellrowborder" valign="top" width="29.8029802980298%" id="mcps1.2.4.1.3"><p id="p1831412633812"><a name="p1831412633812"></a><a name="p1831412633812"></a>说明</p>
</th>
</tr>
</thead>
<tbody><tr id="row095713514369"><td class="cellrowborder" valign="top" width="33.33333333333333%" headers="mcps1.2.4.1.1 "><p id="p143143612389"><a name="p143143612389"></a><a name="p143143612389"></a>网卡</p>
</td>
<td class="cellrowborder" valign="top" width="36.86368636863686%" headers="mcps1.2.4.1.2 "><p id="p2314662389"><a name="p2314662389"></a><a name="p2314662389"></a>具体型号根据实际情况确定，例如Mellanox的CX4/CX5系列网卡。</p>
</td>
<td class="cellrowborder" valign="top" width="29.8029802980298%" headers="mcps1.2.4.1.3 "><p id="p43144633810"><a name="p43144633810"></a><a name="p43144633810"></a>需要支持RDMA和以太网并开启RDMA协议，开启RDMA协议的具体步骤请参见<a href="https://opengauss.org/zh/blogs/RDMA/RDMA.html" target="_blank" rel="noopener noreferrer">《RDMA网络指导》</a>。</p>
</td>
</tr>
<tr id="row19957153511363"><td class="cellrowborder" valign="top" width="33.33333333333333%" headers="mcps1.2.4.1.1 "><p id="p58101109381"><a name="p58101109381"></a><a name="p58101109381"></a>操作系统</p>
</td>
<td class="cellrowborder" valign="top" width="36.86368636863686%" headers="mcps1.2.4.1.2 "><a name="ul1381017104389"></a><a name="ul1381017104389"></a><ul id="ul1381017104389"><li>Arm<a name="ul10810131033818"></a><a name="ul10810131033818"></a><ul id="ul10810131033818"><li>openEuler 22.03 LTS</li><li>openEuler 20.03 LTS</li><li>openEuler 20.03 LTS SP1</li><li>openEuler 20.03 LTS SP3</li></ul>
</li></ul>
<a name="ul1811151093816"></a><a name="ul1811151093816"></a><ul id="ul1811151093816"><li>x86<a name="ul48111610133812"></a><a name="ul48111610133812"></a><ul id="ul48111610133812"><li>openEuler 22.03 LTS</li><li>openEuler 20.03 LTS</li><li>openEuler 20.03 LTS SP1</li><li>openEuler 20.03 LTS SP3</li></ul>
</li></ul>
</td>
<td class="cellrowborder" valign="top" width="29.8029802980298%" headers="mcps1.2.4.1.3 "><p id="p8811910113812"><a name="p8811910113812"></a><a name="p8811910113812"></a>同一SP版本，因内核小版本不一致，或存在内核组件安装时报不兼容错误，需要修改OS weak-modules脚本，具体修改方法请参见<a href="https://opengauss.org/zh/blogs/weak-modules/OS%E5%85%BC%E5%AE%B9%E6%80%A7weak-modules%E8%84%9A%E6%9C%AC%E4%BF%AE%E6%94%B9%E6%96%B9%E6%B3%95.html" target="_blank" rel="noopener noreferrer">《OS兼容性weak-modules脚本修改方法》</a>。</p>
</td>
</tr>
<tr id="row129584352361"><td class="cellrowborder" rowspan="2" valign="top" width="33.33333333333333%" headers="mcps1.2.4.1.1 "><p id="p177171738193816"><a name="p177171738193816"></a><a name="p177171738193816"></a>软件</p>
</td>
<td class="cellrowborder" valign="top" width="36.86368636863686%" headers="mcps1.2.4.1.2 "><p id="p1545313308386"><a name="p1545313308386"></a><a name="p1545313308386"></a>MLNX_OFED_LINUX</p>
</td>
<td class="cellrowborder" valign="top" width="29.8029802980298%" headers="mcps1.2.4.1.3 "><p id="p1636162317388"><a name="p1636162317388"></a><a name="p1636162317388"></a>开启RDMA协议，需要安装RDMA网卡驱动。OS和OFED的配套关系请参见<a href="#table14845200152117">表2</a>。</p>
</td>
</tr>
<tr id="row1595893515362"><td class="cellrowborder" valign="top" headers="mcps1.2.4.1.1 "><a name="ul3453430153812"></a><a name="ul3453430153812"></a><ul id="ul3453430153812"><li>OCK_scrlock_openEuler-20.03-LTS-SP1-aarch64.tar.gz</li><li>OCK_scrlock_openEuler-20.03-LTS-SP1-x86_64.tar.gz</li><li>OCK_scrlock_openEuler-20.03-LTS-SP3-aarch64.tar.gz</li><li>OCK_scrlock_openEuler-20.03-LTS-SP3-x86_64.tar.gz</li><li>OCK_scrlock_openEuler-20.03-LTS-aarch64.tar.gz</li><li>OCK_scrlock_openEuler-20.03-LTS-x86_64.tar.gz</li><li>OCK_scrlock_openEuler-22.03-LTS-aarch64.tar.gz</li><li>OCK_scrlock_openEuler-22.03-LTS-x86_64.tar.gz</li></ul>
</td>
<td class="cellrowborder" valign="top" headers="mcps1.2.4.1.2 "><p id="p836152316381"><a name="p836152316381"></a><a name="p836152316381"></a>SCRLock安装包。openEuler-22.03-LTS-SP1、openEuler-22.03-LTS表示操作系统，aarch64、x86_64表示服务器架构，具体SCRLock安装包请用户根据实际情况选择。</p>
</td>
</tr>
</tbody>
</table>

**表 2**  OS和OFED配套关系

<a name="table14845200152117"></a>
<table><thead align="left"><tr id="row3845140142111"><th class="cellrowborder" rowspan="2" valign="top" id="mcps1.2.4.1.1"><p id="p884512017214"><a name="p884512017214"></a><a name="p884512017214"></a>OS版本</p>
</th>
<th class="cellrowborder" colspan="2" valign="top" id="mcps1.2.4.1.2"><p id="p88457082119"><a name="p88457082119"></a><a name="p88457082119"></a>OFED版本</p>
</th>
</tr>
<tr id="row1925415052312"><th class="cellrowborder" valign="top" id="mcps1.2.4.2.1"><p id="p82540017236"><a name="p82540017236"></a><a name="p82540017236"></a>x86</p>
</th>
<th class="cellrowborder" valign="top" id="mcps1.2.4.2.2"><p id="p17254170132313"><a name="p17254170132313"></a><a name="p17254170132313"></a>Arm</p>
</th>
</tr>
</thead>
<tbody><tr id="row58452010212"><td class="cellrowborder" valign="top" width="20.532053205320533%" headers="mcps1.2.4.1.1 mcps1.2.4.2.1 "><p id="p17879182642219"><a name="p17879182642219"></a><a name="p17879182642219"></a>openEuler 20.03 LTS</p>
</td>
<td class="cellrowborder" valign="top" width="39.843984398439844%" headers="mcps1.2.4.1.2 mcps1.2.4.2.2 "><p id="p18879826122218"><a name="p18879826122218"></a><a name="p18879826122218"></a>MLNX_OFED_LINUX-5.4-3.6.8.1-openeuler20.03-x86_64.tgz</p>
</td>
<td class="cellrowborder" valign="top" width="39.62396239623963%" headers="mcps1.2.4.1.2 "><p id="p15879826152218"><a name="p15879826152218"></a><a name="p15879826152218"></a>MLNX_OFED_LINUX-5.4-3.1.0.0-openeuler20.03-aarch64.tgz</p>
</td>
</tr>
<tr id="row11845170102119"><td class="cellrowborder" valign="top" width="20.532053205320533%" headers="mcps1.2.4.1.1 mcps1.2.4.2.1 "><p id="p1787932610227"><a name="p1787932610227"></a><a name="p1787932610227"></a>openEuler 20.03 LTS SP1</p>
</td>
<td class="cellrowborder" valign="top" width="39.843984398439844%" headers="mcps1.2.4.1.2 mcps1.2.4.2.2 "><p id="p18794268220"><a name="p18794268220"></a><a name="p18794268220"></a>MLNX_OFED_LINUX-5.4-3.6.8.1-openeuler20.03sp1-x86_64.tgz</p>
</td>
<td class="cellrowborder" valign="top" width="39.62396239623963%" headers="mcps1.2.4.1.2 "><p id="p8879102612211"><a name="p8879102612211"></a><a name="p8879102612211"></a>MLNX_OFED_LINUX-5.4-3.1.0.0-openeuler20.03sp1-aarch64.tgz</p>
</td>
</tr>
<tr id="row19846307217"><td class="cellrowborder" valign="top" width="20.532053205320533%" headers="mcps1.2.4.1.1 mcps1.2.4.2.1 "><p id="p687962642213"><a name="p687962642213"></a><a name="p687962642213"></a>openEuler 20.03 LTS SP3</p>
</td>
<td class="cellrowborder" valign="top" width="39.843984398439844%" headers="mcps1.2.4.1.2 mcps1.2.4.2.2 "><p id="p19879102662211"><a name="p19879102662211"></a><a name="p19879102662211"></a>MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler20.03sp3-x86_64.tgz</p>
</td>
<td class="cellrowborder" valign="top" width="39.62396239623963%" headers="mcps1.2.4.1.2 "><p id="p19879192622215"><a name="p19879192622215"></a><a name="p19879192622215"></a>MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler20.03sp3-aarch64.tgz</p>
</td>
</tr>
<tr id="row1055117240224"><td class="cellrowborder" valign="top" width="20.532053205320533%" headers="mcps1.2.4.1.1 mcps1.2.4.2.1 "><p id="p1688042672213"><a name="p1688042672213"></a><a name="p1688042672213"></a>openEuler 22.03 LTS</p>
</td>
<td class="cellrowborder" valign="top" width="39.843984398439844%" headers="mcps1.2.4.1.2 mcps1.2.4.2.2 "><p id="p1488032632210"><a name="p1488032632210"></a><a name="p1488032632210"></a>MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-x86_64.tgz</p>
</td>
<td class="cellrowborder" valign="top" width="39.62396239623963%" headers="mcps1.2.4.1.2 "><p id="p12880112619227"><a name="p12880112619227"></a><a name="p12880112619227"></a>MLNX_OFED_LINUX-5.8-1.1.2.1-openeuler22.03-aarch64.tgz</p>
</td>
</tr>
</tbody>
</table>

### 安装卸载

#### 一键部署SCRLock特性

SCRLock提供简易部署脚本，输入安装路径、安装用户和安装的节点信息，即可一键部署。

- 前置条件
    - 开启RDMA功能依赖CX4/CX5网卡。
    - openGauss已部署完成。
    - 获取对应操作系统和CPU架构的SCRLock安装包，例如：OCK\_scrlock\_openEuler-20.03-LTS-SP1-aarch64.tar.gz。
    - 主备节点已存在“$\{GAUSSHOME\}/lib“目录。
    - 所有需要部署SCRLock的服务器，需要相同的用户和密码，并且该用户需要具备执行**rmmod**、**rpm**、**depmod**、**modprobe**命令权限。

- 操作步骤
    1. 以<SCRLock-install-user\>登录节点并上传安装包，在安装包所在目录执行以下命令解压缩安装包。

        ```
        tar -xzvf OCK_scrlock_openEuler-20.03-LTS-SP1-aarch64.tar.gz
        ```

        解压目录内的文件如下。

        <a name="table8507191432716"></a>
        <table><thead align="left"><tr id="row15071414112712"><th class="cellrowborder" valign="top" width="56.95%" id="mcps1.1.3.1.1"><p id="p9507814172716"><a name="p9507814172716"></a><a name="p9507814172716"></a>文件名</p>
        </th>
        <th class="cellrowborder" valign="top" width="43.05%" id="mcps1.1.3.1.2"><p id="p10507614182712"><a name="p10507614182712"></a><a name="p10507614182712"></a>说明</p>
        </th>
        </tr>
        </thead>
        <tbody><tr id="row15507714172712"><td class="cellrowborder" valign="top" width="56.95%" headers="mcps1.1.3.1.1 "><p id="p8507111419272"><a name="p8507111419272"></a><a name="p8507111419272"></a>OCK_scrlock_openEuler_aarch64.tar.gz</p>
        </td>
        <td class="cellrowborder" valign="top" width="43.05%" headers="mcps1.1.3.1.2 "><p id="p15071142279"><a name="p15071142279"></a><a name="p15071142279"></a>源文件</p>
        </td>
        </tr>
        <tr id="row1650719141279"><td class="cellrowborder" valign="top" width="56.95%" headers="mcps1.1.3.1.1 "><p id="p1550781419273"><a name="p1550781419273"></a><a name="p1550781419273"></a>OCK_scrlock_openEuler_aarch64.tar.gz.txt</p>
        </td>
        <td class="cellrowborder" valign="top" width="43.05%" headers="mcps1.1.3.1.2 "><p id="p25077149277"><a name="p25077149277"></a><a name="p25077149277"></a>签名文件</p>
        </td>
        </tr>
        <tr id="row12507914102713"><td class="cellrowborder" valign="top" width="56.95%" headers="mcps1.1.3.1.1 "><p id="p95072014122711"><a name="p95072014122711"></a><a name="p95072014122711"></a>OCK_scrlock_openEuler_aarch64.tar.gz.cms</p>
        </td>
        <td class="cellrowborder" valign="top" width="43.05%" headers="mcps1.1.3.1.2 "><p id="p1050791416279"><a name="p1050791416279"></a><a name="p1050791416279"></a>描述文件</p>
        </td>
        </tr>
        </tbody>
        </table>

    2. 解压源文件压缩包。

        ```
        tar -xzvf OCK_scrlock_openEuler_aarch64.tar.gz
        ```

        解压后得到如下文件。

        <a name="table1281112820"></a>
        <table><thead align="left"><tr id="row1081912720"><th class="cellrowborder" valign="top" width="50%" id="mcps1.1.3.1.1"><p id="p10820127215"><a name="p10820127215"></a><a name="p10820127215"></a>文件名</p>
        </th>
        <th class="cellrowborder" valign="top" width="50%" id="mcps1.1.3.1.2"><p id="p782121429"><a name="p782121429"></a><a name="p782121429"></a>说明</p>
        </th>
        </tr>
        </thead>
        <tbody><tr id="row14817121023"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p158312922"><a name="p158312922"></a><a name="p158312922"></a>scrlock_lib</p>
        </td>
        <td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p14811129214"><a name="p14811129214"></a><a name="p14811129214"></a>so文件</p>
        </td>
        </tr>
        <tr id="row9810121227"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p98131217212"><a name="p98131217212"></a><a name="p98131217212"></a>scripts</p>
        </td>
        <td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p582012928"><a name="p582012928"></a><a name="p582012928"></a>安装脚本</p>
        </td>
        </tr>
        <tr id="row981312622"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p168171219214"><a name="p168171219214"></a><a name="p168171219214"></a>umdk_rpm</p>
        </td>
        <td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1688121724"><a name="p1688121724"></a><a name="p1688121724"></a>rpm依赖包</p>
        </td>
        </tr>
        <tr id="row10332401714"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p173419401674"><a name="p173419401674"></a><a name="p173419401674"></a>bin</p>
        </td>
        <td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1341140878"><a name="p1341140878"></a><a name="p1341140878"></a>签名验证文件</p>
        </td>
        </tr>
        </tbody>
        </table>

    3. 使用“bin“目录下的verification二进制，进行软件包签名验证。

        ```
        ./bin/verification OCK_scrlock_openEuler_aarch64.tar.gz OCK_scrlock_openEuler_aarch64.tar.gz.cms OCK_scrlock_openEuler_aarch64.tar.gz.txt
        ```

        >[!NOTE]说明
        >verification可执行二进制需要三个参数，按先后顺序分别是：源文件、签名文件、描述文件。

        - 验证成功控制台会输出：

            ```
            Starting to verify OCK_scrlock_openEuler_aarch64.tar.gz...
            Verify the source file passed.
            Verify the sha file passed.
            ```

        - 验证失败控制台会输出：

            ```
            Starting to verify OCK_scrlock_openEuler_aarch64.tar.gz...
            ...
            Verify the source file failed.
            ```

            或者

            ```
            Starting to verify OCK_scrlock_openEuler_aarch64.tar.gz...
            ...
            Verify the sha file failed.
            ```

            >[!NOTE]说明
            >校验失败说明此安装包已被篡改，建议重新获取安装包后再次校验。

    4. 安装包验证成功后，进入“scripts“目录，准备执行部署脚本。

        ```
        cd scripts
        ```

    5. 执行部署脚本，并根据提示输入<SCRLock-install-user\>密码。

        ```
        sh scrlock_install.sh -H '192.168.4.164 192.168.4.165 192.168.4.166' -U omm -G dbgrp -D /home/omm/lib
        ```

        >[!NOTE]说明
        >- -H：集群IP地址。例如：‘_192.168.4.164 192.168.4.165 192.168.4.166_’
        >- -U：数据库管理用户名。例如：omm
        >- -G：数据库管理用户组。例如：dbgrp
        >- -D：“$\{GAUSSHOME\}/lib“库路径。例如：“/home/omm/lib“
        >- -h：查看帮助信息。
        >由于部署脚本的操作需要高权限用户，请在操作执行完成后关闭高权限用户SSH远程登录服务器的权限，以提升系统安全性。

    6. <a name="li9627181442313"></a>切换至数据库管理用户。

        ```
        su - omm
        ```

    7. （可选）kill om\_monitor进程。

        ```
        gs_om -t killmonitor
        ```

    8. <a name="li153702610143"></a>加载环境变量。

        ```
        source ~/.bashrc
        ```

        >[!TIP]须知
        >[6](#li9627181442313)～[8](#li153702610143)也需要在备节点执行。

#### 启用SCRLock特性

启用SCRLock特性，需要通过修改配置文件，重启数据库使其生效。

- 前提条件
    - 主备节点已安装包含SCRLock特性的openGauss版本。
    - 已完成SCRLock特性的一键部署。
- 操作步骤
    1. 以数据库管理用户登录管理节点。
    2. 配置数据库的postgresql.conf文件。
        1. 打开postgresql.conf文件。

            ```
            vim postgresql.conf
            ```

        2. 按“i”进入编辑模式，找到如下参数，根据实际情况进行修改。参数说明请参见[表1](#table2032119112819)。

            ```
            ss_enable_scrlock = off
            ss_enable_srclock_sleep_mode = off
            ss_scrlock_server_port = 8000
            ss_scrlock_worker_count = 2
            ss_scrlock_worker_bind_core = ''
            ss_scrlock_server_bind_core = ''
            ```

            **表 1**  SCRLock的配置参数

            <a name="table2032119112819"></a>
            <table><thead align="left"><tr id="row153213142814"><th class="cellrowborder" valign="top" width="20%" id="mcps1.2.6.1.1"><p id="p1032118122817"><a name="p1032118122817"></a><a name="p1032118122817"></a>参数名称</p>
            </th>
            <th class="cellrowborder" valign="top" width="11.32%" id="mcps1.2.6.1.2"><p id="p12321612289"><a name="p12321612289"></a><a name="p12321612289"></a>参数类型</p>
            </th>
            <th class="cellrowborder" valign="top" width="27.68%" id="mcps1.2.6.1.3"><p id="p1792134743014"><a name="p1792134743014"></a><a name="p1792134743014"></a>参数说明</p>
            </th>
            <th class="cellrowborder" valign="top" width="31.869999999999997%" id="mcps1.2.6.1.4"><p id="p03212162814"><a name="p03212162814"></a><a name="p03212162814"></a>取值范围</p>
            </th>
            <th class="cellrowborder" valign="top" width="9.13%" id="mcps1.2.6.1.5"><p id="p133211811288"><a name="p133211811288"></a><a name="p133211811288"></a>默认值</p>
            </th>
            </tr>
            </thead>
            <tbody><tr id="row113217112281"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p16921116152919"><a name="p16921116152919"></a><a name="p16921116152919"></a>ss_enable_scrlock</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p33216114289"><a name="p33216114289"></a><a name="p33216114289"></a>布尔型</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p10321616286"><a name="p10321616286"></a><a name="p10321616286"></a>用于开启或关闭SCRLock。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><a name="ul259918256327"></a><a name="ul259918256327"></a><ul id="ul259918256327"><li>on，表示开启SCRLock。</li><li>off，表示关闭SCRLock。</li></ul>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p173219132818"><a name="p173219132818"></a><a name="p173219132818"></a>off</p>
            </td>
            </tr>
            <tr id="row1032171132813"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p962235515324"><a name="p962235515324"></a><a name="p962235515324"></a>ss_enable_scrlock_sleep_mode</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p173226192810"><a name="p173226192810"></a><a name="p173226192810"></a>布尔型</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p732212192811"><a name="p732212192811"></a><a name="p732212192811"></a>用于开启或关闭SCRLock睡眠模式。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><a name="ul035519605310"></a><a name="ul035519605310"></a><ul id="ul035519605310"><li>on，表示开启睡眠模式。</li><li>off，表示关闭睡眠模式。</li></ul>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p432251152820"><a name="p432251152820"></a><a name="p432251152820"></a>on</p>
            </td>
            </tr>
            <tr id="row632219111286"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p13393103633316"><a name="p13393103633316"></a><a name="p13393103633316"></a>ss_scrlock_server_port</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p53227110289"><a name="p53227110289"></a><a name="p53227110289"></a>整型</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p13221810289"><a name="p13221810289"></a><a name="p13221810289"></a>表示SCRLock服务端侦听端口号。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><p id="p1920015242136"><a name="p1920015242136"></a><a name="p1920015242136"></a>1024～65535</p>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p18913214346"><a name="p18913214346"></a><a name="p18913214346"></a>8000</p>
            </td>
            </tr>
            <tr id="row832210117286"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p10497618113418"><a name="p10497618113418"></a><a name="p10497618113418"></a>ss_scrlock_worker_count</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p13224115281"><a name="p13224115281"></a><a name="p13224115281"></a>整型</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p332217114288"><a name="p332217114288"></a><a name="p332217114288"></a>表示SCRLock客户端worker数量。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><p id="p36595403134"><a name="p36595403134"></a><a name="p36595403134"></a>2～16</p>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p163223112288"><a name="p163223112288"></a><a name="p163223112288"></a>2</p>
            </td>
            </tr>
            <tr id="row1532261192810"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p8930195020342"><a name="p8930195020342"></a><a name="p8930195020342"></a>ss_scrlock_worker_bind_core</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p1032219172810"><a name="p1032219172810"></a><a name="p1032219172810"></a>字符串</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p1728795315350"><a name="p1728795315350"></a><a name="p1728795315350"></a>表示SCRLock worker占用起止CPU。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><p id="p163221915285"><a name="p163221915285"></a><a name="p163221915285"></a>“开始CPU编号 结束CPU编号”，CPU编号中间空格分开，例如："10 15"</p>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p1632214122810"><a name="p1632214122810"></a><a name="p1632214122810"></a>“”</p>
            </td>
            </tr>
            <tr id="row632218172820"><td class="cellrowborder" valign="top" width="20%" headers="mcps1.2.6.1.1 "><p id="p4128839123517"><a name="p4128839123517"></a><a name="p4128839123517"></a>ss_scrlock_server_bind_core</p>
            </td>
            <td class="cellrowborder" valign="top" width="11.32%" headers="mcps1.2.6.1.2 "><p id="p73229115286"><a name="p73229115286"></a><a name="p73229115286"></a>字符串</p>
            </td>
            <td class="cellrowborder" valign="top" width="27.68%" headers="mcps1.2.6.1.3 "><p id="p2032210192820"><a name="p2032210192820"></a><a name="p2032210192820"></a>表示SCRLock server占用起止CPU。</p>
            </td>
            <td class="cellrowborder" valign="top" width="31.869999999999997%" headers="mcps1.2.6.1.4 "><p id="p193221162818"><a name="p193221162818"></a><a name="p193221162818"></a>“开始CPU编号 结束CPU编号”，CPU编号中间空格分开，例如：“10 15”</p>
            </td>
            <td class="cellrowborder" valign="top" width="9.13%" headers="mcps1.2.6.1.5 "><p id="p83223110285"><a name="p83223110285"></a><a name="p83223110285"></a>“”</p>
            </td>
            </tr>
            </tbody>
            </table>

        3. 按“Esc”键，输入 **:wq!**，按“Enter”保存并退出编辑。

    3. 重启数据库使SCRLock特性生效。
        1. 停止openGauss。

            ```
            cm_ctl stop
            ```

        2. 启动openGauss。

            ```
            cm_ctl start
            ```

            >[!NOTE]说明
            >如果启动失败请根据openGauss日志目录下的“postgresql-YYYY-MM-DD\_HHMMSS.log”日志信息排查错误。

    4. 验证SCRLock特性是否启用成功。

        ```
        gsql -d postgres -p 16600 -c "show ss_enable_scrlock"
        ```

        如果回显如下内容，则启用成功。

        ```
        ss_enable_scrlock
        -------------------
        on
        (1 row)
        ```

        >[!NOTE]说明
        >- -p 16600：16600为数据库端口号，请根据实际情况修改。
        >- 如需查看启用SCRLock特性的日志文件，可以执行以下命令动态查看最后20行。
        >
        > ```
        > tail -fn20 ${GAUSSLOG}/pg_log/scrlock*.log
        > ```

#### 关闭SCRLock特性

关闭SCRLock特性，需要重启数据库使配置生效。

- 操作步骤
    1. 配置数据库根目录下的postgresql.conf文件。
        1. 打开postgresql.conf文件。

            ```
            vim postgresql.conf
            ```

        2. 按“i”进入编辑模式。将参数“ss\_enable\_scrlock“的值改为“off“，关闭SCRLock特性。

            ```
            ss_enable_scrlock = off
            ```

        3. 按“Esc”键，输入 **:wq!**，按“Enter”保存并退出编辑。

    2. 停止openGauss。

        ```
        cm_ctl stop
        ```

    3. （可选）如需卸载SCRLock特性可执行此步骤。删除SCRLock动态库文件，并清除环境变量。

        ```
        rm -f /home/omm/lib/libscrlock.so
        unset OCK_SCRLOCK_LIB_PATH
        ```

        其中，“/home/omm/lib/”表示libscrlock.so所在目录的绝对路径。

    4. 重启openGauss。

        ```
        cm_ctl start
        ```
