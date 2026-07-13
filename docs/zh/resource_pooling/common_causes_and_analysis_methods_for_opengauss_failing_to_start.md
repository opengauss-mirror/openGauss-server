# openGauss无法拉起的常见原因及分析手段

## 一、问题现象

在openGauss集群启动过程中，集群中某个节点启动失败，现象表现为openGauss进程无法启动，池化下通过`cm_ctl query -Cvdip`查询为某个节点的`dms_res`资源的状态显示为`Deleted`;非池化下通过`cm_ctl query -Cvdip`或`gs_om -t status --detail`查询为某个节点的状态显示为`Down`。

## 二、常见原因

GaussDB启动失败的原因有很多种，下面列举几种常见的非代码或数据异常导致的原因：

1. `postgresql.conf`中的参数配置错误
    该类错误信息一般会在启动过程中直接打印到屏幕上，池化集群会打印到DN目录下的`DBstart.log`中，可以登录故障节点机器，进入`$PGDATA`目录下，在`DBstart.log`中查找`FATAL`，可以查找到如下所示的日志信息：（非池化集群如果是使用`gs_om`启动的，一般会直接打印到屏幕上）。

    ```shell
    2024-10-08 11:19:05.721 6704a4a9.10000 [unknown] 139630800800128 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] LOG:  invalid value for parameter "recovery_max_workers": "1as"
    2024-10-08 11:19:05.721 6704a4a9.10000 [unknown] 139630800800128 [unknown] 0 dn_6001_6002_6003 F0000 0 [BACKEND] FATAL:  configuration file "/xxx/postgresql.conf" contains errors
    ```

    同时在该日志的上面，一般会打印出是哪个参数配置的错误，无法通过校验，这种情况下一般修改为正确的参数取值范围即可正常启动。
2. 系统资源不足

    a. 数据库在启动过程中会根据`postgresql.conf`中配置的参数去申请对应的内存大小，如果系统内存资源不足，则可能无法启动，一般报错日志如下：

    ```shell
    2024-10-08 11:30:07.378 6704a73f.1 [unknown] 140397937584512 [unknown] 0 dn_6001_6002_6003 42809 0 [BACKEND] FATAL:  could not create shared memory segment: Cannot allocate memory
    ```

    该日志表示数据库无法申请到所配置大小的共享内存，这种情况可以通过调整`postgresql.conf`中`shared_buffers`的大小解决。

    b. 数据库在启动的故障恢复过程中也会去打开数据文件和日志文件，如果系统到数据库安装用户配置的`open files`的数值过小（在数据库安装用户下，使用`ulimit -a`可以确认当前配置），也可能导致无法启动，一般表现为在数据库的运行日志（`$GAUSSLOG/pg_log/dn_xxx/postgresql-XXXX-XX-XX_XXXXXX.log`）中打印`out of file descriptions`，或者在启动的日志中（前面提到的`DBstart.log`）中打印`insufficient file descriptors available to start server process`。这种情况下一般调整数据库安装用户的系统资源配置（在用户环境变量中配置`ulimit -n xxx`或修改`/etc/security/limits.conf`）即可正常启动。
3. `unix_socket`文件已存在或`unix_socket_directory`目录权限异常
    数据库在启动时需要在`unix_socket_directory`目录下（在`postgresql.conf`中可配置，默认为`/tmp`或者`$PGHOST`）创建一个`.s.PGSQL.XXXX`和`.s.PGSQL.XXXX.lock`的文件（`XXXX`为所配置的的数据库的端口号），如果对应目录下已存在同名文件，那么有可能导致数据库启动异常。一般这种情况会在数据库的运行日志中打印出类似`permission denied`和相关文件名的信息。遇到该现象后可以尝试修改目录权限或手动删除对应的已存在的同名文件即可解决。
4. 数据库运行日志中报错类似`FATAL:  could not locate required checkpoint record`的信息
    数据库在启动阶段需要找到最后一次做checkpoint的xlog，该报错说明数据库无法在当前的pg_xlog目录中找到对应pg_control文件中记录的最后一次做checkpoint的xlog。遇到这种情况时，先确认pg_control中记录的最后一次checkpoint的位点是多少，使用命令`pg_controldata <DN目录>`查询，其中`Latest checkpoint location`即表示最后一次checkpoint的位置。

    ```shell
    $ pg_controldata /xxx/test/dn1
    WARNING: Calculated CRC checksum does not match value stored in file.
    Either the file is corrupt, or it has a different layout than this program
    is expecting.  The results below are untrustworthy.

    pg_control version number:            923
    Catalog version number:               201611171
    Database system identifier:           18020659220698737461
    Database cluster state:               unrecognized status code
    pg_control last modified:             Tue 08 Oct 2024 04:23:38 PM CST
    Latest checkpoint location:           10/9567AE80
    ```

    如上，最后一次checkpoint的位置是`10/9567AE80`，然后根据该位置，获取对应的xlog文件的名称（可以通过`pg_xlogfile_name`函数获取到对应的xlog文件名称，`gsql`连接到数据库中，执行`select pg_xlogfile_name('10/9567AE80');`即可），去pg_xlog目录中找是否存在对应的文件，如果存在对应文件，说明可能是xlog文件中对应的偏移位置无效或损坏，或找不到对应的文件。遇到这种情况，一般如果有备份的xlog话，可以通过备份的xlog尝试恢复，但可能会存在数据丢失的风险。如果有全量的备份且和当前pg_xlog中的xlog文件可以衔接上的话，可以尝试通过PITR方式恢复。如果均没有，请联系技术人员支持。
5. 其他更多常见原因可以参考[服务启动失败](./service_startup_failure.md)。

## 三、分析手段

1. 对于启动失败的原因定位，可结合启动日志和运行日志共同分析。如果不好找到启动日志，可以尝试通过`gaussdb -D <DN目录>`的方式手动启动，观察打印到屏幕的输出寻找相关信息。
2. 数据库启动可粗略的分为参数加载、资源分配、故障恢复三个阶段，通过观察运行日志中的相关日志，判断当前在哪个阶段失败，可以更好的帮助定界。比如如果从运行日志观察到没有打印`StartupXLOG`关键字的信息，说明数据库当前还没到故障恢复阶段，那么问题就和前面两种相关。
3. 部分场景下会因为回放异常导致无法启动，一般常见的有：

    a. 数据库报错`[REDO_LOG_TRACE]WAL contains references to invalid pages`，同时发生PANIC。这种情况是因为回放过程中发生了异常，存在回放某些xlog日志时，找不到对应的数据页面，或者回放不连续，页面的lsn与xlog日志中的lastlsn不匹配导致。遇到该场景时，一般重启也无法恢复，如果有备份的话，一般第一时间建议使用备份恢复。同时备份原有环境的相关日志信息，联系技术人员支持。

    b. 数据库报错`The Page's LSN[xx/xxxxxxxx] bigger than want set LSN [xx/xxxxxxxx]`，同时发生PANIC。这种情况是因为磁盘中的数据页面的LSN比重启后回放完成达到的LSN要大，常见的原因是人为删除了pg_xlog下的文件导致回放的数据缺失，或者pg_xlog下的文件异常导致回放未完成。遇到该场景时，建议第一时间使用备份恢复。

## 四、收集相关信息

对于启动异常的问题，一般需要收集如下信息：

1. 数据库运行日志：需要收集发生时刻的`$GAUSSLOG/pg_log`下面的日志以及最后一次正常运行的日志，一般如果条件允许的话，建议备份所有。
2. 数据库xlog：需要收集发生问题时从回放的起始位置到最新的xlog日志文件。回放的起始位置可以从数据库运行日志中搜索`redo record is at`关键字可以获取到，或者使用`pg_controldata <DN目录>`查询，其中`Latest checkpoint's REDO location`即表示回放开始的位置。如果条件允许的话，建议备份所有。
3. 数据库pg_clog和pg_csnlog目录：这2个目录一般不会很大，建议备份所有，如果目录较大，可以压缩备份，压缩后会很小。
4. 数据文件：如果能够在数据库运行日志中找到明显的报错的数据文件名称，那么备份对应的数据文件即可。
5. DMS日志：池化架构的集群，需要同时收集DMS日志，在`$GAUSSLOG/pg_log/DMS`下面。
6. DSS日志：池化架构的集群，建议同时收集DSS日志，在`$GAUSSLOG/pg_log/DSS`和`$DSS_HOME/log/run`下面。
