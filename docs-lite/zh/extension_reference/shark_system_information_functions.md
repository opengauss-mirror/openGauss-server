# shark-系统信息函数

本章节只包含shark插件新增的系统信息函数。

## 会话信息函数

- @@FETCH_STATUS

    描述：返回最后一条游标FETCH语句的状态，该语句可以是针对连接当前打开的任何游标发出的。0表示FETCH成功，-1表示FETCH失败。

    返回值类型：int

    示例：

    ```
    select @@FETCH_STATUS;
    ```

- @@ROWCOUNT

    描述：返回受上一句影响的行数。如果行数大于20亿，请使用ROWCOUNT_BIG()。在使用JDBC获取上一句影响行数结果期间请勿使用获取元数据的接口访问数据库，否则可能会导致上一条sql被覆盖。

    返回值类型：int

    示例：

    ```
    select @@ROWCOUNT;
    ```

- ROWCOUNT_BIG()

    描述：返回受上一句影响的行数。该函数的功能与@@ROWCOUNT类似，区别在于ROWCOUNT_BIG()的返回类型为bigint。

    返回值类型：bigint

    示例：

    ```
    select ROWCOUNT_BIG();
    ```

- @@SPID

    描述：返回当前用户进程的会话ID。

    返回值类型：bigint

    示例：

    ```
    select @@SPID;
    ```

- scope_identity()

    描述：返回插入到同一作用域中标识列内的最后一个标识值。

    返回值类型：numeric(38, 0)

    示例：

    ```
    openGauss=# CREATE TABLE TZ(Z_id INT IDENTITY PRIMARY KEY, Z_name VARCHAR(20) NOT NULL);
    CREATE TABLE
    openGauss=# INSERT INTO TZ(Z_NAME) VALUES('Lisa');
    INSERT 0 1
    openGauss=# SELECT scope_identity();
    scope_identity 
    ----------------
                1
    (1 row)
    ```

## 对象信息函数

- object_id('[database_name.[schema_name]. | schema_name.]object_name' [, 'object_type'])

    描述：返回数据库对象的oid。如果没有查询权限或者对象不存在则返回NULL。
    
    第二个参数object_type支持以下类型

<table aria-label="表 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>属性名称</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>S</td>
            <td>系统表</td>
        </tr>
        <tr>
            <td>U</td>
            <td>用户表</td>
        </tr>
        <tr>
            <td>V</td>
            <td>视图</td>
        </tr>
        <tr>
            <td>SO</td>
            <td>序列</td>
        </tr>
        <tr>
            <td>C</td>
            <td>check约束</td>
        </tr>
        <tr>
            <td>D</td>
            <td>DEAULTA约束</td>
        </tr>
        <tr>
            <td>F</td>
            <td>FOREIGN KEY约束</td>
        </tr>
        <tr>
            <td>PK</td>
            <td>主键约束</td>
        </tr>
        <tr>
            <td>UQ</td>
            <td>UNIQUE约束</td>
        </tr>
        <tr>
            <td>AF</td>
            <td>聚合函数</td>
        </tr>
        <tr>
            <td>FN</td>
            <td>函数</td>
        </tr>
        <tr>
            <td>P</td>
            <td>存储过程</td>
        </tr>
        <tr>
            <td>TR</td>
            <td>触发器</td>
        </tr>
    </tbody>
</table>

    返回值类型：int

    示例：

    ```
    CREATE TABLE sys.students (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT DEFAULT 0,
        grade DECIMAL(5, 2)
    );
    set search_path = 'sys';
    select object_id('students');
    object_id 
    -----------
    16666
    (1 row)

    select object_id('sys.students', 'U');
    object_id 
    -----------
    16666
    (1 row)
    ```

- objectproperty(oid, property)

    描述：返回插件框架中对象的对应属性结果。对象类型不符合返回NULL。
    
    property可选范围

    返回值类型：int

    **表1** property属性表

<table aria-label="表 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>属性名称</th>
            <th>对象类型</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>IsDefault</td>
            <td>任何对象</td>
            <td>返回0。</td>
        </tr>
        <tr>
            <td>IsDefaultCnst</td>
            <td>任何对象</td>
            <td>是否为DEFAULT约束。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsDeterministic</td>
            <td>函数</td>
            <td>返回0。</td>
        </tr>
        <tr>
            <td>IsIndexed</td>
            <td>表、视图</td>
            <td>有索引的表或视图。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsInlineFunction</td>
            <td>函数</td>
            <td>内联函数。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsSysShipped</td>
            <td>任何对象</td>
            <td>sys框架下的对象。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsPrimaryKey</td>
            <td>任何对象</td>
            <td>是否为PRIMARY KEY约束。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsProcedure</td>
            <td>任何对象</td>
            <td>是否为存储过程。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsRule</td>
            <td>任何对象</td>
            <td>返回0。</td>
        </tr>
        <tr>
            <td>IsScalarFunction</td>
            <td>函数</td>
            <td>是否为标量值函数。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsSchemaBound</td>
            <td>函数、视图</td>
            <td>返回0。</td>
        </tr>
        <tr>
            <td>IsTable</td>
            <td>表</td>
            <td>是否为表。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsTableFunction</td>
            <td>函数</td>
            <td>是否为表值函数。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsTrigger</td>
            <td>任何对象</td>
            <td>是否为触发器。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsUserTable</td>
            <td>表</td>
            <td>是否为用户表。1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsView</td>
            <td>视图</td>
            <td>是否为视图。1=True, 0=False</td>
        </tr>
        <tr>
            <td>OwnerId</td>
            <td>任何对象</td>
            <td>返回对象所有者的oid。</td>
        </tr>
        <tr>
            <td>ExeclsQuotedIdentOn</td>
            <td>函数、存储过程、触发器、视图</td>
            <td>返回1。</td>
        </tr>
        <tr>
            <td>ExeclsIsAnsiNullsOn</td>
            <td>函数、存储过程、触发器、视图</td>
            <td>返回1。</td>
        </tr>
        <tr>
            <td>TableFulltextPopulateStatus</td>
            <td>表</td>
            <td>返回0。</td>
        </tr>
        <tr>
            <td>TableHasVarDecimalStorageFormat</td>
            <td>表</td>
            <td>返回0。</td>
        </tr>
    </tbody>
</table>

    示例：
    其中database为当前数据库

    ```
    CREATE TABLE sys.students (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT DEFAULT 0,
        grade DECIMAL(5, 2)
    );
    set search_path = 'sys';
    select objectproperty(object_id('students'), 'ownerid') as ownerid;
     ownerid 
    ---------
    10
    (1 row)
    select objectproperty(object_id('sys.students'), 'istable') as ownerid;
     ownerid 
    ---------
    1
    (1 row)
    select objectproperty(object_id('database.sys.students'), 'isview') as ownerid;
     ownerid 
    ---------
    0
    (1 row)
    ```

- databasepropertyex(database, property)

    描述：对于指定的数据库，此函数返回指定数据库选项或属性的当前设置。

    参数类型：
    - `database`数据类型为nvarchar(128)，用于指定`databasepropertyex`要返回其命名属性信息的数据库的名称。
    - `property`数据类型为varchar(128)，用于指定要返回的数据库属性名称。

    返回值类型：sql_variant

    表2：property属性表

    <table aria-label="表 2" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>属性名称</th>
                <th>字段说明</th>
                <th>返回值</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Collation</td>
                <td>数据库默认字符序</td>
                <td>返回pg_database中查询数据库的datcollate属性值</td>
            </tr>
            <tr>
                <td>ComparisonStyle</td>
                <td>字符序规则的Windows比较样式</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>Edition</td>
                <td>数据库版本或者服务层级</td>
                <td>返回Standard</td>
            </tr>
            <tr>
                <td>IsAnsiNullsEnabled</td>
                <td>所有和null的比较值被作为unknown</td>
                <td>openGauss中为会话级别参数，默认返回为1</td>
            </tr>
            <tr>
                <td>IsAnsiPaddingEnabled</td>
                <td>在比较或者插入前，字符串将被填充到相同长度</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsAnsiWarningsEnabled</td>
                <td>发生标准错误条件时，会发出错误消息或者警告消息，如果当聚合函数中出现Null值，会发出错误和警告</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsArithmeticAbortEnabled</td>
                <td>如果执行查询时发生溢出或被零除错误，将结束查询</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsAutoClose</td>
                <td>在最后一个用户退出后，数据库完全关闭并释放资源</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsAutoCreateStatistics</td>
                <td>查询优化器根据需要创建单列统计信息以提高查询性能</td>
                <td>openGauss中默认为1，返回1</td>
            </tr>
            <tr>
                <td>IsAutoCreateStatisticsIncremental</td>
                <td>条件允许时，创建的单列统计信息递增</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsAutoShrink</td>
                <td>数据库文件定期收缩</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsAutoUpdateStatistics</td>
                <td>查询优化器会自动更新潜在的过期统计信息</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsClone</td>
                <td>数据库是使用DBCC CLONEDATABASE创建的一个用户的数据库的schema-only和statistic-only的副本</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsCloseCursorsOnCommitEnabled</td>
                <td>事务提交后，会关闭所有打开的游标</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsDatabaseSuspendedForSnapshotBackup</td>
                <td>数据库已挂起</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsFulltextEnabled</td>
                <td>支持对数据库进行全文和语义检索</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsInStandBy</td>
                <td>数据库以只读方式联机，同时支持恢复日志</td>
                <td>1为true，0为false</td>
            </tr>
            <tr>
                <td>IsLocalCursorsDefault</td>
                <td>游标声明默认为LOCAL</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsMemoryOptimizedElevateToSnapshotEnabled</td>
                <td>事务隔离级别设置为读提交，读未提交及其以下隔离级别时，使用SNAPSHOT隔离访问内存优化表</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsMergePublished</td>
                <td>如果安装了复制(备份)，允许支持数据库表发布用来合并复制(备份)</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsNullConcat</td>
                <td>Null拼接操作产生Null</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsNumericRoundAbortEnabled</td>
                <td>表达式中精度缺失将产生错误</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsParameterizationForced</td>
                <td>参数化数据库是否设置为FORCED</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsQuotedIdentifersEnabled</td>
                <td>允许使用双引号</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsPublished</td>
                <td>如果安装了复制，支持发布数据库表供快照复制或者事务复制使用</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsRecursiveTriggersEnable</td>
                <td>递归触发器启用</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsSubscribed</td>
                <td>数据库订阅以发布</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsSyncWithBackup</td>
                <td>数据库为发布数据库或分布式数据库，并且支持在不中断事务复制的情况下还原</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsTornPageDetectionEnabled</td>
                <td>检测断电或者其他系统故障导致的不完全I/O操作</td>
                <td>1为true，0为false</td>
            </tr>
            <tr>
                <td>IsVerifiedClone</td>
                <td>数据库是使用DBCC CLONEDATABASE的WITH VERIFY_CLONEDB选项创建的schema-only和statistics-only的用户数据库复制</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>IsXTPSupported</td>
                <td>数据库是否支持XTP</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>LastGoodCheckDbTime</td>
                <td>指定数据库上最后一次成功的DBCC CHECKDB日期和时间</td>
                <td>返回NULL</td>
            </tr>
            <tr>
                <td>LCID</td>
                <td>排序规则的Windows区域设置标识符</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>MaxSizeInBytes</td>
                <td>最大数据库大小(字节单位)</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>Recovery</td>
                <td>数据库恢复模式</td>
                <td>返回NULL</td>
            </tr>
            <tr>
                <td>ServiceObjective</td>
                <td>描述SQL数据库或Azure Synapse Analytics中的数据库性能级别</td>
                <td>返回NULL</td>
            </tr>
            <tr>
                <td>ServiceObjectiveId</td>
                <td>SQL数据库中的服务目标ID</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>SQLSortOrder</td>
                <td>早期版本中支持的排序ID</td>
                <td>返回0</td>
            </tr>
            <tr>
                <td>Status</td>
                <td>数据库状态</td>
                <td>返回ONLINE</td>
            </tr>
            <tr>
                <td>Updateability</td>
                <td>显示是否可以修改数据</td>
                <td>1为true，0为false</td>
            </tr>
            <tr>
                <td>UserAccess</td>
                <td>显示哪些用户可以访问数据库</td>
                <td>返回NULL</td>
            </tr>
            <tr>
                <td>Version</td>
                <td>用于创建数据库的代码的内部版本号</td>
                <td>返回openGauss版本序号</td>
            </tr>
            <tr>
                <td>ReplicaID</td>
                <td>已连接的超大规模数据库/副本的副本ID</td>
                <td>返回NULL</td>
            </tr>
        </tbody>
    </table>

    示例：

    ```
    openGauss=# SELECT databasepropertyex('existDB','Collation') AS Collation;
    collation  
    -------------
    zh_CN.UTF-8
    (1 row)
    ```

- suser_name(\[server_user_id\])

    描述：返回用户的登录表示名

    参数类型：
    - `server_user_id`数据类型为oid，用于指定`suser_name`要返回的用户的登录标识名对应的oid，当用户不输入任何用户oid时，该函数会默认返回当前用户的登录标识名，如果输入为NULL，该函数返回NULL。

    返回值类型：nvarchar(128)

    示例：

    ```
    openGauss=# SELECT suser_name(10) AS suser_name;
    suser_name 
    ------------
    user_name
    (1 row)
    ```

- suser_sname(\[server_user_sid\])

    描述：返回用户的登录表示名

    参数类型：
    - `server_user_sid`数据类型为varbinary(85)，用于指定`suser_sname`要返回的用户的登录标识名对应的oid，该函数目前等同于`suser_name`。

    返回值类型：nvarchar(128)

    示例：

    ```
    openGauss=# SELECT suser_sname(10::varbinary) AS suser_sname;
    suser_sname 
    -------------
    user_name
    (1 row)
    ```

- @@PROCID

    描述：返回当前模块的oid。模块可以是存储过程、用户自定义函数或触发器。

    返回值类型：oid

    示例：

    ```
    -- 创建存储过程，调用@@PROCID
    openGauss=# CREATE PROCEDURE test_procid
    openGauss-# AS
    openGauss$# DECLARE
    openGauss$#     ProcID integer;
    openGauss$# BEGIN
    openGauss$#     ProcID = @@PROCID;
    openGauss$#     RAISE INFO 'Stored procedure %', ProcID;
    openGauss$# END;
    openGauss$# /
    CREATE PROCEDURE
    
    -- 调用存储过程
    openGauss=# SELECT test_procid();
    INFO:  Stored procedure 49675
    test_procid 
    -------------
    
    (1 row)
    ```
