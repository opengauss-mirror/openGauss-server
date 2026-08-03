# Java SDK对接向量数据库

本文介绍如何使用Java语言调用openGauss向量数据库

## 要求

- 安装java1.8及以上版本
- Apache Maven

## 安装SDK

- 在线安装

    开发者可以直接从maven中央仓库中获取jar包[maven中央仓库下载](https://central.sonatype.com/artifact/org.opengauss/opengauss-jdbc)，也可以在openGauss官网下载[社区官网下载](https://opengauss.org/zh/download/)，运行以下命令安装Java  SDK

    ```xml
    <dependency>
        <groupId>org.opengauss</groupId>
        <artifactId>opengauss-jdbc</artifactId>
        <version>your version</version>
    </dependency>
    ```

- 离线安装

    1） 可以从开源社区下载openGauss-connector-jdbc源码

    ```bash
    git clone https://gitcode.com/opengauss/openGauss-connector-jdbc.git
    ```

    切换到代码目录运行`sh build.sh`,成功编译后会出现两个jar包，分别是opengauss-jdbc-${version}.jar与postgresql.jar（在output目录下）

    2） 可以直接获取相关jdbc的压缩包
    获取链接：`https://download-opengauss.osinfra.cn/archive_test/7.0.0-RC2/openGauss7.0.0-RC2.B019/openEuler20.03/arm/openGauss-JDBC-7.0.0-RC2.tar.gz`，可以根据操作系统版本和服务器架构自行选择对应压缩包，压缩包解压后得到opengauss-jdbc-${version}.jar与postgresql.jar。

## 基本操作

### 1.连接数据库

```java
public Connection getConnection(String username, String passwd)
{
    String driver = "org.opengauss.Driver";
    String sourceURL = "jdbc:opengauss://localhost:port/database_name";
    Connection conn = null;
    
    try {
        Class.forName(driver).getDeclaredConstructor().newInstance();
    } catch(Exception e) {
        e.printStackTrace();
        return null;
    }
    try {
        conn = DriverManager.getConnection(sourceURL, username, passwd);
        System.out.println("Connection succeed!");
    } catch(Exception e) {
        e.printStackTrace();
        return null;
    }
    return conn;
}
```

### 2.创建表

```java
// 执行普通SQL语句
public void ExecuteSQL(Connection conn, String sql)
{
    Statement stmt = null;
    try {
        stmt = conn.createStatement();
        int rc = stmt.executeUpdate(sql);
        stmt.close();
    } catch (SQLException e) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        e.printStackTrace();
    }
}

public void CreateTable(Connection conn, int dim)
{
    String sql = String.format("CREATE TABLE IF NOT EXISTS demotable(id INTEGER, content TEXT, embedding vector(%d));", dim);
    ExecuteSQL(conn, sql);
}
```

### 3.创建索引

```java
// 用L2距离创建HNSW类型的向量索引
public void CreateIndex(Connection conn)
{
    String sql = String.format("CREATE INDEX ON demotable USING hnsw (embedding vector_l2_ops);");
    ExecuteSQL(conn, sql);
}
```

### 4.插入/删除/更新

- 插入

 ```java
public void InsertDataSingle(Connection conn, int id, String content, String vector)
{
    String sql = String.format("INSERT INTO demotable VALUES(%d, '%s', '%s');", id, content, vector);
    ExecuteSQL(conn, sql);
}
```

- 删除

```java
public void DeleteData(Connection conn)
{
    String sql = String.format("DELETE FROM demotable where id > 10;");
    ExecuteSQL(conn, sql);
}
```

- 更新

```java
public void UpdateData(Connection conn, String vector)
{
    String sql = String.format("UPDATE demotable set embedding = '%s' where id = 10;", vector);
    ExecuteSQL(conn, sql);
}
```

### 5.查询

```java
public String findNearestVectors(Connection conn, int efsearch, String vector, int topK)
{
    Statement statement = null;
    ResultSet resultSet = null;
    String res = "";
    // 设置查询参数
    String paramsql = String.format("set hnsw_ef_search = %d;", efsearch);
    ExecuteSQL(conn, paramsql);
    String querysql = String.format("SELECT * FROM demotable ORDER BY embedding <-> '%s' LIMIT %d;", vector, topK);
    try {
        statement = conn.createStatement();
        resultSet = statement.executeQuery(querysql);
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String content = resultSet.getString("content");
            Object embed = resultSet.getObject("embedding");
            // 替换成你希望的结果
            res += "id: " + id + ", content: " + content + ",embedding: " + embed + "\n";
        }
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        try { if (resultSet != null) resultSet.close(); } catch(Exception e) {}
        try { if (statement != null) statement.close(); } catch(Exception e) {}
    }
    return res;
}
```

### 6.多向量并发查询

多向量召回支持在单次搜索请求中同时提交多个查询向量，openGauss将并行对查询向量进行搜索，并返回多组结果。

#### 函数名

```java
public List<List<Map<String, Object>>> executeMultiSearch(Map<String, String> dbConfig,
        String sqlTemplate, List<List<Object>> parameters, Map<String, Object> scanParams, int threadCount)
```

#### 输入参数

- dbConfig:数据库连接配置，包含jdbcUrl、user、password
- sqlTemplate:查询语句，要求是单条查询语句(select为首单词)、包含向量操作符（<->/<=>/<#>/<+>/<~>/<%>）
- parameters：查询参数，要求不为空
- scanParams：需要通过set设置的参数（如hnsw_ef_search、nprobes）
- threadCount:连接池最大连接数，和数据库最大连接数（由参数max_connections设置）有关，由于java使用的HikariCP连接池在创建阶段并没有预设置数据库连接，一般来说，同一时间内执行请求的最大连接数要小于数据库最大连接数，但是数据库对于管理员用户的连接限制会略超过max_connections设置。

#### 输出参数

- 查询结果，形式为`[[{id=1, embedding='[1,2,3]'},{id=2, embedding='[2,2,2]'}], [],...]`，表示n个查询向量对应的limit个结果。

#### 使用案例

```java
import java.util.*;
import org.opengauss.util.ParallelSearch

String jdbcUrl = "jdbc:opengauss://localhost:port/dbname?allowMultiQueries=true";
Map<String, String> dbConfig = new HashMap<>();
dbConfig.put("jdbcUrl", jdbcUrl);
dbConfig.put("username", "YourName");
dbConfig.put("auth", "YourPassword");

String sqlTemplate = "select id from demotable order by embedding <-> '?'::vector limit ?;";
int threadCount = 2;
Map<String, Object> scanParams = new HashMap<>();
scanParams.put("enable_seqscan", "off");
scanParams.put("hnsw_ef_search", 40);

List<List<Object>> parameters = new ArrayList<>();
parameters.add(new ArrayList<>(Arrays.asList(Arrays.toString(new int[]{1, 2, 3})), 1));
parameters.add(new ArrayList<>(Arrays.asList(Arrays.toString(new int[]{2, 2, 3})), 2));

ParallelSearch ps = new ParallelSearch();
List<List<Map<String, Object>>> res = ps.executeMultiSearch(dbConfig, sqlTemplate, parameters,  scanParams, threadCount);
```

## 用例

编译及运行命令：

```bash
javac -cp ./openGauss-connector-jdbc/output/opengauss-jdbc-7.0.0-RC2.jar <yourfilename>.java
java -cp ./openGauss-connector-jdbc/output/opengauss-jdbc-7.0.0-RC2.jar:. <yourfilename>
```

```java
public static void main(String[] args) {
        String username = "test2";     // 替换为你的用户名
        String password = "YourPassword"; // 替换为你的密码
        int embeddingDim = 3;

        Connection conn = getConnection(username, password);
        if (conn != null) {
            CreateTable(conn, embeddingDim);
            CreateIndex(conn);
            InsertDataSingle(conn, 0, "test", "[1,2,3]");
            DeleteData(conn);
            UpdateData(conn, "[1,1,1]");
            String res=findNearestVectors(conn, 20, "[2,2,2]", 2);
            System.out.println("连接成功！"+res);
            try {
                conn.close();
                System.out.println("连接已关闭。");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
```

[更多操作示例参考](https://gitcode.com/opengauss/openGauss-connector-jdbc)
[java常用示例](https://docs.opengauss.org/zh/docs/latest/getting_started/java.html)
