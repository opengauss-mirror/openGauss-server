### 工具介绍
本工具支持离线审计分析SQL语句在openGauss中的语法合法性。包含但不限于以下限制：
1. 仅支持单行SQL文本输入，且SQL之间以`;`分割。
2. 支持A库和B库兼容性语法检查场景，默认为B库兼容性，其他兼容性的语法不兼容语句的报错信息可能不准确。
3. 不支持存储过程的语法兼容校验。
4. 对于SQL语句审计结果的支持判断：
   - 语法兼容：openGauss支持该语法，但是实际使用过程中可能包含字段类型不支持、函数不存在等问题。
   - 语法不兼容：openGauss不支持该语法。
5. 仅支持审计在 openGauss 的语法解析层的错误。属于语义分析层或执行层等阶段处理的报错，本工具不支持审计：如枚举类型的匹配，Insert语句的目标列和值表达式的匹配等场景属于语义层，工具不会做检查。


### 编译工具

下载openGauss源码和Plugin源码，设置环境变量GAUSSHOME，按照官网资料指导编译openGauss源码和dolphin源码。


- openGauss源码路径：

> https://gitcode.com/opengauss/openGauss-server

- openGauss插件仓路径：

> https://gitcode.com/opengauss/Plugin

1. 方式一：直接编译

编译openGauss源码后，进入`/opt/openGauss-server/src/bin/libog_query`路径下，执行`build.sh`脚本即可。

```
cd /opt/openGauss-server/src/bin/libog_query
sh build.sh
```
会在当前目录下编译预提取的解析器源码，并生成一个libog_query.so文件，即为工具的动态库文件。执行`build.sh`支持传入兼容模式参数`--mode=A|B`，默认为B兼容模式，会编译当前目录下的`source_dolphin.tar.gz`，指定为A兼容模式时，会编译当前目录下的`source_server.tar.gz`。

2. 方式二：重新提取解析器源文件

| 环境依赖 | 备注            |
| -------------------------------- | --------------- |
| ruby            | 版本>=3.0.2 |
| ffi            | gem库，版本>=1.17.0 |
| ffi-clang            | gem库，版本>=0.8.0 |
| libclang            | Clang版本>=10 |

```shell
ruby scripts/extract_source_opengauss_dolphin.rb /opt/openGauss-server  /opt/binarylibs /opt/openGauss-server/src/bin/libog_query/opengauss/source compatibility_mode
```

1. 进入`/opt/openGauss-server/src/bin/libog_query`路径下，执行`mkdir -p opengauss/source`创建解析器源代码输出路径后，执行上面的ruby脚本会更新当前目录下的解析器源码tar包。脚本包括四个参数，其中`/opt/openGauss-server`为openGauss-server源码路经，`/opt/binarylibs`为三方库路径，`/opt/openGauss-server/src/bin/libog_query/opengauss/source`为提取的解析器源代码路径，`compatibility_mode`为提取解析器的兼容性模式，取值为A或B，对应更新解析器源码包`source_server.tar.gz`或`source_dolphin.tar.gz`。
2. 之后在`/opt/openGauss-server/src/bin/libog_query`路径下，可以通过`sh build.sh`在当前目录编译得到工具库文件。或在`/opt/openGauss-server`下执行`sh build.sh -m [debug | release | memcheck] -3rd [binarylibs path]`打包脚本将库文件打包到以下路径。

```
openGauss打包二进制路径
├── bin
├── lib
│   └── postgresql
│       └── ***libog_query.so***
└── share...
```

### 运行

1. 确保已编译得到libog_query.so库文件。

2. 确保LD_LIBRARY_PATH中包含了openGauss安装的二进制库路径（需要libcjson、libsecurec和libstdc++三个库）

3. 编写python脚本调用库文件，使用`raw_parser_opengauss_dolphin`接口审计SQL语句，其中接口定义如下：

```
OgQueryParseResult raw_parser_opengauss_dolphin(const char* str);
```
- 参数str：sql文本
- 返回值：OgQueryParseResult 类型，有parse_tree_json、err_msg和 is_passed三个成员变量
    - parse_tree_json：字符串，表示简化语法树信息的JSON字符串
    - err_msg：字符串，表示审计到的语法报错信息
    - is_passed：布尔值，表示是否通过语法规则检查

### 举例

```python
import ctypes
  
class OgQueryParseResult(ctypes.Structure):
    _fields_ = [
        ("parse_tree_json", ctypes.c_char_p),
        ("err_msg", ctypes.c_char_p),
        ("is_passed", ctypes.c_bool)
    ]

try:
    libpg_query = ctypes.CDLL('./libog_query.so')
    libpg_query.raw_parser_opengauss_dolphin.restype = OgQueryParseResult
    libpg_query.raw_parser_opengauss_dolphin.argtypes = [ctypes.c_char_p]

    sql = b"""
    create table t3(a pg_catalog.int4, b pg_catalog.int5 default 10, c varchar2(255) not null, d date, e varchar3(3), primary key(a,b));
    alter table t3 modify id varchar(20), alter column sdate type timestamptz(3);
    """
    result = libpg_query.raw_parser_opengauss_dolphin(sql)
    print(sql)
    print(result.parse_tree_json)
    print(result.err_msg)
    print(result.is_passed)

except Exception as e:
    print("Exception", e)

```

此时回显信息为：

```shell
    create table t3(a pg_catalog.int4, b pg_catalog.int5 default 10, c varchar2(255) not null, d date, e varchar3(3), primary key(a,b));
    alter table t3 modify id varchar(20), alter column sdate type timestamptz(3);
    
{
	"version":	"7.0.0 RC1",
	"stmts":	[{
			"stmtType":	"create",
			"stmts":	[],
			"fields":	[{
					"fieldName":	"a",
					"fieldType":	"int4"
				}, {
					"fieldName":	"b",
					"fieldType":	"int5"
				}, {
					"fieldName":	"c",
					"fieldType":	"varchar"
				}, {
					"fieldName":	"d",
					"fieldType":	"date"
				}, {
					"fieldName":	"e",
					"fieldType":	"varchar3"
				}],
			"constraints":	[{
					"contype":	"DEFAULT",
					"keys":	["b"]
				}, {
					"contype":	"NOTNULL",
					"keys":	["c"]
				}, {
					"contype":	"PRIMARY_KEY",
					"keys":	["a", "b"]
				}]
		}, {
			"stmtType":	"alter table",
			"stmts":	[],
			"fields":	[{
					"fieldName":	"id",
					"fieldType":	"varchar"
				}, {
					"fieldName":	"sdate",
					"fieldType":	"timestamptz"
				}],
			"relations":	[{
					"relName":	"t3"
				}]
		}]
}
None
True
```

- 说明：
	
1. json根据实际情况会包含以下字段，每个字段引用的也是一个json对象：

	```
	stmtType：语句类型
	stmts：嵌套语句
	relations：表名
	fields：字段
	constraints：主键约束
	funscs：函数名
	exprs：表达式名
	objects：删除/注释语句的对象名
	```

2. 语句类型：即StmtType的取值范围，当前支持以下dml和ddl语句类型：

	```
	-- DML
	insert
	delete
	update
	merge
	select
	-- DDL
	create [table]
	create type
	create index
	create sequence
	view
	alter table
	rename
	drop
	truncate
	prepare
	comment
	```
3. 约束类型：即constraintsJSON数组对象的元素中，contype的取值范围，当前支持以下的约束类型：

	```
	PRIMARY_KEY
	UNIQUE_KEY
	FOREIGN_KEY
	CHECK
	NOTNULL
	DEFAULT
	```
4. 对象类型：即DROP或COMMENT语句中对象的类型，当前支持以下的对象类型：

	```
    OBJECT_ACCESS_METHOD,
    OBJECT_AGGREGATE,
    OBJECT_AMOP,
    OBJECT_AMPROC,
    OBJECT_ATTRIBUTE,
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_CONSTRAINT,
    OBJECT_CONTQUERY,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DATA_SOURCE,
    OBJECT_DB4AI_MODEL,
    OBJECT_DEFAULT,
    OBJECT_DOMAIN,
    OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_INDEX_PARTITION,
    OBJECT_INTERNAL,
    OBJECT_INTERNAL_PARTITION,
    OBJECT_LANGUAGE,
    OBJECT_LARGE_SEQUENCE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PACKAGE,
    OBJECT_PACKAGE_BODY,
    OBJECT_PARTITION,
    OBJECT_RLSPOLICY,
    OBJECT_PARTITION_INDEX,
    OBJECT_ROLE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_STREAM,
    OBJECT_SYNONYM,
    OBJECT_TABCONSTRAINT,
    OBJECT_TABLE,
    OBJECT_TABLE_PARTITION,
    OBJECT_TABLESPACE,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER,
    OBJECT_VIEW,
    OBJECT_USER_MAPPING,
    OBJECT_DIRECTORY,
    OBJECT_GLOBAL_SETTING,
    OBJECT_COLUMN_SETTING,
    OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL,
    OBJECT_SUBSCRIPTION,
    OBJECT_EVENT
	```