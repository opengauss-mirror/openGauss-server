### 工具介绍
本工具支持离线审计分析SQL语句在openGauss中的语法合法性。包含但不限于以下限制：
1. 仅支持单行SQL文本输入，且SQL之间以`;`分割。
2. 仅支持`dolphin`兼容性语法检查场景，其他兼容性的语法不兼容语句的报错信息可能不准确。
4. 不支持存储过程的语法兼容校验。
5. 对于SQL语句审计结果的支持判断：
   1. 语法兼容：openGauss支持该语法，但是实际使用过程中可能包含字段类型不支持、函数不存在等问题。
   2. 语法不兼容：openGauss不支持该语法。

### 编译工具

- 直接编译
```
cd opt/libog_query
sh build.sh
```
会在当前目录下生成一个libog_query.so文件，即为工具的动态库文件/

- 重新提取解析器源文件

| 环境依赖 | 备注            |
| -------------------------------- | --------------- |
| ruby            | 版本>=3.0.2 |
| ffi            | gem库，版本>=1.17.0 |
| ffi-clang            | gem库，版本>=0.8.0 |
| libclang            | Clang版本>=10 |

```
ruby scripts/extract_source_opengauss_dolphin.rb /opt/openGauss-server  /opt/binarylibs  /opt/openGauss-server/contrib/dolphin/libog_query/opengauss/source
```

1. 下载openGauss源码和Plugin源码，按照官网资料指导编译openGauss源码和dolphin源码


- openGauss源码路径：

> https://gitee.com/opengauss/openGauss-server

- openGauss插件仓路径：

> https://gitee.com/opengauss/Plugin

2. 进入`/opt/openGauss-server/contrib/dolphin/libog_query`路径下，`mkdir -p opengauss/source`创建解析器源代码输出路径后，执行上面的ruby脚本。其中`/opt/openGauss-server`为openGauss-server源码路经，`/opt/binarylibs`为编译好的三方库路径，`/opt/openGauss-server/contrib/dolphin/libog_query/opengauss/source`为提取的解析器源代码路径。
3. 之后在`/opt/openGauss-server/contrib/dolphin/libog_query/source`路径下，可以通过`sh build.sh`在当前目录编译得到工具库文件。或在`/opt/openGauss-server`下执行`build.sh`打包脚本将库文件打包到以下路径。

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

2. 编写python脚本调用库文件，使用`raw_parser_opengauss_dolphin`接口审计SQL语句，其中接口定义如下：

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
    create table t3(a pg_catalog.int4 b pg_catalog.int5 default 10, c varchar2(255) not null, d date, e varchar3(3), primary key(a,b));
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
				}]
		}]
}

True
```

