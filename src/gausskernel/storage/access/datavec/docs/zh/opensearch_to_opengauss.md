# 从OpenSearch迁移至openGauss

本文档详细介绍 OpenSearch 至 openGauss 数据迁移工具的使用方法及核心迁移规则，帮助用户高效、准确地完成数据迁移工作。

## 环境准备

### 版本限制

| 组件       | 版本要求                   |
| :--------- | :------------------------- |
| OpenSearch | 1.3.4 及以上               |
| openGauss  | 7.0.0-RC1 及以上向量数据库 |
| Python     | 3.7 及以上                 |

### Python 依赖安装

```bash
# openGauss 数据库驱动
pip3 install psycopg2

# OpenSearch 官方 Python 客户端
pip3 install opensearch-py
```

## 工具简介

### 功能概述

本迁移工具支持将 OpenSearch 中的索引完整迁移至 openGauss 数据库：

- 索引映射（Mapping）中的字段 → openGauss 表结构
- 索引存储的文档 → openGauss 表数据

### 运行模式

工具提供三种运行模式，适配不同迁移阶段的需求：

| 模式 | 命令      | 功能说明                                                     |
| :--- | :-------- | :----------------------------------------------------------- |
| 导出 | `export`  | 将 OpenSearch 索引的 Mapping 导出为字段描述 CSV，文档数据导出为数据 CSV |
| 导入 | `import`  | 读取字段描述 CSV 在 openGauss 中建表，通过 `COPY` 命令高效导入数据CSV |
| 迁移 | `migrate` | 一键完成“导出+导入”全流程                                    |

## 迁移工具

### 配置文件

创建 `config.ini` 文件，参考以下模板进行配置：

```ini
[opensearch]
# OpenSearch连接配置（导出、迁移时生效）
host = localhost
port = 9200
username = 
password = 
use_ssl = false
# 索引名称（导入、导出、迁移共用）
index = my_index

[opengauss]
# openGauss数据库连接配置（导入、迁移时生效）
host = localhost
port = 5432
database = your_database
# 请使用md5加密用户
username = your_username
password = ******
# 目标数据库schema，默认为public，配置成其他schema时，需要确保该schema存在
schema = public
# 目标表名（可选，如果不指定则使用索引名作为表名）
table_name = 

[storage]
# CSV文件存储目录（导入、导出、迁移时生效）
data_dir = output

[export]
# 导出配置项（导出、迁移时生效）
scroll_size = 1000
scroll_time = 5m
# 每个CSV文件存储的数据行数（不含表头），0或负数表示不分片
csv_rows_per_file = 1000000

[migrate]
# 迁移模式配置项（迁移时生效）
# 是否保留导出的CSV文件（true/false）
keep_files = false
```

### 迁移脚本

创建`opensearch2opengauss.py`文件，并填充如下代码：

```python
from typing import Dict, Any, List, Tuple, Optional
import csv
import io
import json
import configparser
import sys
import os
import argparse
import glob
import re
import logging
from opensearchpy import OpenSearch
import psycopg2
from psycopg2 import sql


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class OpenSearchToOpenGaussMigrator:
    """OpenSearch到openGauss迁移工具"""
    
    OPENGAUSS_KEYWORDS = {
        "select", "insert", "update", "delete", "drop", "table", "from", "where", "group",
        "by", "having", "order", "limit", "join", "inner", "left", "right", "full", "union",
        "all", "distinct", "as", "on", "and", "or", "not", "null", "true", "false", "case",
        "when", "then", "else", "end", "exists", "like", "in", "between", "is", "like",
        "references", "foreign", "primary", "key", "unique", "check", "default", "constraint",
        "index", "varchar", "text", "int", "bigint", "smallint", "boolean", "timestamp", "user"
    }
    
    TYPE_MAPPING = {
        "byte": "smallint",
        "short": "smallint",
        "integer": "integer",
        "long": "bigint",
        "float": "real",
        "double": "float8",
        "half_float": "real",
        "keyword": "text",
        "text": "text",
        "binary": "bytea",
        "boolean": "boolean",
        "date": "timestamp",
        "date_nanos": "text",
        "ip": "inet",
        "integer_range": "int4range",
        "long_range": "int8range",
        "float_range": "numrange",
        "double_range": "numrange",
        "date_range": "tsrange",
        "object": "jsonb",
        "nested": "jsonb",
        "geo_point": "text",
        "knn_vector": "vector",
        "_id": "text",
        "join:name": "text",
        "join:parent": "text",
    }

    MAX_TABLE_NAME_LENGTH = 58
    
    def __init__(self, config_file: str = 'config.ini', 
                 cli_index: Optional[str] = None,
                 cli_table: Optional[str] = None):
        """
        初始化迁移器
        
        Args:
            config_file: 配置文件路径
            cli_index: 命令行指定的索引名称
            cli_table: 命令行指定的表名
        """
        self.config_file = config_file
        self.cli_index = cli_index
        self.cli_table = cli_table
        self.config = self._init_config()
        self.os_client = None
        self.og_connection = None
        self.og_cursor = None
        self.array_fields = set()
        self.vector_fields = {}
        self.data_files = None
    
    def _init_config(self) -> configparser.ConfigParser:
        """加载配置文件"""
        config = configparser.ConfigParser()
        config.optionxform = str
        
        if not os.path.exists(self.config_file):
            logger.error(f"配置文件不存在: {self.config_file}")
            sys.exit(1)
        
        try:
            config.read(self.config_file, encoding='utf-8')
            logger.info(f"配置文件加载成功: {self.config_file}")
        except Exception as e:
            logger.error(f"读取配置文件失败: {e}")
            sys.exit(1)

        if 'opensearch' not in config:
            logger.error("配置文件中缺少 [opensearch] 段")
            sys.exit(1)

        if 'storage' not in config:
            logger.error("配置文件中缺少 [storage] 段")
            sys.exit(1)
        
        if self.cli_index:
            self.index_name = self.cli_index
        else:
            self.index_name = config.get('opensearch', 'index')
            if not self.index_name:
                logger.error("[opensearch] 段中必须指定 index 参数")
                sys.exit(1)

        self.data_dir = config.get('storage', 'data_dir', fallback='output')
        self.fields_file = os.path.join(self.data_dir, f"{self.index_name}_fields.csv")
        self.data_file_prefix = os.path.join(self.data_dir, self.index_name)
        
        return config
    
    def _load_opensearch_config(self):
        """加载OpenSearch配置"""
        self.os_host = self.config.get('opensearch', 'host', fallback='localhost')
        self.os_port = self.config.getint('opensearch', 'port', fallback=9200)
        self.os_username = self.config.get('opensearch', 'username', fallback=None)
        self.os_password = self.config.get('opensearch', 'password', fallback=None)
        self.os_use_ssl = self.config.getboolean('opensearch', 'use_ssl', fallback=False)
        
        if self.os_username == '':
            self.os_username = None
        if self.os_password == '':
            self.os_password = None

    def _load_opengauss_config(self):
        """加载openGauss配置"""
        if 'opengauss' not in self.config:
            logger.error("配置文件中缺少 [opengauss] 段")
            sys.exit(1)

        self.og_host = self.config.get('opengauss', 'host', fallback='localhost')
        self.og_port = self.config.getint('opengauss', 'port', fallback=5432)
        self.og_database = self.config.get('opengauss', 'database')
        self.og_username = self.config.get('opengauss', 'username')
        self.og_password = self.config.get('opengauss', 'password')
        self.og_schema = self.config.get('opengauss', 'schema', fallback='public')

        if not self.og_database:
            logger.error("[opengauss] 段中必须指定 database 参数")
            sys.exit(1)
        if not self.og_username:
            logger.error("[opengauss] 段中必须指定 username 参数")
            sys.exit(1)
        if not self.og_password:
            logger.error("[opengauss] 段中必须指定 password 参数")
            sys.exit(1)

        if self.cli_table:
            self.table_name = self.cli_table
        else:
            self.table_name = self.config.get('opengauss', 'table_name', fallback=None)
            if not self.table_name:
                self.table_name = self.index_name.replace('-', '_').replace('.', '_')
        
        if self.table_name in self.OPENGAUSS_KEYWORDS:
            self.table_name = f"{self.table_name}_"
        
        if len(self.table_name) > self.MAX_TABLE_NAME_LENGTH:
            logger.error(f"目标表名'{self.table_name}'超过{self.MAX_TABLE_NAME_LENGTH}个字符，迁移无法成功，请修改表名")
            sys.exit(1)
    
    def _load_export_config(self):
        """加载导出配置"""
        self.scroll_size = 1000
        self.scroll_time = '5m'
        self.csv_rows_per_file = 0
        if 'export' in self.config:
            self.scroll_size = self.config.getint('export', 'scroll_size', fallback=1000)
            self.scroll_time = self.config.get('export', 'scroll_time', fallback='5m')
            self.csv_rows_per_file = self.config.getint('export', 'csv_rows_per_file', fallback=0)
        
        self.use_sharding = self.csv_rows_per_file > 0
    
    def _load_migrate_config(self):
        """加载迁移配置"""
        self.migrate_keep_files = False
        if 'migrate' in self.config:
            self.migrate_keep_files = self.config.getboolean('migrate', 'keep_files', fallback=False)
    
    def _get_data_file_path(self, part_num: Optional[int] = None) -> Tuple[str, int]:
        """获取数据文件路径"""
        if self.use_sharding:
            return f"{self.data_file_prefix}_{part_num}.csv", part_num + 1
        else:
            return f"{self.data_file_prefix}.csv", 1
    
    def _extract_part_num(self, filepath: str):
        """从文件路径中提取分片编号"""
        match = re.search(r'_(\d+)\.csv$', filepath)
        return int(match.group(1)) if match else None

    def _find_all_data_files(self) -> List[str]:
        """查找所有数据文件（支持分片）"""
        files = []
        main_file = f"{self.data_file_prefix}.csv"
        if os.path.exists(main_file):
            files.append(main_file)
        
        pattern = f"{self.data_file_prefix}_*.csv"
        part_files = glob.glob(pattern)
        
        if part_files:
            part_files = [f for f in part_files if self._extract_part_num(f) is not None]
            part_files.sort(key=self._extract_part_num)

            if part_files:
                last_part_file = part_files[-1]
                last_part_num = self._extract_part_num(last_part_file)
                if last_part_num != len(part_files):
                    logger.warning(f"分片文件编号与预期不匹配，预期 {last_part_num}，实际存在 {len(part_files)} 个分片文件")
            
                if not files:
                    files = part_files
                else:
                    logger.warning(f"同时存在主文件和分片文件，将使用主文件: {main_file}")
        
        return files

    def _clear_index_csv_files(self):
        """清除所有CSV文件"""
        files = []
        pattern = f"{self.data_file_prefix}_*.csv"
        part_files = glob.glob(pattern)

        if part_files:
            part_files = [f for f in part_files if self._extract_part_num(f) is not None]

            if part_files:
                files = part_files
        
        main_file = f"{self.data_file_prefix}.csv"
        if os.path.exists(main_file):
            files.append(main_file)

        field_file = f"{self.data_file_prefix}_fields.csv"
        if os.path.exists(field_file):
            files.append(field_file)
        
        for file in files:
            os.remove(file)
            logger.info(f"已删除历史文件: {file}")
    
    def _init_opensearch_client(self):
        """初始化OpenSearch客户端"""
        if self.os_client is not None:
            return
        
        self.os_client = OpenSearch(
            hosts=[{'host': self.os_host, 'port': self.os_port}],
            http_compress=True,
            use_ssl=self.os_use_ssl,
            verify_certs=False if self.os_use_ssl else True,
            http_auth=(self.os_username, self.os_password) if self.os_username and self.os_password else None,
            timeout=30
        )
        
        logger.info("OpenSearch客户端已初始化")
    
    def _check_opensearch_connection(self):
        """检测OpenSearch是否可连接"""
        try:
            if not self.os_client.ping():
                logger.error("无法连接到OpenSearch服务")
                sys.exit(1)
            logger.info("OpenSearch服务连接成功")
        except Exception as e:
            logger.error(f"OpenSearch测试连接失败: {e}")
            sys.exit(1)

    def _check_index_exists(self):
        """检测索引是否存在"""
        try:
            if not self.os_client.indices.exists(index=self.index_name):
                logger.error(f"索引 '{self.index_name}' 不存在")
                sys.exit(1)
        except Exception as e:
            logger.error(f"检测索引是否存在失败: {e}")
            sys.exit(1)

    def _get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """获取索引的映射结构"""
        try:
            mapping = self.os_client.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']
        except Exception as e:
            logger.error(f"获取索引映射失败: {e}")
            sys.exit(1)
    
    def _get_vector_dimension(self, field_config: Dict[str, Any]) -> int:
        """获取向量字段的维度"""
        dimension = field_config.get('dimension', 0)
        if dimension > 0:
            return dimension
        
        knn_config = field_config.get('knn', {})
        if isinstance(knn_config, dict):
            dimension = knn_config.get('dimension', 0)
            if dimension > 0:
                return dimension
        
        vector_config = field_config.get('vector', {})
        if isinstance(vector_config, dict):
            dimension = vector_config.get('dimension', 0)
            if dimension > 0:
                return dimension
        
        return 0
    
    def _is_vector_field(self, field_type: str) -> bool:
        """判断字段是否为向量类型"""
        return field_type == 'knn_vector'
    
    def _parse_field(self, field_name: str, field_config: Dict[str, Any], 
                    parent_path: str = '') -> List[Tuple[str, str]]:
        """递归解析字段结构"""
        fields = []
        current_path = f"{parent_path}.{field_name}" if parent_path else field_name
        field_type = field_config.get('type', 'object')
        
        if self._is_vector_field(field_type):
            dimension = self._get_vector_dimension(field_config)
            if dimension > 0:
                self.vector_fields[current_path] = dimension
            else:
                logger.error(f"字段 {current_path} 类型为 {field_type}，但无法获取维度信息")
                sys.exit(1)
            fields.append((current_path, field_type))
            return fields
        
        if field_type == 'object':
            properties = field_config.get('properties', {})
            if properties:
                for sub_field, sub_config in properties.items():
                    fields.extend(self._parse_field(sub_field, sub_config, current_path))
            else:
                fields.append((current_path, field_type))
        
        elif field_type == 'nested':
            properties = field_config.get('properties', {})
            if properties:
                for sub_field, sub_config in properties.items():
                    fields.extend(self._parse_field(sub_field, sub_config, current_path))
            else:
                fields.append((current_path, field_type))
        
        elif field_type == 'join':
            fields.append((f"{current_path}.name", 'join:name'))
            fields.append((f"{current_path}.parent", 'join:parent'))
        
        else:
            fields.append((current_path, field_type))
        
        return fields
    
    def _parse_index_fields(self, index_name: str) -> Dict[str, str]:
        """解析索引中的所有字段结构"""
        mapping = self._get_index_mapping(index_name)
        fields_map = {}
        
        properties = mapping.get('properties', {})
        for field_name, field_config in properties.items():
            parsed_fields = self._parse_field(field_name, field_config)
            for field_path, field_type in parsed_fields:
                fields_map[field_path] = field_type
        
        sorted_fields = dict(sorted(fields_map.items()))
        sorted_fields = {"_id": "_id", **sorted_fields}
        return sorted_fields
    
    def _process_field_name(self, field_path: str) -> str:
        """处理字段名，将字段名称进行标准化处理"""
        target_fieldname = field_path.replace('.', '_')
        if target_fieldname in self.OPENGAUSS_KEYWORDS:
            target_fieldname = f"{target_fieldname}_"
        return target_fieldname
    
    def _get_field_mapping(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """获取索引字段映射关系及目标字段名映射关系"""
        try:
            fields_map = self._parse_index_fields(self.index_name)
            fieldnames = list(fields_map.keys())
            target_fieldnames = {
                field_path: self._process_field_name(field_path) 
                for field_path in fieldnames
            }
            logger.info(f"获取索引映射成功，从映射中解析出 {len(fieldnames)} 个字段")
            return fields_map, target_fieldnames
        except Exception as e:
            logger.error(f"解析索引字段失败: {e}")
            sys.exit(1)

    def _get_index_document_count(self, query: Dict[str, Any]) -> int:
        """获取索引中文档总数"""
        try:
            count_response = self.os_client.count(index=self.index_name, body=query)
            total_docs = count_response['count']
            logger.info(f"索引 {self.index_name} 中发现 {total_docs} 条文档")
            return total_docs
        except Exception as e:
            logger.error(f"获取索引 {self.index_name} 中的文档总数失败: {e}")
            return None

    def _parse_nested_value(self, doc: Dict[str, Any], field_path: str) -> Any:
        """解析文档中嵌套类型中字段的值"""
        parts = field_path.split('.')
        return self._parse_nested_value_recursive(doc, parts, 0, field_path)
    
    def _parse_nested_value_recursive(self, current: Any, parts: List[str], 
                                       index: int, full_path: str) -> Any:
        """递归解析嵌套值"""
        if current is None:
            return None
        
        if index >= len(parts):
            if isinstance(current, list) and full_path not in self.vector_fields:
                self.array_fields.add(full_path)
            return current
        
        current_part = parts[index]
        
        if isinstance(current, list):
            if full_path not in self.vector_fields:
                self.array_fields.add(full_path)
            
            values = []
            for item in current:
                value = self._parse_nested_value_recursive(item, parts, index, full_path)
                values.append(value)
            return self._merge_array_values(values)
        
        elif isinstance(current, dict):
            next_value = current.get(current_part)
            return self._parse_nested_value_recursive(next_value, parts, index + 1, full_path)
        
        else:
            return None
    
    def _merge_array_values(self, values: List[Any]) -> Any:
        """合并数组值"""
        if not values:
            return []
        
        has_dict = any(isinstance(v, dict) for v in values)
        if has_dict:
            return values
        
        has_list = any(isinstance(v, list) for v in values)
        if has_list:
            merged = []
            for v in values:
                if isinstance(v, list):
                    merged.extend(v)
                else:
                    merged.append(v)
            return merged
        
        return values
    
    def _get_field_value(self, doc: Dict[str, Any], field_path: str, field_type: str) -> Any:
        """根据字段路径从文档中获取值"""
        if field_type == '_id':
            return doc.get('_id', None)
        
        if field_type == 'join:name':
            parent_path = field_path[:-len('.name')]
            join_field = doc.get(parent_path, None)
            if isinstance(join_field, dict):
                return join_field.get('name', None)
            return join_field
        
        if field_type == 'join:parent':
            parent_path = field_path[:-len('.parent')]
            join_field = doc.get(parent_path, None)
            if isinstance(join_field, dict):
                return join_field.get('parent', None)
            return None
        
        if '.' not in field_path:
            current = doc.get(field_path, None)
            if field_path in self.vector_fields:
                return current
            if isinstance(current, list):
                self.array_fields.add(field_path)
            return current
        
        return self._parse_nested_value(doc, field_path)
    
    def _ensure_output_dir(self):
        """确保输出目录存在"""
        try:
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
                logger.info(f"创建输出目录: {self.data_dir}")
        except Exception as e:
            logger.error(f"检测/创建输出目录失败: {e}")
            sys.exit(1)

    def _export_fields_csv(self, target_fieldnames: Dict[str, str], fields_map: Dict[str, str]):
        """导出索引字段结构到CSV文件"""
        output_file = self.fields_file
        logger.info(f"导出字段结构到: {output_file}")
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=['field_path', 'field_type', 'is_array', 
                               'target_fieldname', 'target_fieldtype'],
                    restval='',
                    extrasaction='ignore',
                    delimiter=',',
                    quotechar='"',
                    escapechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=False
                )
                writer.writeheader()
                
                for field_path, field_type in fields_map.items():
                    row = {
                        'field_path': field_path,
                        'field_type': field_type,
                        'is_array': str(field_path in self.array_fields),
                        'target_fieldname': target_fieldnames[field_path]
                    }
                    
                    target_type = self.TYPE_MAPPING.get(field_type)
                    if target_type is None:
                        logger.warning(f"字段{field_path}类型为{field_type}，openGauss映射类型不存在，默认映射为 text 类型")
                        target_type = 'text'

                    if field_path in self.vector_fields:
                        row['field_type'] = f"{field_type}({self.vector_fields[field_path]})"
                        row['target_fieldtype'] = f"{target_type}({self.vector_fields[field_path]})"
                    elif row['is_array'] == 'True' and target_type != 'text':
                        row['target_fieldtype'] = f"{target_type}[]"
                    else:
                        row['target_fieldtype'] = target_type
                    
                    writer.writerow(row)
            
            logger.info("字段结构导出完成")
            
        except Exception as e:
            logger.error(f"导出字段结构失败: {e}")
            sys.exit(1)
    
    def _create_csv_writer(self, filepath: str, fieldnames: List[str], 
                           target_fieldnames: Dict[str, str]):
        """创建CSV writer并写入表头"""
        csvfile = open(filepath, 'w', newline='', encoding='utf-8')
        writer = csv.DictWriter(
            csvfile,
            fieldnames=fieldnames,
            restval='',
            extrasaction='ignore',
            delimiter=',',
            quotechar='"',
            escapechar='"',
            quoting=csv.QUOTE_MINIMAL,
            doublequote=False
        )
        writer.writerow(target_fieldnames)
        return csvfile, writer
    
    def _close_csv_writer(self, csvfile_writer, data_files: List[str], current_file_rows: int):
        """关闭CSV writer"""
        if csvfile_writer:
            csvfile_writer.close()
            logger.info(f"CSV文件写入完成: {data_files[-1]} ({current_file_rows} 行)")

    def _convert_doc_to_csv_row(self, hit, fields_map):
        """将文档转换为CSV行数据"""
        doc = {'_id': hit['_id']}
        doc.update(hit.get('_source', {}))
        
        row = {}
        
        for field_path, field_type in fields_map.items():
            value = self._get_field_value(doc, field_path, field_type)
            
            if value is None:
                row[field_path] = 'NULL'
            elif isinstance(value, (dict, list)):
                out_value = json.dumps(value, ensure_ascii=False)
                if field_path not in self.vector_fields:
                    if out_value.startswith('['):
                        out_value = '{' + out_value[1:]
                    if out_value.endswith(']'):
                        out_value = out_value[:-1] + '}'
                row[field_path] = out_value
            else:
                row[field_path] = str(value)
        
        return row

    def _export_data_to_csv(self, query: Dict[str, Any] = None):
        """导出索引数据到CSV文件（支持分片）"""
        self._init_opensearch_client()
        self._check_opensearch_connection()
        self._check_index_exists()
        self._ensure_output_dir()
        self._clear_index_csv_files()
        
        data_files = []
        current_file_rows = 0
        part_num = 1
        current_csvfile = None
        current_writer = None
        
        fields_map, target_fieldnames = self._get_field_mapping()
        fieldnames = list(fields_map.keys())
        
        if query is None:
            query = {"query": {"match_all": {}}}
        total_docs = self._get_index_document_count(query)

        try:
            response = self.os_client.search(
                index=self.index_name,
                body=query,
                scroll=self.scroll_time,
                size=self.scroll_size,
                _source=True
            )
            
            scroll_id = response['_scroll_id']
            total_processed = 0
            hits = response['hits']['hits']
            while hits:
                for hit in hits:
                    if current_writer is None or (self.use_sharding and current_file_rows >= self.csv_rows_per_file):
                        self._close_csv_writer(current_csvfile, data_files, current_file_rows)
                        filepath, part_num = self._get_data_file_path(part_num)
                        current_csvfile, current_writer = self._create_csv_writer(
                            filepath, fieldnames, target_fieldnames
                        )
                        data_files.append(filepath)
                        current_file_rows = 0
                        logger.info(f"创建新文件: {filepath}")
                    
                    row = self._convert_doc_to_csv_row(hit, fields_map)
                    current_writer.writerow(row)
                    current_file_rows += 1
                    total_processed += 1
                
                logger.info(f"已处理: {total_processed}/{total_docs if total_docs else '未知'}")
                response = self.os_client.scroll(scroll_id=scroll_id, scroll=self.scroll_time)
                scroll_id = response['_scroll_id']
                hits = response['hits']['hits']
            
            self._close_csv_writer(current_csvfile, data_files, current_file_rows)
            self.os_client.clear_scroll(scroll_id=scroll_id)
            
        except Exception as e:
            if current_csvfile:
                current_csvfile.close()
            logger.error(f"导出数据失败: {e}")
            sys.exit(1)
        
        logger.info(f"数据导出完成: 共 {total_processed} 条文档，{len(data_files)} 个文件")
        for f in data_files:
            logger.info(f"  {f}")
        
        self.data_files = data_files
        self._export_fields_csv(target_fieldnames, fields_map)
    
    def _init_opengauss_connection(self):
        """初始化openGauss数据库连接"""
        if self.og_connection is not None:
            return
        
        try:
            self.og_connection = psycopg2.connect(
                host=self.og_host,
                port=self.og_port,
                dbname=self.og_database,
                user=self.og_username,
                password=self.og_password,
                options=f"-c search_path={self.og_schema}"
            )
            self.og_connection.set_client_encoding('UTF8')
            self.og_connection.autocommit = False
            self.og_cursor = self.og_connection.cursor()
            
            logger.info("openGauss连接成功")
            
        except Exception as e:
            logger.error(f"openGauss连接失败: {e}")
            sys.exit(1)
    
    def _close_opengauss_connection(self):
        """关闭openGauss数据库连接"""
        if self.og_cursor:
            self.og_cursor.close()
        if self.og_connection:
            self.og_connection.close()
            logger.info("openGauss连接已关闭")
        self.og_cursor = None
        self.og_connection = None
    
    def _read_fields_csv(self) -> List[Tuple[str, str, bool, str, str]]:
        """读取字段CSV文件"""
        fields_info = []
        fields_file = self.fields_file

        if not os.path.exists(fields_file):
            logger.error(f"字段文件不存在: {fields_file}")
            sys.exit(1)
        
        try:
            with open(fields_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    fields_info.append((
                        row['field_path'],
                        row['field_type'],
                        row['is_array'].lower() == 'true',
                        row['target_fieldname'],
                        row['target_fieldtype']
                    ))
            
            logger.info(f"读取字段文件: {fields_file}, 共 {len(fields_info)} 个字段")
            return fields_info
            
        except Exception as e:
            logger.error(f"读取字段文件失败: {e}")
            sys.exit(1)
    
    def _create_table(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """在openGauss中创建表"""
        columns_definitions = []
        for _, _, _, target_fieldname, target_fieldtype in fields_info:
            column_def = sql.Identifier(target_fieldname) + sql.SQL(' ') + sql.SQL(target_fieldtype)
            columns_definitions.append(column_def)
        
        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(columns_definitions)
        )
        
        try:
            drop_table_sql = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                sql.Identifier(self.table_name)
            )
            self.og_cursor.execute(drop_table_sql)
            self.og_cursor.execute(create_table_sql)
            self.og_connection.commit()
            logger.info(f"成功创建表: {self.table_name}")
            
        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"创建表失败: {e}")
            sys.exit(1)
    
    def _create_primary_key(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """创建主键"""
        id_field = next((name for name, field_type, _, _, _ in fields_info if field_type == '_id'), None)
        if id_field is None:
            logger.error(f"索引 {self.index_name} 没有 _id 字段")
            sys.exit(1)
        
        create_primary_key_sql = sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
            sql.Identifier(self.table_name),
            sql.Identifier(f"{self.table_name}_pkey"),
            sql.Identifier(id_field)
        )
        
        try:
            self.og_cursor.execute(create_primary_key_sql)
            self.og_connection.commit()
            logger.info(f"成功创建主键，主键字段: {id_field}")
        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"创建主键失败: {e}")
            sys.exit(1)
    
    def _create_temp_csv_writer(self, fieldnames: List[str]):
        """创建临时CSV writer并写入表头"""
        temp_buffer = io.StringIO()
        writer = csv.DictWriter(
            temp_buffer,
            fieldnames=fieldnames,
            restval='',
            extrasaction='ignore',
            delimiter=',',
            quotechar='"',
            escapechar='"',
            quoting=csv.QUOTE_MINIMAL,
            doublequote=False
        )

        writer.writeheader()
        return temp_buffer, writer

    def handle_array_field_value(self, original_value: str) -> str:
        """数组字段值处理函数"""
        if original_value == 'NULL' or original_value is None:
            return 'NULL'
        elif not original_value.startswith('{'):
            return '{' + original_value + '}'
        else:
            return original_value

    def _copy_batch(self, buffer, fieldnames):
        """批量执行COPY"""
        copy_sql = sql.SQL("""
            COPY {} ({}) 
            FROM STDIN 
            WITH (
                FORMAT CSV, 
                HEADER true, 
                DELIMITER ',', 
                QUOTE '"', 
                ESCAPE '"', 
                NULL 'NULL', 
                ENCODING 'UTF8'
            )
        """).format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join([sql.Identifier(name) for name in fieldnames])
        )
        
        self.og_cursor.copy_expert(copy_sql.as_string(self.og_connection), buffer)
        self.og_connection.commit()

    def _import_single_file(self, data_file: str, target_array_fields: List[str]) -> int:
        """流式处理CSV文件并COPY到数据库表"""
        if not os.path.exists(data_file):
            logger.error(f"数据文件不存在: {data_file}")
            return 0
        
        try:
            logger.info(f"开始流式导入数据: {data_file}")
            with open(data_file, 'r', encoding='utf-8', newline='') as infile:
                reader = csv.DictReader(
                    infile,
                    delimiter=',',
                    quotechar='"',
                    doublequote=True,
                    quoting=csv.QUOTE_MINIMAL,
                    restval=''
                )
                
                temp_buffer, writer = self._create_temp_csv_writer(reader.fieldnames)
                row_count = 0
                batch_size = 10000
                
                for row in reader:
                    processed_row = row.copy()
                    for field in target_array_fields:
                        processed_row[field] = self.handle_array_field_value(row.get(field))
                    
                    writer.writerow(processed_row)
                    row_count += 1

                    if row_count % batch_size == 0:
                        temp_buffer.seek(0)
                        self._copy_batch(temp_buffer, reader.fieldnames)
                        temp_buffer, writer = self._create_temp_csv_writer(reader.fieldnames)
                
                if row_count % batch_size != 0:
                    temp_buffer.seek(0)
                    self._copy_batch(temp_buffer, reader.fieldnames)
                
                logger.info(f"导入完成: {row_count} 行")
                return row_count

        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"导入数据失败 ({data_file}): {e}")
            sys.exit(1)

    def _import_data_with_copy(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """使用COPY命令导入数据"""
        if self.data_files is None:
            data_files = self._find_all_data_files()
        else:
            data_files = self.data_files
        
        if not data_files:
            logger.error(f"未找到任何数据文件 (数据文件: {self.data_file_prefix}_*.csv)")
            sys.exit(1)
        
        logger.info(f"找到 {len(data_files)} 个数据文件:")
        for f in data_files:
            logger.info(f"  {f}")
        
        target_array_fields = [info[3] for info in fields_info if info[2] is True]
        total_rows = 0
        
        for i, data_file in enumerate(data_files, 1):
            logger.info(f"导入文件 [{i}/{len(data_files)}]: {os.path.basename(data_file)}")
            rows = self._import_single_file(data_file, target_array_fields)
            total_rows += rows
        
        logger.info(f"所有数据导入完成! 共导入 {total_rows} 行到表 {self.table_name}")
    
    def export(self) -> None:
        """执行OpenSearch索引导出操作"""
        self._load_opensearch_config()
        self._load_export_config()

        logger.info(f"OpenSearch主机: {self.os_host}:{self.os_port}")
        logger.info(f"索引名称: {self.index_name}")
        
        self.array_fields.clear()
        self.vector_fields.clear()
        self._export_data_to_csv()
    
    def import_data(self) -> None:
        """执行导入数据到openGauss操作"""
        self._load_opengauss_config()
        self._init_opengauss_connection()
        
        logger.info(f"openGauss主机: {self.og_host}:{self.og_port}")
        logger.info(f"数据库: {self.og_database}")
        logger.info(f"schema: {self.og_schema}")
        logger.info(f"表名: {self.table_name}")
        
        try:
            fields_info = self._read_fields_csv()
            self._create_table(fields_info)
            self._import_data_with_copy(fields_info)
            self._create_primary_key(fields_info)
        finally:
            self._close_opengauss_connection()
    
    def migrate(self) -> None:
        """执行迁移（导出后立即导入）"""
        self._load_opensearch_config()
        self._load_export_config()
        self._load_opengauss_config()
        self._load_migrate_config()
        
        try:
            logger.info("[步骤 1/2] 开始导出数据...")
            self.export()
            logger.info("[步骤 2/2] 开始导入数据...")
            self.import_data()
            
            if not self.migrate_keep_files:
                logger.info("清理导出的CSV文件...")
                for f in self.data_files:
                    if os.path.exists(f):
                        os.remove(f)
                        logger.info(f"已删除: {f}")
                if os.path.exists(self.fields_file):
                    os.remove(self.fields_file)
                    logger.info(f"已删除: {self.fields_file}")
            logger.info("迁移完成！")
            
        finally:
            self._close_opengauss_connection()


def _create_default_config(config_file: str):
    """创建默认配置文件"""
    config = configparser.ConfigParser()
    config.optionxform = str
    
    config['opensearch'] = {
        'host': 'localhost',
        'port': '9200',
        'username': '',
        'password': '',
        'use_ssl': 'false',
        'index': 'my_index'
    }
    
    config['opengauss'] = {
        'host': 'localhost',
        'port': '5432',
        'database': 'your_database',
        'username': 'your_username',
        'password': '******',
        'schema': 'public',
        'table_name': ''
    }
    
    config['storage'] = {
        'data_dir': 'output'
    }
    
    config['export'] = {
        'scroll_size': '1000',
        'scroll_time': '5m',
        'csv_rows_per_file': '1000000'
    }
    
    config['migrate'] = {
        'keep_files': 'false'
    }
    
    with open(config_file, 'w', encoding='utf-8') as f:
        config.write(f)
    
    logger.info(f"已创建默认配置文件: {config_file}")
    logger.info("请编辑配置文件后重新运行脚本")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='OpenSearch到openGauss数据迁移工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
运行模式:
  export   - 仅导出OpenSearch数据到CSV文件
  import   - 仅导入CSV数据到openGauss
  migrate  - 一键迁移（导出后立即导入）

使用示例:
  python3 %(prog)s export 
  python3 %(prog)s export --config config.ini
  python3 %(prog)s export --config config.ini --index my_other_index
  
  python3 %(prog)s import
  python3 %(prog)s import --config config.ini
  python3 %(prog)s import --config config.ini --index my_index --table my_table
  
  python3 %(prog)s migrate
  python3 %(prog)s migrate --config config.ini
  python3 %(prog)s migrate --config config.ini --index my_index --table my_table
        """
    )
    
    parser.add_argument('mode', choices=['export', 'import', 'migrate'], help='运行模式')
    parser.add_argument('--config', default='config.ini', help='配置文件路径 (默认: config.ini)')
    parser.add_argument('--index', '-i', help='索引名称 (覆盖配置文件中的opensearch.index)')
    parser.add_argument('--table', '-t', help='目标表名 (覆盖配置文件中的opengauss.table_name)')
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        logger.warning(f"配置文件不存在: {args.config}")
        _create_default_config(args.config)
        sys.exit(0)
    
    migrator = OpenSearchToOpenGaussMigrator(
        config_file=args.config,
        cli_index=args.index,
        cli_table=args.table
    )
    
    if args.mode == 'export':
        migrator.export()
        
    elif args.mode == 'import':
        migrator.import_data()
        
    elif args.mode == 'migrate':
        migrator.migrate()


if __name__ == "__main__":
    main()
```

## 脚本使用

### 导出模式（export）

**命令示例：**

```bash
# 使用默认配置（当前目录下的 config.ini）
python3 opensearch2opengauss.py export

# 指定配置文件路径
python3 opensearch2opengauss.py export --config /path/to/config.ini

# 指定配置文件及索引名（覆盖配置文件中的索引）
python3 opensearch2opengauss.py export --config config.ini --index my_other_index
```

**参数说明：**

| 参数       | 说明                                                  |
| :--------- | :---------------------------------------------------- |
| `export`   | 指定运行模式为导出                                    |
| `--config` | 配置文件路径，不指定时默认为当前目录下的 `config.ini` |
| `--index`  | 指定要导出的索引名，不指定时使用配置文件中的索引名    |

**输出文件：**

导出成功后，在 `storage.data_dir` 指定目录下生成以下文件（以默认 `output/` 为例）：

```tex
output/
├── my_index_1.csv      # 文档数据分片文件 1
├── my_index_2.csv      # 文档数据分片文件 2
├── my_index_3.csv      # 文档数据分片文件 3
├── ...
└── my_index_fields.csv # 索引结构（字段描述）文件
```

### 导入模式（import）

**前置条件：**

请确保导出模式生成的文件已放置在 `storage.data_dir` 指定目录下，且不支持修改原始文件名及文件内容。

**命令示例：**

```bash
# 使用默认配置导入（当前目录下的 config.ini）
python3 opensearch2opengauss.py import

# 指定配置文件
python3 opensearch2opengauss.py import --config config.ini

# 指定配置文件、索引名及目标表名
python3 opensearch2opengauss.py import --config config.ini --index my_index --table my_table
```

**参数说明：**

| 参数       | 说明                                                  |
| :--------- | :---------------------------------------------------- |
| `import`   | 指定运行模式为导入                                    |
| `--config` | 配置文件路径，不指定时默认为当前目录下的 `config.ini` |
| `--index`  | 指定要导入的索引名，不指定时使用配置文件中的索引名    |
| `--table`  | 指定导入后的表名，优先级：命令行 > 配置文件 > 索引名  |

### 迁移模式（migrate）

**命令示例：**

```bash
# 使用默认配置迁移（当前目录下的 config.ini）
python3 opensearch2opengauss.py migrate

# 指定配置文件
python3 opensearch2opengauss.py migrate --config config.ini

# 指定配置文件、索引名及目标表名
python3 opensearch2opengauss.py migrate --config config.ini --index my_index --table my_table
```

**参数说明：**

| 参数       | 说明                                                  |
| :--------- | :---------------------------------------------------- |
| `migrate`  | 指定运行模式为迁移                                    |
| `--config` | 配置文件路径，不指定时默认为当前目录下的 `config.ini` |
| `--index`  | 指定要迁移的索引名，不指定时使用配置文件中的索引名    |
| `--table`  | 指定迁移后的表名，优先级：命令行 > 配置文件 > 索引名  |

### 生成默认配置文件

当配置文件不存在时，运行任意模式命令，脚本会自动生成默认配置文件。

- 命令中指定 `--config` 参数：在指定路径生成配置文件
- 命令中未指定 `--config` 参数：在当前目录下生成 `config.ini`

## 迁移逻辑说明

### 索引映射到表结构

工具查询 OpenSearch 索引映射，将各字段映射为 openGauss 表字段，并额外保留 OpenSearch 元数据字段 `_id` 作为表主键。

**索引映射示例：**

```json
{
  "vector_demo": {
    "mappings": {
      "properties": {
        "category": { "type": "keyword" },
        "description": { "type": "text" },
        "embedding": { "type": "knn_vector", "dimension": 5 },
        "id": { "type": "keyword" },
        "name": { "type": "text" },
        "price": { "type": "float" }
      }
    }
  }
}
```

**迁移后表结构：**

```tex
   Column    |   Type    | Modifiers | Storage  | Description 
-------------+-----------+-----------+----------+-------------
 _id         | text      | not null  | extended | 
 category    | text      |           | extended | 
 description | text      |           | extended | 
 embedding   | vector(5) |           | external | 
 id          | text      |           | extended | 
 name        | text      |           | extended | 
 price       | real      |           | plain    | 

Indexes:
    "vector_demo_pkey" PRIMARY KEY, btree (_id)

Has OIDs: no
Options: orientation=row, compression=no
```

### 数据类型映射规则

工具内置 `TYPE_MAPPING` 映射表，将 OpenSearch 字段类型转换为对应的 openGauss 数据类型，实际以代码中`TYPE_MAPPING`为准，对于 `TYPE_MAPPING` 映射表中未映射的源端字段类型，统一映射为 openGauss TEXT类型。部分映射规则如下：

| OpenSearch 类型       | openGauss 类型 |
| :-------------------- |:-------------|
| `keyword`, `text`     | `TEXT`       |
| `byte`, `short`       | `SMALLINT`   |
| `integer`             | `INTEGER`    |
| `long`                | `BIGINT`     |
| `float`, `half_float` | `REAL`       |
| `double`              | `FLOAT8`     |
| `boolean`             | `BOOLEAN`    |
| `date`                | `TIMESTAMP`  |
| `binary`              | `BYTEA`      |
| `ip`                  | `INET`       |
| `geo_point`           | `TEXT`       |
| `knn_vector`          | `VECTOR(n)`  |
| `_id`                 | `TEXT`       |
| ...                   | ...          |

### 数组数据迁移规则

OpenSearch中字段可能包含多个值（Array），迁移时工具自动检测字段值是否为数组（Python `list` 类型），并按下述规则处理：

| 数组元素类型 | 迁移后类型                | 说明                                                   |
|:-------| :------------------------ | :----------------------------------------------------- |
| 数值相关类型 | `INTEGER[]` / `REAL[]` 等 | 转换为 openGauss 原生数组类型                          |
| 文本相关类型 | `TEXT`                    | 存储为TEXT类型，内容为数组格式的文本，便于建立全文索引 |
| 向量类型   | `VECTOR`                  | 按向量类型处理                                         |

### 对象类型迁移规则

#### nested / object 扁平化

OpenSearch 中的嵌套对象（如 `user.address.city`）在迁移时会被**扁平化处理**，嵌套层级用下划线（`_`）连接：

```text
原字段：user.address.city
迁移后：user_address_city
```

#### join 类型拆分

脚本识别 `join` 类型后，自动拆分为两个独立字段，以保留父子文档间的关联关系。

| 拆分后字段            | 类型 | 说明      |
| :-------------------- | :--- | :-------- |
| `{join_field}.name`   | TEXT | 关系名称  |
| `{join_field}.parent` | TEXT | 父文档_id |

### 关键字冲突处理

由于 OpenSearch 与 openGauss 关键字不同，为避免使用索引信息构造的建表语句中的表名和字段名与 openGauss 关键字冲突，出现 SQL 语法错误，导致创建表结构失败。工具内置了 OPENGAUSS_KEYWORDS 集合，在表名与字段名处理时，如果遇到表名和字段名命中该集合中关键字的情况，迁移工具自动在原始名称后添加下划线（`_`）作为后缀。例如，`user` 将转换为 `user_`。

## 迁移规则限制

### 导出 CSV 文件不支持修改

本迁移工具导出的 CSV 文件不支持任何修改操作，包括重命名文件、修改文件内容或在导入前删除文件。

迁移工具在导入过程中会解析 CSV 文件，以获取目标表结构和表数据。若文件被修改或删除，可能导致以下问题：

- 导入失败
- 导入后的表结构异常
- 导入后的表数据丢失

因此，请保持导出的 CSV 文件原样使用，勿做任何更改。

### 表名长度限制

由于 openGauss 规定对象名长度不得超过 63 个字符，本迁移工具对迁移目标端的表名作出如下限制：**表名长度不得超过 58 个字符。**

原因在于：迁移过程中，工具会根据表名自动生成对应的主键名称，生成规则为 `表名_pkey`。若表名长度超过 58 个字符，则拼接后的主键名将超过 openGauss 允许的 63 个字符上限，从而导致迁移异常。

### 字段名长度限制

由于 openGauss 规定对象名长度不得超过 63 个字符，本迁移工具对OpenSearch索引中的字段名做出如下限制：

- 字段名长度不得超过 63 个字符
- 嵌套类型字段，按照扁平化规则处理后，字段名长度不得超过 63 个字符
- join 类型字段拆分后，字段名长度不得超过 63 个字符

如果字段名长度超过限制，openGauss在建表时会自动截断字段名，仅保留前 63 个字符。请在迁移前检查字段名长度，避免截断导致的迁移异常。

如果超长字段名迁移成功，建议在迁移完成后，修改表字段名，以避免截断导致的查询异常，修改语句参考如下：

```sql
ALTER TABLE table_name RENAME COLUMN old_field_name TO new_field_name;
```
