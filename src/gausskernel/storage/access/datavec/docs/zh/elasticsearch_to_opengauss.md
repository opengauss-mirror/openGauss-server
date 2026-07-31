# 从Elasticsearch迁移至openGauss

本教程通过Python编程语言，支持将Elasticsearch数据迁移至openGauss实例中

## 环境准备

- 已部署7.3 及以上版本的Elasticsearch实例
- 已部署7.0.0-RC1 及以上版本的openGauss实例
- 已安装3.8 及以上版本的Python环境
- 已安装涉及的Python库

```python
pip3 install psycopg2
pip3 install requests
```

## 迁移操作

1. 参考如下迁移脚本elasticsearch2opengauss.py及配置文件config.ini，根据本地部署的Elasticsearch与openGauss进行配置修改：

    迁移配置文件config.ini如下：
    
    ```ini
    [Elasticsearch]
    host = http://127.0.0.1:9200
    # Elasticsearch username and password, if not set, use anonymous access. Not supported yet.
    #username = user_name
    #password = xxxxxx
    
    [openGauss]
    ip = 127.0.0.1
    port = 5432
    database = your_opengauss_db
    username = user_name
    password = xxxxxx
    
    [Table]
    elasticsearch_index_name = your_es_index
    # Default the same as the index name. Not support custom table name yet.
    #opengauss_table_name = your_opengauss_table
    
    [Output]
    folder = output
    
    [Migration]
    # Batch query number of rows. Default value is 10000.
    batch_query_size = 10000
    cleanup_temp_files = true
    ```
    
    elasticsearch2opengauss.py迁移脚本如下：

    ```python
    import psycopg2
    import csv
    import json
    import re
    import requests
    import configparser
    import os
    import sys
    from typing import List, Dict, Any, Tuple, Optional, Union
    from datetime import datetime
    
    # Elasticsearch 配置信息
    es_url = None # Elasticsearch 服务器地址
    es_index = None  # Elasticsearch 索引名
    
    # openGauss 配置信息
    db_host = None
    db_port = None
    db_name = None
    # 需要使用md5加密用户
    db_user = None
    db_password = None
    db_table = None
    
    csv_file_path = None
    cleanup_temp_files = True
    batch_query_size = 10000
    
    RESERVED_KEYWORDS = {
        "select", "insert", "update", "delete", "drop", "table", "from", "where", "group",
        "by", "having", "order", "limit", "join", "inner", "left", "right", "full", "union",
        "all", "distinct", "as", "on", "and", "or", "not", "null", "true", "false", "case",
        "when", "then", "else", "end", "exists", "like", "in", "between", "is", "like",
        "references", "foreign", "primary", "key", "unique", "check", "default", "constraint",
        "index", "unique", "varchar", "text", "int", "bigint", "smallint", "boolean", "timestamp"
    }
    
    def is_valid_linux_path(path) -> bool:
        """Is valid Linux path"""
        if not path or not isinstance(path, str):
            return False
    
        path_str = str(path)
    
        if not path_str.strip():
            return False
    
        if '\x00' in path_str:
            return False
    
        if len(path_str) > 4096:
            return False
    
        if '\x00' in path_str:
            return False
    
        components = path_str.split('/')
        for component in components:
            if not component:
                continue
            if '/' in component or '\x00' in component:
                return False
            if component in ('.', '..'):
                continue
            if len(component) > 255:
                return False
        return True
    
    def read_config(config_file='config.ini'):
        """Read config file"""
        global es_url, es_index, db_host, db_port, db_name, db_user, db_password, db_table, csv_file_path, cleanup_temp_files, batch_query_size
        config = configparser.ConfigParser()
    
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} not found")
    
        config.read(config_file, encoding='utf-8')
    
        es_url = config.get('Elasticsearch', 'host')
        db_host = config.get('openGauss', 'ip')
        db_port = config.getint('openGauss', 'port')
        db_name = config.get('openGauss', 'database')
        db_user = config.get('openGauss', 'username')
        db_password = config.get('openGauss', 'password')
    
        if len(sys.argv) == 3:
            if not sys.argv[1].strip() or not sys.argv[2].strip():
                print("Error: Both Elasticsearch index name and openGauss table name must be provided")
                sys.exit(1)
            es_index = sys.argv[1].strip()
            db_table = sys.argv[2].strip()
        else:
            es_index = config.get('Table', 'elasticsearch_index_name')
            try:
                db_table = config.get('Table', 'opengauss_table_name')
            except configparser.NoOptionError as e:
                db_table = es_index
    
        batch_query_size = config.getint('Migration', 'batch_query_size', fallback=batch_query_size)
        cleanup_temp_files = config.getboolean('Migration', 'cleanup_temp_files', fallback=True)
        output_folder = config.get("Output", "folder", fallback='output')
        os.makedirs(output_folder, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_path = os.path.join(output_folder, f"{es_index}_{timestamp}.csv")
    
        if not is_valid_linux_path(es_index):
            print("Error: Output.folder path is invalid")
            sys.exit(1)
    
        is_enable_stdin_password = os.getenv("enable.env.password", "").lower()
        if is_enable_stdin_password == "true":
            opengauss_password = os.getenv("openGauss.password", "").strip()
            if not opengauss_password.strip():
                print("Error: openGauss password must be provided")
                sys.exit(1)
            db_password = opengauss_password
    
    def fetch_data_from_es():
        """Fetch doc model from Elasticsearch"""
        query = {
            "query": {
                "match_all": {}
            },
            "_source": True
        }
        response = requests.get(f'{es_url}/{es_index}/_search', json=query)
    
        if response.json().get('error'):
            print(f"Fetch index data failed: {response.json()}")
    
        if response.status_code == 200:
            return response.json()['hits']['hits']
        else:
            raise Exception(f"Failed to fetch data from Elasticsearch: {response.status_code}, {response.text}")
    
    def fetch_mapping():
        """Fetch index mapping from Elasticsearch"""
        response = requests.get(f'{es_url}/{es_index}/_mapping')
    
        if response.json().get('error'):
            print(f"Fetch index mapping failed: {response.json()}")
    
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch mapping: {response.status_code}, {response.text}")
    
    def convert_dict_to_jsonb(value):
        """Convert dict to jsonb"""
        if isinstance(value, dict):
            return json.dumps({k: convert_dict_to_jsonb(v) for k, v in value.items()},
                              ensure_ascii=False)
        elif isinstance(value, list):
            return json.dumps([convert_dict_to_jsonb(v) for v in value],
                              ensure_ascii=False)
        else:
            return value
    
    def map_to_opengauss_type(es_type, dim: Optional[int] = None) -> str:
        """Map Elasticsearch types to openGauss types"""
        if isinstance(es_type, (dict, list)):  # 如果 es_type 是字典类型，则需要特殊处理
            return 'JSONB'
        type_map = {
            "long": "BIGINT",
            "integer": "INTEGER",
            "short": "SMALLINT",
            "byte": "SMALLINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "keyword": "VARCHAR",
            "text": "TEXT",
            "date": "TIMESTAMP",
            "binary": "BYTEA",
            "geo_point": "POINT",
            "geo_shape": "GEOMETRY",
            "nested": "JSONB",
            "object": "JSONB",
            "ip": "INET",
            "scaled_float": "REAL",
            "float_vector": f"VECTOR({dim})" if dim else "VECTOR",
            "dense_vector": f"VECTOR({dim})" if dim else "VECTOR",
            "binary_vector": f"BIT({dim})" if dim else "BIT",
            "half_float": "REAL",
            "unsigned_long": "BIGINT",
            "date_nanos": "TIMESTAMP",
            "alias": "TEXT",
        }
    
        if es_type in type_map:
            print(f"es_type:{es_type} ----- og_type: {type_map[es_type]}")
            return type_map[es_type]
        else:
            print(f"Warning: Unsupported Elasticsearch type '{es_type}', defaulting to 'TEXT'")
            return 'TEXT'
    
    def sanitize_name(field_name) -> str:
        """Standardize field name and avoid conflicts with reserved keywords"""
        sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', field_name)
    
        if sanitized_name.lower() in RESERVED_KEYWORDS:
            return f'"{sanitized_name}"'
    
        return sanitized_name
    
    def get_table_fields() -> List[Tuple[str, str]]:
        """Get openGauss table fields"""
        mapping_data = fetch_mapping()
        if not mapping_data:
            print(f"WARN: Cannot found mapping data for '{es_index}'")
            return []
    
        mapping_properties = mapping_data.get(es_index, {}).get('mappings', {}).get('properties', {})
        if not mapping_properties:
            print(f"WARN: Cannot found mapping data for '{es_index}'")
            return []
    
        field_dict = {}
        seen_fields = set()
        for field_name, field_info in mapping_properties.items():
            if field_name in seen_fields:
                continue
    
            es_type = 'text'
            dim = 0
            if isinstance(field_info, dict):
                es_type = field_info.get('type', 'text')
                dim = field_info.get('dims', 0)
            field_type = map_to_opengauss_type(es_type, dim)
            sanitized_name = sanitize_name(field_name)
            field_dict[sanitized_name] = field_type
            seen_fields.add(field_name)
    
        doc_id = "doc_id"
        while doc_id in field_dict:
            doc_id += "_"
    
        table_fields = [(doc_id, "VARCHAR PRIMARY KEY")]
        es_data = fetch_data_from_es()
        if es_data:
            source_fields = es_data[0].get('_source', {}).keys()
            doc_fields = [sanitize_name(col) for col in source_fields]
            for doc_field in doc_fields:
                if doc_field in field_dict:
                    field_type = field_dict[doc_field]
                    table_fields.append((doc_field, field_type))
        else:
            for doc_field in field_dict:
                table_fields.append((doc_field, field_dict[doc_field]))
    
        print(f"Index fields: {[name for name, _ in table_fields]}")
        return table_fields
    
    def create_table_in_opengauss(index_fields) -> None:
        """Create table in openGauss"""
        columns_definition = []
        for field_name, field_type in index_fields:
            columns_definition.append(f"{field_name} {field_type}")
    
        columns_str = ", ".join(columns_definition)
        create_table_sql = (f"DROP TABLE IF EXISTS {sanitize_name(db_table)}; "
                            f"CREATE TABLE {sanitize_name(db_table)} ({columns_str});")
        try:
            # 建立数据库连接并执行创建表 SQL
            connection = psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=db_name,
                user=db_user,
                password=db_password
            )
            cursor = connection.cursor()
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"Table {sanitize_name(db_table)} created successfully.")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def generate_chunk_filename(chunk_id) -> str:
        """Generate CSV chunk filename"""
        base_name, _ = os.path.splitext(csv_file_path)
        return f"{base_name}_part{chunk_id}.csv"
    
    def write_to_csv_file(chunk_file, fields, values, chunk_size: int = 1000) -> None:
        """Write chunk to csv file"""
        if not values:
            return
    
        try:
            with open(chunk_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(fields)
    
                total_rows = len(values)
                for start_idx in range(0, total_rows, chunk_size):
                    end_idx = min(start_idx + chunk_size, total_rows)
                    writer.writerows(values[start_idx:end_idx])
    
        except (IOError, csv.Error) as e:
            print(f"Failed to write CSV file {chunk_file}: {e}")
            raise
    
    def build_search_query(batch_size, search_after) -> Dict[str, Any]:
        """Build Elasticsearch search query body"""
        query = {
            "query": {
                "match_all": {}
            },
            "_source": True,
            "size": batch_size,
            "sort": [
                {"_id": "asc"}
            ]
        }
    
        if search_after:
            query["search_after"] = search_after
    
        return query
    
    def execute_es_query(query) -> List[Dict]:
        """Execute Elasticsearch query"""
        try:
            url = f'{es_url.rstrip("/")}/{es_index}/_search'
            response = requests.post(
                url,
                json=query,
                headers={'Content-Type': 'application/json'},
                timeout=60
            )
    
            data = response.json()
            if data.get('error'):
                print(f"Query index failed: {data}")
    
            response.raise_for_status()
            return data.get('hits', {}).get('hits', [])
        except requests.exceptions.Timeout as e:
            print(f"Query timed out: {e}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"RequestException: {e}")
            raise
        except (KeyError, ValueError) as e:
            print(f"Failed to parse response from Elasticsearch: {e}")
            raise
    
    def parse_elasticsearch_hits(hits) -> List[List[Any]]:
        """Parse Elasticsearch hits to table rows"""
        if not hits:
            return []
    
        rows = []
        for hit in hits:
            es_id = hit.get('_id', '')
            source_data = hit.get('_source', {})
    
            row_values = [es_id]
            for column, value in source_data.items():
                if isinstance(value, (dict, list)):
                    value = convert_dict_to_jsonb(value)
                row_values.append(value)
    
            rows.append(row_values)
    
        return rows
    
    def import_to_opengauss(file_paths, fields) -> None:
        """Import CSV data to openGauss"""
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            client_encoding='utf8'
        )
        cursor = conn.cursor()
    
        try:
            cursor.execute(f"TRUNCATE TABLE {db_table};")
            conn.commit()
    
            total_rows = 0
            for i, csv_file in enumerate(file_paths, 1):
                with open(csv_file, 'rb') as f:
                    copy_sql = f"""
                        COPY {db_table} ({', '.join(fields)})
                        FROM STDIN WITH (FORMAT CSV, HEADER, NULL 'NULL', ENCODING 'UTF8');
                        """
                    cursor.copy_expert(copy_sql, f)
                    conn.commit()
    
                    rows_imported = cursor.rowcount
                    total_rows += rows_imported
                    print(f"Imported {rows_imported} rows from {csv_file}")
    
            print(f"Total imported: {total_rows} rows")
        except Exception as e:
            conn.rollback()
            print(f"Import failed: {e}")
            raise
        finally:
            cursor.close()
    
    def migration(index_fields):
        """Migration Elasticsearch index docs to openGauss table"""
        file_paths = []
        fields = [key for key, _ in index_fields]
        search_after = None
        batch_size = batch_query_size
        total_records = 0
    
        print("Start to export data...")
        while True:
            query = build_search_query(batch_size, search_after)
            hits = execute_es_query(query)
            if not hits:
                break
    
            rows = parse_elasticsearch_hits(hits)
            batch_len = len(rows)
            total_records += batch_len
    
            chunk_id = len(file_paths) + 1
            chunk_file = generate_chunk_filename(chunk_id)
            file_paths.append(chunk_file)
    
            write_to_csv_file(chunk_file, fields, rows)
            print(f"Created chunk {chunk_id}, chunk size: {batch_len} rows")
    
            last_hit = hits[-1]
            search_after = last_hit['sort'] if 'sort' in last_hit else [last_hit['_id']]
    
    
            if len(hits) < batch_size:
                break
    
        print("Finish exporting data, total records: ", total_records)
    
        import_to_opengauss(file_paths, fields)
    
        if cleanup_temp_files:
            for f in file_paths:
                try:
                    os.remove(f)
                except Exception as e:
                    print(f"Failed to delete {f}: {e}")
    
        print(f"Migration completed.")
    
    def main():
        try:
            read_config()
            index_fields = get_table_fields()
            create_table_in_opengauss(index_fields)
            migration(index_fields)
            print(f"Successfully inserted data into table {db_table}.")
        except Exception as e:
            print(f"Migration failed: {e}")
    
    if __name__ == "__main__":
        main()
        print("Migration end.")
    ```

2. 将迁移脚本、配置文件放置在同一目录下，层级如下：

    ```
    ├── elasticsearch2opengauss.py
    └── config.ini
    ```

3. 运行迁移脚本，并查看输出结果：

    ```
    python3 elasticsearch2opengauss.py
    ```

4. 登录openGauss，查看数据是否完成迁移：

   4.1 登录`omm`超级用户：

    ```
    $ su - omm
    $ gsql -d postgres -p 5432
    ```

   4.2 查看迁移表数据量：

    ```
    $ \c your_opengauss_db
    $ \d  #查看迁移后的表名table_name
    $ select count(*) from table_name;
    ```

# 常见问题处理

## 1 Fielddata access on the _id field is disallowed

### 问题现象

迁移报错日志中提示查询数据失败，并且日志中包含如下JSON信息，JSON信息提示`Fielddata access on the _id field is disallowed, you can re-enable it by updating the dynamic cluster setting: indices.id_field_data.enabled`。

```json
{
  "error" : {
    "root_cause" : [
      {
        "type" : "illegal_argument_exception",
        "reason" : "Fielddata access on the _id field is disallowed, you can re-enable it by updating the dynamic cluster setting: indices.id_field_data.enabled"
      }
    ],
    "type" : "search_phase_execution_exception",
    "reason" : "all shards failed",
    "phase" : "query",
    "grouped" : true,
    "failed_shards" : [
      {
        "shard" : 0,
        "index" : "users",
        "node" : "yKLv--_aTEuDYXmCK-oHXg",
        "reason" : {
          "type" : "illegal_argument_exception",
          "reason" : "Fielddata access on the _id field is disallowed, you can re-enable it by updating the dynamic cluster setting: indices.id_field_data.enabled"
        }
      }
    ],
    "caused_by" : {
      "type" : "illegal_argument_exception",
      "reason" : "Fielddata access on the _id field is disallowed, you can re-enable it by updating the dynamic cluster setting: indices.id_field_data.enabled",
      "caused_by" : {
        "type" : "illegal_argument_exception",
        "reason" : "Fielddata access on the _id field is disallowed, you can re-enable it by updating the dynamic cluster setting: indices.id_field_data.enabled"
      }
    }
  },
  "status" : 400
}
```

### 问题原因

 Elasticsearch 高版本中，默认禁用了 `_id` 字段的 fielddata，`_id` 字段是一个特殊的内部字段，启用 fielddata 会占用大量内存。在迁移业务中，批量查询`index`中的数据时，需要根据`_id`进行排序，避免数据重复。

### 问题处理

控制`_id`字段的 fielddata 的参数为`indices.id_field_data.enabled`，默认值为`false`。迁移时，临时开启此参数值为`true`，再次进行迁移，此报错问题解决。修改方式如下：

```bash
# 临时启用（重启后失效）
PUT /_cluster/settings
{
  "transient": {
    "indices.id_field_data.enabled": true
  }
}

# 或永久启用
PUT /_cluster/settings
{
  "persistent": {
    "indices.id_field_data.enabled": true
  }
}
```
