# Migrating from OpenSearch to openGauss

This document describes how to use the OpenSearch-to-openGauss data migration tool and explains the key migration rules, helping you migrate data efficiently and accurately.

## Preparing the Environment

### Version Requirements

| Component  | Version Requirement                             |
| :--------- | :---------------------------------------------- |
| OpenSearch | 1.3.4 or later                                  |
| openGauss  | 7.0.0-RC1 or later with vector database support |
| Python     | 3.7 or later                                    |

### Installing Python Dependencies

```bash
# openGauss database driver
pip3 install psycopg2

# Official OpenSearch Python client
pip3 install opensearch-py
```

## Tool Overview

### Feature Overview

This migration tool supports the complete migration of indexes from OpenSearch to an openGauss database:

- Fields in the index Mapping → openGauss table structure
- Documents stored in the index → openGauss table data

### Running Modes

The tool provides three running modes to meet the requirements of different migration stages:

| Mode | Command | Description |
| :--- | :------ | :---------- |
| Export | `export` | Exports the Mapping of an OpenSearch index as a field description CSV file and exports document data as a data CSV file. |
| Import | `import` | Reads the field description CSV file to create a table in openGauss and efficiently imports the data CSV file using the `COPY` command. |
| Migrate | `migrate` | Completes the entire "export + import" process in one step. |

## Migration Tool

### Configuration File

Create a `config.ini` file and configure it based on the following template:

```ini
[opensearch]
# OpenSearch connection settings (effective in export and migrate modes)
host = localhost
port = 9200
username = 
password = 
use_ssl = false
# Index name (shared by import, export, and migrate modes)
index = my_index

[opengauss]
# openGauss database connection settings (effective in import and migrate modes)
host = localhost
port = 5432
database = your_database
# Use an MD5-encrypted user
username = your_username
password = ****
# Target database schema. The default is public. If another schema is specified, ensure that the schema exists.
schema = public
# Target table name (optional). If not specified, the index name is used as the table name.
table_name = 

[storage]
# CSV file storage directory (effective in import, export, and migrate modes)
data_dir = output

[export]
# Export settings (effective in export and migrate modes)
scroll_size = 1000
scroll_time = 5m
# Number of data rows per CSV file (excluding the header). A value of 0 or a negative value means that data is not split into multiple files.
csv_rows_per_file = 1000000

[migrate]
# Migration mode settings (effective in migrate mode)
# Whether to retain the exported CSV files (true/false)
keep_files = false
```

### Migration Script

Create the `opensearch2opengauss.py` file and fill in the following code:

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
    """OpenSearch to openGauss migration tool"""
    
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
        Initialize the migrator.
        
        Args:
            config_file: Path to the configuration file.
            cli_index: Index name specified on the command line.
            cli_table: Table name specified on the command line.
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
        """Load the configuration file."""
        config = configparser.ConfigParser()
        config.optionxform = str
        
        if not os.path.exists(self.config_file):
            logger.error(f"Configuration file does not exist: {self.config_file}")
            sys.exit(1)
        
        try:
            config.read(self.config_file, encoding='utf-8')
            logger.info(f"Configuration file loaded successfully: {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to read configuration file: {e}")
            sys.exit(1)

        if 'opensearch' not in config:
            logger.error("The [opensearch] section is missing from the configuration file")
            sys.exit(1)

        if 'storage' not in config:
            logger.error("The [storage] section is missing from the configuration file")
            sys.exit(1)
        
        if self.cli_index:
            self.index_name = self.cli_index
        else:
            self.index_name = config.get('opensearch', 'index')
            if not self.index_name:
                logger.error("The index parameter must be specified in the [opensearch] section")
                sys.exit(1)

        self.data_dir = config.get('storage', 'data_dir', fallback='output')
        self.fields_file = os.path.join(self.data_dir, f"{self.index_name}_fields.csv")
        self.data_file_prefix = os.path.join(self.data_dir, self.index_name)
        
        return config
    
    def _load_opensearch_config(self):
        """Load the OpenSearch configuration."""
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
        """Load the openGauss configuration."""
        if 'opengauss' not in self.config:
            logger.error("The [opengauss] section is missing from the configuration file")
            sys.exit(1)

        self.og_host = self.config.get('opengauss', 'host', fallback='localhost')
        self.og_port = self.config.getint('opengauss', 'port', fallback=5432)
        self.og_database = self.config.get('opengauss', 'database')
        self.og_username = self.config.get('opengauss', 'username')
        self.og_password = self.config.get('opengauss', 'password')
        self.og_schema = self.config.get('opengauss', 'schema', fallback='public')

        if not self.og_database:
            logger.error("The database parameter must be specified in the [opengauss] section")
            sys.exit(1)
        if not self.og_username:
            logger.error("The username parameter must be specified in the [opengauss] section")
            sys.exit(1)
        if not self.og_password:
            logger.error("The password parameter must be specified in the [opengauss] section")
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
            logger.error(f"Target table name '{self.table_name}' exceeds {self.MAX_TABLE_NAME_LENGTH} characters. Migration cannot succeed. Please modify the table name.")
            sys.exit(1)
    
    def _load_export_config(self):
        """Load the export configuration."""
        self.scroll_size = 1000
        self.scroll_time = '5m'
        self.csv_rows_per_file = 0
        if 'export' in self.config:
            self.scroll_size = self.config.getint('export', 'scroll_size', fallback=1000)
            self.scroll_time = self.config.get('export', 'scroll_time', fallback='5m')
            self.csv_rows_per_file = self.config.getint('export', 'csv_rows_per_file', fallback=0)
        
        self.use_sharding = self.csv_rows_per_file > 0
    
    def _load_migrate_config(self):
        """Load the migration configuration."""
        self.migrate_keep_files = False
        if 'migrate' in self.config:
            self.migrate_keep_files = self.config.getboolean('migrate', 'keep_files', fallback=False)
    
    def _get_data_file_path(self, part_num: Optional[int] = None) -> Tuple[str, int]:
        """Obtain the data file path."""
        if self.use_sharding:
            return f"{self.data_file_prefix}_{part_num}.csv", part_num + 1
        else:
            return f"{self.data_file_prefix}.csv", 1
    
    def _extract_part_num(self, filepath: str):
        """Extract the shard number from the file path."""
        match = re.search(r'_(\d+)\.csv$', filepath)
        return int(match.group(1)) if match else None

    def _find_all_data_files(self) -> List[str]:
        """Find all data files (sharding supported)."""
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
                    logger.warning(f"Shard file number does not match expectation. Expected {last_part_num}, but {len(part_files)} shard files exist.")
            
                if not files:
                    files = part_files
                else:
                    logger.warning(f"Both main file and shard files exist. The main file will be used: {main_file}")
        
        return files

    def _clear_index_csv_files(self):
        """Clear all CSV files."""
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
            logger.info(f"Deleted historical file: {file}")
    
    def _init_opensearch_client(self):
        """Initialize the OpenSearch client."""
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
        
        logger.info("OpenSearch client initialized.")
    
    def _check_opensearch_connection(self):
        """Check whether OpenSearch is connectable."""
        try:
            if not self.os_client.ping():
                logger.error("Unable to connect to the OpenSearch service.")
                sys.exit(1)
            logger.info("Connected to the OpenSearch service successfully.")
        except Exception as e:
            logger.error(f"OpenSearch test connection failed: {e}")
            sys.exit(1)

    def _check_index_exists(self):
        """Check whether the index exists."""
        try:
            if not self.os_client.indices.exists(index=self.index_name):
                logger.error(f"Index '{self.index_name}' does not exist.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to check whether the index exists: {e}")
            sys.exit(1)

    def _get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """Obtain the mapping structure of the index."""
        try:
            mapping = self.os_client.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']
        except Exception as e:
            logger.error(f"Failed to obtain index mapping: {e}")
            sys.exit(1)
    
    def _get_vector_dimension(self, field_config: Dict[str, Any]) -> int:
        """Obtain the dimension of the vector field."""
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
        """Determine whether the field is of vector type."""
        return field_type == 'knn_vector'
    
    def _parse_field(self, field_name: str, field_config: Dict[str, Any], 
                    parent_path: str = '') -> List[Tuple[str, str]]:
        """Recursively parse the field structure."""
        fields = []
        current_path = f"{parent_path}.{field_name}" if parent_path else field_name
        field_type = field_config.get('type', 'object')
        
        if self._is_vector_field(field_type):
            dimension = self._get_vector_dimension(field_config)
            if dimension > 0:
                self.vector_fields[current_path] = dimension
            else:
                logger.error(f"Field {current_path} is of type {field_type}, but dimension information cannot be obtained")
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
        """Parse the structure of all fields in the index."""
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
        """Process field names and normalize them."""
        target_fieldname = field_path.replace('.', '_')
        if target_fieldname in self.OPENGAUSS_KEYWORDS:
            target_fieldname = f"{target_fieldname}_"
        return target_fieldname
    
    def _get_field_mapping(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Obtain the index field mapping and target field name mapping."""
        try:
            fields_map = self._parse_index_fields(self.index_name)
            fieldnames = list(fields_map.keys())
            target_fieldnames = {
                field_path: self._process_field_name(field_path) 
                for field_path in fieldnames
            }
            logger.info(f"Index mapping obtained successfully, parsed {len(fieldnames)} fields from the mapping")
            return fields_map, target_fieldnames
        except Exception as e:
            logger.error(f"Failed to parse index fields: {e}")
            sys.exit(1)

    def _get_index_document_count(self, query: Dict[str, Any]) -> int:
        """Obtain the total number of documents in the index."""
        try:
            count_response = self.os_client.count(index=self.index_name, body=query)
            total_docs = count_response['count']
            logger.info(f"Found {total_docs} documents in index {self.index_name}")
            return total_docs
        except Exception as e:
            logger.error(f"Failed to obtain the total number of documents in index {self.index_name}: {e}")
            return None

    def _parse_nested_value(self, doc: Dict[str, Any], field_path: str) -> Any:
        """Parse the values of nested-type fields in the document."""
        parts = field_path.split('.')
        return self._parse_nested_value_recursive(doc, parts, 0, field_path)
    
    def _parse_nested_value_recursive(self, current: Any, parts: List[str], 
                                       index: int, full_path: str) -> Any:
        """Recursively parse nested values."""
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
        """Merge array values."""
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
        """Obtain values from the document based on the field path."""
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
        """Ensure that the output directory exists."""
        try:
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
                logger.info(f"Creating output directory: {self.data_dir}")
        except Exception as e:
            logger.error(f"Failed to detect/create output directory: {e}")
            sys.exit(1)

    def _export_fields_csv(self, target_fieldnames: Dict[str, str], fields_map: Dict[str, str]):
        """Export the index field structure to a CSV file."""
        output_file = self.fields_file
        logger.info(f"Exporting field structure to: {output_file}")
        
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
                        logger.warning(f"Field {field_path} is of type {field_type}, no openGauss mapping type exists, defaulting to text type")
                        target_type = 'text'

                    if field_path in self.vector_fields:
                        row['field_type'] = f"{field_type}({self.vector_fields[field_path]})"
                        row['target_fieldtype'] = f"{target_type}({self.vector_fields[field_path]})"
                    elif row['is_array'] == 'True' and target_type != 'text':
                        row['target_fieldtype'] = f"{target_type}[]"
                    else:
                        row['target_fieldtype'] = target_type
                    
                    writer.writerow(row)
            
            logger.info("Field structure export completed.")
            
        except Exception as e:
            logger.error(f"Failed to export field structure: {e}")
            sys.exit(1)
    
    def _create_csv_writer(self, filepath: str, fieldnames: List[str], 
                           target_fieldnames: Dict[str, str]):
        """Create a CSV writer and write the header."""
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
        """Close the CSV writer."""
        if csvfile_writer:
            csvfile_writer.close()
            logger.info(f"CSV file write completed: {data_files[-1]} ({current_file_rows} rows)")

    def _convert_doc_to_csv_row(self, hit, fields_map):
        """Convert a document to CSV row data."""
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
        """Export index data to a CSV file (with sharding support)."""
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
                        logger.info(f"Created new file: {filepath}")
                    
                    row = self._convert_doc_to_csv_row(hit, fields_map)
                    current_writer.writerow(row)
                    current_file_rows += 1
                    total_processed += 1
                
                logger.info(f"Processed: {total_processed}/{total_docs if total_docs else 'unknown'}")
                response = self.os_client.scroll(scroll_id=scroll_id, scroll=self.scroll_time)
                scroll_id = response['_scroll_id']
                hits = response['hits']['hits']
            
            self._close_csv_writer(current_csvfile, data_files, current_file_rows)
            self.os_client.clear_scroll(scroll_id=scroll_id)
            
        except Exception as e:
            if current_csvfile:
                current_csvfile.close()
            logger.error(f"Failed to export data: {e}")
            sys.exit(1)
        
        logger.info(f"Data export completed: {total_processed} documents in total, {len(data_files)} files")
        for f in data_files:
            logger.info(f"  {f}")
        
        self.data_files = data_files
        self._export_fields_csv(target_fieldnames, fields_map)
    
    def _init_opengauss_connection(self):
        """Initialize the openGauss database connection."""
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
            
            logger.info("openGauss connection succeeded.")
            
        except Exception as e:
            logger.error(f"Failed to connect to openGauss: {e}")
            sys.exit(1)
    
    def _close_opengauss_connection(self):
        """Close the openGauss database connection."""
        if self.og_cursor:
            self.og_cursor.close()
        if self.og_connection:
            self.og_connection.close()
            logger.info("openGauss connection closed")
        self.og_cursor = None
        self.og_connection = None
    
    def _read_fields_csv(self) -> List[Tuple[str, str, bool, str, str]]:
        """Read the field CSV file."""
        fields_info = []
        fields_file = self.fields_file

        if not os.path.exists(fields_file):
            logger.error(f"Field file does not exist: {fields_file}")
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
            
            logger.info(f"Reading field file: {fields_file}, {len(fields_info)} fields in total")
            return fields_info
            
        except Exception as e:
            logger.error(f"Failed to read field file: {e}")
            sys.exit(1)
    
    def _create_table(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """Create a table in openGauss."""
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
            logger.info(f"Table created successfully: {self.table_name}")
            
        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"Failed to create table: {e}")
            sys.exit(1)
    
    def _create_primary_key(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """Create the primary key."""
        id_field = next((name for name, field_type, _, _, _ in fields_info if field_type == '_id'), None)
        if id_field is None:
            logger.error(f"Index {self.index_name} has no _id field")
            sys.exit(1)
        
        create_primary_key_sql = sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
            sql.Identifier(self.table_name),
            sql.Identifier(f"{self.table_name}_pkey"),
            sql.Identifier(id_field)
        )
        
        try:
            self.og_cursor.execute(create_primary_key_sql)
            self.og_connection.commit()
            logger.info(f"Primary key created successfully, primary key field: {id_field}")
        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"Failed to create primary key: {e}")
            sys.exit(1)
    
    def _create_temp_csv_writer(self, fieldnames: List[str]):
        """Create a temporary CSV writer and write the header."""
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
        """Handle array field values."""
        if original_value == 'NULL' or original_value is None:
            return 'NULL'
        elif not original_value.startswith('{'):
            return '{' + original_value + '}'
        else:
            return original_value

    def _copy_batch(self, buffer, fieldnames):
        """Execute COPY in batches."""
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
        """Stream-process CSV files and COPY them to database tables."""
        if not os.path.exists(data_file):
            logger.error(f"Data file does not exist: {data_file}")
            return 0
        
        try:
            logger.info(f"Start streaming data import: {data_file}")
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
                
                logger.info(f"Import completed: {row_count} rows")
                return row_count

        except Exception as e:
            self.og_connection.rollback()
            logger.error(f"Failed to import data ({data_file}): {e}")
            sys.exit(1)

    def _import_data_with_copy(self, fields_info: List[Tuple[str, str, bool, str, str]]):
        """Import data using the COPY command."""
        if self.data_files is None:
            data_files = self._find_all_data_files()
        else:
            data_files = self.data_files
        
        if not data_files:
            logger.error(f"No data files found (data file: {self.data_file_prefix}_*.csv)")
            sys.exit(1)
        
        logger.info(f"Found {len(data_files)} data files:")
        for f in data_files:
            logger.info(f"  {f}")
        
        target_array_fields = [info[3] for info in fields_info if info[2] is True]
        total_rows = 0
        
        for i, data_file in enumerate(data_files, 1):
            logger.info(f"Importing file [{i}/{len(data_files)}]: {os.path.basename(data_file)}")
            rows = self._import_single_file(data_file, target_array_fields)
            total_rows += rows
        
        logger.info(f"All data imported! Total {total_rows} rows imported into table {self.table_name}")
    
    def export(self) -> None:
        """Execute the OpenSearch index export operation."""
        self._load_opensearch_config()
        self._load_export_config()

        logger.info(f"OpenSearch host: {self.os_host}:{self.os_port}")
        logger.info(f"Index name: {self.index_name}")
        
        self.array_fields.clear()
        self.vector_fields.clear()
        self._export_data_to_csv()
    
    def import_data(self) -> None:
        """Execute the data import operation to openGauss."""
        self._load_opengauss_config()
        self._init_opengauss_connection()
        
        logger.info(f"openGauss host: {self.og_host}:{self.og_port}")
        logger.info(f"Database: {self.og_database}")
        logger.info(f"schema: {self.og_schema}")
        logger.info(f"Table name: {self.table_name}")
        
        try:
            fields_info = self._read_fields_csv()
            self._create_table(fields_info)
            self._import_data_with_copy(fields_info)
            self._create_primary_key(fields_info)
        finally:
            self._close_opengauss_connection()
    
    def migrate(self) -> None:
        """Execute migration (import immediately after export)."""
        self._load_opensearch_config()
        self._load_export_config()
        self._load_opengauss_config()
        self._load_migrate_config()
        
        try:
            logger.info("[Step 1/2] Start exporting data...")
            self.export()
            logger.info("[Step 2/2] Start importing data...")
            self.import_data()
            
            if not self.migrate_keep_files:
                logger.info("Clean up the exported CSV files...")
                for f in self.data_files:
                    if os.path.exists(f):
                        os.remove(f)
                        logger.info(f"Deleted: {f}")
                if os.path.exists(self.fields_file):
                    os.remove(self.fields_file)
                    logger.info(f"Deleted: {self.fields_file}")
            logger.info("Migration completed!")
            
        finally:
            self._close_opengauss_connection()


def _create_default_config(config_file: str):
    """Create the default configuration file."""
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
    
    logger.info(f"Default configuration file created: {config_file}")
    logger.info("Edit the configuration file and rerun the script.")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='OpenSearch to openGauss data migration tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Run modes:
  export   - Export OpenSearch data to CSV files only
  import   - Import CSV data to openGauss only
  migrate  - One-click migration (import immediately after export)

Usage examples:
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
    
    parser.add_argument('mode', choices=['export', 'import', 'migrate'], help='Run mode')
    parser.add_argument('--config', default='config.ini', help='Configuration file path (default: config.ini)')
    parser.add_argument('--index', '-i', help='Index name (overrides opensearch.index in the configuration file)')
    parser.add_argument('--table', '-t', help='Target table name (overrides opengauss.table_name in the configuration file)')
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        logger.warning(f"Configuration file does not exist: {args.config}")
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

## Script Usage

### Export Mode (`export`)

**Command Examples:**

```bash
# Use the default configuration (config.ini in the current directory)
python3 opensearch2opengauss.py export

# Specify the configuration file path
python3 opensearch2opengauss.py export --config /path/to/config.ini

# Specify the configuration file and index name (overrides the index in the configuration file)
python3 opensearch2opengauss.py export --config config.ini --index my_other_index
```

**Parameter Description**

| Parameter  | Description                                                                                                    |
| :--------- | :------------------------------------------------------------------------------------------------------------- |
| `export`   | Specifies the export mode.                                                                                     |
| `--config` | Configuration file path. If not specified, `config.ini` in the current directory is used by default.           |
| `--index`  | Specifies the name of the index to export. If not specified, the index name in the configuration file is used. |

**Output Files:**

After a successful export, the following files are generated in the directory specified by `storage.data_dir` (using the default `output/` as an example):

```text
output/
├── my_index_1.csv      # Document data shard file 1
├── my_index_2.csv      # Document data shard file 2
├── my_index_3.csv      # Document data shard file 3
├── ...
└── my_index_fields.csv # Index structure (field description) file
```

### Import Mode (`import`)

**Prerequisites:**

Make sure that the files generated in export mode have been placed in the directory specified by `storage.data_dir`. The original file names and file contents must not be modified.

**Command Examples:**

```bash
# Use the default configuration for import (config.ini in the current directory)
python3 opensearch2opengauss.py import

# Specify the configuration file
python3 opensearch2opengauss.py import --config config.ini

# Specify the configuration file, index name, and target table name
python3 opensearch2opengauss.py import --config config.ini --index my_index --table my_table
```

**Parameter Description**

| Parameter  | Description                                                                                                    |
| :--------- | :------------------------------------------------------------------------------------------------------------- |
| `import`   | Specifies the import mode.                                                                                     |
| `--config` | Configuration file path. If not specified, `config.ini` in the current directory is used by default.           |
| `--index`  | Specifies the name of the index to import. If not specified, the index name in the configuration file is used. |
| `--table`  | Specifies the name of the target table. Priority: command line > configuration file > index name.              |

### Migration Mode (`migrate`)

**Command Examples:**

```bash
# Use the default configuration for migration (config.ini in the current directory)
python3 opensearch2opengauss.py migrate

# Specify the configuration file
python3 opensearch2opengauss.py migrate --config config.ini

# Specify the configuration file, index name, and target table name
python3 opensearch2opengauss.py migrate --config config.ini --index my_index --table my_table
```

**Parameter Description**

| Parameter | Description |
| :--------- | :---------- |
| `migrate` | Specifies the migration mode. |
| `--config` | Configuration file path. If not specified, `config.ini` in the current directory is used by default. |
| `--index` | Specifies the name of the index to migrate. If not specified, the index name in the configuration file is used. |
| `--table` | Specifies the name of the target table. Priority: command line > configuration file > index name. |

### Generating the Default Configuration File

If the configuration file does not exist, running a command in any mode automatically generates a default configuration file.

- If the `--config` parameter is specified in the command: generates the configuration file at the specified path.
- If the `--config` parameter is not specified in the command: generates `config.ini` in the current directory.

## Migration Logic

### Mapping an Index to a Table Structure

The tool queries the OpenSearch index mapping, maps each field to a field in an openGauss table, and additionally retains the OpenSearch metadata field `_id` as the primary key of the table.

**Example of an Index Mapping:**

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

**Table Structure After Migration:**

```text
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

### Data Type Mapping Rules

The tool has a built-in `TYPE_MAPPING` mapping table for converting OpenSearch field types to the corresponding openGauss data types. The actual mappings are determined by the `TYPE_MAPPING` in the code. Source field types that are not included in the `TYPE_MAPPING` mapping table are uniformly mapped to the openGauss `TEXT` type. Some mapping rules are as follows:

| OpenSearch Type       | openGauss Type |
| :-------------------- | :------------- |
| `keyword`, `text`     | `TEXT`         |
| `byte`, `short`       | `SMALLINT`     |
| `integer`             | `INTEGER`      |
| `long`                | `BIGINT`       |
| `float`, `half_float` | `REAL`         |
| `double`              | `FLOAT8`       |
| `boolean`             | `BOOLEAN`      |
| `date`                | `TIMESTAMP`    |
| `binary`              | `BYTEA`        |
| `ip`                  | `INET`         |
| `geo_point`           | `TEXT`         |
| `knn_vector`          | `VECTOR(n)`    |
| `_id`                 | `TEXT`         |
| ...                   | ...            |

### Array Data Migration Rules

Fields in OpenSearch may contain multiple values (Array). During migration, the tool automatically detects whether a field value is an array (Python `list` type) and processes it according to the following rules:

| Array Element Type | Type After Migration         | Description                                                                     |
| :----------------- | :--------------------------- | :------------------------------------------------------------------------------ |
| Numeric types      | `INTEGER[]` / `REAL[]`, etc. | Converted to native openGauss array types.                                      |
| Text types         | `TEXT`                       | Stored as text in array format to facilitate the creation of full-text indexes. |
| Vector types       | `VECTOR`                     | Processed according to the vector type.                                         |

### Object Type Migration Rules

#### Flattening `nested` or `object` Types

Nested objects in OpenSearch (such as `user.address.city`) are **flattened** during migration. Nested levels are joined with underscores (`_`):

```text
Original field: user.address.city
Migrated field: user_address_city
```

#### Splitting `join` Types

After identifying a `join` type, the script automatically splits it into two independent fields to preserve the association between parent and child documents.

| Split Field           | Type   | Description         |
| :-------------------- | :----- | :------------------ |
| `{join_field}.name`   | `TEXT` | Relationship name.  |
| `{join_field}.parent` | `TEXT` | Parent document ID. |

### Keyword Conflict Handling

Because OpenSearch and openGauss use different keywords, table names and field names constructed from index information may conflict with openGauss keywords, resulting in SQL syntax errors and failure to create the table structure. The tool has a built-in `OPENGAUSS_KEYWORDS` set. When a table name or field name matches a keyword in this set, the migration tool automatically adds an underscore (`_`) to the original name as a suffix. For example, `user` is converted to `user_`.

## Migration Rule Limitations

### Exported CSV Files Must Not Be Modified

The CSV files exported by this migration tool must not be modified in any way, including renaming files, modifying file contents, or deleting files before import.

During the import process, the migration tool parses the CSV files to obtain the target table structure and table data. If a file is modified or deleted, the following problems may occur:

- Import failure.
- Abnormal table structure after import.
- Data loss after import.

Therefore, use the exported CSV files as-is and do not make any changes.

### Table Name Length Limit

Because openGauss specifies that object names must not exceed 63 characters, the migration tool imposes the following restriction on table names on the target end: **The table name must not exceed 58 characters.**

The reason is that during migration, the tool automatically generates a corresponding primary key name based on the table name, using the naming rule `table_name_pkey`. If the table name exceeds 58 characters, the resulting primary key name will exceed the 63-character limit allowed by openGauss, causing the migration to fail.

### Field Name Length Limit

Because openGauss specifies that object names must not exceed 63 characters, the migration tool imposes the following restrictions on field names in OpenSearch indexes:

- The field name must not exceed 63 characters.
- For fields of nested types, the field name after being processed according to the flattening rules must not exceed 63 characters.
- For fields of the `join` type, the field name after splitting must not exceed 63 characters.

If a field name exceeds the limit, openGauss automatically truncates the field name when creating the table, retaining only the first 63 characters. Check the field name lengths before migration to avoid migration issues caused by truncation.

If a field name exceeding the limit is migrated successfully, it is recommended that you rename the table field after migration to avoid query issues caused by truncation. See the following example for the syntax:

```sql
ALTER TABLE table_name RENAME COLUMN old_field_name TO new_field_name;
```
