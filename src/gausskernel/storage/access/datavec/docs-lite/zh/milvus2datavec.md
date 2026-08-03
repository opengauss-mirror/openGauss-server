# 从Milvus迁移至openGauss DataVec

本教程通过Python编程语言，支持将本地Milvus数据迁移至openGauss DataVec实例中

## 环境准备

- 已部署2.3 及以上版本的Milvus实例
- 已部署7.0.0-RC1 及以上版本的openGauss实例，容器部署参考[容器镜像安装](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)
- 已安装3.8 及以上版本的Python环境
- 已安装涉及的Python库

```python
pip3 install psycopg2
# pymilvus要求2.3+的版本
pip3 install pymilvus
pip3 install numpy
```

## 迁移操作

1. 参考如下迁移脚本milvus2datavec.py及配置文件config.ini，根据本地部署的Milvus与openGauss进行配置修改：

    迁移配置文件config.ini如下：

    ```
    [Milvus]
    host = localhost
    port = 19530
    # Milvus username and password, if not set, use anonymous access. Not supported yet.
    #username = user_name
    #password = xxxxxx

    [openGauss]
    host = localhost
    # Migration user need to use md5 encrypted password
    user = postgres
    password = xxxxxx
    port = 5432
    database = postgres

    [Table]
    milvus_collection_name = test
    opengauss_table_name = test

    [SparseVector]
    # openGauss only support 1000 dimensions for sparsevec
    default_dimension = 1000

    [Output]
    folder = output

    [Migration]
    cleanup_temp_files = true
    ```

    milvus2datavec.py迁移脚本如下：

    ```python
    import psycopg2
    import csv
    from pymilvus import connections, Collection, utility
    import configparser
    import numpy as np
    import os
    import sys
    import logging
    from typing import List, Dict, Any, Optional, Union
    from datetime import datetime

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)

    class MilvusToOpenGaussMigrator:
        def __init__(self, config_file: str = 'config.ini'):
            self.config = self._load_config(config_file)
            self.update_config()
            self.csv_file_path = self._get_csv_file_path()
            self.fields = []
            self.MAX_WINDOW_SIZE = 16384  # Milvus default max query window
            self.SPARSE_DIMENSION = self.config.getint('SparseVector', 'default_dimension', fallback=1000)
            self.MAX_SPARSE_DIMENSION = 1000
            self.milvus_version = None

        def _load_config(self, config_file: str) -> configparser.ConfigParser:
            """Load configuration file"""
            config = configparser.ConfigParser()
            try:
                if not config.read(config_file):
                    raise FileNotFoundError(f"Config file {config_file} not found")
                return config
            except Exception as e:
                logger.error(f"Failed to load config: {e}")
                raise

        def update_config(self):
            """Update configuration from args"""
            if len(sys.argv) == 3:
                if not sys.argv[1].strip() or not sys.argv[2].strip():
                    logger.error("Error: Both Milvus collection name and openGauss table name must be provided")
                    sys.exit(1)
                self.config.set('Table', 'milvus_collection_name', sys.argv[1].strip())
                self.config.set('Table', 'opengauss_table_name', sys.argv[2].strip())

            is_enable_stdin_password = os.getenv("enable.env.password", "").lower()
            if is_enable_stdin_password == "true":
                opengauss_password = os.getenv("openGauss.password", "").strip()
                if not opengauss_password.strip():
                    logger.error("Error: openGauss password must be provided")
                    sys.exit(1)
                self.config.set('openGauss', 'password', opengauss_password)

        def _get_csv_file_path(self) -> str:
            """Generate CSV file path with timestamp"""
            output_folder = self.config.get('Output', 'folder', fallback='output')
            os.makedirs(output_folder, exist_ok=True)
            milvus_collection = self.config.get('Table', 'milvus_collection_name')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return os.path.join(output_folder, f"{milvus_collection}_{timestamp}.csv")

        def _connect_milvus(self) -> Collection:
            """Connect to Milvus"""
            try:
                connections.connect(
                    alias="default",
                    host=self.config.get('Milvus', 'host'),
                    port=self.config.get('Milvus', 'port')
                )

                self.milvus_version = utility.get_server_version()
                logger.info(f"Connected to Milvus {self.milvus_version}")

                collection_name = self.config.get('Table', 'milvus_collection_name')
                collection = Collection(collection_name)
                collection.load()
                self.fields = [field.name for field in collection.schema.fields]
                logger.info(f"Loaded collection: {collection_name}")
                return collection
            except Exception as e:
                logger.error(f"Milvus connection failed: {e}")
                raise

        def _connect_opengauss(self) -> psycopg2.extensions.connection:
            """Connect to openGauss"""
            try:
                conn = psycopg2.connect(
                    user=self.config.get('openGauss', 'user'),
                    password=self.config.get('openGauss', 'password'),
                    host=self.config.get('openGauss', 'host'),
                    port=self.config.get('openGauss', 'port'),
                    database=self.config.get('openGauss', 'database')
                )
                logger.info("Connected to openGauss")
                return conn
            except Exception as e:
                logger.error(f"openGauss connection failed: {e}")
                raise

        def _process_sparse_vector(self, sparse_data: Union[dict, bytes, list], dimension: int) -> str:
            """Convert to openGauss SPARSEVEC format: '{indice:value,...}/dim'"""
            if sparse_data is None:
                return "NULL"

            try:
                # Convert to {index:value} dict
                if dimension is None or dimension <=0:
                    dimension = self.MAX_SPARSE_DIMENSION

                sparse_dict = {}

                if isinstance(sparse_data, dict):
                    sparse_dict = {
                        int(k+1): float(v)
                        for k, v in sparse_data.items()
                    }
                else:
                    raise ValueError(f"Unsupported format: {type(sparse_data)}")

                if not sparse_dict:
                    return "{}/" + str(dimension)

                try:
                    # Sort by index to ensure consistent output

                    sorted_items = sorted(sparse_dict.items(), key=lambda x: x[0])
                    entries = ",".join(f"{k}:{v}" for k, v in sorted_items)
                    return "{" + entries + "}/" + str(dimension)
                except Exception as sort_error:
                    logger.warning(f"Sorting failed, using unsorted vector: {sort_error}")
                    entries = ",".join(f"{k}:{v}" for k, v in sparse_dict.items())
                    return "{" + entries + "}/" + str(dimension)

            except Exception as e:
                logger.error(f"Sparse vector conversion failed: {e}")
                return "NULL"

        def _process_field_value(self, value: Any, field_type: str, dimension: Optional[int] = None) -> str:
            """Convert field value to CSV string"""
            if value is None:
                return "NULL"
            elif field_type == "SPARSE_FLOAT_VECTOR":
                return self._process_sparse_vector(value, dimension or self.SPARSE_DIMENSION)
            elif isinstance(value, (list, np.ndarray)):
                return "[" + ",".join(str(x) for x in value) + "]"
            elif isinstance(value, dict):
                return json.dumps(value)
            else:
                return str(value)

        def _create_opengauss_table(self, conn: psycopg2.extensions.connection, collection: Collection) -> None:
            """Create table with SPARSEVEC columns"""
            table_name = self.config.get('Table', 'opengauss_table_name')
            cursor = conn.cursor()

            try:
                # Check if table exists
                cursor.execute(f"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = '{table_name}');")
                if cursor.fetchone()[0]:
                    logger.warning(f"Table {table_name} exists")
                    return

                # Build CREATE TABLE statement
                columns = []
                for field in collection.schema.fields:
                    dim = field.dim if hasattr(field, 'dim') else None
                    pg_type = self._milvus_to_opengauss_type(field.dtype.name, dim)
                    columns.append(f"{field.name} {pg_type}")

                # Add primary key if exists
                pk_fields = [f.name for f in collection.schema.fields if f.is_primary]
                if pk_fields:
                    columns.append(f"PRIMARY KEY ({', '.join(pk_fields)})")

                create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
                cursor.execute(create_sql)
                conn.commit()
                logger.info(f"Created table: {table_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Table creation failed: {e}")
                raise
            finally:
                cursor.close()

        def _process_milvus_data(self, collection: Collection, batch_data: List[object]) -> List[Dict]:
            """Process data with sparse vector support"""
            if not batch_data:
                return []

            try:
                # Get field metadata
                fields_meta = {}
                for field in collection.schema.fields:
                    fields_meta[field.name] = {
                        "type": field.dtype.name,
                        "dim": getattr(field, 'dim', None)
                    }

                # Process all_results
                processed = []
                for row in batch_data:
                    processed_row = {
                        field: self._process_field_value(
                            row.get(field),
                            fields_meta[field]["type"],
                            fields_meta[field]["dim"]
                        )
                        for field in self.fields
                    }
                    processed.append(processed_row)

                return processed
            except Exception as e:
                logger.error(f"Process milvus data failed: {e}")
                raise

        def _write_to_csv_file(self, chunk_file: str, batch: List[object]):
            """Export batch data to CSV file"""
            if not batch:
                return

            try:
                with open(chunk_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fields)
                    writer.writeheader()
                    chunk_size = 1000
                    for i in range(0, len(batch), chunk_size):
                        writer.writerows(batch[i:i + chunk_size])
            except (IOError, csv.Error) as e:
                logger.error(f"Failed to write CSV file {chunk_file}: {e}")
                raise

        def _generate_chunk_filename(self, chunk_id: int) -> str:
            """Generate chunk filename with consistent naming pattern"""
            base_name, _ = os.path.splitext(self.csv_file_path)
            return f"{base_name}_part{chunk_id}.csv"

        def _cleanup_failed_export(self, file_paths: List[str]):
            """Clean up partially created files on export failure"""
            if not file_paths:
                return

            logger.warning(f"Cleaning up {len(file_paths)} partially created files")
            for file_path in file_paths:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except OSError as e:
                    logger.warning(f"Failed to remove file {file_path}: {e}")

        def _export_to_csv_chunked(self, collection: Collection) -> List[str]:
            """Export data from Milvus collection to CSV chunks"""
            collection.flush()
            total_count = collection.num_entities
            logger.info(f"Total rows to export: {total_count}")

            if total_count == 0:
                logger.warning("Collection is empty, no data to export")
                return []

            batch_size = 10000
            iterator = collection.query_iterator(
                expr="",
                batch_size=batch_size,
                output_fields=self.fields,
                consistency_level="Strong"
            )

            file_paths = []
            exported_rows = 0

            try:
                while True:
                    batch_results = iterator.next()
                    if not batch_results:
                        break

                    chunk_id = len(file_paths) + 1
                    chunk_file = self._generate_chunk_filename(chunk_id)
                    file_paths.append(chunk_file)

                    processed_results = self._process_milvus_data(collection, batch_results)
                    self._write_to_csv_file(chunk_file, processed_results)

                    batch_len = len(batch_results)
                    exported_rows += batch_len

                    if exported_rows % (batch_size * 10) == 0:
                        logger.info(f"Exported {exported_rows}/{total_count} rows")

                    logger.info(f"Created chunk {chunk_id}, chunk size: {batch_len} rows")
                return file_paths
            except Exception as e:
                self._cleanup_failed_export(file_paths)
                logger.error(f"Export failed: {e}")
                raise
            finally:
                iterator.close()

        def _import_to_opengauss(self, conn: psycopg2.extensions.connection, file_paths: List[str]) -> None:
            """Import CSV data to openGauss"""
            table_name = self.config.get('Table', 'opengauss_table_name')
            cursor = conn.cursor()

            try:
                cursor.execute("SET client_encoding TO 'UTF8';")
                conn.commit()

                # Prepare for bulk import
                cursor.execute(f"TRUNCATE TABLE {table_name};")
                conn.commit()

                total_rows = 0
                for i, csv_file in enumerate(file_paths, 1):
                    with open(csv_file, 'rb') as f:
                        # Use COPY for bulk load
                        copy_sql = f"""
                        COPY {table_name} ({', '.join(self.fields)})
                        FROM STDIN WITH (FORMAT CSV, HEADER, NULL 'NULL', ENCODING 'UTF8');
                        """
                        cursor.copy_expert(copy_sql, f)
                        conn.commit()

                        rows_imported = cursor.rowcount
                        total_rows += rows_imported
                        logger.info(f"Imported {rows_imported} rows from {csv_file}")

                logger.info(f"Total imported: {total_rows} rows")
            except Exception as e:
                conn.rollback()
                logger.error(f"Import failed: {e}")
                raise
            finally:
                cursor.close()

        def run_migration(self) -> None:
            """Execute full migration workflow"""
            start_time = datetime.now()
            logger.info("Starting migration")

            try:
                # Step 1: Connect to Milvus
                milvus_collection = self._connect_milvus()

                # Step 2: Export data to CSV
                csv_files = self._export_to_csv_chunked(milvus_collection)

                # Step 3: Connect to openGauss and create table
                opengauss_conn = self._connect_opengauss()
                self._create_opengauss_table(opengauss_conn, milvus_collection)

                # Step 4: Import to openGauss
                self._import_to_opengauss(opengauss_conn, csv_files)

                # Cleanup
                if self.config.getboolean('Migration', 'cleanup_temp_files', fallback=True):
                    for f in csv_files:
                        try:
                            os.remove(f)
                        except Exception as e:
                            logger.warning(f"Failed to delete {f}: {e}")

                logger.info(f"Migration completed in {datetime.now() - start_time}")
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                raise
            finally:
                if 'opengauss_conn' in locals():
                    opengauss_conn.close()
                connections.disconnect("default")

        @staticmethod
        def _milvus_to_opengauss_type(milvus_type: str, dim: Optional[int] = None) -> str:
            """Map Milvus types to openGauss types"""
            type_map = {
                "Int64": "BIGINT",
                "Int32": "INTEGER",
                "Int16": "SMALLINT",
                "Int8": "SMALLINT",
                "Float": "REAL",
                "Double": "DOUBLE PRECISION",
                "Bool": "BOOLEAN",
                "VarChar": "VARCHAR",
                "String": "TEXT",
                "Json": "JSONB",
                "FLOAT_VECTOR": f"VECTOR({dim})" if dim else "VECTOR",
                "BINARY_VECTOR": f"BIT({dim})" if dim else "BIT",
                "SPARSE_FLOAT_VECTOR": "SPARSEVEC"
            }
            return type_map.get(milvus_type, "TEXT")


    if __name__ == "__main__":
        try:
            migrator = MilvusToOpenGaussMigrator()
            migrator.run_migration()
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            exit(1)
        finally:
            logger.info("Migration end.")
    ```

2. 将迁移脚本、配置文件放置在同一目录下，层级如下：

    ```txt
    ├── milvus2datavec.py
    └── config.ini
    ```

3. 运行迁移脚本，并查看输出结果：

    ```
    python3 milvus2datavec.py
    ```

4. 登录openGauss，查看数据是否完成迁移：
    
    4.1 进入容器：

    ```
    $ docker exec -it CONTAINER_ID bash
    ```
    
    4.2 登录`omm`超级用户：

    ```
    $ su omm
    $ gsql -d postgres -p 5432
    ```
    
    4.3 查看迁移表数据量：

    ```
    $ select count(*) from test;
    ```
