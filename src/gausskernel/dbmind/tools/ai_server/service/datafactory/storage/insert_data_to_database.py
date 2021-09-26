#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : insert_data_to_database.py
# Version      :
# Date         : 2021-4-7
# Description  : Inserting data into the database
#############################################################################

try:
    import sys
    import os
    import pymongo
    import sqlite3
    from influxdb import InfluxDBClient

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))
    from common.utils import Common
except ImportError as err:
    sys.exit("insert_data_database.py: Failed to import module: %s." % str(err))


class SaveData:
    def __init__(self, logger):
        self.logger = logger
        try:
            self.host = Common.parser_config_file("database", "host")
            self.port = Common.parser_config_file("database", "port")
            self.user = Common.parser_config_file("database", "user")
        except Exception as err_msg:
            logger.error(err_msg)
            raise Exception(err_msg)
        self.pwd = ""
        self.collection_mapping = {"os": ["OSExporter"],
                                   "database": ["DatabaseExporter"],
                                   "all": ["OSExporter", "DatabaseExporter"]
                                   }

    def insert_data(self, *args):
        pass

    def run(self, agent_data="", kafka_data=""):
        database = Common.parser_config_file("database", "name")
        if database.lower() == "mongodb":
            MongoDb(self.logger).insert_data(agent_data, kafka_data)
        elif database == "influxdb":
            InfluxDb(self.logger).insert_data(agent_data, kafka_data)
        elif database == "sqlite":
            Sqlite(self.logger).insert_data(agent_data, kafka_data)


class MongoDb(SaveData):
    def __init__(self, logger):
        super(MongoDb, self).__init__(logger)

    def insert_data(self, *args):
        """
        insert data to database
        :param args: args[0] is agent data, args[1] is kafka data
        :return: None
        """
        data_list = [data for data in args if data]
        client = None
        max_size = Common.parser_config_file("database", "size")
        max_rows = Common.parser_config_file("database", "max_rows")
        try:
            max_size = int(max_size) if max_size else None
            max_rows = int(max_rows) if max_rows else None
            self.logger.info("Start to insert data: %s." % data_list)
            agent_collection_type = Common.parser_config_file("agent", "collection_type")
            # connect MongoDB
            client = pymongo.MongoClient(host=self.host, port=int(self.port))
            for data in data_list:
                # create and use db
                my_db = client[data["database"]]
                if self.pwd:
                    my_db.authenticate(self.user, self.pwd)
                coll_list = my_db.list_collection_names()
                for collection_type in self.collection_mapping[agent_collection_type]:
                    if not data.get(collection_type, None):
                        self.logger.warn("The %s data does not exist." % collection_type)
                        continue
                    # create collection if collection does not exist
                    if collection_type not in coll_list:
                        my_set = my_db.create_collection(collection_type,
                                                         capped=True, size=max_size, max=max_rows)
                    # get collection if collection exist
                    else:
                        my_set = my_db.get_collection(collection_type)
                    # insert data
                    my_set.insert_one(data[collection_type])
                self.logger.info("Successfully insert data.\n%s" % ("-" * 90))
        except Exception as err_msg:
            self.logger.error(err_msg)
            raise Exception(err_msg)
        finally:
            if client:
                client.close()


class InfluxDb(SaveData):
    def __init__(self, logger):
        super(InfluxDb, self).__init__(logger)

    def insert_data(self, *args):
        """
        insert data to database
        :param args: args[0] is agent data, args[1] is kafka data
        :return: None
        """
        data_list = [data for data in args if data]
        client = None
        try:
            self.logger.info("Start to insert data:%s." % data_list)
            agent_collection_type = Common.parser_config_file("agent", "collection_type")
            # connect influxDb
            if self.pwd:
                client = InfluxDBClient(host=self.host, port=int(self.port),
                                        username=self.user, password=self.pwd)
            else:
                client = InfluxDBClient(host=self.host, port=int(self.port))
            db_list = client.get_list_database()
            for data in data_list:
                db_name = data["database"]
                if db_name not in db_list:
                    # create database if database does not exist
                    client.create_database(db_name)
                    # Create Data Retention Policy
                    client.create_retention_policy("there_days", "3d", "1", db_name, True)

                for collection_type in self.collection_mapping[agent_collection_type]:
                    if not data.get(collection_type, None):
                        self.logger.warn("The %s data does not exist." % collection_type)
                        continue
                    json_body = [
                        {
                            "measurement": collection_type,
                            "fields": data[collection_type]
                        }
                    ]
                    client.write_points(json_body, database=db_name)
                self.logger.info("Successfully insert data.\n%s" % ("-" * 90))
        except Exception as err_msg:
            self.logger.error(err_msg)
            raise Exception(err_msg)
        finally:
            if client:
                client.close()


class Sqlite(SaveData):
    def __init__(self, logger):
        super(Sqlite, self).__init__(logger)

    def insert_data(self, *args):
        """
        insert data to database
        :param args: args[0] is agent data, args[1] is kafka data
        :return: None
        """
        data_list = [data for data in args if data]
        cursor = None
        conn = None
        try:
            self.logger.info("Start to insert data:%s." % data_list)
            database_path = Common.parser_config_file("database", "database_path")
            agent_collection_type = Common.parser_config_file("agent", "collection_type")
            if not os.path.isdir(os.path.realpath(database_path)):
                os.makedirs(os.path.realpath(database_path), mode=0o700)
            for data in data_list:
                # connect or create db
                conn = sqlite3.connect(os.path.realpath(os.path.join(database_path,
                                                                     data["database"] + ".db")))
                self.logger.info("Successfully connect: %s." % data["database"] + "db")

                # get cursor
                cursor = conn.cursor()
                for collection_type in self.collection_mapping[agent_collection_type]:
                    if not data.get(collection_type, None):
                        self.logger.warn("The %s data does not exist." % collection_type)
                        continue
                    columns = []
                    values = []
                    # get table columns and values
                    for key, value in data[collection_type].items():
                        columns.append("%s text" % key)
                        values.append("'" + str(value) + "'")

                    # create table
                    create_tb_sql = "CREATE TABLE IF NOT EXISTS %s (%s);" % (collection_type,
                                                                             ", ".join(columns))
                    self.logger.info("Command for create table: %s" % create_tb_sql)
                    cursor.execute(create_tb_sql)
                    self.logger.info("Successfully create table: %s." % collection_type)

                    # insert data to table
                    insert_data_sql = "insert into %s values (%s);" % (collection_type,
                                                                       ", ".join(values))
                    self.logger.info("Command for insert data: %s" % insert_data_sql)
                    cursor.execute(insert_data_sql)
                    conn.commit()
                self.logger.info("Successfully insert data.\n%s" % ("-" * 90))
        except Exception as err_msg:
            self.logger.error("Failed to insert data to sqlite, \nError: %s" % str(err_msg))
            self.logger.info("Rolling back...")
            conn.rollback()
            self.logger.info("Successfully rollback.\n")
            raise Exception(str(err_msg))
        finally:
            cursor.close()
