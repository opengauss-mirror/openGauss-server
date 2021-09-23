class SqliteHandler:

    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.__conn = None
        self.__cur = None

    def get_conn(self):
        pass

    def create_table(self, sql):
        pass

    def insert(self, sql, data):
        pass

    def fetchone(self, sql):
        pass

    def fetchall(self, sql):
        pass
