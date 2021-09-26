from .foreign.mongodb import MongodbHandler
from .foreign.influxdb import InfluxdbHandler
from .local.sqlite import SqliteHandler


def get_actuate(actuate, host, port, user):
    if actuate == 'mogodb':
        pass
    elif actuate == 'influxdb':
        pass
    elif actuate == 'sqlite':
        pass
    else:
        pass
