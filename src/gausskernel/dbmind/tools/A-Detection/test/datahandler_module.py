import sys
sys.path.append('../')
from detector.monitor import data_handler


dh = data_handler.DataHandler('io_read', './data/metric.db')

#with dh('io_read', './data/metric.db') as db:
#    ts = db.get_timeseries(period=10)
#    print(ts)
#    print(len(ts))
#    ts = db.get_timeseries(period='10S')
#    print(ts)
#    print(len(ts))


dh.connect_db()
ts = dh.get_timeseries(period='10S')
print(ts)
dh.close()
