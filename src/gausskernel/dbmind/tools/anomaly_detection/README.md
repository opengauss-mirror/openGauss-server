![structure](structure.png)

## Introduction to anomaly_detection

**anomaly_detection** is a monitor、 anomaly detection and slow SQL RCA(Root Cause Analysis) tool based on timeseries-forecast algorithm aim at openGauss
metrics, such as IO-Read、IO-Write、CPU-Usage、Memory-Usage、IO-Wait、disk_space、QPS、parts of key GUC parameter(work_mem、shared_buffers、max_connections) and information about slow SQL in the database WDR reporter. anomaly_detection can monitor multi-metric at same
time, and forecast trend of metric in future, if the forecast value in future is beyond the specified scope, it can
notify user timely.

anomaly_detection is composed of two element, **agent** and **detector**, **agent** is deployed on same machine with openGauss,
**detector** can be divided into **collector** and **monitor**, and it is deployed on any machine which can correspond with agent by _http_ or _https_, default method is __http__,
for security reason, we suggest to use _https_.

## anomaly_detection Installition

we suggest to use _anaconda_ to manage your python environment.

**agent**

    python3.6+
    python-dateutil
    configparse
    prettytable

**detector**
    
    sqlparse
    python3.6+
    python-dateutil
    pandas
    flask
    flask_restful
    configparse
    prettytable
    pmdarima

notes:

using ```python -m pip install --upgrade pip``` to upgrade your pip.

you can use ```pip install -r requirements.txt``` to configure the environment.

if you want to use `fprophet`, it is recommended to use ```conda install fbprophet``` 
to install `fbprophet`.


##Parameter explanation

use ```python main.py --help``` to get help:

    usage:
        python main.py start [--role {{agent,collector,monitor}}] # start local service.
        python main.py stop [--role {{agent,collector,monitor}}] # stop local service.
        python main.py start [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,collector,monitor}}]
        # start the remote service.
        python main.py stop [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,collector,
        monitor}}] # stop the remote service.
        python main.py deploy [--user USER] [--host HOST] [--project-path PROJECT_PATH] # deploy project in remote host.
        python main.py diagnosis [--query] [--start_time] [--finish_time] # rca for slow SQL.
        python main.py show_metrics # display all monitored metrics(can only be executed on 'detector' machine).
        python main.py forecast [--metric-name METRIC_NAME] [--forecast-periods FORECAST_PERIODS]
         [--forecast-method {{auto_arima, fbprophet}}] [--save-path SAVE_PATH] # forecast future trend of
         metric(can only be executed on 'detector' machine).
    
    Anomaly-detection: a time series forecast and anomaly detection tool.
    
    positional arguments:
      {start,stop,deploy,show_metrics,forecast,diagnosis}
    
    optional arguments:
      -h, --help            show this help message and exit
      --user USER           User of the remote server.
      --host HOST           IP of the remote server.
      --project-path PROJECT_PATH
                            Project location in remote server.
      --role {agent,collector,monitor}
                            Run as 'agent', 'collector', 'monitor'. Notes: ensure
                            the normal operation of the openGauss in agent.
      --metric-name METRIC_NAME
                            Metric name to be predicted if this parameter is not
                            provided, all metrics in the database will be predicted.
      --query QUERY         target SQL for RCA
      --start_time START_TIME
                            start time of the query
      --finish_time FINISH_TIME
                            finish time of the query
      --period PERIOD       Forecast periods of metric, it should be integernotes:
                            the specific value should be determined to the
                            training data. if this parameter is not provided, the
                            default value '100S' will be used.
      --freq FREQ           forecast gap, time unit: S: Second, M: Minute, H:
                            Hour, D: Day, W: Week.
      --forecast-method FORECAST_METHOD
                            Forecast method, default method is 'auto_arima',if
                            want to use 'fbprophet', you should install fbprophet
                            first.
      --save-path SAVE_PATH
                            Save the results to this path using CSV format, if
                            this parameter is not provided, the result will not be
                            saved.
      -v, --version         show the program's version number and exit


## Introduction to a-detection.conf

the config is divided into section: `database`, `server`, `agent`, `forecast`, `log`, `security`.

in all sections in the config, the `log` and `security` section is public, in addition to this, `
