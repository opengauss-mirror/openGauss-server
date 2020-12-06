![structure](structure.png)

## Introduction to Detection

**Detection** is a monitor and abnormality-detect tool based on timeseries-forecast algorithm aim at openGauss
metrics, such as IO-Read、IO-Write、CPU-Usage、Memory-Usage、Disk-Usage. Detection can monitor multi-metric at same
time, and forecast trend of metric in future, if the forecast value in future is beyond the specified scope, it can
notify user timely.

Detection is composed of two element, **agent** and **detector**, **agent** is deployed on same machine with openGauss,
and **detector** can be deployed on any machine which can correspond with agent by _http_ or _https_, for security
reason, we suggest use _https_.

## Detection Installition

we suggest to use _anaconda_ to manage your python environment

**agent**

    python3.6+
    python-dateutil
    configparse

**detector**

    python3.6+
    pystan
    python-dateutil
    fbprophet
    pandas
    flask
    flask_sqlalchemy
    flask_restful
    configparse

notes:

using ```python -m pip install --upgrade pip``` to upgrade your pip

using ```conda install fbprophet``` to install fbprophet

## Introduction to a-detection.conf

the config is divided into section: `database`, `server`, `agent`, `forecast`, `log`, `security`

in all sections in config, the `log` and `security` section is public, in addition to this, `agent` is used for
**agent** module, `database`, `server`, `forecast` is used in **detector** module. so you should note the
path in every section, make it clear that it is on **agent** or **detector**.

    [agent]
    # timer of source to collect metric info
    source_timer_interval = 1
    # timer of sink to send data to server
    sink_timer_interval = 1
    # maxsize of channel: default value is 300
    channel_capacity = 300

    [security]
    # config for https, if `tls` is False, use http instead
    tls = True
    ca = ./certificate/ca/ca.crt
    server_cert = ./certificate/server/server.crt
    server_key = ./certificate/server/server.key
    agent_cert = ./certificate/agent/agent.crt
    agent_key = ./certificate/agent/agent.key

    [database]
    # location of sqlite
    database_path = ./data/sqlite.db
    # max rows of table in sqlite, in order to prevent table is too large.
    max_rows = 10000
    # frequency to remove surplus rows in table
    max_flush_cache = 1000

    [server]
    host = 127.0.0.1
    # listen host of app
    listen_host = 0.0.0.0
    # listen port of app
    listen_port = 8080

    [forecast]
    # forecast algorithm, fbprophet represent facebook's prophet algorithm
    predict_alg = fbprophet

    [log]
    # relative dirname of log
    log_dir = ./log


## Quick Guide

###1. deploy certificate

if you want to correspond with 'https', you should set `tls = True` firstly, then you should own certificate, 
then place the certificate in the appropriate location(`./certificate/ca`, `./certificate/server`, `./certificate/agent`).
if `tls=False`, then will use 'http' method

wo provide demo [script](shell) to generate certificate

use [script](shell/gen_ca_certificate.sh) to generate ca certificate and secret key. the same goes for other
certificates

     sh gen_ca_certificate.sh

this script will create dirname `certificate` in project, it include three sub-dirname named `ca`, `server`, `agent`,
ca certificate and secret key will be placed in `./certificate/ca`.

you can also use your own ca certificate, just place it in `ca`.

use [script](shell/gen_certificate.sh) to generate server certificate and secret key.

     sh gen_certificate.sh

after generating certificate and secret key, you should place it in corresponding sub-dirname(`./certificate/server` or
 `./certificate/agent`)

###2. Install openGauss

in our [task/metric_task.py](task/metric_task.py), the program will acquire openGauss metrics or openGauss environment automatically,

if not install openGauss, then Exception will occur.

###3. Deploy program

just as said above, the Detection is composed of two modules, respectively are **agent** and **detector**， the agent is
deployed with openGauss, and the detector can be deployed at any machine which can correspond with agent machine.

###4 Start program

*step 1: deploy code*

you can copy the code to other machine or use [script](bin/start.sh)

    sh start.sh --deploy_code

*step 2: start detector in machine which has installed openGauss*

    nohup python main.py -r detector > /dev/null 2>&1 &

or

    sh start.sh --start_detector

*step 3: start agent in agent machine*

    nohup python main.py -r agent > /dev/null 2>&1 &

or

    shb start.sh --start_agent

you can use [script](bin/stop.sh) to stop agent and detector process


###5 Obeserve result

the program has four logs file, respectively are **agent.log**, **server.log**, **monitor.log**, **abnormal.log**.

agent.log: this log record running status of agent module.

server.log: this log record running status of app

monitor.log: this log record monitor status such as forecasting, detecting, etc.

abnormal.log: this log record abnormal status of monitor metric


## Introduction to task

the monitor metric is defined in [task/metric_task.py](task/metric_task.py), the function should return metric value.

the monitor metric is configured in [task/metric_task.conf](task/metric_task.conf)

### How to add monitor metric

it is very easy to add metric that you want:

*step 1: write code in [task/metric_task.py](task/metric_task.py) which get the value of metric.*

*step 2: add metric config in [task/metric_task.conf](task/metric_task.conf)

    instruction of metric config:

    [cpu_usage_usage]
    minimum = 20
    maximum = 100
    data_period = 2000
    forecast_interval = 20S
    forecast_period = 150S

    int config of cpu_usage:

    'maximum': maximum allowable value of cpu_usage, it is considered as Exception if value is highed than it.
    'minimum': minimum allowable value of cpu_usage, it is considered as Exception if value is lower than it.
     note: you should at least provide one of it, if not, the metric will not be monitored.

    'data_period': the value of 'data_period' reprensent time interval or length from now. for example, if we
                  want to get last 100 second data from now, then the value of 'data_period' is '100S'; if we want to get
                  last 20 days from now, then 'data_period' is '20D'; if we want to get last 1000 datasets, then the
                  'data_period' is 1000
    'forecast_interval': the interval of predict operation. for example: if we want to predict 'cpu_usage' every
                         10 seconds, then the value of 'interval' is '10S'.

    'forecast_period': the forecast length, for example, if we want to forecast value of cpu_usage in the future
                       100 seconds at frequency of '1S', then the value of 'forecast_period' should be 100S.

    notes: 'S' -> second
           'M' -> minute
           'H' -> hour
           'D' -> day
           'W' -> week

for example:

if we want to monitor io_read of openGauss:

*step 1:*

    task/metric_task.py

    def io_read():
        child1 = subprocess.Popen(['pidstat', '-d'], stdout=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
        result = child2.communicate()
        if not result[0]:
            return 0.0
        else:
            return result[0].split()[3].decode('utf-8')

*step2:*

    config.conf

    [io_read]
    minimum = 30
    maximum = 100
    data_period = 1000
    forecast_interval = 25S
    forecast_period = 200S

*step3:*

restart your project

