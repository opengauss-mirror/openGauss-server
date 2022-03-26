# DBMind
DBMind is a part of openGauss, which empowers openGauss to carry the autonomous operations and maintenance capabilities. DBMind is leading and open-source. Through DBMind, users can easily discover database problems and the root causes of the problems in seconds.

![DBMind overview](docs/dbmind.png)

# Getting Started

### Prerequisites
In order to run DBMind, the following components should be configured and running.

#### Python Runtime
At least Python 3.6.

#### Third-party Dependencies
Use `pip install` to install the python dependencies.
Type the `pip install` command with dependencies according to the environment you are running:
```
pip install -r requirements-aarch64.txt | requirements-x86.txt
```

#### Prometheus up and Running
Download and run the [Prometheus](https://prometheus.io/) time-series database.

#### Node Exporter
Download and run the [Prometheus node exporter](https://prometheus.io/download/#node_exporter). Node-exporter is to monitor the Linux system. Hence, one Linux environment only needs to deploy one node-exporter.

### DBMind Components
The following DBMind components are required:

**Note: If you want to get higher security, you should use the HTTPS scheme.** 

#### openGauss Exporter
The openGauss-exporter reads data from the database and places it on the Prometheus time-series database.
OpenGauss-exporter is to monitor only one database instance. So if your deployment environment has not only one instance, you should start multiple openGauss-exporters to correspond to monitor multiple database instances.
It needs database access with a user having the role of at least **monadmin** (monitoring administrator) granted to run it. For example, you can grant monadmin privilege to role dbmind as below:
```
ALTER USER dbmind monadmin;
``` 
Use the following command with the parameters below:

```
gs_dbmind component opengauss_exporter ...
```
You can get detailed explanations of this component through passing `--help`:
```
gs_dbmind component opengauss_exporter --help
```

For example, the following command starts it:
```
gs_dbmind component opengauss_exporter --url postgresql://username:password@host:port/database --web.listen-address 0.0.0.0 --web.listen-port 9187 --log.level warn --disable-https ...
```

To test that the exporter is up, type the following command on its host (or use change the localhost to the server address):
```
curl -vv http://localhost:9187/metrics
```

#### Reprocessing Exporter
Reprocessing-exporter is a re-processing module for metrics stored in the Prometheus server. It helps Prometheus to reprocess the metric data then dump the new data into Prometheus. Therefore, only one needs to be started in a deployment environment.
To run it use the command below:
```
gs_dbmind component reprocessing_exporter ...
```
Users can see usage by using `--help` too.

See this example for running the exporter in a single machine development environment:
```
gs_dbmind component reprocessing_exporter 127.0.0.1 9090 --web.listen-address 0.0.0.0 --web.listen-port 9189
```
Use the following command to check that the service is up:
```
curl http://127.0.0.1:9189/metrics
```

### Configure, Start and Stop the DBMind Service 
DBMind service is a memory-resident backend service. Therefore, users should configure it first then start or stop the service by using the configuration.

Service usages:

    $ gs_dbmind service --help
    usage:  service [-h] -c DIRECTORY [--only-run {...}] [--interactive | --initialize] {setup,start,stop}
    
    positional arguments:
      {setup,start,stop}    perform an action for service
    
    optional arguments:
      -h, --help            show this help message and exit
      -c DIRECTORY, --conf DIRECTORY
                            set the directory of configuration files
      --only-run {slow_query_diagnosis,forecast}
                            explicitly set a certain task running in the backend
      --interactive         configure and initialize with interactive mode
      --initialize          initialize and check configurations after configuring.


#### Configure
DBMind offers two methods to configure. The one is an interactive mode by using `--interactive` argument, the other is a modification by hands. 

See this example for configuring in the interactive mode:
```
gs_dbmind service setup -c CONF_DIRECTORY --interactive
```
Then users can type parameters into the shell terminal.

See the following example for configuring by hands:
```
gs_dbmind service setup -c CONF_DIRECTORY
```
After executing the above command, the directory `CONF_DIRECTORY` will generate too many configuration files. Therefore, users should modify these parameters in the `CONF_DIRECTORY/dbmind.conf`. While users finish configuring, this command needs to be run to initialize DBMind according to the `CONF_DIRECTORY/dbmind.conf`.
```
gs_dbmind service setup -c CONF_DIRECTORY --initialize
```

#### Start or Stop the DBMind Service
After configuring, specify your CONF_DIRECTORY, users can start or stop the service directly. 
```
gs_dbmind service start/stop -c CONF_DIRECTORY
```

### Component 
If users want to use a specific component offline. They can use the sub-command `component`:
```
gs_dbmind component xxx ...
```

`xxx` is the name of a component. Users can also get the component list by using the `--help` argument.

For example, use the following component to tune the knobs of a database:
```
gs_dbmind component xtuner --help
```

# LICENSE
Mulan PSL v2

