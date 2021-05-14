[TOC]


# X-Tuner
Knob tuning is extremely important to the database in achieving high performance. Unfortunately, the knob tuning task is an NP-hard problem. Generally speaking, tuning depends on one's experience and understanding of the current system also contains great uncertainties about performance. Hence, some engineers attempt to build an intelligent system with automatic tuning, such as **postgresqltuner.pl** and
**mysqltuner.pl**. Also, with the rising of AI technology, the knob tuning problem has some AI-based solutions, such as OtterTune.
However, current tuning approaches have several limitations:

- DBAs cannot tune too many distinct instances on different environments (e.g., different database vendors);
- traditional machine-learning methods either cannot find the best configurations or relies on excessive high-quality data sets which are rather hard to obtain;
- some reinforcement learning methods lose sight of fewer relations between state (database status) and action (knob) while tuning knobs;

Thus, we want to implement a robust system X-Tuner and try to avoid the above limitations.
X-Tuner is a component of openGauss, implemented with deep reinforcement
learning and global optimization algorithm. X-Tuner is also a DB-tuner framework that you can develop your tuning method on it far from being only a tuning tool.

# How to use
## Installation
Two running modes are supported:
1. Install it to the operating system. After the installation, you can use `gs_xtuner` to tune.
2. Run the source code directly.

Install to the operating system, you can execute：

```
python3 setup.py install
```

If you want to execute from source code, you need to install the dependency first. You can run the following command:

```
pip install -r requirements.txt
```

Then you can switch to the tuner directory and run the following command to get help information:
```
export PYTHONPATH='..'
python3 main.py --help
```


## Dependencies
If you use a Python runtime that OS comes with, you should install Python SDK, such as:

    sudo yum install python3-devel

You should install the following mathematical libraries to your OS, so that some Python libraries can import them.

For CentOS-based OS (e.g., Redhat, CentOS, EulerOS):
    
    sudo yum install lapack lapack-devel blas blas-devel
     
For Debian-based OS (e.g., Ubuntu, KaliLinux):
    
    sudo apt-get install gfortran libopenblas-dev liblapack-dev

You should install the following dependencies by python-pip.
    
    paramiko
    bayesian-optimization
    ptable

If you want to use deep learning, you should also install the following libraries:

    tensorflow>=2.2.0
    keras-rl2
    keras>=2.4.0
    

Note: Firstly, please upgrade your pip: ```python -m pip install --upgrade pip```

## Start tuning
1. Start your database instance first;
2. Choose a benchmark(TPC-C, TPC-H, etc.) and import data to the database;
3. **Write your benchmark script**. There are some demos in the directory tuner/benchmark.
Maybe you can directly use some benchmark scripts. And if you have your scenarios, you can code here, 
please see [benchmark readme](tuner/benchmark/README.md);
4. Modify the configuration file according to the description of the configuration file. The default path of the configuration file is **xtuner.conf**. You can run the `--help` command to obtain the default configuration file path. 
Much important information is configured here.
5. Let's start a tuning journey, you can do the following usage. Please remember that the current working directory is the `tuner`.

Note: There are some demo configuration JSON files in the `share` directory. 


    usage: gs_xtuner [-h] [--db-name DB_NAME] [--db-user DB_USER] [--port PORT]
                     [--host HOST] [--host-user HOST_USER]
                     [--host-ssh-port HOST_SSH_PORT] [-f DB_CONFIG_FILE]
                     [-x TUNER_CONFIG_FILE] [-v]
                     {train,tune,recommend}
    
    X-Tuner: a self-tuning tool integrated by openGauss.
    
    positional arguments:
      {train,tune,recommend}
                            Train a reinforcement learning model or tune database
                            by model. And also can recommend best_knobs according
                            to your workload.
    
    optional arguments:
      -h, --help            show this help message and exit
      -f DB_CONFIG_FILE, --db-config-file DB_CONFIG_FILE
                            You can pass a path of configuration file otherwise
                            you should enter database information by command
                            arguments manually. Please see the template file
                            share/client.json.template.
      -x TUNER_CONFIG_FILE, --tuner-config-file TUNER_CONFIG_FILE
                            This is the path of the core configuration file of the
                            X-Tuner. You can specify the path of the new
                            configuration file. The default path is
                            /path/to/xtuner/xtuner.conf. You can
                            modify the configuration file to control the tuning
                            process.
      -v, --version         show program's version number and exit
    
    Database Connection Information:
      --db-name DB_NAME     The name of database where your workload running on.
      --db-user DB_USER     Use this user to login your database. Note that the
                            user must have sufficient permissions.
      --port PORT           Use this port to connect with the database.
      --host HOST           The IP address of your database installation host.
      --host-user HOST_USER
                            The login user of your database installation host.
      --host-ssh-port HOST_SSH_PORT
                            The SSH port of your database installation host.
                        

### Some examples for three modes
Switch to the tuner directory and run the following example command:

    cd tuner
    export PYTHONPATH='..'  # Set env variable.
    python3 main.py train -f server.json
    python3 main.py tune -f server.json
    python3 main.py recommend -f server.json

Install the X-Tuner and you can run the following example command anywhere:

    gs_xtuner train -f server.json
    gs_xtuner train --port 6789 --db-name tpch --db-user dba --host 10.90.56.172 --host-user omm
    gs_xtuner tune -f server.json
    gs_xtuner recommend -f server.json

A template for server.json:

```json
{
  "db_name": "postgres",
  "db_user": "dba",
  "host": "127.0.0.1",
  "host_user": "dba",
  "port": 5432,
  "ssh_port": 22
}
```

Note: The detailed configurations are configured in the configuration file (the default one is xtuner.conf).

### Watching the log files
1. log/opengauss_tuner.log: The log file records events that occur in runtime. You could locate bugs through it.
2. log/recorder.log: The log file records tuning results. It will report the tuned result for each step and current best knobs.

You could use shell command ```tail -f log/opengauss_tuner.log``` to monitor the tuning process.

# Principle
We have introduced some current tuning approaches and limitations. Now we give a brief introduction to our tuning method for addressing the above-mentioned limitations.

According to a great number of trials on the database, we found that
several knobs have a relation to some states in the database, such as _shared_buffers_ and buffer pool cache hit ratio in the openGauss database. These database internal states can be used in reinforcement learning. Meanwhile, we pass current knobs as a state either and take the benchmark score as a reward.

As for action, we provide two methods which are controlled by ```db_env.step(self, action, is_delta=True)``` at `db_env.py`. If ```is_delta=True```, we regard delta knob value as action. Otherwise, the action is an absolute knob value.
It should be noted that when ```is_delta=False```, the scope of ```action_space``` must be limited in (0, 1) which can be modified in ```self.action_space = Box(low=-1, high=1, shape=(self.nb_actions,), dtype=np.float32)```.
And the activation function of the output layer should be sigmoid for the model `actor` in rl_agent._build_ddpg(nb_actions, nb_states). Based on the above steps, we can gain feedback
when running the benchmark on the new knobs set.

The above steps can be seen as a Markov transfer matrix in the current environment. We can directly use this trained model to tune knobs on other environments that are similar to the current one. However, if the tuning environment is far different, the currently trained model can be also used as the initial parameters on the new tuning process.
Nevertheless, some knobs have no relative internal states, such as random_page_cost, seq_page_cost, etc. Because such knobs are correlative to each other, we use the Global OPtimization algorithm (GOP) to solve this combinatorial optimization problem. Particle Swarm Optimization (PSO) is applied as our heuristic algorithm since its simpleness and stabilization. At the same time, we use Bayesian optimization to solve this problem too.

**For details, see the related sections in the openGauss [online documentation](https://opengauss.org/zh/docs/1.0.1/docs/Quickstart/Quickstart.html).**

# References
1. [Automatic Database Management System Tuning Through Large-scale Machine Learning](http://db.cs.cmu.edu/papers/2017/p1009-van-aken.pdf)
2. [QTune: A Query-Aware Database Tuning System with Deep Reinforcement Learning](https://www.vldb.org/pvldb/vol12/p2118-li.pdf)
3. [An End-to-End Automatic Cloud Database TuningSystem Using Deep Reinforcement Learning](https://dbgroup.cs.tsinghua.edu.cn/ligl/papers/sigmod19-cdbtune.pdf)
