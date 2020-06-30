[TOC]


# X-Tuner
Knob tuning is extremely important to database on achieving high performance. Unfortunately, knob tuning task is an NP-hard problem. Generally speaking, tuning depends on the one's experience and understanding on current system, also contains great uncertainties about performance. Hence some engineers attempt to build intelligent system with automatic tuning, such as **postgresqltuner.pl** and
**mysqltuner.pl**. Also, with the rising of AI technology, knob tuning problem has some ai-based solutions, such like **OtterTune**.

However, current tuning approaches have several limitations:
* DBAs cannot tune too many distinct
instances on different environments (e.g., different database vendors);
* traditional machine-learning
methods either cannot find optimal configurations, or relys on excessive high-quality data sets which are rather hard to obtain;
* some reinforcement learning methods lose sight of less relations between
state(database status) and action(knob) while tuning knobs;

Thus, we want to implement robust system **X-Tuner** and try to avoid above limitations.
**X-Tuner** is a component of _OpenGauss_, implemented with deep reinforcement 
learning and heuristic algorithm. **X-Tuner** is also a **db-tuner framework** that you can develop your own tuning method on it far from being only a tuning tool.

# How to use
## Installation

```shell
pip install -r requirements.txt
```

## Dependencies

    python3.5+
    tensorflow==1.15.2
    keras-rl
    keras
    paramiko

## Start tuning
1. Start your database instance first;
2. Choose a benchmark(TPC-C, TPC-H, etc.) and store data in your database;
3. **Write your own benchmark script**. There are some demos in tuner/benchmark.
Maybe you can directly use some benchmark scripts. And if you have your
own scenarios, you can code here, please see [benchmark readme](tuner/benchmark/Readme.md);
4. Modify some knobs that you will tune, filepath is
tuner/knobs/xxx.py, default knob configuration file is **knobs_htap.py**;
5. Let's start a tuning journey, you can do as the following usage. Please note that current working directory is tuner.


    python main.py [-h] [-m {train,tune}] [-f CONFIG_FILE] [--db-name DB_NAME]
                   [--db-user DB_USER] [--port PORT]
                   [--host HOST] [--host-user HOST_USER]
                   [--host-ssh-port HOST_SSH_PORT]
                   [--scenario {ap,tp,htap}] [--benchmark BENCHMARK]
                   [--model-path MODEL_PATH] [-v]


## Demos
### Training case 1: shell interactive

```
cd tuner
python main.py -m train --db-name postgres\
               --db-user dba\
               --port 1234 --host 192.168.1.2 --host-user opengauss\
               --benchmark tpcc --model-path my_model --scenario tp
```

### Training case 2: load configuration file
Defined a configuration file named dbinfo.json:
```json
{
  "db_name": "postgres",
  "db_user": "dba",
  "db_user_pwd": "Tuning@123",
  "port": 1234,
  "host": "192.168.1.2",
  "host_user": "opengauss",
  "host_user_pwd": "opengauss"
}
```

Run shell command to train a model:
```
cd tuner
python main.py -f dbinfo.json --benchmark tpcc \
               --model-path /home/opengauss/rlmodel.hdf5 \
               -m train --scenario tp 
```

### Tuning case:
```
cd tuner
python main.py -m tune --db-name postgres\
               --db-user dba \
               --port 1234 --host 192.168.1.2 --host-user opengauss\
               --benchmark tpcc --model-path my_model --scenario tp
```
**tip**: you should make sure model-path file exists. And the host user must have `GAUSSHOME` enviroment variable. Otherwise, we could not find `gsql` command while tuning.

### Watching the log files
1. log/opengauss_tuner.log: The log file records events that occur in runtime. You could locate bugs through it.
2. log/recorder.log: The log file records tuning results. It will report tuned result for each step and current optimal knobs.

You could use shell command ```tail -f log/opengauss_tuner.log``` to monitor the tuning process.

# Principle
We have introduced some current tuning approaches and limitations. Now we give a brief introduction about our tuning method on addressing above-mentioned limitations.

According to a great number of trials on database, we found that 
several knobs have relation to some states in database, such as _shared_buffers_ and _buffer pool cache hit ratio_ 
in OpenGauss database. These database internal states can be used in reinforcement learning. Meanwhile, we pass current knobs as state either, and take benchmark score as reward. 
As for action, we provide two methods which are controlled by ```db_env.step(self, action, is_delta=True)``` at `db_env.py`. If ```is_delta=True```, we regard delta knob value as action. Otherwise the action is absolute knob value.
It should be noted that when ```is_delta=False```, the scope of ```action_space``` must be limited in (0, 1) which can be modified in ```self.action_space = Box(low=-1, high=1, shape=(self.nb_actions,), dtype=np.float32)```.
And the activation function of output layer should be **sigmoid** for model **actor** in ```rl_agent._build_ddpg(nb_actions, nb_states)```. Based on above steps, we can gain a feedback
when running benchmark on new knobs set.

Above steps can be seen as a **Markvo transfer matrix** in current
 environment. We can directly use this trained model to tune knobs on other environments that are similar to current one. However, if the tuning environment is far different, current trained model can be also used as the initial parameters on new tuning process.

Nevertheless, some knobs have no relative internal states, such as _random_page_cost_, _seq_page_cost_, etc. Because such knobs are correlative each other, we use heuristic algorithm to solve this
  *combinatorial optimization problem*. Particle Swarm Optimization (PSO) is applied as our heuristic algorithm  
  since its simpleness and stabilization.
  **For details on how to use query information, refer to our paper [2].**

# References
1. [Automatic Database Management System Tuning Through Large-scale Machine Learning](http://db.cs.cmu.edu/papers/2017/p1009-van-aken.pdf)
2. [QTune: A Query-Aware Database Tuning System with Deep Reinforcement Learning](https://www.vldb.org/pvldb/vol12/p2118-li.pdf)
3. [An End-to-End Automatic Cloud Database TuningSystem Using Deep Reinforcement Learning](https://dbgroup.cs.tsinghua.edu.cn/ligl/papers/sigmod19-cdbtune.pdf)
