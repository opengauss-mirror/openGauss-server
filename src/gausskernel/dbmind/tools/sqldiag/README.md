## Introduction to Sqldiag

**Sqldiag** is a robust forecasting framework that allows a DBMS to predict execute time for unknown workload based on 
historical data, The prediction is based on similarity of unknown queries and historical queries. We do not use 
execute-plan since achieving execute-plan will claim resource on user database, also inapplicable for __OLTP__ workload. 
Framework pipeline is:
* prepare train dataset: prepare dataset follow the required format below.
* file_processor: get dataset from csv file.
* sql2vector: train an adaptive model to transform sql to vector.
* sql_template: we divide historical queries into a number of categories based on SQl similarity algorithm.  
* predict: get unknown query and return execute information including 'execute_time', 'cluster id', 'point'.

## Run sqldiag on sample data:

    # train based on train.csv
    python main.py -m train -f data/train.csv
    
    # predict unknown dataset
    python main.py -m predict -f data/predict.csv
    
## Sqldiag Dependencies

    python3.5+
    gensim
    sqlparse
    sklearn
    numpy
    
## Quick guide
## prepare dataset
    # train dataset
    EXEC_TIME,SQL
    
    # predict dataset
    SQL

*  _EXEC_TIME_: execute time for sql.
*  _SQL_: current query string

note: you should separated by commas

train dataset sample: [data/train.csv](data/train.csv)

predict dataset sample: [data/predict.csv](data/predict.csv)

## train based on dataset

    python main.py -m train -f train_file
    
## predict unknown dataset

    python main.py -m predict -f predict_file
