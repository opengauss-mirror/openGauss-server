## Introduction to Sqldiag

**Sqldiag** is a robust forecasting framework that allows a DBMS to predict execute time for unknown workload based on historical data, The prediction is based on similarity of unknown queries and historical queries. We do not use  execute-plan since achieving execute-plan will claim resource on user database, also inapplicable for __OLTP__ workload. 
Framework pipeline is:

```
sqldiag/
├── algorithm
│   ├── diag.py
│   ├── duration_time_model
│   │   ├── dnn.py
│   │   ├── __init__.py
│   │   └── template.py
│   ├── sql_similarity
│   │   ├── __init__.py
│   │   ├── levenshtein.py
│   │   ├── list_distance.py
│   │   └── parse_tree.py
│   └── word2vec.py
├── load_sql_from_wdr.py
├── main.py
├── preprocessing.py
├── README.md
├── requirements.txt
├── result.png
├── sample_data
│   ├── predict.csv
│   └── train.csv
├── sqldiag.conf
└── utils.py
```

* sqldiag.conf : Parameters such as model training path.
* requirements.txt: Dependency package installed using pip.

## Sqldiag Dependencies

    python3.5+
    tensorflow==2.3.1
    pandas
    matplotlib
    gensim==3.8.3
    sqlparse
    sklearn
    numpy

After installing python，run`pip install -r requirements.txt` command to install all required packages.

## Quick guide

## prepare dataset

    # train dataset
    SQL,EXEC_TIME
    
    # predict dataset
    SQL
    
    note: you can use script [load_sqk_from_wdr](load_sql_from_wdr.py) to collect train dataset based on WDR.
    usage: load_sql_from_wdr.py [-h] --port PORT --start_time START_TIME
                            --finish_time FINISH_TIME [--save_path SAVE_PATH]
    example: 
        python load_sql_from_wdr.py --start_time "2021-04-25 00:00:00" --finish_time "2021-04-26 14:00:00" --port 8000
        
*  _EXEC_TIME_: execute time for sql.
*  _SQL_: current query string

train dataset sample: [sample_data/train.csv](data/train.csv)

predict dataset sample: [sample_data/predict.csv](data/predict.csv)

## example for template method using sample dataset 

    # train
    python main.py train -f ./sample_data/train.csv --model template --model-path ./template
    
    # predict
    python main.py predict -f ./sample_data/predict.csv --model template --model-path ./template 
    --predicted-file ./result/t_result

    # predict with threshold
    python main.py predict -f ./sample_data/predict.csv --threshold 0.02 --model template --model-path ./template 
    --predicted-file ./result/t_result
    
    # update model
     python main.py finetune -f ./sample_data/train.csv --model template --model-path ./template 

## example for dnn method using sample dataset 

    # training
    python main.py train -f ./sample_data/train.csv --model dnn --model-path ./dnn_model 
    
    # predict
    python main.py predict -f ./sample_data/predict.csv --model dnn --model-path ./dnn_model 
    --predicted-file ./result/result
    
    # update model
     python main.py finetune -f ./sample_data/train.csv --model dnn --model-path ./dnn_model
