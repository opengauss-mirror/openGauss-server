"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
import argparse
import os
import re
from cluster import Cluster, get_topn_sql
from pre_process import Preprocessor
from file_processor import get_train_dataset, get_test_dataset


def check_dir(dirpath):
    # check model_dir factor
    if not dirpath:
        dirpath = '.'
    if dirpath[-1] == '/':
        dirpath = dirpath[:-1]
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, mode=0o700)
    dirpath += '/'
    return dirpath


def check_validity(name):
    PATH_CHECK_LIST = [" ","|",";","&","$","<",">","`","\\","'","\"","{","}","(",")","[","]","~","*","?","!","\n"]
    if(name.strip() == ""):
        return
    for rac in PATH_CHECK_LIST:
        flag = name.find(rac)
        if flag >= 0:
            raise ExecutionError
        else:
            continue

def check_file(filepath):
    if not filepath:
        raise ValueError('Bad parameters!')
    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath + ' not exists!')


def train(logpath, modeldir, batch_size=256, epochs=100):
    modelpath = modeldir + 'model.h5'
    dictpath = modeldir + 'word_dict.json'
    for filepath in [logpath, modelpath, dictpath]:
        check_validity(filepath)
    check_file(logpath)
    # load data
    train_data = get_train_dataset(logpath)

    # pre-process
    from autoencoder import AutoEncoder  # lazy load

    pre_processor = Preprocessor(filepath=dictpath)
    train_sr, time_sr = pre_processor.pre_process(train_data)
    autoencoder = AutoEncoder(shape=(train_sr.shape[1], train_sr.shape[2]), filepath=modelpath)
    cluster_model = Cluster(dirpath=modeldir)

    # train
    autoencoder.fit(train_sr, batch_size=batch_size, epochs=epochs)
    train_vector = autoencoder.transfer(train_sr)
    predict_result, cluster_number, dist_tbl = cluster_model.classify(train_vector)
    top_index = get_topn_sql(dist_tbl, topn=1)
    topn_sql = train_data[top_index][:, -1]  # typical SQL template for each cluster
    cluster_model.get_cluster_info(predict_result, time_sr, cluster_number)
    print("Train complete!")
    return cluster_number, topn_sql


def predict(querypath, modeldir, ratio):
    modelpath = modeldir + 'model.h5'
    dictpath = modeldir + 'word_dict.json'
    clusterpath = modeldir + 'Kmeans_model.pkl'
    infopath = modeldir + 'cluster_info.json'
    for filepath in [querypath, modelpath, dictpath, clusterpath, infopath]:
        check_validity(filepath)
        check_file(filepath)
    # load data
    predict_data = get_test_dataset(querypath)

    # predict
    from autoencoder import AutoEncoder  # lazy load

    pre_processor = Preprocessor(alpha=ratio, filepath=dictpath)
    predict_sr = pre_processor.transform(predict_data)
    autoencoder = AutoEncoder(shape=(predict_sr.shape[1], predict_sr.shape[2]), filepath=modelpath)
    cluster_model = Cluster(dirpath=modeldir)
    predict_vector = autoencoder.transfer(predict_sr)
    result = cluster_model.predict(predict_vector)
    print('Predict result is: ')
    print(result)
    return result


def main(method, train_dir, model_dir, predict_dir, ratio):
    model_dir = check_dir(model_dir)

    # full process
    if method == 'all':
        train(train_dir, model_dir)
        predict(predict_dir, model_dir, ratio)
    # train-only
    elif method == 'train':
        train(train_dir, model_dir)
    # predict-only
    elif method == 'predict':
        predict(predict_dir, model_dir, ratio)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Slow SQL Diagnose')
    parser.add_argument('method', choices=['all', 'train', 'predict'], help='Execution style')
    parser.add_argument('--train', help='History Log Data Directory')
    parser.add_argument('--model', help='Output data directory')
    parser.add_argument('--predict', help='To-be-predicted workload data directory')
    parser.add_argument('--ratio', type=float, default=0.5, help='Ratio threshold for retrain recommend')
    args = vars(parser.parse_args())
    main(args['method'], args['train'], args['model'], args['predict'], args['ratio'])
