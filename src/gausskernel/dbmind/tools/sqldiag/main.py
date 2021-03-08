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
import stat

from src.file_processor import get_train_dataset, get_test_dataset
from src.sql_template import SqlTemplate
from src.w2vector import Word2Vector

__version__ = '1.0.0'
w2v_model_name = 'w2v.model'
w2v_model_parameter = {'max_len': 300,
                       'sg': 1,
                       'hs': 1,
                       'min_count': 0,
                       'window': 1,
                       'size': 5,
                       'iter': 50,
                       'workers': 4
                       }


def train(template_dir, data):
    w2v_model_path = os.path.join(template_dir, w2v_model_name)
    w2v = Word2Vector(**w2v_model_parameter)
    if not os.path.exists(w2v_model_path):
        w2v.train(sentence=data)
    else:
        w2v.load_model(w2v_model_path)
        w2v.update(sentence=data)
    w2v.save_model(w2v_model_path)
    sql_template = SqlTemplate(template_dir, word2vec_model=w2v)
    sql_template.template = data


def predict(template_dir, data):
    w2v_model_path = os.path.join(template_dir, w2v_model_name)
    w2v = Word2Vector(**w2v_model_parameter)
    w2v.load_model(w2v_model_path)
    sql_template = SqlTemplate(template_dir, word2vec_model=w2v)
    result = sql_template.predict_batch_exec_time(data)
    return result


def parse_args():
    parser = argparse.ArgumentParser(description='sqldiag')
    parser.add_argument('-m', '--mode', required=True, choices=['train', 'predict'])
    parser.add_argument('-f', '--file', required=True)
    parser.version = __version__
    return parser.parse_args()


def main(args):
    mode = args.mode
    filepath = args.file
    template_dir = os.path.realpath('./template')
    if not os.path.exists(template_dir):
        os.makedirs(template_dir, mode=0o700)
    if oct(os.stat(template_dir).st_mode)[-3:] != '700':
        os.chmod(template_dir, stat.S_IRWXU)
    if mode == 'train':
        train_data = get_train_dataset(filepath)
        train(template_dir, train_data)
    if mode == 'predict':
        predict_data = get_test_dataset(filepath)
        result = predict(template_dir, predict_data)
        print(result)


if __name__ == '__main__':
    main(parse_args())
