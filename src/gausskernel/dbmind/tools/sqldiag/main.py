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
import logging
import sys
from configparser import ConfigParser

from algorithm.diag import SQLDiag
from utils import ResultSaver

__version__ = '2.0.0'
__description__ = 'SQLdiag integrated by openGauss.'


def parse_args():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('mode', choices=['train', 'predict', 'finetune'],
                        help='The training mode is to perform feature extraction and '
                             'model training based on historical SQL statements. '
                             'The prediction mode is to predict the execution time of '
                             'a new SQL statement through the trained model.')
    parser.add_argument('-f', '--csv-file', type=argparse.FileType('r'),
                        help='The data set for training or prediction. '
                             'The file format is CSV. '
                             'If it is two columns, the format is (SQL statement, duration time). '
                             'If it is three columns, '
                             'the format is (timestamp of SQL statement execution time, SQL statement, duration time).')
    parser.add_argument('--predicted-file', help='The file path to save the predicted result.')
    parser.add_argument('--model', default='template', choices=['template', 'dnn'],
                        help='Choose the model model to use.')
    parser.add_argument('--model-path', required=True,
                        help='The storage path of the model file, used to read or save the model file.')
    parser.add_argument('--config-file', default='sqldiag.conf')
    parser.version = __version__
    return parser.parse_args()


def get_config(filepath):
    cp = ConfigParser()
    cp.read(filepath, encoding='UTF-8')
    return cp


def main(args):
    logging.basicConfig(level=logging.INFO)
    model = SQLDiag(args.model, args.csv_file, get_config(args.config_file))
    if args.mode == 'train':
        model.fit()
        model.save(args.model_path)
    elif args.mode == 'predict':
        if not args.predicted_file:
            logging.error("The [--predicted-file] parameter is required for predict mode")
            sys.exit(1)
        model.load(args.model_path)
        pred_result = model.transform()
        ResultSaver().save(pred_result, args.predicted_file)
        logging.info('predicted result in saved in {}'.format(args.predicted_file))
    elif args.mode == 'finetune':
        model.fine_tune(args.model_path)
        model.save(args.model_path)


if __name__ == '__main__':
    main(parse_args())
