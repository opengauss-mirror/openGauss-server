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

from .algorithm.diag import SQLDiag
from .utils import ResultSaver, is_valid_conf
from .preprocessing import LoadData, split_sql

__version__ = '2.0.0'
__description__ = 'SQLdiag integrated by openGauss.'


def parse_args(argv):
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
    parser.add_argument('--query', help='Input the querys to predict.')
    parser.add_argument('--threshold', help='Slow SQL threshold.')
    parser.add_argument('--model-path', required=True,
                        help='The storage path of the model file, used to read or save the model file.')
    parser.add_argument('--config-file', default='sqldiag.conf')
    parser.version = __version__
    return parser.parse_args(argv)


def get_config(filepath):
    cp = ConfigParser()
    cp.read(filepath, encoding='UTF-8')
    return cp


def main(argv):
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    if not is_valid_conf(args.config_file):
        logging.fatal('The [--config-file] parameter is incorrect')
        sys.exit(1)

    model = SQLDiag(args.model, get_config(args.config_file))
    if args.mode in ('train', 'finetune'):
        if not args.csv_file:
            logging.fatal('The [--csv-file] parameter is required for train mode')
            sys.exit(1)
        train_data = LoadData(args.csv_file).train_data
        if args.mode == 'train':
            model.fit(train_data)
        else:
            model.fine_tune(args.model_path, train_data)
        model.save(args.model_path)
    else:
        model.load(args.model_path)
        if args.csv_file and not args.query:
            predict_data = LoadData(args.csv_file).predict_data
        elif args.query and not args.csv_file:
            predict_data = split_sql(args.query)
        else:
            logging.error('The predict model only supports [--csv-file] or [--query] at the same time.')
            sys.exit(1)
        args.threshold = -100 if not args.threshold else float(args.threshold)
        pred_result = model.transform(predict_data)
        if args.predicted_file:
            if args.model == 'template':
                info_sum = []
                for stats, _info in pred_result.items():
                    if _info:
                        _info = list(filter(lambda item: item[1]>=args.threshold, _info))
                        for item in _info:
                            item.insert(1, stats)
                        info_sum.extend(_info)
                ResultSaver().save(info_sum, args.predicted_file)
            else:
                pred_result = list(filter(lambda item: float(item[1])>=args.threshold, pred_result))
                ResultSaver().save(pred_result, args.predicted_file)
        else:
            from prettytable import PrettyTable

            display_table = PrettyTable()
            if args.model == 'template':
                display_table.field_names = ['sql', 'status', 'predicted time', 'most similar template']
                display_table.align = 'l'
                status = ('Suspect illegal SQL', 'No SQL information', 'No SQL template found', 'Fine match')
                for stats in status:
                    if pred_result[stats]:
                        for sql, predicted_time, similariest_sql in pred_result[stats]:
                            if predicted_time >= args.threshold or stats == 'Suspect illegal sql':
                                display_table.add_row([sql, stats, predicted_time, similariest_sql])
            else:
                display_table.field_names = ['sql', 'predicted time']
                display_table.align = 'l'
                for sql, predicted_time in pred_result:
                    if float(predicted_time) >= args.threshold:
                        display_table.add_row([sql, predicted_time])
            print(display_table.get_string())


if __name__ == '__main__':
    main(sys.argv[1:])
