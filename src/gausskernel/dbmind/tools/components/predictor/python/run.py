"""
 openGauss is licensed under Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:

 http://license.coscl.org.cn/MulanPSL2

 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 See the Mulan PSL v2 for more details.
 
 Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 Description: The APIs to call ml functions and reply to the client requests.
"""

import os
import ssl

# Open source libraries
from datetime import datetime
from flask import Flask, request, jsonify
import numpy as np
from werkzeug.utils import secure_filename
import logging.config

from model import ModelInfo, RnnModel
import settings
from certs import aes_cbc_decrypt_with_path

# global variables
os.environ['CUDA_VISIBLE_DEVICES'] = settings.GPU_CONFIG
app = Flask(__name__)
loaded_model = None
model_config = None
req_logger = None
model_logger = None
parse_logger = None
tb_url = None


def __get_flask_params__():
    '''
    Returns the connection parameters of the Flask server app
    :return: tuple of debug, server host and server port number
    '''
    server_debug = int(settings.DEFAULT_FLASK_DEBUG)
    server_host = settings.DEFAULT_FLASK_SERVER_HOST
    server_port = int(settings.DEFAULT_FLASK_SERVER_PORT)
    return server_debug, server_host, server_port


def __port_in_use__(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


@app.route('/check', methods=['POST'])
def check():
    global req_logger
    """
    API for check the
    :return:
    """
    if request.method == 'POST':
        req_logger.info(request.data)
        return '0'


@app.route('/configure', methods=['POST'])
def configure_training():
    '''
    API for configuring model, needs to be called before prediction or train
    CURL format:
        curl -X POST -d '{"max_epoch":"200", "learning_rate":"0.01", "hidden_units":"60", "batch_size": "1000",
        "dim_red": "0.7","model_name":"rlstm"}' -H 'Content-Type: application/json' 'https://127.0.0.1:5000/configure'
    JSON Parameters:
        'max_epoch':        [OPTIONAL] default 500
        'learning_rate':    [OPTIONAL] default 1
        'hidden_units':     [OPTIONAL] default 50
        'batch_size':       [OPTIONAL] default 5000
        'template_name'     [OPTIONAL] network type of the target model, default rlstm
                            model
        'model_name':       [COMPULSORY] model name to be saved, can be a already trained model
        'labels':           [COMPULSORY] target labels to predict
        'dim_red':          [OPTIONAL] part of variance explained by PCA, default -1 means no PCA
    :return:    0:  Success
                F:  TypeError
                I:  Invalid parameter type
                M:  Missing compulsory argument
    '''
    global model_config
    global req_logger
    global model_logger
    global tb_url
    if request.method == 'POST':
        req_logger.info(request.data)
        arg_json = request.get_json()
        if 'model_name' in arg_json:
            model_name = arg_json['model_name']
            model_config = ModelInfo(model_name)
        else:
            return 'M'
        return model_config.get_info(arg_json)


@app.route('/train', methods=['GET', 'POST'])
def train():
    '''
    API for training the model, should be called after configuration
    CURL format:
        curl -X POST -F file=@/path/to/encoded/data 'https://127.0.0.1:5000/train'
    :return: a jsonified result
        {
            'final_model_name': 'xxx',
            're_startup': 'xxx',
            're_total': 'xxx',
            'converged': '0/1',
            'feature_length': '???'
        }
        Errors:     'M': Missing compulsory parameter in json
        Errors:     'R': session is running
    '''
    global running
    global model_config
    global req_logger
    global model_logger
    if request.method == 'POST':
        if running == 1:
            return 'R'
        running = 1
        # save file
        if 'file' in request.files:
            f = request.files['file']
        else:
            return 'M'
        base_path = os.path.dirname(__file__)
        dtObj = datetime.now()
        fname = str(dtObj.year) + '-' + str(dtObj.month) + '-' + str(dtObj.day) + '_' \
                + str(dtObj.hour) + '-' + str(dtObj.minute) + '-' + str(dtObj.second) + '-' \
                + secure_filename(f.filename)
        file_path = os.path.realpath(os.path.join(
            base_path, settings.PATH_UPLOAD, fname))
        f.save(file_path)
        # trigger training
        try:
            model = RnnModel(model_config)
            val_re = model.fit(file_path)
        except:
            running = 0
            raise
        re_startup, re_total, re_row, re_mem = -1, -1, -1, -1
        converged = 1
        for v in val_re:
            if v > 2:
                converged = 0
                break
        for i in range(int(model_config.label_length)):
            if model_config.model_targets[i] == 'S':
                re_startup = val_re[i]
            elif model_config.model_targets[i] == 'T':
                re_total = val_re[i]
            elif model_config.model_targets[i] == 'R':
                re_row = val_re[i]
            elif model_config.model_targets[i] == 'M':
                re_mem = val_re[i]
        res = {
            're_startup': re_startup,
            're_total': re_total,
            're_row': re_row,
            're_mem': re_mem,
            'max_startup': float(model_config.max_startup),
            'max_total': float(model_config.max_total),
            'max_row': float(model_config.max_row),
            'max_mem': float(model_config.max_mem),
            'converged': converged,
            'feature_length': int(model_config.feature_length)
        }

        running = 0
        model_logger.info(jsonify(res))
        return jsonify(res)


@app.route('/track_process', methods=['POST'])
def track_process():
    '''
    return the log file path that records the model's training process information
    CURL format:
        curl -X POST -d '{"modelName":"test"}' -H 'Content-Type: application/json'
        'https://127.0.0.1:5000/track_process'

    :return:    log_path if the training log exists
                F if the log file has not been generated or contents nothing
                M if missing compulsory parameter
    '''
    global tb_url
    if request.method == 'POST':
        req_logger.info(request.data)
        arg_json = request.get_json()
        if 'modelName' in arg_json:
            model_name = arg_json['modelName']
            base_path = os.path.dirname(__file__)
            log_path = os.path.realpath(os.path.join(base_path, settings.PATH_LOG, model_name + '_log.json'))
            if not (os.path.exists(log_path) and os.path.getsize(log_path)):
                return 'F'
            else:
                return log_path
        else:
            return 'M'


@app.route('/model_setup', methods=['POST'])
def setup():
    '''
    API for setup up the model for prediction.
    CURL format:
        curl -X POST -d '{"model_name": "rlstm"}' -H 'Content-Type: application/json'
        'https://127.0.0.1:5000/model_setup'
    JSON Parameter:
        'model_name':   [COMPULSORY] name of the model to be activated for predict route
    :return:    M:  Missing compulsory parameter in json
                i:  Internal error when loading
                N:  Model not found
    '''
    global loaded_model
    global running
    global req_logger
    global model_logger
    if request.method == 'POST':
        req_logger.info('request for setup is {}'.format(request.data))
        if running == 1:
            return 'R'
        running = 1
        arg_json = request.get_json()
        if 'model_name' in arg_json:
            model_name = arg_json['model_name']
            model_config = ModelInfo(model_name)
            try:
                loaded_model = RnnModel(model_config)
                loaded_model.load()
            except KeyError:
                running = 0
                return 'N'
            except FileNotFoundError:
                running = 0
                return 'N'
            except:
                running = 0
                return 'i'
        else:
            running = 0
            return 'M'
        return '0'


@app.route('/predict', methods=['POST'])
def predict():
    '''
    Route for prediction, should be called after setup to choose the model to predict
    CURL format:
        curl -X POST -F file=@/path/to/encoded/data 'https://127.0.0.1:5000/predict'
    :return: a jsonified result
        {
            'pred_startup': 'xxx',
            'pred_total': 'xxx',
            'successful': '0/1'
        }

        Failures:   'M':    Missing compulsory parameter in json
                    'S':    Session is not loaded, setup required
    '''
    global running
    global loaded_model
    global req_logger
    global model_logger
    if request.method == 'POST':
        if not (loaded_model and running == 1):
            model_logger.error('model is not loaded or running is %d' % running)
            return 'S'
        if 'file' in request.files:
            f = request.files['file']
        else:
            return 'M'
        base_path = os.path.dirname(__file__)
        dtObj = datetime.now()
        fname = str(dtObj.year) + '-' + str(dtObj.month) + '-' + str(dtObj.day) + '_' \
                + str(dtObj.hour) + '-' + str(dtObj.minute) + '-' + str(dtObj.second) + '-' \
                + 'tmp.csv'
        file_path = os.path.realpath(os.path.join(
            base_path, settings.PATH_UPLOAD, fname))
        f.save(file_path)
        # trigger prediction
        try:
            pred = loaded_model.predict(file_path)
        except:
            model_logger.error('Model prediction failed.')
            running = 0
            os.remove(file_path)
            raise
        pred_startup, pred_total, pred_row, pred_mem = None, None, None, None
        info = loaded_model.model_info
        for i in range(len(info.model_targets)):
            if info.model_targets[i] == 'S':
                pred_startup = pred[:, i] * info.max_startup
                pred_startup = str(list(pred_startup.astype(int)))[1:-1]
            elif info.model_targets[i] == 'T':
                pred_total = pred[:, i] * info.max_total
                pred_total = str(list(pred_total.astype(int)))[1:-1]
            elif info.model_targets[i] == 'R':
                pred_row = np.exp(pred[:, i] * info.max_row) - 1
                pred_row = str(list(pred_row.astype(int)))[1:-1]
            elif info.model_targets[i] == 'M':
                pred_mem = pred[:, i] * info.max_mem
                pred_mem = str(list(pred_mem.astype(int)))[1:-1]
        res = {
            'pred_startup': pred_startup,
            'pred_total': pred_total,
            'pred_rows': pred_row,
            'pred_mem': pred_mem
        }
        model_logger.debug(jsonify(res))
        os.remove(file_path)
        running = 0
        return jsonify(res)

def run():
    global running
    global req_logger
    global model_logger
    global parse_logger
    running = 0
    logging.config.fileConfig('log.conf')
    req_logger = logging.getLogger()
    model_logger = logging.getLogger('model')
    parse_logger = logging.getLogger('parse')
    key = aes_cbc_decrypt_with_path(settings.PATH_SSL)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=settings.PATH_SERVER_CRT, keyfile=settings.PATH_SERVER_KEY, password=key)
    context.load_verify_locations(settings.PATH_CA)
    context.verify_mode = ssl.CERT_REQUIRED
    server_debug, server_host, server_port = __get_flask_params__()
    app.run(host=server_host, port=server_port, debug=server_debug, threaded=True, ssl_context=context)
    exit(0)


if __name__ == '__main__':
    run()
