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
 Description: The general utilities and APIs of machine learning models.
"""

import os
import pickle

import ast
from keras.backend.tensorflow_backend import set_session
import tensorflow as tf
import keras
import time
from keras import backend as K
from keras.models import load_model, Sequential
from sklearn.model_selection import train_test_split
from keras.layers import LSTM, Dense, CuDNNLSTM
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
import shutil
from keras.preprocessing import sequence
from keras.callbacks import Callback
import logging.config

import settings

class LossHistory(Callback):
    """
    This function recods the training process to the target log file.
    """
    def __init__(self, log_path, model_name, max_epoch):
        self.log_path = log_path
        self.model_name = model_name
        self.max_epoch = max_epoch
    def on_train_begin(self, logs={}):
        self.losses = []
        self.val_acc = []
    def on_epoch_end(self, epoch, logs={}):
        now = time.time()
        local_time = time.localtime(now)
        self.losses.append(logs.get('loss'))
        if epoch % 100 == 0:
            json_log = open(self.log_path, mode='at', buffering=1)
            json_log.write(time.strftime('%Y-%m-%d %H:%M:%S', local_time) +
                           ' [TRAINING] [MODEL]: %s  [Epoch]: %d/%d  [Loss]: %.2f' %
                           (self.model_name, epoch, self.max_epoch, logs['loss']) + '\n')
            json_log.close()

class FeatureParser():
    """
    This is the feature_parser class for AI Engine, includes the methods to parse encoded file.
    """

    def __init__(self, model_info, filename):
        """
        The file should be in the format of <query_id>, <plan_node_id>, <parent_node_id>, <enc>, <startup_time>,
        <total_time>, <actual_rows> for <enc>, it should be an list of encoded feature with fixed length, Which
        must be ensured by the backend side.
        'dim_red': the 'n_components' of PCA.
        'filename': the path of target file to parse.
        'model_name': the model that the parser adapted to
        """
        self.dim_red = round(float(model_info.dim_red), 2)
        self.filename = filename
        self.model_name = model_info.model_name
        logging.config.fileConfig('log.conf')
        self.parse_logger = logging.getLogger('parse')

    def parse(self, is_train=True):
        try:
            df_tmp = pd.read_csv(self.filename, header=None,
                                 names=["query_id", "plan_node_id", "parent_node_id", "enc",
                                        "startup_time", "total_time", "actual_rows", "peak_mem"], index_col=False)

            df = df_tmp.sort_values(by=['query_id', 'plan_node_id'])
            df.reset_index(drop=True, inplace=True)
            enc_arr = np.array([list(map(float, df['enc'].values[i].split())) for i in range(len(df))])
            df['enc'] = list(enc_arr)

        except FileNotFoundError:
            self.parse_logger.error('The encoding file is not found.')
            raise
        except KeyError:
            self.parse_logger.error('Missing compulsory encoding information.')
            raise
        except:
            raise
        if self.dim_red > 0:
            path_pca_model = os.path.realpath(
                os.path.join(settings.PATH_MODELS, self.model_name, self.model_name + '.pkl'))
            if is_train:
                try:
                    reload_pca = open(path_pca_model, 'rb')
                    dim_reducer = pickle.load(reload_pca)
                    reload_pca.close()
                    reduced = dim_reducer.transform(enc_arr)
                    df['enc'] = list(reduced)
                except:
                    dim_reducer = PCA(self.dim_red, svd_solver='full')
                    dim_reducer.fit(enc_arr)
                    reduced = dim_reducer.transform(enc_arr)
                    df['enc'] = list(reduced)
                    self.parse_logger.debug('[reduce ratio]:{}'.format(self.dim_red))
                    self.parse_logger.debug('[PCA] n_dim:{}'.format(dim_reducer.n_components_))
                    self.parse_logger.debug('[PCA] explained:{}'.format(np.sum(dim_reducer.explained_variance_ratio_)))
                if not os.path.exists(path_pca_model):
                    os.mknod(path_pca_model, 0o600)
                pca_to_save = open(path_pca_model, 'wb')
                pickle.dump(dim_reducer, pca_to_save)
                pca_to_save.close()
            else:
                pred_reload_pca = open(path_pca_model, 'rb')
                dim_reducer = pickle.load(pred_reload_pca)
                pred_reload_pca.close()
                reduced = dim_reducer.transform(enc_arr)
                df['enc'] = list(reduced)
        df.sort_values(inplace=True, by=["query_id", "plan_node_id"])
        feature_length = len(df.iloc[0]['enc'])
        arr_enc = []
        arr_child = []
        arr_startup = []
        arr_total = []
        arr_rows = []
        arr_mem = []
        indx = np.loadtxt(self.filename, delimiter=",", usecols=(0, 1, 2), dtype=np.int)
        children = [[] for _ in range(len(df))]
        base = 0
        prev = 0
        for index, row in df.iterrows():
            if prev != row.query_id:
                base = index
                prev = row.query_id
            if row.parent_node_id != 0:
                (children[base + row.parent_node_id - 1]).append(row.plan_node_id)
        df["children"] = children
        for i in indx:
            qid = i[0]
            nid = i[1]
            enc = []
            child = []
            serial = df[(df.query_id == qid) & (df.plan_node_id == nid)]
            arr_startup.append(serial.startup_time.values[0])
            arr_total.append(serial.total_time.values[0])
            arr_rows.append(serial.actual_rows.values[0])
            arr_mem.append(serial.peak_mem.values[0])
            self.gen_data(df, qid, nid, enc, child, nid)
            arr_enc.append(enc)
            arr_child.append(child)
        return feature_length, arr_enc, arr_child, arr_startup, arr_total, arr_rows, arr_mem

    def gen_data(self, df, qid, nid, enc, child, base_nid):
        serial = df[(df.query_id == qid) & (df.plan_node_id == nid)]
        try:
            enc.append(serial.enc.values[0])
        except:
            self.parse_logger.error('Failed to parse encoding information.')
            raise
        child_list_tmp = serial.children.tolist()[0]
        child_list = [x - base_nid for x in child_list_tmp]
        child.append(child_list)
        for child_id in child_list_tmp:
            self.gen_data(df, qid, child_id, enc, child, base_nid)



class ModelInfo:
    """ This is model_info class that keeps the parameters about the model configuration

        'max_epoch':        [OPTIONAL] default 500
        'learning_rate':    [OPTIONAL] default 1
        'hidden_units':     [OPTIONAL] default 50
        'batch_size':       [OPTIONAL] default 5000
        'model_name':       [COMPULSORY] model name to be saved, can be a already trained model
        'dim_red':          [OPTIONAL] part of variance explained by PCA, default 0 means no PCA
        'model_targets':    [COMPULSORY] target labels to predict
    """

    def __init__(self, model_name):
        self.model_name = model_name
        self.max_epoch = 500
        self.learning_rate = 1
        self.hidden_units = 50
        self.batch_size = 500
        self.dim_red = -1
        self.model_targets = ''
        self.model_dir = os.path.realpath(os.path.join(settings.PATH_MODELS_INFO, self.model_name))
        self.conf_path = os.path.realpath(os.path.join(self.model_dir, self.model_name + '.conf'))
        self.model_path = os.path.realpath(os.path.join(self.model_dir, self.model_name + '.h5'))
        self.feature_length = None
        self.label_length = None
        self.max_startup = None
        self.max_total = None
        self.max_row = None
        self.max_mem = None
        self.last_epoch = None
        logging.config.fileConfig('log.conf')
        self.model_logger = logging.getLogger('model')

    def get_info(self, arg_json):
        """
        get the model information from curl request and update the config parameters
        :param arg_json: the json format of received curl request
        :return: 0:  Success
                 F:  TypeError
                 I:  Invalid parameter type
                 M:  Missing compulsory argument
        """
        if 'labels' in arg_json.keys():
            self.model_targets = str(arg_json['labels'])
        else:
            return 'M'
        for key in arg_json.keys():
            if key == 'max_epoch':
                try:
                    max_epoch = int(arg_json[key])
                    if max_epoch <= 0:
                        return 'F'
                    self.max_epoch = max_epoch
                except TypeError:
                    return 'F'
            elif key == 'model_name':
                self.model_name = str(arg_json['model_name'])
            elif key == 'learning_rate':
                try:
                    learning_rate = round(float(arg_json[key]), 2)
                    if learning_rate <= 0:
                        return 'F'
                    self.learning_rate = learning_rate
                except TypeError:
                    return 'F'
            elif key == 'hidden_units':
                try:
                    hidden_units = int(arg_json[key])
                    if hidden_units <= 0:
                        return 'F'
                    self.hidden_units = hidden_units
                except TypeError:
                    return 'F'
            elif key == 'batch_size':
                try:
                    batch_size = int(arg_json[key])
                    if batch_size <= 0:
                        return 'F'
                    self.batch_size = batch_size
                except TypeError:
                    return 'F'
            elif key == 'labels':
                tmp_targets = arg_json[key]
                if len(tmp_targets) != len(set(tmp_targets)):
                    return 'F'
                checklist = ['S', 'T', 'R', 'M']
                model_targets = ''
                for i in checklist:
                    if i in tmp_targets:
                        model_targets += i
                self.model_targets = model_targets
                self.label_length = len(model_targets)
            elif key == 'dim_red':
                try:
                    dim_red = round(float(arg_json[key]), 2)
                    if dim_red <= 0 and dim_red != -1:
                        return 'F'
                    self.dim_red = dim_red
                except TypeError:
                    return 'F'
            elif key == 'template_name':
                if arg_json[key] != 'rlstm':
                    return 'F'
            else:
                return 'I'
        if os.path.exists(self.conf_path) and os.path.getsize(self.conf_path):
            self.update_info()
        else:
            self.dump_dict()
        return '0'

    def update_info(self):
        params_ = self.load_dict(self.conf_path)
        self.feature_length = params_['feature_length']
        self.max_startup = params_['max_startup']
        self.max_total = params_['max_total']
        self.max_row = params_['max_total']
        self.max_mem = params_['max_mem']
        self.last_epoch = params_['last_epoch']
        if self.check_params():
            return self.dump_dict()
        else:
            return False

    def to_dict(self):
        params_dict = {}
        try:
            params_dict['model_name'] = self.model_name
            params_dict['max_epoch'] = self.max_epoch
            params_dict['learning_rate'] = self.learning_rate
            params_dict['hidden_units'] = self.hidden_units
            params_dict['batch_size'] = self.batch_size
            params_dict['dim_red'] = self.dim_red
            params_dict['model_targets'] = self.model_targets
            params_dict['label_length'] = self.label_length
            params_dict['max_startup'] = self.max_startup
            params_dict['max_total'] = self.max_total
            params_dict['max_row'] = self.max_row
            params_dict['max_mem'] = self.max_mem
            params_dict['last_epoch'] = self.last_epoch
            params_dict['model_path'] = self.model_path
            params_dict['conf_path'] = self.conf_path
            params_dict['model_dir'] = self.model_dir
            params_dict['feature_length'] = self.feature_length
            self.model_logger.info(params_dict)
        except ValueError:
            self.model_logger.error('Model Info ERROR: missing compulsory parameter.')
            raise
        except:
            raise
        return params_dict

    def dump_dict(self):
        """
        save model information
        :return:
        """
        params_dict = self.to_dict()
        if self.configure_check(params_dict):
            if not os.path.exists(self.conf_path):
                os.mknod(self.conf_path, 0o600)
            with open(self.conf_path, 'w') as cf:
                cf.write(str(params_dict))
            return True
        else:
            return False

    def load_dict(self, conf_path):
        """
        load the model information
        :param conf_path: path to model configurations
        :return: model_infor in dictionary format
        """
        with open(conf_path, 'r') as conf:
            params_dict = ast.literal_eval(conf.read())
            return params_dict

    def configure_check(self, params_dict):
        '''
        To determine whether the model needs to be re-initialized
        :param model_name: name of the model
        :return:
        '''
        if not os.path.isdir(self.model_dir):
            os.makedirs(self.model_dir, mode=0o700)
            return True
        elif not os.path.exists(self.conf_path):
            if os.path.getsize(self.conf_path):
                return True
        else:
            saved_conf = self.load_dict(self.conf_path)
            checklist = ['dim_red', 'model_targets']
            for item in checklist:
                if str(params_dict[item]) != str(saved_conf[item]):
                    return False
            return True

    def load_info(self):
        params_dict = self.load_dict(self.conf_path)
        try:
            self.model_name = params_dict['model_name']
            self.max_epoch = params_dict['max_epoch']
            self.learning_rate = params_dict['learning_rate']
            self.hidden_units = params_dict['hidden_units']
            self.batch_size = params_dict['batch_size']
            self.dim_red = params_dict['dim_red']
            self.feature_length = params_dict['feature_length']
            self.label_length = params_dict['label_length']
            self.max_startup = params_dict['max_startup']
            self.max_total = params_dict['max_total']
            self.max_row = params_dict['max_row']
            self.max_mem = params_dict['max_mem']
            self.model_targets = params_dict['model_targets']
            self.last_epoch = params_dict['last_epoch']
            self.model_path = params_dict['model_path']
            self.conf_path = params_dict['conf_path']
            self.model_dir = params_dict['model_dir']
        except KeyError:
            self.model_logger.error('Some of the model parameters are missing.')
            raise
        except:
            raise

    def check_params(self):
        params_dict = self.to_dict()
        for val in params_dict.values():
            if 'None' == str(val):
                self.model_logger.warning(
                    'The params of model is not complete, and the params are as following: {}'.format(params_dict))
                return False
        return True



    def make_epsilon(self):
        epsilon_startup = 1 / float(self.max_startup)
        epsilon_total = 1 / float(self.max_total)
        epsilon_row = 1 / float(self.max_row)
        epsilon_mem = 1 / float(self.max_mem)
        epsilon_arr = []
        for label in self.model_targets:
            if label == 'S':
                epsilon_arr.append(epsilon_startup)
            elif label == 'T':
                epsilon_arr.append(epsilon_total)
            elif label == 'R':
                epsilon_arr.append(epsilon_row)
            elif label == 'M':
                epsilon_arr.append(epsilon_mem)
        return epsilon_arr


class RnnModel():
    """
    This is the rnn_model class that keeps APIs for ml functions.
    """

    def __init__(self, model_info):
        config = tf.compat.v1.ConfigProto()
        config.gpu_options.allow_growth = True
        self.graph = tf.Graph()
        self.session = tf.compat.v1.Session(config=config, graph=self.graph)
        self.model = None
        self.model_info = model_info
        logging.config.fileConfig('log.conf')
        self.model_logger = logging.getLogger('model')

    def _build_model(self, epsilon):
        model = Sequential()
        try:
            model.add(CuDNNLSTM(units=int(self.model_info.hidden_units), return_sequences=True,
                                input_shape=(None, int(self.model_info.feature_length))))
            model.add(CuDNNLSTM(units=int(self.model_info.hidden_units), return_sequences=False))
        except:
            model.add(LSTM(units=int(self.model_info.hidden_units), return_sequences=True,
                           input_shape=(None, int(self.model_info.feature_length))))
            model.add(LSTM(units=int(self.model_info.hidden_units), return_sequences=False))
        model.add(Dense(units=int(self.model_info.hidden_units), activation='relu'))
        model.add(Dense(units=int(self.model_info.hidden_units), activation='relu'))
        model.add(Dense(units=int(self.model_info.label_length), activation='sigmoid'))
        optimizer = keras.optimizers.Adadelta(lr=float(self.model_info.learning_rate), rho=0.95)
        ratio_error = ratio_error_loss_wrapper(epsilon)
        ratio_acc_2 = ratio_error_acc_wrapper(epsilon, 2)
        model.compile(loss=ratio_error, metrics=[ratio_acc_2], optimizer=optimizer)
        return model

    def parse(self, filename):
        '''
        parse the file and get the encoded features
        :param filename: the path of file to parse
        :return: feature: the features for training
                 label: the labels for training
                 need_init: whether the model need init
        '''
        parser = FeatureParser(self.model_info, filename)
        feature_length, arr_enc, arr_child, arr_startup, arr_total, arr_row, arr_mem = \
            parser.parse()
        need_init = self.check_need_init(feature_length)
        if need_init:
            max_startup, max_total, max_row, max_mem = np.max(arr_startup), np.max(arr_total), np.max(arr_row), np.max(
                arr_mem)
            self.model_info.max_startup = max(max_startup, 1)
            self.model_info.max_total = max(max_total, 1)
            self.model_info.max_row = max(max_row, 1)
            self.model_info.max_mem = max(max_mem, 1)
            self.model_info.feature_length = feature_length
            shutil.rmtree(self.model_info.model_path, ignore_errors=True)
            shutil.rmtree(os.path.realpath(
                os.path.join(settings.PATH_LOG, self.model_info.model_name)), ignore_errors=True)
        arr_startup = np.array(arr_startup, dtype=float).reshape((-1, 1))
        arr_total = np.array(arr_total, dtype=float).reshape((-1, 1))
        arr_row = np.array(arr_row, dtype=float).reshape((-1, 1))
        arr_mem = np.array(arr_mem, dtype=float).reshape((-1, 1))
        arr_startup /= float(self.model_info.max_startup)
        arr_total /= float(self.model_info.max_total)
        arr_row /= float(self.model_info.max_row)
        arr_mem /= float(self.model_info.max_mem)
        label = None
        for target in self.model_info.model_targets:
            if label is None:
                if target == 'S':
                    label = arr_startup
                elif target == 'T':
                    label = arr_total
                elif target == 'R':
                    label = arr_row
                elif target == 'M':
                    label = arr_mem
            else:
                if target == 'S':
                    label = np.hstack((label, arr_startup))
                elif target == 'T':
                    label = np.hstack((label, arr_total))
                elif target == 'R':
                    label = np.hstack((label, arr_row))
                elif target == 'M':
                    label = np.hstack((label, arr_mem))
        max_len = 0
        for sample in arr_enc:
            max_len = max(len(sample), max_len)
        feature = sequence.pad_sequences(arr_enc, maxlen=max_len)
        self.model_logger.debug('Sequence padding to max_len: %d', max_len)
        return feature, label, need_init

    def check_need_init(self, feature_length):
        '''
        To determine whether the model needs to be re-initialized
        :param model_name: name of the model
        :return:
        '''
        if not (os.path.exists(self.model_info.model_path) and os.path.getsize(self.model_info.model_path)):
            return True
        conf_dict = self.model_info.load_dict(self.model_info.conf_path)
        # check model's params
        if not (feature_length == self.model_info.feature_length \
                and self.model_info.label_length == conf_dict['label_length'] \
                and self.model_info.hidden_units == conf_dict['hidden_units']):
            return True
        return False

    def fit(self, filename):
        keras.backend.clear_session()
        set_session(self.session)
        with self.graph.as_default():
            feature, label, need_init = self.parse(filename)
            os.environ['CUDA_VISIBLE_DEVICES'] = '0'
            epsilon = self.model_info.make_epsilon()
            if need_init:
                epoch_start = 0
                self.model = self._build_model(epsilon)
            else:
                epoch_start = int(self.model_info.last_epoch)
                ratio_error = ratio_error_loss_wrapper(epsilon)
                ratio_acc_2 = ratio_error_acc_wrapper(epsilon, 2)
                self.model = load_model(self.model_info.model_path,
                                        custom_objects={'ratio_error': ratio_error, 'ratio_acc': ratio_acc_2})
            self.model_info.last_epoch = int(self.model_info.max_epoch) + epoch_start
            self.model_info.dump_dict()
            log_path = os.path.realpath(os.path.join(settings.PATH_LOG, self.model_info.model_name + '_log.json'))
            if not os.path.exists(log_path):
                os.mknod(log_path, mode=0o600)
            json_logging_callback = LossHistory(log_path, self.model_info.model_name, self.model_info.last_epoch)
            X_train, X_val, y_train, y_val = \
                train_test_split(feature, label, test_size=0.1)
            self.model.fit(X_train, y_train, epochs=self.model_info.last_epoch,
                           batch_size=int(self.model_info.batch_size), validation_data=(X_val, y_val),
                           verbose=0, initial_epoch=epoch_start, callbacks=[json_logging_callback])
            self.model.save(self.model_info.model_path)
            val_pred = self.model.predict(X_val)
            val_re = get_ratio_errors_general(val_pred, y_val, epsilon)
            self.model_logger.debug(val_re)
            del self.model
            return val_re

    def predict(self, filename):
        with self.graph.as_default():
            try:
                parser = FeatureParser(self.model_info, filename)
                feature_length, arr_enc, _, _, _, _, _ = parser.parse(is_train=False)
                debug_info = '\n'
                for tree in arr_enc:
                    for node in tree:
                        for code in node:
                            debug_info += str(code)
                            debug_info += ' '
                        debug_info += '\n'
                    debug_info += '\n'
                self.model_logger.debug(debug_info)
                max_len = 0
                for sample in arr_enc:
                    max_len = max(len(sample), max_len)
                feature = sequence.pad_sequences(arr_enc, maxlen=max_len)
                pred = self.model.predict(x=feature)
                self.model_info.dump_dict()
                return pred
            except FileNotFoundError:
                self.model_logger.error('The file to predict is not found.')
                raise
            except:
                raise

    def load(self):
        """
        Routine to load pre-trained model for prediction purpose
        :param model_name:  name of the checkpoint
        :return: tf.Session, out_nodes
        """
        keras.backend.clear_session()
        set_session(self.session)
        self.model_info.load_info()
        with self.graph.as_default():
            epsilon = self.model_info.make_epsilon()
            ratio_error = ratio_error_loss_wrapper(epsilon)
            ratio_acc_2 = ratio_error_acc_wrapper(epsilon, 2)
            try:
                self.model = load_model(self.model_info.model_path,
                                        custom_objects={'ratio_error': ratio_error, 'ratio_acc': ratio_acc_2})
            except FileNotFoundError:
                self.model_logger.error('Failed to load model information file.')
                raise
            except:
                raise


def get_ratio_errors_general(pred_arr, true_arr, epsilon_arr):
    errors = []
    for i in range(len(epsilon_arr)):
        pred, true, epsilon = pred_arr[:, i], true_arr[:, i], epsilon_arr[i]
        ratio_1 = (pred + epsilon) / (true + epsilon)
        ratio_2 = (true + epsilon) / (pred + epsilon)
        ratio = np.maximum(ratio_1, ratio_2).mean()
        errors.append(ratio)
    return errors


def ratio_error_loss(y_true, y_pred, epsilon):
    """
    Calculate the ratio error for the loss function.
    :param y_true:
    :param y_pred:
    :param epsilon:
    :return:
    """
    ratio_1 = keras.layers.Lambda(lambda x: (x[0] + x[2]) / (x[1] + x[2]))([y_true, y_pred, epsilon])
    ratio_2 = keras.layers.Lambda(lambda x: (x[0] + x[2]) / (x[1] + x[2]))([y_pred, y_true, epsilon])
    ratio = K.maximum(ratio_1, ratio_2)
    loss = K.mean(ratio)
    return loss


def ratio_error_loss_wrapper(epsilon):
    """
    Wrapper function which calculates ratio error for the loss function.
    :param epsilon:
    :return:
    """
    epsilon = K.constant(epsilon)

    def ratio_error(y_true, y_pred):
        return ratio_error_loss(y_true, y_pred, epsilon)

    return ratio_error


def ratio_error_acc(y_true, y_pred, epsilon, threshold):
    """
    Calculate the ratio error accuracy with the threshold.
    :param y_true:
    :param y_pred:
    :param epsilon:
    :param threshold:
    :return:
    """
    ratio_1 = keras.layers.Lambda(lambda x: (x[0] + x[2]) / (x[1] + x[2]))([y_true, y_pred, epsilon])
    ratio_2 = keras.layers.Lambda(lambda x: (x[0] + x[2]) / (x[1] + x[2]))([y_pred, y_true, epsilon])
    ratio = K.maximum(ratio_1, ratio_2)
    mask = K.cast(K.less(ratio, threshold), dtype="float32")
    return K.mean(mask)


def ratio_error_acc_wrapper(epsilon, threshold):
    """
    Wrapper function which calculates ratio error for the ratio error accuracy with the threshold.
    :param epsilon:
    :param threshold:
    :return:
    """
    epsilon = K.constant(epsilon)
    threshold = K.constant(threshold)

    def ratio_acc(y_true, y_pred):
        return ratio_error_acc(y_true, y_pred, epsilon, threshold)

    return ratio_acc
