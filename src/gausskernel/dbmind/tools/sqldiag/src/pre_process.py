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
import json
import os
import stat
import re
import numpy as np


class Preprocessor(object):
    def __init__(self, alpha=0.5, filepath=None):
        self.symbol_list = ['\'', '(', ')', ',', ';', '%', '\n']  # stop list
        self.pattern_date = re.compile(r'\d{4}-\d{1,2}-\d{1,2}')
        self.pattern_float = re.compile(r'^[-+]?\d+$|^[-+]?\d+\.\d+$')
        self.word_dict = dict()
        self.word_count = 2  # 0 and 1 have special mean in dictionary
        self.max_len = 0
        self.alpha = alpha  # ratio for re-training
        self.filepath = filepath

    def split_line(self, line):
        """
        function: remove stop letter
        :param line: input line string
        :return: output line string
        """
        i = 0
        while i < len(line):
            if line[i] in self.symbol_list:
                line = line[:i] + line[i + 1:]
            else:
                i += 1
        return line

    def word2id(self, line):
        """
        function: transform line to int vector
        :return: line vector
        """
        tmp = []
        for i in range(len(line)):
            if line[i] in self.word_dict:
                tmp += [int(self.word_dict[line[i]])]
            else:
                tmp += [self.word_count]
                self.word_dict[line[i]] = self.word_count
                self.word_count += 1
        return tmp

    def save(self):
        # remove previous file
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
        json_dict = json.dumps(self.word_dict, indent=4)

        with os.fdopen(os.open(self.filepath, os.O_WRONLY | os.O_CREAT, stat.S_IWUSR | stat.S_IRUSR),'w') as f:
            f.write(json_dict)

    def load(self):
        with open(self.filepath) as f:
            self.word_dict = json.load(f)

    def pre_process(self, data_sr):
        """
        function: pre-process for train and test data
        :param data_sr: data series
        :return: dataset, info of dataset
        """
        dataset = []
        exe_time = []
        wait_time = []
        lock_time = []
        dataset_tmp = []
        for line in data_sr:
            # line factor: [execution time, lock time, wait time, sql text]
            exe_time += [float(line[0])]
            wait_time += [float(line[1])]
            lock_time += [float(line[2])]
            line[-1] = " ".join(self.split_line(line[-1]).strip().split())
            dataset_tmp += [self.word2id(line[-1].split())]
            if len(line[-1].split()) > self.max_len:
                self.max_len = len(line[-1].split())
        for line in dataset_tmp:
            line_vector = [[0] * (self.word_count - 1)] * (self.max_len - len(line))
            for i in line:
                line_vector += [[0] * (int(i) - 1) + [1] + [0] * (self.word_count - 1 - int(i))]
            dataset += [line_vector]
        time_vectors = dict()
        time_vectors['exe_time'] = np.array(exe_time)
        time_vectors['wait_time'] = np.array(wait_time)
        time_vectors['lock_time'] = np.array(lock_time)

        # word in dict will not appear space, so we can use this key saving word_count
        self.word_dict['word count'] = self.word_count - 1
        # word in dict will not appear space, so we can use this key saving max_len
        self.word_dict['max len'] = self.max_len

        self.save()

        dataset_np = np.array(dataset).astype('float32')
        return dataset_np, time_vectors

    def transform(self, data_sr):
        self.load()
        count = self.word_dict['word count']  # word_count of original training data
        self.word_count = len(self.word_dict) - 1
        self.max_len = self.word_dict['max len']
        dataset = []
        for line in data_sr:
            line = " ".join(self.split_line(line).strip().split())
            line_tmp = self.word2id(line.split())
            line_vector = [[0] * count] * (self.max_len - len(line_tmp))
            for i in line_tmp:
                i = min(i, count - 1)
                line_vector += [[0] * (i - 1) + [1] + [0] * (count - i)]
            line_vector = line_vector[:self.max_len]
            dataset += [line_vector]

        if self.word_count > count * (1 + self.alpha):
            print('Ratio of new data has reached your set threshold, suggest re-training!')

        self.save()

        dataset_np = np.array(dataset).astype('float32')
        return dataset_np
