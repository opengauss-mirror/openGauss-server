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
import heapq
import json
import os
import stat

import joblib
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from predict_method import smoothing


def calculate_best_cluster_number(data, n_sequence, n_samples=5000):
    """
    function: calculate the best number of cluster(hyper-parameter K)
    :param n_sequence: array-like, can be iterable.
    :param data: input data
    :param n_samples: sample from data to calculate
    :return: best number of cluster
    """
    idx = np.random.choice(len(data), size=n_samples)
    data_train = data[idx]

    scores = list()

    model = KMeans(random_state=123)
    for index, cluster_number in enumerate(n_sequence):
        model.n_clusters = cluster_number
        model.fit(data_train)
        score = silhouette_score(data_train, model.labels_)
        scores.append(score)

    best_cluster_number = n_sequence[np.argmax(scores)]
    return best_cluster_number


def euclidean_distance(v1, v2):
    distance = np.sqrt(np.sum(np.square(v1 - v2)))
    return distance


def cosine_distance(v1, v2):
    norm_1 = np.linalg.norm(v1, 2)
    norm_2 = np.linalg.norm(v2, 2)
    distance = np.dot(v1, v2) / (norm_1 * norm_2)
    return distance


def get_topn_sql(dist_tbl, topn=1):
    """
    function: get the top N nearest vectors from each cluster center.
    :param dist_tbl: distance table structure
    :param topn: number of top nearest, default 1
    :return: index of top n nearest vectors
    """
    topn_index = []
    for c in dist_tbl:
        dist = heapq.nsmallest(topn, c.keys())  # top n index, the distance from center.
        idx = map(lambda x: c.get(x), dist)
        topn_index.extend(idx)

    return topn_index


class Cluster(object):
    def __init__(self, dirpath='', topn=1):
        self.model = KMeans()
        self.cluster_info = dict()
        self.model_path = dirpath + 'Kmeans_model.pkl'
        self.cluster_info_path = dirpath + 'cluster_info.json'
        self.topn = topn

    def save(self):
        for file in [self.cluster_info_path, self.model_path]:
            if os.path.exists(file):
                os.remove(file)
        joblib.dump(self.model, self.model_path)
        json_dict = json.dumps(self.cluster_info, indent=4)

        with os.fdopen(os.open(self.cluster_info_path, os.O_WRONLY | os.O_CREAT, stat.S_IWUSR | stat.S_IRUSR),'w') as f:
            f.write(json_dict)

    def load(self):
        self.model = joblib.load(self.model_path)
        with open(self.cluster_info_path) as f:
            self.cluster_info = json.load(f)

    def classify(self, cluster_vectors):
        n_clusters = calculate_best_cluster_number(cluster_vectors,
                                                   n_samples=int(len(cluster_vectors) * 0.5),
                                                   n_sequence=range(5, 50, 2))
        self.model = KMeans(n_clusters=n_clusters, random_state=666)
        predict_result = self.model.fit_predict(cluster_vectors)

        centers = self.model.cluster_centers_
        dist_tbl = [{} for _ in range(n_clusters)]  # distance table: [{distance: vector index}]

        for i, v in enumerate(cluster_vectors):
            y = predict_result[i]
            dist = euclidean_distance(centers[y], v)
            dist_tbl[y][dist] = i

        return predict_result, n_clusters, dist_tbl

    def get_cluster_info(self, predict_result, time_vectors, cluster_number=10):
        for i in range(cluster_number):
            idx = str(i)
            time_series = dict()
            self.cluster_info[idx] = dict()
            for time in ['exe_time', 'wait_time', 'lock_time']:
                time_series[time] = np.array(time_vectors[time][predict_result == i])
            self.cluster_info[idx]['exe_time'] = smoothing(time_series['exe_time'])
            self.cluster_info[idx]['wait_time'] = np.mean(time_series['wait_time'])
            self.cluster_info[idx]['lock_time'] = np.mean(time_series['lock_time'])
        self.save()

    def predict(self, cluster_vectors):
        """
        function: predict classified result
        :param cluster_vectors: input data series
        :return: time series
        """
        self.load()
        predict_cluster = self.model.predict(cluster_vectors)
        result = list()
        for i in predict_cluster:
            result += [self.cluster_info[str(i)]['exe_time']]
        return result
