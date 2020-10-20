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
from main import train, predict
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score

trans2sec = 10 ** 12


def main():
    train_file = 'data/train.csv'
    test_file = 'data/test.csv'
    label_file = 'data/label.csv'
    # create dataset
    test_label = list()
    with open(label_file) as f:
        for line in f.readlines():
            line = line.strip()
            test_label += [float(line)]

    cluster_number, top_sql = train('data/train.csv', 'data/', 2000, 40)
    print('Best cluster number is: ' + str(cluster_number))
    print('Typical SQL template is: ')
    print(top_sql)
    result = predict('data/test.csv', 'data/', 0.1)

    # plot
    x = range(len(result))
    scores = r2_score(test_label, result, multioutput='variance_weighted')
    plt.scatter(x, test_label, marker='o', label='actual value')
    plt.scatter(x, result, marker='*', label='predicted value')
    plt.title("acc: " + str(scores * 100))
    plt.legend()
    plt.show()


if __name__ == '__main__':
    main()
