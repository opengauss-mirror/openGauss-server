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
import numpy as np
import os
from keras.layers import Input, LSTM
from keras.models import Model


class AutoEncoder(object):
    def __init__(self, shape, filepath=None, encoding_dim=1):
        if not shape:
            raise ValueError('shape is bad.')

        self.encoding_dim = encoding_dim
        self.filepath = filepath

        # build the network.
        inputs = Input(shape=shape)
        # encoder layers
        encoded = LSTM(128, return_sequences=True)(inputs)
        encoded = LSTM(32, return_sequences=True)(inputs)
        encoded = LSTM(8, return_sequences=True)(encoded)

        # encoder output layer and decoder input layer:
        encoded = LSTM(encoding_dim, return_sequences=True)(encoded)

        # decoder layers
        decoded = LSTM(8, return_sequences=True)(encoded)
        decoded = LSTM(32, return_sequences=True)(encoded)
        decoded = LSTM(128, return_sequences=True)(decoded)
        decoded = LSTM(shape[-1], activation='tanh', return_sequences=True)(decoded)

        # create model
        self.autoencoder = Model(inputs=inputs, outputs=decoded)
        self.encoder = Model(inputs=inputs, outputs=encoded)

        # compile autoencoder
        self.autoencoder.compile(optimizer='adam', loss='mse')

    def save(self):
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
        self.encoder.save_weights(self.filepath)

    def load(self):
        self.encoder.load_weights(self.filepath)

    def fit(self, X, batch_size=128, epochs=100):
        self.autoencoder.fit(X, X, epochs=epochs, batch_size=batch_size, shuffle=True)
        self.save()

    def transfer(self, X):
        self.load()
        rv = self.encoder.predict(X)
        rv = np.reshape(rv, newshape=(X.shape[0], self.encoding_dim * X.shape[1]))
        return rv
