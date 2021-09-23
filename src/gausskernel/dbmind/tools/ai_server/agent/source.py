#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : source.py
# Version      :
# Date         : 2021-4-7
# Description  :
#############################################################################


class Source:
    """
    This is father class which is used for acquiring mutiple metric data at same time.
    """

    def __init__(self):
        self._channel_manager = None

    def start(self):
        pass

    def stop(self):
        pass

    @property
    def channel_manager(self):
        return self._channel_manager

    @channel_manager.setter
    def channel_manager(self, channel_manager):
        self._channel_manager = channel_manager
