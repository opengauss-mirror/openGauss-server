#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : sink.py
# Version      :
# Date         : 2021-4-7
# Description  :
#############################################################################


class Sink:
    """
    This is father class which is used for getting data from ChannelManager object and 
    sending data at a specified time interval.
    """

    def __init__(self):
        self._channel_manager = None
        self.running = False

    @property
    def channel_manager(self):
        return self._channel_manager

    @channel_manager.setter
    def channel_manager(self, channel_manager):
        self._channel_manager = channel_manager

    def process(self):
        pass

    def start(self):
        self.running = True
        self.process()

    def stop(self):
        self.running = False

