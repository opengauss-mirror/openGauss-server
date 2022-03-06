# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import logging
import os
import signal
from abc import ABC, abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed, wait
import concurrent
from multiprocessing import Event

from dbmind.common import utils
from dbmind.common.platform import WIN32

IN_PROCESS = 'DBMind [Worker Process] [IN PROCESS]'
PENDING = 'DBMind [Worker Process] [IDLE]'


def _initializer():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, signal.SIG_IGN)
    utils.set_proc_title(PENDING)


def function_starter(func, *args, **kwargs):
    utils.set_proc_title(IN_PROCESS)
    try:
        return func(*args, **kwargs)
    finally:
        utils.set_proc_title(PENDING)


class AbstractWorker(ABC):
    CLOSED = 0
    RUNNING = 1

    def __init__(self, worker_num):
        self.worker_num = worker_num
        self.status = self.RUNNING

    @abstractmethod
    def _parallel_execute(self, func, iterable):
        pass

    @abstractmethod
    def _submit(self, func, synchronized, args):
        pass

    @abstractmethod
    def as_completed(self, funcs):
        pass

    def apply(self, func, synchronized=True, args=()):
        logging.info('Dispatch the task %s (%s) to workers.', func.__name__, args)
        return self._submit(func, synchronized, args)

    def parallel_execute(self, func, iterable):
        if self.status == self.CLOSED:
            logging.warning('Worker already exited.')
            return
        logging.info('Dispatch the multiple tasks %s to workers.', func.__name__)
        return self._parallel_execute(func, iterable)

    @abstractmethod
    def terminate(self, cancel_futures):
        self.status = self.CLOSED


class _ProcessPoolExecutor(ProcessPoolExecutor):

    @staticmethod
    def _wait_for_notify(event):
        event.wait()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make the process pool is a fixed process pool, which creates many idle processes and waits for the
        # scheduler's task. Why not use lazy-loading mode? Because the worker process forked from the master process,
        # the master process maybe have some running backend threads while forking. This action will cause unexpected
        # behaviors, such as timed backend threads also being forked and run in the child process.
        event = Event()
        for _ in range(self._max_workers):
            self.submit(self._wait_for_notify, event)
        event.set()

    def shutdown(self, wait=True, *, cancel_futures=False):
        # Added cancel_futures into shutdown() method in version 3.9.
        # Hence, we have to force to kill all sub-processes explicitly.
        logging.debug('Terminate workerProcesses: cancel_futures: %s, backends: %s.',
                      cancel_futures, list(self._processes.keys()))
        if cancel_futures and len(self._processes) > 0:
            for pid in self._processes.keys():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    logging.warning('Not found the process ID %d to kill.', pid)
            os.wait()
        else:
            super().shutdown()


class ProcessWorker(AbstractWorker):
    def __init__(self, worker_num):
        if worker_num <= 0:
            worker_num = max(os.cpu_count() // 2, 3)
            logging.warning(
                '[ProcessWorker] automatically set worker_num = %d due to target error.', worker_num
            )
        if WIN32:
            from concurrent.futures.thread import ThreadPoolExecutor
            self.pool = ThreadPoolExecutor(worker_num)
        else:
            self.pool = _ProcessPoolExecutor(worker_num, initializer=_initializer)

        super().__init__(worker_num)

    def _parallel_execute(self, func, iterable):
        futures = []

        for params in iterable:
            if isinstance(params, dict):
                args = list()
                kwargs = params
            else:
                args = list(params)
                kwargs = dict()
            args.insert(0, func)
            futures.append(self.pool.submit(function_starter, *args, **kwargs))

        wait(futures)
        results = []
        for future in futures:
            try:
                results.append(future.result())
            except concurrent.futures.process.BrokenProcessPool:
                # killed by parent process
                results.append(None)
            except Exception as e:
                results.append(None)
                logging.exception(e)
        return results

    def _submit(self, func, synchronized, args):
        args = list(args)
        args.insert(0, func)
        if synchronized:
            return self.pool.submit(function_starter, *args).result()
        else:
            return self.pool.submit(function_starter, *args)

    def as_completed(self, funcs):
        return as_completed(funcs)

    def terminate(self, cancel_futures):
        super().terminate(cancel_futures)
        self.pool.shutdown(True, cancel_futures=cancel_futures)


def get_worker_instance(_type, process_num, hosts=None) -> AbstractWorker:
    if _type == 'local':
        return ProcessWorker(process_num)
    elif _type == 'dist':
        raise NotImplementedError
    else:
        raise ValueError('Invalid configuration: [WORKER] type: %s.' % _type)

