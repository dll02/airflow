# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import multiprocessing
import subprocess
import time

from builtins import range

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

PARALLELISM = configuration.get('core', 'PARALLELISM')


class LocalWorker(multiprocessing.Process, LoggingMixin):
    # 不同的线程启动LocalWorker
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True

    def run(self):
        while True:
            # 不断循环读取queue中的元素，没有时则阻塞
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.log.info("%s running %s", self.__class__.__name__, command)
            command = "exec bash -c '{0}'".format(command)
            try:
                # 启动子进程执行命令
                subprocess.check_call(command, shell=True)
                state = State.SUCCESS
            except subprocess.CalledProcessError as e:
                state = State.FAILED
                self.log.error("Failed to execute task %s.", str(e))
                # TODO: Why is this commented out?
                # raise e
            # 结果加入result_queue
            self.result_queue.put((key, state))
            # 标记队列运行数-1
            self.task_queue.task_done()
            time.sleep(1) # 防止过度消耗cpu 执行完一次休眠1s


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    使用python多线程包 使用队列并发执行task
    of tasks.
    """

    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        # 根据并发度启动本地执行worker
        self.workers = [
            LocalWorker(self.queue, self.result_queue)
            for _ in range(self.parallelism)
        ]

        for w in self.workers:
            w.start()

    def execute_async(self, key, command, queue=None):
        # 异步执行放入相关队列
        self.queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            # 同步则是挨个等待执行结果 并改变状态
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        # Sending poison pill to all worker
        for _ in self.workers:
            self.queue.put((None, None))

        # Wait for commands to finish
        self.queue.join()
        self.sync()
