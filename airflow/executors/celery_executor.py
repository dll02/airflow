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

import subprocess
import time

from celery import Celery
from celery import states as celery_states

from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

PARALLELISM = configuration.get('core', 'PARALLELISM')

'''
To start the celery worker, run the command:
airflow worker
'''

if configuration.has_option('celery', 'celery_config_options'):
    celery_configuration = import_string(
        configuration.get('celery', 'celery_config_options')
    )
else:
    celery_configuration = DEFAULT_CELERY_CONFIG

app = Celery(
    configuration.get('celery', 'CELERY_APP_NAME'),
    config_source=celery_configuration)

# Celery 通过装饰器app.task创建Task对象，Task对象提供两个核心功能：
# 将任务消息发送到队列和声明 Worker 接收到消息后需要执行的具体函数。
#  command 被格式化：airflow run <dag_id> <task_id> <execution_date> --local --pool <pool> -sd <python_file>
@app.task # 使用@app.task装饰器将该函数转换为Celery任务。
def execute_command(command):
    log = LoggingMixin().log
    log.info("Executing command in Celery: %s", command)
    try:
        # 检查返回的call back
        # 如果需要处理任务的结果，则需要使用回调函数等机制来获取结果
        # shell=True表示在shell中运行命令
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        log.error(e)
        raise AirflowException('Celery command failed')


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """
    def start(self):
        self.tasks = {}
        self.last_state = {}

    # execute_command是Celery Task任务实例，下文会介绍
    def execute_async(self, key, command,
                      queue=DEFAULT_CELERY_CONFIG['task_default_queue']):
        self.log.info( "[celery] queuing {key} through celery, "
                       "queue={queue}".format(**locals()))
        # 通过 execute_async 异步提交到 Celery 集群，将返回的任务句柄保存在 tasks。
        self.tasks[key] = execute_command.apply_async(
            args=[command], queue=queue)
        self.last_state[key] = celery_states.PENDING

    # 同步任务状态，根据任务状态进行不同处理
    # Scheduler 通过 sync 方法轮询任务句柄获取任务状态，并根据任务状态回调 success 或者 fail 更新状态。
    def sync(self):
        self.log.debug("Inquiring about %s celery task(s)", len(self.tasks))
        for key, async in list(self.tasks.items()):
            try:
                state = async.state
                if self.last_state[key] != state:
                    if state == celery_states.SUCCESS:
                        self.success(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    elif state == celery_states.FAILURE:
                        self.fail(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    elif state == celery_states.REVOKED:
                        self.fail(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    else:
                        self.log.info("Unexpected state: %s", async.state)
                    self.last_state[key] = async.state
            except Exception as e:
                self.log.error("Error syncing the celery executor, ignoring it:")
                self.log.exception(e)

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    async.state not in celery_states.READY_STATES
                    for async in self.tasks.values()]):
                time.sleep(5)
        self.sync()
