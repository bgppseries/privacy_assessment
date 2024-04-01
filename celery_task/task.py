import time
import os
from flask import current_app
from celery import Task, Celery
from . import config
from .Indicator_K_Json2 import Config
import uuid


import logging
from logging.handlers import TimedRotatingFileHandler
from celery.utils.log import get_task_logger

# 配置日志
logger = get_task_logger(__name__)
file_handler = TimedRotatingFileHandler('celery.log', when='midnight')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



celery=Celery()
celery.config_from_object(config)

class MyTask(Task): # celery 基类
    def on_success(self, retval, task_id, args, kwargs):
        # 执行成功的操作
        #todo logger
        print('MyTasks 基类回调，任务执行成功')
        return super(MyTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # 执行失败的操作
        # 任务执行失败，可以调用接口进行失败报警等操作
        #todo logger
        print('MyTasks 基类回调，任务执行失败')
        return super(MyTask, self).on_failure(exc, task_id, args, kwargs, einfo)


""""
    执行逻辑：针对不同的API接口
        任务
"""

from .config import Config
############## 任务接收API接口设计
@celery.task(bind=True,base=MyTask)
def Recv_api(Config):
    








# 执行导入指令
@celery.task(bind=True,base=MyTask)
def start_import(file)


# 执行评估指令
@celery.task(bind=True,base=MyTask)
def start_evaluate(url,k,l,t):
    print("########################################################################")
    print("*******************************评估启动**********************************")
    work_uuid = str(uuid.uuid4())  ##定义uuid
    print("评估任务uuid为：", work_uuid)
    Config(k,l,t,url,work_uuid).Run()

