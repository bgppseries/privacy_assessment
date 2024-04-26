import configparser
import time
from celery import Task, Celery,chain,group
from . import config
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
from celery.utils.log import get_task_logger

from .data_import_handel import csv_importer,json_importer
# 配置日志

logger = logging.getLogger(__name__)
file_handler = TimedRotatingFileHandler('celery.log', when='midnight')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


celery=Celery()
celery.config_from_object(config)

global_uuid=uuid.uuid4()

import json
import math
import time
import pandas as pd
import uuid
from celery_task.Indicator_K_Json2 import Data_availability,Data_compliance,Desensitization_data_character,Desensitization_data_quality_evalution, privacy_protection_metrics
from celery.result import AsyncResult


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




############## 任务接收API接口设计
##      只用于可视化的api，数据平台的todo
@celery.task(bind=True,base=MyTask)
def Recv_api(Config):
    pass


# 执行csv导入指令
@celery.task(bind=True,base=MyTask)
def csv_start_import(uuid,file,docker_id,discribition,ID,QIDs,SA):
    logger.info('任务号',uuid,'执行导入程序，任务描述：',discribition,'任务配置,ID:',ID,'QIDs: ',QIDs,'SA:',SA)
    csv_importer(file,docker_id,ID,QIDs,SA,logger)
        
# 执行json导入指令
@celery.task(bind=True,base=MyTask)
def json_start_import(uuid,file,docker_id,discribition,ID,QIDs,SA):
    logger.info('任务号',uuid,'执行json导入任务，任务描述：',discribition,'任务配置,ID:',ID,'QIDs: ',QIDs,'SA:',SA)
    json_importer(file,docker_id,ID,QIDs,SA,logger)




@celery.task(bind=True,base=MyTask)
def start_grouptask(self):
    task_map = {}  # 用于存储任务 ID 与函数绑定的字典
    # 绑定任务 ID 与函数
    task_map[self.request.id] = start_grouptask.__name__
    # 执行一连串的任务
    subtasks = group(subtask.s(i) for i in range(10))
    result = subtasks.apply_async()
    ##result.join()
    logger.info(json.dumps(task_map))
    return json.dumps(task_map)


@celery.task(bind=True,base=MyTask)
def subtask(self,i):
    time.sleep(60)
    logger.info(i)
    print(self.request.id)

# 执行评估指令
@celery.task(bind=True,base=MyTask)
def start_evaluate(self,src_url,un_table_name,to_url,table_name,QIDs,SA,ID,k=2,l=2,t=0.95):
    print("########################################################################")
    print("*******************************评估启动**********************************")
    worker_id = start_evaluate.request.id
    logger.info('json评估任务启动，任务配置：%s %s',worker_id,SA)
    
    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker=reid_risk.delay(worker_id,src_url,un_table_name,to_url,table_name,QIDs,SA,ID)
    reid_risk_worker_id=reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)

    # 异步执行 Desensitization_quality 函数，并记录任务 ID
    Desensitization_quality_worker = Desensitization_quality.delay(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    Desensitization_quality_worker_id = Desensitization_quality_worker.id
    logger.info("匿名数据质量评估已启动，子进程id是：%s", Desensitization_quality_worker_id)

    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    privacy_protection_worker_id = privacy_protection_worker.id
    logger.info("隐私保护性度量评估已启动，子进程id是：%s", privacy_protection_worker_id)
    # 轮询各个函数执行情况
    while True:
        # 获取各个函数的异步结果对象
        reid_risk_result = AsyncResult(reid_risk_worker.id)
        compliance_result = AsyncResult(compliance_worker.id)
        availability_result = AsyncResult(availability_worker.id)
        Desensitization_character_result = AsyncResult(Desensitization_character_worker.id)
        Desensitization_quality_result = AsyncResult(Desensitization_quality_worker.id)
        privacy_protection_result = AsyncResult(privacy_protection_worker.id)
        
        # 判断是否所有函数都成功执行
        if (reid_risk_result.successful() and compliance_result.successful() and
            availability_result.successful() and Desensitization_character_result.successful() and
            Desensitization_quality_result.successful() and privacy_protection_result.successful()):
            # 打印成功信息
            logger.info("{}所有评估任务均已成功执行！".format(worker_id))
            break
        else:
            # 若未全部成功执行，则等待一段时间后继续轮询
            time.sleep(15)  # 等待5秒后再次轮询


@celery.task(bind=True,base=MyTask)
def reid_risk(self,uuid,src_url,un_table_name,to_url,table_name,QIDs,SA,ID):
    from .risk import reidentity_risk_assess
    ridrisk,sarisk=reidentity_risk_assess(uuid,src_url,un_table_name,to_url,table_name,QIDs,SA,ID,2,0.0004,0)
    logger.info('评估任务id: {},风险评估结果：{}'.format(uuid,ridrisk))
    logger.info('评估任务id: {},统计推断攻击结果: {}'.format(uuid,sarisk))

from .config import Config

@celery.task(bind=True,base=MyTask)
def compliance(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    Series_quasi = handler._Function_Series_quasi()##结果为升序
    ##数据合规性
    handler1 = Data_compliance(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler1.runL(Series_quasi)

@celery.task(bind=True,base=MyTask)
def availability(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    Series_quasi = handler._Function_Series_quasi()##结果为升序
    ##数据可用性
    handler2 = Data_availability(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler2.runL(Series_quasi)

@celery.task(bind=True,base=MyTask)
def Desensitization_character(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    Series_quasi = handler._Function_Series_quasi()##结果为升序
    ##匿名集数据特征
    handler3 = Desensitization_data_character(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler3.runL(Series_quasi)   

@celery.task(bind=True,base=MyTask)
def Desensitization_quality(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
        Series_quasi = handler._Function_Series_quasi()##结果为升序
        ##匿名数据质量评估
        handler4 = Desensitization_data_quality_evalution(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
        handler4.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        

@celery.task(bind=True,base=MyTask)
def privacy_protection(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
        Series_quasi = handler._Function_Series_quasi()##结果为升序
        ##隐私保护性度量
        handler5 = privacy_protection_metrics(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
        handler5.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量

