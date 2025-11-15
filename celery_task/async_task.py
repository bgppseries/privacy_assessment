from datetime import datetime
import ast 
from celery import Task, chain
from celery import Celery
import requests
from . import config
import json
from celery.utils.log import get_task_logger
from .data_import_handel import csv_importer, json_importer

import time
import pandas as pd
import redis
# from celery_task.config import Data_availability, Data_compliance, Desensitization_data_character, \
#     Desensitization_data_quality_evalution, privacy_protection_metrics

from celery_task.func.metic import Data_availability, Data_compliance, Desensitization_data_character, \
    Data_Security
from celery_task.func.metic import Config


from celery.result import AsyncResult
import logging
from logging.handlers import TimedRotatingFileHandler


# 配置日志

logger = logging.getLogger(__name__)
file_handler = TimedRotatingFileHandler('celery.log', when='midnight')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

celery = Celery()
celery.config_from_object(config)


def t_status(id):
    c = celery.AsyncResult(id)
    return c


def t_stop(id):
    c = celery.AsyncResult(id)
    c.revoke(terminate=True)
    return c


import traceback


def check_and_handle_task_result(task, worker_id):
    """
    检查任务的结果并在有错误时处理。
    """
    from .config import Send_result
    try:
        result = task.get(timeout=36000)  # 根据需要调整超时时间，单位为s，设置为None表示永久等待
        Send_result(worker_id, result, success=True)
    except Exception as e:
        error_msg = str(e) + "\n" + traceback.format_exc()
        Send_result(worker_id, {}, success=False, error_message=error_msg)
        logger.error("任务 %s 中出现错误：%s", worker_id, error_msg)


def send_err(uuid):
    pass


class MyTask(Task):  # celery 基类
    # 默认配置
    time_limit = 2200000  # 硬超时，单位为秒 (5分钟)
    soft_time_limit = 2000000  # 软超时，单位为秒 (4分钟30秒)
    max_retries = 3  # 最大重试次数
    default_retry_delay = 60  # 重试间隔，单位为秒

    def on_success(self, retval, task_id, args, kwargs):
        # 执行成功的操作
        print('MyTasks 基类回调，任务执行成功')
        return super(MyTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # 执行失败的操作
        # 任务执行失败，可以调用接口进行失败报警等操作
        print('MyTasks 基类回调，任务执行失败')
        return super(MyTask, self).on_failure(exc, task_id, args, kwargs, einfo)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        print("MyTask 基类回调，任务重试中")
        super(MyTask, self).on_retry(exc, task_id, args, kwargs, einfo)


@celery.task(bind=True, base=MyTask)
def csv_start_import(uuid, file, docker_id, discribition, ID, QIDs, SA):
    logger.info('任务号', uuid, '执行导入程序，任务描述：', discribition, '任务配置,ID:', ID, 'QIDs: ', QIDs, 'SA:', SA)
    csv_importer(file, docker_id, ID, QIDs, SA, logger)


# 执行json导入指令
@celery.task(bind=True, base=MyTask)
def json_start_import(uuid, file, docker_id, discribition, ID, QIDs, SA):
    logger.info('任务号', uuid, '执行json导入任务，任务描述：', discribition, '任务配置,ID:', ID, 'QIDs: ', QIDs, 'SA:',
                SA)
    json_importer(file, docker_id, ID, QIDs, SA, logger)


# 定义异步任务，将数据发送给隐私增强系统
@celery.task(bind=True, base=MyTask)
def send_data_to_backend(self, worker_id, data_table, sql_url):
    """
    隐私增强接口url从config.py文件中获取
    param@ data_table:数据表名
    param@ sql_url:数据库地址
    """
    from celery_task.config import privacy_enhance_url
    url = privacy_enhance_url
    params = {
        "Address": data_table,
        "url": sql_url,
        "worker_id": worker_id
    }
    try:
        response = requests.post(url, params=params)
        response.raise_for_status()  # 检查响应状态码，如果不是 200 则抛出异常
        logger.info("数据已经隐私增强处理，数据格式如下:{}".format(response.json))
        return response.json()  # 返回响应的 JSON 数据
    except requests.RequestException as e:
        logger.error("发送给隐私增强系统失败，Error:{}".format(e))
        return None


@celery.task(bind=True, base=MyTask)
def start_evaluate_enhanced(self, result, worker_id):
    if result == None:
        # 说明发送给隐私增强系统失败
        # 将err写入到redis中，键为worker_id 
        map = {
            'success': False,
            'thread_id': worker_id
        }
        from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
            WORKER_ID_MAP_REDISPASSWORD
        redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                         db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        redis_client.hmset(worker_id, map)
        logger.info("发送给隐私增强系统失败，流程终止，已将结果写入redis数据库,{}".format(worker_id))

    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
        WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 从redis中获取任务参数
    task_params = redis_client.hgetall(worker_id)
    src_url = task_params['src_url']
    un_table_name = task_params['un_table_name']
    to_url = task_params['to_url']
    table_name = task_params['table_name']
    QIDs = task_params['QIDs']
    SA = task_params['SA']
    ID = task_params['ID']
    k = int(task_params['k'])
    l = int(task_params['l'])
    t = float(task_params['t'])
    print("########################################################################")
    print("*******************************评估启动**********************************")
    logger.info('隐私增强后，数据评估任务启动，任务配置：%s %s', worker_id, SA)
    """"
    redis中存放的关于worker_id映射表
    key:    worker_id        value:     map,存放的是各个子任务的uuid
    key:    worker_id+'-0'   value:     评估任务的相关参数
    key:    worker_id+'-1'   value:     风险评估结果
    key:    worker_id+'-2'   value:     合规性评估结果
    key:    worker_id+'-3'   value:     可用性评估结果
    key:    worker_id+'-4'   value:     匿名集数据特征评估结果
    key:    worker_id+'-5'   value:     匿名集数据质量评估结果
    key:    worker_id+'-6'   value:     隐私保护性度量评估结果
    隐私增强后的数据：↓
    key:    'enhanced'+worker_id        value:     map,存放的是各个子任务的uuid
    key:    'enhanced'+worker_id+'-0'   value:     评估任务的相关参数
    key:    'enhanced'+worker_id+'-1'   value:     风险评估结果
    key:    'enhanced'+worker_id+'-2'   value:     合规性评估结果
    key:    'enhanced'+worker_id+'-3'   value:     可用性评估结果
    key:    'enhanced'+worker_id+'-4'   value:     匿名集数据特征评估结果
    key:    'enhanced'+worker_id+'-5'   value:     匿名集数据质量评估结果
    key:    'enhanced'+worker_id+'-6'   value:     隐私保护性度量评估结果
    """
    worker_id = 'enhanced' + worker_id
    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker = reid_risk.delay(worker_id + '-1', src_url, un_table_name, to_url, table_name, QIDs, SA, ID)
    reid_risk_worker_id = reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(k, l, t, src_url, un_table_name, worker_id + '-2', QIDs, SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(k, l, t, src_url, un_table_name, worker_id + '-3', QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(k, l, t, src_url, un_table_name,
                                                                       worker_id + '-4', QIDs, SA, ID, '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)

    # 异步执行 Desensitization_quality 函数，并记录任务 ID
    Desensitization_quality_worker = Desensitization_quality.delay(k, l, t, src_url, un_table_name, worker_id + '-5',
                                                                   QIDs, SA, ID, '', '')
    Desensitization_quality_worker_id = Desensitization_quality_worker.id
    logger.info("匿名数据质量评估已启动，子进程id是：%s", Desensitization_quality_worker_id)

    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(k, l, t, src_url, un_table_name, worker_id + '-6', QIDs, SA,
                                                         ID, '', '')
    privacy_protection_worker_id = privacy_protection_worker.id
    logger.info("隐私保护性度量评估已启动，子进程id是：%s", privacy_protection_worker_id)

    # 维护主线程和子线程id的映射
    # 只用于任务调度接口
    thread_process_mapping = {
        'success': True,
        'thread_id': worker_id,
        'reid_risk_worker_id': reid_risk_worker_id,
        'compliance_worker_id': compliance_worker_id,
        'availability_worker_id': availability_worker_id,
        'Desensitization_character_worker_id': Desensitization_character_worker_id,
        'Desensitization_quality_worker_id': Desensitization_quality_worker_id,
        'privacy_protection_worker_id': privacy_protection_worker_id
    }
    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
        WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    redis_client.hmset(worker_id, thread_process_mapping)

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


# 创建任务链
def process_data_chain(worker_id, data_table, sql_url):
    chain_task = chain(
        send_data_to_backend.s(worker_id, data_table, sql_url) |
        start_evaluate_enhanced.s(worker_id=worker_id)
    )
    chain_task.delay()


@celery.task(bind=True, base=MyTask)
def test_1(self, a1, a2, a3):
    print("父进程第一个参数：", a1)
    print("父进程第二个参数：", a2)
    print("父进程第三个参数：", a3)
    return a3


@celery.task(bind=True, base=MyTask)
def test_2(self, a1, a2):
    print("子进程第一个参数：", a1)
    print("子进程第二个参数：", a2)
    # print("子进程第三个参数：",a3)


def process_list_test(a1, a2, a3):
    chain_tt = chain(
        test_1.s(a1, a2, a3) |
        test_2.s(a2=a2)
    )
    chain_tt.delay()


# 辅助函数---执行评估指令

def update_task_status_to_completed(worker_id):
    # 将任务的状态描述改为已完成 并计算耗时
    # 构造 Redis 键
    task_key = worker_id + '-0'
    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
        WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 从 Redis 获取任务数据
    task_data_json = redis_client.get(task_key)
    if not task_data_json:
        print(f"Task with key {task_key} not found.")
        return {"error": "Task not found"}
    # 将 JSON 字符串转换为字典
    task_data = json.loads(task_data_json)

    # 更新状态为 "completed"
    task_data['status'] = 'completed'
    task_data['completion_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())  # 添加完成时间
    # 将时间字符串转换为 datetime 对象
    created_time_obj = datetime.strptime(task_data['start_time'], "%Y-%m-%d %H:%M:%S")
    completion_time_obj = datetime.strptime(task_data['completion_time'], "%Y-%m-%d %H:%M:%S")
    # 计算耗时
    time_diff = completion_time_obj - created_time_obj
    # 转换为可读格式 (天、小时、分钟、秒)
    days = time_diff.days
    hours, remainder = divmod(time_diff.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # 格式化耗时
    formatted_duration = f"{days}d {hours}h {minutes}m {seconds}s" if days > 0 else f"{hours}h {minutes}m {seconds}s"
    task_data['cost_time'] = formatted_duration

    # 将更新后的字典转换为 JSON 字符串
    updated_task_json = json.dumps(task_data)

    # 将更新后的任务数据保存回 Redis
    redis_client.set(task_key, updated_task_json)


# 执行评估指令
@celery.task(bind=True, base=MyTask)
def start_evaluate(self, src_url, un_table_name, to_url, table_name, QIDs, SA, ID, scene, des, k=2, l=2, t=0.95):
    print("*******************************评估启动**********************************")
    worker_id = start_evaluate.request.id
    logger.info('json评估任务启动，任务配置：%s %s', worker_id, SA)

    """"
    redis中存放的关于worker_id映射表
    key:    worker_id        value:     map,存放的是各个子任务的uuid
    key:    worker_id+'-0'   value:     评估任务的相关参数
    key:    worker_id+'-1'   value:     风险评估结果
    key:    worker_id+'-2'   value:     合规性评估结果
    key:    worker_id+'-3'   value:     可用性评估结果
    key:    worker_id+'-4'   value:     匿名集数据特征评估结果
    key:    worker_id+'-5'   value:     隐私保护性度量评估结果
    key: TASKS_ZSET_KEY  zset集合    存放任务id和时间戳
    """
    tt = worker_id
    worker_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # 保存参数到 Redis
    task_params = {
        'id': tt,
        'start_time': worker_time,
        'completion_time': '',
        'cost_time': '',
        "parameters": {
            'src_url': src_url,
            'un_table_name': un_table_name,
            'to_url': to_url,
            'table_name': table_name,
            'QIDs': QIDs,
            'SA': SA,
            'ID': ID,
            'k': k,
            'l': l,
            't': t,
            'data_scene': scene
        },
        "results": {},
        "rank": {},
        "evaluate": {},
        'description': des,
        'status': 'in_progress'
    }
    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
        WORKER_ID_MAP_REDISPASSWORD, TASKS_ZSET_KEY
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 将所有值转换为字符串
    # task_params = {k: (json.dumps(v) if isinstance(v, list) else v) for k, v in task_params.items()}
    # redis_client.hmset(worker_id+'-0', task_params)
    # 将字典转换为 JSON 字符串
    task_params_json = json.dumps(task_params)

    # 将 JSON 字符串存储到 Redis
    redis_client.set(worker_id + '-0', task_params_json)
    # 添加任务到 ZSET，使用时间戳作为分数
    timestamp = time.time()
    redis_client.zadd(TASKS_ZSET_KEY, {worker_id: timestamp})

    # 场景--任务id映射存到redis中
    redis_client.hset(f'scene:{scene}',worker_id,table_name)

    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker = reid_risk.delay(worker_id + '-1', src_url, un_table_name, to_url, table_name, QIDs, SA, ID)
    reid_risk_worker_id = reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(k, l, t, src_url, un_table_name, worker_id + '-2', QIDs,
                                         SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(k, l, t, src_url, un_table_name, worker_id + '-3',
                                             QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(k, l, t, src_url,
                                                                       un_table_name, worker_id + '-4', QIDs, SA, ID,
                                                                       '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)


    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(k, l, t, src_url, un_table_name,
                                                         worker_id + '-5', QIDs, SA, ID, '', '')
    privacy_protection_worker_id = privacy_protection_worker.id
    logger.info("隐私保护性度量评估已启动，子进程id是：%s", privacy_protection_worker_id)

    # 维护主线程和子线程id的映射
    # 只用于任务调度接口
    thread_process_mapping = {
        'thread_id': worker_id,
        'reid_risk_worker_id': reid_risk_worker_id,
        'compliance_worker_id': compliance_worker_id,
        'availability_worker_id': availability_worker_id,
        'Desensitization_character_worker_id': Desensitization_character_worker_id,
        'privacy_protection_worker_id': privacy_protection_worker_id
    }
    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
        WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    redis_client.hmset(worker_id, thread_process_mapping)

    # 轮询各个函数执行情况
    while True:
        # 获取各个函数的异步结果对象
        reid_risk_result = AsyncResult(reid_risk_worker.id)
        compliance_result = AsyncResult(compliance_worker.id)
        availability_result = AsyncResult(availability_worker.id)
        Desensitization_character_result = AsyncResult(Desensitization_character_worker.id)
        privacy_protection_result = AsyncResult(privacy_protection_worker.id)

        # 判断是否所有函数都成功执行
        if (reid_risk_result.successful() and compliance_result.successful() and
                availability_result.successful() and Desensitization_character_result.successful() and privacy_protection_result.successful()):
            # 如果全部完成，要更新任务状态
            logger.info("评估指标运算线程均已成功执行，开始更新任务状态")
            update_task_status_to_completed(worker_id)
            logger.info("任务状态已更新为完成，开始合并汇总任务结果")
            # 将任务合并汇总 计算总得分
            cal_rank_all(worker_id)
            # 打印成功信息
            print("{}所有评估任务均已成功执行！".format(worker_id))
            logger.info("{}所有评估任务均已成功执行！".format(worker_id))
            ## 将数据写入对应的 redis 数据库中
            
            break
        else:
            ## todo 线程任务挂掉
            # 若未全部成功执行，则等待一段时间后继续轮询
            time.sleep(15)  # 等待15秒后再次轮询




# 目前是写死的状态，todo 后续可以改为动态获取
@celery.task(bind=True, base=MyTask)
def reid_risk(self, uuid, src_url, un_table_name, to_url, table_name, QIDs, SA, ID):
    from .risk import reidentity_risk_assess
    import random
    ridrisk = round(random.uniform(0.01, 0.2), 2)
    sarisk = round(random.uniform(0.01, 0.2), 2)
    differ = round(random.uniform(0.01, 0.2), 2)
    from .config import Send_result
    Send_result(worker_id=uuid,res_dict={
        "重识别率":ridrisk,
        "统计推断还原率":sarisk,
        "差分攻击还原率":differ,
        "背景知识水平假设":4,
        "背景属性残缺率假设":0,
    })
    # ridrisk, sarisk = reidentity_risk_assess(uuid, src_url, un_table_name, to_url, table_name, QIDs, SA, ID, 2, 0.00004,
                                            #  0)

    logger.info('评估任务id: {},风险评估结果：{}'.format(uuid, ridrisk))
    logger.info('评估任务id: {},统计推断攻击结果: {}'.format(uuid, sarisk))




# 数据合规性分支
@celery.task(bind=True, base=MyTask)
def compliance(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    # 反序列化 Series
    try:
        handler = Config(k, l, t, url, address, worker_uuid, QIDs, SA, ID, '', '')
        _TemAll = handler._Function_Data()
        Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序

        Series_quasi = {
            'data': Series_quasi.tolist(),
            'index': [str(idx) for idx in Series_quasi.index]
        }
        _TemAll = _TemAll.to_dict(orient='records')
        # 反序列化 Series
        
        series_quasi = pd.Series(data=Series_quasi['data'], index=[ast.literal_eval(idx) if isinstance(idx, str) and (idx.startswith(('"', "'")) or idx.lstrip('-').replace('.', '').isdigit() or idx.startswith('(')) else idx for idx in Series_quasi['index']] if 'ast' in locals() else Series_quasi['index'])
        ## series_quasi = pd.Series(data=Series_quasi['data'], index=Series_quasi['index'])
        ## TODO 修改

        tem_all = pd.DataFrame(_TemAll)
        logger.info(f"Deserialized series_quasi: {series_quasi.head()}")
        logger.info(f"Deserialized tem_all: {tem_all.head()}")
    except Exception as e:
        logger.error(f"Error deserializing series_quasi or tem_all: {e}")
        raise
    ##数据合规性
    handler1 = Data_compliance(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    logger.info('数据合规性评估开始启动：{}'.format(worker_uuid))
    handler1.runL(series_quasi, tem_all)  ##传递准标识符集合，以及准标识符对应的数量

# 数据可用性评估函数分支
@celery.task(bind=True, base=MyTask)
def availability(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    handler = Config(k, l, t, url, address, worker_uuid, QIDs, SA, ID, '', '')
    _TemAll = handler._Function_Data()
    Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序

    Series_quasi = {
        'data': Series_quasi.tolist(),
        'index': [str(idx) for idx in Series_quasi.index]
    }
    _TemAll = _TemAll.to_dict(orient='records')
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[ast.literal_eval(idx) if isinstance(idx, str) and (idx.startswith(('"', "'")) or idx.lstrip('-').replace('.', '').isdigit() or idx.startswith('(')) else idx for idx in Series_quasi['index']] if 'ast' in locals() else Series_quasi['index'])
    tem_all = pd.DataFrame(_TemAll)
    ##数据可用性
    handler2 = Data_availability(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler2.runL(series_quasi, tem_all)

# 数据特征评分支
@celery.task(bind=True, base=MyTask)
def Desensitization_character(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url,
                              scene):
    handler = Config(k, l, t, url, address, worker_uuid, QIDs, SA, ID, '', '')
    _TemAll = handler._Function_Data()
    Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序

    Series_quasi = {
        'data': Series_quasi.tolist(),
        'index': [str(idx) for idx in Series_quasi.index]
    }
    _TemAll = _TemAll.to_dict(orient='records')
    #  反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[ast.literal_eval(idx) if isinstance(idx, str) and (idx.startswith(('"', "'")) or idx.lstrip('-').replace('.', '').isdigit() or idx.startswith('(')) else idx for idx in Series_quasi['index']] if 'ast' in locals() else Series_quasi['index'])
    tem_all = pd.DataFrame(_TemAll)
    ## 匿名集数据特征
    handler3 = Desensitization_data_character(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler3.runL(series_quasi, tem_all)

# 第二版废除该分支 合并到数据特征
# @celery.task(bind=True, base=MyTask)
# def Desensitization_quality(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url,
#                             scene):
#     # 反序列化 Series
#     series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
#     print(series_quasi)
#     tem_all = pd.DataFrame(_TemAll)
#     ##匿名数据质量评估
#     handler4 = Desensitization_data_quality_evalution(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
#     handler4.runL(series_quasi, tem_all)  ##传递准标识符集合，以及准标识符对应的数量

# 数据安全性分支
@celery.task(bind=True, base=MyTask)
def privacy_protection(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    handler = Config(k, l, t, url, address, worker_uuid, QIDs, SA, ID, '', '')
    _TemAll = handler._Function_Data()
    Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序

    Series_quasi = {
        'data': Series_quasi.tolist(),
        'index': [str(idx) for idx in Series_quasi.index]
    }
    _TemAll = _TemAll.to_dict(orient='records')
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[ast.literal_eval(idx) if isinstance(idx, str) and (idx.startswith(('"', "'")) or idx.lstrip('-').replace('.', '').isdigit() or idx.startswith('(')) else idx for idx in Series_quasi['index']] if 'ast' in locals() else Series_quasi['index'])
    tem_all = pd.DataFrame(_TemAll)
    ##隐私保护性度量
    handler5 = Data_Security(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler5.runL(series_quasi, tem_all)  ##传递准标识符集合，以及准标识符对应的数量

# 为甚要反序列化 Series？
# 前端或者别的服务把 pandas.Series 转成 dict（序列化）

# 传给 celery 任务（其实已经不是 Series，而是“可以json化的结构”）

# 你用 pandas 的构造函数重新拼成 Series（反序列化）




def sendvalue(worker_id, key, value, valuetype):
    """
    向 Redis 添加 key 和 value，支持存储单一类型数据或列表数据，并确保数据类型被保留。
    参数:
        worker_ud (str): Redis 连接字符串。
        key (str): 要存储的小指标的名称。
        value (int, float, list): 要存储的小指标结果值Value，可以是单一值或列表。
        valuetype (type): 值的类型（int 或 float 或 list）。
    """
    try:
        # 初始化 Redis 连接
        from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
            WORKER_ID_MAP_REDISPASSWORD, TASKS_ZSET_KEY
        redis_conn = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                       db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)

        current_value = redis_conn.get(worker_id)
        if not current_value:
            raise ValueError(f"No data found for worker_id '{worker_id}'")

        # 解析现有数据
        data = json.loads(current_value)

        # 确保 "results" 字段存在并为字典
        if "results" not in data or not isinstance(data["results"], dict):
            data["results"] = {}

        # 验证值的类型
        if isinstance(value, list):
            # 如果是列表，验证每个元素是否匹配指定类型
            if not all(isinstance(v, valuetype) for v in value):
                raise ValueError(f"All elements in the list must be of type {valuetype}")
        else:
            # 如果是单一值，确保值的类型匹配指定类型
            if not isinstance(value, valuetype):
                raise ValueError(f"Value must be of type {valuetype}, but got {type(value)}")

        # 添加或更新 "results" 中的键值
        data["results"][key] = value

        # 保存修改后的数据回 Redis
        redis_conn.set(worker_id, json.dumps(data))
        print(f"Updated 'results' field for worker_id '{worker_id}': {data['results']}")

    except Exception as e:
        print(f"Error occurred: {e}")

def SendCombineTarget(worker_id, key, value, valuetype):
    """
    写大指标的值
    向 Redis 添加 key 和 value，支持存储单一类型数据或列表数据，并确保数据类型被保留。

    参数:
        worker_id (str): Redis 中的key。
        key (str): 要存储的指标名称。
        value (int, float, list): 要存储的指标结果值，可以是单一值或列表。
        valuetype (type): 值的类型（int 或 float 或 list）。
    """
    try:
        # 初始化 Redis 连接
        from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
            WORKER_ID_MAP_REDISPASSWORD, TASKS_ZSET_KEY
        redis_conn = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                       db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        val = redis_conn.get(worker_id)
        if val:
            # 解析 JSON 数据
            data = json.loads(val)

            # 写入大指标的值
            data["rank"][key] =value
            redis_conn.set(worker_id, json.dumps(data))
            print(f"List {value} of type {valuetype.__name__} stored in Redis with key '{key}'")
        else:
            print("errno","worker_id+'-0' 不存在")

    except Exception as e:
        print(f"Error occurred: {e}")

def Send_Combine_eval(worker_id, key, value):
    """

    """
    try:
        # 初始化 Redis 连接
        from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
            WORKER_ID_MAP_REDISPASSWORD, TASKS_ZSET_KEY
        redis_conn = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                       db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        val = redis_conn.get(worker_id)
        if val:
            # 解析 JSON 数据
            data = json.loads(val)

            # 写入大指标的值
            data["evaluate"][key] =value
            redis_conn.set(worker_id, json.dumps(data))
            print("写入成功：",key)
        else:
            print("errno","worker_id+'-0' 不存在")

    except Exception as e:
        print(f"Error occurred: {e}")



def getvalue(worker_id, key):
    """
    从 Redis 中获取 worker_id 对应的数据，并从 "results" 字段中获取指定 key 的值。

    参数:
        worker_id (str): Redis 中存储的 worker ID。
        key (str): 指定的指标名称。

    返回:
        tuple: (值, 类型)，例如 (123, int)，(45.6, float)，或 ([1, 2, 3], int)。
    """
    try:
        # 初始化 Redis 连接
        from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, \
            WORKER_ID_MAP_REDISPASSWORD, TASKS_ZSET_KEY
        redis_conn = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT,
                                       db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        # 从 Redis 获取 JSON 数据
        current_value = redis_conn.get(worker_id)
        if not current_value:
            print(f"No data found for worker_id '{worker_id}'")
            return None, None

        # 解析 JSON 数据
        data = json.loads(current_value)

        # 检查 "results" 字段是否存在且为字典
        if "results" not in data or not isinstance(data["results"], dict):
            print(f"'results' field not found or invalid in data for worker_id '{worker_id}'")
            return None, None

        # 获取 "results" 中的 key 的值
        result_value = data["results"].get(key)
        if result_value is None:
            print(f"Key '{key}' not found in 'results'")
            return None, None

        # 检查值的类型
        if isinstance(result_value, list):
            # 确定列表中元素的类型
            valuetype = type(result_value[0]) if result_value else None
            return result_value, valuetype
        else:
            return result_value, type(result_value)

    except Exception as e:
        print(f"Error occurred: {e}")
        return None, None



# 可用性  已完成
def CombineTarget_Data_availability(worker_id,num):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    可用性的指标结果写入redis中
    由于全是负向指标，所以应该是1减去所得值

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:
        List_Data_availability = ["数据可辨别度", "数据记录匿名率","平均泛化程度","数据损失度",
                                  "基于熵的平均数据损失度","唯一记录占比"]
        res={
            "evaluate":"",
            "desp":"",
            "评估结果":"",
            "改进意见":"",
        }
        # 将大指标结果写入
        SendCombineTarget(worker_id, "可用性结果", num, float)
        logger.info("可用性指标融合完成，结果：%s",num)

        if num >= 0.9:
            res["evaluate"] = "优秀"
            res["desp"] = "可用性极高，仅需持续优化和监控。"
            res["评估结果"] = " 数据完整性：数据的完整性和区分能力接近最佳，隐私保护措施对可用性的影响极小。数据损失程度：隐私保护技术引入的隐私损失度接近于 0，意味着数据分析精度几乎未受影响。"
            res["改进意见"] = "1. 持续监控和优化：继续监控隐私保护措施的效果，确保其在不同场景下保持高可用性。2. 优化隐私参数：针对隐私保护措施，定期优化相关参数，进一步提升数据可用性。3. 细化泛化策略：在高敏感场景中尝试更细粒度的泛化，减少信息损失。"

        elif num >= 0.8:
            res["evaluate"] = "良好"
            res["desp"] = "可用性较高，适度优化等价组和保护算法。"
            res["评估结果"] = "数据区分能力：保持良好，绝大部分记录之间仍然可区分。隐私损失度：对部分分析任务产生轻微影响，但整体数据集仍具备较强实用性。"
            res["改进意见"] = "1. 优化等价组划分策略：通过更合理的划分，减少平均泛化程度，提升数据分析的精确度。2. 结合多种隐私保护技术：在现有技术基础上，尝试混合使用隐私保护技术，如扰动和屏蔽，进一步优化数据保留质量。 动态调整隐私保护参数：根据分析需求调整隐私保护措施，特别是在不影响安全性的情况下提升数据精确性。"

        elif num >= 0.6:
            res["evaluate"] = "中等"
            res["desp"] = "可用性中等，需要全面优化泛化策略和隐私保护措施。"
            res["评估结果"] = "数据精确性：精确性和分析能力下降，对数据驱动的决策有一定限制。隐私损失度：隐私保护措施显著增加了数据的不确定性，影响了部分实用性。"
            res["改进意见"] =  "1. 优化隐私保护算法：改进隐私保护算法，减少泛化范围。2. 细化等价组：缩小等价组规模，提高数据区分能力，增强对分析任务的支持。3. 提高保护措施的灵活性：根据应用场景调整保护策略，对关键性分析任务减少泛化和损失。"

        else:
            res["evaluate"] = "较低"
            res["desp"] = "可用性较低，必须重新设计隐私保护框架以提升数据的实用性。"
            res["评估结果"] = "数据区分能力：区分能力严重下降，数据对分析任务的支持有限。隐私损失度：保护措施对数据造成的影响过大，难以有效支撑实际应用需求。"
            res["改进意见"] =  "1. 重新设计隐私保护框架：采用更精细化的保护策略，结合泛化、扰动、数据屏蔽等技术，减少不必要的隐私损失。2. 减少等价组规模：通过优化等价组划分算法，减少每组记录数量，提升数据的区分能力和精确性。3. 增强灵活性：允许在关键任务中动态调整保护级别，在必要时降低隐私保护强度以提高数据实用性。4. 持续评估与调整：通过实时评估可用性和隐私保护效果，不断优化参数和策略。"
        Send_Combine_eval(worker_id,"可用性结果",res)
        return num

    except Exception as e:
        print(f"Error occurred: {e}")


# 合规性  已完成
def CombineTarget_Data_compliance(worker_id,num):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并合规性的指标，将结果写入redis中
    由于全是正向指标，所以就是所得值

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
    """
    try:

        SendCombineTarget(worker_id, "合规性结果",num, float)
        logger.info("合规性结果%s",num)

        res={
            "evaluate":"",
            "desp":"",
            "评估结果":"",
            "改进意见":"",
        }


        if num >= 0.9:
            res["evaluate"] = "优秀"
            res["desp"] = "隐私保护效果极佳，仅需保持现有措施和监控。"
            res["评估结果"] = "合规性结果表明隐私保护措施全面到位，数据集中敏感属性的隐私风险降至较低水平。所有子指标（如 K-匿名性、L-多样性、(α,k)-匿名性 和 T-紧密性）均达到或超过基本合规要求，表明等价类划分合理且均衡，敏感属性的分布差异可控且一致性良好。同质攻击、重识别攻击和背景知识攻击的风险显著降低，数据的安全性和匿名性得到了充分保障。此类数据适用于高隐私敏感场景。"
            res["改进意见"] = "1.	定期合规性检查：持续评估数据集的合规性，确保在动态环境下合规性不变。2.	提高匿名和多样性级别：在高隐私需求的场景中，可考虑提高 K 和 L 值，进一步强化数据保护。3.	动态调整紧密性阈值：根据数据发布情况，在保持安全的同时提升数据分析精度。"


        elif num >= 0.8:
            res["evaluate"] = "良好"
            res["desp"] = "隐私保护良好，适度优化高风险属性和等价类。"
            res["评估结果"] = "合规性结果表明数据合规性较高，但仍有一定优化空间。隐私保护措施在大部分情况下表现良好，某些子指标可能稍有不足，导致部分敏感属性的分布差异略高或多样性水平稍低。整体而言，隐私保护已经有效抵御了大部分攻击风险，但对于高敏感度场景，可能需要提高匿名性级别，以进一步平衡隐私保护和数据实用性。"
            res["改进意见"] = "1. 优化等价类划分策略: 重新划分高风险等价类，确保其均衡性和多样性。2. 增强隐私保护技术: 使用其他高级隐私保护技术，加强高风险属性的保护。3. 定期评估: 通过定期合规性评估，识别和修复潜在风险。"

        elif num >= 0.6:
            res["evaluate"] = "中等"
            res["desp"] = "隐私保护一般，需要对高风险指标进行重点优化。"
            res["评估结果"] = "合规性结果表示隐私保护和数据安全性已达到基础水平，但仍面临一定风险。部分等价类的敏感属性分布差异较大，或等价类记录数不足，可能暴露于重识别攻击和同质攻击风险。尽管隐私保护措施对多数敏感属性有效，但数据的某些部分可能需要进一步优化，以减少敏感属性的泄露风险并提高多样性级别，确保整体数据合规性满足更高要求。"
            res["改进意见"] = "1. 提高匿名级别: 增加 K 和 L 的值，增强隐私保护效果。2. 动态调整等价类划分: 合理调整等价类划分，减少敏感属性的频率偏差。3. 增加数据失真: 通过添加噪声或随机化技术，进一步降低敏感属性的重识别风险。4. 针对高风险等价类进行重点优化: 对高风险组合实施额外保护措施。"

        else:
            res["evaluate"] = "较低"
            res["desp"] = "隐私保护不足，需全面升级技术和策略以提升合规性。"
            res["评估结果"] = "合规性结果表明隐私保护措施存在明显不足，数据的合规性和安全性无法满足基本标准。多数子指标未达标，导致敏感属性的分布差异过大，重识别和同质攻击风险显著增加。等价类划分不均或频率控制不合理，使得隐私泄露风险极高。此类数据可能不适合发布，建议全面优化隐私保护策略，提高匿名性和多样性水平，减少分布不一致性，确保数据的安全与合规。"
            res["改进意见"] = "1. 全面升级隐私保护技术: 引入更高级的隐私保护方法。2. 重新划分等价类: 优化等价类的划分策略，确保所有等价类满足基本要求。3. 动态调整隐私保护措施: 根据实际数据使用情况，实时更新隐私保护策略。4. 加强数据失真: 通过更深层次的模糊化和随机化技术，减少敏感属性的可识别性。"
        Send_Combine_eval(worker_id,"合规性结果",res)
    except Exception as e:
        print(f"Error occurred: {e}")


# 安全性 已完成
def CombineTarget_privacy_protection_metrics(worker_id,num):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并安全性的指标，将结果写入redis中
    由于全是负向指标，所以应该是1减去所得值

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
    """
    try:
        List_privacy_protection_metrics = ["敏感属性重识别风险", "基于准标识符重识别风险", "整体重识别风险"]
        SendCombineTarget(worker_id, "安全性结果",  num, float)
        logger.info("安全性融合完成，结果：%s",  num)

        res={
            "evaluate":"",
            "desp":"",
            "评估结果":"",
            "改进意见":"",
        }

        if num >= 0.9:
            res["evaluate"] = "优秀"
            res["desp"] = "安全性非常高，仅需持续优化与监控。"
            res["评估结果"] = "数据集中敏感属性、整体数据以及单个属性的重识别风险均处于极低水平。隐私保护策略已成功减少了数据的不确定性，高风险等价组被有效分散或泛化。 用户隐私受到了严格的保护，发布数据的安全性极高。"
            res["改进意见"] = "1. 持续优化现有隐私保护技术: 在发布前进一步优化差分隐私参数，以保持低风险。2. 定期监控与评估: 通过周期性检查，确保数据发布后的动态环境中安全性仍然可靠。3. 提升泛化和动态调整: 针对潜在风险高的等价组，实时调整泛化策略，确保其持续稳定。"

        elif num >= 0.8:
            res["evaluate"] = "良好"
            res["desp"] = "良好安全性，适度加强高风险属性保护。"
            res["评估结果"] = "敏感属性和整体数据的重识别风险相对较低，但仍需关注可能的风险点。 高风险属性 可能在某些情况下构成隐私泄露的威胁。数据发布的安全性总体较高，但需要关注极少数等价组。"
            res["改进意见"] = "1. 加强高风险属性保护: 对 敏感属性进一步应用隐私保护算法。2. 优化泛化策略: 针对发现的高风险等价组，实施更细粒度的泛化操作。3. 提升数据安全发布流程: 在数据发布前，对高风险组合应用额外的模糊处理。4. 用户隐私教育: 发布方可通过文档或声明，让用户了解隐私保护机制及其作用。"

        elif num >= 0.6:
            res["evaluate"] = "中等"
            res["desp"] = "中等安全性，需全面优化高风险属性及等价组。"
            res["评估结果"] = " 敏感属性和整体数据的重识别风险已达中等水平，部分属性或组合存在较大隐私泄露风险。 高风险等价组仍较为集中，某些单个属性的重识别风险较低。 数据安全性有明显提升空间。"
            res["改进意见"] =  "1. 实施更高级别的隐私保护措施: 优先考虑使用差分隐私或 k-匿名方法。2. 加强泛化深度: 进一步泛化高风险属性，减少重识别可能性。3. 动态调整等价组: 根据风险高的属性组合调整等价组的划分，降低其重识别概率。4. 引入数据失真技术: 通过适度添加噪声或随机化技术，进一步降低数据的可识别性。"

        else:
            res["evaluate"] = "较低"
            res["desp"] = "安全性不足，必须升级技术、调整策略并限制数据发布。"
            res["评估结果"] = " 数据集中敏感属性和整体数据的重识别风险较高，可能造成重大隐私泄露。  当前隐私保护技术效果有限，需要全面优化。"
            res["改进意见"] =  "1. 升级隐私保护技术: 立即采用高级技术，限制数据重识别能力。2. 深度泛化策略: 对所有高风险属性进行更深层次的泛化或模糊化处理。3. 严格数据筛选: 限制敏感数据的发布，尤其是重识别风险极高的属性组合。4. 实施动态风险监控: 建立持续监控机制，实时评估并修复可能的隐私漏洞。5. 多级保护机制: 联合使用多种隐私保护技术，确保高风险数据得到足够的安全性支持。"
        Send_Combine_eval(worker_id, "安全性结果", res)
    except Exception as e:
        print(f"Error occurred: {e}")


# 数据质量  已完成
def CombineTarget_data_quality_evalution(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并数据质量的指标，将结果写入redis中
    由于全是负向指标，所以应该是1减去所得值
    10:7:7:9:5

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:
        List_data_quality_evalution = ["唯一性", "分布泄露", "熵泄露", "正面信息披露", "KL散度"]

        ##权重随着数据隐私属性个数变化而变化
        ##全部都以列表形式存储
        ##按照   10:7:7:9:5 来
        Weight_data_quality_evalution = [10, 7, 7, 9, 5]
        if len(List_data_quality_evalution) != len(Weight_data_quality_evalution):
            raise ValueError("安全性指标类型的数量必须与指标权重个数一致")

        #分母，其代表每个列表长度乘上对应权重的和
        sums_Denominator = 0

        ##融合指标
        Ans_data_quality_evalution = 0.0
        valuesALL_data_quality_evalution = []
        length = 0
        for i in range(len(List_data_quality_evalution)):
            # 使用 getvalue 函数从 Redis 中获取数据
            value, valuetype = getvalue(worker_id, List_data_quality_evalution[i])
            if value is None:
                print(f"No value found for key '{List_data_quality_evalution[i]}' in Redis.")
                return

            ##必须确保每一个key对应的值均为列表类型
            sums_Denominator += (Weight_data_quality_evalution[i] * len(value))
            valuesALL_data_quality_evalution.append(value)

        for i in range(len( valuesALL_data_quality_evalution )):
            for eachNum in  valuesALL_data_quality_evalution[i]:
                Ans_data_quality_evalution += ( eachNum *  Weight_data_quality_evalution[i] / sums_Denominator)

        ##由于全是负向指标，所以应该用1 - 所得值
        Ans_data_quality_evalution =  1 - Ans_data_quality_evalution

        SendCombineTarget(worker_id, "数据质量结果", Ans_data_quality_evalution, float)
        # print("数据质量结果", Ans_data_quality_evalution)

        res={
            "evaluate":"",
            "desp":"",
            "评估结果":"",
            "改进意见":"",
        }


        if Ans_data_quality_evalution >= 0.9:
            res["evaluate"] = "优秀"
            res["desp"] = "数据质量极高，隐私保护效果显著，但需改善数据的实用性。"
            res["评估结果"] = "隐私保护：隐私保护技术应用后，数据隐私泄露风险显著降低，几乎不存在重识别和敏感信息泄露风险。数据质量：虽然隐私保护效果优异，但对数据的可用性和精确度影响较大，实用性有限。"
            res["改进意见"] = "1.平衡隐私与实用性：通过调整隐私保护强度（如减少泛化或增加数据扰动）提高数据的实用性。2.优化等价类划分：细化等价类划分，减少分布和熵泄露，提升数据分析的可靠性。3.混合保护技术：结合多种隐私保护技术，确保隐私性与数据质量的平衡。"

        elif Ans_data_quality_evalution >= 0.8:
            res["evaluate"] = "良好"
            res["desp"] = "数据质量较高，隐私保护和数据实用性之间有较好的平衡。"
            res["评估结果"] = "隐私保护：隐私保护技术对减少敏感信息泄露有明显效果，但对数据分析的支持能力仍需加强。数据质量：数据的区分能力和精确度较好，但隐私保护对部分分析任务可能造成限制。"
            res["改进意见"] = "1.优化数据发布策略：减少隐私保护对分布和熵的影响，确保数据分布的合理性。2.细化隐私参数：调整隐私保护参数，适当降低保护强度以提高数据实用性。3.动态调整等价类：根据应用场景，优化等价类划分策略，增强数据的分析能力。"


        elif Ans_data_quality_evalution >= 0.6:
            res["evaluate"] = "中等"
            res["desp"] = "数据质量中等，隐私保护技术影响显著，需优化策略。"
            res["评估结果"] = "隐私保护：隐私保护对减少敏感信息泄露起到了作用，但在部分场景中仍存在优化空间。数据质量：隐私保护导致数据精确性和区分能力下降，实用性受限。"
            res["改进意见"] = "1.加强数据质量优化：通过优化等价类划分策略，减少熵泄露和分布泄露的负面影响。2.调整隐私保护技术：尝试数据扰动等方法，控制数据分布的一致性变化。3.动态监控和反馈：定期评估数据质量，调整隐私保护措施以改善数据可用性。"

        else:
            res["evaluate"] = "较低"
            res["desp"] = "数据质量较低，隐私保护强度不足，需全面加强保护措施。"
            res["评估结果"] = "隐私保护：隐私保护效果较差，敏感信息存在显著泄露风险。数据质量：数据质量尚可，但隐私保护效果不足可能导致敏感信息泄露。"
            res["改进意见"] = "1.提升隐私保护强度：采用更高强度的隐私保护技术。2.优化分布控制策略：增加数据扰动和泛化，减少分布泄露的影响。3.减少敏感信息暴露：动态调整数据发布内容，减少敏感属性的信息泄露风险。"

        Send_Combine_eval(worker_id, "数据质量结果", res)
        return Ans_data_quality_evalution

    except Exception as e:
        print(f"Error occurred: {e}")



# 数据特征  已完成
def CombineTarget_Data_character(worker_id,num):
    # 参数说明:
    # ----------
    # worker_id : str
    #     当前处理任务的唯一标识符，用于定位任务或用户。
    # num : float
    #     数据特征评估得分（范围通常为0~1），用于判断当前数据的隐私保护能力及数据特征表现。
    try:

        SendCombineTarget(worker_id, "数据特征结果", num, float)
        logger.info("数据特征融合完成，结果：%s", num)
        if num >= 0.85:
            res = {
                "evaluate": "优秀",
                "desp": "数据特征表现非常优异",
                "评估结果": "数据特征表现优异，指标全面满足隐私保护要求。准标识符维数和敏感属性维数较低，等价组划分均衡，敏感属性保护到位。结果显示数据具有较强的内在隐私保护能力。",
                "改进意见": "1. 保持现有隐私保护措施: 持续监测并优化数据发布策略。\n2. 动态调整等价组划分: 根据实际需求进一步优化等价组的划分，确保数据实用性与隐私性平衡。\n3. 提升敏感属性保护技术: 继续采用先进技术，保持高水平的隐私保护。",
            }
        elif num >= 0.7:
            res = {
                "evaluate": "良好",
                "desp": "数据特征表现良好",
                "评估结果": "数据在多数隐私保护指标上表现良好，准标识符控制合理，等价组划分基本均衡，但部分敏感属性仍存在泄露风险。",
                "改进意见": "1. 加强对敏感属性的保护，特别是风险点的识别。\n2. 进一步优化等价组设计，提升整体隐私保护水平。\n3. 引入差分隐私等增强措施，提升鲁棒性。",
            }
        elif num >= 0.5:
            res = {
                "evaluate": "中等",
                "desp": "数据特征表现中等",
                "评估结果": "数据的隐私保护能力处于中等水平，部分指标未达标，存在提升空间。等价组划分存在偏差，敏感属性可能暴露。",
                "改进意见": "1. 优化数据预处理流程，减少敏感属性暴露概率。\n2. 考虑引入更严格的隐私建模技术（如k-匿名、l多样性等）。\n3. 加强监测机制，持续评估保护效果。",
            }
        else:
            res = {
                "evaluate": "较低",
                "desp": "数据特征表现较弱",
                "评估结果": "当前数据隐私保护能力较弱，存在明显风险。敏感属性暴露严重，等价组划分不合理，亟需改进。",
                "改进意见": "1. 重新设计数据发布策略，优先处理敏感属性泄露问题。\n2. 加强等价组划分算法的精度与鲁棒性。\n3. 引入强隐私保护机制，如差分隐私、同态加密等。",
            }
        Send_Combine_eval(worker_id, "数据特征结果", res)
    except Exception as e:
        print(f"Error occurred: {e}")


# 数据隐私风险
def CombineTarget_Data_riskvalue(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并数据特征的指标，将结果写入redis中

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:

        SendCombineTarget(worker_id, "隐私风险度量", 0.9, float)
        logger.info("隐私风险度量融合完成，结果：%s", 0.9)
        res={
            "evaluate":"优秀",
            "desp":"隐私保护效果极佳，风险极低，建议维持现有策略并动态监测。",
            "评估结果":"数据集隐私泄露风险极低，所有隐私保护指标均表现出色。攻击者通过直接或间接方式识别用户身份的可能性非常小。敏感属性保护措施全面有效，数据发布后的隐私风险控制在最小范围内。",
            "改进意见":"1.维持现有隐私保护措施：继续使用当前隐私保护技术，同时保持对最新技术发展的跟踪和适应。2.动态隐私保护评估：定期评估隐私保护效果，确保在数据使用环境变化时仍能保持较低的隐私风险。3.优化数据发布策略：在低风险水平下优化数据的实用性，使数据分析价值最大化。",
        }
        Send_Combine_eval(worker_id,"隐私风险度量",res)
        return 0.9

    except Exception as e:
        print(f"Error occurred: {e}")


from celery_task.func.network import Getcompliance, GetCharacter, GetAvailability, GetSecurity


# 增加一个函数来处理 None 值
def none2zero(x):
    return x if x is not None else 0

# 总函数
# 该函数主要是对-0处写入evaluate字段和rank字段
def cal_rank_all(worker_id):
    # todo -1三种攻击写死的，需要修改
    num1 = Getcompliance(worker_id + '-2')
    num2 = GetCharacter(worker_id + '-4')
    num3 = GetAvailability(worker_id + '-3')
    num4 = GetSecurity(worker_id + '-5')

    FinalNum = num1 * 0.1 + num2 * 0.3 + num3 * 0.3 + num4 * 0.3
    # FinalNum 修改规则导入 todo
    worker_id=worker_id+'-0'

    CombineTarget_Data_availability(worker_id,num3)# 可用性
    CombineTarget_Data_character(worker_id,num2)# 数据特征
    CombineTarget_Data_riskvalue(worker_id)# 数据隐私风险
    CombineTarget_privacy_protection_metrics(worker_id,num4)# 安全性
    CombineTarget_Data_compliance(worker_id,num1)# 合规性
    res = {
        "value":FinalNum,
        "rank":"",
    }
    # todo 评价逻辑final写死的，
    if FinalNum >= 0.9:
        res["rank"] = "优秀"
    elif FinalNum >= 0.8:
        res["rank"] = "良好"
    elif FinalNum >= 0.6:
        res["rank"] = "中等"
    else:
        res["rank"] = "较低"

    Send_Combine_eval(worker_id,"评估得分",res)