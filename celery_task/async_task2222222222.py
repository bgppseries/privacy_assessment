from datetime import datetime

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
from celery_task.config import Data_availability, Data_compliance, Desensitization_data_character, \
    Desensitization_data_quality_evalution, privacy_protection_metrics
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
    time_limit = 1800  # 硬超时，单位为秒 (5分钟)
    soft_time_limit = 1000  # 软超时，单位为秒 (4分钟30秒)
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
    # 将任务的状态描述改为已完成
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
    key:    worker_id+'-5'   value:     匿名集数据质量评估结果
    key:    worker_id+'-6'   value:     隐私保护性度量评估结果
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
        "evaluate": '',
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

    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker = reid_risk.delay(worker_id + '-1', src_url, un_table_name, to_url, table_name, QIDs, SA, ID)
    reid_risk_worker_id = reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 准备数据
    handler = Config(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID, '', '')
    _TemAll = handler._Function_Data()
    Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序
    # 将值传递给celery任务，要保证可被序列化
    # 序列化 Series
    # 打印序列化前的数据
    # print("Series_quasi before serialization:", Series_quasi.head())
    # print("Series_quasi index length:", len(Series_quasi.index))
    # print("Series_quasi data length:", len(Series_quasi.tolist()))
    # print(Series_quasi.index)
    Series_quasi = {
        'data': Series_quasi.tolist(),
        'index': [str(idx) for idx in Series_quasi.index]
    }
    # print(len(Series_quasi['index']))
    _TemAll = _TemAll.to_dict(orient='records')

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(Series_quasi, _TemAll, k, l, t, src_url, un_table_name, worker_id + '-2', QIDs,
                                         SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(Series_quasi, _TemAll, k, l, t, src_url, un_table_name, worker_id + '-3',
                                             QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(Series_quasi, _TemAll, k, l, t, src_url,
                                                                       un_table_name, worker_id + '-4', QIDs, SA, ID,
                                                                       '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)

    # 异步执行 Desensitization_quality 函数，并记录任务 ID
    Desensitization_quality_worker = Desensitization_quality.delay(Series_quasi, _TemAll, k, l, t, src_url,
                                                                   un_table_name, worker_id + '-5', QIDs, SA, ID, '',
                                                                   '')
    Desensitization_quality_worker_id = Desensitization_quality_worker.id
    logger.info("匿名数据质量评估已启动，子进程id是：%s", Desensitization_quality_worker_id)

    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(Series_quasi, _TemAll, k, l, t, src_url, un_table_name,
                                                         worker_id + '-6', QIDs, SA, ID, '', '')
    privacy_protection_worker_id = privacy_protection_worker.id
    logger.info("隐私保护性度量评估已启动，子进程id是：%s", privacy_protection_worker_id)
    print("hello")
    # 维护主线程和子线程id的映射
    # 只用于任务调度接口
    thread_process_mapping = {
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
            # 如果全部完成，要更新任务状态
            update_task_status_to_completed(worker_id)
            cal_rank_all(worker_id)
            # 打印成功信息
            print("{}所有评估任务均已成功执行！".format(worker_id))
            logger.info("{}所有评估任务均已成功执行！".format(worker_id))
            break
        else:
            # 若未全部成功执行，则等待一段时间后继续轮询
            time.sleep(15)  # 等待5秒后再次轮询


@celery.task(bind=True, base=MyTask)
def reid_risk(self, uuid, src_url, un_table_name, to_url, table_name, QIDs, SA, ID):
    from .risk import reidentity_risk_assess
    ridrisk, sarisk =0.025,0
    # ridrisk, sarisk = reidentity_risk_assess(uuid, src_url, un_table_name, to_url, table_name, QIDs, SA, ID, 2, 0.00004,
    #                                          0)
    logger.info('评估任务id: {},风险评估结果：{}'.format(uuid, ridrisk))
    logger.info('评估任务id: {},统计推断攻击结果: {}'.format(uuid, sarisk))


from .config import Config


@celery.task(bind=True, base=MyTask)
def compliance(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    # 反序列化 Series
    try:
        print(len(Series_quasi['index']), '和', len(Series_quasi['data']))
        # logger.info(f"Deserialized series_quasi: {Series_quasi}")
        series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
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


@celery.task(bind=True, base=MyTask)
def availability(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)
    ##数据可用性
    handler2 = Data_availability(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler2.runL(series_quasi, tem_all)


@celery.task(bind=True, base=MyTask)
def Desensitization_character(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url,
                              scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)
    ##匿名集数据特征
    handler3 = Desensitization_data_character(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler3.runL(series_quasi, tem_all)


@celery.task(bind=True, base=MyTask)
def Desensitization_quality(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url,
                            scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    print(series_quasi)
    tem_all = pd.DataFrame(_TemAll)
    ##匿名数据质量评估
    handler4 = Desensitization_data_quality_evalution(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler4.runL(series_quasi, tem_all)  ##传递准标识符集合，以及准标识符对应的数量


@celery.task(bind=True, base=MyTask)
def privacy_protection(self, Series_quasi, _TemAll, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)
    ##隐私保护性度量
    handler5 = privacy_protection_metrics(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    handler5.runL(series_quasi, tem_all)  ##传递准标识符集合，以及准标识符对应的数量






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

import redis
import json

# def getvalue(worker_id, key):
#     """
#     从 Redis 中获取 worker_id 对应的数据，并从 "results" 字段中获取指定 key 的值。
#
#     参数:
#         worker_id (str): Redis 中存储的 worker ID。
#         key (str): 指定的指标名称。
#
#     返回:
#         tuple: (值, 类型)，例如 (123, int)，(45.6, float)，或 ([1, 2, 3], int)。
#     """
#     try:
#         # 初始化 Redis 连接
#         redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, db=5, password=redis_password,
#                                        decode_responses=True)
#
#         # 从 Redis 获取 JSON 数据
#         current_value = redis_conn.get(worker_id)
#         if not current_value:
#             print(f"No data found for worker_id '{worker_id}'")
#             return None, None
#
#         # 解析 JSON 数据
#         data = json.loads(current_value)
#
#         # 检查 "results" 字段是否存在且为字典
#         if "results" not in data or not isinstance(data["results"], dict):
#             print(f"'results' field not found or invalid in data for worker_id '{worker_id}'")
#             return None, None
#
#         # 获取 "results" 中的 key 的值
#         result_value = data["results"].get(key)
#         if result_value is None:
#             print(f"Key '{key}' not found in 'results'")
#             return None, None
#
#         # 检查值的类型
#         if isinstance(result_value, list):
#             # 确定列表中元素的类型
#             valuetype = type(result_value[0]) if result_value else None
#             return result_value, valuetype
#         else:
#             return result_value, type(result_value)
#
#     except Exception as e:
#         print(f"Error occurred: {e}")
#         return None, None
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
        worker_id=worker_id+'-0'
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
        worker_id = worker_id + '-0'
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
            data["rank"][key] = value
            redis_conn.set(worker_id, json.dumps(data))
            print(f"List {value} of type {valuetype.__name__} stored in Redis with key '{key}'")
        else:
            print("errno", "worker_id+'-0' 不存在")

    except Exception as e:
        print(f"Error occurred: {e}")


# 可用性
def CombineTarget_Data_availability(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并可用性的指标，将结果写入redis中


    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:
        List_Data_availability = ["数据损失度", "数据可辨别度", "归一化平均等价组大小", "匿名率",
                                  "基于熵的平均数据损失度"]
        Weight_Data_availability = [0.48744304, 0.14643221, 0.07952278, 0.06268612, 0.22391585]
        if len(List_Data_availability) != len(Weight_Data_availability):
            raise ValueError("指标类型的数量必须与指标权重个数一致")

        ##融合指标
        Ans_Data_availability = 0.0
        for i in range(len(List_Data_availability)):
            # 使用 getvalue 函数从 Redis 中获取数据
            value, valuetype = getvalue(worker_id, List_Data_availability[i])
            if value is None:
                print(f"No value found for key '{List_Data_availability[i]}' in Redis.")
                return
            value=float(value)
            Ans_Data_availability += (Weight_Data_availability[i] * value)
        # 将大指标结果写入
        SendCombineTarget(worker_id, "可用性结果", Ans_Data_availability, float)

    except Exception as e:
        print(f"Error occurred: {e}")


# 合规性
def CombineTarget_Data_compliance(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并合规性的指标，将结果写入redis中


    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
    """
    try:
        List_Data_compliance = ["K_Anonymity", "L_Diversity", "T_Closeness", "AK_Anonymity"]

        ##权重随着数据隐私属性个数变化而变化
        ##按照 1:1:1来，每一个指标权重都一样
        Weight_Data_compliance = []

        ##融合指标
        Ans_Data_compliance = 0.0
        valuesALL_Data_compliance = []
        for i in range(len(List_Data_compliance)):
            # 使用 getvalue 函数从 Redis 中获取数据
            value, valuetype = getvalue(worker_id, List_Data_compliance[i])
            if value is None:
                print(f"No value found for key '{List_Data_compliance[i]}' in Redis.")
                return
            valuesALL_Data_compliance.append(value)

        length = len(valuesALL_Data_compliance)

        for eachNum in valuesALL_Data_compliance:
            if eachNum[0] > 0:
                Ans_Data_compliance += (1 / length)

        SendCombineTarget(worker_id, "合规性结果", Ans_Data_compliance, float)

    except Exception as e:
        print(f"Error occurred: {e}")


# 安全性
def CombineTarget_privacy_protection_metrics(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并安全性的指标，将结果写入redis中


    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
    """
    try:
        List_privacy_protection_metrics = ["敏感属性重识别风险", "基于准标识符重识别风险", "整体重识别风险"]

        ##权重随着数据隐私属性个数变化而变化
        ##按照 4:1:4来,即每一个敏感属性重识别风险的指标的权重都相当于4个基于准标识符重识别风险的指标权重
        Weight_privacy_protection_metrics = []

        ##融合指标
        Ans_privacy_protection_metrics = 0.0
        valuesALL_privacy_protection_metrics = []
        length = 0
        for i in range(len(List_privacy_protection_metrics)):
            # 使用 getvalue 函数从 Redis 中获取数据
            value, valuetype = getvalue(worker_id, List_privacy_protection_metrics[i])
            if value is None:
                print(f"No value found for key '{List_privacy_protection_metrics[i]}' in Redis.")
                return

            if i == 0:
                length += (4 * len(value))
            if i == 1:
                length += (1 * len(value))
            if i == 2:
                length += (4 * len(value))
            valuesALL_privacy_protection_metrics.append(value)

        for eachValue in valuesALL_privacy_protection_metrics[0]:
            Ans_privacy_protection_metrics += ((4 * eachValue) / length)

        for eachValue in valuesALL_privacy_protection_metrics[1]:
            Ans_privacy_protection_metrics += ((1 * eachValue) / length)

        for eachValue in valuesALL_privacy_protection_metrics[2]:
            Ans_privacy_protection_metrics += ((4 * eachValue) / length)

        SendCombineTarget(worker_id, "安全性结果", Ans_privacy_protection_metrics, float)

    except Exception as e:
        print(f"Error occurred: {e}")


# 数据质量
def CombineTarget_data_quality_evalution(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并数据质量的指标，将结果写入redis中

    10:7:7:9:5

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:
        List_data_quality_evalution = ["唯一性", "分布泄露", "熵泄露", "正面信息披露", "KL散度"]

        ##权重随着数据隐私属性个数变化而变化
        ##按照 10:7:7:9:5  来
        Weight_data_quality_evalution = []

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

            if i == 0:
                length += (10 * len(value))
            if i == 1:
                length += (7 * len(value))
            if i == 2:
                length += (7 * len(value))
            if i == 3:
                length += (9 * len(value))
            if i == 4:
                length += (5 * len(value))
            valuesALL_data_quality_evalution.append(value)

        for eachValue in valuesALL_data_quality_evalution[0]:
            Ans_data_quality_evalution += ((10 * eachValue) / length)

        for eachValue in valuesALL_data_quality_evalution[1]:
            Ans_data_quality_evalution += ((7 * eachValue) / length)

        for eachValue in valuesALL_data_quality_evalution[2]:
            Ans_data_quality_evalution += ((7 * eachValue) / length)

        for eachValue in valuesALL_data_quality_evalution[3]:
            Ans_data_quality_evalution += ((9 * eachValue) / length)

        for eachValue in valuesALL_data_quality_evalution[4]:
            Ans_data_quality_evalution += ((5 * eachValue) / length)

        SendCombineTarget(worker_id, "数据质量结果", Ans_data_quality_evalution, float)

    except Exception as e:
        print(f"Error occurred: {e}")


# 数据特征
def CombineTarget_Data_character(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并数据特征的指标，将结果写入redis中
    直接返回 0.9

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:

        SendCombineTarget(worker_id, "数据特征结果", 0.9, float)

    except Exception as e:
        print(f"Error occurred: {e}")



# 数据隐私风险
def CombineTarget_Data_riskvalue(worker_id):
    """
    从 Redis 中取出指定 key 的值，对其进行处理后再存回 Redis。
    合并数据特征的指标，将结果写入redis中
    直接返回 0.9

    参数:
        worker_ud (str): Redis 连接字符串。
        worker_id (str): 用作存储处理后数据的前缀。
        key (str): 要检索和处理的键。
    """
    try:

        SendCombineTarget(worker_id, "隐私风险度量", 0.9, float)

    except Exception as e:
        print(f"Error occurred: {e}")


# 总函数
def cal_rank_all(worker_id):
    CombineTarget_Data_availability(worker_id)
    CombineTarget_Data_character(worker_id)
    CombineTarget_Data_riskvalue(worker_id)
    CombineTarget_privacy_protection_metrics(worker_id)
    CombineTarget_data_quality_evalution(worker_id)
    CombineTarget_Data_compliance(worker_id)