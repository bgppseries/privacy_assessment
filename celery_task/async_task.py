from celery import Task, chain
from celery import Celery
import requests
from . import config
import json
from celery.utils.log import get_task_logger
from .data_import_handel import csv_importer,json_importer

import time
import pandas as pd
import redis
from celery_task.config import Data_availability,Data_compliance,Desensitization_data_character,Desensitization_data_quality_evalution, privacy_protection_metrics
from celery.result import AsyncResult

import logging
from logging.handlers import TimedRotatingFileHandler


##2024.9.19  配置日志
## TimedRotatingFileHandler 每日凌晨生成一个新的日志文件
## 包含时间戳（'%(asctime)s'）、记录器名称（'%(name)s'）、日志级别（'%(levelname)s'）和日志消息（'%(message)s'）。
logger = logging.getLogger(__name__)
file_handler = TimedRotatingFileHandler('celery.log', when='midnight')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


##2024.9.19 使用同级文件夹下config作为配置文件配置处理器
celery=Celery()
celery.config_from_object(config)


##2024.9.19  ../app.file调用
def t_status(id):
    c = celery.AsyncResult(id)
    return c
def t_stop(id):
    c=celery.AsyncResult(id)
    c.revoke(terminate=True)
    return c

import traceback
def check_and_handle_task_result(task, worker_id):
    """
    检查任务的结果并在有错误时处理。
    2024.9.19 错误结果弹出函数
    """
    from .config import Send_result
    try:
        result = task.get(timeout=36000)  # 根据需要调整超时时间，单位为s，设置为None表示永久等待
        Send_result(worker_id, result, success=True)
    except Exception as e:
        error_msg = str(e) + "\n" + traceback.format_exc()
        Send_result(worker_id, {}, success=False, error_message=error_msg)
        logger.error("任务 %s 中出现错误：%s", worker_id, error_msg)

##2024.9.19 无调用
def send_err(uuid):
    pass


##2024.9.19 有调试用print_delete,后续是否删除待定
class MyTask(Task): # celery 基类
    def on_success(self, retval, task_id, args, kwargs):
        # 执行成功的操作
        print('MyTasks 基类回调，任务执行成功')
        return super(MyTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # 执行失败的操作
        # 任务执行失败，可以调用接口进行失败报警等操作
        print('MyTasks 基类回调，任务执行失败')
        return super(MyTask, self).on_failure(exc, task_id, args, kwargs, einfo)


##2024.9.19 self 参数代表 Celery 任务的实例本身，通过 @app.task(bind=True) 装饰器进行绑定。
## 通过 self，你可以在任务函数内部访问任务实例的属性和方法，例如任务请求、重新尝试任务等

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


# 定义异步任务，将数据发送给隐私增强系统
##2024.9.19 只进行发送和格式确定
@celery.task(bind=True,base=MyTask)
def send_data_to_backend(self,worker_id,data_table,sql_url):
    """
    隐私增强接口url从config.py文件中获取
    param@ data_table:数据表名
    param@ sql_url:数据库地址
    """
    from celery_task.config import privacy_enhance_url
    url=privacy_enhance_url
    params = {
        "Address": data_table,
        "url": sql_url,
        "worker_id":worker_id
    }
    try:
        response = requests.post(url, params=params)
        response.raise_for_status()  # 检查响应状态码，如果不是 200 则抛出异常
        logger.info("数据已经隐私增强处理，数据格式如下:{}".format(response.json))
        return response.json()  # 返回响应的 JSON 数据
    except requests.RequestException as e:
        logger.error("发送给隐私增强系统失败，Error:{}".format(e))
        return None


##2024.9.19 下创建任务连时调用
@celery.task(bind=True,base=MyTask)
def start_evaluate_enhanced(self,result,worker_id):
    if result==None:
        # 说明发送给隐私增强系统失败
        # 将err写入到redis中，键为worker_id 
        map={
            'success':False,
            'thread_id': worker_id
        }
        from .config import WORKER_ID_MAP_REDISADDRESS,WORKER_ID_MAP_REDISDBNUM,WORKER_ID_MAP_REDISPORT,WORKER_ID_MAP_REDISPASSWORD
        redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM,password=WORKER_ID_MAP_REDISPASSWORD)
        redis_client.hmset(worker_id,map)
        logger.info("发送给隐私增强系统失败，流程终止，已将结果写入redis数据库,{}".format(worker_id))
        

    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
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
    logger.info('隐私增强后，数据评估任务启动，任务配置：%s %s',worker_id,SA)
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
    worker_id='enhanced'+worker_id
    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker=reid_risk.delay(worker_id+'-1',src_url,un_table_name,to_url,table_name,QIDs,SA,ID)
    reid_risk_worker_id=reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(k, l, t, src_url, un_table_name, worker_id+'-2', QIDs, SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(k, l, t, src_url, un_table_name, worker_id+'-3', QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(k, l, t, src_url, un_table_name, worker_id+'-4', QIDs, SA, ID, '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)

    # 异步执行 Desensitization_quality 函数，并记录任务 ID
    Desensitization_quality_worker = Desensitization_quality.delay(k, l, t, src_url, un_table_name, worker_id+'-5', QIDs, SA, ID, '', '')
    Desensitization_quality_worker_id = Desensitization_quality_worker.id
    logger.info("匿名数据质量评估已启动，子进程id是：%s", Desensitization_quality_worker_id)

    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(k, l, t, src_url, un_table_name, worker_id+'-6', QIDs, SA, ID, '', '')
    privacy_protection_worker_id = privacy_protection_worker.id
    logger.info("隐私保护性度量评估已启动，子进程id是：%s", privacy_protection_worker_id)

    # 维护主线程和子线程id的映射
    # 只用于任务调度接口
    thread_process_mapping = {
        'success':True,
        'thread_id': worker_id,
        'reid_risk_worker_id': reid_risk_worker_id,
        'compliance_worker_id': compliance_worker_id,
        'availability_worker_id': availability_worker_id,
        'Desensitization_character_worker_id': Desensitization_character_worker_id,
        'Desensitization_quality_worker_id': Desensitization_quality_worker_id,
        'privacy_protection_worker_id': privacy_protection_worker_id
    }
    from .config import WORKER_ID_MAP_REDISADDRESS,WORKER_ID_MAP_REDISDBNUM,WORKER_ID_MAP_REDISPORT,WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM,password=WORKER_ID_MAP_REDISPASSWORD)
    redis_client.hmset(worker_id,thread_process_mapping)

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


# 创建任务链，设置异步执行
##2024.9.20 chain函数是Celery中的一个构造器，用于创建一个任务链
#.s()是一个快捷方式，用于创建一个任务的签名，它将任务转换为一个可调用的对象，这样你就可以传递参数而不立即执行它
# |操作符用于连接任务，它表示前一个任务的输出将作为后一个任务的输入
def process_data_chain(worker_id,data_table,sql_url):
    chain_task = chain(
        send_data_to_backend.s(worker_id,data_table,sql_url) | 
        start_evaluate_enhanced.s(worker_id=worker_id)
        )
    chain_task.delay()


##2024.9.20 三个测试函数，没啥用，后续可以按照 print_delete 删除
@celery.task(bind=True,base=MyTask)
def test_1(self,a1,a2,a3):
    
    print("父进程第一个参数：",a1)
    print("父进程第二个参数：",a2)
    print("父进程第三个参数：",a3)
    return a3

@celery.task(bind=True,base=MyTask)
def test_2(self,a1,a2):
    print("子进程第一个参数：",a1)
    print("子进程第二个参数：",a2)
    # print("子进程第三个参数：",a3)

def process_list_test(a1,a2,a3):
    chain_tt=chain(
        test_1.s(a1,a2,a3) | 
        test_2.s(a2=a2)
    )
    chain_tt.delay()


# 执行评估指令
## app.file 调用
@celery.task(bind=True,base=MyTask)
def start_evaluate(self,src_url,un_table_name,to_url,table_name,QIDs,SA,ID,scene,des,k=2,l=2,t=0.95):
    print("########################################################################")
    print("*******************************评估启动**********************************")
    worker_id = start_evaluate.request.id
    logger.info('json评估任务启动，任务配置：%s %s',worker_id,SA)
    
    """"
    redis中存放的关于worker_id映射表
    key:    worker_id        value:     map,存放的是各个子任务的uuid
    ## 子线程id
    key:    worker_id+'-0'   value:     评估任务的相关参数
    key:    worker_id+'-1'   value:     风险评估结果
    key:    worker_id+'-2'   value:     合规性评估结果
    key:    worker_id+'-3'   value:     可用性评估结果
    key:    worker_id+'-4'   value:     匿名集数据特征评估结果
    key:    worker_id+'-5'   value:     匿名集数据质量评估结果
    key:    worker_id+'-6'   value:     隐私保护性度量评估结果
    """
    tt='worker_'+worker_id
    # 保存参数到 Redis
    task_params = {
        'id':tt,
        "parameters":{
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
            'data_scene':scene
        },
        "results": [],
        'description':des,
        'status':'in_progress'
    }
    from .config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPORT, WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 将所有值转换为字符串
    # task_params = {k: (json.dumps(v) if isinstance(v, list) else v) for k, v in task_params.items()}
    # redis_client.hmset(worker_id+'-0', task_params)
    # 将字典转换为 JSON 字符串
    task_params_json = json.dumps(task_params)

    # 将 JSON 字符串存储到 Redis
    redis_client.set(worker_id + '-0', task_params_json)

    # 异步执行 reid_risk 函数，并记录任务 ID
    reid_risk_worker=reid_risk.delay(worker_id+'-1',src_url,un_table_name,to_url,table_name,QIDs,SA,ID)
    reid_risk_worker_id=reid_risk_worker.id
    logger.info("风险评估已启动，子进程id是：{}".format(reid_risk_worker_id))

    # 准备数据
    handler = Config(k, l, t, src_url, un_table_name, worker_id, QIDs, SA, ID,'','')
    _TemAll = handler._Function_Data()
    Series_quasi = handler._Function_Series_quasi(_TemAll)##结果为升序
    # 将值传递给celery任务，要保证可被序列化
    # 序列化 Series
    # 打印序列化前的数据
    print("Series_quasi before serialization:", Series_quasi.head())
    print("Series_quasi index length:", len(Series_quasi.index))
    print("Series_quasi data length:", len(Series_quasi.tolist()))
    print(Series_quasi.index)
    Series_quasi = {
        'data': Series_quasi.tolist(),
        'index': [str(idx) for idx in Series_quasi.index]
    }
    print(len(Series_quasi['index']))
    _TemAll = _TemAll.to_dict(orient='records')

    # 异步执行 compliance 函数，并记录任务 ID
    compliance_worker = compliance.delay(Series_quasi,_TemAll,k, l, t, src_url, un_table_name, worker_id+'-2', QIDs, SA, ID, '', '')
    compliance_worker_id = compliance_worker.id
    logger.info("合规性评估已启动，子进程id是：%s", compliance_worker_id)

    # 异步执行 availability 函数，并记录任务 ID
    availability_worker = availability.delay(Series_quasi,_TemAll,k, l, t, src_url, un_table_name, worker_id+'-3', QIDs, SA, ID, '', '')
    availability_worker_id = availability_worker.id
    logger.info("可用性评估已启动，子进程id是：%s", availability_worker_id)

    # 异步执行 Desensitization_character 函数，并记录任务 ID
    Desensitization_character_worker = Desensitization_character.delay(Series_quasi,_TemAll,k, l, t, src_url, un_table_name, worker_id+'-4', QIDs, SA, ID, '', '')
    Desensitization_character_worker_id = Desensitization_character_worker.id
    logger.info("匿名集数据特征评估已启动，子进程id是：%s", Desensitization_character_worker_id)

    # 异步执行 Desensitization_quality 函数，并记录任务 ID
    Desensitization_quality_worker = Desensitization_quality.delay(Series_quasi,_TemAll,k, l, t, src_url, un_table_name, worker_id+'-5', QIDs, SA, ID, '', '')
    Desensitization_quality_worker_id = Desensitization_quality_worker.id
    logger.info("匿名数据质量评估已启动，子进程id是：%s", Desensitization_quality_worker_id)

    # 异步执行 privacy_protection 函数，并记录任务 ID
    privacy_protection_worker = privacy_protection.delay(Series_quasi,_TemAll,k, l, t, src_url, un_table_name, worker_id+'-6', QIDs, SA, ID, '', '')
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
        'Desensitization_quality_worker_id': Desensitization_quality_worker_id,
        'privacy_protection_worker_id': privacy_protection_worker_id
    }
    from .config import WORKER_ID_MAP_REDISADDRESS,WORKER_ID_MAP_REDISDBNUM,WORKER_ID_MAP_REDISPORT,WORKER_ID_MAP_REDISPASSWORD
    redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM,password=WORKER_ID_MAP_REDISPASSWORD)
    redis_client.hmset(worker_id,thread_process_mapping)

    # 轮询各个函数执行情况
    while True:
        # 获取各个函数的异步结果对象
        reid_risk_result = AsyncResult(reid_risk_worker.id)
        compliance_result = AsyncResult(compliance_worker.id)
        availability_result = AsyncResult(availability_worker.id)
        Desensitization_character_result = AsyncResult(Desensitization_character_worker.id)
        Desensitization_quality_result = AsyncResult(Desensitization_quality_worker.id)
        privacy_protection_result = AsyncResult(privacy_protection_worker.id)
        5
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

##2024.9.20 反序列化获得的数据，并在其中解析所需的 5 种数据
@celery.task(bind=True,base=MyTask)
def compliance(self,Series_quasi,_TemAll,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    # 反序列化 Series
    try:
        print(len(Series_quasi['index']),'和',len(Series_quasi['data']))
        # logger.info(f"Deserialized series_quasi: {Series_quasi}")
        series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
        tem_all = pd.DataFrame(_TemAll)
        logger.info(f"Deserialized series_quasi: {series_quasi.head()}")
        logger.info(f"Deserialized tem_all: {tem_all.head()}")
    except Exception as e:
        logger.error(f"Error deserializing series_quasi or tem_all: {e}")
        raise

    ##数据合规性
    handler1 = Data_compliance(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    logger.info('数据合规性评估开始启动：{}'.format(worker_uuid))
    handler1.runL(series_quasi,tem_all)                          ##传递准标识符集合，以及准标识符对应的数量
    

@celery.task(bind=True,base=MyTask)
def availability(self,Series_quasi,_TemAll,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    # 反序列化 Series
    # print(Series_quasi)
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)
    ##数据可用性
    handler2 = Data_availability(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler2.runL(series_quasi,tem_all)

@celery.task(bind=True,base=MyTask)
def Desensitization_character(self,Series_quasi,_TemAll,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)
    ##匿名集数据特征
    handler3 = Desensitization_data_character(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler3.runL(series_quasi,tem_all)   

@celery.task(bind=True,base=MyTask)
def Desensitization_quality(self,Series_quasi,_TemAll,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    print(series_quasi)
    tem_all = pd.DataFrame(_TemAll)    
    ##匿名数据质量评估
    handler4 = Desensitization_data_quality_evalution(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler4.runL(series_quasi,tem_all)                          ##传递准标识符集合，以及准标识符对应的数量
        

@celery.task(bind=True,base=MyTask)
def privacy_protection(self,Series_quasi,_TemAll,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    # 反序列化 Series
    series_quasi = pd.Series(data=Series_quasi['data'], index=[eval(idx) for idx in Series_quasi['index']])
    tem_all = pd.DataFrame(_TemAll)    
    ##隐私保护性度量
    handler5 = privacy_protection_metrics(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    handler5.runL(series_quasi,tem_all)                          ##传递准标识符集合，以及准标识符对应的数量

