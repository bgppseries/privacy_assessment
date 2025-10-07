import json
import os
from datetime import datetime
from flask import jsonify, request
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import traceback

from celery_task.async_task import start_evaluate
from . import api_file
from .module import allowed_file, start_handle
from mylog.logger import mylogger,set_logger
from logging import INFO
from celery_task.config import Config
import uuid
# 日志
logger=mylogger(__name__,INFO)
set_logger(logger)

## 处理文件形式的API，处理数据库格式的还有点麻烦，等联调吧todo
## 需要同时上传两个文件，脱敏前，脱敏后，todo
## 如果只上传一个数据文件，那就一个吧
## 如果联调，这怎么办
## 故事线就是：收到原始文件，先评估，不合格，直接发给隐私增强，再接收隐私增强后的数据，再评估，打印评估报告，这就是一个完整的评估流程
## 那么接口api就要分离，本接口只支持前端form表单可视化，故事线等联调
####    这个接口作为独立评估
# 2024年4月25日15:53:51 文件方式取消，函数废除
@api_file.route('/upload',methods=['POST'])
def upload_file():
    to=os.getcwd()+'\\data\\unhandeled'
    bg_dir=os.getcwd()+'\\data\\bg'
    logger.info("file api receive a work")

    file =request.files.get('assess_file')
    if file and allowed_file(file.filename):
        logger.info('file is available , start evaluate')
        file_url=to+'/'+file.filename
        file_url=file_url.replace('\\','/')
        file.save(file_url)
        logger.info('file is saved ',file_url)
        ### 接收脱敏前文件
        ##  如果用户上传了，就保存，否则
        src_file=request.files.get('src_file')
        if src_file and allowed_file(src_file.filename):
            src_url=to+'/'+src_file.filename
            src_url=src_url.replace('\\','/')
            src_file.save(src_url)
            logger.info('src_file is saved',src_url)
        else:
            src_url=file_url

        ### 创建Config
        ##     必填字段
        QIDs=request.form.get('QIDs')
        if QIDs:
            logger.info('recv assess work QIDs',QIDs)
        else:
            err="illegal input for QIDs"
            logger.error(err)
            return err
        
        SA=request.form.get('SA')
        if SA:
            logger.info('recv assess work SA',SA)
        else:
            err="illegal input for SA"
            logger.error(err)
            return err
        sc=request.form.get('scene')
        if sc:
            logger.info('recv assess work scene',sc)
        else:
            err='illegal input for scene'
            logger.error(err)
            return err
        ##      选填字段
        k=request.form.get('k_anonymity')
        if k:
            logger.info('recv assess work k_anonymity')
        else:
            k=2
        l=request.form.get('l_diversity')
        if l:
            logger.info('recv assess work l_diversity')
        else:
            l=2
        t=request.form.get('t_cloesness')
        if t:
            logger.info('recv assess work t_cloesness')
        else:
            t=0.95
        ID=request.form.get('ID')
        ## 为任务创建一个worker uuid
        uid=uuid.uuid4()
        bg_file=request.files.get('back_info')
        if bg_file and allowed_file(bg_file.filename):
            ##### 如果收到了辅助背景知识？？？用户为什么要上传辅助知识
            bg_url=to+'/'+file.filename
            bg_url=bg_url.replace('\\','/')
            bg_file.save(bg_url)
            logger.info('file is save ',bg_url)
        else:
            bg_url=None
        #### 生成隐私度量相关Config配置
        config=Config(k,l,t,src_url,file_url,uid,QIDs,SA,ID,bg_url,sc)
        res="recv task config is:"
        start_handle(config,logger)
    else:
        logger.error("err: input illegal file type or can't recv file")
        res="please input particular file, now we can only support csv and json"
    return res



# api：处理收到的数据，先评估
@api_file.route('/privacy_assess',methods=["POST"])
def json_test():
    data = request.json
    # 处理接收到的 JSON 报文
    q=data['Quasi_identifiers']
    idd=data['Direct_identifiers']
    sa=data['Sensitive_attributes']
    src_url=data['private_data_url']
    un_table_name=data['private_data_table']
    scene=data['data_scene']
    description=data['description']
    k=data['k-anonymity']
    l=data['l-diversity']
    t=data['t-closeness']

    k=int(k)
    l=int(l)
    t=float(t)
    # 启动评估任务
    # 将未脱敏的数据先评估一遍
    result=start_evaluate.delay(src_url,un_table_name,src_url,un_table_name,q,sa,idd,scene,description,k,l,t)
    print("task_id is: ",result.id)
    logger.info("test success:{}".format(data))
    worker_id=result.id
    
    # 发送给隐私增强系统
    # from celery_task.async_task import process_data_chain
    # process_data_chain(worker_id,un_table_name,src_url)

    # 返回响应
    return worker_id


# api:  获取评估结果
@api_file.route('/privacy_res',methods=["GET"])
def get_work_assess():
    """
    根据主线程任务ID获取各个子线程的任务结果。
    """
    data=request.json
    worker_id=data['Worker_Id']
    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS,WORKER_ID_MAP_REDISDBNUM,WORKER_ID_MAP_REDISPASSWORD,WORKER_ID_MAP_REDISPORT
    try:
        # 建立到 Redis 的连接
        redis_client=redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT,host=WORKER_ID_MAP_REDISADDRESS,db=WORKER_ID_MAP_REDISDBNUM,password=WORKER_ID_MAP_REDISPASSWORD)
        # 检查连接是否成功
        redis_client.ping()
        print("Connected to Redis server.")
    except redis.ConnectionError as e:
        # 处理连接错误
        print("Failed to connect to Redis server:", e)
    except Exception as e:
        # 处理其他异常
        print("An error occurred:", e)
    #  连接建立后，根据workerid判断是否已完成全部任务
    #  放在调度接口

    #  连接建立后，根据workerid取数据
    try:
        # 使用主线程ID作为前缀从Redis中获取任务结果
        keys = redis_client.keys(f'{worker_id}-*')
        # 存储结果的字典
        results = {}
        # 遍历结果并获取任务结果
        for key in keys:
            # 提取子线程ID
            child_process_id = key.decode('utf-8')
            # 排除后缀为 '-0' 的键
            if child_process_id.endswith('-0'):
                continue
            # 获取并解码结果JSON
            result_json = redis_client.get(child_process_id).decode('utf-8')
            result_dict = json.loads(result_json)
            # 使用子线程ID作为键存储结果
            results[child_process_id] = result_dict
        ## 支持可扩展性，将评估逻辑持久化
        return jsonify(results), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500






# api:  进程调度接口        
from celery_task.async_task import t_status,t_stop

@api_file.route('/status',methods=["POST"])
def get_assess_status():
    data=request.json
    worker_id=data['Worker_Id']
    action_type=data['Type']
    #   获取参数
    if not worker_id or action_type is None:
        return jsonify({'error': 'Worker_Id and Type fields are required.'}), 400

    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS,WORKER_ID_MAP_REDISDBNUM,WORKER_ID_MAP_REDISPASSWORD,WORKER_ID_MAP_REDISPORT
    try:
        # 建立到 Redis 的连接
        redis_client=redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT,host=WORKER_ID_MAP_REDISADDRESS,db=WORKER_ID_MAP_REDISDBNUM,password=WORKER_ID_MAP_REDISPASSWORD)
        # 检查连接是否成功
        redis_client.ping()
        print("Connected to Redis server.")
    except redis.ConnectionError as e:
        # 处理连接错误
        print("Failed to connect to Redis server:", e)
        return jsonify({'error': 'Failed to connect to Redis server.'}), 500
    except Exception as e:
        # 处理其他异常
        print("An error occurred:", e)
        return jsonify({'error': 'An error occurred while connecting to Redis.'}), 500
    #  连接建立后，根据workerid判断是否已完成全部任务

    # 获取所有子任务的ID
    subtask_ids = []
    subtask_field = []
    hash_data = redis_client.hgetall(worker_id)
    if hash_data:
        for field, value in hash_data.items():
            field_str = field.decode()
            value_str = value.decode()
            subtask_ids.append(value_str)
            subtask_field.append(field_str)

    #  根据任务类型获取执行不同的处理逻辑
    if action_type == 0:  # 停止任务
        for subtask_id in subtask_ids:
            result = t_stop(subtask_id)
        return jsonify({'msg': 'All subtasks killed', 'task_id': worker_id}), 200

    elif action_type == 1:  # 查看任务状态
        # subtask_ids = []
        # subtask_field = []
        # hash_data = redis_client.hgetall(worker_id)
        # if hash_data:
        #     for field, value in hash_data.items():
        #         field_str = field.decode()
        #         value_str = value.decode()
        #         subtask_ids.append(value_str)
        #         subtask_field.append(field_str)
        results = {'msg': {}}
        for i in range(len(subtask_ids)):
            result = t_status(subtask_ids[i])
            results['msg'][subtask_field[i]] ={
                        'subworker_id': subtask_ids[i],
                        'state': result.state,
                        'traceback': result.traceback
            }
            
        return jsonify(results), 200

    else:
        return jsonify({'error': 'Invalid Type value. Expected values are 0, 1, 2, or 3.'}), 400







# api: 获取当前时间
@api_file.route('/time',methods=["GET","POST"])
def get_time():
    return datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

# api: 获取数据库表名和列名
engines={}
@api_file.route('/get-tables', methods=['POST'])
def get_tables():
    data = request.get_json()
    db_url = data.get('url')
    print("收到的数据库URL:", db_url)

    if not db_url:
        return jsonify({'success': False, 'message': '缺少数据库URL'}), 400

    try:
        # 创建数据库连接引擎
        engine = create_engine(db_url)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # 保存引擎供后续使用（可选）
        engines['engine'] = engine

        return jsonify({'success': True, 'tables': tables})

    except SQLAlchemyError as e:
        print("SQLAlchemyError:", str(e))
        return jsonify({'success': False, 'message': str(e)}), 500
    except Exception as e:
        print("其他错误:", traceback.format_exc())
        return jsonify({'success': False, 'message': '内部服务器错误', 'error': str(e)}), 500

@api_file.route('/get-columns', methods=['POST'])
def get_columns():
    data = request.get_json()
    table_name = data.get('table')
    if not table_name:
        return jsonify({'success': False, 'message': '缺少表名'}), 400

    engine = engines.get('engine')
    if not engine:
        return jsonify({'success': False, 'message': '数据库连接尚未建立'}), 400

    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        return jsonify({'success': True, 'columns': columns})
    except SQLAlchemyError as e:
        return jsonify({'success': False, 'message': str(e)}), 500