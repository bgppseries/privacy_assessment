import os
from datetime import datetime
from flask import request

from celery_task.celery import start_evaluate
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

@api_file.route('/test',methods=["GET","POST"])
def test():
    res=request.form.get('user_name')
    res=str(res)
    tutu=res
    print(res)
    print(request.form)
    for item in request.form:
        print(item)
    file=request.files.get('bg_file')
    if file:
        print("success recv file",file.filename)
    else :
        print("NNNNNNN")
    res="okok"

    from celery_task.celery import json_start_evaluate,add
    import uuid
    u=uuid.uuid4
    logger.info("recv:")
    logger.info('task uuid: %s',tutu)
    result=json_start_evaluate.delay(tutu)
    res=result.id
    logger.info('worker_id: %s',str(res))
    result=add.delay(1,2)
    res=result.id
    logger.info('worker_id: %s',str(res))
    return str(res)

@api_file.route('/json_test',methods=["POST"])
def json_test():
    data = request.json
    # 处理接收到的 JSON 数据
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
    
    result=start_evaluate.delay(src_url,un_table_name,src_url,un_table_name,q,sa,idd,k,l,t)
    print("task_id is: ",result.id)
    logger.info("test success:{}".format(data))
    # 返回响应
    return result.id





### 本接口处理第一次隐私文件上传
@api_file.route('/to_assess',methods=["POST"])
def beginassess():
    pass



### 本接口处理隐私增强后回传的脱敏数据
@api_file.route('/assess')
def assess_file():
    pass




@api_file.route('/hello')
def hello():
    return 'hello world!'

@api_file.route('/time',methods=["GET","POST"])
def get_time():
    return datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

