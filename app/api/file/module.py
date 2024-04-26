# -*- coding: utf-8 -*-
# ''''
#     一些库函数
# '''
import os.path
import zipfile
from time import time
from uuid import uuid4
from configparser import ConfigParser
import data_handle_model.csv_handle
from celery_task.celery import start_evaluate,csv_start_import,json_start_import
from celery_task.config import Config

def start_handle(config,logger):
    # 对收到的文件进行处理
    # 1. 将数据导入到neo4j中
    # 2. 将评估任务提交到消息队列中

    # 先不管数据库写入
    file_type=config.json_address.resplit('.',1)[1]
    conf=ConfigParser()
    conf.read('./setting/set.config')
    info=conf['neo4j']
    docker_id=info['docker_id']
    logger.info('数据库配置成功')
    # todo 添加一些描述信息
    dis=''
    filename=os.path.basename(config.address)
    if file_type=='csv':
        # 这个还要改，先不管
        logger.info('file is csv,copy file to docker')
        csv_start_import.delay(filename,docker_id,dis,config.ID,config.QIDs,config.SA)
    elif file_type=='json':
        logger.info('file is json,copy file to docker')
        json_start_evaluate.delay(config)
        json_start_import.delay(filename,docker_id,dis,config.ID,config.QIDs,config.SA)
        


ALLOWED_EXTENSIONS = {'json', 'csv'}
# 目前只支持json和csv
##ALLOWED_EXTENSIONS在allowed_file函数中用到

def allowed_file(filename):
    """
            判断上传的文件是否合法
            :param filename:文件名
            :return: bool
    """
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS



if __name__=='__main__':
    print(os.getcwd())



