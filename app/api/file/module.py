# -*- coding: utf-8 -*-
import os.path
import zipfile
from time import time
from uuid import uuid4
from configparser import ConfigParser
import data_handle_model.csv_handle
from data_handle_model.json_handel import json_importer
from celery_task.task import start_evaluate


def start_handle(filename,path,logger):
    # 对收到的文件进行处理
    # 1. 将数据导入到neo4j中
    # 2. 将评估任务提交到消息队列中
    file_type=filename.resplit('.',1)[1]
    config=ConfigParser()
    config.read('./setting/set.config')
    info=config['neo4j']
    docker_id=info['docker_id']
    logger.info('')
    if file_type=='csv':
        # 这个还要改，先不管
        start_handle_csv(filename,path)
    elif file_type=='json':
        logger.info('file is json,copy file to docker')
        start_evaluate.delay()
        json_importer(filename,docker_id)
        


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





def start_handle_csv(path,to):
    data_handle_model.handle.read_csv(path, to)
    print("handle csv success")
    ##todo logger
    os.remove(path)
    data_handle_model.importer()

def start_handle_zip(path_from,path_to):
    """
    将zip文件解压到指定目录
    todo 指定目录是写死的
    :param path:zip文件目录
    :return:
    """
    with zipfile.ZipFile(path_from,'r') as zip:
        zip.extractall(path_to)
    os.remove(path_from)
    data_handle_model.importer()



def apoc_json(file):
    sql=''



if __name__=='__main__':
    print(os.getcwd())



