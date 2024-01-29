import time
import os
from flask import current_app
from celery import Task, Celery
from . import config
from .Indicator_K_Json2 import Config
import uuid
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



@celery.task(bind=True, base=MyTask)
def apptask(self):
    #print(current_app.config)
    print("==============%s " % current_app.config["SQLALCHEMY_DATABASE_URI"])
    print("++++++++++++++%s " % os.getenv("DATABASE_URL"))
    print('it is the first step')
    time.sleep(5)
    return 'success'

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




# @celery.task(bind=True,base=MyTask)
# def utli(self):
#     print(time.time())
#     time.sleep(5)
#     get_time.delay()
#     apptask.delay()
#     return 'it is the final step'



# @celery.task(bind=True,base=MyTask)
# def start(self,url):
#     strat_evaluate(url)
#     return '任务已提交，正在执行中...'


