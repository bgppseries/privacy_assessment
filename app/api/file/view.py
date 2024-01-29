import os
from datetime import datetime
from flask import request, render_template
from . import api_file
from .module import allowed_file, start_handle_csv, start_handle
from mylog.logger import mylogger,set_logger
from logging import INFO

logger=mylogger(__name__,INFO)
set_logger(logger)

## 处理文件形式的API，处理数据库格式的还有点麻烦，等联调吧todo
@api_file.route('/upload',methods=['POST'])
def upload_file():
    UPLOADED_PRIVATE_DEST=os.path.abspath(os.path.join(os.getcwd(), "../../.."))+'\\data\\test'
    #UPLOADED是测试用的,在server.py中调用是C:\Users\data\test，错滴
    to=os.getcwd()+'\\data\\unhandeled'
    logger.info("file api receive a work")
    #todo 多文件上传，还有如何判断哪些文件是一个执行任务
    file =request.files['file']
    if file and allowed_file(file.filename):
        logger.info('file is available , start evaluate')
        file_url=to+'/'+file.filename
        file_url=file_url.replace('\\','/')
        file.save(file_url)
        logger.info('file is save ',file_url)
        #todo 感觉这里IO有点爆
        start_handle(file.filename,file_url,logger)
    else:
        print("err")
        file_url="please input particular file, now we can only support csv and json"
    return file_url


@api_file.route('/hello')
def hello():
    return 'hello world!'

@api_file.route('/time',methods=["GET","POST"])
def get_time():
    return datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")

