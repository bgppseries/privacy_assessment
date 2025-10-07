import time
import logging
import logging.handlers
from logging import *
import os
from configparser import ConfigParser


"""
日志等级：
    DEBUG	debug级别用来记录详细的信息，方便定位问题进行调试，在生产环境我们一般不开启DEBUG
    INFO	用来记录关键代码点的信息，以便代码是否按照我们预期的执行，生产环境通常会设置INFO级别
    WARNING	记录某些不预期发生的情况，如磁盘不足
    ERROR	由于一个更严重的问题导致某些功能不能正常运行时记录的信息
    CRITICAL	当发生严重错误，导致应用程序不能继续运行时记录的信息
    日志级别重要程度逐次提高，logging分别提供了5个对应级别的方法。默认情况下日志的级别是WARGING， 
    ####低于WARING的日志信息都不会输出。
"""
class mylogger(Logger):
    def __init__(self, name, level=NOTSET):
        super(mylogger, self).__init__(name=name, level=level)

    def _log(
            self,
            level,
            msg,
            args,
            exc_info=None,
            extra=None,
            stack_info=False
    )->None:
        """

        :param level:
        :param msg:
        :param args:
        :param exc_info:
        :param extra:
        :param stack_info:
        :return:
        """
        super(mylogger, self)._log(level, msg, args, exc_info, extra, stack_info)



def set_logger(logger):
    config=ConfigParser()
    path=os.path.dirname(__file__)##找当前文件logger.py的目录
    path=os.path.dirname(path)## 找当前文件的上层目录
    path = os.path.join(path,'/setting/set.config') # 拼接文件的路径
    config.read("./setting/set.config",encoding='UTF-8')
    set=config['log']
    LOG_FILENAME=set['logfile_name']
    print("日志文件名：",LOG_FILENAME)
    #创建一个格式器formatter
    formatter = logging.Formatter('%(asctime)s - %(process)d-%(threadName)s - '
                                  '%(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    #标准流处理器
    console_handler = logging.StreamHandler()
    #流处理器与格式器结合
    console_handler.setFormatter(formatter)
    #与日志绑定
    logger.addHandler(console_handler)
    # log output to file
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILENAME, maxBytes=10485760, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

