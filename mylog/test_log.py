from logging import INFO
from mylog.logger import mylogger,set_logger

logger=mylogger(__name__,INFO)
set_logger(logger)
def test_logger():

    logger.info('[info] 这条日志只会记录在MongoDB中')
    logger.error('[error] 这条日志会发送到WebHook机器人上')
    logger.warning('[warning] 这条日志也会发送到WebHook',)

if __name__=='__main__':
    test_logger()
