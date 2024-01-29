#coding:utf-8
# 消息代理使用rabbitmq。与celery实例化时broker参数意义相同
broker_url = "amqp://qwb:784512@192.168.1.121:5672/test"

# 结果存储使用redis(默认数据库-零)。与celery实例化时backend参数意义相同
result_backend = 'redis://:ningzaichun@192.168.1.121:6379/1'

# LOG配置
worker_log_format = "[%(asctime)s] [%(levelname)s] %(message)s"

# Celery指定时区，默认UTC
timezone = "Asia/Shanghai"

#有警告CPendingDeprecationWarning: The broker_connection_retry configuration setting will no longer
broker_connection_retry_on_startup = True
