#coding:utf-8
# 消息代理使用rabbitmq。与celery实例化时broker参数意义相同
broker_url = "amqp://qwb:784512@127.0.0.1:5672/test"

# 结果存储使用redis(默认数据库-零)。与celery实例化时backend参数意义相同
result_backend = 'redis://:qwb@127.0.0.1:5678/1'

# LOG配置
worker_log_format = "[%(asctime)s] [%(levelname)s] %(message)s"

# Celery指定时区，默认UTC
timezone = "Asia/Shanghai"

#有警告CPendingDeprecationWarning: The broker_connection_retry configuration setting will no longer
broker_connection_retry_on_startup = True


import json
import time
import pandas as pd

class Config:
    def __init__(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param url: 输入脱敏前文件地址
        :param address:选取的文件地址
        :param worker_uuid: 任务id
        :param QIDs:间接标志符列表
        :param SA:隐私属性
        :param ID: 直接标识符
        '''
        #self.graph = Graph("http://192.168.1.121:7474/browser/", auth=("neo4j", "123456"))
        #################数据库相关配置

        #################隐私评估通用配置
        self.to_assess_url = address  ##待评价的数据文件地址
        self.src_url=url              ##脱敏前的数据地址
        self.address_dict = {}        ##待评价的文件地址列表

        self.address_dict[self.json_address] = [["sex","age","id_number","phone"] , ["day_start","day_end","hotel_name"]]
        ## ↑↑↑↑↑↑↑没搞懂
        
        self.worker_uuid=worker_uuid
        self.K_Anonymity = k                                        ##定义好的K-Anonymity数
        self.L_diversity = l                                               ##定义好的L_diversity数
        self.T_closeness = t                                             ##定义好的T紧密度

        ##################风险评估配置---乔万邦
        self.QIDs=QIDs
        self.SA=SA
        self.ID=ID

        ##################可用性评估-----耿志颖
        self.n_s = 0                                                           ##数据集中被隐匿的记录个数，即原数据集中有却没有在脱敏数据集发布的记录个数，暂时定义为0
        # self.address_NI_dict = {self.json_address:["age","zip"]  }# 计算ILoss函数时针对的准标识符中的具体属性集

    def _Function_Data(self,address):
        '''
        :param address:选取的文件地址
        :return: 返回文件的所有数据
        '''
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)  ##99976条数据
        return result

    def _Function_List_quasi(self, address):
        '''
        :param address: 选取的json文件地址
        :return: 返回数据中的所有准标识符，及其数量
        用时16秒
        '''
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)   ##99976条数据
            return pd.value_counts([[each[each_attr] for each_attr in self.address_dict[address][0]]  for each in result])

    def _Probabilistic_Distribution_Privacy(self,address,each_privacy):
        '''
        :param address:选取的json文件地址
        :param each_privacy:选取的某一个隐私属性
        :return:返回针对选取的隐私属性的概率分布
        用时0.23秒
        '''
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)
            return pd.value_counts([each[each_privacy] for each in result], normalize=True)

    def _Num_address(self,address):
        '''
        :param address: 选取的json文件地址
        :return: 返回所有数据的个数
        '''
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)  ##99976条数据
            return len(result)

    def Send_Result(self):
        graph = self.graph
        Testuuid = self.uuid
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        query1 = '''
        merge (a:worker {time:'%s',uuid:'%s'})
        with a
            match(d:result {name:'评估结果'}) merge(d)-[:include]->(a)

        merge (a1:exp_class{name:'隐私数据', uuid:'%s'})
        merge (b1:exp_class {name:'隐私策略', uuid:'%s'})
        merge (c1:exp_class {name:'行为动作', uuid:'%s'})
        with a1,b1,c1
            match(d1:worker {uuid:'%s'}) merge(d1)-[:include]->(a1) merge(d1)-[:include]->(b1) merge(d1)-[:include]->(c1)

        merge (a2:exp_first_level_metric {name:'匿名集数据特征', uuid:'%s'})
        merge (b2:exp_first_level_metric {name:'数据质量评估', uuid:'%s'})
        merge (c2:exp_first_level_metric {name:'隐私保护性度量', uuid:'%s'})
        with a2,b2,c2
            match(d2:exp_class {name:'隐私数据', uuid:'%s'}) merge(d2)-[:include]->(a2) merge(d2)-[:include]->(b2) merge(d2)-[:include]->(c2)

        merge (a3:exp_first_level_metric {name:'不可逆性', uuid:'%s'})
        merge (b3:exp_first_level_metric {name:'复杂性', uuid:'%s'})
        merge (c3:exp_first_level_metric {name:'偏差性', uuid:'%s'})
        merge (e3:exp_first_level_metric {name:'数据可用性', uuid:'%s'})
        merge (f3:exp_first_level_metric {name:'合规性', uuid:'%s'})
        with a3,b3,c3,e3,f3
            match(d3:exp_class {name:'隐私策略', uuid:'%s'}) merge(d3)-[:include]->(a3) merge(d3)-[:include]->(b3) merge(d3)-[:include]->(c3) merge(d3)-[:include]->(e3)  merge(d3)-[:include]->(f3) 

        merge (a4:exp_first_level_metric {name:'延伸控制性', uuid:'%s'})
        merge (b4:exp_first_level_metric {name:'场景', uuid:'%s'})
        with a4,b4
            match(d4:exp_class {name:'行为动作', uuid:'%s'}) merge(d4)-[:include]->(a4) merge(d4)-[:include]->(b4)  
        ''' % (time_str, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid)
        graph.run(query1)

