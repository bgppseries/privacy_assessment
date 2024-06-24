#coding:utf-8

# 开发环境与生产环境配置分离

# # 消息代理使用rabbitmq。与celery实例化时broker参数意义相同
# broker_url = "amqp://qwb:784512@127.0.0.1:5673/privacy_assess"

# # 结果存储使用redis(默认数据库-零)。与celery实例化时backend参数意义相同
# result_backend = 'redis://:qwb@127.0.0.1:6379/1'

# # LOG配置
# worker_log_format = "[%(asctime)s] [%(levelname)s] %(message)s"

# # Celery指定时区，默认UTC
# timezone = "Asia/Shanghai"

# #有警告CPendingDeprecationWarning: The broker_connection_retry configuration setting will no longer
# broker_connection_retry_on_startup = True

# 西电隐私增强的url，用于隐私增强任务的提交
privacy_enhance_url="http://10.10.55.25:8082/common/desensitization/eTPSS"
# 服务器部署时，要替换成内网地址
# privacy_enhance_url="http://10.10.55.25:8082/common/desensitization/eTPSS"

# 消息代理使用rabbitmq。与celery实例化时broker参数意义相同
broker_url = "amqp://qwb:784512@127.0.0.1:5672/test"

# 结果存储使用redis(默认数据库-零)。与celery实例化时backend参数意义相同
result_backend = 'redis://:qwb@127.0.0.1:5678/1'

# # LOG配置
# worker_log_format = "[%(asctime)s] [%(levelname)s] %(message)s"

# Celery指定时区，默认UTC
timezone = "Asia/Shanghai"

#有警告CPendingDeprecationWarning: The broker_connection_retry configuration setting will no longer
broker_connection_retry_on_startup = True

WORKER_ID_MAP_REDISADDRESS='localhost'
WORKER_ID_MAP_REDISPORT=5678
WORKER_ID_MAP_REDISDBNUM=3
WORKER_ID_MAP_REDISPASSWORD='qwb'

import redis

def Send_result(worker_id, res_dict, success=True, error_message=""):
    """
    持久性地将任务结果与子进程ID和执行状态记录到 Redis 中。新增异常处理
    
    参数：
    - worker_id：子进程的ID。
    - res_dict：包含任务结果的字典。
    - success：布尔标志，指示任务是否无错误执行。
    - error_message：如果任务失败，则包含错误消息的字符串。
    """
    try:
        # 连接到 Redis
        redis_client = redis.StrictRedis(host=WORKER_ID_MAP_REDISADDRESS, port=WORKER_ID_MAP_REDISPORT, db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        redis_client.ping()
        print("已连接到 Redis 服务器。")
    except redis.ConnectionError as e:
        print("无法连接到 Redis 服务器：", e)
        return
    except Exception as e:
        print("发生错误：", e)
        return

    try:
        # 更新结果字典，包括成功标志和错误消息
        res_dict.update({"success": success, "error_message": error_message})
        result_json = json.dumps(res_dict, ensure_ascii=False)
        redis_client.set(worker_id, result_json)
        print("子进程 {} 的结果已成功记录。".format(worker_id))
    except Exception as e:
        print("记录结果时发生错误：", e)

import json
import math
import time
import pandas as pd
import uuid
from sqlalchemy import create_engine,text

# from celery_task.Indicator_K_Json2 import Data_availability,Data_compliance,Desensitization_data_character,Desensitization_data_quality_evalution, privacy_protection_metrics

def Run(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
    '''
    :param k: 输入K匿名
    :param l: 输入l多样性
    :param t: 输入T紧密度

    :param url: 输入脱敏前文件地址
    :param address:脱敏后的文件地址,或者mysql中数据库中存储的表名
    :param worker_uuid: 任务id

    :param bg_url: 输入的额外背景知识
    :param scene: 数据场景(脱敏方式??)
    :param QIDs:准标志符列表
    :param SA:隐私属性集合
    :param ID: 直接标识符集合
    '''
    handler = Config(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    Series_quasi = handler._Function_Series_quasi()##结果为升序
    print(Series_quasi)
    _TemAll = handler._Function_Data()
    RunList = {}
    ##数据合规性
    handler1 = Data_compliance(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    RunList["合规性"] = handler1.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
    ##数据可用性
    handler2 = Data_availability(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    RunList["可用性"] = handler2.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
    ##匿名集数据特征
    handler3 = Desensitization_data_character(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    RunList["数据特征"] = handler3.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
    ##匿名数据质量评估
    handler4 = Desensitization_data_quality_evalution(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    RunList["质量"] = handler4.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
    ##隐私保护性度量
    handler5 = privacy_protection_metrics(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    RunList["安全性"] = handler5.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
    return RunList


class Config:
    def __init__(self,k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度

        :param url: 输入脱敏前文件地址
        :param address:脱敏后的文件地址,或者mysql中数据库中存储的表名
        :param worker_uuid: 任务id

        :param bg_url: 输入的额外背景知识
        :param scene: 数据场景(脱敏方式??)
        :param QIDs:准标志符列表
        :param SA:隐私属性集合
        :param ID: 直接标识符集合
        '''
        #self.graph = Graph("http://192.168.1.121:7474/browser/", auth=("neo4j", "123456"))
        #################数据库相关配置
 
        #################隐私评估通用配置
        self.scene=scene             ## 数据使用场景
    
        self.json_address = address  ##脱敏后的数据文件地址
        self.src_url=url              ##脱敏前的数据的数据库地址
        self.address_Attr = [QIDs, SA]##数据文件的属性（主要包含：准标识符属性和敏感属性）
        # self.address_Attr = [["Sex","Race","Age","Education-Num","Workclass","Relationship","Hours per week"] , ["Target"]]##数据文件的属性（主要包含：准标识符属性和敏感属性）
        ## ↑↑↑↑↑↑↑准标识符集合以及隐私属性集合，这里暂时内定了
        
        self.worker_uuid=worker_uuid
        self.K_Anonymity = k                                        ##定义好的K-Anonymity数
        self.L_diversity = l                                               ##定义好的L_diversity数
        self.T_closeness = t                                             ##定义好的T紧密度

        ##################风险评估配置---乔万邦
        self.QIDs=QIDs
        self.SA=SA
        self.ID=ID
        self.bg_url=bg_url

        ##################可用性评估-----耿志颖
        self.n_s = 0                                                           ##数据集中被隐匿的记录个数，即原数据集中有却没有在脱敏数据集发布的记录个数，暂时定义为0

    def _Function_Data(self):
        '''
        :return: 返回文件的所有数据
        10万条数据，0.29秒
        '''
        ##如果读取sql文件，将其转化为DataFrame格式
        # url='mysql+pymysql://root:784512@localhost:3306/local_test'
        query = f"SELECT * FROM {self.json_address}"
        # 创建SQLAlchemy的Engine对象
        engine = create_engine(self.src_url).connect()
        return pd.read_sql(sql=text(query),con=engine)

        # ##如果为json文件，将其转化为DataFrame格式
        # if self.json_address.endswith(".json"):  
        #     # with open(self.json_address, 'r', encoding='UTF-8') as f:
        #     #     result = json.load(f)
        #     return pd.read_json(self.json_address)
        # elif self.json_address.endswith(".csv"):
        #     return pd.read_csv(self.json_address)

    def _Function_Series_quasi(self):
        '''
        :return: 返回数据中的所有准标识符，及其数量,按照升序排列,<class 'pandas.core.series.Series'>格式
        10万条数据，用时0.3秒
        '''
        _TemAll = self._Function_Data()
        return _TemAll.groupby(self.address_Attr[0], sort=False).size().sort_values()

    def _Probabilistic_Distribution_Privacy(self,each_privacy):
        '''
        :param each_privacy:选取的某一个隐私属性或者某几个隐私属性
        :return:返回针对选取的隐私属性的概率分布,默认降序
        10万条数据，用时0.29秒
        '''
        _TemAll = self._Function_Data()
        return _TemAll[each_privacy].value_counts(normalize=True)

    def _Num_address(self):
        '''
        :return: 返回所有数据的个数
        统一函数接口，一切以_Function_Data为起始点，将数据转化为DataFrame格式，再处理
        10万条数，0.2秒
        '''
        return len(self._Function_Data())

 

    def Send_Result_neo4j(self):
        ##将指标返回值写入知识图谱，待定
        pass
        # graph = self.graph
        # Testuuid = self.uuid
        # time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # query1 = '''
        # merge (a:worker {time:'%s',uuid:'%s'})
        # with a
        #     match(d:result {name:'评估结果'}) merge(d)-[:include]->(a)

        # merge (a1:exp_class{name:'隐私数据', uuid:'%s'})
        # merge (b1:exp_class {name:'隐私策略', uuid:'%s'})
        # merge (c1:exp_class {name:'行为动作', uuid:'%s'})
        # with a1,b1,c1
        #     match(d1:worker {uuid:'%s'}) merge(d1)-[:include]->(a1) merge(d1)-[:include]->(b1) merge(d1)-[:include]->(c1)

        # merge (a2:exp_first_level_metric {name:'匿名集数据特征', uuid:'%s'})
        # merge (b2:exp_first_level_metric {name:'数据质量评估', uuid:'%s'})
        # merge (c2:exp_first_level_metric {name:'隐私保护性度量', uuid:'%s'})
        # with a2,b2,c2
        #     match(d2:exp_class {name:'隐私数据', uuid:'%s'}) merge(d2)-[:include]->(a2) merge(d2)-[:include]->(b2) merge(d2)-[:include]->(c2)

        # merge (a3:exp_first_level_metric {name:'不可逆性', uuid:'%s'})
        # merge (b3:exp_first_level_metric {name:'复杂性', uuid:'%s'})
        # merge (c3:exp_first_level_metric {name:'偏差性', uuid:'%s'})
        # merge (e3:exp_first_level_metric {name:'数据可用性', uuid:'%s'})
        # merge (f3:exp_first_level_metric {name:'合规性', uuid:'%s'})
        # with a3,b3,c3,e3,f3
        #     match(d3:exp_class {name:'隐私策略', uuid:'%s'}) merge(d3)-[:include]->(a3) merge(d3)-[:include]->(b3) merge(d3)-[:include]->(c3) merge(d3)-[:include]->(e3)  merge(d3)-[:include]->(f3) 

        # merge (a4:exp_first_level_metric {name:'延伸控制性', uuid:'%s'})
        # merge (b4:exp_first_level_metric {name:'场景', uuid:'%s'})
        # with a4,b4
        #     match(d4:exp_class {name:'行为动作', uuid:'%s'}) merge(d4)-[:include]->(a4) merge(d4)-[:include]->(b4)  
        # ''' % (time_str, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid, Testuuid)
        # graph.run(query1)



##匿名集数据特征
class Desensitization_data_character(Config):
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取文件的地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)

    def Average_degree_annonymity(self, Series_quasi):
        '''
        :param Series_quasi:选取标签对应的所有的准标识符，及其数量
        :return: 返回平均泛化程度，即平均等价类大小
        '''
        address_All = self._Num_address()  ##得到address中元素的总数
        length = len(Series_quasi)
        return round(address_All / length, 4)

    def Dimension_QID(self):
        '''
        :return: 返回准标识符（QID）维数
        '''
        return len(self.address_Attr[0])

    def Dimension_SA(self):
        '''
        :return: 返回敏感属性（SA）维数
        '''
        return len(self.address_Attr[1])

    def Attribute_SA(self):
        '''
        :return: 返回敏感属性（SA）
        '''
        return self.address_Attr[1]

    def Inherent_Privacy(self, Series_quasi):
        '''
        :param Series_quasi: 数据集的所有准标识符，及其对应等价组大小
        :return:返回与数据集概率分布的不确定性相同的均匀分布区间长度,数值越大，安全性越低
        '''
        Num1 = 0.0
        Num_All = self._Num_address()
        for each in Series_quasi:
            Num1 += (each / Num_All) * math.log2(Num_All / each)
        return int(2 ** Num1)



    def runL(self, Series_quasi):
        Dimen_QID = self.Dimension_QID()
        print("准标识符维数为：", Dimen_QID)
        Dimen_SA = self.Dimension_SA()
        print("敏感属性维数为：", Dimen_SA)
        Attribute_SA = self.Attribute_SA()
        print("敏感属性种类为：", Attribute_SA)
        Average_anonymity = self.Average_degree_annonymity(Series_quasi)
        print("平均泛化程度为：", Average_anonymity)
        Inherent_Priv = self.Inherent_Privacy( Series_quasi)
        print("固有隐私为：", Inherent_Priv)

        return {
            "准标识符维数":Dimen_QID,
            "敏感属性维数":Dimen_SA,
            "敏感属性种类":Attribute_SA,
            "平均泛化程度":Average_anonymity,
            "固有隐私":Inherent_Priv
        }
        # Send_result(self.worker_uuid,res_dict={
        #     "准标识符维数":Dimen_QID,
        #     "敏感属性维数":Dimen_SA,
        #     "敏感属性种类":Attribute_SA,
        #     "平均泛化程度":Average_anonymity,
        #     "固有隐私":Inherent_Priv
        # })


##匿名数据质量评估
class Desensitization_data_quality_evalution(Config):
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)


    def Get_Distribution_Leakage(self, each_privacy, Series_quasi_keys):
        '''
        :param each_privacy:选取的隐私属性
        :param Series_quasi_keys:所有的准标识符
        :return:返回数据集的分布泄露，即分布泄露可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每一个给定的等价类，测量原始数据集和已发布数据集中敏感属性分布之间的泄露。
        10万条数据，原代码用时1816.1875秒，即30分钟；现在用时150秒，即3分钟
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy)  ##得到某一个敏感属性的概率分布
        _TemAll = self._Function_Data()                                                                   ##得到所有数据，DataFrame格式
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)                                                         ##等价组个数
        res = 0                                                                                                       ##返回值
        Attr = ()                                                                                                     ##对应的等价组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            sum_leakage = 0                                                                                  ##分布泄露，Distribution Leakage
            for key in Distribution_P.keys():
                if key in Distribution_QP.keys():
                    sum_leakage += (Distribution_QP[key] - Distribution_P[key]) ** 2
                else:
                    sum_leakage += (Distribution_P[key]) ** 2
            if math.sqrt(sum_leakage) > res:  ##返回等价组中最大的那个分布泄露
                res = math.sqrt(sum_leakage)
                Attr = Series_quasi_keys[i]
        return res,Attr



    def Get_Entropy_Leakage(self, each_privacy, Series_quasi_keys):
        '''
        :param each_privacy:选取的隐私属性
        :param Series_quasi_keys:所有的准标识符
        :return:返回数据集的熵泄露，即通过原始分布的初始熵与等价组的熵之间的差异度，来衡量等价类中个体隐私泄露的程度。
        10万条数据，用时45秒
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy)  ##得到某一个敏感属性的概率分布
        Hmax = -(sum([i * math.log2(i) for i in Distribution_P]))                              ##数据整体关于某一个敏感属性的熵值
        _TemAll = self._Function_Data()                                                                   ##得到所有数据，DataFrame格式
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)                                                         ##等价组个数
        res = 0                                                                                                       ##返回值
        Attr = ()
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Entropy_leakage = float(Hmax)                                                           ##熵泄露，Entropy Leakage
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            for each_value in Distribution_QP:
                Entropy_leakage += each_value * math.log2(each_value)
            if math.fabs(Entropy_leakage) > res:
                res = math.fabs(Entropy_leakage)
                Attr = Series_quasi_keys[i]
        return res,Attr
    

    def Get_Positive_Information_Disclosure(self, each_privacy, Series_quasi):
        '''
        :param each_privacy: 选择的某一个隐私属性
        :param Series_quasi: 整体数据的准标识符，及其对应等价组大小
        :return: 返回选定隐私属性的正面信息披露的最大值。正面信息披露是指准标识数据对隐私数据的披露程度
        10万条数据，4秒
        '''
        _TemAll = self._Function_Data()
        Num_All = self._Num_address()  ##所有数据的个数,10万
        max_value = 0.0                ##将其初始化为0.0
        Attr = ()
        Grouped = _TemAll.groupby(each_privacy, sort=False)  ##将数据按照所选敏感属性分组
        for each in Grouped:                                                                ##选取某个敏感属性值
            # print(each[0],"      ",len(each[1]))
            Quasi_P = each[1][self.address_Attr[0]].value_counts()  ##得到所有包含这个敏感属性值的等价组
            Positive_Num = 0                                                                ##所有包含这个敏感属性值的等价组大小之和
            for each_quasi in Quasi_P.keys():
                Positive_Num += Series_quasi[each_quasi]
            tem = (Positive_Num - len(each[1])) / len(each[1])
            if tem > max_value:                                           ##选取最小的和
                max_value = tem
                Attr = each[0]
        return round(tem, 4),Attr
    

    def Get_KL_Divergence(self,each_privacy,Series_quasi_keys):        
        '''
        :param Series_quasi_keys: 文件中所有的准标识符
        :param each_privacy:选取的隐私属性
        :return: 返回所有等价组中的最大KL散度，即用KL散度来衡量每一个等价组中的隐私属性分布与整体分布之间的距离 
        10万条数据，用时50秒
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy)##返回整个数据中某一个敏感属性的概率分布
        length = len(Series_quasi_keys)
        res = 0
        Attr = ()
        _TemAll = self._Function_Data()
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            Num_distance = 0
            for each_Attribute in Distribution_QP.keys():   ##KL散度计算
                Num_distance += (Distribution_QP[each_Attribute] * math.log2( Distribution_QP[each_Attribute] / Distribution_Privacy[each_Attribute]))
            if Num_distance > res:                          ##找到最大值
                res = Num_distance  
                Attr = Series_quasi_keys[i]
        return round(res,4),Attr
    
    def Get_Uniqueness(self,Series_quasi):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:返回基于熵的数据损失度
        返回数据集中唯一记录的占比。在最简单的定义中，如果等价类中只有一个记录，那么
        该记录被认为是唯一的。唯一记录比非唯一记录更容易被重新识别
        '''
        Num_All = self._Num_address()
        Num_Uniqueness = 0
        for Num_each in Series_quasi:
            if Num_each == 1:
                Num_Uniqueness += 1
            else:  ##由于Series_quasi存储着所有等价组大小，而且按升序排列
                break
        return Num_Uniqueness/Num_All
        



    def runL(self, Series_quasi):
        Uniqueness_proportion = self.Get_Uniqueness(Series_quasi)  
        print(f"数据集中唯一记录的占比为：{round(Uniqueness_proportion * 100, 2)}%，唯一记录比非唯一记录更容易被重新识别，极易遭受重标识攻击和偏斜攻击")
        listD = [];listE = [];listF = [];listKL=[]
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start = time.time()
            _max,Attr = self.Get_Distribution_Leakage(each_privacy, Series_quasi.keys())
            print(f"数据集针对{each_privacy}的分布泄露为：{round(_max, 4)},对应等价组为{Attr}")
            listD.append(f"数据集针对{each_privacy}的分布泄露为：{round(_max, 4)},对应等价组为{Attr}")
            # print(f"耗时为：{time.time() - start}")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start2 = time.time()
            _max,Attr = self.Get_Entropy_Leakage(each_privacy, Series_quasi.keys())
            print(f"数据集针对{each_privacy}的熵泄露为：{round(_max, 4)},对应等价组为{Attr}")
            listE.append(f"数据集针对{each_privacy}的熵泄露为：{round(_max, 4)},对应等价组为{Attr}")
            # print(f"耗时为：{time.time() - start2}")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start3 = time.time()
            _max,Attr = self.Get_Positive_Information_Disclosure(each_privacy, Series_quasi)
            print(f"数据集针对{each_privacy}的正面信息披露为：{_max},对应属性值为{Attr}")
            listF.append(f"数据集针对{each_privacy}的正面信息披露为：{_max},对应属性值为{Attr}")
            # print(f"耗时为：{time.time()-start3}")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start4 = time.time()
            _max,Attr = self.Get_KL_Divergence(each_privacy,Series_quasi.keys())
            print(f"数据集针对{each_privacy}的KL散度为：{_max},对应等价组为{Attr}")
            listKL.append(f"数据集针对{each_privacy}的KL散度为：{_max},对应等价组为{Attr}")
            # print(f"耗时为：{time.time()-start4}")
        
        return {
            "唯一性":f'{round(Uniqueness_proportion * 100, 2)}%',
            "分布泄露":listD,
            "熵泄露":listE,
            "正面信息披露":listF,
            "KL_Divergence":listKL
        }
        # Send_result(self.worker_uuid,res_dict={
        #     "唯一性":f'{round(Uniqueness_proportion * 100, 2)}%',
        #     "分布泄露":listD,
        #     "熵泄露":listE,
        #     "正面信息披露":listF,
        #     "KL_Divergence":listKL
        # })


##隐私保护性度量
class privacy_protection_metrics(Config):
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)

    def Get_Entropy_based_Re_indentification_Risk(self,  Series_quasi_keys):
        '''
        :param Series_quasi_keys:数据集中所有的准标识符
        :return:整体的基于熵的重识别风险
        10万条数据，4秒
        '''
        _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())##得到所有数据中所选敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)                    ##将数据按照所有的敏感属性分组，如性别，年龄等
        length = len(Series_quasi_keys)                                                         ##等价组个数
        res = 0                                                                                                       ##返回值，重识别风险
        for i in range(length):
            ##得到某个等价组的所有敏感属性的概率分布,默认降序
            Distribution_Quasi = Grouped.get_group(Series_quasi_keys[i])[self.address_Attr[1]].value_counts(normalize=True)
            ReIndent_Risk = float(Hmax)                                                       ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_Quasi:                                       ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:                                                                      ##得到重识别风险的最大值
                res = ReIndent_Risk
            if (res / Hmax) > 0.9:                                                                          ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4),Series_quasi_keys[i]
        return round(float(res / Hmax), 4),Series_quasi_keys[i]

    def Get_Entropy_based_Re_indentification_Risk_QID(self,  each_QID):
        '''
        :param each_QID:选择的准标识符中的某一属性值
        :return:返回基于某一（准标识符中的）属性的风险度量
        10万条数据，用时3秒
        '''
        _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())##得到所有数据中所选敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Grouped = _TemAll.groupby(each_QID, sort=False)                    ##将数据按照选取的某一（准标识符中的）属性分组，如性别，年龄等
        res = 0                                                                                                       ##返回值，重识别风险
        Attr = ()                                                           ##返回值，风险值对应的属性值
        for each in Grouped:                                                                    ##选取某个准标识符属性值，如性别='男'
            Distribution_Quasi = each[1][self.address_Attr[1]].value_counts(normalize=True)  ##得到所有包含这个准标识符属性值的等价组概率分布
            ReIndent_Risk = float(Hmax)                                                       ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_Quasi:                                       ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:                                                                      ##得到重识别风险的最大值
                res = ReIndent_Risk
                Attr = each[0]
            if (res / Hmax) > 0.9:                                                                          ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4),Attr
        return round(float(res / Hmax), 4),Attr



    def Get_Entropy_based_Re_indentification_Risk_with(self,  Series_quasi_keys, each_privacy):
        '''
        :param Series_quasi_keys:数据集中所有的准标识符
        :param each_privacy: 选取的敏感属性
        :return:返回某一个敏感属性的基于熵的重识别风险
        10万条数据，用时2秒
        '''
        _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[each_privacy].drop_duplicates())##得到所有数据中所选敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)                                                         ##等价组个数
        res = 0                                                                                                       ##返回值，重识别风险
        res_quasi = ()                                                                                          ##返回值，风险最大的等价组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            ReIndent_Risk = float(Hmax)                                         ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_QP:                               ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:                                                     ##得到重识别风险的最大值
                res = ReIndent_Risk
                res_quasi = Series_quasi_keys[i]
            if (res / Hmax) > 0.9:                                                          ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4),res_quasi
        return round(float(res / Hmax), 4),res_quasi

    def runL(self, Series_quasi):
        listP = []
        listQ = []  ##一行同时初始化多个参数，要用分号而不要用逗号
        for each_privacy in self.address_Attr[1]:
            # start1 = time.time()
            Entropy_Re_Risk_with,QList = self.Get_Entropy_based_Re_indentification_Risk_with(Series_quasi.keys(),each_privacy)
            print(f"针对敏感属性{each_privacy}基于熵的重识别风险为：{Entropy_Re_Risk_with * 100}%，对应等价组为{QList}")  ##149秒/132秒/132秒
            listP.append(f"针对敏感属性{each_privacy}基于熵的重识别风险为：{Entropy_Re_Risk_with * 100}%，对应等价组为{QList}")
            # print(f"耗时为：{time.time() - start1}")
        for each_QID in self.address_Attr[0]:
            # start2 = time.time()
            Entropy_Re_Risk_QID,Attr = self.Get_Entropy_based_Re_indentification_Risk_QID(each_QID)
            print(f"针对准标识符属性{each_QID}基于熵的重识别风险为：{round(Entropy_Re_Risk_QID * 100, 2)}%，对应属性值为{Attr}")
            listQ.append(f"针对准标识符属性{each_QID}基于熵的重识别风险为：{round(Entropy_Re_Risk_QID * 100, 2)}%，对应属性值为{Attr}")
            # print(f"耗时为：{time.time() - start2}")
        # start3 = time.time()
        Entropy_Re_Risk,QList = self.Get_Entropy_based_Re_indentification_Risk( Series_quasi.keys())
        print(f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}")
        # print(f"耗时为：{time.time() - start3}")

        return {
            "敏感属性的重识别风险":listP,
            "单个属性的重识别风险":listQ,
            "基于熵的重识别风险":f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}"
        }
        # Send_result(self.worker_uuid,res_dict={
        #     "敏感属性的重识别风险":listP,
        #     "单个属性的重识别风险":listQ,
        #     "基于熵的重识别风险":f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}"
        # })




##数据合规性
class Data_compliance(Config):
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)

    def IsKAnonymity(self,Series_quasi):
        '''
        :param Series_quasi:文件中所有的准标识符，及其数量
        :return: 返回数据集是否符合K匿名，即用准标识符划分出的等价组其包含的记录个数是否均大于K。若满足K匿名，返回True；反之，返回False
        '''
        if Series_quasi[0] < self.K_Anonymity:   ##由于Series_quasi默认排序为降序，我们将其改为升序，所以选择最小的等价组与K比较
            return False
        return True

    def IsLDiversity(self,Series_quasi_keys,each_privacy):
        '''
        :param address：选取的文件地址
        :param each_privacy:选取的隐私属性
        :return: 返回输入数据集是否符合L多样性，即每一个等价组中是否包含L个以上不同的隐私属性。若满足L多样性，返回True；反之，返回False
        10万条数据，耗时0.5秒
        '''
        if self.L_diversity == 1: ##若L多样性值为1，必定符合
            return True
        length = len(Series_quasi_keys)
        _TemAll = self._Function_Data()##获得文件所有数据，DataFrame格式
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        for i in range(length):
            ##得到某一等价组的某一隐私属性的L多样性，即有多少个不同的属性值
            Diversity = len(Grouped.get_group(Series_quasi_keys[i])[each_privacy].drop_duplicates())
            if Diversity < self.L_diversity:
                return False, Series_quasi_keys[i]
        return True,-1

    def IsTCloseness(self,Series_quasi_keys,each_privacy):
        '''
        :param Series_quasi_keys: 文件中所有的准标识符
        :param each_privacy:选取的隐私属性
        :return: 返回输入等价组Series_quasi是否符合T紧密性，即每一个等价组中的隐私属性分布与整体分布之间的距离是否小于T_Closeness。若满足，返回True；反之，返回False
        显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，这里采用的是EMD
        10万条数据，用时3秒
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy)##返回整个数据中某一个敏感属性的概率分布
        length = len(Series_quasi_keys)
        _TemAll = self._Function_Data()
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            # print(Distribution_QP)
            Num_distance = 0
            for each_Attribute in Distribution_QP.keys():
                if Distribution_QP[each_Attribute] > Distribution_Privacy[each_Attribute]:
                    # print(Distribution_QP[each_Attribute],"    ", Distribution_Privacy[each_Attribute])
                    Num_distance += (Distribution_QP[each_Attribute] - Distribution_Privacy[each_Attribute]) 
            if Num_distance > self.T_closeness:
                return False,Series_quasi_keys[i]
        return True,-1

    def IsAKAnonymity(self, Series_quasi_keys, each_privacy):
        '''
        :param Series_quasi_keys:数据集的所有准标识符
        :param each_privacy:选定的隐私属性
        :return: (α,k)-Anonymity确保在所有等价类中，没有任何一个敏感属性可以占主导地位。这里内定α=0.5。每一个等价类中不能有任何一个敏感属性占比超过0.5。
        10万条数据，耗时0.3秒
        '''
        _TemAll = self._Function_Data()
        length = len(Series_quasi_keys)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            if Distribution_QP[0]>0.5:
                return False,Series_quasi_keys[i]
        return True,-1



    def runL(self, Series_quasi):
        BoolK = self.IsKAnonymity(Series_quasi)  ##若为True，满足K匿名；反之，不满足
        _list = []
        if BoolK:                                                        ##若不满足K匿名，则必不满足(0.5,k)-Anonymity
            print(f"满足{self.K_Anonymity}-匿名")
            _list.append(f"满足{self.K_Anonymity}-匿名")
        else:
            print(f"准标识符为{Series_quasi.keys()[0]}的等价组不满足{self.K_Anonymity}-匿名，安全性低，容易遭受偏斜攻击和重标识攻击！")
            _list.append(f"准标识符为{Series_quasi.keys()[0]}的等价组不满足{self.K_Anonymity}-匿名，安全性低，容易遭受偏斜攻击和重标识攻击！")

        listL = []; listT = []; listAK = []
        for each_privacy in self.address_Attr[1]:
            # start = time.time()
            BoolL,QList = self.IsLDiversity(Series_quasi.keys(),each_privacy)                  ##34秒
            if BoolL:
                print(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
                listL.append(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
            else:
                print(f"{QList}的等价组其隐私属性{each_privacy}不满足{self.L_diversity}-多样性，容易遭受偏斜攻击和重标识攻击！")
                listL.append(f"{QList}的等价组其隐私属性{each_privacy}不满足{self.L_diversity}-多样性，容易遭受偏斜攻击和重标识攻击！")
            # start1 = time.time()
            # print(f"消耗时间{start1 - start}")
            BoolT,QList = self.IsTCloseness(Series_quasi.keys(), each_privacy)  ##180秒
            if BoolT:
                print(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
                listT.append(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
            else:
                print(f"{QList}的等价组其隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")
                listT.append(f"隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")
            # print(f"消耗时间{time.time()-start1}")

            if BoolK:                                                                               ##若不满足K匿名，则必不满足(0.5,k)-Anonymity
                # start2 = time.time()
                BoolAK, QList = self.IsAKAnonymity(Series_quasi.keys(), each_privacy)  ##180秒
                if BoolAK:
                    print(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                    listAK.append(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                else:
                    print(f"{QList}的等价组其隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                    listAK.append(f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                # print(f"耗时为：{time.time() - start2}")
            
            else:
                print(f"{QList}的等价组其隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                listAK.append(f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")

        return {
            "K-Anonymity":_list[0],
            "L-Diversity":listL,
            "T-Closeness":listT,
            "(α,k)-Anonymity":listAK
        }
        # Send_result(self.worker_uuid, res_dict={
        #     "K-Anonymity":_list[0],
        #     "L-Diversity":listL,
        #     "T-Closeness":listT,
        #     "(α,k)-Anonymity":listAK
        # })
        # _list.append(listL)
        # _list.append(listT)
        # _list.append(listAK)


##数据可用性
class Data_availability(Config):
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)

    def Get_CDM(self, Series_quasi):
        '''
        :param Series_quasi: 所有准标识符及其数量
        :return: 返回数据可辨别度CDM， 就是将所有的等价组个数K的平方累加
        '''
        AccumulationN = 0
        for each in Series_quasi:
            AccumulationN += each ** 2
        return AccumulationN

    def Get_CAVG(self,Series_quasi):
        '''
        :param Series_quasi: 所有准标识符及其数量
        :return: 返回归一化的平均等价组大小度量（就是用之前的平均泛化程度/K）
        '''
        address_All = self._Num_address()  ##得到address中元素的总数
        length = len(Series_quasi)                                    ##得到所有等价组个数
        return round(address_All / (length * self.K_Anonymity), 4)

    def Get_SupRatio(self):
        '''
        :return: 返回数据记录隐匿率，即隐匿的记录数目 / 原数据集的总个数，由于记录隐匿元组和整体数量相比少的多的多，所以直接除，没有声明保留小数点几位
        '''
        return self.n_s / (self._Num_address() + self.n_s)

    def Get_NCP(self,Series_quasi):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:数据损失度评价   归一化确定性惩罚
        即便是同一种数据，泛化结果也多有不同；因此这个函数还仅仅是初版，10万条数据，耗时2秒
        '''
        ILoss = 0
        length = len(self.address_Attr[0])  ##准标识符属性个数
        Age_range = 100         ##年龄整体的取值范围

        for i in range(len(Series_quasi)):
            NI_loss = self._Thread_NI_Loss(Series_quasi.keys()[i], Series_quasi[i], Age_range)
            ILoss += NI_loss
        return round(ILoss / length, 4)


    def _Thread_NI_Loss(self,Series_quasi_key, Series_quasi_num, Age_range):
        ageNum = Series_quasi_key[1].split("-")
        NI_Loss = (0 + (int(ageNum[1]) - int(ageNum[0])) / Age_range + Series_quasi_key[2].count("*") / len( Series_quasi_key[2]) + Series_quasi_key[3].count("*") / len(Series_quasi_key[3])) * Series_quasi_num  ##小数点后保留4位小数
        return NI_Loss

    def Get_Entropy_based_Average_loss(self, Series_quasi):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:返回基于熵的数据损失度
        原本的函数或者说论文有问题（随笔-基于熵的隐私度量），其对熵的理解有本质上的错误，故将函数修改。
        针对等价组个数，将现有熵值 / 最大熵，即为数据损失度。越接近1，损失度越大；越接近0，损失度越小。
        最大熵表示原始数据的熵值，表示原数据的不确定性；现有熵并非香农熵，而是表明现有数据的不确定性
        例如：原数据数据个数为8，泛化后分布为（2,2,4），则基于熵的数据损失度为1/2
        '''
        Num_All = self._Num_address()
        S0 = math.log2(Num_All)  ##原始数据的匿名熵,即最大熵
        Num_Loss = 0
        for Num_each in Series_quasi:
            Num_Loss += (math.log2(Num_each)) * Num_each / Num_All
        return Num_Loss / S0

    def runL(self, Series_quasi):
        CDM = self.Get_CDM(Series_quasi)
        print(f"数据可辨别度为：{CDM}")
        SupRatio = self.Get_SupRatio()
        print(f"数据记录匿名率为：{round(SupRatio*100,2)}%")
        CAVG = self.Get_CAVG(Series_quasi)
        print(f"归一化平均等价组大小度量：{CAVG}")
        # start = time.time()
        NCP = self.Get_NCP(Series_quasi)
        print(f"数据损失度为：",NCP)
        # print(f"耗时为{time.time()-start}")
        Entropy_based_Average_Loss = self.Get_Entropy_based_Average_loss(Series_quasi)  ##0.23秒
        print(f"基于熵的平均数据损失度为：{round(Entropy_based_Average_Loss * 100, 2)}%")

        return {
            "数据可辨别度":CDM,
            "数据记录匿名率":f"{round(SupRatio*100,2)}%",
            "归一化平均等价组大小度量":CAVG,
            "数据损失度":NCP,
            "基于熵的平均数据损失度":f"{round(Entropy_based_Average_Loss * 100, 2)}%"
        }
        # Send_result(self.worker_uuid,res_dict={
        #     "数据可辨别度":CDM,
        #     "数据记录匿名率":f"{round(SupRatio*100,2)}%",
        #     "归一化平均等价组大小度量":CAVG,
        #     "数据损失度":NCP,
        #     "基于熵的平均数据损失度":f"{round(Entropy_based_Average_Loss * 100, 2)}%"
        # })







if __name__ == '__main__':
    global_uuid = str(uuid.uuid4())  ##定义全局变量uuid数据
    print("uuid为：", global_uuid)
    _dict = {}
    _list = ['local_test_hotel_0_0_0', 'local_test_hotel_0_0_1', 'local_test_hotel_0_0_2', 'local_test_hotel_0_1_0', 'local_test_hotel_0_1_1', 'local_test_hotel_0_1_2', 'local_test_hotel_0_2_0', 'local_test_hotel_0_2_1', 'local_test_hotel_0_2_2', 'local_test_hotel_0_3_0', 'local_test_hotel_0_3_1', 'local_test_hotel_0_3_2', 'local_test_hotel_0_4_0', 'local_test_hotel_0_4_1', 'local_test_hotel_0_4_2', 'local_test_hotel_0_5_0', 'local_test_hotel_0_5_1', 'local_test_hotel_0_5_2', 'local_test_hotel_1_0_0', 'local_test_hotel_1_0_1', 'local_test_hotel_1_0_2', 'local_test_hotel_1_1_0', 'local_test_hotel_1_1_1', 'local_test_hotel_1_1_2', 'local_test_hotel_1_2_0', 'local_test_hotel_1_2_1', 'local_test_hotel_1_2_2', 'local_test_hotel_1_3_0', 'local_test_hotel_1_3_1', 'local_test_hotel_1_3_2', 'local_test_hotel_1_4_0', 'local_test_hotel_1_4_1', 'local_test_hotel_1_4_2', 'local_test_hotel_1_5_0', 'local_test_hotel_1_5_1', 'local_test_hotel_1_5_2']
    for each in _list:
        _dict[each] = Run(2,2,0.95,'mysql+pymysql://root:784512@localhost:3306/local_test',each,global_uuid,["sex","age","id_number","phone"],["day_start","day_end","hotel_name"],"","","")
        # Config(2,2,0.95,"","./data/csv/adult_with_pii.csv",global_uuid,"","","","","").Run()
        print("********************************************************8")
    
    Send_result(global_uuid,res_dict=_dict)