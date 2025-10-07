#date:2025_0501
# Author:Gzy
# 代码详解
# 前面4个全局变量，代表redis数据库的基本信息：ip，端口，编号，密码
#import  我们使用的库

#两个写入函数（重要），Send_result和sendvalue用来将评估结果写入到redis数据库中。其中，Send_result写入详细结果，sendvalue写入指标结果。

#120行之后的run函数为，总函数调用，我们在这个函数中评估所有领域的结果。具体而言，声明基类Config，然后每个领域都有一个子类来继承基类。通过每个子类的runL函数来执行各个领域中指标的评估任务


# Config类为基类，包含一些基础的通用的函数，比如：初始化，等价类，读取数据库数据，得到数据集大小等
# Desensitization_data_character  为Config子类，包含数据特征领域内的所有指标实现，以及一个runL函数，通过runL函数依次调用该领域内所有评估指标代码，然后使用Send_result和sendvalue将评估结果写入到redis数据库中。
# Desensitization_data_quality_evalution  为Config子类，包含数据质量评估领域内的所有指标实现，以及一个runL函数，通过runL函数依次调用该领域内所有评估指标代码，然后使用Send_result和sendvalue将评估结果写入到redis数据库中。
# privacy_protection_metrics  为Config子类，包含安全性度量领域内的所有指标实现，以及一个runL函数，通过runL函数依次调用该领域内所有评估指标代码，然后使用Send_result和sendvalue将评估结果写入到redis数据库中。
# Data_compliance  为Config子类，包含合规性领域内的所有指标实现，以及一个runL函数，通过runL函数依次调用该领域内所有评估指标代码，然后使用Send_result和sendvalue将评估结果写入到redis数据库中。
# Data_availability  为Config子类，包含可用性领域内的所有指标实现，以及一个runL函数，通过runL函数依次调用该领域内所有评估指标代码，然后使用Send_result和sendvalue将评估结果写入到redis数据库中。


from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISPORT, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD
"""
WORKER_ID_MAP_REDISADDRESS = '192.168.1.107'
WORKER_ID_MAP_REDISPORT = 5678
WORKER_ID_MAP_REDISDBNUM = 5
WORKER_ID_MAP_REDISPASSWORD = 'qwb'
"""
# ZSET 键  用于 任务先后顺序
TASKS_ZSET_KEY = 'tasks'

import redis
import json
import math
import time
import datetime
import pandas as pd
import uuid
import psutil
import mysql.connector
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed



def Send_result(worker_id, res_dict, success=True, error_message="no error"):
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
        redis_client = redis.StrictRedis(
                                        host=WORKER_ID_MAP_REDISADDRESS,
                                         port=WORKER_ID_MAP_REDISPORT,
                                         db=WORKER_ID_MAP_REDISDBNUM,
                                         password=WORKER_ID_MAP_REDISPASSWORD)
        redis_client.ping()
        print("已连接到 Redis 服务器。", worker_id, "正在写入")
    except redis.ConnectionError as e:
        print("无法连接到 Redis 服务器：", worker_id, "连接失败", e)
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


def sendvalue(worker_id, key, value, valuetype):
    """
    向 Redis 添加 key 和 value，支持存储单一类型数据或列表数据，并确保数据类型被保留。
    即存储int，float或者str类型的单个数据，或者多个int，float或者str类型的列表数据

    参数:
        worker_ud (str): Redis 连接字符串。
        key (str): 要存储的小指标的名称。
        value (int, float, str): 要存储的小指标结果值Value，可以是单一值或单一类型的列表。
        valuetype (type): 值的类型（int 或 float 或 str）。
    """
    try:
        redis_conn = redis.StrictRedis(
            host=WORKER_ID_MAP_REDISADDRESS,
            port=WORKER_ID_MAP_REDISPORT,
            db=WORKER_ID_MAP_REDISDBNUM,
            password=WORKER_ID_MAP_REDISPASSWORD)

        current_value = redis_conn.get(worker_id)
        if not current_value:
            raise ValueError(f"No data found for worker_id '{worker_id}'")

        # 解析现有数据
        data = json.loads(current_value)

        # 确保 "results" 字段存在并为字典
        if "results" not in data or not isinstance(data["results"], dict):
            data["results"] = {}

        # 验证值的类型
        if isinstance(value, list):
            # 如果是列表，验证每个元素是否匹配指定类型
            if not all(isinstance(v, valuetype) for v in value):
                raise ValueError(f"All elements in the list must be of type {valuetype}")
        else:
            # 如果是单一值，确保值的类型匹配指定类型
            if not isinstance(value, valuetype):
                raise ValueError(f"Value must be of type {valuetype}, but got {type(value)}")

        # 添加或更新 "results" 中的键值
        data["results"][key] = value

        # 保存修改后的数据回 Redis
        redis_conn.set(worker_id, json.dumps(data))
        print(f"Updated 'results' field for worker_id '{worker_id}': {data['results']}")

    except Exception as e:
        print(f"Error occurred: {e}", key, " ", value, " ", valuetype)



def sendvalue2(worker_id, key, value, valuetype):
    """
    向 Redis 添加 key 和 value，支持存储单一类型数据或列表数据，并确保数据类型被保留。
    即存储int，float或者str类型的单个数据，或者多个int，float或者str类型的列表数据
    写入归一化值
    参数:
        worker_ud (str): Redis 连接字符串。
        key (str): 要存储的小指标的名称。
        value (int, float, str): 要存储的小指标结果值Value，可以是单一值或单一类型的列表。
        valuetype (type): 值的类型（int 或 float 或 str）。
    """
    try:
        redis_conn = redis.StrictRedis(
            host=WORKER_ID_MAP_REDISADDRESS,
            port=WORKER_ID_MAP_REDISPORT,
            db=WORKER_ID_MAP_REDISDBNUM,
            password=WORKER_ID_MAP_REDISPASSWORD)

        current_value = redis_conn.get(worker_id)
        if not current_value:
            raise ValueError(f"No data found for worker_id '{worker_id}'")

        # 解析现有数据
        data = json.loads(current_value)

        # 确保 "norm" 字段存在并为字典
        if "norm" not in data or not isinstance(data["norm"], dict):
            data["norm"] = {}

        # 验证值的类型
        if isinstance(value, list):
            # 如果是列表，验证每个元素是否匹配指定类型
            if not all(isinstance(v, valuetype) for v in value):
                raise ValueError(f"All elements in the list must be of type {valuetype}")
        else:
            # 如果是单一值，确保值的类型匹配指定类型
            if not isinstance(value, valuetype):
                raise ValueError(f"Value must be of type {valuetype}, but got {type(value)}")

        # 添加或更新 "norm" 中的键值
        data["norm"][key] = value

        # 保存修改后的数据回 Redis
        redis_conn.set(worker_id, json.dumps(data))
        print(f"Updated 'norm' field for worker_id '{worker_id}': {data['norm']}")

    except Exception as e:
        print(f"Error occurred: {e}", key, " ", value, " ", valuetype)

# ----------------------------------------------------------------总函数---------------------------------------------------------------------------------

def Run(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
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
    handler = Config(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)
    start1 = time.time()
    _TemAll = handler._Function_Data()
    # 数据集的等价类（一切的基础）
    Series_quasi = handler._Function_Series_quasi(_TemAll)  ##结果为升序

    # --------------------------------------多个评估领域--------------------------------------
    ##数据合规性
    handler1 = Data_compliance(k, l, t, url, address, worker_uuid+'-1', QIDs, SA, ID, bg_url, scene)
    handler1.runL(Series_quasi, _TemAll)  ##传递准标识符集合，以及准标识符对应的数量
    ##匿名集数据特征
    handler3 = Desensitization_data_character(k, l, t, url, address, worker_uuid+'-2', QIDs, SA, ID, bg_url, scene)
    handler3.runL(Series_quasi, _TemAll)  ##传递准标识符集合，以及准标识符对应的数量
    # ##数据可用性
    handler2 = Data_availability(k, l, t, url, address, worker_uuid+'-3', QIDs, SA, ID, bg_url, scene)
    handler2.runL(Series_quasi, _TemAll)  ##传递准标识符集合，以及准标识符对应的数量
    # ##数据安全性
    handler5 = Data_Security(k, l, t, url, address, worker_uuid+'-4', QIDs, SA, ID, bg_url, scene)
    handler5.runL(Series_quasi, _TemAll)  ##传递准标识符集合，以及准标识符对应的数量


class Config:
    def __init__(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
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

        #################隐私评估通用配置
        self.scene = scene  ## 数据使用场景

        self.json_address = address  ##脱敏后的数据文件地址
        self.src_url = url  ##脱敏前的数据的数据库地址
        self.address_Attr = [QIDs, SA]  ##数据文件的属性（主要包含：准标识符属性和敏感属性）

        self.worker_uuid = worker_uuid
        self.K_Anonymity = k  ##定义好的K-Anonymity数
        self.L_diversity = l  ##定义好的L_diversity数
        self.T_closeness = t  ##定义好的T紧密度

        ##################风险评估配置---乔万邦
        self.QIDs = QIDs
        self.SA = SA
        self.ID = ID
        self.bg_url = bg_url

        ##################可用性评估-----耿志颖
        self.n_s = 0  ##数据集中被隐匿的记录个数，即原数据集中有却没有在脱敏数据集发布的记录个数，暂时定义为0

    def _Function_Data(self):
        '''
        :return: 返回文件的所有数据
        读取 JSON 或 CSV 文件并返回一个 pandas DataFrame
        '''
        ##如果读取sql文件，将其转化为DataFrame格式
        # url='mysql+pymysql://root:784512@localhost:3306/local_test'
        columns = self.QIDs + self.SA
        query = f"SELECT {', '.join(columns)}  FROM {self.json_address}"
        # 创建SQLAlchemy的Engine对象
        engine = create_engine(self.src_url).connect()
        return pd.read_sql(sql=text(query), con=engine)

    def _Function_Series_quasi(self, _TemAll):
        '''
        :return: 返回数据中的所有准标识符，及其数量,按照升序排列,<class 'pandas.core.series.Series'>格式
        10万条数据，用时0.3秒
        '''
        # _TemAll = self._Function_Data()
        return _TemAll.groupby(self.address_Attr[0], sort=False).size().sort_values()

    def _Probabilistic_Distribution_Privacy(self, each_privacy, _TemAll):
        '''
        :param each_privacy:选取的某一个隐私属性或者某几个隐私属性
        :return:返回针对选取的隐私属性的概率分布,默认降序
        10万条数据，用时0.29秒
        '''
        # _TemAll = self._Function_Data()
        return _TemAll[each_privacy].value_counts(normalize=True)

    def _Probabilistic_Distribution_All_Privacy(self,  _TemAll):
        '''
        10万条数据，用时0.29秒
        '''
        return _TemAll[self.address_Attr[1]].value_counts(normalize=True)

    def _Num_address(self, _TemAll):
        '''
        :return: 返回所有数据的个数
        统一函数接口，一切以_Function_Data为起始点，将数据转化为DataFrame格式，再处理
        10万条数，0.2秒
        '''
        return len(_TemAll)

    def get_memory_usage(self):
        '''
        查看当时的内存占用情况
        '''
        process = psutil.Process()
        mem_info = process.memory_info()
        return mem_info.rss / (1024 * 1024 * 1024)  # Convert bytes to GB

    #   指标结果极差归一化
    def range_normalize(self, min_value, max_value, current_value, is_positive=True):
        if is_positive:
            # 对于正向指标（即指标数值越大越好），采用极差归一化方法
            normalized_value = (current_value - min_value) / (max_value - min_value)
        else:
            # 对于负向指标（即指标数值越小越好），则使用逆极差归一化方法
            normalized_value = (max_value - current_value) / (max_value - min_value)
        return round(normalized_value, 6)

    #   得到列表的平均值
    def calculate_average(self, lst):
        if not lst:
            return 0.0  # 处理空列表的情况，返回默认值或者你认为合适的值
        total_sum = sum(lst)
        average = total_sum / len(lst)
        return average


##匿名集数据特征
class Desensitization_data_character(Config):
    def __init__(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取文件的地址
        '''
        super().__init__(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)

    def Average_degree_annonymity(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi:选取标签对应的所有的准标识符，及其数量
        :return: 返回平均泛化程度，即平均等价类大小
        '''
        address_All = self._Num_address(_TemAll)  ##得到address中元素的总数
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

    def Inherent_Privacy(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 数据集的所有准标识符，及其对应等价组大小
        :return:返回与数据集概率分布的不确定性相同的均匀分布区间长度,数值越大，安全性越低
        '''
        Num1 = 0.0
        Num_All = self._Num_address(_TemAll)
        for each in Series_quasi:
            Num1 += (each / Num_All) * math.log2(Num_All / each)
        return int(2 ** Num1)

    def runL(self, Series_quasi, _TemAll):
        # 数据集记录数量
        Num_All = self._Num_address(_TemAll)

        Dimen_QID = self.Dimension_QID()
        # print("准标识符维数 函数运行完成！")
        Dimen_SA = self.Dimension_SA()
        # print("敏感属性维数 函数运行完成！")
        Attribute_SA = self.Attribute_SA()
        # print("敏感属性种类 函数运行完成！")
        Average_anonymity = self.Average_degree_annonymity(Series_quasi, _TemAll)
        # print("平均泛化程度 函数运行完成！")
        Inherent_Priv = self.Inherent_Privacy(Series_quasi, _TemAll)
        # print("固有隐私 函数运行完成！：")

        Send_result(self.worker_uuid,res_dict={
            "准标识符维数":Dimen_QID,
            "敏感属性维数":Dimen_SA,
            "敏感属性种类":Attribute_SA,
            "平均泛化程度":Average_anonymity,
            "固有隐私":Inherent_Priv
        })

        #将数据写入redis中
        sendvalue(self.worker_uuid, "准标识符维数",Dimen_QID, int)
        sendvalue(self.worker_uuid, "敏感属性维数",Dimen_SA, int)
        sendvalue(self.worker_uuid, "敏感属性种类",Attribute_SA, str)
        sendvalue(self.worker_uuid, "平均泛化程度",Average_anonymity, float)
        sendvalue(self.worker_uuid, "固有隐私",Inherent_Priv, int)

        # 将数据写入redis中
        sendvalue2(self.worker_uuid, "数据特征结果", self.range_normalize(1, Num_All, Average_anonymity, False), float)





##数据安全性
class Data_Security(Config):
    def __init__(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)

    def Get_Distribution_Leakage(self, each_privacy, Series_quasi_keys, _TemAll):
        '''
        :param each_privacy:选取的隐私属性
        :param Series_quasi_keys:所有的准标识符
        :return:返回数据集的分布泄露，即分布泄露可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每一个给定的等价类，测量原始数据集和已发布数据集中敏感属性分布之间的泄露。
        10万条数据，原代码用时1816.1875秒，即30分钟；现在用时150秒，即3分钟
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##得到某一个敏感属性的概率分布
        # _TemAll = self._Function_Data()                                                                   ##得到所有数据，DataFrame格式
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)  ##等价组个数
        res = 0  ##返回值
        Attr = ()  ##对应的等价组
        print("分布泄露函数运行完成！,马上计算")
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            sum_leakage = 0  ##分布泄露，Distribution Leakage
            for key in Distribution_P.keys():
                if key in Distribution_QP.keys():
                    sum_leakage += (Distribution_QP[key] - Distribution_P[key]) ** 2
                else:
                    sum_leakage += (Distribution_P[key]) ** 2
            if math.sqrt(sum_leakage) > res:  ##返回等价组中最大的那个分布泄露
                res = math.sqrt(sum_leakage)
                Attr = Series_quasi_keys[i]
        return round(res,6), Attr

    def Get_Distribution_Leakage_Max(self, each_privacy,_TemAll):
        '''
        :param each_privacy:选取的隐私属性
        :return:返回数据集的分布泄露，即分布泄露可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每一个给定的等价类，测量原始数据集和已发布数据集中敏感属性分布之间的泄露。
        10万条数据，原代码用时1816.1875秒，即30分钟；现在用时150秒，即3分钟
        归一化处理处理需要求得最大值，即原始数据中的分布泄露，这里使用理论上的原始数据分布泄露，而不是真实的分布泄露
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##得到某一个敏感属性的概率分布
        probs = Distribution_P.values.tolist()  # Series → list
        min_prob = min(probs)
        res = 1 - 2* min_prob + sum(p ** 2 for p in probs)
        return round(res,6)

    def Get_Entropy_Leakage(self, each_privacy, Series_quasi_keys, _TemAll):
        '''
        :param each_privacy:选取的隐私属性
        :param Series_quasi_keys:所有的准标识符
        :return:返回数据集的熵泄露，即通过原始分布的初始熵与等价组的熵之间的差异度，来衡量等价类中个体隐私泄露的程度。
        10万条数据，用时45秒
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##得到某一个敏感属性的概率分布
        Hmax = -(sum([i * math.log2(i) for i in Distribution_P]))  ##数据整体关于某一个敏感属性的熵值
        # _TemAll = self._Function_Data()                                                                   ##得到所有数据，DataFrame格式
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)  ##等价组个数
        res = 0  ##返回值
        Attr = ()
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Entropy_leakage = float(Hmax)  ##熵泄露，Entropy Leakage
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            for each_value in Distribution_QP:
                Entropy_leakage += each_value * math.log2(each_value)
            if math.fabs(Entropy_leakage) > res:
                res = math.fabs(Entropy_leakage)
                Attr = Series_quasi_keys[i]
        return round(res,6), Attr

    def Get_Entropy_Leakage_Max(self, each_privacy, _TemAll):
        '''
        :param each_privacy:选取的隐私属性
        :return:返回数据集的熵泄露，即通过原始分布的初始熵与等价组的熵之间的差异度，来衡量等价类中个体隐私泄露的程度。
        10万条数据，用时45秒
        用于归一化
        '''
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##得到某一个敏感属性的概率分布
        Hmax = -(sum([i * math.log2(i) for i in Distribution_P]))  ##数据整体关于某一个敏感属性的熵值
        return round(Hmax,6)


    def Get_Positive_Information_Disclosure(self, each_privacy, Series_quasi, _TemAll):
        '''
        :param each_privacy: 选择的某一个隐私属性
        :param Series_quasi: 整体数据的准标识符，及其对应等价组大小
        :return: 返回选定隐私属性的隐私增益的最大值。隐私增益是指准标识数据对隐私数据的披露程度
        10万条数据，4秒
        平均隐私增益
        '''

        AverageTem = 0
        Grouped = _TemAll.groupby(each_privacy, sort=False)  ##将数据按照所选敏感属性分组
        for each in Grouped:  ##选取某个敏感属性值
            Quasi_P = each[1][self.address_Attr[0]].value_counts()  ##得到所有包含这个敏感属性值的等价组
            Positive_Num = 0  ##所有包含这个敏感属性值的等价组大小之和
            for each_quasi in Quasi_P.keys():
                Positive_Num += Series_quasi[each_quasi]
            AverageTem += (Positive_Num - len(each[1])) / len(each[1])
        return round(AverageTem / len(Grouped),6)     ##选取隐私增益的平均值

    def Get_Positive_Information_Disclosure_Max(self, each_privacy,  _TemAll):
        '''
        :param each_privacy: 选择的某一个隐私属性
        :return: 返回选定隐私属性的隐私增益的最大值。隐私增益是指准标识数据对隐私数据的披露程度
        10万条数据，4秒
        平均隐私增益
        '''
        AverageTem = 0
        address_All = self._Num_address(_TemAll)  ##得到address中元素的总数
        Grouped = _TemAll.groupby(each_privacy, sort=False)  ##将数据按照所选敏感属性分组
        for each in Grouped:  ##选取某个敏感属性值
            AverageTem += (address_All - len(each[1])) / len(each[1])
        return round(AverageTem / len(Grouped),6)     ##选取隐私增益的平均值

    def Get_KL_Divergence(self, each_privacy, Series_quasi_keys, _TemAll):
        '''
        :param Series_quasi_keys: 文件中所有的准标识符
        :param each_privacy:选取的隐私属性
        :return: 返回所有等价组中的最大KL_Divergence，即用KL_Divergence来衡量每一个等价组中的隐私属性分布与整体分布之间的距离
        10万条数据，用时50秒
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##返回整个数据中某一个敏感属性的概率分布
        length = len(Series_quasi_keys)
        res = 0
        Attr = ()
        # _TemAll = self._Function_Data()
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            Num_distance = 0
            for each_Attribute in Distribution_QP.keys():  ##KL_Divergence计算
                Num_distance += (Distribution_QP[each_Attribute] * math.log2(
                    Distribution_QP[each_Attribute] / Distribution_Privacy[each_Attribute]))
            if Num_distance > res:  ##找到最大值
                res = Num_distance
                Attr = Series_quasi_keys[i]
        return round(res,6), Attr

    def Get_KL_Divergence_Max(self, each_privacy, _TemAll):
        '''
        :param each_privacy:选取的隐私属性
        :return: 返回所有等价组中的最大KL_Divergence，即用KL_Divergence来衡量每一个等价组中的隐私属性分布与整体分布之间的距离
        10万条数据，用时50秒
        用于归一化处理
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##返回整个数据中某一个敏感属性的概率分布
        probs = Distribution_Privacy.values.tolist()  # Series → list
        min_prob = min(probs)
        res = math.log2(1/min_prob)
        return round(res,6)


    def Get_Entropy_based_Re_indentification_Risk(self, Series_quasi_keys, _TemAll):
        '''
        :param Series_quasi_keys:数据集中所有的准标识符
        :return:整体的基于熵的重识别风险
        10万条数据，4秒
        '''
        # _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())  ##得到所有数据中所有敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据按照所有的敏感属性分组，如性别，年龄等
        # print( Grouped.get_group(Series_quasi_keys[0])[self.address_Attr[1]].value_counts(normalize=True))
        # print(Hmax)
        length = len(Series_quasi_keys)  ##等价组个数
        res = 0  ##返回值，重识别风险
        for i in range(length):
            ##得到某个等价组的所有敏感属性的概率分布,默认降序
            Distribution_Quasi = Grouped.get_group(Series_quasi_keys[i])[self.address_Attr[1]].value_counts(
                normalize=True)
            ReIndent_Risk = float(Hmax)  ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_Quasi:  ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:  ##得到重识别风险的最大值
                res = ReIndent_Risk
            if (res / Hmax) > 0.9:  ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4), Series_quasi_keys[i]
        return round(float(res / Hmax), 4), Series_quasi_keys[i]


    def Get_Entropy_based_Re_indentification_Risk_QID(self, each_QID, _TemAll):
        '''
        :param each_QID:选择的准标识符中的某一属性值
        :return:返回基于某一（准标识符中的）属性的风险度量
        10万条数据，用时3秒
        '''
        # _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())  ##得到所有数据中所有敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Grouped = _TemAll.groupby(each_QID, sort=False)  ##将数据按照选取的某一（准标识符中的）属性分组，如性别，年龄等
        res = 0  ##返回值，重识别风险
        Attr = ()  ##返回值，风险值对应的属性值
        for each in Grouped:  ##选取某个准标识符属性值，如性别='男'
            Distribution_Quasi = each[1][self.address_Attr[1]].value_counts(normalize=True)  ##得到所有包含这个准标识符属性值的等价组概率分布
            ReIndent_Risk = float(Hmax)  ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_Quasi:  ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:  ##得到重识别风险的最大值
                res = ReIndent_Risk
                Attr = each[0]
            if (res / Hmax) > 0.9:  ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4), Attr
        return round(float(res / Hmax), 4), Attr


    def Get_ERIRQ_Min(self, _TemAll):
        '''
        :return:返回基于某一（准标识符中的）属性的风险度量
        10万条数据，用时3秒
        用于归一化
        '''
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())  ##得到所有数据中所有敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Distribution_P = self._Probabilistic_Distribution_All_Privacy( _TemAll)  ##得到所有数据中所有敏感属性的概率分布
        HE = -(sum([i * math.log2(i) for i in Distribution_P]))  ##数据整体关于某一个敏感属性的熵值
        return round(float((Hmax - HE) / Hmax), 4)

    def Get_Entropy_based_Re_indentification_Risk_with(self, Series_quasi_keys, each_privacy, _TemAll):
        '''
        :param Series_quasi_keys:数据集中所有的准标识符
        :param each_privacy: 选取的敏感属性
        :return:返回某一个敏感属性的基于熵的重识别风险
        10万条数据，用时2秒
        '''
        # _TemAll = self._Function_Data()                                                         ##得到所有数据，DataFrame格式
        Num_Privacy = len(_TemAll[each_privacy].drop_duplicates())  ##得到所有数据中所选敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)

        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)  ##等价组个数
        res = 0  ##返回值，重识别风险
        res_quasi = ()  ##返回值，风险最大的等价组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            ReIndent_Risk = float(Hmax)  ##重识别风险初始化为Hmax，Re_indentification Risk
            for each_value in Distribution_QP:  ##计算每个等价组的重识别风险
                ReIndent_Risk += each_value * math.log2(each_value)
            if ReIndent_Risk > res:  ##得到重识别风险的最大值
                res = ReIndent_Risk
                res_quasi = Series_quasi_keys[i]
            if (res / Hmax) > 0.9:  ##如果重识别风险大于90%，直接返回
                return round(float(res / Hmax), 4), res_quasi
        return round(float(res / Hmax), 4), res_quasi


    def Get_ERIR_Min(self,  each_privacy, _TemAll):
        '''
        :param each_privacy: 选取的敏感属性
        :return:返回某一个敏感属性的基于熵的重识别风险
        用于归一化
        '''
        Num_Privacy = len(_TemAll[each_privacy].drop_duplicates())  ##得到所有数据中所选敏感属性的种类个数，即set集大小
        Hmax = math.log2(Num_Privacy)
        Distribution_P = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##得到所选敏感属性的概率分布
        HE = -(sum([i * math.log2(i) for i in Distribution_P]))  ##数据整体关于某一个敏感属性的熵值
        return round(float((Hmax - HE) / Hmax), 4)



    def runL(self, Series_quasi, _TemAll):
        listP = []
        listQ = []  ##一行同时初始化多个参数，要用分号而不要用逗号
        listD = [];
        listE = [];
        listF = [];
        listKL = []

        valueDistribution_Leakage = [];
        valueMaxDistribution_Leakage = [];
        valueEntropy_Leakage = [];
        valueMaxEntropy_Leakage = [];
        valuePositive_Information_Disclosure = [];
        valueMaxPositive_Information_Disclosure = [];
        valueKL_Divergence = [];
        valueMaxKL_Divergence = [];
        ValuePrivacy = [];
        ValueMinPrivacy = [];
        ValueQusai = [];
        ValueMinQusai = [];
        ValueALL = [];

        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            _max, Attr = self.Get_Distribution_Leakage(each_privacy, Series_quasi.keys(), _TemAll)
            # print(f"数据集针对{each_privacy}的分布泄露为：{round(_max, 4)},对应等价组为{Attr}")
            listD.append(f"数据集针对{each_privacy}的分布泄露为：{round(_max, 4)},对应等价组为{Attr}")
            valueDistribution_Leakage.append(_max)
            MaxValue = self.Get_Distribution_Leakage_Max(each_privacy, _TemAll)  # 数值用于归一化
            valueMaxDistribution_Leakage.append(MaxValue)
        # print(valueDistribution_Leakage)
        # print(valueMaxDistribution_Leakage)
        currentValue = self.calculate_average(valueDistribution_Leakage)
        MV = self.calculate_average(valueMaxDistribution_Leakage)
        valueDistribution_Leakage2 = self.range_normalize(0, MV, currentValue, False)     ##归一化结果，float，0到1之间
        # print(f"归一化后的      分布泄露为{self.range_normalize(0, MV, currentValue, False)}")

        print("分布泄露计算完成！")

        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            _max, Attr = self.Get_Entropy_Leakage(each_privacy, Series_quasi.keys(), _TemAll)
            # print(f"数据集针对{each_privacy}的熵泄露为：{round(_max, 4)},对应等价组为{Attr}")
            listE.append(f"数据集针对{each_privacy}的熵泄露为：{round(_max, 4)},对应等价组为{Attr}")
            valueEntropy_Leakage.append(_max)
            MaxValue = self.Get_Entropy_Leakage_Max(each_privacy, _TemAll)  # 数值用于归一化
            valueMaxEntropy_Leakage.append(MaxValue)
        # print(valueEntropy_Leakage)
        # print(valueMaxEntropy_Leakage)
        currentValue = self.calculate_average(valueEntropy_Leakage)
        MV = self.calculate_average(valueMaxEntropy_Leakage)
        valueEntropy_Leakage2 = self.range_normalize(0, MV, currentValue, False)  ##归一化结果，float，0到1之间
        # print(f"归一化后的      熵泄露为{self.range_normalize(0, MV, currentValue, False)}")


        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            AverageNum = self.Get_Positive_Information_Disclosure(each_privacy, Series_quasi, _TemAll)
            # print(f"数据集针对{each_privacy}的隐私增益为：{AverageNum}")
            listF.append(f"数据集针对{each_privacy}的隐私增益为：{AverageNum}")
            valuePositive_Information_Disclosure.append(AverageNum)
            MaxValue = self.Get_Positive_Information_Disclosure_Max(each_privacy, _TemAll)  # 数值用于归一化
            valueMaxPositive_Information_Disclosure.append(MaxValue)
        # print(valuePositive_Information_Disclosure)
        # print(valueMaxPositive_Information_Disclosure)
        currentValue = self.calculate_average(valuePositive_Information_Disclosure)
        MV = self.calculate_average(valueMaxPositive_Information_Disclosure)
        valuePositive_Information_Disclosure2 = self.range_normalize(0, MV, currentValue, True)  ##归一化结果，float，0到1之间
        # print(f"归一化后的      熵泄露为{self.range_normalize(0, MV, currentValue, True)}")
        print("熵泄露计算完成！")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            _max, Attr = self.Get_KL_Divergence(each_privacy, Series_quasi.keys(), _TemAll)
            # print(f"数据集针对{each_privacy}的KL_Divergence为：{_max},对应等价组为{Attr}")
            listKL.append(f"数据集针对{each_privacy}的KL_Divergence为：{_max},对应等价组为{Attr}")
            valueKL_Divergence.append(_max)
            MaxValue = self.Get_KL_Divergence_Max(each_privacy, _TemAll)  # 数值用于归一化
            valueMaxKL_Divergence.append(MaxValue)
        # print(valueKL_Divergence)
        # print(valueMaxKL_Divergence)
        currentValue = self.calculate_average(valueKL_Divergence)
        MV = self.calculate_average(valueMaxKL_Divergence)
        valueKL_Divergence2 = self.range_normalize(0, MV, currentValue, False)  ##归一化结果，float，0到1之间
        # print(f"归一化后的      熵泄露为{self.range_normalize(0, MV, currentValue, False)}")

        print("KL_Divergence计算完成！")
        for each_privacy in self.address_Attr[1]:
            Entropy_Re_Risk_with, QList = self.Get_Entropy_based_Re_indentification_Risk_with(Series_quasi.keys(),
                                                                                              each_privacy, _TemAll)
            # print(f"针对敏感属性{each_privacy}基于熵的重识别风险为：{Entropy_Re_Risk_with * 100}%，对应等价组为{QList}")  ##149秒/132秒/132秒
            listP.append(
                f"针对敏感属性{each_privacy}基于熵的重识别风险为：{Entropy_Re_Risk_with * 100}%，对应等价组为{QList}")
            ValuePrivacy.append(Entropy_Re_Risk_with)
            MinValue = self.Get_ERIR_Min(each_privacy, _TemAll)  # 数值用于归一化
            ValueMinPrivacy.append(MinValue)
        # print(ValuePrivacy)
        # print(ValueMinPrivacy)
        currentValue = self.calculate_average(ValuePrivacy)
        MV = self.calculate_average(ValueMinPrivacy)  ##最小值
        ValuePrivacy2 = self.range_normalize(MV, 1, currentValue, False)     ##归一化结果，float，0到1之间
        # print(f"归一化后的      敏感属性重识别风险为{self.range_normalize(MV, 1, currentValue, False)}")
        print("敏感属性重识别风险计算完成！")

        for each_QID in self.address_Attr[0]:
            Entropy_Re_Risk_QID, Attr = self.Get_Entropy_based_Re_indentification_Risk_QID(each_QID, _TemAll)
            # print(f"针对准标识符属性{each_QID}基于熵的重识别风险为：{round(Entropy_Re_Risk_QID * 100, 2)}%，对应属性值为{Attr}")
            listQ.append(
                f"针对准标识符属性{each_QID}基于熵的重识别风险为：{round(Entropy_Re_Risk_QID * 100, 2)}%，对应属性值为{Attr}")
            ValueQusai.append(Entropy_Re_Risk_QID)
            MinValue = self.Get_ERIRQ_Min(_TemAll)  # 数值用于归一化
            ValueMinQusai.append(MinValue)
        # print(ValueQusai)
        # print(ValueMinQusai)
        currentValue = self.calculate_average(ValueQusai)
        MV = self.calculate_average(ValueMinQusai)  ##最小值
        ValueQusai2 = self.range_normalize(MV, 1, currentValue, False)   ##归一化结果，float，0到1之间
        # print(f"归一化后的      准标识符重识别风险为{self.range_normalize(MV, 1, currentValue, False)}")
        ##这个归一化结果注定不准确，因为最大值有可能不是1，而是由数据集决定。这里为了不使用原始数据，我们默认其为1
        # 例如，原始数据中我们默认基于任何准标识符都可以重识别出某个记录，但是，可以很简单举个例子
        # 在原始数据中，每个记录都是不同的，我们可以很简单重识别，但是如果仅仅基于性别这一个准标识符，我们很难重识别出某一条记录。
        print("准标识符重识别风险计算完成！")

        Entropy_Re_Risk, QList = self.Get_Entropy_based_Re_indentification_Risk(Series_quasi.keys(), _TemAll)
        # print(f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}")
        ValueALL.append(Entropy_Re_Risk)
        ValueALL2 = self.range_normalize(0, 1, ValueALL[0], False)   ##归一化结果，float，0到1之间


        Send_result(self.worker_uuid,res_dict={
            "分布泄露": listD,
            "熵泄露": listE,
            "隐私增益": listF,
            "KL_Divergence": listKL,
            "敏感属性的重识别风险":listP,
            "单个属性的重识别风险":listQ,
            "基于熵的重识别风险":f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}"
        })

        #将数据写入redis中
        sendvalue(self.worker_uuid, "分布泄露",valueDistribution_Leakage, float)
        sendvalue(self.worker_uuid, "熵泄露",valueEntropy_Leakage, float)
        sendvalue(self.worker_uuid, "隐私增益",valuePositive_Information_Disclosure, float)
        sendvalue(self.worker_uuid, "KL_Divergence",valueKL_Divergence, float)
        sendvalue(self.worker_uuid, "敏感属性重识别风险", ValuePrivacy, float)
        sendvalue(self.worker_uuid, "准标识符重识别风险", ValueQusai, float)
        sendvalue(self.worker_uuid, "整体重识别风险", ValueALL, float)

        #将数据写入redis中
        sendvalue2(self.worker_uuid, "分布泄露",valueDistribution_Leakage2, float)
        sendvalue2(self.worker_uuid, "熵泄露",valueEntropy_Leakage2, float)
        sendvalue2(self.worker_uuid, "隐私增益",valuePositive_Information_Disclosure2, float)
        sendvalue2(self.worker_uuid, "KL_Divergence",valueKL_Divergence2, float)
        sendvalue2(self.worker_uuid, "敏感属性重识别风险", ValuePrivacy2, float)
        sendvalue2(self.worker_uuid, "准标识符重识别风险", ValueQusai2, float)
        sendvalue2(self.worker_uuid, "整体重识别风险", ValueALL2, float)




##数据合规性
class Data_compliance(Config):
    def __init__(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)

    def IsKAnonymity(self, Series_quasi):
        '''
        :param Series_quasi:文件中所有的准标识符，及其数量
        :return: 返回数据集是否符合K匿名，即用准标识符划分出的等价组其包含的记录个数是否均大于K。若满足K匿名，返回True；反之，返回False
        '''
        if Series_quasi.iloc[0] < self.K_Anonymity:  ##由于Series_quasi默认排序为降序，我们将其改为升序，所以选择最小的等价组与K比较
            return False
        return True

    def IsLDiversity(self, Series_quasi_keys, each_privacy, _TemAll):
        '''
        :param address：选取的文件地址
        :param each_privacy:选取的隐私属性
        :return: 返回输入数据集是否符合L多样性，即每一个等价组中是否包含L个以上不同的隐私属性。若满足L多样性，返回True；反之，返回False
        10万条数据，耗时0.5秒
        '''
        if self.L_diversity == 1:  ##若L多样性值为1，必定符合
            return True
        length = len(Series_quasi_keys)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        for i in range(length):
            ##得到某一等价组的某一隐私属性的L多样性，即有多少个不同的属性值
            Diversity = len(Grouped.get_group(Series_quasi_keys[i])[each_privacy].drop_duplicates())
            if Diversity < self.L_diversity:
                return False, Series_quasi_keys[i]
        return True, -1

    def IsTCloseness(self, Series_quasi_keys, each_privacy, _TemAll):
        '''
        :param Series_quasi_keys: 文件中所有的准标识符
        :param each_privacy:选取的隐私属性
        :return: 返回输入等价组Series_quasi是否符合T紧密性，即每一个等价组中的隐私属性分布与整体分布之间的距离是否小于T_Closeness。若满足，返回True；反之，返回False
        显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，这里采用的是EMD
        10万条数据，用时3秒
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)  ##返回整个数据中某一个敏感属性的概率分布
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        length = len(Series_quasi_keys)


        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            Num_distance = 0
            for each_Attribute in Distribution_QP.keys():
                if Distribution_QP[each_Attribute] > Distribution_Privacy[each_Attribute]:
                    Num_distance += (Distribution_QP[each_Attribute] - Distribution_Privacy[each_Attribute])
            if Num_distance > self.T_closeness:
                return False, Series_quasi_keys[i]

        return True, -1


    def IsAKAnonymity(self, Series_quasi_keys, each_privacy, _TemAll):
        '''
        :param Series_quasi_keys:数据集的所有准标识符
        :param each_privacy:选定的隐私属性
        :return: (α,k)-Anonymity确保在所有等价类中，没有任何一个敏感属性可以占主导地位。这里内定α=0.5。每一个等价类中不能有任何一个敏感属性占比超过0.5。
        10万条数据，耗时0.3秒
        '''

        length = len(Series_quasi_keys)
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)  ##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)

            if Distribution_QP.iloc[0] > 0.5:
                return False, Series_quasi_keys[i]
        return True, -1

    def runL(self, Series_quasi, _TemAll):
        BoolK = self.IsKAnonymity(Series_quasi)  ##若为True，满足K匿名；反之，不满足
        _list = []
        ValueK_Anonymity = [];
        ValueL_Diversity = [];
        ValueT_Closeness = [];
        ValueAK_Anonymity = []

        if BoolK:  ##若不满足K匿名，则必不满足(0.5,k)-Anonymity
            # print(f"满足{self.K_Anonymity}-匿名")
            _list.append(f"满足{self.K_Anonymity}-匿名")
            ValueK_Anonymity.append(1)
        else:
            print(
                f"准标识符为{Series_quasi.keys()[0]}的等价组不满足{self.K_Anonymity}-匿名，安全性低，容易遭受偏斜攻击和重标识攻击！")
            _list.append(
                f"准标识符为{Series_quasi.keys()[0]}的等价组不满足{self.K_Anonymity}-匿名，安全性低，容易遭受偏斜攻击和重标识攻击！")
            ValueK_Anonymity.append(0)

        listL = [];
        listT = [];
        listAK = []
        for each_privacy in self.address_Attr[1]:
            start = time.time()
            BoolL, QList = self.IsLDiversity(Series_quasi.keys(), each_privacy, _TemAll)  ##34秒
            if BoolL:
                # print(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
                listL.append(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
                ValueL_Diversity.append(1)

            else:
                # print(f"{QList}的等价组其隐私属性{each_privacy}不满足{self.L_diversity}-多样性，容易遭受偏斜攻击和重标识攻击！")
                listL.append(
                    f"{QList}的等价组其隐私属性{each_privacy}不满足{self.L_diversity}-多样性，容易遭受偏斜攻击和重标识攻击！")
                ValueL_Diversity.append(0)


            BoolT, QList = self.IsTCloseness(Series_quasi.keys(), each_privacy, _TemAll)  ##180秒
            if BoolT:
                # print(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
                listT.append(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
                ValueT_Closeness.append(1)
            else:
                # print(f"{QList}的等价组其隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")
                listT.append(f"隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")
                ValueT_Closeness.append(0)


            if BoolK:  ##若不满足K匿名，则必不满足(0.5,k)-Anonymity
                BoolAK, QList = self.IsAKAnonymity(Series_quasi.keys(), each_privacy, _TemAll)  ##180秒
                if BoolAK:
                    # print(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                    listAK.append(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                    ValueAK_Anonymity.append(1)

                else:
                    # print( f"{QList}的等价组其隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                    listAK.append(f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                    ValueAK_Anonymity.append(0)

            else:
                ##如果不满足2-匿名的话，必定不满足（0.5,2）-匿名。由于等家类是安装大小升序排列，我们选取第一个（最小值）就可以了
                # print(f"{Series_quasi.keys()[0]}的等价组其隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                listAK.append(
                    f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity，容易遭受偏斜攻击和重标识攻击！")
                ValueAK_Anonymity.append(0)

        Send_result(self.worker_uuid, res_dict={
            "K-Anonymity":_list[0],
            "L-Diversity":listL,
            "T-Closeness":listT,
            "(α,k)-Anonymity":listAK
        })

        #将数据发送到redis库中
        #K匿名的列表，存储int值
        sendvalue(self.worker_uuid, "K_Anonymity", ValueK_Anonymity, int)
        sendvalue(self.worker_uuid, "L_Diversity", ValueL_Diversity, int)
        sendvalue(self.worker_uuid, "T_Closeness", ValueT_Closeness, int)
        sendvalue(self.worker_uuid, "AK_Anonymity", ValueAK_Anonymity, int)

        # 合并所有列表
        all_values = ValueK_Anonymity + ValueL_Diversity + ValueT_Closeness + ValueAK_Anonymity
        ratio = all_values.count(1) / len(all_values)
        # 将数据写入redis中
        sendvalue2(self.worker_uuid, "合规性结果", self.range_normalize(0, 1, round(ratio, 6), True), float)






##数据可用性
class Data_availability(Config):
    def __init__(self, k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k, l, t, url, address, worker_uuid, QIDs, SA, ID, bg_url, scene)

    def Get_CDM(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 所有准标识符及其数量
        :return: 返回数据可辨别度CDM， 就是将所有的等价组个数K的平方累加
        无需归一化处理
        '''
        AccumulationN = 0
        for each in Series_quasi:
            AccumulationN += each ** 2
        return AccumulationN

    def Get_CAVG(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 所有准标识符及其数量
        :return: 返回归一化的平均等价组大小度量（就是用之前的平均泛化程度/K）
        无需归一化处理
        不能只保留小说点后4位，太少了，数值太小
        '''
        address_All = self._Num_address(_TemAll)  ##得到address中元素的总数
        length = len(Series_quasi)  ##得到所有等价组个数
        return   address_All / (length)

    def Get_SupRatio(self, _TemAll):
        '''
        :return: 返回数据记录隐匿率，即隐匿的记录数目 / 原数据集的总个数，由于记录隐匿元组和整体数量相比少的多的多，所以直接除，没有声明保留小数点几位
        '''
        return self.n_s / (self._Num_address(_TemAll) + self.n_s)

    def Get_NCP(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:数据损失度评价   归一化确定性惩罚
        即便是同一种数据，泛化结果也多有不同；因此这个函数还仅仅是初版，10万条数据，耗时2秒
        无需归一化处理
        '''
        ILoss = 0
        length = len(self.address_Attr[0])  ##准标识符属性个数
        Age_range = 100  ##年龄整体的取值范围

        for i in range(len(Series_quasi)):
            NI_loss = self._Thread_NI_Loss(Series_quasi.keys()[i], Series_quasi.iloc[i], Age_range)
            ILoss += NI_loss
        return round(ILoss / (length * self._Num_address(_TemAll)), 4)

    def _Thread_NI_Loss(self, Series_quasi_key, Series_quasi_num, Age_range):
        NI_Loss = 0
        for each in Series_quasi_key:
            if isinstance(each, str):
                if '*' in each:
                    NI_Loss += each.count("*") / len(each)
                elif '-' in each:  # 判断是否为年龄型数据
                    ageNum = each.split("-")
                    if len(ageNum) == 2 and ageNum[0].isdigit() and ageNum[1].isdigit():
                        # 确保两个部分是数字,确保年龄区间为0到100，不存在100岁以上的记录
                        NI_Loss += (int(ageNum[1]) - int(ageNum[0])) / Age_range


        NI_Loss *= Series_quasi_num  ##小数点后保留4位小数
        return NI_Loss

    def Get_Entropy_based_Average_loss(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:返回基于熵的数据损失度
        原本的函数或者说论文有问题（随笔-基于熵的隐私度量），其对熵的理解有本质上的错误，故将函数修改。
        针对等价组个数，将现有熵值 / 最大熵，即为数据损失度。越接近1，损失度越大；越接近0，损失度越小。
        最大熵表示原始数据的熵值，表示原数据的不确定性；现有熵并非香农熵，而是表明现有数据的不确定性
        例如：原数据数据个数为8，泛化后分布为（2,2,4），则基于熵的数据损失度为1/2
        '''
        Num_All = self._Num_address(_TemAll)
        S0 = math.log2(Num_All)  ##原始数据的匿名熵,即最大熵
        Num_Loss = 0
        for Num_each in Series_quasi:
            Num_Loss += (math.log2(Num_each)) * Num_each / Num_All
        return Num_Loss / S0

    def Get_Uniqueness(self, Series_quasi, _TemAll):
        '''
        :param Series_quasi: 数据集中所有的准标识符
        :return:返回基于熵的数据损失度
        返回数据集中唯一记录的占比。在最简单的定义中，如果等价类中只有一个记录，那么
        该记录被认为是唯一的。唯一记录比非唯一记录更容易被重新识别
        '''
        Num_All = self._Num_address(_TemAll)
        Num_Uniqueness = 0
        for Num_each in Series_quasi:
            if Num_each == 1:
                Num_Uniqueness += 1
            else:  ##由于Series_quasi存储着所有等价组大小，而且按升序排列
                break
        return Num_Uniqueness / Num_All

    def runL(self, Series_quasi, _TemAll):
        # 数据集记录数量
        Num_All = self._Num_address(_TemAll)

        CDM = self.Get_CDM(Series_quasi, _TemAll)
        # print(f"数据可辨别度为：{CDM}")
        SupRatio = self.Get_SupRatio(_TemAll)
        # print(f"数据记录匿名率为：{round(SupRatio * 100, 2)}%")
        CAVG = self.Get_CAVG(Series_quasi, _TemAll)
        # print(f"归一化平均等价组大小度量 ：{CAVG}")
        start = time.time()
        NCP = self.Get_NCP(Series_quasi, _TemAll)
        # print(f"数据损失度为：", NCP)
        Entropy_based_Average_Loss = self.Get_Entropy_based_Average_loss(Series_quasi, _TemAll)  ##0.23秒
        # print(f"基于熵的平均数据损失度为：{round(Entropy_based_Average_Loss * 100, 2)}%")

        Uniqueness_proportion = self.Get_Uniqueness(Series_quasi, _TemAll)
        # print( f"数据集中唯一记录的占比为：{round(Uniqueness_proportion * 100, 2)}%，唯一记录比非唯一记录更容易被重新识别，极易遭受重标识攻击和偏斜攻击")


        Send_result(self.worker_uuid,res_dict={
            "数据可辨别度":CDM,
            "数据记录匿名率":f"{round(SupRatio*100,2)}%",
            "平均泛化程度":CAVG,
             "数据损失度":NCP,
            "基于熵的平均数据损失度":f"{round(Entropy_based_Average_Loss * 100, 2)}%",
            "唯一记录占比": f'{round(Uniqueness_proportion * 100, 2)}%',
        })


        sendvalue(self.worker_uuid, "数据可辨别度", CDM, int)
        sendvalue(self.worker_uuid, "数据记录匿名率", SupRatio, float)
        sendvalue(self.worker_uuid, "平均泛化程度", CAVG, float)
        sendvalue(self.worker_uuid, "数据损失度", NCP, float)
        sendvalue(self.worker_uuid, "基于熵的平均数据损失度",Entropy_based_Average_Loss, float)
        sendvalue(self.worker_uuid, "唯一记录占比",Uniqueness_proportion, float)

        sendvalue2(self.worker_uuid, "数据可辨别度", self.range_normalize(Num_All,Num_All*Num_All,CDM,False) , float)
        sendvalue2(self.worker_uuid, "数据记录匿名率", self.range_normalize(0, 1, SupRatio, False), float)
        sendvalue2(self.worker_uuid, "平均泛化程度", self.range_normalize(1, Num_All, CAVG, False), float)
        sendvalue2(self.worker_uuid, "数据损失度", self.range_normalize(0, 1, NCP, False), float)
        sendvalue2(self.worker_uuid, "基于熵的平均数据损失度",self.range_normalize(0, 1, Entropy_based_Average_Loss, False), float)
        sendvalue2(self.worker_uuid, "唯一记录占比",self.range_normalize(0, 1, Uniqueness_proportion, True), float)




