# time：2023_11_01
# Author：Gzy
# Goal：K-anonymity

from py2neo import Graph, Node, Relationship, RelationshipMatcher, NodeMatcher
import math
import pandas as pd
import os
import json
from concurrent.futures import ThreadPoolExecutor,as_completed
import numpy as np
import time
import uuid


# self.uuid = str(uuid.uuid4())  ##定义全局变量uuid数据
# print("uuid为：", self.uuid)

class Config:
    def __init__(self,k,l,t,address,uuid):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        :param uuid: 任务id
        '''
        self.graph = Graph("http://192.168.1.121:7474/browser/", auth=("neo4j", "123456"))

        self.json_address = address  ##待评价的数据文件地址
        self.address_dict = {}
        self.address_dict[self.json_address] = [["sex","age","id_number","phone"] , ["day_start","day_end","hotel_name"]]
        self.uuid=uuid
        self.K_Anonymity = k                                        ##定义好的K-Anonymity数
        self.L_diversity = l                                               ##定义好的L_diversity数
        self.T_closeness = t                                             ##定义好的T紧密度
        self.n_s = 24                                                           ##数据集中被隐匿的记录个数，即原数据集中有却没有在脱敏数据集发布的记录个数，暂时定义为0
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

    def Run(self):
        self.Send_Result()                                                                   ##先创建节点
        address = self.json_address                                                 ##选择的文件
        List_quasi = self._Function_List_quasi(address)               ##获取所有准标识符 15秒
        handler1 = Privacy_data(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler1.run(List_quasi)                          ##传递标签和其对应的准标识符集合，以及准标识符对应的数量
        handler2 = Privacy_policy(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler2.run(List_quasi)                          ##传递标签和其对应的准标识符集合，以及准标识符对应的数量

#脱敏数据：主要包括匿名集数据特征、数据质量评估和隐私保护性度量
class Privacy_data(Config):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

    def _Thread_Probabilistic_Distribution_privacy(self,address,List_quasi_key,each_privacy,Hmax,_TemAll):
        '''
        :param address:选取的文件地址
        :param List_quasi_key:选取的准标识符，作为查询条件得到对应等价类
        :param each_privacy: 选取的隐私属性
        :param Hamx: 针对选取隐私属性的最大熵
        :param _TemAll:选取的文件地址中的所有数据（DatFrame格式）
        :return:返回基于熵的重识别风险度量，通过属性值相同的等价组的相对熵与整个数据最大熵得到
        '''
        KL_i_max= float(Hmax)
        length = len(List_quasi_key)
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件
        Attribute_List_quasi =  pd.value_counts(_QueryCondition.loc[:,each_privacy], normalize=True)
        for each_value in Attribute_List_quasi:
            KL_i_max += each_value * math.log2(each_value)
        return round(float(KL_i_max / Hmax) ,4)

    def _Thread_Probabilistic_Distribution_Combine_Privacy(self,address, each_QID_value, each_QID, Hmax, _TemAll):
        '''
        :param address:选取的文件地址
        :param each_QID_value:选取的属性值，用于区分等价组
        :param each_QID:选取的准标识符中的属性
        :param Hmax:所有敏感属性的最大熵
        :param _TemAll:选取的文件地址中的所有数据（DatFrame格式）
        :return:返回基于熵的重识别风险度量，通过属性值相同的等价组的相对熵与整个数据最大熵得到
        '''
        KL_i_max = float(Hmax)
        _QueryCondition = _TemAll.query(f"{each_QID}=='{each_QID_value}'")  ##query为查询条件
        Distribution_Combine_List = pd.value_counts(_QueryCondition.loc[:, self.address_dict[address][1]].values.tolist(), normalize=True)
        for each_value in Distribution_Combine_List:
            KL_i_max += each_value * math.log2(each_value)
        return round(float(KL_i_max / Hmax ),4)

    def _Thread_Probabilistic_Distribution_Combine_PrivacyAll(self,address, List_quasi_key, Hmax, _TemAll):
        '''
        :param address:选取的文件地址
        :param List_quasi_key: 选取的准标识符的各个属性值，用于区分等价组
        :param Hmax: 针对所有敏感属性的最大熵
        :param _TemAll:选取的文件地址中的所有数据（DatFrame格式）
        :return:返回基于熵的重识别风险度量，通过属性值相同的等价组的相对熵与整个数据最大熵得到
        '''
        KL_i_max = float(Hmax)
        length = len(List_quasi_key)
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件
        Distribution_Combine_List = pd.value_counts(_QueryCondition.loc[:, self.address_dict[address][1]].values.tolist(), normalize=True)
        for each_value in Distribution_Combine_List:
            KL_i_max += each_value * math.log2(each_value)
        return round(float(KL_i_max / Hmax ),4)

    def _Thread_Entropy_Leakage(self,address,List_quasi_key, Hmax, _TemAll,each_privacy):
        '''
        :param address:选取的json文件地址
        :param List_quasi_key: 选取的准标识符用于区分等价组
        :param Hmax: 整个数据集针对某个隐私属性的熵值
        :param _TemAll: 文件中的所有数据
        :param each_privacy: 选取的某个隐私属性
        :return: 返回等价组的分布泄露
        '''
        Entropy_leakage = float(Hmax)
        length = len(List_quasi_key)
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件,找到对应等价组
        Distribution_Privacy_List = pd.value_counts(_QueryCondition.loc[:, each_privacy], normalize=True)
        for each_value in Distribution_Privacy_List:
            Entropy_leakage += each_value * math.log2(each_value)
        return float(math.fabs(Entropy_leakage))

    def _Thread_Distribution_Leakage(self,address,List_quasi_key, List_privacy, _TemAll,each_privacy):
        '''
        :param address:选取的json文件地址
        :param List_quasi_key: 选取的准标识符用于区分等价组
        :param List_privacy: 整个数据集针对某个隐私属性的概率分布
        :param _TemAll: 文件中的所有数据
        :param each_privacy: 选取的某个隐私属性
        :return: 返回等价组的分布泄露
        '''
        sum_leakage = 0.0
        length = len(List_quasi_key)
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件,找到对应等价组
        Distribution_Privacy_List = pd.value_counts(_QueryCondition.loc[:, each_privacy], normalize=True)
        for key in List_privacy.keys():
            if key in Distribution_Privacy_List.keys():
                sum_leakage += (Distribution_Privacy_List[key] - List_privacy[key]) ** 2
            else:
                sum_leakage += (List_privacy[key]) ** 2
        return float(math.sqrt(sum_leakage))

    def _Thread_Positive_Information(self, Name_quasiL, query, Num_address, _TemAll, List_quasi):
        '''
        :param Name_quasiL: 所有准标识符的名字（这里为["sex","age","id_number","phone"]）
        :param query: 查询语句，这里通过隐私属性值找到需要的所有数据
        :param Num_address: 数据集的所有数据个数
        :param _TemAll: 数据集的所有数据
        :param List_quasi: 数据集的所有准标识符，及其等价组大小
        :return: 返回选定的隐私属性的某一个具体的属性值的正面信息披露
        '''
        _QueryCondition = _TemAll.query(query)                                                          ##query为查询条件,找到相同隐私属性值的行（如："day_start = ''2023-10-14"）
        Qusai_Privacy_DataFrame=  _QueryCondition.loc[:, Name_quasiL].drop_duplicates()     ##找到满足query的所有行所属的所有等价组的准标识符
        Qusai_Privacy_List = Qusai_Privacy_DataFrame.values.tolist()                        ##将DataFrame转化为list
        All_Quasi_key = List_quasi.index.tolist()                                                               ##数据集中所有的等价组
        Num2 = 0
        for each in Qusai_Privacy_List:
            Num1 = All_Quasi_key.index(each)
            Num2 += List_quasi[Num1]
        return round((Num_address / Num2) - 1 , 4)

    def run(self, List_quasi):      ##运行程序，主要运行匿名集数据特征、数据质量评估和隐私保护性度量
        address = self.json_address
        handler1 = Desensitization_data_character(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler1.runL(List_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        handler2 = Desensitization_data_quality_evalution(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler2.runL(List_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        handler3 = privacy_protection_metrics(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler3.runL(List_quasi)                          ##传递准标识符集合，以及准标识符对应的数量

##匿名集数据特征
class Desensitization_data_character(Privacy_data):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取文件的地址
        '''
        super().__init__(k,l,t,address)

    def Average_degree_annonymity(self,address,List_quasi):
        '''
        :param List_quasi:选取标签对应的所有的准标识符，及其数量
        :return: 返回平均泛化程度，即平均等价类大小
        '''
        address_All = self._Num_address(address)  ##得到address中元素的总数
        length = len(List_quasi)
        return round(address_All / length,4)

    def Dimension_QID(self,address):
        '''
        :param address:选取的标签
        :return: 返回准标识符（QID）维数
        '''
        return len(self.address_dict[address][0])

    def Dimension_SA(self,address):
        '''
        :param address:选取的标签
        :return: 返回敏感属性（SA）维数
        '''
        return len(self.address_dict[address][1])

    def Attribute_SA(self,address):
        '''
        :param address: 选取的文件地址
        :return: 返回敏感属性（SA）
        '''
        return self.address_dict[address][1]

    def Inherent_Privacy(self,address,List_quasi):
        '''
        :param address: 选取的文件地址
        :param List_quasi: 数据集的所有准标识符，及其对应等价组大小
        :return:返回与数据集概率分布的不确定性相同的均匀分布区间长度
        '''
        Num1 = 0.0
        length =  self._Num_address(address)
        for each in List_quasi:
            Num1 += (each/length) * math.log2(length/each)
        return int(2 ** Num1)

    def Send_Result_Desensitization_data_character(self,_list):
        '''
        :param _list: 得到的一些评估数值
        :return: 将评估数值写入到neo4j中
        '''
        graph = self.graph
        length1 = len(_list[2])
        _str = ", ".join([f"value{i}:'{_list[2][i]}'" for i in range(length1)])
        query = '''
        merge(a: exp_second_level_metric{name: '准标识符维数',desc:'准标识符的数量，其中准标识符是指用来划分等价组的各个属性',value:'%s', uuid:'%s'})
        merge(b: exp_second_level_metric {name: '敏感属性维数',desc:'敏感属性的数量',value:'%s', uuid:'%s'})
        merge(c: exp_second_level_metric {name: '敏感属性种类',desc:'敏感属性的具体属性名',%s, uuid:'%s'})
        merge(e: exp_second_level_metric {name: '平均泛化程度',desc:'所有等价组的平均泛化程度',value:'%s', uuid:'%s'})
        merge(f: exp_second_level_metric {name: '固有隐私',desc:'固有隐私为一个数值，其含义为：区间长度为该数值的均匀分布随机变量与数据概率分布有相同的不确定性。因此数值越小，泛化能力越高，安全性越好。',value:'%s', uuid:'%s'})
        with a, b, c,e,f
            match(d: exp_first_level_metric {name: '匿名集数据特征', uuid:'%s'})
            merge(d) - [: include]->(a)  merge(d) - [: include]->(b)  merge(d) - [: include]->(c)  merge(d) - [: include]->(e)  merge(d) - [: include]->(f) 
        ''' % (_list[0],self.uuid, _list[1],self.uuid,  _str,self.uuid, _list[3],self.uuid, _list[4],self.uuid, self.uuid)
        graph.run(query)

    def runL(self,List_quasi):
        address = self.json_address
        Dimen_QID = self.Dimension_QID(address)
        print("准标识符维数为：",Dimen_QID)
        Dimen_SA = self.Dimension_SA(address)
        print("敏感属性维数为：",Dimen_SA)
        Attribute_SA = self.Attribute_SA(address)
        print("敏感属性种类为：",Attribute_SA)
        Average_anonymity = self.Average_degree_annonymity(address, List_quasi)
        print("平均泛化程度为：",Average_anonymity)
        Inherent_Priv = self.Inherent_Privacy(address,List_quasi)
        print("固有隐私为：",Inherent_Priv)
        self.Send_Result_Desensitization_data_character([str(Dimen_QID), str(Dimen_SA), Attribute_SA, str(Average_anonymity), str(Inherent_Priv)])

##匿名数据质量评估
class Desensitization_data_quality_evalution(Privacy_data):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k,l,t,address)

    def Get_Entropy_based_Average_loss(self,address,List_quasi):
        '''
        :param address: 选取的address
        :param List_quasi: 数据集中所有的准标识符
        :return:返回基于熵的数据损失度
        原本的函数或者说论文有问题，其对熵的理解有本质上的错误，故将函数修改。
        针对等价组个数，将现有熵值 / 最大熵，即为数据损失度。越接近1，损失度越大；越接近0，损失度越小。
        '''
        result = self._Function_Data(address)
        address_All = len(result)
        S0 = math.log2(address_All)  ##在匿名程度最大的情况下的匿名熵
        Num_Loss = 0
        for Num_each in List_quasi:
            Num_Loss += (math.log2(Num_each)) * Num_each / address_All
        return Num_Loss / S0

    def Get_Distribution_Leakage(self,address,each_privacy,List_quasi_keys):
        '''
        :param address:选取的文件地址
        :param each_privacy:选取的隐私属性
        :param List_quasi_keys:所有的准标识符
        :return:返回数据集的分布泄露，即分布泄露可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每一个给定的等价类，测量原始数据集和已发布数据集中敏感属性分布之间的泄露。
        用时120秒，即2分钟
        '''
        List_privacy = self._Probabilistic_Distribution_Privacy(address,each_privacy)  ##得到某一个敏感属性的概率分布,0.23秒
        Leakage_List = []
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)                       ##99976条数据
            _TemAll = pd.DataFrame(result)  ##0.06秒
        # 创建线程池
        with ThreadPoolExecutor(50) as t:
            for List_quasi_key in List_quasi_keys:
                Num_distance = t.submit(self._Thread_Distribution_Leakage, address,List_quasi_key, List_privacy, _TemAll,each_privacy)
                Leakage_List.append(Num_distance)
            _max = max([future.result() for future in as_completed(Leakage_List)])
        return _max

    def Get_Entropy_Leakage(self,address,each_privacy,List_quasi_keys):
        '''
        :param address:选取的文件地址
        :param each_privacy:选取的隐私属性
        :param List_quasi_keys:所有的准标识符
        :return:返回数据集的熵泄露，即通过原始分布的初始熵与等价组的熵之间的差异度，来衡量等价类中个体隐私泄露的程度。
        用时108秒
        '''
        List_privacy = self._Probabilistic_Distribution_Privacy(address,each_privacy)  ##得到某一个敏感属性的概率分布,0.23秒
        Hmax = -(sum([i * math.log2(i) for i in List_privacy]))                 ##整体的熵值
        Leakage_List = []
        with open(address, 'r', encoding='UTF-8') as f:
            result = json.load(f)                       ##99976条数据
            _TemAll = pd.DataFrame(result)  ##0.06秒
        # 创建线程池
        with ThreadPoolExecutor(50) as t:
            for List_quasi_key in List_quasi_keys:
                Num_distance = t.submit(self._Thread_Entropy_Leakage, address,List_quasi_key, Hmax, _TemAll,each_privacy)
                Leakage_List.append(Num_distance)
            _max = max([future.result() for future in as_completed(Leakage_List)])
        return _max

    def Get_Positive_Information_Disclosure(self, address, each_privacy, List_quasi):
        '''
        :param address: 选择文件地址
        :param each_privacy: 选择的某一个隐私属性
        :param List_quasi: 整体数据的准标识符，及其对应等价组大小
        :return: 返回选定隐私属性的正面信息披露的最大值。正面信息披露是指准标识数据对隐私数据的披露程度
        用时25秒
        '''
        result = self._Function_Data(address)
        Positive_Information_List = []
        Num_address = len(result)                    ##所有数据的个数,99978
        _TemAll = pd.DataFrame(result)          ##0.06秒
        List_each_privacy = pd.DataFrame([each[each_privacy] for each in result]).drop_duplicates()  ##得到选定敏感属性的set集，及其个数
        length = len(List_each_privacy)
        # 创建线程池
        with ThreadPoolExecutor(50) as t:
            for i in range(length):
                Positive_Information = t.submit(self._Thread_Positive_Information, self.address_dict[address][0] ,  f"{each_privacy}== '{List_each_privacy.iloc[i,0]}'", Num_address, _TemAll, List_quasi)
                Positive_Information_List.append(Positive_Information)
            _max = max([future.result() for future in as_completed(Positive_Information_List)])
        return _max

    def Send_Result_data_quality_evalution(self,_list):
        '''
        :param _list: 得到的一些评估数值
        :return: 将评估数值写入到neo4j中
        '''
        graph = self.graph
        AllP = self.address_dict[self.json_address][1]  ##所有的L多样性结果
        length1 = len(AllP)
        _str1 = ", ".join([f"value_{AllP[i]}:'{_list[1][i]}'" for i in range(length1)])
        _str2 = ", ".join([f"value_{AllP[i]}:'{_list[2][i]}'" for i in range(length1)])
        _str3 = ", ".join([f"value_{AllP[i]}:'{_list[3][i]}'" for i in range(length1)])
        query = '''
        merge(a3: exp_second_level_metric{name: '基于熵的数据损失度',desc:'基于最大熵与各个等价组的熵来度量数据的损失度',value:'%s', uuid:'%s'})
        merge(b3: exp_second_level_metric{name: '分布泄露',desc:'分布泄露可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每一个给定的等价类，测量原始数据集和已发布数据集中敏感属性分布之间的泄露。',%s, uuid:'%s'})
        merge(c3: exp_second_level_metric{name: '熵泄露',desc:'通过原始分布的初始熵与等价组的熵之间的差异度，来衡量等价类中个体隐私泄露的程度。',%s, uuid:'%s'})
        merge(f3: exp_second_level_metric{name: '正面信息披露',desc:'通过度量准标识数据对隐私数据的影响，来衡量隐私数据泄露的程度',%s, uuid:'%s'})
        
        with a3,b3,c3,f3
            match(d3: exp_first_level_metric {name: '数据质量评估', uuid:'%s'}) 
            merge(d3) - [: include]->(a3) merge(d3) - [: include]->(b3)   merge(d3) - [: include]->(c3)   merge(d3) - [: include]->(f3) 
        ''' % (_list[0],self.uuid, _str1,self.uuid,_str2,self.uuid, _str3,self.uuid, self.uuid)
        graph.run(query)

    def runL(self,List_quasi):
        address = self.json_address
        Entropy_based_Average_Loss = self.Get_Entropy_based_Average_loss(address,List_quasi)  ##0.23秒
        print(f"基于熵的平均数据损失度为：{round(Entropy_based_Average_Loss * 100,2)}%")
        listD = []; listE = []; listF = []
        for each_privacy in self.address_dict[address][1]:          ##遍历所有的敏感属性
            _max = self.Get_Distribution_Leakage(address, each_privacy, List_quasi.keys())
            print(f"数据集针对{each_privacy}的分布泄露为：{round(_max,4)}")
            listD.append(round(_max,4))
        for each_privacy in self.address_dict[address][1]:          ##遍历所有的敏感属性
            _max = self.Get_Entropy_Leakage(address, each_privacy, List_quasi.keys())
            print(f"数据集针对{each_privacy}的熵泄露为：{round(_max,4)}")
            listE.append(round(_max,4))
        for each_privacy in self.address_dict[address][1]:          ##遍历所有的敏感属性
            _max = self.Get_Positive_Information_Disclosure(address, each_privacy, List_quasi)
            print(f"数据集针对{each_privacy}的正面信息披露为：{_max}")
            listF.append(_max)

        self.Send_Result_data_quality_evalution([f"{round(Entropy_based_Average_Loss * 100,2)}%",listD,listE,listF])


##隐私保护性度量
class privacy_protection_metrics(Privacy_data):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k,l,t,address)

    def Get_Entropy_based_Re_indentification_Risk(self,address,List_quasi_keys):
        '''
        :param address:选择的文件地址
        :param List_quasi_keys:数据集中所有的准标识符
        :return:整体的基于熵的重识别风险
        原本138秒，修改后，引入并行计算，现在用时134秒或者更长,时间没啥变化，
        但我依旧愿意用并发来执行以应对之后数据更大的情况
        '''
        result = self._Function_Data(address)
        _TemAll = pd.DataFrame(result)  ##0.06秒
        privacy_All = len(pd.DataFrame([[each[each_attr] for each_attr in self.address_dict[address][1]] for each in result]).drop_duplicates())  ##得到所有敏感属性的set集个数
        Hmax = math.log2(privacy_All)
        Risk_List = []
        # 创建线程池
        with ThreadPoolExecutor(50) as t:
            for List_quasi_key in List_quasi_keys:
                Num_distance = t.submit(self._Thread_Probabilistic_Distribution_Combine_PrivacyAll, address,List_quasi_key, Hmax, _TemAll)
                Risk_List.append(Num_distance)
            _max = max([future.result() for future in as_completed(Risk_List)])
        return _max

    def Get_Entropy_based_Re_indentification_Risk_QID(self,address,each_QID):
        '''
        :param address:选择的标签
        :param each_QID:选择的准标识符中的某一属性值
        :return:返回基于某一（准标识符中的）属性的风险度量
        原本797秒，174,25,42；现在修改后，447秒/157秒/14秒/23秒
        '''
        result = self._Function_Data(address)
        _TemAll = pd.DataFrame(result)  ##0.06秒
        privacy_All  = len(pd.DataFrame([[each[each_attr] for each_attr in self.address_dict[address][1]] for each in result]).drop_duplicates())  ##得到所有敏感属性的set集个数
        Hmax = math.log2(privacy_All)
        List_Attr = pd.DataFrame([each[each_QID] for each in result]).drop_duplicates()  ##得到选定属性（多是准标识符）的set集
        Risk_List = []
        # 创建线程池
        with ThreadPoolExecutor(4) as t:
            for each_Attr in List_Attr.loc[:,0]:
                ##把下载任务提交给线程池
                Num_distance = t.submit(self._Thread_Probabilistic_Distribution_Combine_Privacy, address, each_Attr, each_QID, Hmax, _TemAll)
                Risk_List.append(Num_distance)
            _max = max([future.result() for future in as_completed(Risk_List)])
        return _max

    def Get_Entropy_based_Re_indentification_Risk_with(self,address,List_quasi_keys,each_privacy):
        '''
        :param address: 选取的address
        :param List_quasi_keys:数据集中所有的准标识符
        :param each_privacy: 选取的敏感属性
        :return:返回某一个敏感属性的基于熵的重识别风险
        未修改前，用时2948秒，即49分钟；现在，149秒/132秒/132秒
        '''
        result = self._Function_Data(address)
        _TemAll = pd.DataFrame(result)  ##0.06秒
        privacy_All = len(pd.DataFrame([each[each_privacy] for each in result]).drop_duplicates())   ##整个数据中某个隐私属性的set集个数，0.4秒
        Hmax = math.log2(privacy_All)
        length = len(List_quasi_keys)
        Risk_List = []
        #创建线程池
        with ThreadPoolExecutor(80) as t:
            for i in range(length):
                ##把下载任务提交给线程池
                Num_distance = t.submit(self._Thread_Probabilistic_Distribution_privacy, address, List_quasi_keys[i],each_privacy,Hmax,_TemAll)
                Risk_List.append(Num_distance)
            _max = max([future.result() for future in as_completed(Risk_List)])
        return _max  ##返回最大值，以此来度量重识别风险

    def Send_Result_privacy_protection_metrics(self,_list):
        '''
        :param _list: 得到的一些评估数值
        :return: 将评估数值写入到neo4j中
        '''
        graph = self.graph
        AllP = self.address_dict[self.json_address][1]                               ##所有的隐私属性名称
        length1 = len(AllP)
        _str1 = ", ".join([f"value_{AllP[i]}:'{_list[0][i]}'" for i in range(length1)])
        AllQ = self.address_dict[self.json_address][0]                              ##所有的准标识符
        length2 = len(AllQ)
        _str2 = ", ".join([f"value_{AllQ[i]}:'{_list[1][i]}'" for i in range(length2)])

        query = '''
        merge(a4: exp_second_level_metric{name: '针对单个敏感属性的基于熵的重识别风险',desc:'基于熵，计算针对某一个敏感属性的的重识别风险',%s, uuid:'%s'})
        merge(b4: exp_second_level_metric {name: '针对单个属性的基于熵的重识别风险',desc:'基于熵，计算针对某一个准标识符中的属性的重识别风险',%s, uuid:'%s'})
        merge(c4: exp_second_level_metric {name: '基于熵的重识别风险',desc:'基于熵，通过最大熵和相对熵得到整体数据的重识别风险',value:'%s', uuid:'%s'})
        with a4, b4, c4
            match(d4: exp_first_level_metric {name: '隐私保护性度量', uuid:'%s'}) 
            merge(d4) - [: include]->(a4)  merge(d4) - [: include]->(b4)  merge(d4) - [: include]->(c4)  
        ''' % (_str1,self.uuid, _str2,self.uuid,  _list[-1],self.uuid,  self.uuid)
        graph.run(query)

    def runL(self,List_quasi):
        address = self.json_address
        listP = []; listQ = []   ##一行同时初始化多个参数，要用分号而不要用逗号
        for each_privacy in self.address_dict[address][1]:
            Entropy_Re_Risk_with = self.Get_Entropy_based_Re_indentification_Risk_with(address,List_quasi.keys(),each_privacy)
            print(f"针对敏感属性{each_privacy}基于熵的重识别风险为：{Entropy_Re_Risk_with * 100}%")  ##149秒/132秒/132秒
            listP.append(f'{round(Entropy_Re_Risk_with * 100,2)}%')
        for each_QID in self.address_dict[address][0]:
            Entropy_Re_Risk_QID = self.Get_Entropy_based_Re_indentification_Risk_QID( address,each_QID)  ##447秒/157秒/14秒/23秒
            print(f"针对属性{each_QID}基于熵的重识别风险为：{round(Entropy_Re_Risk_QID * 100 ,2)}%")
            listQ.append(f'{round(Entropy_Re_Risk_QID * 100,2)}%')
        Entropy_Re_Risk = self.Get_Entropy_based_Re_indentification_Risk(address,List_quasi.keys())  ##130-200秒
        print(f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%")
        self.Send_Result_privacy_protection_metrics([listP,listQ,f'{round(Entropy_Re_Risk * 100,2)}%'])

##隐私策略：主要包括合规性和数据可用性
class Privacy_policy(Config):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

    # def _NI_Range(self,address,NI_attr,attrs = {}):
    #     '''
    #     :param address:选取的address
    #     :param NI_attr:准标识符中的某一个特定的属性值
    #     :param attrs: 空或者字典类型的准标识符
    #     :return:若attrs为空，返回整个属性值的取值范围；反之，返回对应的等价组的属性值的数据损失：等价组包含的记录数 * 取值范围
    #     '''
    #     matcher = NodeMatcher(self.graph)
    #     if attrs == {}:
    #         NI_list = [each[NI_attr] for each in list(matcher.match(address))]  ##得到标签中所有指定属性值NI_attr的取值
    #         NI_list2 = [int(each2.replace("*", "")) for each2 in NI_list]  ##这里需要根据泛化结果，更改Loss的评价标准
    #         _NI_Range = (max(NI_list2) - min(NI_list2) + 1) * (10 ** NI_list[0].count("*"))  ##这里需要根据泛化结果，更改Loss的评价标准
    #     else:
    #         n = " and ".join([f'_.{key}="{value}"' for key, value in attrs.items()])  ##查询条件
    #         List_Q = list(matcher.match(address).where(n))
    #         # NI_list = [each[NI_attr] for each in list(matcher.match(address).where(n))]  ##得到标签中所有指定属性值NI_attr的列表
    #         _NI_Range = 1 * (10 ** List_Q[0][NI_attr].count("*") - 1) * len(List_Q)  ##这里需要根据泛化结果，更改Loss的评价标准
    #     return _NI_Range

    def _Thread_Probabilistic_Distribution_Quasi(self,address,List_quasi_key,each_privacy,Attribute_dict,_TemAll):
        '''
        :param address: 选取的文件地址
        :param List_quasi_key: 选取的准标识符，作为查询条件得到对应等价组
        :param each_privacy:选取的敏感属性
        :param Attribute_dict:通过_Probabilistic_Distribution函数得到的整个数据中敏感属性的概率分布
        :param _TemAll:选取的文件地址中的所有数据（DatFrame格式）
        :return:返回满足条件的等价组中某一隐私属性的概率分布与总的概率分布之间的KL散度
        '''
        Num_distance = 0
        length = len(List_quasi_key)
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件
        Attribute_dict2 = pd.value_counts(_QueryCondition.loc[:,each_privacy], normalize=True)
        for each_Attribute in Attribute_dict2.keys():
            Num_distance += (Attribute_dict2[each_Attribute] * math.log2( Attribute_dict2[each_Attribute] / Attribute_dict[each_Attribute]))
        return Num_distance

    def _Thread_Probabilistic_Distribution_Privacy(self,address,List_quasi_key,each_privacy,_TemAll):
        length = len(List_quasi_key)                    ##准标识符维数
        query = " and ".join([f"{self.address_dict[address][0][i] }== '{List_quasi_key[i]}'"  for i in range(length)])
        _QueryCondition = _TemAll.query(query)  ##query为查询条件，找出所有等价组
        Attribute_dict2 = pd.value_counts(_QueryCondition.loc[:,each_privacy], normalize=True)  ##得出等价组的概率分布
        return Attribute_dict2[0]  ##只返回最大的那个

    def _Thread_NI_Loss(self,List_quasi_key, List_quasi_num, Age_range):
        ageNum = List_quasi_key[1].split("-")
        NI_Loss = (0 + (int(ageNum[1]) - int(ageNum[0])) / Age_range + List_quasi_key[2].count("*") / len( List_quasi_key[2]) + List_quasi_key[3].count("*") / len(List_quasi_key[3])) * List_quasi_num  ##小数点后保留4位小数
        return NI_Loss

    def run(self, List_quasi):   ##运行程序，主要运行合规性和数据可用性
        address = self.json_address
        handler1 = Data_compliance(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler1.runL(List_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        handler2 = Data_availability(self.K_Anonymity,self.L_diversity,self.T_closeness,address)
        handler2.runL(List_quasi)                          ##传递准标识符集合，以及准标识符对应的数量


##数据合规性
class Data_compliance(Privacy_policy):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

    def IsKAnonymity(self,List_quasi):
        '''
        :param List_quasi:文件中所有的准标识符，及其数量
        :return: 返回数据集是否符合K匿名，即用准标识符划分出的等价组其包含的记录个数是否均大于K。若满足K匿名，返回True；反之，返回False
        '''
        if List_quasi[-1] < self.K_Anonymity:   ##由于List_quasi默认排序为降序，所以选择最小的等价组与K比较
            return False
        return True

    def IsLDiversity(self,address,each_privacy):
        '''
        :param address：选取的文件地址
        :param each_privacy:选取的隐私属性
        :return: 返回输入数据集是否符合L多样性，即每一个等价组中是否包含L个以上不同的隐私属性。若满足L多样性，返回True；反之，返回False
        '''
        if self.L_diversity == 1: ##若L多样性值为1，必定符合
            return True
        result = self._Function_Data(address)
        List_Name =  self.address_dict[address][0] + [each_privacy]                                        ##选取标签对应的准标识符以及选择评估的隐私属性
        List_quasi_privacy = pd.DataFrame([[each[each_attr] for each_attr in List_Name] for each in result]).drop_duplicates()                ##0.3秒
        List_LDiversity = pd.value_counts(List_quasi_privacy.iloc[:,0:-1].values.tolist())  ##18秒
        if List_LDiversity[-1] < self.L_diversity:
            return  False
        return True

    def IsTCloseness(self,address,List_quasi_keys,each_privacy):
        '''
        :param address: 选取的文件地址
        :param List_quasi_keys: 文件中所有的准标识符
        :param each_privacy:选取的隐私属性
        :return: 返回输入等价组List_quasi是否符合T紧密性，即每一个等价组中的隐私属性分布与整体分布之间的距离是否小于T_Closeness。若满足，返回True；反之，返回False
        显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，EMD过于麻烦，以后有时间可以实现一下；这里采用的是KLD
        原本2950.89秒(或者2710秒)，即49分钟；现在优化后，120秒
        '''
        result = self._Function_Data(address)
        _TemAll = pd.DataFrame(result)  ##0.06秒
        Attribute_dict = pd.value_counts([each[each_privacy] for each in result], normalize=True) ##返回整个数据中敏感属性的概率分布
        length = len(List_quasi_keys)
        List_distance = []
        #创建线程池
        with ThreadPoolExecutor(80) as t:   ##80或者100
            for i in range(length):
                ##把下载任务提交给线程池
                Num_distance = t.submit(self._Thread_Probabilistic_Distribution_Quasi, address, List_quasi_keys[i],each_privacy,Attribute_dict,_TemAll)
                List_distance.append(Num_distance)
            for future in as_completed(List_distance):
                if future.result() > self.T_closeness:
                    return  False
        return True

    def IsAKAnonymity(self, address, List_quasi_keys, each_privacy):
        '''
        :param address:数据集的地址
        :param List_quasi_keys:数据集的所有准标识符
        :param each_privacy:选定的隐私属性
        :return: (α,k)-Anonymity确保在所有等价类中，没有任何一个敏感属性可以占主导地位。这里内定α=0.5。每一个等价类中不能有任何一个敏感属性占比超过0.5。
        耗时120秒
        '''
        result = self._Function_Data(address)
        _TemAll = pd.DataFrame(result)  ##0.06秒
        length = len(List_quasi_keys)
        List_Num = []
        #创建线程池
        with ThreadPoolExecutor(80) as t:
            for i in range(length):
                ##把下载任务提交给线程池
                Num_distance = t.submit(self._Thread_Probabilistic_Distribution_Privacy, address, List_quasi_keys[i],each_privacy, _TemAll)
                List_Num.append(Num_distance)
            for future in as_completed(List_Num):
                if future.result() > 0.5:
                    return  False
        return True

    def Send_Result_Data_compliance(self,_list):
        '''
        :param _list: 得到的一些评估数值
        :return: 将评估数值写入到neo4j中
        '''
        graph = self.graph
        if len(_list) == 1 :
            query = '''
            merge(a1: exp_second_level_metric{name: 'K-Anonymity',desc:'数据集是否符合K匿名，即用准标识符划分出的等价组其包含的记录个数是否均大于K',value:'%s', uuid:'%s'})
            merge(b1: exp_second_level_metric {name: 'L-Diversity',desc:'数据集是否符合L多样性，即每一个等价组中是否包含L个以上不同的隐私属性（多样性有很多种定义，这里选取最简单明了的一个）',value:'不满足', uuid:'%s'})
            merge(c1: exp_second_level_metric {name: 'T-Closeness',desc:'数据集是否符合T紧密性，即每一个等价组中的隐私属性分布与整体分布之间的距离是否小于给定的数值T（显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，这里采用的是KLD）', value:'不满足', uuid:'%s'})
            merge(f1: exp_second_level_metric {name: '(α,k)-Anonymity',desc:'(α,k)-Anonymity确保在所有等价类中，没有任何一个敏感属性可以占主导地位。这里内定α=0.5。每一个等价类中不能有任何一个敏感属性占比超过0.5。', value:'不满足', uuid:'%s'})
            with a1, b1, c1,f1
                match(d1: exp_first_level_metric {name: '合规性', uuid:'%s'}) 
                merge(d1) - [: include]->(a1)  merge(d1) - [: include]->(b1)  merge(d1) - [: include]->(c1)  merge(d1) - [: include]->(f1)  
            ''' % (_list[0],self.uuid,self.uuid,self.uuid, self.uuid, self.uuid)
        else:
            AllP = self.address_dict[self.json_address][1]                               ##所有的L多样性结果
            length1 = len(AllP)
            _str1 = ", ".join([f"value_{AllP[i]}:'{_list[1][i]}'" for i in range(length1)])
            _str2 = ", ".join([f"value_{AllP[i]}:'{_list[2][i]}'" for i in range(length1)])
            _str3 = ", ".join([f"value_{AllP[i]}:'{_list[3][i]}'" for i in range(length1)])
            query = '''
            merge(a1: exp_second_level_metric{name: 'K-Anonymity',desc:'数据集是否符合K匿名，即用准标识符划分出的等价组其包含的记录个数是否均大于K',value:'%s', uuid:'%s'})
            merge(b1: exp_second_level_metric {name: 'L-Diversity',desc:'数据集是否符合L多样性，即每一个等价组中是否包含L个以上不同的隐私属性（多样性有很多种定义，这里选取最简单明了的一个）',%s, uuid:'%s'})
            merge(c1: exp_second_level_metric {name: 'T-Closeness',desc:'数据集是否符合T紧密性，即每一个等价组中的隐私属性分布与整体分布之间的距离是否小于给定的数值T（显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，这里采用的是KLD）',%s, uuid:'%s'})
            merge(f1: exp_second_level_metric {name: '(α,k)-Anonymity',desc:'(α,k)-Anonymity确保在所有等价类中，没有任何一个敏感属性可以占主导地位。这里内定α=0.5。每一个等价类中不能有任何一个敏感属性占比超过0.5。',%s, uuid:'%s'})
            with a1, b1, c1,f1
                match(d1: exp_first_level_metric {name: '合规性', uuid:'%s'}) 
                merge(d1) - [: include]->(a1)  merge(d1) - [: include]->(b1)  merge(d1) - [: include]->(c1)   merge(d1) - [: include]->(f1)  
            ''' % (_list[0],self.uuid,_str1,self.uuid,_str2,self.uuid,_str3,self.uuid, self.uuid)
        graph.run(query)

    def runL(self, List_quasi):
        address = self.json_address
        BoolK = self.IsKAnonymity(List_quasi)  ##若为True，满足K匿名；反之，不满足
        _list = []
        if BoolK:                                                        ##若不满足K匿名，则不需要考虑L多样性
            print(f"满足{self.K_Anonymity}-匿名")
            _list.append(f"满足{self.K_Anonymity}-匿名")
            listL = []; listT = []; listAK = []
            for each_privacy in self.address_dict[address][1]:
                BoolL = self.IsLDiversity(address, each_privacy)                  ##34秒
                if BoolL:
                    print(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
                    listL.append(f"隐私属性{each_privacy}满足{self.L_diversity}-多样性")
                else:
                    print(f"隐私属性{each_privacy}不满足{self.L_diversity}-多样性")
                    listL.append(f"隐私属性{each_privacy}不满足{self.L_diversity}-多样性")

                BoolT = self.IsTCloseness(address, List_quasi.keys(), each_privacy)  ##180秒
                if BoolT:
                    print(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
                    listT.append(f"隐私属性{each_privacy}满足{self.T_closeness}-紧密性")
                else:
                    print(f"隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")
                    listT.append(f"隐私属性{each_privacy}不满足{self.T_closeness}-紧密性")

                BoolAK = self.IsAKAnonymity(address, List_quasi.keys(), each_privacy)  ##180秒
                if BoolAK:
                    print(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                    listAK.append(f"隐私属性{each_privacy}满足(0.5,{self.K_Anonymity})-Anonymity")
                else:
                    print(f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity")
                    listAK.append(f"隐私属性{each_privacy}不满足(0.5,{self.K_Anonymity})-Anonymity")
            _list.append(listL)
            _list.append(listT)
            _list.append(listAK)
        else:
            print(f"不满足{self.K_Anonymity}-匿名")
            _list.append(f"不满足{self.K_Anonymity}-匿名")
        self.Send_Result_Data_compliance(_list)

##数据可用性
class Data_availability(Privacy_policy):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

    def Get_CDM(self,address,List_quasi):
        '''
        :param address: 选取文件地址
        :param List_quasi: 所有准标识符及其数量
        :return: 返回数据可辨别度CDM， 就是将所有的等价组个数K的平方累加
        '''
        address_All = self._Num_address(address)  ##得到address中元素的总数
        AccumulationN = 0
        for each in List_quasi:
            AccumulationN += each ** 2
        Num = round(AccumulationN/(address_All ** 2),6)
        return 1 - Num

    def Get_CAVG(self,address,List_quasi):
        '''
        :param address: 选取的文件地址
        :param List_quasi: 所有准标识符及其数量
        :return: 返回归一化的平均等价组大小度量（就是用之前的平均泛化程度/K）
        '''
        address_All = self._Num_address(address)  ##得到address中元素的总数
        length = len(List_quasi)                                    ##得到所有等价组个数
        return round(address_All / (length * self.K_Anonymity), 4)

    def Get_SupRatio(self,address):
        '''
        :param graph: 选取的图谱
        :param address: 选取的标签
        :return: 返回数据记录隐匿率，即隐匿的记录数目 / 原数据集的总个数，由于记录隐匿元组和整体数量相比少的多的多，所以直接除，没有声明保留小数点几位
        '''
        return self.n_s / (self._Num_address(address) + self.n_s)

    def Get_NCP(self,address,List_quasi):
        '''
        :param address: 选取的address
        :param List_quasi: 数据集中所有的准标识符
        :return:数据损失度评价   归一化确定性惩罚
        即便是同一种数据，泛化结果也多有不同；因此这个函数还仅仅是初版
        '''
        ILoss = 0
        length = len(self.address_dict[address][0])  ##准标识符属性个数
        Age_range = 100         ##年龄整体的取值范围
        List_loss = []

        #创建线程池
        with ThreadPoolExecutor(80) as t:   ##80或者100
            for i in range(len(List_quasi)):
                NI_loss = t.submit(self._Thread_NI_Loss, List_quasi.keys()[i],  List_quasi[i],Age_range)
                List_loss.append(NI_loss)
            for future in as_completed(List_loss):
                ILoss += future.result()
        return round(ILoss / length, 4)

    def Send_Result_Data_availability(self,_list):
        '''
        :param _list: 得到的一些评估数值
        :return: 将评估数值写入到neo4j中
        '''
        graph = self.graph
        query = '''
        merge(a2: exp_second_level_metric{name: '数据损失度',desc:'数据损失度评价',value:'%s', uuid:'%s'})
        merge(b2: exp_second_level_metric {name: '数据可辨别度量',desc:'所谓数据可辨别度就是将所有的等价组个数K的平方累加',value:'%s', uuid:'%s'})
        merge(c2: exp_second_level_metric {name: '数据记录匿名率',desc:'返回数据记录隐匿率，即隐匿的记录数目 / 原数据集的总个数',value:'%s', uuid:'%s'})
        merge(e2: exp_second_level_metric {name: '归一化平均等价组大小度量',desc:'所谓归一化平均等价组大小度量就是用平均泛化程度 / K',value:'%s', uuid:'%s'})
        with a2, b2, c2,e2
            match(d2: exp_first_level_metric {name: '数据可用性', uuid:'%s'}) 
            merge(d2) - [: include]->(a2)  merge(d2) - [: include]->(b2)  merge(d2) - [: include]->(c2)  merge(d2) - [: include]->(e2)  
        ''' % (_list[0],self.uuid, _list[1],self.uuid, _list[2],self.uuid, _list[3],self.uuid, self.uuid)
        graph.run(query)


    def runL(self, List_quasi):
        address = self.json_address
        CDM = self.Get_CDM(address, List_quasi)                     ##0.25秒
        print(f"数据可辨别度为：{round(CDM*100,4)}%")  ##数值越小，越接近0越不可辨别；数值越大，越接近1越可以辨别
        SupRatio = self.Get_SupRatio(address)                           ##0.25秒
        print(f"数据记录匿名率为：{SupRatio*100}%")
        CAVG = self.Get_CAVG(address,List_quasi)
        print(f"归一化平均等价组大小度量：{CAVG}")
        NCP = self.Get_NCP(address, List_quasi)
        print(f"数据损失度为：",NCP)
        self.Send_Result_Data_availability([f"{NCP}", f"{round(CDM*100,4)}%", f"{SupRatio*100}%", f"{CAVG}"])


class Action(Config):  ##行为动作
    def __init__(self, k, l,t):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        '''
        super().__init__(k,l,t)

    def Run(self):
        pass


if __name__ == '__main__':
    Config(2,2,8,"./data/hotel_mid3.json").Run()
    print("********************************************************8")

