from py2neo import Graph, Node, Relationship, RelationshipMatcher, NodeMatcher
import math
import pandas as pd
import os
import json
from concurrent.futures import as_completed, wait, ALL_COMPLETED, FIRST_EXCEPTION
from concurrent.futures.thread import ThreadPoolExecutor
import queue
import numpy as np
import time
import uuid


global_uuid = str(uuid.uuid4())  ##定义全局变量uuid数据
print("uuid为：", global_uuid)

class Config:
    def __init__(self,k,l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        # self.graph = Graph("http://localhost:7474/browser/", auth=("neo4j", "123456"))
        # self.graph = Graph("http://192.168.1.121:7474/browser/", auth=("neo4j", "123456"))

        self.json_address = address  ##待评价的数据文件地址
        self.address_Attr = [["sex","age","id_number","phone"] , ["day_start","day_end","hotel_name"]]##数据文件的属性（主要包含：准标识符属性和敏感属性）

        self.K_Anonymity = k                                        ##定义好的K-Anonymity数
        self.L_diversity = l                                               ##定义好的L_diversity数
        self.T_closeness = t                                             ##定义好的T紧密度
        self.n_s = 0                                                           ##数据集中被隐匿的记录个数，即原数据集中有却没有在脱敏数据集发布的记录个数，暂时定义为0

    def _Function_Data(self):
        '''
        :return: 返回文件的所有数据
        10万条数据，0.29秒
        '''
        with open(self.json_address, 'r', encoding='UTF-8') as f:
            result = json.load(f)
        return pd.DataFrame(result)

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
        10万条数，0.2秒
        '''
        with open(self.json_address, 'r', encoding='UTF-8') as f:
            result = json.load(f)  ##10万条数据
            return len(result)

    def Run(self):
        Series_quasi = self._Function_Series_quasi()##结果为升序
        print(Series_quasi)
        _TemAll = self._Function_Data()


        # Grouped = _TemAll.groupby("hotel_name", sort=False)
        # i = 0
        # for each in Grouped:
        #     if(i==5):
        #         break
        #     print(each)
        #     Quasi_P = each[1][self.address_Attr[0]].value_counts()
        #     print(Quasi_P)   ##得到所有包含敏感数据值的等价组。
        #     for each_quasi in Quasi_P.keys():
        #         print(Series_quasi[each_quasi],end ="     ")
        #     print("")
        #     i+=1

        # handler1 = Privacy_data(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        # handler1.run(Series_quasi)                          ##传递标签和其对应的准标识符集合，以及准标识符对应的数量
        # handler2 = Privacy_policy(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        # handler2.run(Series_quasi)                          ##传递标签和其对应的准标识符集合，以及准标识符对应的数量
        

        ##数据合规性
        handler1 = Data_compliance(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        handler1.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        ##数据可用性
        handler2 = Data_availability(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        handler2.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        ##匿名集数据特征
        handler3 = Desensitization_data_character(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        handler3.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        ##匿名数据质量评估
        handler4 = Desensitization_data_quality_evalution(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        handler4.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
        ##隐私保护性度量
        handler5 = privacy_protection_metrics(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
        handler5.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量


# #脱敏数据：主要包括匿名集数据特征、数据质量评估和隐私保护性度量
# class Privacy_data(Config):
#     def __init__(self, k, l,t,address):
#         '''
#         :param k: 输入K匿名
#         :param l: 输入l多样性
#         :param t: 输入T紧密度
#         :param address:选取的文件地址
#         '''
#         super().__init__(k,l,t,address)
#
#
#     def run(self, Series_quasi):      ##运行程序，主要运行匿名集数据特征、数据质量评估和隐私保护性度量
#         handler1 = Desensitization_data_character(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
#         handler1.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
#         handler2 = Desensitization_data_quality_evalution(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
#         handler2.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
#         handler3 = privacy_protection_metrics(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
#         handler3.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量


##匿名集数据特征
class Desensitization_data_character(Config):
    def __init__(self, k, l, t, address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取文件的地址
        '''
        super().__init__(k, l, t, address)

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


##匿名数据质量评估
class Desensitization_data_quality_evalution(Config):
    def __init__(self, k, l, t, address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k, l, t, address)

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
        return res



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
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布,默认降序
            Entropy_leakage = float(Hmax)                                                           ##熵泄露，Entropy Leakage
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            for each_value in Distribution_QP:
                Entropy_leakage += each_value * math.log2(each_value)
            if math.fabs(Entropy_leakage) > res:
                res = math.fabs(Entropy_leakage)
        return res


    def Get_Positive_Information_Disclosure(self, each_privacy, Series_quasi):
        '''
        :param each_privacy: 选择的某一个隐私属性
        :param Series_quasi: 整体数据的准标识符，及其对应等价组大小
        :return: 返回选定隐私属性的正面信息披露的最大值。正面信息披露是指准标识数据对隐私数据的披露程度
        10万条数据，4秒
        '''
        _TemAll = self._Function_Data()
        Num_All = self._Num_address()  ##所有数据的个数,10万
        min_value = float("inf")                ##将其初始化为一个无穷大的数
        Attr = ()
        Grouped = _TemAll.groupby(each_privacy, sort=False)  ##将数据按照所选敏感属性分组
        for each in Grouped:                                                                ##选取某个敏感属性值
            Quasi_P = each[1][self.address_Attr[0]].value_counts()  ##得到所有包含这个敏感属性值的等价组
            Positive_Num = 0                                                                ##所有包含这个敏感属性值的等价组大小之和
            for each_quasi in Quasi_P.keys():
                Positive_Num += Series_quasi[each_quasi]
            if Positive_Num < min_value:                                           ##选取最小的和
                min_value = Positive_Num
                Attr = each[0]
        return round((Num_All / min_value) - 1 , 4),Attr


    def runL(self, Series_quasi):
        Entropy_based_Average_Loss = self.Get_Entropy_based_Average_loss(Series_quasi)  ##0.23秒
        print(f"基于熵的平均数据损失度为：{round(Entropy_based_Average_Loss * 100, 2)}%")
        listD = [];listE = [];listF = []
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start = time.time()
            _max = self.Get_Distribution_Leakage(each_privacy, Series_quasi.keys())
            print(f"数据集针对{each_privacy}的分布泄露为：{round(_max, 4)}")
            listD.append(round(_max, 4))
            # print(f"耗时为：{time.time() - start}")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start2 = time.time()
            _max = self.Get_Entropy_Leakage(each_privacy, Series_quasi.keys())
            print(f"数据集针对{each_privacy}的熵泄露为：{round(_max, 4)}")
            listE.append(round(_max, 4))
            # print(f"耗时为：{time.time() - start2}")
        for each_privacy in self.address_Attr[1]:  ##遍历所有的敏感属性
            # start3 = time.time()
            _max,Attr = self.Get_Positive_Information_Disclosure(each_privacy, Series_quasi)
            print(f"数据集针对{each_privacy}的正面信息披露为：{_max},对应属性值为{Attr}")
            listF.append(_max)
            # print(f"耗时为：{time.time()-start3}")



##隐私保护性度量
class privacy_protection_metrics(Config):
    def __init__(self, k, l, t, address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取的文件地址
        '''
        super().__init__(k, l, t, address)

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
        listP = [];
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
            listQ.append(f'{round(Entropy_Re_Risk_QID * 100, 2)}%')
            # print(f"耗时为：{time.time() - start2}")
        # start3 = time.time()
        Entropy_Re_Risk,QList = self.Get_Entropy_based_Re_indentification_Risk( Series_quasi.keys())
        print(f"整个数据集基于熵的重识别风险为：{Entropy_Re_Risk * 100}%，对应等价组为{QList}")
        # print(f"耗时为：{time.time() - start3}")



# ##隐私策略：主要包括合规性和数据可用性
# class Privacy_policy(Config):
#     def __init__(self, k, l,t,address):
#         '''
#         :param k: 输入K匿名
#         :param l: 输入l多样性
#         :param t: 输入T紧密度
#         :param address:选取的文件地址
#         '''
#         super().__init__(k,l,t,address)
#
#     def run(self, Series_quasi):   ##运行程序，主要运行合规性和数据可用性
#         handler1 = Data_compliance(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
#         handler1.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量
#         handler2 = Data_availability(self.K_Anonymity,self.L_diversity,self.T_closeness,self.json_address)
#         handler2.runL(Series_quasi)                          ##传递准标识符集合，以及准标识符对应的数量


##数据合规性
class Data_compliance(Config):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

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
        显而易见，t_closeness的一个关键在于如何定义两个分布之间的距离，EMD过于麻烦，以后有时间可以实现一下；这里采用的是KLD
        10万条数据，用时20秒
        '''
        Distribution_Privacy = self._Probabilistic_Distribution_Privacy(each_privacy)##返回整个数据中某一个敏感属性的概率分布
        length = len(Series_quasi_keys)
        _TemAll = self._Function_Data()
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)##将数据分组
        for i in range(length):
            ##得到某个等价组的某个敏感属性的概率分布
            Distribution_QP = Grouped.get_group(Series_quasi_keys[i])[each_privacy].value_counts(normalize=True)
            Num_distance = 0
            for each_Attribute in Distribution_QP.keys():
                Num_distance += (Distribution_QP[each_Attribute] * math.log2( Distribution_QP[each_Attribute] / Distribution_Privacy[each_Attribute]))
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


        _list.append(listL)
        _list.append(listT)
        _list.append(listAK)


##数据可用性
class Data_availability(Config):
    def __init__(self, k, l,t,address):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address:选取的文件地址
        '''
        super().__init__(k,l,t,address)

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


    def runL(self, Series_quasi):
        CDM = self.Get_CDM(Series_quasi)
        print(f"数据可辨别度为：{CDM}")
        SupRatio = self.Get_SupRatio()
        print(f"数据记录匿名率为：{SupRatio*100}%")
        CAVG = self.Get_CAVG(Series_quasi)
        print(f"归一化平均等价组大小度量：{CAVG}")
        # start = time.time()
        NCP = self.Get_NCP(Series_quasi)
        print(f"数据损失度为：",NCP)
        # print(f"耗时为{time.time()-start}")



if __name__ == '__main__':
    Config(2,2,8,"./data/json/3/hotel_1_3_1.json").Run()
    print("********************************************************8")


##原本以为value_counts函数是有极限的，没想到是自己用法错误，人家函数很好，是自己废物了
# ************************************************************凡是过往，皆为序章************************************************************
# for row in Series_quasi.iterrows():
#     print(row[1][1])
# print(len(_QueryCondition))


# Tem = _TemAll.groupby(self.address_Attr[0],sort=False)
# print(Tem.size())
# _Tem2 = Tem.size()
#
# for i in range(10):
#     print(_Tem2.keys()[i], "                  ",_Tem2[i])
#
# print("****************************************************")
# print(_Tem2[('男', '0-50', '81****************', '134********')])
# print(len(_Tem2))
# print(_Tem2.agg('max'))
# # print(_Tem2.sort_values())
# print("****************************************************")
# _Tem3 = _Tem2.sort_values()
# for i in range(10):
#     print(_Tem3.keys()[i], "                  ",_Tem3[i])
# print(_Tem3.keys()[-1], "                  ",_Tem3[-1])


# result = self._Function_Data(address)
# _TemAll = pd.DataFrame(result)  ##0.06秒
# Tem = _TemAll.groupby(self.address_Attr[0],sort=False).size().sort_values()
# print(Tem)
# print(type(Tem))


# i = 0
# for each in Grouped:
#     if(i==5):
#         break
#     print(each[0],"等价组大小为：",Series_quasi[each[0]])
#     print(each[1]["day_start"].value_counts(normalize=True))
#     i+=1

# Privacy_drop = _TemAll["hotel_name"].drop_duplicates()
# print(Privacy_drop)
# for i in range(0,8):
#     print(Grouped.get_group(Privacy_drop[i]))
#     print("*********************************************************************************************")