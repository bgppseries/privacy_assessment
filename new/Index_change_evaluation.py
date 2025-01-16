import re
import json
import os
import copy

raw_data={
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-1": {
        "error_message": "",
        "success": True,
        "统计推断还原率": 0.0,
        "重识别率": 0.05
    },
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-2": {
        ## 数据合规性
        "(α,k)-Anonymity": [
            "隐私属性hotel_name满足(0.5,2)-Anonymity"
        ],
        "K-Anonymity": "满足2-匿名",
        "L-Diversity": [
            "隐私属性hotel_name满足2-多样性"
        ],
        "T-Closeness": [
            "隐私属性hotel_name满足0.95-紧密性"
        ],
        "error_message": "",
        "success": True
    },
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-3": {
        # 可用性评估
        "error_message": "",
        "success": True,
        "基于熵的平均数据损失度": "48.43%",
        "归一化平均等价组大小度量": 94.697,
        "数据可辨别度": 31689984,
        "数据记录匿名率": "0.0%"
    },
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-4": {
        # 数据特征
        "error_message": "",
        "success": True,
        "准标识符维数": 4,
        "固有隐私": 378,
        "平均泛化程度": 189.3939,
        "敏感属性种类": [
            "hotel_name"
        ],
        "敏感属性维数": 1
    },
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-5": {
        # 数据质量评估
        "KL_Divergence": [
            "数据集针对hotel_name的KL散度为：3.8502,对应等价组为('女', '50-100', '12****************', '14********')"
        ],
        "error_message": "",
        "success": True,
        "分布泄露": [
            "数据集针对hotel_name的分布泄露为：0.3622,对应等价组为('女', '50-100', '12****************', '14********')"
        ],
        "唯一性": "0.0%",
        "正面信息披露": [
            "数据集针对hotel_name的正面信息披露为：95.8829,对应属性值为斯维登酒店"
        ],
        "熵泄露": [
            "数据集针对hotel_name的熵泄露为：3.8082,对应等价组为('女', '50-100', '12****************', '14********')"
        ]
    },
    "86d222f8-2ec7-421c-93d8-5e78ef54d5d5-6": {
        # 隐私保护度量
        "error_message": "",
        "success": True,
        "单个属性的重识别风险": [
            "针对准标识符属性sex基于熵的重识别风险为：0.19%，对应属性值为男",
            "针对准标识符属性age基于熵的重识别风险为：0.21%，对应属性值为50-100",
            "针对准标识符属性id_number基于熵的重识别风险为：0.66%，对应属性值为46****************",
            "针对准标识符属性phone基于熵的重识别风险为：0.34%，对应属性值为14********"
        ],
        "基于熵的重识别风险": "整个数据集基于熵的重识别风险为：56.66%，对应等价组为('男', '0-50', '65****************', '13********')",
        "敏感属性的重识别风险": [
            "针对敏感属性hotel_name基于熵的重识别风险为：56.66%，对应等价组为('女', '50-100', '12****************', '14********')"
        ]
    }
}

class DataCleaning:
    # 初始化方法，用于创建对象时设置属性
    def __init__(self, raw_data):
        self.raw_data = raw_data # 实例属性

    # 实例方法
    def replace_keys(self,raw_data):
        replaced_data = {}
        for key, value in raw_data.items():
            # 替换 key 的后缀
            if key.endswith("-0"):
                worker_id=key-'-0'
                replaced_key = "评估任务的相关参数"
            elif key.endswith("-1"):
                replaced_key = "风险评估结果"
            elif key.endswith("-2"):
                replaced_key = "合规性评估结果"
            elif key.endswith("-3"):
                replaced_key = "可用性评估结果"
            elif key.endswith("-4"):
                replaced_key = "匿名集数据特征评估结果"
            elif key.endswith("-5"):
                replaced_key = "匿名集数据质量评估结果"
            elif key.endswith("-6"):
                replaced_key = "隐私保护性度量评估结果"
            else:
                replaced_key = key  # 保留其他未匹配的 key

            replaced_data[replaced_key] = value
        return replaced_data

    def data_extraction(self):
        clean_data = self.replace_keys(self.raw_data)
        ### 风险评估结果 不需要处理
        ### 可用性评估结果 不需要处理
        ### 匿名集数据特征评估结果 不需要处理

        ### 合规性评估结果 处理
        self.compliance_assessment(clean_data)

        ### 匿名集数据质量评估结果 处理
        self.data_quality_evaluation(clean_data)

        ### 隐私保护性度量评估结果 处理
        self.privacy_protection_measure(clean_data)
        self.raw_data = clean_data

        # print(f"===== clean data =====\n{self.raw_data}")
        
    def compliance_assessment(self, clean_data):
        if "合规性评估结果" in clean_data:
            item = clean_data["合规性评估结果"]
        else:
            return
        # 提取最后一个数字的通用函数
        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)  # 匹配整数或小数
            return numbers[-1] if numbers else None  # 取最后一个数字
        a_k_Anonymity = {}
        K_me = {}
        l_value = {}
        T_close = {}
        AnonymityandLvalue=tuple(map(float, (re.search(r'\((.*?)\)', item['(α,k)-Anonymity'][0]).group(1)).split(',')))
        a_k_Anonymity['AnonymityandLvalue'] = AnonymityandLvalue
        K_me['k_value'] = extract_last_number(str(item['K-Anonymity']))
        l_value['l_value'] = extract_last_number(str(item['L-Diversity']))
        T_close['T_value'] = extract_last_number(str(item['T-Closeness']))
        ## (α,k)-Anonymity
        for Anonymity_str in item['(α,k)-Anonymity']:
            attribute_match = re.search(r'(?<=隐私属性)(.*)(?=(满足|不满足))', Anonymity_str)
            attribute = attribute_match.group(1) if attribute_match else None
            satisfaction_match = re.search(r'(满足|不满足)', Anonymity_str)
            satisfaction = satisfaction_match.group(1) if satisfaction_match else None
            a_k_Anonymity[attribute] = satisfaction
            ## print(f"===== a_k_Anonymity =====\n{a_k_Anonymity}")
        
        for Anonymity_str in item["L-Diversity"]:
            attribute_match = re.search(r'(?<=隐私属性)(.*)(?=(满足|不满足))', Anonymity_str)
            attribute = attribute_match.group(1) if attribute_match else None
            satisfaction_match = re.search(r'(满足|不满足)', Anonymity_str)
            satisfaction = satisfaction_match.group(1) if satisfaction_match else None
            l_value[attribute] = satisfaction
            ## print(f"===== L-Diversity =====\n{a_k_Anonymity}")
        
        for Anonymity_str in item["T-Closeness"]:
            attribute_match = re.search(r'(?<=隐私属性)(.*)(?=(满足|不满足))', Anonymity_str)
            attribute = attribute_match.group(1) if attribute_match else None
            satisfaction_match = re.search(r'(满足|不满足)', Anonymity_str)
            satisfaction = satisfaction_match.group(1) if satisfaction_match else None
            T_close[attribute] = satisfaction
            ## print(f"===== T-Closeness =====\n{a_k_Anonymity}")
        
        satisfaction_match = re.search(r'(满足|不满足)', item["K-Anonymity"])
        satisfaction = satisfaction_match.group(1) if satisfaction_match else None
        K_me['整体'] = satisfaction

        item['(α,k)-Anonymity'] = a_k_Anonymity
        item["K-Anonymity"] = K_me
        item["L-Diversity"] = l_value
        item["T-Closeness"] = T_close
        # print(f"===== 合规性评估结果 =====\n{item}")

    ### 等价组未加入 todo
    def data_quality_evaluation(self, clean_data):
        if "匿名集数据质量评估结果" in clean_data:
            item = clean_data["匿名集数据质量评估结果"]
        else:
            return
        
        ### 分布泄露
        Distributed_leakage = {}
        ### KL_Divergence
        KL_Divergence = {}
        ### 正面信息披露
        Positive_information_disclosure = {}
        ### 熵泄露
        Entropy_leakage = {}

        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)  # 匹配整数或小数
            return numbers[0] if numbers else None  # 取第一个数字
        
        for Privacy_data_str in item["分布泄露"]:
            attribute_match = re.search(r'(?<=数据集针对)(.*)(?=(的分布泄))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            Distributed_leakage[attribute] = extract_last_number(Privacy_data_str)
        ## print(f"===== 分布泄露 =====\n{Distributed_leakage}")
        
        for Privacy_data_str in item["KL_Divergence"]:
            attribute_match = re.search(r'(?<=数据集针对)(.*)(?=(的KL散度为))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            KL_Divergence[attribute] = extract_last_number(Privacy_data_str)
        ## print(f"===== KL_Divergence =====\n{KL_Divergence}")

        for Privacy_data_str in item["正面信息披露"]:
            attribute_match = re.search(r'(?<=数据集针对)(.*)(?=(的正面信息披))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            Positive_information_disclosure[attribute] = extract_last_number(Privacy_data_str)
        ## print(f"===== 正面信息披露 =====\n{Positive_information_disclosure}")

        for Privacy_data_str in item["熵泄露"]:
            attribute_match = re.search(r'(?<=数据集针对)(.*)(?=(的熵泄露))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            Entropy_leakage[attribute] = extract_last_number(Privacy_data_str)
        ## print(f"===== 熵泄露 =====\n{Entropy_leakage}")

        # 合并列表元素为一个字符串
        item["KL_Divergence"] = KL_Divergence
        item["分布泄露"] = Distributed_leakage
        item["正面信息披露"] = Positive_information_disclosure
        item["熵泄露"] = Entropy_leakage
        ## print(f"===== 匿名集数据质量评估结果 =====\n{item}")

    def privacy_protection_measure(self, clean_data):
        if "隐私保护性度量评估结果" in clean_data:
            item = clean_data["隐私保护性度量评估结果"]
        else:
            return
        
        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)  # 匹配整数或小数
            return numbers[0] if numbers else None  # 取第一个数字
        
        Reidentification_risk = {}
        Reidentification_risk_entirety = {}
        Reidentification_risk_sensitiveness = {}
        
        for Privacy_data_str in item["单个属性的重识别风险"]:
            attribute_match = re.search(r'(?<=针对准标识符属性)(.*)(?=(基于熵的重))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            Reidentification_risk[attribute] = extract_last_number(Privacy_data_str)
        ### print(f"===== 单个属性的重识别风险 =====\n{Reidentification_risk}")

        for Privacy_data_str in item["敏感属性的重识别风险"]:
            attribute_match = re.search(r'(?<=针对敏感属性)(.*)(?=(基于熵的重))', Privacy_data_str)
            attribute = attribute_match.group(1) if attribute_match else None
            Reidentification_risk_sensitiveness[attribute] = extract_last_number(Privacy_data_str)
        #### print(f"===== 敏感属性的重识别风险 =====\n{Reidentification_risk}")

        Reidentification_risk_entirety['value'] = extract_last_number(item["基于熵的重识别风险"])

        item["单个属性的重识别风险"] = Reidentification_risk
        item["基于熵的重识别风险"] = Reidentification_risk_entirety
        item["敏感属性的重识别风险"] = Reidentification_risk_sensitiveness
        # print(f"===== 隐私保护性度量评估结果 =====\n{item}")

    def out(self, path = './new/data/out.json'):
        # print(f"=====输出的数据为 =====\n{self.raw_data}")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.raw_data, f, ensure_ascii=False, indent=4)
        print(f"数据已保存到{os.getcwd()}\\{path}")

class DataProcessing:
    def __init__(self, raw_data:dict):
        self.raw_data = raw_data # 实例属性
        self.Core_data = {}
        self.core_data_initialization()
        self.change_level = {}
        self.change_level_initialization()
        
    
    def __init__(self, json_path:str):
        with open(json_path, 'r', encoding='utf-8') as file:
            self.raw_data = json.load(file)
        self.Core_data = {}
        self.core_data_initialization()
        self.change_level = {}
        self.change_level_initialization()
    
    ## 初始化
    # def core_data_initialization(self):
    #     self.Core_data["敏感属性维度"] =  copy.deepcopy(self.raw_data["匿名集数据特征评估结果"]["敏感属性维数"])
    #     self.Core_data["敏感属性"] = {}
    #     ## todo => 之后可能单独输出中间变量，目前设为固定值
    #     for sensitive_attribute in self.raw_data["匿名集数据特征评估结果"]["敏感属性种类"]:
    #         self.Core_data["敏感属性"][sensitive_attribute] = {}
    #         self.Core_data["敏感属性"][sensitive_attribute]["取值种类"] = 10
    #         self.Core_data["敏感属性"][sensitive_attribute]["取值"] = []
    #         ## 之后接口，应该用不到，
    #         self.Core_data["敏感属性"][sensitive_attribute]["等价类分布"] = float(self.raw_data["匿名集数据质量评估结果"]["分布泄露"][sensitive_attribute]) * 10000
    #         self.Core_data["敏感属性"][sensitive_attribute]["整体分布"] = 10000
        
    #     ##todo => 改为类似隐私属性的
    #     self.Core_data["参考属性"] = copy.deepcopy(self.raw_data["隐私保护性度量评估结果"]["单个属性的重识别风险"])
    #     ## todo => 之后可能单独输出中间变量，目前设为固定值
    #     self.Core_data["数据总体"] = 100000
    #     self.Core_data["数据的等价类[自身]"] = {
    #         "等价组大小":copy.deepcopy(self.raw_data["可用性评估结果"]["归一化平均等价组大小度量"]),
    #         "等价组参考属性":copy.deepcopy(list(self.raw_data["隐私保护性度量评估结果"]["单个属性的重识别风险"].keys())), # type: ignore
    #         ## k_匿名度 = "等价组大小"（最小）
    #         ## L-多样性 = "等价组参考属性"
    #         ## todo , 有待实际考察，应该比这个少，随便取 0.8 修正
    #         "等价组数量" : int(self.Core_data["数据总体"] / self.raw_data["可用性评估结果"]["归一化平均等价组大小度量"] * 0.8),
    #         "等价组的值":[]
    #     }
        
    #     ## todo => 之后可以在计算中单独输出记录
    #     self.Core_data["隐匿的记录数目"] = 0

    #     ### 没啥用，但是计算需要，看看可不可以删了
    #     self.Core_data["k-匿名度"] = copy.deepcopy(self.raw_data["合规性评估结果"]["K-Anonymity"]["k_value"])
    #     # self.Core_data["等价组个数"] = 1
    #     # 归入"等价组数量"
    #     # self.Core_data["敏感属性在原数据中的分布"] = 1
    #     # 归入"敏感属性"

    #     # self.Core_data["包含某一敏感属性的等价类的值"] = 1
    #     # self.Core_data["某一敏感属性在原数据中的个数"] = 1
    #     ## todo => 归入分布，但是肯定不精确，之后再说

    #     # self.Core_data["敏感属性的种类个数"] = 1
    #     # 归入"敏感属性"

    #     '''
    #     ## 废除，并入"敏感属性"
    #     ## todo => 之后可以在计算中单独输出记录
    #     self.Core_data["敏感属性在等价类中的分布"] = copy.deepcopy(self.raw_data["匿名集数据质量评估结果"]["分布泄露"])
    #     item = self.Core_data["敏感属性在等价类中的分布"]
    #     self.Core_data["敏感属性在等价类中的分布"] = {key: float(value)*10000 for key, value in item.items()}
    #     ## 直接按比例缩放
        
    #     ## 废除想法
    #     # values = {key: float(value) for key, value in item.items()}
    #     # sorted_items = sorted(values.items(), key=lambda x: x[1])
    #     # self.Core_data["敏感属性在等价类中的分布"] = {key: (index + 1) * 100 for index, (key, value) in enumerate(sorted_items)}
    #     ### 分布按照 "分布泄露" 从小到大排列
    #     '''

    def core_data_initialization(self):
        self.Core_data["敏感属性维度"] =  copy.deepcopy(self.raw_data["匿名集数据特征评估结果"]["敏感属性维数"])
        self.Core_data["敏感属性"] = {}
        ## todo => 之后可能单独输出中间变量，目前设为固定值
        for sensitive_attribute in self.raw_data["匿名集数据特征评估结果"]["敏感属性种类"]:
            self.Core_data["敏感属性"][sensitive_attribute] = {}
            self.Core_data["敏感属性"][sensitive_attribute]["取值种类"] = 0
            self.Core_data["敏感属性"][sensitive_attribute]["取值"] = []
            ## 之后接口，应该用不到，
            self.Core_data["敏感属性"][sensitive_attribute]["等价类分布"] = 0
            self.Core_data["敏感属性"][sensitive_attribute]["整体分布"] = 0
        
        ##todo => 改为类似隐私属性的
        self.Core_data["参考属性"] = copy.deepcopy(self.raw_data["隐私保护性度量评估结果"]["单个属性的重识别风险"])
        ## todo => 之后可能单独输出中间变量，目前设为固定值
        self.Core_data["数据总体"] = 0
        self.Core_data["数据的等价类[自身]"] = {
            "等价组大小":0,
            "等价组参考属性":copy.deepcopy(list(self.raw_data["隐私保护性度量评估结果"]["单个属性的重识别风险"].keys())), # type: ignore
            ## k_匿名度 = "等价组大小"（最小）
            ## L-多样性 = "等价组参考属性"
            ## todo , 有待实际考察，应该比这个少，随便取 0.8 修正
            "等价组数量" : 0,
            "等价组的值":[]
        }
        
        ## todo => 之后可以在计算中单独输出记录
        self.Core_data["隐匿的记录数目"] = 0

        ### 没啥用，但是计算需要，看看可不可以删了
        self.Core_data["k-匿名度"] = 0
        # self.Core_data["等价组个数"] = 1
        # 归入"等价组数量"
        # self.Core_data["敏感属性在原数据中的分布"] = 1
        # 归入"敏感属性"

        # self.Core_data["包含某一敏感属性的等价类的值"] = 1
        # self.Core_data["某一敏感属性在原数据中的个数"] = 1
        ## todo => 归入分布，但是肯定不精确，之后再说

        # self.Core_data["敏感属性的种类个数"] = 1
        # 归入"敏感属性"

        '''
        ## 废除，并入"敏感属性"
        ## todo => 之后可以在计算中单独输出记录
        self.Core_data["敏感属性在等价类中的分布"] = copy.deepcopy(self.raw_data["匿名集数据质量评估结果"]["分布泄露"])
        item = self.Core_data["敏感属性在等价类中的分布"]
        self.Core_data["敏感属性在等价类中的分布"] = {key: float(value)*10000 for key, value in item.items()}
        ## 直接按比例缩放
        
        ## 废除想法
        # values = {key: float(value) for key, value in item.items()}
        # sorted_items = sorted(values.items(), key=lambda x: x[1])
        # self.Core_data["敏感属性在等价类中的分布"] = {key: (index + 1) * 100 for index, (key, value) in enumerate(sorted_items)}
        ### 分布按照 "分布泄露" 从小到大排列
        '''

    def change_level_initialization(self):
        self.change_level = {}
        self.change_level["风险评估结果"] = {}
        self.change_level["风险评估结果"]["统计推断还原率"] = 0
        self.change_level["风险评估结果"]["重识别率"] = 0

        self.change_level["合规性评估结果"] = {}
        self.change_level["合规性评估结果"]["(α,k)-Anonymity"] = 0
        ## 只讨论(a)
        self.change_level["合规性评估结果"]["K-Anonymity"] = 0
        self.change_level["合规性评估结果"]["L-Diversity"] = 0
        self.change_level["合规性评估结果"]["T-Closeness"] = 0

        self.change_level["可用性评估结果"] = {}
        self.change_level["可用性评估结果"]["基于熵的平均数据损失度"] = 0
        self.change_level["可用性评估结果"]["归一化平均等价组大小度量"] = 0
        self.change_level["可用性评估结果"]["数据可辨别度"] = 0
        self.change_level["可用性评估结果"]["数据记录匿名率"] = 0

        self.change_level["匿名集数据特征评估结果"] = {}
        self.change_level["匿名集数据特征评估结果"]["准标识符维数"] = 0
        self.change_level["匿名集数据特征评估结果"]["固有隐私"] = 0
        self.change_level["匿名集数据特征评估结果"]["平均泛化程度"] = 0
        self.change_level["匿名集数据特征评估结果"]["敏感属性维数"] = 0

        self.change_level["匿名集数据质量评估结果"] = {}
        self.change_level["匿名集数据质量评估结果"]["KL_Divergence"] = {}
        self.change_level["匿名集数据质量评估结果"]["分布泄露"] = {}
        self.change_level["匿名集数据质量评估结果"]["正面信息披露"] = {}
        self.change_level["匿名集数据质量评估结果"]["熵泄露"] = {}
        for key in self.raw_data["匿名集数据质量评估结果"]["KL_Divergence"]:
            self.change_level["匿名集数据质量评估结果"]["KL_Divergence"][key] = 0
            self.change_level["匿名集数据质量评估结果"]["分布泄露"][key] = 0
            self.change_level["匿名集数据质量评估结果"]["正面信息披露"][key] = 0
            self.change_level["匿名集数据质量评估结果"]["熵泄露"][key] = 0

        self.change_level["隐私保护性度量评估结果"] = {}
        self.change_level["隐私保护性度量评估结果"]["基于熵的重识别风险"] = 0


    ## 输出到文件
    def out(self, path = './new/data/out.json'):
        # print(f"=====输出的数据 raw data 为 =====\n{self.raw_data}")
        # print(f"=====输出的数据 Core_data 为 =====\n{self.Core_data}")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.raw_data, f, ensure_ascii=False, indent=4)
        with open(path, "a", encoding="utf-8") as f:
            json.dump(self.Core_data, f, ensure_ascii=False, indent=4)
        with open(path, "a", encoding="utf-8") as f:
            json.dump(self.change_level, f, ensure_ascii=False, indent=4)
        print(f"数据已保存到{os.getcwd()}\\{path}")

    def calculate_output_property(self):
        return 
    

    ## 可供调整的属性有限，在此列出可以调整的属性及规范输入
    # K-Anonymity, 0
    # L-Diversity, 1
    # T-Closeness, 2
    # (α,k)-Anonymity 3
    # 基于熵的平均数据损失度, 4
    ## 均匀时最小
    # 归一化平均等价组大小度量, 5
    # 数据可辨别度, 6
    # 准标识符维数, 7
    # 固有隐私, 8
    # 平均泛化程度, 9
    # 敏感属性维数, 10
    # 基于熵的重识别风险, 11
    # KL_Divergence, 12
    # 分布泄露, 13
    # 正面信息披露, 14
    # 熵泄露, 15
    # 单个属性的重识别风险, 16
    # 敏感属性的重识别风险, 17
    ## up == True : 向上调
    ## up == False : 向下调整
    ## 12 <= flag <= 17是，需要输入调整的具体属性值 sensitive_attribute
    def core_data_adjust(self, flag, up = True, sensitive_attribute = "", level_change = 3):
        flag = int(flag)
        if(up == True):
            change_flag = level_change
        else:
            change_flag = -1 * level_change
        match flag:
            case 0:
                self.Core_data["数据的等价类[自身]"]["等价组大小"] -= change_flag
                return
            case 1:
                self.Core_data["敏感属性"]["取值种类"] += change_flag
                return
            case 2:
                self.Core_data["敏感属性"]["整体分布"] -= change_flag
                return
            case 3:
                self.Core_data["敏感属性"]["等价类分布"] += change_flag
                return
            case 4:
                self.Core_data["敏感属性"]["等价类分布"] -= change_flag
                return
            case 5:
                self.Core_data["数据的等价类[自身]"]["等价组大小"] += change_flag
            case 6:
                self.Core_data["敏感属性"]["等价类分布"] -= change_flag
                return
            case 7:
                self.Core_data["数据的等价类[自身]"]["取值种类"] += change_flag
            
                return
            case 2:
                self.Core_data["敏感属性"]["等价组参考属性"] -= change_flag
                return
            case 3:
                self.Core_data["敏感属性"]["等价类分布"] += change_flag
                return
            case 4:
                self.Core_data["敏感属性"]["等价类分布"] -= change_flag
                self.Core_data["数据总体"] += change_flag
                return
            case 5:
                self.Core_data["数据的等价类[自身]"]["等价组大小"] += change_flag
            case _:
                print("输入不合规，请重新输入")
        

if __name__ == '__main__':
    data_object = DataCleaning(raw_data)
    data_object.data_extraction()
    data_object.out()
    ## 注释掉输出
    data_processing = DataProcessing('./new/data/out.json')
    # data_processing.show()
    data_processing.out()