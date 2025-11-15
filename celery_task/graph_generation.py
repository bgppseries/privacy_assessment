import json
import math
import time
import pandas as pd
import numpy as np
import uuid
import psutil
from sqlalchemy import create_engine,text
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple  # 确保导入Optional
import hashlib
import random
import inspect 

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
        读取 JSON 或 CSV 文件并返回一个 pandas DataFrame
        '''
        ##如果读取sql文件，将其转化为DataFrame格式
        # url='mysql+pymysql://root:784512@localhost:3306/local_test'
        columns = self.QIDs+self.SA
        ## query = f"SELECT {', '.join(columns)}  FROM {self.json_address}"
        query = f"SELECT {', '.join([f'`{col}`' for col in columns])}  FROM {self.json_address}"
        # 创建SQLAlchemy的Engine对象
        engine = create_engine(self.src_url).connect()
        return pd.read_sql(sql=text(query),con=engine)


    def _Function_Series_quasi(self,_TemAll):
        '''
        :return: 返回数据中的所有准标识符，及其数量,按照升序排列,<class 'pandas.core.series.Series'>格式
        10万条数据，用时0.3秒
        '''
        # _TemAll = self._Function_Data()
        return _TemAll.groupby(self.address_Attr[0], sort=False).size().sort_values()

    def _Probabilistic_Distribution_Privacy(self,each_privacy,_TemAll):
        '''
        :param each_privacy:选取的某一个隐私属性或者某几个隐私属性
        :return:返回针对选取的隐私属性的概率分布,默认降序
        10万条数据，用时0.29秒
        '''
        # _TemAll = self._Function_Data()
        return _TemAll[each_privacy].value_counts(normalize=True)

    def _Num_address(self,_TemAll):
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
        return mem_info.rss / (1024 * 1024* 1024)  # Convert bytes to GB

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

    def Average_degree_annonymity(self, Series_quasi,_TemAll):
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

    def Inherent_Privacy(self, Series_quasi,_TemAll):
        '''
        :param Series_quasi: 数据集的所有准标识符，及其对应等价组大小
        :return:返回与数据集概率分布的不确定性相同的均匀分布区间长度,数值越大，安全性越低
        '''
        Num1 = 0.0
        Num_All = self._Num_address(_TemAll)
        for each in Series_quasi:
            Num1 += (each / Num_All) * math.log2(Num_All / each)
        return int(2 ** Num1)

    def runL(self, Series_quasi,_TemAll):
        Dimen_QID = self.Dimension_QID()
        print("准标识符维数为：", Dimen_QID)
        Dimen_SA = self.Dimension_SA()
        print("敏感属性维数为：", Dimen_SA)
        Attribute_SA = self.Attribute_SA()
        print("敏感属性种类为：", Attribute_SA)
        Average_anonymity = self.Average_degree_annonymity(Series_quasi,_TemAll)
        print("平均泛化程度为：", Average_anonymity)
        Inherent_Priv = self.Inherent_Privacy( Series_quasi,_TemAll)
        print("固有隐私为：", Inherent_Priv)
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
        sendvalue(self.worker_uuid, "敏感属性种类",Attribute_SA, int)
        sendvalue(self.worker_uuid, "平均泛化程度",Average_anonymity, float)
        sendvalue(self.worker_uuid, "固有隐私",Inherent_Priv, int)

### 获取数据库值，挑选有代表性的节点，构成图
## TODO:选取有交集的节点
class Graph(Config):
    import pandas as pd
    from typing import Dict, List, Set
    def __init__(self, k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene):
        '''
        :param k: 输入K匿名
        :param l: 输入l多样性
        :param t: 输入T紧密度
        :param address: 选取文件的地址
        '''
        super().__init__(k,l,t,url,address,worker_uuid,QIDs,SA,ID,bg_url,scene)
    
    ### 存储图数据到文件/数据库
    def store_graph_data(self, json_str: str, flag: int = 0):
        import os

        # 获取当前代码文件（graph_generation.py）的绝对路径
        current_file_path = os.path.abspath(__file__)
        # 获取当前文件所在目录（celery_task/）
        current_dir = os.path.dirname(current_file_path)
        # 构造目标目录（graph_data/）的绝对路径
        target_dir = os.path.join(current_dir, f"../data/{self.worker_uuid}")
        # 规范化路径（自动处理 ".." 等相对路径符号）
        target_dir = os.path.normpath(target_dir)
        os.makedirs(target_dir, exist_ok=True)

        # 生成具体文件路径（例如 output.png）
        file_path = os.path.join(target_dir, f"{flag}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json_str)

    ### 准标识维度图获取
    def buildQuasiRepresentationDimensionGraph(self, max_categories: int = 50, graph_id: int = 13) -> str:
        """
        提取QID的所有值，生成指定格式的图数据并返回JSON格式字符串
        
        :param max_categories: 每个QID最多保留的种类数量，默认为20
        :return: 符合指定节点和边格式的图数据JSON字符串
        """
        try:
            # 获取完整数据和QID列表
            data = self._Function_Data()
            qids = self.QIDs
            scene = getattr(self, 'scene', '')  # 从实例获取场景信息，默认空字符串
            
            if len(qids) < 2:
                print("警告：至少需要2个QID才能建立关联关系")
                return json.dumps({"nodes": [], "edges": []})
            
            # 为每个QID值生成唯一ID的辅助函数
            def get_unique_id(qid: str, value: str) -> int:
                """生成唯一整数ID"""
                return hash(f"{qid}::{value}") % 1000000  # 取模防止ID过大
            
            # 为每个QID获取出现次数最多的前N个值
            qid_top_values = {}
            for qid in qids:
                if qid not in data.columns:
                    continue
                    
                # 计算每个值的出现次数并排序
                value_counts = data[qid].value_counts().reset_index()
                value_counts.columns = [qid, 'count']
                
                # 只保留前max_categories个值
                top_values = value_counts.head(max_categories)
                qid_top_values[qid] = {
                    'values': set(top_values[qid].astype(str)),
                    'data': top_values,
                    'id_mapping': {str(val): get_unique_id(qid, str(val)) for val in top_values[qid]}
                }
            
            # 收集所有节点 - 按指定格式
            nodes = []
            node_ids = set()
            
            for qid in qids:
                if qid not in qid_top_values:
                    continue
                    
                for _, row in qid_top_values[qid]['data'].iterrows():
                    value = str(row[qid])
                    node_id = qid_top_values[qid]['id_mapping'][value]
                    
                    if node_id not in node_ids:
                        # 计算节点大小，基于出现次数做适当缩放
                        size = min(30 + row['count'] * 0.5, 100)  # 限制最大尺寸
                        
                        nodes.append({
                            "category": qid,  # 任务名称使用QID
                            "id": node_id,    # 整数ID
                            "name": value,    # 节点名称为QID值
                            "scene": scene,   # 场景信息
                        })
                        node_ids.add(node_id)
            
            # 计算QID之间的关联关系（边）- 按指定格式
            edges = []
            edge_pairs = set()
            
            # 遍历所有QID对组合
            for i in range(len(qids)):
                for j in range(i + 1, len(qids)):
                    qid1 = qids[i]
                    qid2 = qids[j]
                    
                    if qid1 not in qid_top_values or qid2 not in qid_top_values:
                        continue
                    
                    # 提取两个QID列，只保留在top列表中的值
                    filtered_data = data[
                        data[qid1].astype(str).isin(qid_top_values[qid1]['values']) & 
                        data[qid2].astype(str).isin(qid_top_values[qid2]['values'])
                    ]
                    
                    # 去除空值
                    qid_pair_data = filtered_data[[qid1, qid2]].dropna()
                    
                    # 计算每对值的共现次数（关联强度）
                    co_occurrence = qid_pair_data.groupby([qid1, qid2]).size().reset_index(name='count')
                    
                    # 生成边
                    for _, row in co_occurrence.iterrows():
                        source_value = str(row[qid1])
                        target_value = str(row[qid2])
                        
                        source_id = qid_top_values[qid1]['id_mapping'][source_value]
                        target_id = qid_top_values[qid2]['id_mapping'][target_value]
                        
                        # 确保边的唯一性
                        edge_key = frozenset([source_id, target_id])
                        if edge_key not in edge_pairs:
                            edges.append({
                                "scene": scene,                # 场景信息
                                "source": source_id,           # 源节点ID
                                "target": target_id,           # 目标节点ID
                            })
                            edge_pairs.add(edge_key)
            
            # 构建图数据字典
            graph_data = {
                "nodes": nodes,
                "links": edges
            }

            # 2. 转换为JSON字符串
            json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str
            
        except Exception as e:
            error_msg = f"生成QID图数据时发生错误: {str(e)}"
            print(error_msg)
            return json.dumps({"error": error_msg})
    
    ### 敏感属性/敏感属性维度图获取
    def buildSensitiveAttributesGraph(self, max_categories: int = 50, graph_id: int = 14) -> str:
        """
        提取QID的所有值，生成指定格式的图数据并返回JSON格式字符串
        
        :param max_categories: 每个QID最多保留的种类数量，默认为20
        :return: 符合指定节点和边格式的图数据JSON字符串
        """
        try:
            # 获取完整数据和QID列表
            data = self._Function_Data()
            qids = self.SA
            scene = getattr(self, 'scene', '')  # 从实例获取场景信息，默认空字符串
            
            if len(qids) < 2:
                print("警告：至少需要2个SA才能建立关联关系")
                return json.dumps({"nodes": [], "edges": []})
            
            # 为每个QID值生成唯一ID的辅助函数
            def get_unique_id(qid: str, value: str) -> int:
                """生成唯一整数ID"""
                return hash(f"{qid}::{value}") % 1000000  # 取模防止ID过大
            
            # 为每个QID获取出现次数最多的前N个值
            qid_top_values = {}
            for qid in qids:
                if qid not in data.columns:
                    continue
                    
                # 计算每个值的出现次数并排序
                value_counts = data[qid].value_counts().reset_index()
                value_counts.columns = [qid, 'count']
                
                # 只保留前max_categories个值
                top_values = value_counts.head(max_categories)
                qid_top_values[qid] = {
                    'values': set(top_values[qid].astype(str)),
                    'data': top_values,
                    'id_mapping': {str(val): get_unique_id(qid, str(val)) for val in top_values[qid]}
                }
            
            # 收集所有节点 - 按指定格式
            nodes = []
            node_ids = set()
            
            for qid in qids:
                if qid not in qid_top_values:
                    continue
                    
                for _, row in qid_top_values[qid]['data'].iterrows():
                    value = str(row[qid])
                    node_id = qid_top_values[qid]['id_mapping'][value]
                    
                    if node_id not in node_ids:
                        # 计算节点大小，基于出现次数做适当缩放
                        size = min(30 + row['count'] * 0.5, 100)  # 限制最大尺寸
                        
                        nodes.append({
                            "category": qid,  # 任务名称使用QID
                            "id": node_id,    # 整数ID
                            "name": value,    # 节点名称为QID值
                            "scene": scene,   # 场景信息
                        })
                        node_ids.add(node_id)
            
            # 计算QID之间的关联关系（边）- 按指定格式
            edges = []
            edge_pairs = set()
            
            # 遍历所有QID对组合
            for i in range(len(qids)):
                for j in range(i + 1, len(qids)):
                    qid1 = qids[i]
                    qid2 = qids[j]
                    
                    if qid1 not in qid_top_values or qid2 not in qid_top_values:
                        continue
                    
                    # 提取两个QID列，只保留在top列表中的值
                    filtered_data = data[
                        data[qid1].astype(str).isin(qid_top_values[qid1]['values']) & 
                        data[qid2].astype(str).isin(qid_top_values[qid2]['values'])
                    ]
                    
                    # 去除空值
                    qid_pair_data = filtered_data[[qid1, qid2]].dropna()
                    
                    # 计算每对值的共现次数（关联强度）
                    co_occurrence = qid_pair_data.groupby([qid1, qid2]).size().reset_index(name='count')
                    
                    # 生成边
                    for _, row in co_occurrence.iterrows():
                        source_value = str(row[qid1])
                        target_value = str(row[qid2])
                        
                        source_id = qid_top_values[qid1]['id_mapping'][source_value]
                        target_id = qid_top_values[qid2]['id_mapping'][target_value]
                        
                        # 确保边的唯一性
                        edge_key = frozenset([source_id, target_id])
                        if edge_key not in edge_pairs:
                            edges.append({
                                "scene": scene,                # 场景信息
                                "source": source_id,           # 源节点ID
                                "target": target_id,           # 目标节点ID
                            })
                            edge_pairs.add(edge_key)
            
            # 构建图数据字典
            graph_data = {
                "nodes": nodes,
                "links": edges
            }

            # 2. 转换为JSON字符串
            json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str
            
        except Exception as e:
            error_msg = f"生成QID图数据时发生错误: {str(e)}"
            print(error_msg)
            return json.dumps({"error": error_msg})

    ### 最小等价类图获取 / over
    def buildSmallestEquivalenceClasses(self, num: int = 50, attributes: List[str] = None, graph_id: int = 11) -> str:
        """
        按照所选属性标识等价类，获取数量最小的10个等价类，并转换为nodes和lines格式
        
        :param attributes: 用于标识等价类的属性列表，默认为self.QIDs
        :return: 包含nodes和lines的JSON字符串
        """
        try:
            # 处理默认属性列表
            if attributes is None:
                attributes = self.QIDs
            
            # 获取完整数据
            data = self._Function_Data()
            
            # 检查属性是否都存在于数据中
            missing_attrs = [attr for attr in attributes if attr not in data.columns]
            if missing_attrs:
                error_msg = f"以下属性不存在于数据中: {', '.join(missing_attrs)}"
                print(error_msg)
                return json.dumps({"error": error_msg})
            
            # 获取场景信息
            scene = getattr(self, 'scene', '')
            
            # 按所选属性分组，计算每个等价类的数量
            equivalence_classes = data.groupby(attributes).size().reset_index(name='count')
            
            # 按数量升序排序，取最小的10个
            smallest_classes = equivalence_classes.sort_values('count').head(num)
            
            # 生成节点ID的辅助函数
            def get_node_id(attr: str, value: str) -> int:
                """为属性值生成唯一整数ID"""
                hash_obj = hashlib.md5(f"{attr}::{value}".encode())
                return int(hash_obj.hexdigest(), 16) % 1000000
            
            # 收集所有节点
            nodes = []
            node_ids = set()
            class_nodes = []  # 存储等价类组合节点
            
            # 1. 先添加属性值节点
            for attr in attributes:
                # 获取该属性在选中等价类中的所有值
                values = set()
                for _, row in smallest_classes.iterrows():
                    values.add(str(row[attr]))
                
                for value in values:
                    node_id = get_node_id(attr, value)
                    if node_id not in node_ids:
                        # 根据出现次数计算节点大小
                        count = sum(1 for _, row in smallest_classes.iterrows() if str(row[attr]) == value)
                        symbol_size = 30 + count * 5
                        
                        nodes.append({
                            "category": attr,
                            "id": node_id,
                            "name": value,
                            "scene": scene,
                            "symbolSize": min(int(symbol_size), 80)  # 限制最大尺寸
                        })
                        node_ids.add(node_id)
            
            # 2. 添加等价类组合节点
            for idx, row in smallest_classes.iterrows():
                class_id = 1000000 + idx  # 确保与属性值节点ID不冲突
                class_name = ", ".join([f"{attr}: {row[attr]}" for attr in attributes])
                
                class_nodes.append({
                    "category": "equivalence_class",
                    "id": class_id,
                    "name": f"等价类 {idx+1}",
                    "scene": scene,
                    "details": class_name,
                    "count": row['count']
                })
                node_ids.add(class_id)
            
            # 合并所有节点
            nodes.extend(class_nodes)
            
            # 生成连线
            lines = []
            line_ids = set()
            
            # 连接等价类节点与其包含的属性值节点
            for idx, row in smallest_classes.iterrows():
                class_id = 1000000 + idx
                
                for attr in attributes:
                    value = str(row[attr])
                    value_node_id = get_node_id(attr, value)
                    
                    # 确保连线唯一
                    line_key = (class_id, value_node_id)
                    if line_key not in line_ids:
                        lines.append({
                            "scene": scene,
                            "source": class_id,
                            "target": value_node_id,
                            "value": attr  # 连线值为属性名
                        })
                        line_ids.add(line_key)
            
            # 构建结果
            graph_data = {
                "nodes": nodes,
                "links": lines,
            }
            
            # 转换为JSON并返回
            json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str
            
        except Exception as e:
            error_msg = f"生成等价类图数据时发生错误: {str(e)}"
            print(error_msg)
            return json.dumps({"error": error_msg})

    ### 固有隐私图获取
    def buildInherentPrivacyGraph(self, graph_id: int = 16) -> str:
        """
        生成固有隐私图谱（基于Config类的QIDs/SA/ID配置）
        固有隐私：直接标识符（ID）+ 敏感属性（SA）+ 其关联关系
        """
        try:
            # 1. 复用父类方法读取数据，获取表结构和基础信息
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)
            
            table_name = self.json_address  # 表名（来自Config的json_address）
            direct_ids = self.ID            # 直接标识符（固有隐私核心，如身份证号）
            sensitive_attrs = self.SA       # 敏感属性（固有隐私，如病历）
            quasi_ids = self.QIDs           # 准标识符（与固有隐私关联的字段）
            scene = self.scene
            hash_base = f"{table_name}_{self.worker_uuid}_{scene}"  # 唯一哈希基准

            # 2. 初始化节点和边存储
            nodes: List[Dict] = []
            node_map: Dict[Tuple[str, str], int] = {}  # (节点类型, 唯一标识) → 节点ID
            links: List[Dict] = []
            link_set: Set[Tuple[int, int, str]] = set()  # 去重：(源ID, 目标ID, 边类型)

            # 3. 生成节点（完全基于Config配置的隐私属性）
            # 3.1 数据表节点（数据源头）
            category = "数据表"
            unique_key = table_name
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 50,
                    "总记录数": self._Num_address(_TemAll),
                    "直接标识符": direct_ids,
                    "敏感属性": sensitive_attrs,
                    "准标识符": quasi_ids
                })

            # 3.2 直接标识符节点（核心固有隐私，如身份证号、用户ID）
            for direct_id in direct_ids:
                if direct_id not in _TemAll.columns:
                    continue  # 跳过表中不存在的字段
                category = "直接标识符（固有隐私）"
                unique_key = direct_id
                key = (category, unique_key)
                if key not in node_map:
                    # 直接标识符的固有特征：唯一值占比（通常接近100%，用于身份定位）
                    unique_ratio = round(len(_TemAll[direct_id].drop_duplicates()) / len(_TemAll), 4)
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"直接标识符：{direct_id}",
                        "scene": scene,
                        "symbolSize": 45,
                        "数据类型": str(_TemAll[direct_id].dtype),
                        "唯一值占比": f"{unique_ratio*100}%",
                        "隐私等级": "极高（可直接定位身份）"
                    })

            # 3.3 敏感属性节点（固有隐私，如病历、余额）
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue
                category = "敏感属性（固有隐私）"
                unique_key = sa
                key = (category, unique_key)
                if key not in node_map:
                    # 敏感属性的固有特征：唯一值数量（反映隐私多样性）
                    unique_count = len(_TemAll[sa].drop_duplicates())
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"敏感属性：{sa}",
                        "scene": scene,
                        "symbolSize": 40,
                        "数据类型": str(_TemAll[sa].dtype),
                        "唯一值数量": unique_count,
                        "隐私等级": "高（涉及个人权益）"
                    })

            # 3.4 准标识符节点（与固有隐私关联的辅助字段）
            for qid in quasi_ids:
                if qid not in _TemAll.columns:
                    continue
                category = "准标识符（关联隐私）"
                unique_key = qid
                key = (category, unique_key)
                if key not in node_map:
                    # 准标识符的固有特征：唯一值数量（用于辅助定位）
                    unique_count = len(_TemAll[qid].drop_duplicates())
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"准标识符：{qid}",
                        "scene": scene,
                        "symbolSize": 35,
                        "数据类型": str(_TemAll[qid].dtype),
                        "唯一值数量": unique_count,
                        "作用": "辅助关联直接标识符，提升隐私泄露风险"
                    })

            # 4. 生成边（体现固有隐私的天然关联）
            # 4.1 数据表 → 直接标识符（包含关系）
            src_table = ("数据表", table_name)
            for direct_id in direct_ids:
                if direct_id not in _TemAll.columns:
                    continue
                tgt_direct = ("直接标识符（固有隐私）", direct_id)
                if src_table in node_map and tgt_direct in node_map:
                    src_id = node_map[src_table]
                    tgt_id = node_map[tgt_direct]
                    link_key = (src_id, tgt_id, "包含固有隐私")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "包含直接标识符"},
                            "link_type": "固有包含",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.2 数据表 → 敏感属性（包含关系）
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue
                tgt_sa = ("敏感属性（固有隐私）", sa)
                if src_table in node_map and tgt_sa in node_map:
                    src_id = node_map[src_table]
                    tgt_id = node_map[tgt_sa]
                    link_key = (src_id, tgt_id, "包含敏感隐私")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "包含敏感属性"},
                            "link_type": "固有包含",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.3 数据表 → 准标识符（包含关系）
            for qid in quasi_ids:
                if qid not in _TemAll.columns:
                    continue
                tgt_qid = ("准标识符（关联隐私）", qid)
                if src_table in node_map and tgt_qid in node_map:
                    src_id = node_map[src_table]
                    tgt_id = node_map[tgt_qid]
                    link_key = (src_id, tgt_id, "包含关联字段")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "包含准标识符"},
                            "link_type": "关联包含",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.4 准标识符 → 直接标识符（天然关联：准标识符可辅助定位直接标识符）
            for qid in quasi_ids:
                if qid not in _TemAll.columns:
                    continue
                src_qid = ("准标识符（关联隐私）", qid)
                for direct_id in direct_ids:
                    if direct_id not in _TemAll.columns:
                        continue
                    tgt_direct = ("直接标识符（固有隐私）", direct_id)
                    if src_qid in node_map and tgt_direct in node_map:
                        src_id = node_map[src_qid]
                        tgt_id = node_map[tgt_direct]
                        link_key = (src_id, tgt_id, "辅助关联")
                        if link_key not in link_set:
                            links.append({
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "辅助定位身份"},
                                "link_type": "天然关联",
                                "scene": scene
                            })
                            link_set.add(link_key)

            # 4.5 直接标识符 → 敏感属性（天然关联：直接标识符对应唯一敏感属性）
            for direct_id in direct_ids:
                if direct_id not in _TemAll.columns:
                    continue
                src_direct = ("直接标识符（固有隐私）", direct_id)
                for sa in sensitive_attrs:
                    if sa not in _TemAll.columns:
                        continue
                    tgt_sa = ("敏感属性（固有隐私）", sa)
                    if src_direct in node_map and tgt_sa in node_map:
                        # 验证关联：直接标识符是否与敏感属性一一对应（固有隐私的核心关联）
                        is_one2one = len(_TemAll.groupby(direct_id)[sa].nunique().unique()) <= 1
                        src_id = node_map[src_direct]
                        tgt_id = node_map[tgt_sa]
                        link_key = (src_id, tgt_id, "唯一关联")
                        if link_key not in link_set:
                            links.append({
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "唯一对应敏感属性" if is_one2one else "关联敏感属性"},
                                "link_type": "隐私关联",
                                "scene": scene,
                                "关联类型": "一对一" if is_one2one else "一对多"
                            })
                            link_set.add(link_key)

            # 5. 返回图谱结果
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({
                "nodes": [],
                "links": [],
                "error": f"固有隐私图谱生成失败：{str(e)}"
            }, ensure_ascii=False)

    ### 最小等价类 K 依据
    def getEquivalenceClassBasis(self, graph_id: int = 28) -> str:
        json_str = self.buildSmallestEquivalenceClasses()
        self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
        return json_str

    ### 至少包含L个不同的敏感值
    def buildMinSensitiveAttributeGroups(self, n: int = 20, sensitive_attr: str = None, graph_id: int = 29) -> str:
        """
        生成图谱数据：
        1. 每个QID的取值拆为独立节点（如age=25、gender=男）
        2. 等价组作为中间节点，关联QID节点和隐私属性值节点
        3. 完整列出所有隐私属性值，不合并（保留原始分布）
        """
        try:
            # 1. 基础数据与参数校验
            data = self._Function_Data()
            qids = self.QIDs
            scene = getattr(self, "scene", "")

            # 校验准标识符
            if not qids:
                return json.dumps({
                    "error": "未设置准标识符（QIDs）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 校验并确定隐私属性（默认取第一个SA）
            if sensitive_attr is None:
                if not self.SA:
                    return json.dumps({
                        "error": "未设置隐私属性（SA）",
                        "nodes": [],
                        "links": []
                    }, ensure_ascii=False)
                sensitive_attr = self.SA[0]

            # 校验数据列是否存在
            required_columns = qids + [sensitive_attr]
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                return json.dumps({
                    "error": f"数据缺少必要列：{', '.join(missing_cols)}",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 2. 按QID分组，计算每个等价组的隐私属性多样性
            grouped = data.groupby(qids, sort=False)
            group_info_list = []  # 存储每个等价组的完整信息

            for group_key, group_data in grouped:
                # 计算当前等价组的隐私属性不同值数量（用于筛选最少的n个组）
                sensitive_values_clean = group_data[sensitive_attr].dropna()  # 去除空值
                if pd.api.types.is_string_dtype(sensitive_values_clean):
                    unique_sensitive_vals = sensitive_values_clean.astype(str).drop_duplicates()
                else:
                    unique_sensitive_vals = sensitive_values_clean.drop_duplicates()
                sensitive_count = len(unique_sensitive_vals)

                # 收集等价组的核心信息（含所有隐私属性值，不去重）
                group_info = {
                    "group_key": group_key,  # 原始分组键（如(25, "男", "北京")）
                    "group_key_str": "_".join(map(str, group_key)) if isinstance(group_key, (list, tuple)) else str(group_key),
                    "group_name": f"等价组[{', '.join([f'{k}={v}' for k, v in zip(qids, group_key)])}]" if isinstance(group_key, (list, tuple)) else f"等价组[{qids[0]}={group_key}]",
                    "sensitive_count": sensitive_count,  # 隐私属性不同值数量
                    "total_records": len(group_data),  # 等价组总记录数
                    "all_sensitive_vals": sensitive_values_clean.tolist()  # 所有隐私属性值（含重复，完整列出）
                }
                group_info_list.append(group_info)

            # 3. 筛选“隐私属性不同值最少”的n个等价组
            if len(group_info_list) < n:
                n = len(group_info_list)  # 若实际组数不足n，取全部
            min_sensitive_groups = sorted(
                group_info_list,
                key=lambda x: x["sensitive_count"]  # 按隐私属性多样性升序排序
            )[:n]

            # 4. 生成节点（三层节点：QID值节点、等价组节点、隐私属性值节点）
            nodes = []
            node_map = {}  # 用于去重：key=(节点类型, 节点名称), value=节点ID

            # 4.1 生成QID值节点（每个QID的每个取值拆为独立节点）
            for group in min_sensitive_groups:
                group_key = group["group_key"]
                # 遍历当前等价组的所有QID及其取值
                for qid_name, qid_val in zip(qids, group_key):
                    node_type = f"QID_{qid_name}"  # 节点类型：区分不同QID（如QID_age、QID_gender）
                    node_name = str(qid_val)  # 节点名称：QID的具体取值（如25、男）
                    node_key = (node_type, node_name)

                    # 去重：相同QID类型+相同取值的节点只创建一次
                    if node_key in node_map:
                        continue

                    # 生成唯一节点ID（基于QID类型+取值）
                    hash_str = f"qid::{node_type}::{node_name}"
                    node_id = int(hashlib.md5(hash_str.encode()).hexdigest(), 16) % 1000000

                    # 计算节点大小：基于该QID取值在筛选组中的出现次数
                    qid_val_count = sum(
                        1 for g in min_sensitive_groups
                        if str(g["group_key"][qids.index(qid_name)]) == node_name
                    )
                    symbol_size = 30 + min(qid_val_count * 5, 50)  # 限制最大尺寸

                    # 添加QID值节点
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"准标识符[{qid_name}]的取值"
                    })
                    node_map[node_key] = node_id

            # 4.2 生成等价组节点
            for group in min_sensitive_groups:
                node_type = "equivalence_group"
                node_name = group["group_name"]
                node_key = (node_type, node_name)

                if node_key in node_map:
                    continue

                # 生成唯一节点ID（基于等价组键）
                hash_str = f"group::{group['group_key_str']}"
                node_id = int(hashlib.md5(hash_str.encode()).hexdigest(), 16) % 1000000

                # 节点大小：基于等价组总记录数
                symbol_size = 40 + min(group["total_records"] * 3, 80)

                # 添加等价组节点
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": symbol_size,
                    "desc": f"含{group['total_records']}条记录，隐私属性不同值{group['sensitive_count']}个"
                })
                node_map[node_key] = node_id

            # 4.3 生成隐私属性值节点（完整列出所有值，不合并）
            for group in min_sensitive_groups:
                node_type = f"SA_{sensitive_attr}"  # 节点类型：区分不同隐私属性（如SA_disease）
                # 遍历当前等价组的所有隐私属性值（含重复，完整列出）
                for idx, sensitive_val in enumerate(group["all_sensitive_vals"]):
                    node_name = str(sensitive_val)
                    # 为重复的隐私属性值添加唯一后缀（确保节点不重复，如“感冒_1”“感冒_2”）
                    unique_node_name = f"{node_name}_{idx}" if group["all_sensitive_vals"].count(sensitive_val) > 1 else node_name
                    node_key = (node_type, unique_node_name)

                    if node_key in node_map:
                        continue

                    # 生成唯一节点ID（基于隐私属性+唯一名称）
                    hash_str = f"sa::{node_type}::{unique_node_name}"
                    node_id = int(hashlib.md5(hash_str.encode()).hexdigest(), 16) % 1000000

                    # 节点大小：固定基础尺寸（因需完整展示所有值，避免尺寸差异干扰）
                    symbol_size = 25

                    # 添加隐私属性值节点
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": unique_node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"隐私属性[{sensitive_attr}]的取值"
                    })
                    node_map[node_key] = node_id

            # 5. 生成边（两层关联：QID值节点→等价组节点，等价组节点→隐私属性值节点）
            links = []
            link_set = set()  # 用于去重：key=(源节点ID, 目标节点ID, 边类型), value=无

            # 5.1 生成“QID值节点→等价组节点”的边
            for group in min_sensitive_groups:
                # 获取当前等价组节点的ID
                eq_group_node_key = ("equivalence_group", group["group_name"])
                eq_group_node_id = node_map.get(eq_group_node_key)
                if not eq_group_node_id:
                    continue

                # 遍历当前等价组的所有QID值节点，建立关联
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    qid_node_key = (f"QID_{qid_name}", str(qid_val))
                    qid_node_id = node_map.get(qid_node_key)
                    if not qid_node_id:
                        continue

                    # 边去重：相同源、目标、类型的边只创建一次
                    link_key = (qid_node_id, eq_group_node_id, "QID_to_Group")
                    if link_key in link_set:
                        continue

                    links.append({
                        "scene": scene,
                        "source": qid_node_id,
                        "target": eq_group_node_id,
                        "value": f"{qid_name}→等价组",
                        "label": {"show": True, "formatter": qid_name}  # 边标签：显示QID名称
                    })
                    link_set.add(link_key)

            # 5.2 生成“等价组节点→隐私属性值节点”的边
            for group in min_sensitive_groups:
                # 获取当前等价组节点的ID
                eq_group_node_key = ("equivalence_group", group["group_name"])
                eq_group_node_id = node_map.get(eq_group_node_key)
                if not eq_group_node_id:
                    continue

                # 遍历当前等价组的所有隐私属性值节点，建立关联
                for idx, sensitive_val in enumerate(group["all_sensitive_vals"]):
                    # 匹配隐私属性值节点的唯一名称
                    node_name = str(sensitive_val)
                    unique_node_name = f"{node_name}_{idx}" if group["all_sensitive_vals"].count(sensitive_val) > 1 else node_name
                    sa_node_key = (f"SA_{sensitive_attr}", unique_node_name)
                    sa_node_id = node_map.get(sa_node_key)
                    if not sa_node_id:
                        continue

                    # 边去重
                    link_key = (eq_group_node_id, sa_node_id, "Group_to_SA")
                    if link_key in link_set:
                        continue

                    links.append({
                        "scene": scene,
                        "source": eq_group_node_id,
                        "target": sa_node_id,
                        "value": f"等价组→{sensitive_attr}",
                        "label": {"show": True, "formatter": sensitive_attr}  # 边标签：显示隐私属性名称
                    })
                    link_set.add(link_key)

            # 6. 构建最终图谱数据（仅返回nodes和links）
            final_graph = {
                "nodes": nodes,
                "links": links
            }
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            error_msg = f"生成图谱数据失败：{str(e)}"
            print(error_msg)
            return json.dumps({
                "error": error_msg,
                "nodes": [],
                "links": []
            }, ensure_ascii=False)

    ### T-Closeness
    def buildSensitiveFreqTopBottomGroupsGraph(self, n: int = 20, freq_type: str = "max", sensitive_attr: str = None, graph_id: int = 30) -> str:
        """
        核心方法：获取敏感属性频率最高/最低的n个等价组，生成知识图谱（nodes + links）
        依赖：通过self.QIDs（准标识符）、self.SA（隐私属性）、self._Function_Data()（数据）获取核心参数
        
        参数：
            n: 需筛选的等价组数量（默认3）
            freq_type: 筛选类型（"max"=最高频率组，"min"=最低频率组，默认"max"）
            sensitive_attr: 目标隐私属性（默认取self.SA[0]）
        
        返回：
            包含nodes和links的JSON字符串；错误时返回含error的JSON
        """
        try:
            # --------------------------
            # 1. 核心数据与参数初始化（保留原逻辑）
            # --------------------------
            data = self._Function_Data()
            qids = self.QIDs
            scene = getattr(self, 'scene', '')
            
            # 处理目标隐私属性
            if sensitive_attr is None:
                if not self.SA:
                    return json.dumps({
                        "error": "未设置隐私属性（self.SA为空）",
                        "nodes": [],
                        "links": []
                    }, ensure_ascii=False)
                sensitive_attr = self.SA[0]

            # --------------------------
            # 2. 基础校验（保留原逻辑）
            # --------------------------
            if not qids:
                return json.dumps({
                    "error": "未设置准标识符（self.QIDs为空）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            required_cols = qids + [sensitive_attr]
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                return json.dumps({
                    "error": f"数据缺少必要列：{', '.join(missing_cols)}",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            if freq_type not in ["max", "min"]:
                return json.dumps({
                    "error": f"freq_type必须为'max'或'min'，当前为{freq_type}",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # --------------------------
            # 3. 等价组频率计算（核心优化：修复group_sa_dict生成逻辑）
            # --------------------------
            # 3.1 数据预处理：删除隐私属性空值，避免无效计算
            data_clean = data.dropna(subset=[sensitive_attr]).copy()
            if data_clean.empty:
                return json.dumps({
                    "error": "隐私属性列全为空值，无法计算频率",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            # 3.2 生成等价组唯一标识（数字ID，比元组操作更快）
            data_clean['group_id'] = data_clean.groupby(qids).ngroup()
            total_groups = data_clean['group_id'].nunique()
            if total_groups == 0:
                return json.dumps({
                    "error": "未生成任何等价组（可能准标识符取值无重复）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            # 3.3 批量计算组内隐私属性频率（向量化操作）
            # 3.3.1 统计组内各隐私值的计数
            group_sa_count = data_clean.groupby(['group_id', sensitive_attr]).size().reset_index(name='count')
            # 3.3.2 计算组内总记录数
            group_total = data_clean.groupby('group_id').size().reset_index(name='total_records')
            # 3.3.3 计算归一化频率（保留4位小数）
            group_sa_freq = pd.merge(group_sa_count, group_total, on='group_id')
            group_sa_freq['freq'] = (group_sa_freq['count'] / group_sa_freq['total_records']).round(4)
            
            # 3.4 批量获取每组的max/min频率及对应隐私值（优化lambda兼容性）
            def get_freq_val(group, freq_col, agg_type, sa_col):
                if agg_type == 'max':
                    idx = group[freq_col].idxmax()
                else:
                    idx = group[freq_col].idxmin()
                return pd.Series([group.loc[idx, freq_col], group.loc[idx, sa_col]], index=[f'{agg_type}_freq', f'{agg_type}_freq_val'])
            
            # 移除include_groups=False，兼容低版本pandas
            freq_agg = group_sa_freq.groupby('group_id').apply(
                lambda x: get_freq_val(x, 'freq', freq_type, sensitive_attr)
            ).reset_index()
            
            # 3.5 生成等价组基础信息（QID组合、组名等）
            group_base = data_clean.groupby('group_id').agg(
                group_key=('group_id', lambda x: tuple(data_clean.loc[x.index[0], qids].values)),
                group_key_str=('group_id', lambda x: '_'.join(map(str, data_clean.loc[x.index[0], qids].values))),
                group_name=('group_id', lambda x: f"组[{', '.join([f'{k}={v}' for k, v in zip(qids, data_clean.loc[x.index[0], qids].values)])}")
            ).reset_index()
            
            # 3.6 合并所有组信息（生成最终的组列表）
            group_freq_info = pd.merge(group_base, group_total, on='group_id')
            group_freq_info = pd.merge(group_freq_info, freq_agg, on='group_id')
            
            # 修复：生成隐私值-频率字典（用agg+list替代apply，避免兼容性问题）
            group_sa_dict = group_sa_freq.groupby('group_id').agg(
                sensitive_vals=(sensitive_attr, lambda x: x.astype(str).tolist()),
                sensitive_freqs=('freq', lambda x: x.tolist())
            ).reset_index()
            # 构建键值对字典
            group_sa_dict['sensitive_freq'] = group_sa_dict.apply(
                lambda row: dict(zip(row['sensitive_vals'], row['sensitive_freqs'])), axis=1
            )
            # 只保留需要的列
            group_sa_dict = group_sa_dict[['group_id', 'sensitive_freq']]
            # 合并到主表
            group_freq_info = pd.merge(group_freq_info, group_sa_dict, on='group_id')
            
            # --------------------------
            # 4. 筛选目标等价组（优化：用nlargest/nsmallest替代sorted）
            # --------------------------
            if freq_type == "max":
                target_groups = group_freq_info.nlargest(min(n, total_groups), 'max_freq').to_dict('records')
            else:
                target_groups = group_freq_info.nsmallest(min(n, total_groups), 'min_freq').to_dict('records')
            
            if not target_groups:
                return json.dumps({
                    "error": f"筛选后无可用组（请求数量{n}，实际可用组{total_groups}）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # --------------------------
            # 5. 生成知识图谱节点（优化：集合去重+专用映射表）
            # --------------------------
            nodes = []
            node_set = set()  # 节点去重：存储"类型::名称"字符串
            # 预定义专用映射表（后续生成边时直接调用）
            qid_node_map = {}  # key: (QID类型, QID值) → value: 节点ID
            group_node_map = {}  # key: 组名 → value: 节点ID
            sa_node_map = {}    # key: (SA类型, SA值) → value: 节点ID

            # 5.1 第一层：QID值节点
            for group in target_groups:
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    node_type = f"QID_{qid_name}"
                    node_name = str(qid_val)
                    node_key_str = f"{node_type}::{node_name}"
                    
                    if node_key_str in node_set:
                        continue
                    
                    # 简化节点ID生成（用hash替代md5，减少计算量）
                    node_id = abs(hash(node_key_str)) % 1000000
                    # 计算节点大小（基于该QID值在目标组中的出现次数）
                    qid_occur_count = sum(1 for g in target_groups if str(g["group_key"][qids.index(qid_name)]) == node_name)
                    symbol_size = 25 + min(qid_occur_count * 5, 40)
                    
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"准标识符[{qid_name}]的取值"
                    })
                    node_set.add(node_key_str)
                    qid_node_map[(node_type, node_name)] = node_id

            # 5.2 第二层：等价组节点
            for group in target_groups:
                node_type = "equivalence_group"
                node_name = group["group_name"]
                node_key_str = f"{node_type}::{node_name}"
                
                if node_key_str in node_set:
                    continue
                
                node_id = abs(hash(f"group::{group['group_key_str']}")) % 1000000
                symbol_size = 35 + min(group["total_records"] * 3, 60)
                
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": symbol_size,
                    "desc": f"记录数：{group['total_records']} | 隐私值{freq_type}频率：{group[f'{freq_type}_freq']}({group[f'{freq_type}_freq_val']})"
                })
                node_set.add(node_key_str)
                group_node_map[node_name] = node_id

            # 5.3 第三层：隐私属性节点
            for group in target_groups:
                node_type = f"SA_{sensitive_attr}"
                for sen_val, freq in group["sensitive_freq"].items():
                    node_name = str(sen_val)
                    node_key_str = f"{node_type}::{node_name}"
                    
                    if node_key_str in node_set:
                        continue
                    
                    node_id = abs(hash(node_key_str)) % 1000000
                    # 计算节点大小（基于该隐私值在目标组中的平均频率）
                    avg_freq = sum(g["sensitive_freq"].get(sen_val, 0.0) for g in target_groups) / len(target_groups)
                    symbol_size = 25 + min(int(avg_freq * 50), 40)
                    
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"隐私属性[{sensitive_attr}] | 平均频率：{avg_freq:.4f}"
                    })
                    node_set.add(node_key_str)
                    sa_node_map[(node_type, node_name)] = node_id

            # --------------------------
            # 6. 生成知识图谱边（优化：用专用映射表替代全局node_map查询）
            # --------------------------
            links = []
            link_set = set()  # 边去重：存储"源ID::目标ID::边类型"字符串

            # 6.1 边1：QID值节点 → 等价组节点
            for group in target_groups:
                group_name = group["group_name"]
                eq_node_id = group_node_map.get(group_name)
                if not eq_node_id:
                    continue
                
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    qid_node_type = f"QID_{qid_name}"
                    qid_node_name = str(qid_val)
                    qid_node_id = qid_node_map.get((qid_node_type, qid_node_name))
                    if not qid_node_id:
                        continue
                    
                    link_key_str = f"{qid_node_id}::{eq_node_id}::QID_to_Group"
                    if link_key_str in link_set:
                        continue
                    
                    links.append({
                        "scene": scene,
                        "source": qid_node_id,
                        "target": eq_node_id,
                        "label": {"show": True, "formatter": qid_name},
                        "link_type": "QID_to_Group"
                    })
                    link_set.add(link_key_str)

            # 6.2 边2：等价组节点 → 隐私属性节点
            for group in target_groups:
                group_name = group["group_name"]
                eq_node_id = group_node_map.get(group_name)
                if not eq_node_id:
                    continue
                
                sa_node_type = f"SA_{sensitive_attr}"
                for sen_val, freq in group["sensitive_freq"].items():
                    sa_node_name = str(sen_val)
                    sa_node_id = sa_node_map.get((sa_node_type, sa_node_name))
                    if not sa_node_id:
                        continue
                    
                    link_key_str = f"{eq_node_id}::{sa_node_id}::Group_to_SA"
                    if link_key_str in link_set:
                        continue
                    
                    links.append({
                        "scene": scene,
                        "source": eq_node_id,
                        "target": sa_node_id,
                        "label": {"show": True, "formatter": f"频率:{freq}"},
                        "link_type": "Group_to_SA"
                    })
                    link_set.add(link_key_str)

            # --------------------------
            # 7. 返回最终图谱（保留原逻辑）
            # --------------------------
            final_graph = {
                "nodes": nodes,
                "links": links
            }

            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str

        except Exception as e:
            # 打印详细错误信息，方便调试
            import traceback
            error_detail = traceback.format_exc()
            print(f"报错详情：{error_detail}")
            return json.dumps({
                "error": f"生成图谱失败：{str(e)}",
                "error_detail": error_detail,
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
    
    ### α -Diversity
    def buildTopSensitiveRatioGroups(self, n: int = 20, sensitive_attr: str = None, graph_id: int = 11) -> str:
        """
        构建敏感属性值分布最高比例的等价组图谱
        不考虑alpha阈值，仅获取比例最高的n个等价组
        """
        try:
            # 获取数据
            data = self._Function_Data()
            qids = self.QIDs
            scene = getattr(self, 'scene', '')
            
            # 处理目标敏感属性
            if sensitive_attr is None:
                if not self.SA:
                    return json.dumps({
                        "error": "未设置敏感属性（self.SA为空）",
                        "nodes": [],
                        "links": []
                    }, ensure_ascii=False)
                sensitive_attr = self.SA[0]

            # 基础校验
            if not qids:
                return json.dumps({
                    "error": "未设置准标识符（self.QIDs为空）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
                
            required_cols = qids + [sensitive_attr]
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                return json.dumps({
                    "error": f"数据缺少必要列：{', '.join(missing_cols)}",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 按准标识符分组
            grouped = data.groupby(qids, sort=False)
            group_info = []
            
            for group_key, group_data in grouped:
                # 计算敏感属性各取值的占比
                sensitive_vals_clean = group_data[sensitive_attr].dropna()
                if pd.api.types.is_string_dtype(sensitive_vals_clean):
                    sensitive_ratio = sensitive_vals_clean.astype(str).value_counts(normalize=True).round(4)
                else:
                    sensitive_ratio = sensitive_vals_clean.value_counts(normalize=True).round(4)
                ratio_dict = sensitive_ratio.to_dict()
                
                # 提取最高占比
                max_ratio = sensitive_ratio.max() if not sensitive_ratio.empty else 0.0
                max_ratio_val = sensitive_ratio.idxmax() if not sensitive_ratio.empty else None

                group_info.append({
                    "group_key": group_key,
                    "group_key_str": "_".join(map(str, group_key)) if isinstance(group_key, (list, tuple)) else str(group_key),
                    "group_name": f"组[{', '.join([f'{k}={v}' for k, v in zip(qids, group_key)])}]" if isinstance(group_key, (list, tuple)) else f"组[{qids[0]}={group_key}]",
                    "total_records": len(group_data),
                    "sensitive_ratio": ratio_dict,
                    "max_sensitive_ratio": max_ratio,
                    "max_ratio_val": max_ratio_val
                })
            
            if not group_info:
                return json.dumps({
                    "error": "未生成任何等价组（准标识符取值无重复）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 按最高占比排序，取前n个组
            sorted_groups = sorted(group_info, key=lambda x: x["max_sensitive_ratio"], reverse=True)
            target_groups = sorted_groups[:min(n, len(sorted_groups))]

            # 生成节点
            nodes = []
            node_map = {}
            
            # QID值节点
            for group in target_groups:
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    node_type = f"QID_{qid_name}"
                    node_name = str(qid_val)
                    node_key = (node_type, node_name)
                    
                    if node_key in node_map:
                        continue
                    
                    node_id = int(hashlib.md5(f"{node_type}::{node_name}".encode()).hexdigest(), 16) % 1000000
                    qid_occur = sum(1 for g in target_groups if str(g["group_key"][qids.index(qid_name)]) == node_name)
                    symbol_size = 25 + min(qid_occur * 5, 40)
                    
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"准标识符[{qid_name}]的取值"
                    })
                    node_map[node_key] = node_id
            
            # 等价组节点
            for group in target_groups:
                node_type = "equivalence_group"
                node_name = group["group_name"]
                node_key = (node_type, node_name)
                
                if node_key in node_map:
                    continue
                
                node_id = int(hashlib.md5(f"group::{group['group_key_str']}".encode()).hexdigest(), 16) % 1000000
                symbol_size = 35 + min(group["total_records"] * 3, 60)
                desc = f"记录数：{group['total_records']} | 敏感值最高占比：{group['max_sensitive_ratio']}({group['max_ratio_val']})"
                
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": symbol_size,
                    "desc": desc,
                    "max_sensitive_ratio": group["max_sensitive_ratio"]
                })
                node_map[node_key] = node_id
            
            # 敏感属性节点
            for group in target_groups:
                node_type = f"SA_{sensitive_attr}"
                for sen_val, ratio in group["sensitive_ratio"].items():
                    node_name = str(sen_val)
                    node_key = (node_type, node_name)
                    
                    if node_key in node_map:
                        continue
                    
                    node_id = int(hashlib.md5(f"{node_type}::{node_name}".encode()).hexdigest(), 16) % 1000000
                    avg_ratio = sum(g["sensitive_ratio"].get(sen_val, 0.0) for g in target_groups) / len(target_groups)
                    symbol_size = 25 + min(int(avg_ratio * 50), 40)
                    is_max_ratio_val = (sen_val == group["max_ratio_val"])
                    
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"敏感属性[{sensitive_attr}] | 平均占比：{avg_ratio:.4f} | 最高占比值：{'是' if is_max_ratio_val else '否'}"
                    })
                    node_map[node_key] = node_id

            # 生成边
            links = []
            link_set = set()
            
            # QID值节点 → 等价组节点
            for group in target_groups:
                eq_node_key = ("equivalence_group", group["group_name"])
                eq_node_id = node_map.get(eq_node_key)
                if not eq_node_id:
                    continue
                
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    qid_node_key = (f"QID_{qid_name}", str(qid_val))
                    qid_node_id = node_map.get(qid_node_key)
                    if not qid_node_id:
                        continue
                    
                    link_key = (qid_node_id, eq_node_id, "QID_to_Group")
                    if link_key in link_set:
                        continue
                    
                    links.append({
                        "scene": scene,
                        "source": qid_node_id,
                        "target": eq_node_id,
                        "label": {"show": True, "formatter": qid_name},
                        "link_type": "QID_to_Group"
                    })
                    link_set.add(link_key)
            
            # 等价组节点 → 敏感属性节点
            for group in target_groups:
                eq_node_key = ("equivalence_group", group["group_name"])
                eq_node_id = node_map.get(eq_node_key)
                if not eq_node_id:
                    continue
                
                sa_node_type = f"SA_{sensitive_attr}"
                for sen_val, ratio in group["sensitive_ratio"].items():
                    sa_node_key = (sa_node_type, str(sen_val))
                    sa_node_id = node_map.get(sa_node_key)
                    if not sa_node_id:
                        continue
                    
                    link_key = (eq_node_id, sa_node_id, "Group_to_SA")
                    if link_key in link_set:
                        continue
                    
                    label_formatter = f"占比:{ratio}" + ("(最高)" if sen_val == group["max_ratio_val"] else "")
                    
                    links.append({
                        "scene": scene,
                        "source": eq_node_id,
                        "target": sa_node_id,
                        "label": {"show": True, "formatter": label_formatter},
                        "link_type": "Group_to_SA",
                        "sensitive_ratio": ratio
                    })
                    link_set.add(link_key)
            # 7. 返回仅包含nodes和links的结果
            final_graph = {
                "nodes": nodes,
                "links": links
            }

            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            return json.dumps({
                "error": f"生成(∝,k)-匿名组图谱失败：{str(e)}",
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
    
    ### 数据可辨别度
    def buildTopCDMContributionGroups(self, n: int = 20, graph_id: int = 7) -> str:
        """
        构建数据可辨别度（CDM）计算相关的图谱，展示等价组、准标识符、总记录数的关系
        保留与 buildTopSensitiveRatioGroups 一致的节点和边存储格式
        :param n: 最多展示的等价组数量（按等价组大小降序取前n个）
        :return: 包含nodes和links的JSON字符串
        """
        try:
            # 1. 获取基础数据
            data = self._Function_Data()  # 主表数据
            qids = self.QIDs  # 准标识符列表
            scene = getattr(self, 'scene', '')  # 数据场景
            total_records = self._Num_address(data)  # 总记录数 N

            # 2. 基础校验
            if not qids:
                return json.dumps({
                    "error": "未设置准标识符（self.QIDs为空）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            if not all(qid in data.columns for qid in qids):
                missing = [qid for qid in qids if qid not in data.columns]
                return json.dumps({
                    "error": f"数据缺少准标识符列：{', '.join(missing)}",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 3. 按准标识符分组，计算等价组大小（用于CDM计算）
            grouped = data.groupby(qids, sort=False)
            group_info = []
            for group_key, group_data in grouped:
                group_size = len(group_data)  # 等价组大小 k_i
                group_key_str = "_".join(map(str, group_key)) if isinstance(group_key, (list, tuple)) else str(group_key)
                group_name = f"组[{', '.join([f'{k}={v}' for k, v in zip(qids, group_key)])}]" if isinstance(group_key, (list, tuple)) else f"组[{qids[0]}={group_key}]"
                
                # 计算该组对CDM的贡献（k_i²）
                cdm_contribution = group_size **2
                
                group_info.append({
                    "group_key": group_key,
                    "group_key_str": group_key_str,
                    "group_name": group_name,
                    "size": group_size,  # 等价组大小 k_i
                    "cdm_contribution": cdm_contribution  # 对CDM的贡献值
                })
            
            if not group_info:
                return json.dumps({
                    "error": "未生成任何等价组（准标识符取值无重复）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 4. 按等价组大小降序，取前n个组（突出对CDM影响较大的组）
            sorted_groups = sorted(group_info, key=lambda x: x["size"], reverse=True)
            target_groups = sorted_groups[:min(n, len(sorted_groups))]

            # 5. 生成节点（保持与敏感属性图谱一致的结构）
            nodes = []
            node_map = {}  # 用于去重：(node_type, node_name) → node_id

            # 5.1 准标识符值节点（QID值）
            for group in target_groups:
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    node_type = f"QID_{qid_name}"  # 如 "QID_age"
                    node_name = str(qid_val)
                    node_key = (node_type, node_name)
                    
                    if node_key in node_map:
                        continue  # 避免重复节点
                    
                    # 生成唯一ID（哈希取模确保长度可控）
                    node_id = int(hashlib.md5(f"{node_type}::{node_name}".encode()).hexdigest(), 16) % 1000000
                    # 节点大小：出现次数越多，尺寸越大
                    occur_count = sum(1 for g in target_groups if str(g["group_key"][qids.index(qid_name)]) == node_name)
                    symbol_size = 25 + min(occur_count * 5, 40)  # 控制最大尺寸
                    
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": node_name,
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"准标识符[{qid_name}]的取值"
                    })
                    node_map[node_key] = node_id

            # 5.2 等价组节点（核心计算单元）
            for group in target_groups:
                node_type = "equivalence_group"
                node_name = group["group_name"]
                node_key = (node_type, node_name)
                
                if node_key in node_map:
                    continue
                
                node_id = int(hashlib.md5(f"group::{group['group_key_str']}".encode()).hexdigest(), 16) % 1000000
                # 节点大小：等价组越大，尺寸越大（体现对CDM的影响）
                symbol_size = 35 + min(group["size"] * 2, 60)  # 控制最大尺寸
                # 描述包含CDM贡献信息
                desc = (f"等价组大小：{group['size']}条记录 | "
                        f"CDM贡献（k_i²）：{group['cdm_contribution']}")
                
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": symbol_size,
                    "desc": desc,
                    "size": group["size"],  # 等价组大小
                    "cdm_contribution": group["cdm_contribution"]  # CDM贡献值
                })
                node_map[node_key] = node_id

            # 5.3 总记录数节点（N，用于归一化）
            node_type = "total_records"
            node_name = f"N={total_records}"
            node_key = (node_type, node_name)
            
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"total::{total_records}".encode()).hexdigest(), 16) % 1000000
                # 固定较大尺寸，突出重要性
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": 50,
                    "desc": f"总记录数（用于CDM归一化：/N²）"
                })
                node_map[node_key] = node_id

            # 5.4 CDM结果节点（最终计算值）
            cdm_value = sum(g["cdm_contribution"] for g in group_info) / (total_records** 2)  # 完整CDM计算
            node_type = "cdm_result"
            node_name = f"CDM={cdm_value:.4f}"
            node_key = (node_type, node_name)
            
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"cdm::{cdm_value}".encode()).hexdigest(), 16) % 1000000
                # 固定尺寸，突出结果
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": node_name,
                    "scene": scene,
                    "symbolSize": 55,
                    "desc": f"数据可辨别度：Σ(k_i²)/N² = {cdm_value:.4f}"
                })
                node_map[node_key] = node_id

            # 6. 生成边（关联节点，保持一致格式）
            links = []
            link_set = set()  # 用于去重：(source, target, link_type)

            # 6.1 准标识符值 → 等价组（QID值组合形成等价组）
            for group in target_groups:
                eq_node_key = ("equivalence_group", group["group_name"])
                eq_node_id = node_map.get(eq_node_key)
                if not eq_node_id:
                    continue
                
                group_key = group["group_key"]
                for qid_name, qid_val in zip(qids, group_key):
                    qid_node_key = (f"QID_{qid_name}", str(qid_val))
                    qid_node_id = node_map.get(qid_node_key)
                    if not qid_node_id:
                        continue
                    
                    link_key = (qid_node_id, eq_node_id, "QID_to_Group")
                    if link_key in link_set:
                        continue
                    
                    links.append({
                        "scene": scene,
                        "source": qid_node_id,
                        "target": eq_node_id,
                        "label": {"show": True, "formatter": qid_name},  # 显示准标识符名称
                        "link_type": "QID_to_Group"
                    })
                    link_set.add(link_key)

            # 6.2 等价组 → 总记录数（等价组属于总数据集）
            total_node_key = ("total_records", f"N={total_records}")
            total_node_id = node_map.get(total_node_key)
            if total_node_id:
                for group in target_groups:
                    eq_node_key = ("equivalence_group", group["group_name"])
                    eq_node_id = node_map.get(eq_node_key)
                    if not eq_node_id:
                        continue
                    
                    link_key = (eq_node_id, total_node_id, "Group_to_Total")
                    if link_key in link_set:
                        continue
                    
                    links.append({
                        "scene": scene,
                        "source": eq_node_id,
                        "target": total_node_id,
                        "label": {"show": True, "formatter": "属于总集"},
                        "link_type": "Group_to_Total"
                    })
                    link_set.add(link_key)

            # 6.3 总记录数 → CDM结果（用于归一化计算）
            cdm_node_key = ("cdm_result", f"CDM={cdm_value:.4f}")
            cdm_node_id = node_map.get(cdm_node_key)
            if total_node_id and cdm_node_id:
                link_key = (total_node_id, cdm_node_id, "Total_to_CDM")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": total_node_id,
                        "target": cdm_node_id,
                        "label": {"show": True, "formatter": "分母(N²)"},
                        "link_type": "Total_to_CDM"
                    })
                    link_set.add(link_key)

            # 6.4 等价组 → CDM结果（贡献分子部分）
            if cdm_node_id:
                for group in target_groups:
                    eq_node_key = ("equivalence_group", group["group_name"])
                    eq_node_id = node_map.get(eq_node_key)
                    if not eq_node_id:
                        continue
                    
                    link_key = (eq_node_id, cdm_node_id, "Group_to_CDM")
                    if link_key in link_set:
                        continue
                    
                    # 显示该组对CDM的贡献值
                    label_formatter = f"贡献{group['cdm_contribution']}"
                    links.append({
                        "scene": scene,
                        "source": eq_node_id,
                        "target": cdm_node_id,
                        "label": {"show": True, "formatter": label_formatter},
                        "link_type": "Group_to_CDM",
                        "contribution": group["cdm_contribution"]  # 携带贡献值
                    })
                    link_set.add(link_key)

            # 7. 返回最终图谱数据
            final_graph = {
                "nodes": nodes,
                "links": links
            }
            
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:  # 补充except子句，修复try语句错误
            return json.dumps({
                "error": f"生成CDM图谱失败：{str(e)}",
                "nodes": [],
                "links": []
            }, ensure_ascii=False)

    ### 数据记录匿名率
    def buildSupRatioGraph(self, graph_id: int = 10) -> str:
        """
        构建记录隐匿率（SupRatio）计算相关的图谱
        基于单表数据，展示目标表、原表总记录数、隐匿记录数与隐匿率结果的依赖关系
        :return: 包含nodes和links的JSON字符串（格式对齐buildTopSensitiveRatioGroups）
        """
        try:
            # 1. 基础数据与参数获取
            _TemAll = self._Function_Data()  # 读取单表原始数据（DataFrame）
            scene = getattr(self, 'scene', '默认数据场景')  # 数据场景（取自Config类属性）
            original_table_name = self.json_address  # 目标表名（脱敏后表名，即单表名）
            
            # 2. 核心数据计算与校验
            # 2.1 原表总记录数（N）
            original_total_count = self._Num_address(_TemAll)
            if original_total_count <= 0:
                return json.dumps({
                    "error": "原表总记录数不能为0或负数",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            # 2.2 隐匿记录数（S = self.n_s）
            hidden_record_count = self.n_s
            if hidden_record_count < 0:
                return json.dumps({
                    "error": "隐匿记录数不能为负数",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            
            # 2.3 计算记录隐匿率（SupRatio = S/(N+S)）
            sup_ratio = hidden_record_count / (original_total_count + hidden_record_count)
            sup_ratio_rounded = round(sup_ratio, 4)  # 保留4位小数，便于展示

            # 3. 生成节点（含去重逻辑，对齐原函数node_map设计）
            nodes = []
            node_map = {}  # 键：(节点类型, 核心标识)，值：节点id，避免重复
            hash_base = f"{original_table_name}_{scene}"  # 哈希基础值，确保多场景唯一

            # 3.1 目标表节点（数据载体）
            node_type = "original_table"
            node_core_key = original_table_name
            node_key = (node_type, node_core_key)
            if node_key not in node_map:
                # 生成唯一id（哈希取模控制长度，避免溢出）
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{node_core_key}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": original_table_name,
                    "scene": scene,
                    "symbolSize": 40,  # 固定尺寸，突出数据来源地位
                    "desc": f"记录隐匿率计算的数据源单表，存储原始数据",
                    "table_name": original_table_name  # 携带表名属性，便于后续复用
                })
                node_map[node_key] = node_id

            # 3.2 原表总记录数节点（统计值）
            node_type = "original_total_count"
            node_core_key = original_total_count
            node_key = (node_type, node_core_key)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{node_core_key}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"原表总记录数={original_total_count}",
                    "scene": scene,
                    "symbolSize": 35,  # 小于表节点，体现层级关系
                    "desc": f"从{original_table_name}表中统计的原始记录总数（N）",
                    "count": original_total_count  # 携带原始统计值
                })
                node_map[node_key] = node_id

            # 3.3 隐匿记录数节点（统计值）
            node_type = "hidden_record_count"
            node_core_key = hidden_record_count
            node_key = (node_type, node_core_key)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{node_core_key}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"隐匿记录数={hidden_record_count}",
                    "scene": scene,
                    "symbolSize": 35,  # 与原表总记录数节点尺寸一致
                    "desc": f"{original_table_name}表中未被脱敏发布的记录数（S）",
                    "hidden_count": hidden_record_count  # 携带隐匿记录数
                })
                node_map[node_key] = node_id

            # 3.4 记录隐匿率结果节点（计算值）
            node_type = "sup_ratio_result"
            node_core_key = sup_ratio_rounded
            node_key = (node_type, node_core_key)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{node_core_key}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"记录隐匿率={sup_ratio_rounded}",
                    "scene": scene,
                    "symbolSize": 50,  # 最大尺寸，突出结果重要性
                    "desc": f"记录隐匿率=S/(N+S) = {hidden_record_count}/({original_total_count}+{hidden_record_count}) = {sup_ratio_rounded}",
                    "ratio": sup_ratio_rounded,  # 携带最终计算结果
                    "calculation_formula": "隐匿记录数/(原表总记录数+隐匿记录数)"  # 携带计算公式
                })
                node_map[node_key] = node_id

            # 4. 生成边（关联节点，对齐原函数link_set去重逻辑）
            links = []
            link_set = set()  # 键：(源节点id, 目标节点id, 边类型)，避免重复边

            # 4.1 目标表 → 原表总记录数（统计依赖：表是总记录数的来源）
            source_key = ("original_table", original_table_name)
            target_key = ("original_total_count", original_total_count)
            if source_key in node_map and target_key in node_map:
                source_id = node_map[source_key]
                target_id = node_map[target_key]
                link_key = (source_id, target_id, "table_to_original_count")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "统计总记录数"},  # 显示边含义
                        "link_type": "table_to_original_count",
                        "desc": f"从{original_table_name}表统计得到原表总记录数"
                    })
                    link_set.add(link_key)

            # 4.2 原表总记录数 → 隐匿率结果（计算依赖：总记录数参与分母计算）
            source_key = ("original_total_count", original_total_count)
            target_key = ("sup_ratio_result", sup_ratio_rounded)
            if source_key in node_map and target_key in node_map:
                source_id = node_map[source_key]
                target_id = node_map[target_key]
                link_key = (source_id, target_id, "original_count_to_ratio")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "分母组成（+S）"},  # 明确计算角色
                        "link_type": "original_count_to_ratio",
                        "desc": f"原表总记录数作为隐匿率计算的分母组成部分"
                    })
                    link_set.add(link_key)

            # 4.3 隐匿记录数 → 隐匿率结果（计算依赖：隐匿数既是分子也是分母组成）
            source_key = ("hidden_record_count", hidden_record_count)
            target_key = ("sup_ratio_result", sup_ratio_rounded)
            if source_key in node_map and target_key in node_map:
                source_id = node_map[source_key]
                target_id = node_map[target_key]
                link_key = (source_id, target_id, "hidden_count_to_ratio")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "分子/分母组成（+N）"},  # 明确双重角色
                        "link_type": "hidden_count_to_ratio",
                        "desc": f"隐匿记录数既是隐匿率计算的分子，也是分母组成部分"
                    })
                    link_set.add(link_key)

            # 5. 组装最终图谱数据并返回
            final_graph = {
                "nodes": nodes,
                "links": links,
            }
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            # 统一错误处理，格式与原函数一致
            return json.dumps({
                "error": f"生成记录隐匿率图谱失败：{str(e)}",
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
        
    ### 平均泛化程度
    def buildAvgGeneralizationDegreeGraph(self, top_n_classes: int = 20, graph_id: int = 15) -> str:
        """
        构建平均泛化程度（平均等价类大小）计算相关的图谱
        基于单表数据，展示目标表、总记录数、等价类、等价类数量与结果的依赖关系
        :param top_n_classes: 展示的典型等价类数量（默认前5个）
        :return: 包含nodes和links的JSON字符串
        """
        try:
            # 1. 获取基础数据
            _TemAll = self._Function_Data()  # 单表数据
            series_quasi = self._Function_Series_quasi(_TemAll)  # 等价类及大小（Series_quasi）
            scene = getattr(self, 'scene', '默认场景')
            table_name = self.json_address  # 目标表名
            qids = self.QIDs  # 准标识符列表

            # 2. 核心数据计算与校验
            total_record_count = self._Num_address(_TemAll)  # 总记录数 N
            equivalence_class_count = len(series_quasi)  # 等价类数量 M
            if total_record_count <= 0:
                return json.dumps({"error": "总记录数不能为0或负数", "nodes": [], "links": []}, ensure_ascii=False)
            if equivalence_class_count <= 0:
                return json.dumps({"error": "等价类数量不能为0或负数", "nodes": [], "links": []}, ensure_ascii=False)
            avg_degree = round(total_record_count / equivalence_class_count, 4)  # 平均泛化程度 R

            # 3. 生成节点（含去重逻辑）
            nodes = []
            node_map = {}  # 键：(类型, 核心标识)，值：id
            hash_base = f"{table_name}_{scene}"

            # 3.1 目标表节点
            node_type = "original_table"
            node_key = (node_type, table_name)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": table_name,
                    "scene": scene,
                    "symbolSize": 40,
                    "desc": "存储原始数据的单表，平均泛化程度计算的数据源"
                })
                node_map[node_key] = node_id

            # 3.2 总记录数节点
            node_type = "total_record_count"
            node_key = (node_type, total_record_count)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{total_record_count}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"总记录数={total_record_count}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"从{table_name}表中统计的所有记录总数（N）",
                    "count": total_record_count
                })
                node_map[node_key] = node_id

            # 3.3 等价类数量节点
            node_type = "equivalence_class_count"
            node_key = (node_type, equivalence_class_count)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{equivalence_class_count}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"等价类数量={equivalence_class_count}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"按准标识符{qids}分组后的等价组总数（M）",
                    "class_count": equivalence_class_count
                })
                node_map[node_key] = node_id

            # 3.4 典型等价类节点（展示top_n_classes个）
            top_classes = series_quasi.head(top_n_classes)  # 取前N个等价类
            for idx, (group_key, group_size) in enumerate(top_classes.items(), 1):
                node_type = "equivalence_class"
                # 格式化等价类名称（如 "age=25, gender=男"）
                if isinstance(group_key, tuple):
                    group_name = f"等价组{idx}[{', '.join([f'{k}={v}' for k, v in zip(qids, group_key)])}]"
                else:
                    group_name = f"等价组{idx}[{qids[0]}={group_key}]"
                node_key = (node_type, group_name)
                if node_key not in node_map:
                    node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{group_name}".encode()).hexdigest(), 16) % 1000000
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": group_name,
                        "scene": scene,
                        "symbolSize": 30 + min(group_size // 5, 20),  # 尺寸随组大小变化
                        "desc": f"准标识符组合对应的等价组，包含{group_size}条记录",
                        "size": group_size  # 组内记录数
                    })
                    node_map[node_key] = node_id

            # 3.5 平均泛化程度结果节点
            node_type = "avg_generalization_degree"
            node_key = (node_type, avg_degree)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{avg_degree}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"平均泛化程度={avg_degree}",
                    "scene": scene,
                    "symbolSize": 50,
                    "desc": f"平均等价类大小=总记录数/等价类数量={total_record_count}/{equivalence_class_count}={avg_degree}",
                    "degree": avg_degree,
                    "formula": "平均泛化程度 = 总记录数 / 等价类数量"
                })
                node_map[node_key] = node_id

            # 4. 生成边（含去重逻辑）
            links = []
            link_set = set()  # 键：(source_id, target_id, link_type)

            # 4.1 目标表 → 总记录数（统计总记录数）
            source_key = ("original_table", table_name)
            target_key = ("total_record_count", total_record_count)
            if source_key in node_map and target_key in node_map:
                source_id, target_id = node_map[source_key], node_map[target_key]
                link_key = (source_id, target_id, "table_to_total_count")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "统计总记录数"},
                        "link_type": "table_to_total_count"
                    })
                    link_set.add(link_key)

            # 4.2 目标表 → 典型等价类（生成等价组）
            source_key = ("original_table", table_name)
            for (node_type, group_name), class_node_id in node_map.items():
                if node_type != "equivalence_class":
                    continue
                target_key = (node_type, group_name)
                if target_key not in node_map:
                    continue
                source_id = node_map[source_key]
                target_id = class_node_id
                link_key = (source_id, target_id, "table_to_equivalence_class")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "生成等价组"},
                        "link_type": "table_to_equivalence_class"
                    })
                    link_set.add(link_key)

            # 4.3 典型等价类 → 等价类数量（计入总数）
            target_key = ("equivalence_class_count", equivalence_class_count)
            if target_key in node_map:
                target_id = node_map[target_key]
                for (node_type, group_name), class_node_id in node_map.items():
                    if node_type != "equivalence_class":
                        continue
                    source_id = class_node_id
                    link_key = (source_id, target_id, "class_to_class_count")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": source_id,
                            "target": target_id,
                            "label": {"show": True, "formatter": "计入总数"},
                            "link_type": "class_to_class_count"
                        })
                        link_set.add(link_key)

            # 4.4 总记录数 → 平均泛化程度（分子）
            source_key = ("total_record_count", total_record_count)
            target_key = ("avg_generalization_degree", avg_degree)
            if source_key in node_map and target_key in node_map:
                source_id, target_id = node_map[source_key], node_map[target_key]
                link_key = (source_id, target_id, "total_to_avg_degree")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "分子（总记录数）"},
                        "link_type": "total_to_avg_degree"
                    })
                    link_set.add(link_key)

            # 4.5 等价类数量 → 平均泛化程度（分母）
            source_key = ("equivalence_class_count", equivalence_class_count)
            target_key = ("avg_generalization_degree", avg_degree)
            if source_key in node_map and target_key in node_map:
                source_id, target_id = node_map[source_key], node_map[target_key]
                link_key = (source_id, target_id, "class_count_to_avg_degree")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": source_id,
                        "target": target_id,
                        "label": {"show": True, "formatter": "分母（等价类数量）"},
                        "link_type": "class_count_to_avg_degree"
                    })
                    link_set.add(link_key)

            # 5. 返回最终图谱
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            return json.dumps({
                "error": f"生成平均泛化程度图谱失败：{str(e)}",
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
    
    def buildAverageGeneralizationDegreeUsability(self, top_n_classes: int = 20, graph_id: int = 8) -> str:
        """
        构建平均泛化程度（Average Generalization Degree）可视化数据
        :param top_n_classes: 展示的典型等价类数量（默认前20个）
        :return: 包含nodes和links的JSON字符串
        """
        return self.buildAvgGeneralizationDegreeGraph(top_n_classes=top_n_classes, graph_id=8)

    ### 唯一记录占比（Get_Uniqueness）图谱构建
    def buildUniquenessRatioGraph(self, top_n_unique_equivalence: int = 20, graph_id: int = 12) -> str:
        """
        构建唯一记录占比（Get_Uniqueness）计算图谱，基于单表数据
        :param top_n_unique_equivalence: 展示的典型唯一等价组实例数量（默认前20个）
        :return: 含nodes/links的JSON字符串（格式对齐buildTopSensitiveRatioGroups）
        """
        try:
            # 1. 基础数据获取与校验（确保在try内部，缩进正确）
            _TemAll = self._Function_Data()  # 读取单表数据（DataFrame，继承自Config）
            series_quasi = self._Function_Series_quasi(_TemAll)  # 等价类及大小（升序排列）
            scene = getattr(self, 'scene', '通用数据场景')  # 从Config类获取场景
            table_name = self.json_address  # 目标表名（mysql表名，继承自Config）
            qids = self.QIDs  # 准标识符列表（继承自Config，如["age", "job", "city"]）

            # 核心计算（贴合Get_Uniqueness函数逻辑，修复NumPy类型转换）
            # 总记录数：转为Python原生int（避免NumPy int64序列化错误）
            total_record = int(self._Num_address(_TemAll))  
            unique_eq_count = 0  # 唯一等价组数量（分子）
            unique_eq_list = []  # 存储唯一等价组（组大小=1，含分组键和大小）

            # 遍历等价组：转换NumPy类型+收集唯一组（series_quasi升序排列）
            for eq_size, eq_key in zip(series_quasi.values, series_quasi.index):
                eq_size_py = int(eq_size)  # 关键：将NumPy int64转为Python int
                if eq_size_py == 1:
                    unique_eq_count += 1
                    unique_eq_list.append((eq_key, eq_size_py))  # 存储Python类型
                else:
                    break  # 升序排列，后续组大小均>1，终止循环

            # 合法性校验
            if total_record <= 0:
                return json.dumps({
                    "error": "总记录数不能为0或负数",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)
            # 计算唯一记录占比（确保除法结果为Python float）
            uniqueness_ratio = round(unique_eq_count / total_record, 4)

            # 2. 生成节点（含去重，修复变量未定义问题）
            nodes = []
            node_map = {}  # 键：(节点类型, 核心标识)，值：节点id（避免重复）
            hash_base = f"{table_name}_{scene}"  # 哈希基础：确保多表/多场景节点唯一

            # 2.1 目标表节点（数据源头）
            node_type = "original_table"
            node_key = (node_type, table_name)
            if node_key not in node_map:
                # 生成唯一id（哈希取模控制长度，避免溢出）
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": table_name,
                    "scene": scene,
                    "symbolSize": 40,  # 突出数据源头地位
                    "desc": f"唯一记录占比计算的数据源单表，含准标识符{qids}和敏感属性"
                })
                node_map[node_key] = node_id

            # 2.2 总记录数节点（公式分母）
            node_type = "total_record_count"
            node_key = (node_type, total_record)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"{node_type}::{hash_base}::{total_record}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"总记录数={total_record}",
                    "scene": scene,
                    "symbolSize": 35,  # 小于表节点，体现层级
                    "desc": f"从{table_name}表统计的所有原始记录总数（公式分母）",
                    "count": total_record  # 携带原始值，便于前端复用（Python int）
                })
                node_map[node_key] = node_id

            # 2.3 唯一等价组数量节点（公式分子）
            node_type = "unique_equivalence_count"
            node_key = (node_type, unique_eq_count)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"unique_eq_count::{hash_base}::{unique_eq_count}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"唯一等价组数量={unique_eq_count}",
                    "scene": scene,
                    "symbolSize": 35,  # 与总记录数节点尺寸一致
                    "desc": f"按准标识符{qids}分组后，组大小=1的等价组总数（公式分子）",
                    "unique_count": unique_eq_count  # 携带原始值（Python int）
                })
                node_map[node_key] = node_id

            # 2.4 典型唯一等价组实例节点（修复eq_name/node_key未定义，确保Python类型）
            # 取前N个唯一组（避免超出列表长度）
            top_unique_eq = unique_eq_list[:min(top_n_unique_equivalence, len(unique_eq_list))]
            for idx, (eq_key, eq_size) in enumerate(top_unique_eq, 1):
                # 格式化等价组名称（适配多准标识符/单准标识符场景）
                if isinstance(eq_key, tuple):
                    # 多准标识符：如(28, "工程师") → "唯一组1[age=28, job=工程师]"
                    eq_name = f"唯一组{idx}[{', '.join([f'{k}={v}' for k, v in zip(qids, eq_key)])}]"
                else:
                    # 单准标识符：如28 → "唯一组1[age=28]"
                    eq_name = f"唯一组{idx}[{qids[0]}={eq_key}]"
                
                # 定义节点键（避免重复）
                node_type = "unique_equivalence"
                node_key = (node_type, eq_name)
                if node_key not in node_map:
                    node_id = int(hashlib.md5(f"unique_eq_instance::{hash_base}::{eq_name}".encode()).hexdigest(), 16) % 1000000
                    nodes.append({
                        "category": node_type,
                        "id": node_id,
                        "name": eq_name,
                        "scene": scene,
                        "symbolSize": 30,  # 固定尺寸（组大小均=1）
                        "desc": f"准标识符分组实例，仅含1条记录（易被重识别）",
                        "size": eq_size  # 携带组大小（Python int，确保序列化）
                    })
                    node_map[node_key] = node_id

            # 2.5 唯一记录占比结果节点（突出最终结果）
            node_type = "uniqueness_ratio"
            node_key = (node_type, uniqueness_ratio)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"uniqueness_ratio::{hash_base}::{uniqueness_ratio}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": node_type,
                    "id": node_id,
                    "name": f"唯一记录占比={uniqueness_ratio}",
                    "scene": scene,
                    "symbolSize": 50,  # 最大尺寸，突出结果重要性
                    "desc": f"唯一记录占比=唯一等价组数量/总记录数={unique_eq_count}/{total_record}={uniqueness_ratio}",
                    "ratio": uniqueness_ratio,  # 携带结果（Python float）
                    "formula": "唯一记录占比 = 唯一等价组数量 ÷ 总记录数"  # 明确计算公式
                })
                node_map[node_key] = node_id

            # 3. 生成边（含去重，确保所有节点已定义）
            links = []
            link_set = set()  # 键：(源id, 目标id, 边类型) → 避免重复边

            # 3.1 目标表 → 总记录数（统计依赖：总记录数来自表）
            src_key = ("original_table", table_name)
            tgt_key = ("total_record_count", total_record)
            if src_key in node_map and tgt_key in node_map:
                src_id = node_map[src_key]
                tgt_id = node_map[tgt_key]
                link_key = (src_id, tgt_id, "table_to_total_count")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": "统计总记录数"},
                        "link_type": "table_to_total_count",
                        "desc": "从目标表统计得到总记录数"
                    })
                    link_set.add(link_key)

            # 3.2 目标表 → 唯一等价组实例（分组依赖：唯一组来自表）
            src_id = node_map[("original_table", table_name)]
            for (node_type, eq_name), eq_node_id in node_map.items():
                if node_type == "unique_equivalence":  # 仅匹配唯一组节点
                    link_key = (src_id, eq_node_id, "table_to_unique_equivalence")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": src_id,
                            "target": eq_node_id,
                            "label": {"show": True, "formatter": "生成唯一组"},
                            "link_type": "table_to_unique_equivalence",
                            "desc": "从目标表按准标识符分组得到唯一等价组"
                        })
                        link_set.add(link_key)

            # 3.3 唯一等价组实例 → 唯一等价组数量（统计依赖：总数来自实例）
            tgt_id = node_map[("unique_equivalence_count", unique_eq_count)]
            for (node_type, eq_name), eq_node_id in node_map.items():
                if node_type == "unique_equivalence":  # 仅匹配唯一组节点
                    link_key = (eq_node_id, tgt_id, "unique_eq_to_count")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": eq_node_id,
                            "target": tgt_id,
                            "label": {"show": True, "formatter": "计入唯一组总数"},
                            "link_type": "unique_eq_to_count",
                            "desc": "唯一等价组实例计入总数统计"
                        })
                        link_set.add(link_key)

            # 3.4 唯一等价组数量 → 唯一记录占比（计算依赖：分子）
            src_key = ("unique_equivalence_count", unique_eq_count)
            tgt_key = ("uniqueness_ratio", uniqueness_ratio)
            if src_key in node_map and tgt_key in node_map:
                src_id = node_map[src_key]
                tgt_id = node_map[tgt_key]
                link_key = (src_id, tgt_id, "unique_count_to_ratio")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": "分子（唯一组数量）"},
                        "link_type": "unique_count_to_ratio",
                        "desc": "唯一等价组数量作为公式分子"
                    })
                    link_set.add(link_key)

            # 3.5 总记录数 → 唯一记录占比（计算依赖：分母）
            src_key = ("total_record_count", total_record)
            tgt_key = ("uniqueness_ratio", uniqueness_ratio)
            if src_key in node_map and tgt_key in node_map:
                src_id = node_map[src_key]
                tgt_id = node_map[tgt_key]
                link_key = (src_id, tgt_id, "total_to_uniqueness_ratio")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": "分母（总记录数）"},
                        "link_type": "total_to_uniqueness_ratio",
                        "desc": "总记录数作为公式分母"
                    })
                    link_set.add(link_key)

            # 4. 组装图谱并返回（修复JSON序列化问题）
            final_graph = {
                "nodes": nodes,
                "links": links,
                "calculation_info": {  # 补充计算信息，便于前端展示
                    "formula": "唯一记录占比 = 唯一等价组数量 ÷ 总记录数",
                    "total_record": total_record,
                    "unique_equivalence_count": unique_eq_count,
                    "uniqueness_ratio": uniqueness_ratio
                }
            }
            # 序列化：确保所有值为Python原生类型（已修复）
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            # 统一错误格式，便于前端捕获
            error_msg = f"生成唯一记录占比图谱失败：{str(e)}"
            print(error_msg)  # 调试用：打印错误详情
            return json.dumps({
                "error": error_msg,
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
    

        """辅助函数：格式化等价组名称（适配多/单准标识符场景）"""
        if isinstance(eq_key, tuple):
            return ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
        else:
            return f"{qids[0]}={eq_key}"
        
    ### 数据损失度
    def buildNCPGraph(self, top_n_equivalence: int = 20, graph_id: int = 9) -> str:
        """
        构建NCP（归一化确定性惩罚）数据损失度图谱，内置_Thread_NI_Loss基础实现
        :param top_n_equivalence: 展示的典型等价组实例数量
        :return: 含nodes/links的JSON字符串
        """
        try:
            # 1. 基础数据获取与校验
            _TemAll = self._Function_Data()
            series_quasi = self._Function_Series_quasi(_TemAll)
            scene = self.scene
            table_name = self.json_address
            qids = self.address_Attr[0]  # 准标识符列表
            age_range = 100  # 年龄泛化范围（与原函数一致）

            # 2. 核心计算（含_Thread_NI_Loss内置实现）
            total_record = int(self._Num_address(_TemAll))  # 总记录数（转为Python int）
            qid_dimension = len(qids)  # 准标识符维度数
            if total_record <= 0 or qid_dimension <= 0:
                return json.dumps({
                    "error": "总记录数或准标识符维度数不能为0或负数",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 2.1 内置：单等价组信息损失计算（_Thread_NI_Loss）
            def _thread_ni_loss(eq_key, eq_size, age_range):
                """
                模拟计算单个等价组的信息损失（需根据实际业务逻辑调整）
                此处简化逻辑：假设年龄泛化范围越大，损失越高；组大小越大，损失越低
                """
                # 解析分组键中的年龄（假设准标识符含"age"，且位置在qids[0]）
                if isinstance(eq_key, tuple) and len(eq_key) >= 1:
                    age_val = eq_key[0]
                else:
                    age_val = eq_key  # 单准标识符场景
                
                # 简化计算：信息损失 = 年龄值/年龄范围 ÷ 组大小（示例逻辑）
                try:
                    age_loss = float(age_val) / age_range if age_range != 0 else 0.0
                    group_loss = age_loss / float(eq_size) if eq_size != 0 else 0.0
                    return round(group_loss, 4)
                except (ValueError, TypeError):
                    return 0.0  # 异常值处理

            # 2.2 计算所有等价组的信息损失及总损失
            eq_info_list = []
            total_info_loss = 0.0
            for eq_key, eq_size in zip(series_quasi.index, series_quasi.values):
                eq_size_py = int(eq_size)  # 组大小转Python int
                ni_loss = _thread_ni_loss(eq_key, eq_size_py, age_range)  # 调用内置方法
                total_info_loss += ni_loss
                # 格式化等价组名称（直接在循环内处理，移除辅助函数）
                if isinstance(eq_key, tuple):
                    eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
                else:
                    eq_name = f"{qids[0]}={eq_key}" if qids else f"组{len(eq_info_list)+1}"
                eq_info_list.append({
                    "eq_key": eq_key,
                    "eq_size": eq_size_py,
                    "ni_loss": ni_loss,
                    "eq_name": eq_name
                })

            # 2.3 计算NCP结果
            ncp = round(total_info_loss / (qid_dimension * total_record), 4) if (qid_dimension * total_record) != 0 else 0.0

            # 3. 生成节点（含去重）
            nodes = []
            node_map = {}
            hash_base = f"{table_name}_{scene}"

            # 3.1 目标表节点
            node_key = ("original_table", table_name)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"original_table::{hash_base}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "original_table",
                    "id": node_id,
                    "name": table_name,
                    "scene": scene,
                    "symbolSize": 40,
                    "desc": f"NCP计算的数据源单表，含准标识符{qids}"
                })
                node_map[node_key] = node_id

            # 3.2 总记录数节点
            node_key = ("total_record_count", total_record)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"total_record::{hash_base}::{total_record}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "total_record_count",
                    "id": node_id,
                    "name": f"总记录数={total_record}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"表中所有记录总数（NCP分母组成）",
                    "count": total_record
                })
                node_map[node_key] = node_id

            # 3.3 准标识符维度数节点
            node_key = ("qid_dimension_count", qid_dimension)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"qid_dimension::{hash_base}::{qid_dimension}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "qid_dimension_count",
                    "id": node_id,
                    "name": f"准标识符维度={qid_dimension}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"准标识符字段个数（{qids}），NCP分母组成",
                    "dimension": qid_dimension
                })
                node_map[node_key] = node_id

            # 3.4 泛化参数节点（年龄范围）
            node_key = ("generalization_param", f"Age_range={age_range}")
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"gen_param::{hash_base}::Age_range={age_range}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "generalization_param",
                    "id": node_id,
                    "name": f"年龄泛化范围={age_range}",
                    "scene": scene,
                    "symbolSize": 30,
                    "desc": "计算信息损失的参数",
                    "param_name": "Age_range",
                    "param_value": age_range
                })
                node_map[node_key] = node_id

            # 3.5 典型等价组实例节点（取Top N）
            top_eq_list = sorted(eq_info_list, key=lambda x: x["ni_loss"], reverse=True)[:min(top_n_equivalence, len(eq_info_list))]
            for idx, eq in enumerate(top_eq_list, 1):
                node_key = ("equivalence_class", eq["eq_name"])
                if node_key not in node_map:
                    node_id = int(hashlib.md5(f"eq_instance::{hash_base}::{eq['eq_name']}".encode()).hexdigest(), 16) % 1000000
                    symbol_size = 30 + min(eq["eq_size"] // 10, 10) + min(int(eq["ni_loss"] * 5), 5)
                    nodes.append({
                        "category": "equivalence_class",
                        "id": node_id,
                        "name": f"等价组{idx}({eq['eq_name']})",
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"组大小={eq['eq_size']} | 信息损失={eq['ni_loss']}",
                        "eq_size": eq["eq_size"],
                        "ni_loss": eq["ni_loss"]
                    })
                    node_map[node_key] = node_id

            # 3.6 总信息损失节点
            node_key = ("total_info_loss", round(total_info_loss, 4))
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"total_loss::{hash_base}::{round(total_info_loss,4)}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "total_info_loss",
                    "id": node_id,
                    "name": f"总信息损失={round(total_info_loss,4)}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": "所有等价组信息损失累加和（NCP分子）",
                    "total_loss": round(total_info_loss, 4)
                })
                node_map[node_key] = node_id

            # 3.7 NCP结果节点
            node_key = ("ncp_result", ncp)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"ncp::{hash_base}::{ncp}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "ncp_result",
                    "id": node_id,
                    "name": f"NCP数据损失度={ncp}",
                    "scene": scene,
                    "symbolSize": 50,
                    "desc": f"NCP=总信息损失/(准标识符维度×总记录数)={round(total_info_loss,4)}/({qid_dimension}×{total_record})={ncp}",
                    "ncp": ncp,
                    "formula": "NCP = 总信息损失 ÷ (准标识符维度数 × 总记录数)"
                })
                node_map[node_key] = node_id

            # 4. 生成边（含去重）
            links = []
            link_set = set()
            # 预存关键节点ID
            param_node_id = node_map.get(("generalization_param", f"Age_range={age_range}"), -1)
            total_loss_node_id = node_map.get(("total_info_loss", round(total_info_loss, 4)), -1)
            ncp_node_id = node_map.get(("ncp_result", ncp), -1)
            total_record_node_id = node_map.get(("total_record_count", total_record), -1)
            qid_dim_node_id = node_map.get(("qid_dimension_count", qid_dimension), -1)
            table_node_id = node_map.get(("original_table", table_name), -1)

            # 4.1 目标表 → 总记录数
            if table_node_id != -1 and total_record_node_id != -1:
                link_key = (table_node_id, total_record_node_id, "table_to_total_count")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": table_node_id,
                        "target": total_record_node_id,
                        "label": {"show": True, "formatter": "统计总记录数"},
                        "link_type": "table_to_total_count"
                    })
                    link_set.add(link_key)

            # 4.2 目标表 → 准标识符维度数
            if table_node_id != -1 and qid_dim_node_id != -1:
                link_key = (table_node_id, qid_dim_node_id, "table_to_qid_dimension")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": table_node_id,
                        "target": qid_dim_node_id,
                        "label": {"show": True, "formatter": "统计QID维度"},
                        "link_type": "table_to_qid_dimension"
                    })
                    link_set.add(link_key)

            # 4.3 目标表 → 等价组实例
            if table_node_id != -1:
                for eq in top_eq_list:
                    eq_node_id = node_map.get(("equivalence_class", eq["eq_name"]), -1)
                    if eq_node_id == -1:
                        continue
                    link_key = (table_node_id, eq_node_id, "table_to_equivalence")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": table_node_id,
                            "target": eq_node_id,
                            "label": {"show": True, "formatter": "生成等价组"},
                            "link_type": "table_to_equivalence"
                        })
                        link_set.add(link_key)

            # 4.4 泛化参数 → 等价组实例
            if param_node_id != -1:
                for eq in top_eq_list:
                    eq_node_id = node_map.get(("equivalence_class", eq["eq_name"]), -1)
                    if eq_node_id == -1:
                        continue
                    link_key = (param_node_id, eq_node_id, "param_to_equivalence")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": param_node_id,
                            "target": eq_node_id,
                            "label": {"show": True, "formatter": "提供年龄范围"},
                            "link_type": "param_to_equivalence"
                        })
                        link_set.add(link_key)

            # 4.5 等价组实例 → 总信息损失
            if total_loss_node_id != -1:
                for eq in top_eq_list:
                    eq_node_id = node_map.get(("equivalence_class", eq["eq_name"]), -1)
                    if eq_node_id == -1:
                        continue
                    link_key = (eq_node_id, total_loss_node_id, "eq_to_total_loss")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": eq_node_id,
                            "target": total_loss_node_id,
                            "label": {"show": True, "formatter": f"贡献损失={eq['ni_loss']}"},
                            "link_type": "eq_to_total_loss"
                        })
                        link_set.add(link_key)

            # 4.6 总信息损失 → NCP结果
            if total_loss_node_id != -1 and ncp_node_id != -1:
                link_key = (total_loss_node_id, ncp_node_id, "total_loss_to_ncp")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": total_loss_node_id,
                        "target": ncp_node_id,
                        "label": {"show": True, "formatter": "分子（总损失）"},
                        "link_type": "total_loss_to_ncp"
                    })
                    link_set.add(link_key)

            # 4.7 总记录数 → NCP结果
            if total_record_node_id != -1 and ncp_node_id != -1:
                link_key = (total_record_node_id, ncp_node_id, "total_to_ncp_denominator")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": total_record_node_id,
                        "target": ncp_node_id,
                        "label": {"show": True, "formatter": "分母组成（×维度）"},
                        "link_type": "total_to_ncp_denominator"
                    })
                    link_set.add(link_key)

            # 4.8 准标识符维度数 → NCP结果
            if qid_dim_node_id != -1 and ncp_node_id != -1:
                link_key = (qid_dim_node_id, ncp_node_id, "dimension_to_ncp_denominator")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": qid_dim_node_id,
                        "target": ncp_node_id,
                        "label": {"show": True, "formatter": "分母组成（×总数）"},
                        "link_type": "dimension_to_ncp_denominator"
                    })
                    link_set.add(link_key)

            # 5. 组装返回
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            return json.dumps({
                "error": f"生成NCP图谱失败：{str(e)}",
                "nodes": [],
                "links": []
            }, ensure_ascii=False)

    ### 基于熵的数据损失度
    def buildEntropyBasedLossGraph(self, top_n_equivalence: int = 20, graph_id: int = 11) -> str:
        """
        构建基于熵的数据损失度图谱，基于单表数据
        :param top_n_equivalence: 展示的典型等价组实例数量（默认前12个）
        :return: 含nodes/links的JSON字符串（格式对齐buildTopSensitiveRatioGroups）
        """
        try:
            # 1. 基础数据获取与校验
            _TemAll = self._Function_Data()  # 读取单表数据（DataFrame）
            series_quasi = self._Function_Series_quasi(_TemAll)  # 等价组（分组键+大小，升序）
            scene = getattr(self, 'scene', '通用数据场景')
            table_name = self.json_address  # 目标表名（mysql表名）
            qids = self.QIDs  # 准标识符列表（从Config类获取）

            # 2. 核心计算（贴合Get_Entropy_based_Average_loss逻辑）
            # 2.1 统计总记录数（转为Python原生int，避免JSON序列化错误）
            total_record = int(self._Num_address(_TemAll))
            if total_record <= 0:
                return json.dumps({
                    "error": "总记录数不能为0或负数（无法计算log₂）",
                    "nodes": [],
                    "links": []
                }, ensure_ascii=False)

            # 2.2 计算最大熵（原始熵 S0 = log₂(总记录数)）
            max_entropy = math.log2(total_record)
            max_entropy_rounded = round(max_entropy, 4)

            # 2.3 计算每个等价组的熵贡献 + 现有熵（current_entropy）
            eq_info_list = []  # 存储等价组信息（分组键、大小、熵贡献）
            current_entropy = 0.0  # 现有熵（分子）

            for eq_key, eq_size in zip(series_quasi.index, series_quasi.values):
                eq_size_py = int(eq_size)  # 组大小转Python int
                # 计算单组熵贡献：(log₂(组大小) × 组大小) / 总记录数
                if eq_size_py == 1:
                    # 处理log₂(1)=0的情况，避免计算冗余
                    eq_entropy_contrib = 0.0
                else:
                    eq_log = math.log2(eq_size_py)
                    eq_entropy_contrib = (eq_log * eq_size_py) / total_record
                
                current_entropy += eq_entropy_contrib
                # 格式化等价组名称（直接处理，无额外辅助函数）
                if isinstance(eq_key, tuple):
                    eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
                else:
                    eq_name = f"{qids[0]}={eq_key}" if qids else f"组{len(eq_info_list)+1}"
                
                eq_info_list.append({
                    "eq_key": eq_key,
                    "eq_size": eq_size_py,
                    "entropy_contrib": round(eq_entropy_contrib, 4),
                    "eq_name": eq_name
                })

            # 2.4 计算最终基于熵的数据损失度
            current_entropy_rounded = round(current_entropy, 4)
            entropy_loss_degree = round(current_entropy / max_entropy, 4) if max_entropy != 0 else 0.0

            # 3. 生成节点（含去重，用node_map避免重复）
            nodes = []
            node_map = {}  # 键：(节点类型, 核心标识)，值：节点id
            hash_base = f"{table_name}_{scene}"  # 哈希基础，确保多场景唯一

            # 3.1 目标表节点（数据源头）
            node_key = ("original_table", table_name)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"original_table::{hash_base}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "original_table",
                    "id": node_id,
                    "name": table_name,
                    "scene": scene,
                    "symbolSize": 40,
                    "desc": f"基于熵的损失度计算数据源单表，含准标识符{qids}和敏感属性"
                })
                node_map[node_key] = node_id

            # 3.2 总记录数节点（最大熵计算基础）
            node_key = ("total_record_count", total_record)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"total_record::{hash_base}::{total_record}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "total_record_count",
                    "id": node_id,
                    "name": f"总记录数={total_record}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"表中所有原始记录总数，用于计算最大熵（S0=log₂({total_record})）",
                    "count": total_record
                })
                node_map[node_key] = node_id

            # 3.3 最大熵节点（原始熵，分母）
            node_key = ("max_entropy", max_entropy_rounded)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"max_entropy::{hash_base}::{max_entropy_rounded}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "max_entropy",
                    "id": node_id,
                    "name": f"最大熵={max_entropy_rounded}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"原始数据的最大不确定性（S0=log₂({total_record})），损失度公式分母",
                    "entropy": max_entropy_rounded,
                    "formula": f"S0 = log₂({total_record})"
                })
                node_map[node_key] = node_id

            # 3.4 典型等价组实例节点（选Top N个，按熵贡献降序，突出高贡献组）
            top_eq_list = sorted(eq_info_list, key=lambda x: x["entropy_contrib"], reverse=True)[:min(top_n_equivalence, len(eq_info_list))]
            for idx, eq in enumerate(top_eq_list, 1):
                node_key = ("equivalence_class", eq["eq_name"])
                if node_key not in node_map:
                    node_id = int(hashlib.md5(f"eq_instance::{hash_base}::{eq['eq_name']}".encode()).hexdigest(), 16) % 1000000
                    # 节点尺寸：组大小越大+熵贡献越高，尺寸越大（体现对现有熵的影响）
                    symbol_size = 30 + min(eq["eq_size"] // 5, 10) + min(int(eq["entropy_contrib"] * 20), 5)
                    nodes.append({
                        "category": "equivalence_class",
                        "id": node_id,
                        "name": f"等价组{idx}({eq['eq_name']})",
                        "scene": scene,
                        "symbolSize": symbol_size,
                        "desc": f"组大小={eq['eq_size']} | 熵贡献={eq['entropy_contrib']}",
                        "eq_size": eq["eq_size"],
                        "entropy_contrib": eq["entropy_contrib"]
                    })
                    node_map[node_key] = node_id

            # 3.5 现有熵节点（中间结果，分子）
            node_key = ("current_entropy", current_entropy_rounded)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"current_entropy::{hash_base}::{current_entropy_rounded}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "current_entropy",
                    "id": node_id,
                    "name": f"现有熵={current_entropy_rounded}",
                    "scene": scene,
                    "symbolSize": 35,
                    "desc": f"泛化后数据的实际不确定性（Σ(熵贡献)），损失度公式分子",
                    "entropy": current_entropy_rounded,
                    "formula": "现有熵 = Σ( (log₂(组大小)×组大小)/总记录数 )"
                })
                node_map[node_key] = node_id

            # 3.6 基于熵的数据损失度节点（最终结果）
            node_key = ("entropy_based_loss", entropy_loss_degree)
            if node_key not in node_map:
                node_id = int(hashlib.md5(f"entropy_loss::{hash_base}::{entropy_loss_degree}".encode()).hexdigest(), 16) % 1000000
                nodes.append({
                    "category": "entropy_based_loss",
                    "id": node_id,
                    "name": f"熵损失度={entropy_loss_degree}",
                    "scene": scene,
                    "symbolSize": 50,  # 最大尺寸，突出结果重要性
                    "desc": f"基于熵的数据损失度=现有熵/最大熵={current_entropy_rounded}/{max_entropy_rounded}={entropy_loss_degree}",
                    "loss_degree": entropy_loss_degree,
                    "formula": "熵损失度 = 现有熵 ÷ 最大熵"
                })
                node_map[node_key] = node_id

            # 4. 生成边（含去重，用link_set避免重复）
            links = []
            link_set = set()  # 键：(源id, 目标id, 边类型)
            # 预存关键节点ID（容错处理，避免节点未生成导致崩溃）
            total_record_node_id = node_map.get(("total_record_count", total_record), -1)
            max_entropy_node_id = node_map.get(("max_entropy", max_entropy_rounded), -1)
            current_entropy_node_id = node_map.get(("current_entropy", current_entropy_rounded), -1)
            entropy_loss_node_id = node_map.get(("entropy_based_loss", entropy_loss_degree), -1)
            table_node_id = node_map.get(("original_table", table_name), -1)

            # 4.1 目标表 → 总记录数（统计依赖）
            if table_node_id != -1 and total_record_node_id != -1:
                link_key = (table_node_id, total_record_node_id, "table_to_total_count")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": table_node_id,
                        "target": total_record_node_id,
                        "label": {"show": True, "formatter": "统计总记录数"},
                        "link_type": "table_to_total_count"
                    })
                    link_set.add(link_key)

            # 4.2 总记录数 → 最大熵（计算依赖：log₂(N)）
            if total_record_node_id != -1 and max_entropy_node_id != -1:
                link_key = (total_record_node_id, max_entropy_node_id, "total_to_max_entropy")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": total_record_node_id,
                        "target": max_entropy_node_id,
                        "label": {"show": True, "formatter": "计算S0=log₂(N)"},
                        "link_type": "total_to_max_entropy"
                    })
                    link_set.add(link_key)

            # 4.3 目标表 → 等价组实例（分组依赖）
            if table_node_id != -1:
                for eq in top_eq_list:
                    eq_node_id = node_map.get(("equivalence_class", eq["eq_name"]), -1)
                    if eq_node_id == -1:
                        continue
                    link_key = (table_node_id, eq_node_id, "table_to_equivalence")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": table_node_id,
                            "target": eq_node_id,
                            "label": {"show": True, "formatter": "生成等价组"},
                            "link_type": "table_to_equivalence"
                        })
                        link_set.add(link_key)

            # 4.4 等价组实例 → 现有熵（计算依赖：累加熵贡献）
            if current_entropy_node_id != -1:
                for eq in top_eq_list:
                    eq_node_id = node_map.get(("equivalence_class", eq["eq_name"]), -1)
                    if eq_node_id == -1:
                        continue
                    link_key = (eq_node_id, current_entropy_node_id, "eq_to_current_entropy")
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,
                            "source": eq_node_id,
                            "target": current_entropy_node_id,
                            "label": {"show": True, "formatter": f"贡献熵={eq['entropy_contrib']}"},
                            "link_type": "eq_to_current_entropy",
                            "entropy_contrib": eq["entropy_contrib"]
                        })
                        link_set.add(link_key)

            # 4.5 现有熵 → 熵损失度（计算依赖：分子）
            if current_entropy_node_id != -1 and entropy_loss_node_id != -1:
                link_key = (current_entropy_node_id, entropy_loss_node_id, "current_entropy_to_loss")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": current_entropy_node_id,
                        "target": entropy_loss_node_id,
                        "label": {"show": True, "formatter": "分子（现有熵）"},
                        "link_type": "current_entropy_to_loss"
                    })
                    link_set.add(link_key)

            # 4.6 最大熵 → 熵损失度（计算依赖：分母）
            if max_entropy_node_id != -1 and entropy_loss_node_id != -1:
                link_key = (max_entropy_node_id, entropy_loss_node_id, "max_entropy_to_loss")
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": max_entropy_node_id,
                        "target": entropy_loss_node_id,
                        "label": {"show": True, "formatter": "分母（最大熵）"},
                        "link_type": "max_entropy_to_loss"
                    })
                    link_set.add(link_key)

            # 5. 组装图谱并返回
            final_graph = {
                "nodes": nodes,
                "links": links,
                "calculation_info": {
                    "formula": "熵损失度 = 现有熵 ÷ 最大熵",
                    "max_entropy": max_entropy_rounded,
                    "current_entropy": current_entropy_rounded,
                    "entropy_loss_degree": entropy_loss_degree,
                    "total_record": total_record
                }
            }
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选）
            return json_str

        except Exception as e:
            error_msg = f"生成基于熵的数据损失度图谱失败：{str(e)}"
            print(error_msg)  # 调试用：打印错误详情
            return json.dumps({
                "error": error_msg,
                "nodes": [],
                "links": []
            }, ensure_ascii=False)

    ### 分布泄露
    def buildDistributionLeakageGraph(self, top_n_equivalence: int = 8, each_privacy: str = None, graph_id: int = 21, sample_ratio: float = 0.3) -> str:
        """
        构建分布泄露（Distribution Leakage）图谱，基于单表数据
        新增参数：sample_ratio（抽样比例，默认0.3，设为1则全量，提速核心）
        :param each_privacy: 目标敏感属性（默认取self.SA[0]）
        :param top_n_equivalence: 展示的等价组数量（含最大泄露组，默认前8个）
        :return: 含nodes/links的JSON字符串（格式对齐buildTopSensitiveRatioGroups）
        """
        try:
            # --------------------------
            # 1. 基础数据获取与参数校验（新增抽样提速）
            # --------------------------
            _TemAll = self._Function_Data()
            scene = getattr(self, 'scene', '通用数据场景')
            table_name = self.json_address
            qids = self.address_Attr[0]

            # 数据抽样：数据量超1000条时启用，减少计算量
            if sample_ratio < 1.0 and len(_TemAll) > 1000:
                _TemAll = _TemAll.dropna(subset=qids + (each_privacy,) if each_privacy else qids).sample(
                    frac=sample_ratio, random_state=42
                )

            # 处理敏感属性
            if each_privacy is None:
                if not self.SA:
                    return json.dumps({"error": "未设置敏感属性（self.SA为空）", "nodes": [], "links": []}, ensure_ascii=False)
                each_privacy = self.SA[0]
            if each_privacy not in _TemAll.columns:
                return json.dumps({"error": f"数据缺少敏感属性列：{each_privacy}", "nodes": [], "links": []}, ensure_ascii=False)

            # 数据预处理：删除敏感属性空值，避免无效计算
            _TemAll = _TemAll.dropna(subset=[each_privacy]).copy()
            if _TemAll.empty:
                return json.dumps({"error": "敏感属性列全为空值，无法计算分布", "nodes": [], "links": []}, ensure_ascii=False)

            # --------------------------
            # 2. 核心计算（修复兼容问题+简化逻辑）
            # --------------------------
            # 2.1 计算全局敏感属性分布（保留原逻辑）
            global_dist = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)
            global_dist_dict = global_dist.round(4).to_dict()
            global_dist_str = ", ".join([f"{k}={v}" for k, v in global_dist_dict.items()])

            # 2.2 生成等价组：用数字ID标识，替代元组键
            _TemAll['eq_id'] = _TemAll.groupby(qids).ngroup()
            total_eq = _TemAll['eq_id'].nunique()
            if total_eq == 0:
                return json.dumps({"error": "未生成任何等价组（准标识符取值无重复）", "nodes": [], "links": []}, ensure_ascii=False)

            # 2.3 批量计算等价组信息（修复include_groups=False问题）
            # 2.3.1 批量统计组内敏感属性分布
            eq_sa_count = _TemAll.groupby(['eq_id', each_privacy]).size().reset_index(name='count')
            eq_total = _TemAll.groupby('eq_id').size().reset_index(name='eq_size')
            eq_sa_dist = pd.merge(eq_sa_count, eq_total, on='eq_id')
            eq_sa_dist['freq'] = (eq_sa_dist['count'] / eq_sa_dist['eq_size']).round(4)

            # 2.3.2 批量计算欧氏距离（移除include_groups=False）
            global_dist_df = pd.DataFrame(list(global_dist_dict.items()), columns=[each_privacy, 'global_freq'])
            global_dist_df['global_freq'] = global_dist_df['global_freq'].round(4)
            eq_all_dist = pd.merge(eq_sa_dist, global_dist_df, on=each_privacy, how='right').fillna({'count': 0, 'freq': 0.0, 'eq_id': -1})

            def calc_euclidean(group):
                sq_diff = (group['freq'] - group['global_freq']) ** 2
                euclidean = math.sqrt(sq_diff.sum())
                normalized = round(euclidean / 2, 4)
                return pd.Series({'euclidean_dist': round(euclidean, 4), 'normalized_leakage': normalized})

            # 修复：移除include_groups=False
            eq_leakage = eq_all_dist.groupby('eq_id').apply(calc_euclidean).reset_index()
            eq_leakage = pd.merge(eq_leakage, eq_total, on='eq_id')

            # 2.3.3 补充等价组名称与分布字典（修复include_groups=False）
            # 生成等价组QID组合（移除include_groups=False）
            eq_qid_map = _TemAll.groupby('eq_id').apply(
                lambda x: tuple(x[qids].iloc[0].values)
            ).reset_index(name='eq_key')

            # 格式化等价组名称
            def format_eq_name(eq_key):
                if isinstance(eq_key, tuple):
                    return ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
                return f"{qids[0]}={eq_key}" if qids else f"组_{hash(eq_key)[:6]}"

            eq_qid_map['eq_name'] = eq_qid_map['eq_key'].apply(format_eq_name)

            # 生成组内分布字典与字符串（移除include_groups=False）
            eq_dist_dict = eq_sa_dist.groupby('eq_id').apply(
                lambda x: dict(zip(x[each_privacy].astype(str), x['freq']))
            ).reset_index(name='eq_dist_dict')
            eq_dist_dict['eq_dist_str'] = eq_dist_dict['eq_dist_dict'].apply(
                lambda x: ", ".join([f"{k}={v}" for k, v in x.items()])
            )

            # 合并所有信息
            eq_leakage_list = pd.merge(eq_leakage, eq_qid_map, on='eq_id')
            eq_leakage_list = pd.merge(eq_leakage_list, eq_dist_dict, on='eq_id')
            eq_leakage_list = eq_leakage_list.to_dict('records')

            # 2.4 找到最大泄露组
            max_leakage_eq = max(eq_leakage_list, key=lambda x: x["euclidean_dist"])
            final_leakage_value = max_leakage_eq["normalized_leakage"]
            max_leakage_eq_id = max_leakage_eq["eq_id"]

            # --------------------------
            # 3. 筛选展示的等价组（限制数量，最多8个）
            # --------------------------
            top_n_equivalence = min(top_n_equivalence, 8)  # 强制限制最大8个组，提速
            sorted_eq_list = sorted(eq_leakage_list, key=lambda x: x["euclidean_dist"], reverse=True)
            display_eq_list = sorted_eq_list[:min(top_n_equivalence, len(sorted_eq_list))]

            # 确保最大泄露组在列表中
            max_in_display = any(eq["eq_id"] == max_leakage_eq_id for eq in display_eq_list)
            if not max_in_display and len(display_eq_list) < top_n_equivalence:
                display_eq_list.append(max_leakage_eq)

            # --------------------------
            # 4. 生成节点（简化ID生成，减少计算）
            # --------------------------
            nodes = []
            node_set = set()
            node_id_map = {
                "table": -1, "sens_attr": -1, "global_dist": -1,
                "max_leakage_eq": -1, "leakage_result": -1,
                "eq_instances": {}
            }
            hash_base = f"{table_name}_{scene}_{each_privacy}"

            # 4.1 目标表节点
            node_key = f"original_table::{table_name}"
            if node_key not in node_set:
                node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                nodes.append({
                    "category": "original_table", "id": node_id, "name": table_name, "scene": scene,
                    "symbolSize": 40, "desc": f"数据源单表，含准标识符{qids}和敏感属性{each_privacy}"
                })
                node_set.add(node_key)
                node_id_map["table"] = node_id

            # 4.2 敏感属性节点
            node_key = f"sensitive_attribute::{each_privacy}"
            if node_key not in node_set:
                node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                nodes.append({
                    "category": "sensitive_attribute", "id": node_id, "name": f"敏感属性：{each_privacy}", "scene": scene,
                    "symbolSize": 35, "desc": "当前计算分布泄露的敏感属性"
                })
                node_set.add(node_key)
                node_id_map["sens_attr"] = node_id

            # 4.3 全局敏感分布节
            node_key = f"global_sensitive_dist::{global_dist_str}"
            if node_key not in node_set:
                node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                nodes.append({
                    "category": "global_sensitive_dist", "id": node_id, "name": f"全局分布：{global_dist_str}", "scene": scene,
                    "symbolSize": 35, "desc": f"全表{each_privacy}的概率分布（计算泄露的基准）", "dist_dict": global_dist_dict
                })
                node_set.add(node_key)
                node_id_map["global_dist"] = node_id

            # 4.4 等价组实例节点（批量生成）
            for idx, eq in enumerate(display_eq_list, 1):
                node_key = f"equivalence_class::{eq['eq_name']}"
                if node_key not in node_set:
                    node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                    symbol_size = 30 + min(eq["eq_size"] // 10, 10) + min(int(eq["euclidean_dist"] * 10), 5)
                    is_max_mark = "（最大泄露组）" if eq["eq_id"] == max_leakage_eq_id else ""
                    nodes.append({
                        "category": "equivalence_class", "id": node_id, "name": f"等价组{idx}{is_max_mark}：{eq['eq_name']}", "scene": scene,
                        "symbolSize": symbol_size, "desc": f"组大小={eq['eq_size']} | 组内分布：{eq['eq_dist_str']} | 欧氏距离={eq['euclidean_dist']}",
                        "eq_key": str(eq["eq_key"]), "eq_dist_dict": eq["eq_dist_dict"], "euclidean_dist": eq["euclidean_dist"],
                        "is_max_leakage": (eq["eq_id"] == max_leakage_eq_id)
                    })
                    node_set.add(node_key)
                    node_id_map["eq_instances"][eq["eq_id"]] = node_id

            # 4.5 最大泄露等价组节点
            node_key = f"max_leakage_equivalence::{max_leakage_eq['eq_name']}"
            if node_key not in node_set:
                node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                nodes.append({
                    "category": "max_leakage_equivalence", "id": node_id, "name": f"最大泄露组：{max_leakage_eq['eq_name']}", "scene": scene,
                    "symbolSize": 40, "desc": f"所有等价组中分布泄露最大 | 欧氏距离={max_leakage_eq['euclidean_dist']} | 组内分布：{max_leakage_eq['eq_dist_str']}",
                    "eq_key": str(max_leakage_eq["eq_key"]), "euclidean_dist": max_leakage_eq["euclidean_dist"]
                })
                node_set.add(node_key)
                node_id_map["max_leakage_eq"] = node_id

            # 4.6 分布泄露结果节点
            node_key = f"distribution_leakage::{final_leakage_value}"
            if node_key not in node_set:
                node_id = abs(hash(f"{node_key}_{hash_base}")) % 1000000
                nodes.append({
                    "category": "distribution_leakage", "id": node_id, "name": f"分布泄露值={final_leakage_value}", "scene": scene,
                    "symbolSize": 50, "desc": f"分布泄露=最大欧氏距离/2 = {max_leakage_eq['euclidean_dist']}/2 = {final_leakage_value}",
                    "leakage_value": final_leakage_value, "formula": "分布泄露 = 最大欧氏距离（组分布-全局分布） ÷ 2"
                })
                node_set.add(node_key)
                node_id_map["leakage_result"] = node_id

            # --------------------------
            # 5. 生成边（保留原逻辑，用预存映射表）
            # --------------------------
            links = []
            link_set = set()
            table_id = node_id_map["table"]
            sens_attr_id = node_id_map["sens_attr"]
            global_dist_id = node_id_map["global_dist"]
            max_leakage_eq_node_id = node_id_map["max_leakage_eq"]
            leakage_result_id = node_id_map["leakage_result"]
            eq_instance_ids = node_id_map["eq_instances"]

            # 5.1 目标表 → 敏感属性
            if table_id != -1 and sens_attr_id != -1:
                link_key = f"{table_id}::{sens_attr_id}::table_to_sensitive_attr"
                if link_key not in link_set:
                    links.append({
                        "scene": scene, "source": table_id, "target": sens_attr_id,
                        "label": {"show": True, "formatter": "关联敏感属性"}, "link_type": "table_to_sensitive_attr"
                    })
                    link_set.add(link_key)

            # 5.2 目标表 → 全局敏感分布
            if table_id != -1 and global_dist_id != -1:
                link_key = f"{table_id}::{global_dist_id}::table_to_global_dist"
                if link_key not in link_set:
                    links.append({
                        "scene": scene, "source": table_id, "target": global_dist_id,
                        "label": {"show": True, "formatter": "计算全局分布"}, "link_type": "table_to_global_dist"
                    })
                    link_set.add(link_key)

            # 5.3 目标表 → 等价组实例
            if table_id != -1:
                for eq in display_eq_list:
                    eq_id = eq["eq_id"]
                    eq_node_id = eq_instance_ids.get(eq_id, -1)
                    if eq_node_id == -1:
                        continue
                    link_key = f"{table_id}::{eq_node_id}::table_to_equivalence"
                    if link_key not in link_set:
                        links.append({
                            "scene": scene, "source": table_id, "target": eq_node_id,
                            "label": {"show": True, "formatter": "生成等价组"}, "link_type": "table_to_equivalence"
                        })
                        link_set.add(link_key)

            # 5.4 等价组实例 → 自身（标记组内分布）
            for eq in display_eq_list:
                eq_id = eq["eq_id"]
                eq_node_id = eq_instance_ids.get(eq_id, -1)
                if eq_node_id == -1:
                    continue
                link_key = f"{eq_node_id}::{eq_node_id}::eq_to_self_dist"
                if link_key not in link_set:
                    links.append({
                        "scene": scene,
                        "source": eq_node_id,
                        "target": eq_node_id,
                        "label": {"show": True, "formatter": f"组内分布：{eq['eq_dist_str']}"},
                        "link_type": "eq_to_self"
                    })
                    link_set.add(link_key)

            # 5.5 等价组实例 → 全局敏感分布（计算欧氏距离）
            if global_dist_id != -1:
                for eq in display_eq_list:
                    eq_id = eq["eq_id"]
                    eq_node_id = eq_instance_ids.get(eq_id, -1)
                    if eq_node_id == -1:
                        continue
                    link_key = f"{eq_node_id}::{global_dist_id}::eq_to_global_dist_leakage"
                    if link_key not in link_set:
                        links.append({
                            "scene": scene, "source": eq_node_id, "target": global_dist_id,
                            "label": {"show": True, "formatter": f"欧氏距离={eq['euclidean_dist']}"}, 
                            "link_type": "eq_to_global_dist_leakage", "euclidean_dist": eq["euclidean_dist"]
                        })
                        link_set.add(link_key)

            # 5.6 最大泄露等价组（展示列表内）→ 单独标记的最大泄露组节点
            max_eq_display_node_id = -1
            for eq in display_eq_list:
                if eq["eq_id"] == max_leakage_eq_id:
                    max_eq_display_node_id = eq_instance_ids.get(eq["eq_id"], -1)
                    break
            if max_eq_display_node_id != -1 and max_leakage_eq_node_id != -1:
                link_key = f"{max_eq_display_node_id}::{max_leakage_eq_node_id}::eq_to_max_leakage_eq"
                if link_key not in link_set:
                    links.append({
                        "scene": scene, "source": max_eq_display_node_id, "target": max_leakage_eq_node_id,
                        "label": {"show": True, "formatter": "最大泄露组"}, "link_type": "eq_to_max_leakage_eq"
                    })
                    link_set.add(link_key)

            # 5.7 单独标记的最大泄露组 → 分布泄露结果（生成最终值）
            if max_leakage_eq_node_id != -1 and leakage_result_id != -1:
                link_key = f"{max_leakage_eq_node_id}::{leakage_result_id}::max_leakage_eq_to_result"
                if link_key not in link_set:
                    links.append({
                        "scene": scene, "source": max_leakage_eq_node_id, "target": leakage_result_id,
                        "label": {"show": True, "formatter": f"泄露值={max_leakage_eq['euclidean_dist']}/2"}, 
                        "link_type": "max_leakage_eq_to_result"
                    })
                    link_set.add(link_key)

            # --------------------------
            # 6. 组装图谱并返回（简化JSON序列化，提速）
            # --------------------------
            final_graph = {
                "nodes": nodes, 
                "links": links
                }
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            error_msg = f"生成分布泄露图谱失败：{str(e)}"
            print(f"报错详情：{error_detail}")
            return json.dumps({
                "error": error_msg,
                "error_detail": error_detail,
                "nodes": [],
                "links": []
            }, ensure_ascii=False)
    ### 熵泄露
    def buildEntropyLeakageGraph(self, top_n_equivalence: int = 10, each_privacy: str = None, graph_id: int = 22) -> str:
        """
        构建熵泄露图谱（适配无required_cols参数的_Function_Data方法）
        """
        try:
            # 1. 参数初始化与校验
            if each_privacy is None:
                if not hasattr(self, 'SA') or not self.SA:
                    return json.dumps({"error": "未配置敏感属性（self.SA为空）", "nodes": [], "links": []}, ensure_ascii=False)
                each_privacy = self.SA[0]
            
            # 读取完整数据（不传入required_cols参数，适配原始方法）
            _TemAll = self._Function_Data()
            if _TemAll.empty or not isinstance(_TemAll, pd.DataFrame):
                return json.dumps({"error": "数据读取失败或为空", "nodes": [], "links": []}, ensure_ascii=False)
            
            # 基础参数与必要列校验
            table_name = getattr(self, 'json_address', 'unknown_table')
            qids = getattr(self, 'QIDs', getattr(self, 'address_Attr', [[]])[0])
            scene = getattr(self, 'scene', 'default_scene')
            
            # 校验准标识符和敏感属性是否存在
            required_cols = qids + [each_privacy]
            missing_cols = [col for col in required_cols if col not in _TemAll.columns]
            if missing_cols:
                return json.dumps({"error": f"数据缺少必要列：{', '.join(missing_cols)}", "nodes": [], "links": []}, ensure_ascii=False)
            
            # 仅保留需要的列（在读取后过滤，避免修改_Function_Data）
            _TemAll = _TemAll[required_cols].copy()
            hash_base = f"{table_name}_{scene}_{each_privacy}"

            # 2. 核心计算
            # 2.1 全局敏感属性分布与熵
            global_dist = self._Probabilistic_Distribution_Privacy(each_privacy, _TemAll)
            if global_dist.empty:
                return json.dumps({"error": "全局分布计算失败", "nodes": [], "links": []}, ensure_ascii=False)
            global_dist = global_dist[global_dist > 0].round(4)
            global_dist_dict = global_dist.to_dict()
            global_dist_str = ", ".join([f"{k}={v}" for k, v in global_dist_dict.items()])

            # 2.2 全局熵Hmax
            hmax = -sum(p * math.log2(p) for p in global_dist.values if p > 0)
            hmax_rounded = round(hmax, 4)

            # 2.3 等价组生成与计算
            grouped = _TemAll.groupby(qids, sort=False) if qids else _TemAll.groupby(lambda x: 0)
            series_quasi_keys = list(grouped.groups.keys()) if hasattr(grouped, 'groups') else []
            if not series_quasi_keys:
                return json.dumps({"error": "未生成等价组", "nodes": [], "links": []}, ensure_ascii=False)

            # 2.4 等价组熵与泄露值
            eq_leakage_list = []
            for eq_key in series_quasi_keys:
                try:
                    eq_data = grouped.get_group(eq_key)
                except KeyError:
                    continue
                eq_dist = eq_data[each_privacy].value_counts(normalize=True).round(4)
                eq_dist = eq_dist[eq_dist > 0]
                if eq_dist.empty:
                    continue
                eq_entropy = -sum(qp * math.log2(qp) for qp in eq_dist.values if qp > 0)
                eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)]) if isinstance(eq_key, tuple) else f"{qids[0]}={eq_key}"
                eq_leakage_list.append({
                    "eq_key": eq_key, "eq_name": eq_name, "eq_size": len(eq_data),
                    "eq_entropy": round(eq_entropy, 4), "entropy_leakage": round(abs(hmax - eq_entropy), 4)
                })

            if not eq_leakage_list:
                return json.dumps({"error": "无有效等价组数据", "nodes": [], "links": []}, ensure_ascii=False)

            # 2.5 最大泄露组与归一化结果
            max_leakage_eq = max(eq_leakage_list, key=lambda x: x["entropy_leakage"])
            total_record = self._Num_address(_TemAll)
            normalize_denominator = math.log2(total_record) if total_record > 1 else 1.0
            final_leakage = round(max_leakage_eq["entropy_leakage"] / normalize_denominator, 4)

            # 2.6 筛选展示的等价组
            display_eq_list = sorted(eq_leakage_list, key=lambda x: x["entropy_leakage"], reverse=True)[:top_n_equivalence]
            if max_leakage_eq not in display_eq_list:
                display_eq_list.insert(0, max_leakage_eq)

            # 3. 生成节点
            nodes, node_map = [], {}

            # 3.1 目标表节点
            node_id = int(hashlib.md5(f"table::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("table", table_name)] = node_id
            nodes.append({"category": "original_table", "id": node_id, "name": f"表：{table_name}", "scene": scene, "symbolSize": 40})

            # 3.2 敏感属性节点
            node_id = int(hashlib.md5(f"sens::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("sens_attr", each_privacy)] = node_id
            nodes.append({"category": "sensitive_attribute", "id": node_id, "name": f"敏感属性：{each_privacy}", "scene": scene, "symbolSize": 35})

            # 3.3 全局分布节点
            node_id = int(hashlib.md5(f"global_dist::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("global_dist", global_dist_str)] = node_id
            nodes.append({"category": "global_sensitive_dist", "id": node_id, "name": f"全局分布：{global_dist_str}", "scene": scene, "symbolSize": 35})

            # 3.4 全局熵节点
            node_id = int(hashlib.md5(f"hmax::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("hmax", hmax_rounded)] = node_id
            nodes.append({"category": "global_sensitive_entropy", "id": node_id, "name": f"全局熵Hmax={hmax_rounded}", "scene": scene, "symbolSize": 35})

            # 3.5 等价组节点
            for idx, eq in enumerate(display_eq_list, 1):
                node_id = int(hashlib.md5(f"eq::{hash_base}::{eq['eq_name']}".encode()).hexdigest(), 16) % 1000000
                node_map[("eq", eq["eq_name"])] = node_id
                nodes.append({
                    "category": "equivalence_class", "id": node_id,
                    "name": f"等价组{idx}：{eq['eq_name']}", "scene": scene,
                    "symbolSize": 30 + min(int(eq["entropy_leakage"] * 20), 15)
                })

            # 3.6 最大泄露组节点
            node_id = int(hashlib.md5(f"max_eq::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("max_eq", max_leakage_eq["eq_name"])] = node_id
            nodes.append({"category": "max_leakage_equivalence", "id": node_id, "name": f"最大泄露组：{max_leakage_eq['eq_name']}", "scene": scene, "symbolSize": 40})

            # 3.7 总记录数节点
            node_id = int(hashlib.md5(f"total::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("total", total_record)] = node_id
            nodes.append({"category": "total_record_count", "id": node_id, "name": f"总记录数：{total_record}", "scene": scene, "symbolSize": 35})

            # 3.8 熵泄露结果节点
            node_id = int(hashlib.md5(f"leakage::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[("leakage", final_leakage)] = node_id
            nodes.append({"category": "entropy_leakage", "id": node_id, "name": f"熵泄露值：{final_leakage}", "scene": scene, "symbolSize": 50})

            # 4. 生成边
            links, link_set = [], set()

            # 4.1 表 → 敏感属性
            src, tgt = node_map[("table", table_name)], node_map[("sens_attr", each_privacy)]
            links.append({"scene": scene, "source": src, "target": tgt, "label": {"formatter": "包含属性"}, "link_type": "table_to_sens"})

            # 4.2 表 → 全局分布
            src, tgt = node_map[("table", table_name)], node_map[("global_dist", global_dist_str)]
            links.append({"scene": scene, "source": src, "target": tgt, "label": {"formatter": "计算分布"}, "link_type": "table_to_global"})

            # 4.3 全局分布 → 全局熵
            src, tgt = node_map[("global_dist", global_dist_str)], node_map[("hmax", hmax_rounded)]
            links.append({"scene": scene, "source": src, "target": tgt, "label": {"formatter": "计算Hmax"}, "link_type": "dist_to_hmax"})

            # 4.4 表 → 等价组
            src_table = node_map[("table", table_name)]
            for eq in display_eq_list:
                tgt_eq = node_map[("eq", eq["eq_name"])]
                if (src_table, tgt_eq) not in link_set:
                    links.append({"scene": scene, "source": src_table, "target": tgt_eq, "label": {"formatter": "生成组"}, "link_type": "table_to_eq"})
                    link_set.add((src_table, tgt_eq))

            # 4.5 全局熵 → 等价组
            src_hmax = node_map[("hmax", hmax_rounded)]
            for eq in display_eq_list:
                tgt_eq = node_map[("eq", eq["eq_name"])]
                if (src_hmax, tgt_eq) not in link_set:
                    links.append({"scene": scene, "source": src_hmax, "target": tgt_eq, "label": {"formatter": f"泄露={eq['entropy_leakage']}"}, "link_type": "hmax_to_eq"})
                    link_set.add((src_hmax, tgt_eq))

            # 4.6 等价组 → 最大泄露组
            src_eq = node_map[("eq", max_leakage_eq["eq_name"])]
            tgt_max = node_map[("max_eq", max_leakage_eq["eq_name"])]
            links.append({"scene": scene, "source": src_eq, "target": tgt_max, "label": {"formatter": "最大泄露"}, "link_type": "eq_to_max"})

            # 4.7 最大泄露组 → 结果
            src_max = node_map[("max_eq", max_leakage_eq["eq_name"])]
            tgt_res = node_map[("leakage", final_leakage)]
            links.append({"scene": scene, "source": src_max, "target": tgt_res, "label": {"formatter": "分子"}, "link_type": "max_to_res"})

            # 4.8 总记录数 → 结果
            src_total = node_map[("total", total_record)]
            tgt_res = node_map[("leakage", final_leakage)]
            links.append({"scene": scene, "source": src_total, "target": tgt_res, "label": {"formatter": "分母"}, "link_type": "total_to_res"})

            # 5. 返回结果
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({"error": f"失败：{str(e)}", "nodes": [], "links": []}, ensure_ascii=False)

    ### 隐私增益
    def buildPrivacyGainGraph(self, max_eq_groups: int = 10, graph_id: int = 23) -> str:
        """
        自动从self.SA读取敏感属性，生成符合指定格式的隐私增益图谱
        :param max_eq_groups: 最大等价组数量（默认20个，取大组优先）
        :return: 含nodes和links的JSON字符串，错误时返回带error的JSON
        """
        try:
            # -------------------------- 1. 数据准备与校验（新增scene校验） --------------------------
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)
            
            # 新增：校验scene参数（Node和Link需统一场景）
            if not hasattr(self, "scene"):
                return json.dumps({"nodes": [], "links": [], "error": "未配置scene参数"}, ensure_ascii=False)
            scene = self.scene  # 统一场景值（如"phone"）
            
            if not self.SA:
                return json.dumps({"nodes": [], "links": [], "error": "SA敏感属性集合为空"}, ensure_ascii=False)
            all_sensitive_attrs = self.SA
            qids = self.address_Attr[0]
            table_name = self.json_address
            total_records = self._Num_address(_TemAll)
            # 哈希基础新增scene，确保同场景同配置节点ID唯一
            hash_base = f"{table_name}_{self.worker_uuid}_{scene}"

            # 校验准标识符
            missing_qids = [q for q in qids if q not in _TemAll.columns]
            if missing_qids:
                return json.dumps({"nodes": [], "links": [], "error": f"准标识符不存在：{missing_qids}"}, ensure_ascii=False)

            # -------------------------- 2. 等价组处理（逻辑不变） --------------------------
            all_series_quasi = self._Function_Series_quasi(_TemAll).sort_values(ascending=False)
            limited_series_quasi = all_series_quasi.head(max_eq_groups)
            if limited_series_quasi.empty:
                return json.dumps({"nodes": [], "links": [], "error": "无有效等价组"}, ensure_ascii=False)

            # -------------------------- 3. 生成Nodes（核心修改：type→category + 加scene） --------------------------
            nodes = []
            node_map = {}  # (category, 关键值) → ID（原type改为category，保持映射逻辑）

            # 3.1 原始数据表节点（修改：type→category + 加scene）
            node_key = ("原始数据表", table_name)
            node_id = int(hashlib.md5(f"table::{hash_base}".encode()).hexdigest(), 16) % 1000000
            node_map[node_key] = node_id
            nodes.append({
                "category": "原始数据表",  # 原"type"改为"category"
                "id": node_id,
                "name": f"表：{table_name}",
                "scene": scene,  # 新增：统一场景
                "symbolSize": 45,
                "total_records": total_records,
                "eq_limit": max_eq_groups
            })

            # 3.2 准标识符节点（修改：type→category + 加scene）
            qid_str = ", ".join(qids)
            node_key = ("准标识符", qid_str)
            node_id = int(hashlib.md5(f"qid::{hash_base}::{qid_str}".encode()).hexdigest(), 16) % 1000000
            node_map[node_key] = node_id
            nodes.append({
                "category": "准标识符",  # 原"type"改为"category"
                "id": node_id,
                "name": f"准标识符：{qid_str}",
                "scene": scene,  # 新增：统一场景
                "symbolSize": 35
            })

            # 3.3 敏感属性节点（修改：type→category + 加scene）
            sa_nodes = []
            for sa in all_sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue
                node_key = ("敏感属性", sa)
                node_id = int(hashlib.md5(f"sa::{hash_base}::{sa}".encode()).hexdigest(), 16) % 1000000
                node_map[node_key] = node_id
                nodes.append({
                    "category": "敏感属性",  # 原"type"改为"category"
                    "id": node_id,
                    "name": f"敏感属性：{sa}",
                    "scene": scene,  # 新增：统一场景
                    "symbolSize": 35
                })
                sa_nodes.append(sa)

            # 3.4 等价组节点（修改：type→category + 加scene）
            eq_nodes = []
            for eq_idx, (eq_key, eq_size) in enumerate(limited_series_quasi.items()):
                eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)]) if isinstance(eq_key, tuple) else f"{qids[0]}={eq_key}"
                node_key = ("等价组", eq_name)
                node_id = int(hashlib.md5(f"eq::{hash_base}::{eq_name}".encode()).hexdigest(), 16) % 1000000
                node_map[node_key] = node_id
                nodes.append({
                    "category": "等价组",  # 原"type"改为"category"
                    "id": node_id,
                    "name": eq_name,
                    "scene": scene,  # 新增：统一场景
                    "symbolSize": 30 + min(eq_size // 1000, 10),
                    "eq_size": eq_size,
                    "is_limited": len(all_series_quasi) > max_eq_groups
                })
                eq_nodes.append(eq_name)

            # 3.5 敏感分组节点（修改：type→category + 加scene）
            sens_group_nodes = {}
            for sa in sa_nodes:
                sens_groups = _TemAll.groupby(sa, sort=False)
                sens_group_nodes[sa] = []
                for sens_val, sens_data in sens_groups:
                    sens_eq_keys = sens_data.groupby(qids).size().index
                    if not any(key in limited_series_quasi.index for key in sens_eq_keys):
                        continue
                    
                    sens_size = len(sens_data)
                    sens_unique_key = f"{sa}={sens_val}"
                    node_key = ("敏感分组", sens_unique_key)
                    node_id = int(hashlib.md5(f"sens::{hash_base}::{sens_unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[node_key] = node_id
                    nodes.append({
                        "category": "敏感分组",  # 原"type"改为"category"
                        "id": node_id,
                        "name": sens_unique_key,
                        "scene": scene,  # 新增：统一场景
                        "symbolSize": 30 + min(sens_size // 1000, 10),
                        "sens_size": sens_size
                    })
                    sens_group_nodes[sa].append((sens_val, node_id, sens_size, sens_unique_key))  # 新增sens_unique_key用于映射

            # 3.6 隐私增益结果节点（修改：type→category + 加scene）
            gain_nodes = {}
            for sa in sa_nodes:
                # 计算平均增益（逻辑不变）
                avg_gain = 0.0
                grouped_sa = _TemAll.groupby(sa, sort=False)
                if grouped_sa:
                    avg_tem = 0.0
                    valid_groups = 0
                    for each in grouped_sa:
                        sens_eq_keys = each[1][qids].value_counts().keys()
                        if not any(key in limited_series_quasi.index for key in sens_eq_keys):
                            continue
                        
                        positive_num = 0
                        for quasi_key in sens_eq_keys:
                            if quasi_key in limited_series_quasi.index:
                                positive_num += limited_series_quasi[quasi_key]
                        avg_tem += (positive_num - len(each[1])) / len(each[1])
                        valid_groups += 1
                    if valid_groups > 0:
                        avg_gain = round(avg_tem / valid_groups, 6)
                
                # 计算最大增益（逻辑不变）
                max_gain = 0.0
                if grouped_sa and len(grouped_sa) > 0:
                    max_tem = 0.0
                    for each in grouped_sa:
                        max_tem += (total_records - len(each[1])) / len(each[1])
                    max_gain = round(max_tem / len(grouped_sa), 6)
                
                # 平均增益节点（修改：type→category + 加scene）
                avg_unique_key = f"{sa}_avg_gain"
                node_key = ("隐私增益结果", avg_unique_key)
                avg_node_id = int(hashlib.md5(f"avg_gain::{hash_base}::{avg_unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[node_key] = avg_node_id
                nodes.append({
                    "category": "隐私增益结果",  # 原"type"改为"category"
                    "id": avg_node_id,
                    "name": f"{sa}平均增益={avg_gain}",
                    "scene": scene,  # 新增：统一场景
                    "symbolSize": 40,
                    "gain_type": "average"
                })
                
                # 最大增益节点（修改：type→category + 加scene）
                max_unique_key = f"{sa}_max_gain"
                node_key = ("隐私增益结果", max_unique_key)
                max_node_id = int(hashlib.md5(f"max_gain::{hash_base}::{max_unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[node_key] = max_node_id
                nodes.append({
                    "category": "隐私增益结果",  # 原"type"改为"category"
                    "id": max_node_id,
                    "name": f"{sa}最大增益={max_gain}",
                    "scene": scene,  # 新增：统一场景
                    "symbolSize": 40,
                    "gain_type": "max"
                })
                
                gain_nodes[sa] = (avg_node_id, max_node_id, avg_unique_key, max_unique_key)  # 新增unique_key用于映射

            # -------------------------- 4. 生成Links（核心修改：relation→link_type + 加scene） --------------------------
            links = []
            link_set = set()

            # 4.1 数据表 → 准标识符（修改：加scene + relation→link_type）
            src_key = ("原始数据表", table_name)
            tgt_key = ("准标识符", qid_str)
            src_id = node_map[src_key]
            tgt_id = node_map[tgt_key]
            link_label = "包含准标识符"
            links.append({
                "scene": scene,  # 新增：统一场景
                "source": src_id,
                "target": tgt_id,
                "label": {"show": True, "formatter": link_label},
                "link_type": "包含"  # 原"relation"改为"link_type"
            })
            link_set.add((src_id, tgt_id, link_label))

            # 4.2 数据表 → 敏感属性（修改：加scene + relation→link_type）
            src_id = node_map[("原始数据表", table_name)]
            for sa in sa_nodes:
                tgt_key = ("敏感属性", sa)
                tgt_id = node_map[tgt_key]
                link_label = f"包含{sa}"
                link_key = (src_id, tgt_id, link_label)
                if link_key not in link_set:
                    links.append({
                        "scene": scene,  # 新增：统一场景
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": link_label},
                        "link_type": "包含"  # 原"relation"改为"link_type"
                    })
                    link_set.add(link_key)

            # 4.3 准标识符 → 等价组（修改：加scene + relation→link_type）
            src_key = ("准标识符", qid_str)
            src_id = node_map[src_key]
            for eq_name in eq_nodes:
                tgt_key = ("等价组", eq_name)
                tgt_id = node_map[tgt_key]
                link_label = "生成等价组"
                link_key = (src_id, tgt_id, link_label)
                if link_key not in link_set:
                    links.append({
                        "scene": scene,  # 新增：统一场景
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": link_label},
                        "link_type": "分组生成"  # 原"relation"改为"link_type"
                    })
                    link_set.add(link_key)

            # 4.4 敏感属性 → 敏感分组（修改：加scene + relation→link_type）
            for sa in sa_nodes:
                src_key = ("敏感属性", sa)
                src_id = node_map[src_key]
                for sens_val, sens_id, _, sens_unique_key in sens_group_nodes.get(sa, []):
                    tgt_key = ("敏感分组", sens_unique_key)
                    tgt_id = sens_id
                    link_label = f"生成{sa}分组"
                    link_key = (src_id, tgt_id, link_label)
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,  # 新增：统一场景
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"show": True, "formatter": link_label},
                            "link_type": "分组生成"  # 原"relation"改为"link_type"
                        })
                        link_set.add(link_key)

            # 4.5 等价组 → 敏感分组（修改：加scene + relation→link_type）
            for sa in sa_nodes:
                for sens_val, sens_id, _, sens_unique_key in sens_group_nodes.get(sa, []):
                    tgt_key = ("敏感分组", sens_unique_key)
                    tgt_id = sens_id
                    sens_data = _TemAll[_TemAll[sa] == sens_val]
                    eq_in_sens = sens_data.groupby(qids).size().index
                    for eq_key in eq_in_sens:
                        if eq_key not in limited_series_quasi.index:
                            continue
                        
                        eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)]) if isinstance(eq_key, tuple) else f"{qids[0]}={eq_key}"
                        src_key = ("等价组", eq_name)
                        src_id = node_map.get(src_key, -1)
                        if src_id == -1:
                            continue
                        
                        link_label = f"包含{sa}={sens_val}"
                        link_key = (src_id, tgt_id, link_label)
                        if link_key not in link_set:
                            links.append({
                                "scene": scene,  # 新增：统一场景
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"show": True, "formatter": link_label},
                                "link_type": "归属包含"  # 原"relation"改为"link_type"
                            })
                            link_set.add(link_key)

            # 4.6 敏感分组+等价组 → 平均增益（修改：加scene + relation→link_type）
            for sa in sa_nodes:
                avg_gain_id, _, avg_unique_key, _ = gain_nodes[sa]
                tgt_key = ("隐私增益结果", avg_unique_key)
                tgt_id = avg_gain_id
                for sens_val, sens_id, sens_size, sens_unique_key in sens_group_nodes.get(sa, []):
                    sens_data = _TemAll[_TemAll[sa] == sens_val]
                    eq_in_sens = sens_data.groupby(qids).size().index
                    for eq_key in eq_in_sens:
                        if eq_key not in limited_series_quasi.index:
                            continue
                        
                        # 敏感分组→平均增益
                        src_key = ("敏感分组", sens_unique_key)
                        src_id = sens_id
                        link_label = f"敏感组大小={sens_size}"
                        link_key = (src_id, tgt_id, link_label)
                        if link_key not in link_set:
                            links.append({
                                "scene": scene,  # 新增：统一场景
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"show": True, "formatter": link_label},
                                "link_type": "计算依赖"  # 原"relation"改为"link_type"
                            })
                            link_set.add(link_key)
                        
                        # 等价组→平均增益
                        eq_name = ", ".join([f"{k}={v}" for k, v in zip(qids, eq_key)]) if isinstance(eq_key, tuple) else f"{qids[0]}={eq_key}"
                        src_key = ("等价组", eq_name)
                        src_id = node_map.get(src_key, -1)
                        if src_id == -1:
                            continue
                        link_label = f"等价组大小={limited_series_quasi[eq_key]}"
                        link_key = (src_id, tgt_id, link_label)
                        if link_key not in link_set:
                            links.append({
                                "scene": scene,  # 新增：统一场景
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"show": True, "formatter": link_label},
                                "link_type": "计算依赖"  # 原"relation"改为"link_type"
                            })
                            link_set.add(link_key)

            # 4.7 数据表+敏感分组 → 最大增益（修改：加scene + relation→link_type）
            for sa in sa_nodes:
                max_gain_id, _, _, max_unique_key = gain_nodes[sa]
                tgt_key = ("隐私增益结果", max_unique_key)
                tgt_id = max_gain_id
                
                # 数据表→最大增益
                src_key = ("原始数据表", table_name)
                src_id = node_map[src_key]
                link_label = f"总记录数={total_records}"
                link_key = (src_id, tgt_id, link_label)
                if link_key not in link_set:
                    links.append({
                        "scene": scene,  # 新增：统一场景
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"show": True, "formatter": link_label},
                        "link_type": "计算依赖"  # 原"relation"改为"link_type"
                    })
                    link_set.add(link_key)
                
                # 敏感分组→最大增益
                for sens_val, sens_id, sens_size, sens_unique_key in sens_group_nodes.get(sa, []):
                    src_key = ("敏感分组", sens_unique_key)
                    src_id = sens_id
                    link_label = f"敏感组大小={sens_size}"
                    link_key = (src_id, tgt_id, link_label)
                    if link_key not in link_set:
                        links.append({
                            "scene": scene,  # 新增：统一场景
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"show": True, "formatter": link_label},
                            "link_type": "计算依赖"  # 原"relation"改为"link_type"
                        })
                        link_set.add(link_key)

            # -------------------------- 5. 返回结果（仅保留nodes和links） --------------------------
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({
                "nodes": [],
                "links": [],
                "error": f"图谱生成失败：{str(e)}"
            }, ensure_ascii=False)
    
    ### KL
    def buildKLDivergenceGraphWithSpecFormat(self, max_eq_groups: int = 20, graph_id: int = 24) -> str:
        """
        适配指定格式的KL散度图谱：Node含category/scene，Link含link_type/scene
        :param max_eq_groups: 最大等价组数量（默认20，避免冗余）
        :return: 含nodes和links的JSON字符串，错误时返回带error的JSON
        """
        try:
            # 1. 基础数据准备与校验
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)
            if not self.SA:
                return json.dumps({"nodes": [], "links": [], "error": "未配置敏感属性SA"}, ensure_ascii=False)
            if not self.address_Attr[0]:
                return json.dumps({"nodes": [], "links": [], "error": "未配置准标识符QID"}, ensure_ascii=False)
            if not hasattr(self, "scene"):
                return json.dumps({"nodes": [], "links": [], "error": "未配置scene参数"}, ensure_ascii=False)
            
            sa_list = self.SA  # 敏感属性列表
            qid_list = self.address_Attr[0]  # 准标识符列表
            table_name = self.json_address  # 数据表名
            scene = self.scene  # 场景（如"phone"，从Config继承）
            hash_base = f"{table_name}_{self.worker_uuid}_{scene}"  # 节点ID哈希基础
            grouped_qid = _TemAll.groupby(qid_list, sort=False)  # QID分组对象

            # 2. 筛选等价组（大组优先，限制数量）
            all_eq = self._Function_Series_quasi(_TemAll)
            sorted_eq = all_eq.sort_values(ascending=False)
            limited_eq = sorted_eq.head(max_eq_groups)
            if limited_eq.empty:
                return json.dumps({"nodes": [], "links": [], "error": "无有效等价组"}, ensure_ascii=False)

            # 3. 生成Node（严格匹配指定格式：category/id/name/scene/symbolSize）
            nodes: List[Dict] = []
            node_map: Dict[Tuple[str, str], int] = {}  # (节点类别, 唯一标识) → ID

            # 辅助函数：生成符合格式的Node
            def add_node(node_category: str, unique_key: str, name: str, symbol_size: int = 35) -> None:
                key = (node_category, unique_key)
                if key not in node_map:
                    # 生成唯一ID（哈希后取模，避免过长）
                    node_id = int(hashlib.md5(f"{node_category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    # 严格按指定格式添加Node字段
                    nodes.append({
                        "category": node_category,
                        "id": node_id,
                        "name": name,
                        "scene": scene,
                        "symbolSize": symbol_size
                    })

            # 3.1 各类型Node创建（按业务逻辑分类）
            # 数据表Node（category=table）
            add_node(
                node_category="table",
                unique_key=table_name,
                name=f"数据表：{table_name}",
                symbol_size=45  # 数据表节点稍大
            )

            # 准标识符Node（category=quasi_identifier）
            qid_unique_key = ",".join(qid_list)
            add_node(
                node_category="quasi_identifier",
                unique_key=qid_unique_key,
                name=f"准标识符：{qid_unique_key}",
                symbol_size=40
            )

            # 敏感属性Node（category=sensitive_attribute，匹配示例格式）
            global_dist_cache: Dict[str, pd.Series] = {}  # 缓存全局分布，避免重复计算
            for sa in sa_list:
                # 敏感属性Node
                add_node(
                    node_category="sensitive_attribute",
                    unique_key=sa,
                    name=f"敏感属性：{sa}",  # 与示例"敏感属性：age"格式一致
                    symbol_size=35
                )
                # 全局分布Node（category=global_distribution）
                global_dist = self._Probabilistic_Distribution_Privacy(sa, _TemAll)
                global_dist_cache[sa] = global_dist
                add_node(
                    node_category="global_distribution",
                    unique_key=f"global_{sa}",
                    name=f"全局分布：{sa}",
                    symbol_size=35
                )

            # 等价组Node（category=equivalence_class）
            eq_name_map: Dict[str, Tuple] = {}  # 等价组名称→原始QID组合
            for eq_idx, (eq_key, eq_size) in enumerate(limited_eq.items(), 1):
                # 格式化等价组名称（如"Age=30,Sex=男"）
                eq_name = ",".join([f"{qid}={val}" for qid, val in zip(qid_list, eq_key)]) if isinstance(eq_key, tuple) else f"{qid_list[0]}={eq_key}"
                eq_name_map[eq_name] = eq_key
                add_node(
                    node_category="equivalence_class",
                    unique_key=eq_name,
                    name=f"等价组：{eq_name}",
                    symbol_size=30 + min(eq_size // 1000, 15)  # 大小与记录数正相关
                )

            # KL散度Node（category=kl_divergence）+ 最大KL散度Node（category=max_kl_divergence）
            max_kl_cache: Dict[str, Tuple[str, str]] = {}  # SA→(最大KL等价组名, 最大KL唯一键)
            for sa in sa_list:
                global_dist = global_dist_cache[sa]
                max_kl_val = -1.0
                max_kl_eq_name = ""

                # 遍历等价组生成KL散度Node
                for eq_name, eq_key in eq_name_map.items():
                    # 计算KL散度
                    eq_data = grouped_qid.get_group(eq_key)
                    eq_sa_dist = eq_data[sa].value_counts(normalize=True)
                    kl_val = 0.0
                    for sa_val, eq_prob in eq_sa_dist.items():
                        if sa_val in global_dist.index and global_dist[sa_val] > 0:
                            kl_val += eq_prob * math.log2(eq_prob / global_dist[sa_val])
                    kl_val = round(kl_val, 6)

                    # 更新最大KL记录
                    if kl_val > max_kl_val:
                        max_kl_val = kl_val
                        max_kl_eq_name = eq_name

                    # 添加KL散度Node
                    kl_unique_key = f"kl_{sa}@{eq_name}"
                    add_node(
                        node_category="kl_divergence",
                        unique_key=kl_unique_key,
                        name=f"KL散度：{sa}@{eq_name}={kl_val}",
                        symbol_size=30 + min(int(kl_val * 5), 20)  # KL值越大，节点越大
                    )

                # 添加最大KL散度Node
                if max_kl_val >= 0 and max_kl_eq_name:
                    max_kl_unique_key = f"max_kl_{sa}"
                    add_node(
                        node_category="max_kl_divergence",
                        unique_key=max_kl_unique_key,
                        name=f"最大KL散度：{sa}={max_kl_val}",
                        symbol_size=45  # 最大KL节点稍大，突出显示
                    )
                    max_kl_cache[sa] = (max_kl_eq_name, max_kl_unique_key)

            # 4. 生成Link（严格匹配指定格式：scene/source/target/label/link_type）
            links: List[Dict] = []
            link_set: Set[Tuple[int, int, str]] = set()  # 去重键：(源ID, 目标ID, link_type)

            # 辅助函数：生成符合格式的Link
            def add_link(source_key: Tuple[str, str], target_key: Tuple[str, str], label_formatter: str, link_type: str) -> None:
                # 检查源/目标节点是否存在
                if source_key not in node_map or target_key not in node_map:
                    return
                src_id = node_map[source_key]
                tgt_id = node_map[target_key]
                # 检查Link是否已存在（避免重复）
                link_unique_key = (src_id, tgt_id, link_type)
                if link_unique_key not in link_set:
                    # 严格按指定格式添加Link字段
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": label_formatter},  # 与示例label格式一致
                        "link_type": link_type  # 自定义链路类型，便于前端识别
                    })
                    link_set.add(link_unique_key)

            # 4.1 各类型Link创建（按业务逻辑定义link_type）
            # 数据表 → 准标识符（link_type=table_to_qid）
            add_link(
                source_key=("table", table_name),
                target_key=("quasi_identifier", qid_unique_key),
                label_formatter="包含准标识符",
                link_type="table_to_qid"
            )

            # 数据表 → 敏感属性（link_type=table_to_sa）
            for sa in sa_list:
                add_link(
                    source_key=("table", table_name),
                    target_key=("sensitive_attribute", sa),
                    label_formatter=f"包含敏感属性{sa}",
                    link_type="table_to_sa"
                )

            # 敏感属性 → 全局分布（link_type=sa_to_global_dist）
            for sa in sa_list:
                add_link(
                    source_key=("sensitive_attribute", sa),
                    target_key=("global_distribution", f"global_{sa}"),
                    label_formatter="生成全局分布",
                    link_type="sa_to_global_dist"
                )

            # 准标识符 → 等价组（link_type=qid_to_eq，与示例"生成组"逻辑一致）
            for eq_name in eq_name_map.keys():
                add_link(
                    source_key=("quasi_identifier", qid_unique_key),
                    target_key=("equivalence_class", eq_name),
                    label_formatter="生成组",  # 与示例label.formatter="生成组"一致
                    link_type="qid_to_eq"  # 可对应示例的"table_to_eq"，按实际逻辑调整
                )

            # 等价组 → KL散度（link_type=eq_to_kl）
            for sa in sa_list:
                for eq_name in eq_name_map.keys():
                    kl_unique_key = f"kl_{sa}@{eq_name}"
                    add_link(
                        source_key=("equivalence_class", eq_name),
                        target_key=("kl_divergence", kl_unique_key),
                        label_formatter="计算KL值",
                        link_type="eq_to_kl"
                    )

            # 全局分布 → KL散度（link_type=global_dist_to_kl）
            for sa in sa_list:
                global_key = ("global_distribution", f"global_{sa}")
                for eq_name in eq_name_map.keys():
                    kl_unique_key = f"kl_{sa}@{eq_name}"
                    add_link(
                        source_key=global_key,
                        target_key=("kl_divergence", kl_unique_key),
                        label_formatter="基准分布",
                        link_type="global_dist_to_kl"
                    )

            # KL散度 → 最大KL散度（link_type=kl_to_max_kl）
            for sa in sa_list:
                if sa not in max_kl_cache:
                    continue
                max_kl_eq_name, max_kl_unique_key = max_kl_cache[sa]
                kl_key = ("kl_divergence", f"kl_{sa}@{max_kl_eq_name}")
                max_kl_key = ("max_kl_divergence", max_kl_unique_key)
                add_link(
                    source_key=kl_key,
                    target_key=max_kl_key,
                    label_formatter="最大KL来源",
                    link_type="kl_to_max_kl"
                )

            # 5. 返回仅含nodes和links的JSON（严格匹配需求）
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            error_msg = f"图谱生成失败：{str(e)}"
            return json.dumps({"nodes": [], "links": [], "error": error_msg}, ensure_ascii=False)
    
    def _get_entropy_based_risk(self, series_quasi_keys, each_privacy, _TemAll, graph_id: int = 11) -> Tuple[float, any]:
        """简化风险计算，仅返回最大风险值和对应等价组"""
        num_privacy = len(_TemAll[each_privacy].drop_duplicates())
        if num_privacy == 0:
            return (0.0, None)
        hmax = math.log2(num_privacy) if num_privacy > 0 else 1e-6
        if hmax == 0:
            return (0.0, None)

        grouped = _TemAll.groupby(self.address_Attr[0], sort=False)
        max_risk = 0.0
        risk_eq_key = None

        # 仅计算一次风险，不记录所有等价组风险（减少计算量）
        for eq_key in series_quasi_keys:
            try:
                eq_data = grouped.get_group(eq_key)
            except KeyError:
                continue

            dist = eq_data[each_privacy].value_counts(normalize=True)
            group_risk = hmax + sum(p * math.log2(p) for p in dist.values if p > 0)
            normalized_risk = group_risk / hmax

            if normalized_risk > max_risk:
                max_risk = normalized_risk
                risk_eq_key = eq_key

            if normalized_risk > 0.9:  # 高风险提前退出
                return (round(normalized_risk, 4), risk_eq_key)

        return (round(max_risk, 4), risk_eq_key)

    ### 敏感属性的重识别风险图谱
    def buildReidentificationRiskGraph(self, max_nodes: int = 50, graph_id: int = 25) -> str:
        """
        随机筛选等价组以控制节点数量（大幅减少计算时间）
        :param max_nodes: 最大节点数量（建议20-100）
        """
        try:
            # 1. 基础数据准备
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)

            qids = self.address_Attr[0]
            sensitive_attrs = self.address_Attr[1]
            if not qids or not sensitive_attrs:
                return json.dumps({"nodes": [], "links": [], "error": "准标识符或敏感属性配置为空"}, ensure_ascii=False)

            table_name = self.json_address
            scene = self.scene
            hash_base = f"{table_name}_{self.worker_uuid}_{scene}"

            # 2. 等价组处理（核心优化：随机筛选，移除全量风险计算）
            series_quasi = self._Function_Series_quasi(_TemAll)
            series_quasi_keys = series_quasi.keys()
            if isinstance(series_quasi_keys, pd.MultiIndex):
                series_quasi_keys = [tuple(levels) for levels in series_quasi_keys]
            else:
                series_quasi_keys = list(series_quasi_keys)
            if not series_quasi_keys:
                return json.dumps({"nodes": [], "links": [], "error": "未生成有效等价组"}, ensure_ascii=False)

            grouped_qid = _TemAll.groupby(qids, sort=False)

            # 3. 节点配额计算（与之前相同，但筛选逻辑改为随机）
            fixed_nodes = 2 + 3 * len(sensitive_attrs)  # 固定节点数量
            allocable_nodes = max(0, max_nodes - fixed_nodes)
            max_eq_nodes = allocable_nodes // 2  # 等价组最大数量

            # 核心优化：随机筛选等价组（移除全量风险预计算）
            # 若等价组数量 <= 目标数量，全部保留；否则随机抽样
            if len(series_quasi_keys) <= max_eq_nodes:
                selected_eq_keys = series_quasi_keys
            else:
                selected_eq_keys = random.sample(series_quasi_keys, max_eq_nodes)

            # 4. 节点生成（仅包含随机筛选的等价组）
            nodes: List[Dict] = []
            node_map: Dict[Tuple[str, str], int] = {}

            # 4.1 原始数据表节点
            category = "原始数据表"
            unique_key = table_name
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 45,
                    "total_records": len(_TemAll)
                })

            # 4.2 准标识符集合节点
            category = "准标识符集合"
            qid_str = ",".join(qids)
            unique_key = qid_str
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"准标识符：{qid_str}",
                    "scene": scene,
                    "symbolSize": 40
                })

            # 4.3 随机筛选的等价组节点
            eq_unique_keys: List[str] = []
            for eq_key in selected_eq_keys:
                if isinstance(eq_key, tuple):
                    eq_name = "&".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
                else:
                    eq_name = f"{qids[0]}={eq_key}"
                eq_unique_key = f"eq_{eq_name}"
                eq_unique_keys.append(eq_unique_key)

                eq_size = series_quasi.loc[eq_key] if isinstance(series_quasi.index, pd.MultiIndex) else series_quasi[eq_key]
                category = "等价组"
                key = (category, eq_unique_key)
                if key not in node_map:
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{eq_unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"等价组：{eq_name}",
                        "scene": scene,
                        "symbolSize": 30 + min(int(eq_size) // 1000, 15),
                        "record_count": int(eq_size)
                    })

            # 4.4 敏感属性 + 最大熵基准节点（与之前相同）
            max_entropy_map: Dict[str, str] = {}
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue

                # 敏感属性节点
                category = "敏感属性"
                unique_key = f"sa_{sa}"
                key = (category, unique_key)
                if key not in node_map:
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"敏感属性：{sa}",
                        "scene": scene,
                        "symbolSize": 35
                    })

                # 最大熵基准节点
                category = "最大熵基准"
                sa_unique_count = len(_TemAll[sa].drop_duplicates())
                hmax = math.log2(sa_unique_count) if sa_unique_count > 0 else 0
                unique_key = f"hmax_{sa}"
                key = (category, unique_key)
                if key not in node_map:
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"最大熵：{sa}_Hmax",
                        "scene": scene,
                        "symbolSize": 35,
                        "value": round(hmax, 4)
                    })
                max_entropy_map[sa] = unique_key

            # 4.5 组内分布节点（仅对应随机筛选的等价组）
            dist_map: Dict[Tuple[str, str], str] = {}
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue
                for eq_key in selected_eq_keys:  # 只处理随机筛选的等价组
                    if isinstance(eq_key, tuple):
                        eq_name = "&".join([f"{k}={v}" for k, v in zip(qids, eq_key)])
                    else:
                        eq_name = f"{qids[0]}={eq_key}"
                    eq_unique_key = f"eq_{eq_name}"

                    try:
                        eq_data = grouped_qid.get_group(eq_key)
                    except KeyError:
                        continue

                    dist = eq_data[sa].value_counts(normalize=True)
                    dist_unique_key = f"dist_{sa}@{eq_unique_key}"
                    category = "组内敏感属性分布"
                    key = (category, dist_unique_key)
                    if key not in node_map:
                        node_id = int(hashlib.md5(f"{category}::{hash_base}::{dist_unique_key}".encode()).hexdigest(), 16) % 1000000
                        node_map[key] = node_id
                        nodes.append({
                            "category": category,
                            "id": node_id,
                            "name": f"分布：{sa}@{eq_name}",
                            "scene": scene,
                            "symbolSize": 30,
                            "top2": {k: round(v, 4) for k, v in dist.head(2).items()}
                        })
                    dist_map[(sa, eq_unique_key)] = dist_unique_key

            # 4.6 风险结果节点（与之前相同）
            risk_map: Dict[str, Tuple[str, str]] = {}
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns or sa not in max_entropy_map:
                    continue

                risk_value, risk_eq_key = self._get_entropy_based_risk(series_quasi_keys, sa, _TemAll)

                # 确保风险等价组在筛选列表中，否则用第一个筛选的等价组
                if risk_eq_key not in selected_eq_keys and selected_eq_keys:
                    risk_eq_key = selected_eq_keys[0]

                if isinstance(risk_eq_key, tuple):
                    risk_eq_name = "&".join([f"{k}={v}" for k, v in zip(qids, risk_eq_key)])
                else:
                    risk_eq_name = f"{qids[0]}={risk_eq_key}" if risk_eq_key is not None else "未知"
                risk_eq_unique_key = f"eq_{risk_eq_name}"

                category = "重识别风险结果"
                unique_key = f"risk_{sa}"
                key = (category, unique_key)
                if key not in node_map:
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    risk_percent = round(risk_value * 100, 2)
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"风险：{sa}_{risk_percent}%",
                        "scene": scene,
                        "symbolSize": 40,
                        "risk_level": "高风险" if risk_percent >= 70 else "中风险" if risk_percent >=30 else "低风险"
                    })
                risk_map[sa] = (unique_key, risk_eq_unique_key)

            # 5. 边生成（仅连接存在的节点）
            links: List[Dict] = []
            # （边生成逻辑与之前相同，省略重复代码）
            # 6.1 原始数据表 -> 准标识符集合
            source_key = ("原始数据表", table_name)
            target_key = ("准标识符集合", qid_str)
            if source_key in node_map and target_key in node_map:
                src_id = node_map[source_key]
                tgt_id = node_map[target_key]
                links.append({
                    "scene": scene,
                    "source": src_id,
                    "target": tgt_id,
                    "label": {"formatter": "包含准标识符"},
                    "link_type": "包含"
                })

            # 6.2 准标识符集合 -> 筛选的等价组
            for eq_unique_key in eq_unique_keys:
                source_key = ("准标识符集合", qid_str)
                target_key = ("等价组", eq_unique_key)
                if source_key in node_map and target_key in node_map:
                    src_id = node_map[source_key]
                    tgt_id = node_map[target_key]
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "生成等价组"},
                        "link_type": "分组生成"
                    })

            # 6.3 原始数据表 -> 敏感属性
            for sa in sensitive_attrs:
                if sa not in _TemAll.columns:
                    continue
                source_key = ("原始数据表", table_name)
                target_key = ("敏感属性", f"sa_{sa}")
                if source_key in node_map and target_key in node_map:
                    src_id = node_map[source_key]
                    tgt_id = node_map[target_key]
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": f"包含{sa}"},
                        "link_type": "包含"
                    })

            # 6.4 敏感属性 -> 最大熵基准
            for sa, hmax_key in max_entropy_map.items():
                source_key = ("敏感属性", f"sa_{sa}")
                target_key = ("最大熵基准", hmax_key)
                if source_key in node_map and target_key in node_map:
                    src_id = node_map[source_key]
                    tgt_id = node_map[target_key]
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "计算Hmax"},
                        "link_type": "计算基准"
                    })

            # 6.5 等价组 -> 组内分布
            for (sa, eq_key), dist_key in dist_map.items():
                source_key = ("等价组", eq_key)
                target_key = ("组内敏感属性分布", dist_key)
                if source_key in node_map and target_key in node_map:
                    src_id = node_map[source_key]
                    tgt_id = node_map[target_key]
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "属性分布"},
                        "link_type": "归属"
                    })

            # 6.6 最大熵基准 -> 组内分布
            for sa, hmax_key in max_entropy_map.items():
                for (dist_sa, eq_key), dist_key in dist_map.items():
                    if dist_sa == sa:
                        source_key = ("最大熵基准", hmax_key)
                        target_key = ("组内敏感属性分布", dist_key)
                        if source_key in node_map and target_key in node_map:
                            src_id = node_map[source_key]
                            tgt_id = node_map[target_key]
                            links.append({
                                "scene": scene,
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "参照基准"},
                                "link_type": "基准"
                            })

            # 6.7 组内分布 -> 风险结果
            for sa, (risk_key, risk_eq_key) in risk_map.items():
                dist_key = dist_map.get((sa, risk_eq_key))
                if dist_key:
                    source_key = ("组内敏感属性分布", dist_key)
                    target_key = ("重识别风险结果", risk_key)
                    if source_key in node_map and target_key in node_map:
                        src_id = node_map[source_key]
                        tgt_id = node_map[target_key]
                        links.append({
                            "scene": scene,
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "计算风险"},
                            "link_type": "风险计算"
                        })

            # 6.8 等价组 -> 风险结果
            for sa, (risk_key, risk_eq_key) in risk_map.items():
                source_key = ("等价组", risk_eq_key)
                target_key = ("重识别风险结果", risk_key)
                if source_key in node_map and target_key in node_map:
                    src_id = node_map[source_key]
                    tgt_id = node_map[target_key]
                    links.append({
                        "scene": scene,
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "风险来源"},
                        "link_type": "来源"
                    })

            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({
                "nodes": [],
                "links": [],
                "error": f"图谱生成失败：{str(e)}"
            }, ensure_ascii=False)

    # 风险计算函数（保持不变）
    def Get_Entropy_based_Re_indentification_Risk_QID(self, each_QID, _TemAll):
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())
        Hmax = math.log2(Num_Privacy) if Num_Privacy > 0 else 1e-6
        Grouped = _TemAll.groupby(each_QID, sort=False)
        res = 0.0
        Attr = None
        for each in Grouped:
            Distribution_Quasi = each[1][self.address_Attr[1]].value_counts(normalize=True)
            ReIndent_Risk = float(Hmax)
            for each_value in Distribution_Quasi:
                ReIndent_Risk += each_value * math.log2(each_value) if each_value > 0 else 0
            if ReIndent_Risk > res:
                res = ReIndent_Risk
                Attr = each[0]
            if (res / Hmax) > 0.9:
                return round(float(res / Hmax), 4), Attr
        return round(float(res / Hmax), 4), Attr

    # 生成准标识符的重识别风险图谱
    def buildQIDRiskGraph(self, max_equivalence_groups: int = 50, graph_id: int = 26) -> str:
        try:
            # 1. 数据准备
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)
            
            table_name = self.json_address
            qids = self.QIDs  # 准标识符列表
            sensitive_attr = self.SA[0] if self.SA else ""  # 敏感属性（默认取第一个）
            if not qids or not sensitive_attr:
                return json.dumps({"nodes": [], "links": [], "error": "准标识符或敏感属性为空"}, ensure_ascii=False)
            
            scene = self.scene
            hash_base = f"{table_name}_{sensitive_attr}_{scene}"

            # 2. 初始化存储（节点、边、映射关系）
            nodes: List[Dict] = []
            node_map: Dict[Tuple[str, str], int] = {}  # (类型, 唯一标识) -> ID
            links: List[Dict] = []
            link_set: Set[Tuple[int, int, str]] = set()  # 避免重复边

            # 3. 生成节点（直接处理，无辅助函数）
            # 3.1 数据表节点
            category = "数据表"
            unique_key = table_name
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 50,
                    "记录数": len(_TemAll),
                    "字段": qids + [sensitive_attr]
                })

            # 3.2 敏感属性节点
            category = "敏感属性"
            unique_key = sensitive_attr
            key = (category, unique_key)
            if key not in node_map:
                sa_unique_count = len(_TemAll[sensitive_attr].drop_duplicates())
                hmax = math.log2(sa_unique_count) if sa_unique_count > 0 else 0
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"敏感属性：{sensitive_attr}",
                    "scene": scene,
                    "symbolSize": 40,
                    "唯一值数量": sa_unique_count,
                    "最大熵Hmax": round(hmax, 4)
                })

            # 3.3 准标识符节点 + 等价组节点 + 风险值节点
            for qid in qids:
                # 3.3.1 准标识符字段节点
                category = "准标识符"
                unique_key = qid
                key = (category, unique_key)
                if key not in node_map:
                    qid_unique_count = len(_TemAll[qid].drop_duplicates())
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": f"准标识符：{qid}",
                        "scene": scene,
                        "symbolSize": 35,
                        "数据类型": str(_TemAll[qid].dtype),
                        "唯一值数量": qid_unique_count
                    })

                # 3.3.2 计算风险及最高风险等价组
                risk_value, top_attr = self.Get_Entropy_based_Re_indentification_Risk_QID(qid, _TemAll)
                risk_percent = round(risk_value * 100, 2)
                risk_level = "高风险" if risk_percent >= 70 else "中风险" if risk_percent >= 30 else "低风险"

                # 3.3.3 风险值节点
                category = "风险值"
                unique_key_risk = f"{qid}_risk"
                key_risk = (category, unique_key_risk)
                if key_risk not in node_map:
                    node_id_risk = int(hashlib.md5(f"{category}::{hash_base}::{unique_key_risk}".encode()).hexdigest(), 16) % 1000000
                    node_map[key_risk] = node_id_risk
                    nodes.append({
                        "category": category,
                        "id": node_id_risk,
                        "name": f"{qid}风险：{risk_percent}%",
                        "scene": scene,
                        "symbolSize": 40,
                        "风险值": risk_value,
                        "风险等级": risk_level,
                        "最高风险等价组": str(top_attr)
                    })

                # 3.3.4 最高风险等价组节点
                if top_attr is not None:
                    category = "等价组"
                    unique_key_eq = f"{qid}={top_attr}"
                    key_eq = (category, unique_key_eq)
                    if key_eq not in node_map:
                        # 获取等价组详情
                        grouped = _TemAll.groupby(qid, sort=False)
                        try:
                            eq_data = grouped.get_group(top_attr)
                            dist = eq_data[sensitive_attr].value_counts(normalize=True).head(3).to_dict()
                            group_size = len(eq_data)
                        except KeyError:
                            dist = {}
                            group_size = 0
                        # 生成节点
                        node_id_eq = int(hashlib.md5(f"{category}::{hash_base}::{unique_key_eq}".encode()).hexdigest(), 16) % 1000000
                        node_map[key_eq] = node_id_eq
                        nodes.append({
                            "category": category,
                            "id": node_id_eq,
                            "name": f"{qid}={top_attr}",
                            "scene": scene,
                            "symbolSize": 30,
                            "组内记录数": group_size,
                            "敏感属性分布": {k: round(v, 4) for k, v in dist.items()}
                        })

            # 4. 生成边（直接处理，无辅助函数）
            # 4.1 表 -> 准标识符
            src_table = ("数据表", table_name)
            for qid in qids:
                tgt_qid = ("准标识符", qid)
                if src_table in node_map and tgt_qid in node_map:
                    src_id = node_map[src_table]
                    tgt_id = node_map[tgt_qid]
                    link_key = (src_id, tgt_id, "包含")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "包含准标识符"},
                            "link_type": "包含",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.2 表 -> 敏感属性
            tgt_sa = ("敏感属性", sensitive_attr)
            if src_table in node_map and tgt_sa in node_map:
                src_id = node_map[src_table]
                tgt_id = node_map[tgt_sa]
                link_key = (src_id, tgt_id, "包含")
                if link_key not in link_set:
                    links.append({
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "包含敏感属性"},
                        "link_type": "包含",
                        "scene": scene
                    })
                    link_set.add(link_key)

            # 4.3 准标识符 -> 等价组（最高风险）
            for qid in qids:
                src_qid = ("准标识符", qid)
                _, top_attr = self.Get_Entropy_based_Re_indentification_Risk_QID(qid, _TemAll)
                if top_attr is not None:
                    tgt_eq = ("等价组", f"{qid}={top_attr}")
                    if src_qid in node_map and tgt_eq in node_map:
                        src_id = node_map[src_qid]
                        tgt_id = node_map[tgt_eq]
                        link_key = (src_id, tgt_id, "分组")
                        if link_key not in link_set:
                            links.append({
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "生成等价组"},
                                "link_type": "分组",
                                "scene": scene
                            })
                            link_set.add(link_key)

            # 4.4 等价组 -> 敏感属性（分布依赖）
            for qid in qids:
                _, top_attr = self.Get_Entropy_based_Re_indentification_Risk_QID(qid, _TemAll)
                if top_attr is not None:
                    src_eq = ("等价组", f"{qid}={top_attr}")
                    tgt_sa = ("敏感属性", sensitive_attr)
                    if src_eq in node_map and tgt_sa in node_map:
                        src_id = node_map[src_eq]
                        tgt_id = node_map[tgt_sa]
                        link_key = (src_id, tgt_id, "分布依赖")
                        if link_key not in link_set:
                            links.append({
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "包含敏感属性分布"},
                                "link_type": "分布依赖",
                                "scene": scene
                            })
                            link_set.add(link_key)

            # 4.5 准标识符 -> 风险值（计算依赖）
            for qid in qids:
                src_qid = ("准标识符", qid)
                tgt_risk = ("风险值", f"{qid}_risk")
                if src_qid in node_map and tgt_risk in node_map:
                    src_id = node_map[src_qid]
                    tgt_id = node_map[tgt_risk]
                    link_key = (src_id, tgt_id, "风险计算")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "计算风险值"},
                            "link_type": "风险计算",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.6 敏感属性 -> 风险值（基准依赖）
            src_sa = ("敏感属性", sensitive_attr)
            for qid in qids:
                tgt_risk = ("风险值", f"{qid}_risk")
                if src_sa in node_map and tgt_risk in node_map:
                    src_id = node_map[src_sa]
                    tgt_id = node_map[tgt_risk]
                    link_key = (src_id, tgt_id, "基准依赖")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "提供Hmax基准"},
                            "link_type": "基准依赖",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 4.7 等价组 -> 风险值（最高风险来源）
            for qid in qids:
                _, top_attr = self.Get_Entropy_based_Re_indentification_Risk_QID(qid, _TemAll)
                if top_attr is not None:
                    src_eq = ("等价组", f"{qid}={top_attr}")
                    tgt_risk = ("风险值", f"{qid}_risk")
                    if src_eq in node_map and tgt_risk in node_map:
                        src_id = node_map[src_eq]
                        tgt_id = node_map[tgt_risk]
                        link_key = (src_id, tgt_id, "风险来源")
                        if link_key not in link_set:
                            links.append({
                                "source": src_id,
                                "target": tgt_id,
                                "label": {"formatter": "贡献最高风险"},
                                "link_type": "风险来源",
                                "scene": scene
                            })
                            link_set.add(link_key)

            # 5. 返回结果
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({
                "nodes": [],
                "links": [],
                "error": f"图谱生成失败：{str(e)}"
            }, ensure_ascii=False)

    # 保留你的整体风险计算函数
    def Get_Entropy_based_Re_indentification_Risk(self, Series_quasi_keys, _TemAll):
        Num_Privacy = len(_TemAll[self.address_Attr[1]].drop_duplicates())
        Hmax = math.log2(Num_Privacy) if Num_Privacy > 0 else 1e-6
        Grouped = _TemAll.groupby(self.address_Attr[0], sort=False)
        length = len(Series_quasi_keys)
        res = 0.0
        risk_key = None
        for i in range(length):
            try:
                Distribution_Quasi = Grouped.get_group(Series_quasi_keys[i])[self.address_Attr[1]].value_counts(normalize=True)
            except KeyError:
                continue
            ReIndent_Risk = float(Hmax)
            for each_value in Distribution_Quasi:
                ReIndent_Risk += each_value * math.log2(each_value) if each_value > 0 else 0
            if ReIndent_Risk > res:
                res = ReIndent_Risk
                risk_key = Series_quasi_keys[i]
            if (res / Hmax) > 0.9:
                return round(float(res / Hmax), 4), risk_key
        return round(float(res / Hmax), 4), risk_key

    # 新增：生成整体重识别风险图谱（无辅助函数）
    def buildOverallRiskGraph(self, graph_id: int = 27) -> str:
        try:
            # 1. 数据准备
            _TemAll = self._Function_Data()
            if _TemAll.empty:
                return json.dumps({"nodes": [], "links": [], "error": "数据表为空"}, ensure_ascii=False)
            
            table_name = self.json_address
            quasi_combo = self.QIDs  # 准标识符组合（如["age", "gender"]）
            sensitive_attr = self.SA[0] if self.SA else ""  # 敏感属性
            if not quasi_combo or not sensitive_attr:
                return json.dumps({"nodes": [], "links": [], "error": "准标识符组合或敏感属性为空"}, ensure_ascii=False)
            
            # 准标识符组合名称（如"age,gender"）
            quasi_combo_name = ",".join(quasi_combo)
            scene = self.scene
            hash_base = f"{table_name}_{quasi_combo_name}_{sensitive_attr}"  # 唯一哈希基准

            # 2. 计算整体风险及最高风险等价组
            series_quasi = self._Function_Series_quasi(_TemAll)
            series_quasi_keys = series_quasi.keys().tolist()  # 所有等价组键
            overall_risk, top_risk_eq_key = self.Get_Entropy_based_Re_indentification_Risk(series_quasi_keys, _TemAll)
            risk_percent = round(overall_risk * 100, 2)
            risk_level = "高风险" if risk_percent >= 70 else "中风险" if risk_percent >= 30 else "低风险"

            # 3. 初始化存储
            nodes: List[Dict] = []
            node_map: Dict[Tuple[str, str], int] = {}  # (类型, 唯一标识) -> 节点ID
            links: List[Dict] = []
            link_set: Set[Tuple[int, int, str]] = set()  # 避免重复边

            # 4. 生成节点（直接显式处理）
            # 4.1 表节点
            category = "数据表"
            unique_key = table_name
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 50,
                    "总记录数": len(_TemAll),
                    "包含字段": quasi_combo + [sensitive_attr]
                })

            # 4.2 准标识符组合节点
            category = "准标识符组合"
            unique_key = quasi_combo_name
            key = (category, unique_key)
            if key not in node_map:
                eq_total = len(series_quasi_keys)  # 等价组总数
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"准标识符组合：{quasi_combo_name}",
                    "scene": scene,
                    "symbolSize": 45,
                    "包含字段": quasi_combo,
                    "等价组总数": eq_total
                })

            # 4.3 敏感属性节点
            category = "敏感属性"
            unique_key = sensitive_attr
            key = (category, unique_key)
            if key not in node_map:
                sa_unique_count = len(_TemAll[sensitive_attr].drop_duplicates())
                hmax = math.log2(sa_unique_count) if sa_unique_count > 0 else 0
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"敏感属性：{sensitive_attr}",
                    "scene": scene,
                    "symbolSize": 40,
                    "唯一值数量": sa_unique_count,
                    "最大熵Hmax": round(hmax, 4)
                })

            # 4.4 最高风险等价组节点
            if top_risk_eq_key is not None:
                category = "最高风险等价组"
                # 格式化等价组名称（如(30, '男') → "age=30&gender=男"）
                if isinstance(top_risk_eq_key, tuple):
                    eq_name_parts = [f"{k}={v}" for k, v in zip(quasi_combo, top_risk_eq_key)]
                    eq_name = "&".join(eq_name_parts)
                else:
                    eq_name = f"{quasi_combo[0]}={top_risk_eq_key}"
                unique_key = eq_name
                key = (category, unique_key)
                if key not in node_map:
                    # 获取等价组详情
                    grouped = _TemAll.groupby(quasi_combo, sort=False)
                    try:
                        eq_data = grouped.get_group(top_risk_eq_key)
                        dist = eq_data[sensitive_attr].value_counts(normalize=True).head(3).to_dict()
                        group_size = len(eq_data)
                    except KeyError:
                        dist = {}
                        group_size = 0
                    # 生成节点
                    node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                    node_map[key] = node_id
                    nodes.append({
                        "category": category,
                        "id": node_id,
                        "name": eq_name,
                        "scene": scene,
                        "symbolSize": 35,
                        "组内记录数": group_size,
                        "敏感属性分布": {k: round(v, 4) for k, v in dist.items()}
                    })

            # 4.5 整体风险值节点
            category = "整体重识别风险"
            unique_key = "overall_risk"
            key = (category, unique_key)
            if key not in node_map:
                node_id = int(hashlib.md5(f"{category}::{hash_base}::{unique_key}".encode()).hexdigest(), 16) % 1000000
                node_map[key] = node_id
                nodes.append({
                    "category": category,
                    "id": node_id,
                    "name": f"整体风险：{risk_percent}%",
                    "scene": scene,
                    "symbolSize": 50,
                    "风险值": overall_risk,
                    "风险等级": risk_level,
                    "最高风险等价组": str(top_risk_eq_key)
                })

            # 5. 生成边（直接显式处理）
            # 5.1 表 → 准标识符组合
            src_table = ("数据表", table_name)
            tgt_quasi = ("准标识符组合", quasi_combo_name)
            if src_table in node_map and tgt_quasi in node_map:
                src_id = node_map[src_table]
                tgt_id = node_map[tgt_quasi]
                link_key = (src_id, tgt_id, "包含")
                if link_key not in link_set:
                    links.append({
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "包含准标识符组合"},
                        "link_type": "包含",
                        "scene": scene
                    })
                    link_set.add(link_key)

            # 5.2 表 → 敏感属性
            tgt_sa = ("敏感属性", sensitive_attr)
            if src_table in node_map and tgt_sa in node_map:
                src_id = node_map[src_table]
                tgt_id = node_map[tgt_sa]
                link_key = (src_id, tgt_id, "包含")
                if link_key not in link_set:
                    links.append({
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "包含敏感属性"},
                        "link_type": "包含",
                        "scene": scene
                    })
                    link_set.add(link_key)

            # 5.3 准标识符组合 → 最高风险等价组
            if top_risk_eq_key is not None:
                src_quasi = ("准标识符组合", quasi_combo_name)
                eq_name = "&".join([f"{k}={v}" for k, v in zip(quasi_combo, top_risk_eq_key)]) if isinstance(top_risk_eq_key, tuple) else f"{quasi_combo[0]}={top_risk_eq_key}"
                tgt_eq = ("最高风险等价组", eq_name)
                if src_quasi in node_map and tgt_eq in node_map:
                    src_id = node_map[src_quasi]
                    tgt_id = node_map[tgt_eq]
                    link_key = (src_id, tgt_id, "生成")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "生成等价组"},
                            "link_type": "生成",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 5.4 最高风险等价组 → 敏感属性
            if top_risk_eq_key is not None:
                eq_name = "&".join([f"{k}={v}" for k, v in zip(quasi_combo, top_risk_eq_key)]) if isinstance(top_risk_eq_key, tuple) else f"{quasi_combo[0]}={top_risk_eq_key}"
                src_eq = ("最高风险等价组", eq_name)
                tgt_sa = ("敏感属性", sensitive_attr)
                if src_eq in node_map and tgt_sa in node_map:
                    src_id = node_map[src_eq]
                    tgt_id = node_map[tgt_sa]
                    link_key = (src_id, tgt_id, "分布依赖")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "包含敏感属性分布"},
                            "link_type": "分布依赖",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 5.5 敏感属性 → 整体风险值
            src_sa = ("敏感属性", sensitive_attr)
            tgt_risk = ("整体重识别风险", "overall_risk")
            if src_sa in node_map and tgt_risk in node_map:
                src_id = node_map[src_sa]
                tgt_id = node_map[tgt_risk]
                link_key = (src_id, tgt_id, "基准")
                if link_key not in link_set:
                    links.append({
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "提供Hmax基准"},
                        "link_type": "基准",
                        "scene": scene
                    })
                    link_set.add(link_key)

            # 5.6 最高风险等价组 → 整体风险值
            if top_risk_eq_key is not None:
                eq_name = "&".join([f"{k}={v}" for k, v in zip(quasi_combo, top_risk_eq_key)]) if isinstance(top_risk_eq_key, tuple) else f"{quasi_combo[0]}={top_risk_eq_key}"
                src_eq = ("最高风险等价组", eq_name)
                tgt_risk = ("整体重识别风险", "overall_risk")
                if src_eq in node_map and tgt_risk in node_map:
                    src_id = node_map[src_eq]
                    tgt_id = node_map[tgt_risk]
                    link_key = (src_id, tgt_id, "风险来源")
                    if link_key not in link_set:
                        links.append({
                            "source": src_id,
                            "target": tgt_id,
                            "label": {"formatter": "贡献最高风险"},
                            "link_type": "风险来源",
                            "scene": scene
                        })
                        link_set.add(link_key)

            # 5.7 准标识符组合 → 整体风险值
            src_quasi = ("准标识符组合", quasi_combo_name)
            tgt_risk = ("整体重识别风险", "overall_risk")
            if src_quasi in node_map and tgt_risk in node_map:
                src_id = node_map[src_quasi]
                tgt_id = node_map[tgt_risk]
                link_key = (src_id, tgt_id, "计算")
                if link_key not in link_set:
                    links.append({
                        "source": src_id,
                        "target": tgt_id,
                        "label": {"formatter": "计算整体风险"},
                        "link_type": "计算",
                        "scene": scene
                    })
                    link_set.add(link_key)

            # 6. 返回结果
            final_graph = {"nodes": nodes, "links": links}
            json_str = json.dumps(final_graph, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)  # 存储到文件（可选） 
            return json_str

        except Exception as e:
            return json.dumps({
                "nodes": [],
                "links": [],
                "error": f"图谱生成失败：{str(e)}"
            }, ensure_ascii=False)

    ### 重实别风险
    def buildReidentificationRateGraph(self, max_combinations: int = 20, graph_id: int = 18) -> str:
        """构建重识别率图谱，输出与参考格式一致的数据"""
        import pandas as pd
        import json
        from itertools import combinations

        # 获取原始数据
        _TemAll = self._Function_Data()
        total_records = self._Num_address(_TemAll)
        qids = self.QIDs  # 准标识符列表（如["user_id", "user_name", "province"]）
        reid_rates = []

        # 计算不同准标识符组合的重识别率
        for dim in range(min(4, len(qids)), 0, -1):
            for combo in combinations(qids, dim):
                eq_classes = _TemAll.groupby(list(combo)).size()
                unique_records = sum(1 for cnt in eq_classes if cnt == 1)
                reid_rate = round(unique_records / total_records * 100, 2)
                reid_rates.append({
                    "combination": "+".join(combo),
                    "dimension": dim,
                    "reid_rate": reid_rate,
                    "unique_count": unique_records
                })

        # 排序并取top组合（最多max_combinations个）
        reid_rates.sort(key=lambda x: x["reid_rate"], reverse=True)
        top_combinations = reid_rates[:max_combinations]

        # 构建节点（完全匹配参考格式）
        nodes = []
        node_id = 1  # 节点ID从1开始递增
        qid_node_ids = {}  # 存储准标识符节点ID，用于后续链接构建

        # 1. 核心数据表节点（对应参考格式的original_table）
        core_table_id = node_id
        nodes.append({
            "category": "original_table",
            "id": core_table_id,
            "name": "user_basic_information_8",
            "scene": "网络社区",
            "symbolSize": 40,
            "desc": "存储原始数据的单表，重识别率分析的数据源"
        })
        node_id += 1

        # 2. 总记录数节点（对应参考格式的total_record_count）
        total_count_id = node_id
        nodes.append({
            "category": "total_record_count",
            "id": total_count_id,
            "name": f"总记录数={total_records}",
            "scene": "网络社区",
            "symbolSize": 35,
            "desc": f"从user_basic_information_8表中统计的所有记录总数（N={total_records}）",
            "count": total_records
        })
        node_id += 1

        # 3. 准标识符节点（新增category：qid）
        for qid in qids:
            qid_node_ids[qid] = node_id
            nodes.append({
                "category": "qid",
                "id": node_id,
                "name": qid,
                "scene": "网络社区",
                "symbolSize": 35,
                "desc": f"重识别率分析的准标识符，数据类型为字符串/数值",
                "data_type": "object" if qid in ["user_id", "user_name", "province"] else "int64"
            })
            node_id += 1

        # 4. 组合节点（对应参考格式的equivalence_class，category：reid_combination）
        combo_node_ids = {}  # 存储组合节点ID，用于后续链接构建
        for idx, combo in enumerate(top_combinations, 1):
            combo_key = combo["combination"]
            combo_node_ids[combo_key] = node_id
            # 节点大小：重识别率越高，尺寸越大（30-50区间）
            symbol_size = 30 + min(20, combo["reid_rate"] * 0.2)
            nodes.append({
                "category": "reid_combination",
                "id": node_id,
                "name": f"组合{idx}[{combo_key}]",
                "scene": "网络社区",
                "symbolSize": round(symbol_size, 1),
                "desc": f"准标识符组合{combo_key}，重识别率{combo['reid_rate']}%，可唯一识别{combo['unique_count']}条记录",
                "reid_rate": combo["reid_rate"],
                "unique_count": combo["unique_count"],
                "dimension": combo["dimension"]
            })
            node_id += 1

        # 5. 重识别风险节点（对应参考格式的avg_generalization_degree，category：reid_risk）
        for idx, combo in enumerate(top_combinations, 1):
            combo_key = combo["combination"]
            nodes.append({
                "category": "reid_risk",
                "id": node_id,
                "name": f"{combo_key}\n还原率：{combo['reid_rate']}%",
                "scene": "网络社区",
                "symbolSize": 30.0,
                "desc": f"准标识符组合{combo_key}的重识别风险，还原率{combo['reid_rate']}%",
                "还原率(%)": combo["reid_rate"],
                "可还原": "是" if combo["reid_rate"] > 0 else "否",
                "风险等级": "高" if combo["reid_rate"] > 50 else "中" if combo["reid_rate"] > 0 else "低"
            })
            node_id += 1

        # 构建链接（完全匹配参考格式：含scene、label、link_type）
        links = []

        # 链接1：核心表 → 总记录数
        links.append({
            "scene": "网络社区",
            "source": core_table_id,
            "target": total_count_id,
            "label": {"show": True, "formatter": "统计总记录数"},
            "link_type": "table_to_total_count"
        })

        # 链接2：核心表 → 准标识符
        for qid, qid_id in qid_node_ids.items():
            links.append({
                "scene": "网络社区",
                "source": core_table_id,
                "target": qid_id,
                "label": {"show": True, "formatter": "提取准标识符"},
                "link_type": "table_to_qid"
            })

        # 链接3：准标识符 → 对应组合（一个组合关联多个准标识符）
        for combo in top_combinations:
            combo_key = combo["combination"]
            combo_id = combo_node_ids[combo_key]
            for qid in combo_key.split("+"):
                qid_id = qid_node_ids[qid]
                links.append({
                    "scene": "网络社区",
                    "source": qid_id,
                    "target": combo_id,
                    "label": {"show": True, "formatter": "组成组合"},
                    "link_type": "qid_to_combination"
                })

        # 链接4：组合 → 重识别风险
        risk_node_start_id = combo_node_ids[top_combinations[-1]["combination"]] + 1  # 风险节点起始ID
        for idx, combo in enumerate(top_combinations):
            combo_id = combo_node_ids[combo["combination"]]
            risk_id = risk_node_start_id + idx
            links.append({
                "scene": "网络社区",
                "source": combo_id,
                "target": risk_id,
                "label": {"show": True, "formatter": f"还原率：{combo['reid_rate']}%"},
                "link_type": "combination_to_risk"
            })

        # 构建最终图谱数据（nodes + links，与参考格式一致）
        graph_data = {
            "nodes": nodes,
            "links": links
        }

        # 存储并返回JSON字符串
        json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
        self.store_graph_data(json_str, flag=graph_id)
        return json_str
    
    ### 统计推断还原率
    def buildSimplifiedInferenceGraph(self, max_pairs: int = 10, bg_sample_ratio: float = 0.3, graph_id: int = 19) -> str:
        """
        适配无独立背景表的场景：从核心表中拆分数据模拟背景知识
        :param max_pairs: 最大展示的“QID-SA”推断风险对数量
        :param bg_sample_ratio: 从核心表中拆分作为背景知识的数据比例（0~1，默认0.3）
        :return: 包含nodes和links的JSON字符串
        """
        from scipy.stats import pearsonr, chi2_contingency
        from sqlalchemy import create_engine, text
        import json
        import os
        import pandas as pd
        from scipy import sparse  # 新增稀疏矩阵导入（若用方案3可保留）

        try:
            # 1. 读取核心数据（唯一的原始表）
            core_data = self._Function_Data()
            if core_data.empty:
                raise ValueError("核心数据表（_Function_Data读取）无有效记录")
            total_records = len(core_data)
            if total_records < 10:  # 数据量太少无法拆分
                raise ValueError(f"核心表数据量不足（仅{total_records}条），无法拆分模拟背景知识")

            # 2. 处理背景知识：无独立背景表时，从核心表拆分数据模拟
            bg_data = None
            # 尝试使用bg_url（若配置且有效），否则直接拆分核心表
            if self.bg_url:
                try:
                    # 尝试读取配置的bg_url（兼容CSV/MySQL）
                    if self.bg_url.endswith((".csv", ".txt")):
                        if os.path.exists(self.bg_url):
                            bg_data = pd.read_csv(self.bg_url)
                        else:
                            print(f"配置的背景文件[{self.bg_url}]不存在，将从核心表拆分数据模拟背景知识")
                    else:
                        # 尝试连接MySQL背景表，失败则走拆分逻辑
                        bg_engine = create_engine(self.src_url).connect()
                        bg_table = self.json_address + "_bg"
                        ### bg_query = f"SELECT {', '.join(self.QIDs)} FROM {bg_table}"
                        bg_query = f"SELECT {', '.join([f'`{qid}`' for qid in self.QIDs])} FROM {bg_table}"
                        bg_data = pd.read_sql(sql=text(bg_query), con=bg_engine)
                except Exception as e:
                    print(f"读取配置的背景知识失败：{str(e)}，将从核心表拆分数据模拟")

            # 若背景数据仍为空，则从核心表中随机拆分部分数据作为模拟背景
            if bg_data is None or bg_data.empty:
                # 拆分比例限制在0.1~0.5之间（避免背景数据过多或过少）
                bg_sample_ratio = max(0.1, min(0.5, bg_sample_ratio))
                # 随机抽样（不重复）
                bg_data = core_data.sample(frac=bg_sample_ratio, random_state=42)  # 固定随机种子确保结果可复现
                # 仅保留QID字段（模拟背景知识通常只包含准标识符，不含敏感属性）
                bg_data = bg_data[self.QIDs].copy()
                print(f"已从核心表[{self.json_address}]中随机拆分{bg_sample_ratio*100:.1f}%数据（{len(bg_data)}条）作为模拟背景知识")

            # 3. 核心属性定义（复用Config类）
            qids = self.QIDs
            sas = self.SA
            scene = self.scene
            table_name = self.json_address
            if not qids or not sas:
                raise ValueError("Config类的QIDs（准标识符）或SA（敏感属性）列表不能为空")

            # 4. 计算核心指标（逻辑不变，使用拆分的bg_data）
            qid_coverage = {}
            for qid in qids:
                if qid not in core_data.columns or qid not in bg_data.columns:
                    qid_coverage[qid] = 0.0
                    continue
                # 计算核心数据与模拟背景数据的QID覆盖率
                core_qid_unique = set(core_data[qid].dropna().astype(str))
                bg_qid_unique = set(bg_data[qid].dropna().astype(str))
                if not core_qid_unique:
                    qid_coverage[qid] = 0.0
                    continue
                coverage = round(len(core_qid_unique & bg_qid_unique) / len(core_qid_unique) * 100, 2)
                qid_coverage[qid] = coverage

            # 计算QID与SA的相关性及推断还原率
            inference_pairs = []
            for qid in qids:
                for sa in sas:
                    if qid not in core_data.columns or sa not in core_data.columns:
                        continue
                    valid_data = core_data[[qid, sa]].dropna()
                    if len(valid_data) < 10:
                        continue
                    
                    # 相关性计算（数值型/分类型）
                    if pd.api.types.is_numeric_dtype(valid_data[qid]) and pd.api.types.is_numeric_dtype(valid_data[sa]):
                        corr_coef, _ = pearsonr(valid_data[qid], valid_data[sa])
                        corr_strength = abs(corr_coef)
                    else:
                        # ========== 改进的分类型相关性计算 start ==========
                        def filter_low_freq(series, min_count=5):
                            """过滤出现次数低于min_count的类别，减少交叉表维度"""
                            value_counts = series.value_counts()
                            keep_values = value_counts[value_counts >= min_count].index
                            return series[series.isin(keep_values)]
                        
                        filtered_qid = filter_low_freq(valid_data[qid], min_count=5)
                        filtered_sa = filter_low_freq(valid_data[sa], min_count=5)
                        valid_filtered = pd.DataFrame({qid: filtered_qid, sa: filtered_sa}).dropna()
                        
                        if valid_filtered.empty:
                            continue  # 过滤后无有效数据，跳过该组计算
                        
                        contingency_table = pd.crosstab(valid_filtered[qid], valid_filtered[sa])
                        # ========== 改进的分类型相关性计算 end ==========
                        
                        chi2, _, _, _ = chi2_contingency(contingency_table)
                        corr_strength = min(1.0, round(chi2 / (len(valid_filtered) * (min(contingency_table.shape) - 1)), 3))
                    
                    # 推断还原率
                    strength_coef = 1.0 if corr_strength >= 0.6 else 0.5 if corr_strength >= 0.3 else 0.2
                    reconstruction_rate = round(qid_coverage[qid] * strength_coef, 2)
                    inference_pairs.append({
                        "qid": qid, "sa": sa, "reconstruction_rate": reconstruction_rate,
                        "qid_coverage": qid_coverage[qid], "corr_strength": round(corr_strength, 3)
                    })

            # 筛选top风险对
            inference_pairs.sort(key=lambda x: x["reconstruction_rate"], reverse=True)
            top_inference_pairs = inference_pairs[:max_pairs]

            # 5. 构建节点（与之前一致）
            nodes = []
            node_ids = set()
            node_mapping = {"core_table": None, "bg_data": None, "qid": {}, "sa": {}, "inference": {}}

            # 核心数据表节点
            core_table_id = hash(f"core_table::{table_name}") % 1000000
            if core_table_id not in node_ids:
                nodes.append({
                    "category": "核心数据表",
                    "id": core_table_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 55,
                    "总记录数": total_records
                })
                node_ids.add(core_table_id)
                node_mapping["core_table"] = core_table_id

            # 模拟背景知识节点（标注“模拟”字样）
            bg_name = f"模拟背景（源自{table_name}）"
            bg_data_id = hash(f"bg_data::{bg_name}") % 1000000
            if bg_data_id not in node_ids:
                nodes.append({
                    "category": "背景知识（模拟）",  # 区分模拟背景
                    "id": bg_data_id,
                    "name": bg_name,
                    "scene": scene,
                    "symbolSize": 50,
                    "记录数": len(bg_data),
                    "来源": f"从核心表拆分{bg_sample_ratio*100:.1f}%数据"
                })
                node_ids.add(bg_data_id)
                node_mapping["bg_data"] = bg_data_id

            # 准标识符节点
            for qid in qids:
                qid_id = hash(f"qid::{qid}") % 1000000
                if qid_id not in node_ids:
                    nodes.append({
                        "category": "准标识符",
                        "id": qid_id,
                        "name": qid,
                        "scene": scene,
                        "symbolSize": 35,
                        "背景覆盖率(%)": qid_coverage[qid]
                    })
                    node_ids.add(qid_id)
                    node_mapping["qid"][qid] = qid_id

            # 敏感属性节点
            for sa in sas:
                sa_id = hash(f"sa::{sa}") % 1000000
                if sa_id not in node_ids:
                    nodes.append({
                        "category": "敏感属性",
                        "id": sa_id,
                        "name": sa,
                        "scene": scene,
                        "symbolSize": 40
                    })
                    node_ids.add(sa_id)
                    node_mapping["sa"][sa] = sa_id

            # 推断风险节点
            for pair in top_inference_pairs:
                pair_key = f"inference::{pair['qid']}→{pair['sa']}"
                inf_id = hash(pair_key) % 1000000
                if inf_id not in node_ids:
                    symbol_size = min(30 + pair["reconstruction_rate"] * 0.3, 60)
                    nodes.append({
                        "category": "推断风险",
                        "id": inf_id,
                        "name": f"{pair['qid']}→{pair['sa']}\n还原率：{pair['reconstruction_rate']}%",
                        "scene": scene,
                        "symbolSize": round(symbol_size, 1),
                        "还原率(%)": pair["reconstruction_rate"]
                    })
                    node_ids.add(inf_id)
                    node_mapping["inference"][pair_key] = inf_id

            # 6. 构建链路（与之前一致）
            links = []
            edge_pairs = set()

            # 核心表 ↔ QID/SA
            for qid, qid_id in node_mapping["qid"].items():
                edge_key = frozenset([node_mapping["core_table"], qid_id])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": node_mapping["core_table"],
                        "target": qid_id,
                        "label": {"formatter": "包含QID"}
                    })
                    edge_pairs.add(edge_key)
            for sa, sa_id in node_mapping["sa"].items():
                edge_key = frozenset([node_mapping["core_table"], sa_id])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": node_mapping["core_table"],
                        "target": sa_id,
                        "label": {"formatter": "包含SA"}
                    })
                    edge_pairs.add(edge_key)

            # 模拟背景 ↔ QID
            for qid, qid_id in node_mapping["qid"].items():
                edge_key = frozenset([node_mapping["bg_data"], qid_id])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": node_mapping["bg_data"],
                        "target": qid_id,
                        "label": {"formatter": f"覆盖率：{qid_coverage[qid]}%"}
                    })
                    edge_pairs.add(edge_key)

            # QID/SA ↔ 推断风险
            for pair in top_inference_pairs:
                qid, sa = pair["qid"], pair["sa"]
                pair_key = f"inference::{qid}→{sa}"
                inf_id = node_mapping["inference"].get(pair_key)
                qid_id = node_mapping["qid"].get(qid)
                sa_id = node_mapping["sa"].get(sa)
                if not inf_id or not qid_id or not sa_id:
                    continue
                # QID→推断风险
                edge_key_qid = frozenset([qid_id, inf_id])
                if edge_key_qid not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": qid_id,
                        "target": inf_id,
                        "label": {"formatter": "关联输入"}
                    })
                    edge_pairs.add(edge_key_qid)
                # SA→推断风险
                edge_key_sa = frozenset([sa_id, inf_id])
                if edge_key_sa not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": sa_id,
                        "target": inf_id,
                        "label": {"formatter": "推断目标"}
                    })
                    edge_pairs.add(edge_key_sa)

            # 7. 输出结果
            graph_data = {"nodes": nodes, "links": links}
            json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            return json_str

        except Exception as e:
            error_msg = f"生成统计推断还原率图谱时发生错误: {str(e)}"
            print(error_msg)
            return json.dumps({"error": error_msg})      

    ### 差分攻击还原率
    def buildDifferentialAttackGraph(self, target_record_idx: int = 0, max_sa: int = 5, graph_id: int = 20) -> str:
        """
        单表场景差分攻击还原率图谱：从核心表拆分数据模拟攻击所需的两个数据集，仅输出nodes和links
        :param target_record_idx: 目标记录的索引（攻击对象，默认第0条）
        :param max_sa: 最大展示的敏感属性风险数量
        :return: 包含nodes和links的JSON字符串（与现有图谱格式一致）
        """
        try:
            import pandas as pd
            import numpy as np

            # 1. 读取唯一的核心表数据
            core_data = self._Function_Data()
            if core_data.empty:
                raise ValueError("核心数据表（_Function_Data读取）无有效记录")
            total_records = len(core_data)
            if total_records < 2:  # 至少2条记录（含/不含目标各1条）
                raise ValueError(f"核心表数据量不足（仅{total_records}条），无法构建差分攻击对比数据集")
            table_name = self.json_address or "core_table"
            scene = self.scene or "default"

            # 2. 单表拆分：构建差分攻击的两个对比数据集（D和D'）
            # 2.1 选择攻击目标记录（索引无效时随机选）
            if target_record_idx < 0 or target_record_idx >= total_records:
                target_record_idx = np.random.randint(0, total_records)
                print(f"目标记录索引[{target_record_idx}]无效，随机选择第{target_record_idx}条作为攻击目标")
            target_record = core_data.iloc[target_record_idx]  # 攻击目标的真实记录

            # 2.2 构建D（含目标）和D'（不含目标）
            D = core_data.copy()  # 完整数据（含目标）
            D_prime = D.drop(index=target_record_idx).reset_index(drop=True)  # 剔除目标后的数据

            # 3. 模拟脱敏操作（单表内对D和D'执行相同逻辑，贴近真实场景）
            def mock_desensitize(data: pd.DataFrame) -> pd.DataFrame:
                """模拟通用脱敏逻辑：仅泛化QID，保留SA（真实场景需替换为实际脱敏算法）"""
                desensitized = data.copy()
                qids = self.QIDs
                for qid in qids:
                    if qid not in desensitized.columns:
                        continue
                    # 字符串QID（如user_id、province）：截取前N位泛化
                    if pd.api.types.is_object_dtype(desensitized[qid]):
                        desensitized[qid] = desensitized[qid].astype(str).str[:6]  # 如user_id: 12345678→123456
                    # 数值QID（如age）：按区间分组泛化
                    elif pd.api.types.is_numeric_dtype(desensitized[qid]):
                        desensitized[qid] = (desensitized[qid] // 10) * 10  # 如age:25→20，37→30
                return desensitized

            # 对两个数据集执行相同脱敏，得到对比用的脱敏结果
            D_safe = mock_desensitize(D)  # D的脱敏结果
            D_prime_safe = mock_desensitize(D_prime)  # D'的脱敏结果

            # 4. 计算差分攻击核心指标（还原率）
            sa_list = self.SA
            if not sa_list:
                raise ValueError("Config类的SA（敏感属性）列表不能为空")
            
            # 4.1 对比脱敏结果，找到差异组（仅D_safe有而D_prime_safe无的QID泛化组）
            # 按QID分组统计，找到D_safe独有的组
            D_qid_groups = D_safe.groupby(self.QIDs).size().reset_index(name='count')
            D_prime_qid_groups = D_prime_safe.groupby(self.QIDs).size().reset_index(name='count')
            # 合并对比，筛选D独有的组
            diff_groups = pd.merge(
                D_qid_groups, D_prime_qid_groups, 
                on=self.QIDs, how='left', suffixes=('_D', '_Dp')
            ).query("count_Dp.isna()")  # count_Dp为空表示D_prime无此组
            has_diff = not diff_groups.empty

            # 4.2 计算每个敏感属性的还原率
            differential_metrics = []
            for sa in sa_list:
                if sa not in D.columns:
                    differential_metrics.append({
                        "sa": sa,
                        "还原率(%)": 0.0,
                        "可还原": False,
                        "目标值": "N/A",
                        "原因": "属性不在核心表中"
                    })
                    continue
                
                # 核心还原逻辑：差异组对应目标记录，组内唯一则可100%还原
                if has_diff:
                    # 取第一个差异组（通常仅1个，对应目标记录）
                    diff_qid = diff_groups[self.QIDs].iloc[0]
                    # 找到差异组在原始数据D中对应的所有记录
                    original_diff_records = D[(D[self.QIDs] == diff_qid).all(axis=1)]
                    # 目标记录是否在差异组中（理论上一定在）
                    target_in_diff = (original_diff_records.index == target_record_idx).any()
                    
                    if target_in_diff:
                        # 还原率 = 1 / 组内记录数 × 100%（组内唯一则100%）
                        group_size = len(original_diff_records)
                        reconstruction_rate = round(100.0 / group_size, 2)
                        can_reconstruct = (group_size == 1)  # 组内唯一则可确定还原
                        reason = f"差异组含{group_size}条记录，{'可唯一还原' if can_reconstruct else '还原概率低'}"
                    else:
                        reconstruction_rate = 0.0
                        can_reconstruct = False
                        reason = "目标记录不在差异组中，无法定位"
                else:
                    reconstruction_rate = 0.0
                    can_reconstruct = False
                    reason = "脱敏结果无差异，无法通过差分攻击定位"
                
                differential_metrics.append({
                    "sa": sa,
                    "还原率(%)": reconstruction_rate,
                    "可还原": can_reconstruct,
                    "目标值": str(target_record[sa]) if sa in target_record.index else "N/A",
                    "原因": reason
                })

            # 4.3 计算整体还原率（可还原SA数/总SA数）
            total_reconstructable = sum(1 for m in differential_metrics if m["可还原"])
            overall_rate = round((total_reconstructable / len(sa_list)) * 100, 2) if sa_list else 0.0

            # 筛选风险最高的前N个敏感属性（按还原率排序）
            differential_metrics.sort(key=lambda x: x["还原率(%)"], reverse=True)
            top_sa_metrics = differential_metrics[:max_sa]

            # 5. 构建图谱节点（适配现有格式，分类清晰）
            nodes = []
            node_ids = set()
            node_mapping = {
                "core_table": None,       # 原始核心表（根节点）
                "D": None,                # 数据集D（含目标）
                "D_prime": None,          # 数据集D'（不含目标）
                "D_safe": None,           # 脱敏后D
                "D_prime_safe": None,     # 脱敏后D'
                "target_record": None,    # 目标记录（攻击对象）
                "qid": {},                # 准标识符
                "sa": {},                 # 敏感属性
                "diff_risk": {}           # 差分攻击风险节点
            }

            # 5.1 核心表节点（唯一原始表）
            core_table_id = hash(f"core::{table_name}") % 1000000
            if core_table_id not in node_ids:
                nodes.append({
                    "category": "核心数据表",
                    "id": core_table_id,
                    "name": f"表：{table_name}",
                    "scene": scene,
                    "symbolSize": 60,
                    "总记录数": total_records,
                    "差分攻击目标": f"第{target_record_idx}条记录"
                })
                node_ids.add(core_table_id)
                node_mapping["core_table"] = core_table_id

            # 5.2 对比数据集节点（D和D'，均源自核心表）
            # D节点（含目标）
            D_id = hash(f"D::{table_name}") % 1000000
            if D_id not in node_ids:
                nodes.append({
                    "category": "对比数据集（含目标）",
                    "id": D_id,
                    "name": f"数据集D\n（{len(D)}条，含目标）",
                    "scene": scene,
                    "symbolSize": 52,
                    "来源": f"核心表完整数据"
                })
                node_ids.add(D_id)
                node_mapping["D"] = D_id

            # D'节点（不含目标）
            D_prime_id = hash(f"Dprime::{table_name}") % 1000000
            if D_prime_id not in node_ids:
                nodes.append({
                    "category": "对比数据集（不含目标）",
                    "id": D_prime_id,
                    "name": f"数据集D'\n（{len(D_prime)}条，剔除目标）",
                    "scene": scene,
                    "symbolSize": 50,
                    "来源": f"核心表剔除第{target_record_idx}条"
                })
                node_ids.add(D_prime_id)
                node_mapping["D_prime"] = D_prime_id

            # 5.3 脱敏后数据集节点（D_safe和D'_safe）
            D_safe_id = hash(f"Dsafe::{table_name}") % 1000000
            if D_safe_id not in node_ids:
                nodes.append({
                    "category": "脱敏后数据集",
                    "id": D_safe_id,
                    "name": f"脱敏D_safe\n（{len(D_safe)}条）",
                    "scene": scene,
                    "symbolSize": 48,
                    "脱敏操作": "QID泛化（字符串前6位/数值10区间）"
                })
                node_ids.add(D_safe_id)
                node_mapping["D_safe"] = D_safe_id

            D_prime_safe_id = hash(f"Dprimesafe::{table_name}") % 1000000
            if D_prime_safe_id not in node_ids:
                nodes.append({
                    "category": "脱敏后数据集",
                    "id": D_prime_safe_id,
                    "name": f"脱敏D'_safe\n（{len(D_prime_safe)}条）",
                    "scene": scene,
                    "symbolSize": 46,
                    "脱敏操作": "与D_safe一致"
                })
                node_ids.add(D_prime_safe_id)
                node_mapping["D_prime_safe"] = D_prime_safe_id

            # 5.4 目标记录节点（攻击对象）
            target_id = hash(f"target::{target_record_idx}") % 1000000
            if target_id not in node_ids:
                nodes.append({
                    "category": "攻击目标记录",
                    "id": target_id,
                    "name": f"目标记录（索引{target_record_idx}）",
                    "scene": scene,
                    "symbolSize": 42,
                    "QID特征": ", ".join([f"{q}:{target_record[q]}" for q in self.QIDs if q in target_record.index])
                })
                node_ids.add(target_id)
                node_mapping["target_record"] = target_id

            # 5.5 准标识符节点（关联载体）
            for qid in self.QIDs:
                qid_id = hash(f"qid::{qid}") % 1000000
                if qid_id not in node_ids:
                    nodes.append({
                        "category": "准标识符",
                        "id": qid_id,
                        "name": qid,
                        "scene": scene,
                        "symbolSize": 35,
                        "数据类型": str(core_data[qid].dtype) if qid in core_data.columns else "unknown",
                        "作用": "脱敏差异对比的核心载体"
                    })
                    node_ids.add(qid_id)
                    node_mapping["qid"][qid] = qid_id

            # 5.6 敏感属性节点（攻击目标）
            for sa in sa_list:
                sa_id = hash(f"sa::{sa}") % 1000000
                if sa_id not in node_ids:
                    nodes.append({
                        "category": "敏感属性",
                        "id": sa_id,
                        "name": sa,
                        "scene": scene,
                        "symbolSize": 40,
                        "数据类型": str(core_data[sa].dtype) if sa in core_data.columns else "unknown",
                        "目标记录值": str(target_record[sa]) if sa in target_record.index else "N/A"
                    })
                    node_ids.add(sa_id)
                    node_mapping["sa"][sa] = sa_id

            # 5.7 差分攻击风险节点（核心风险展示）
            for metric in top_sa_metrics:
                sa = metric["sa"]
                risk_key = f"diff_risk::{sa}"
                risk_id = hash(risk_key) % 1000000
                if risk_id not in node_ids:
                    # 节点大小与还原率正相关（30~60）
                    symbol_size = min(30 + metric["还原率(%)"] * 0.3, 60)
                    nodes.append({
                        "category": "差分攻击风险",
                        "id": risk_id,
                        "name": f"{sa}\n还原率：{metric['还原率(%)']}%",
                        "scene": scene,
                        "symbolSize": round(symbol_size, 1),
                        "还原率(%)": metric["还原率(%)"],
                        "可还原": "是" if metric["可还原"] else "否",
                        "风险等级": "极高" if metric["还原率(%)"] >= 80 else "高" if metric["还原率(%)"] >= 50 else "中"
                    })
                    node_ids.add(risk_id)
                    node_mapping["diff_risk"][sa] = risk_id

            # 6. 构建链路（展示差分攻击的完整逻辑流）
            links = []
            edge_pairs = set()  # 确保链路唯一

            # 6.1 核心表 → 对比数据集（D和D'均源自核心表）
            for target_node, target_name in [(node_mapping["D"], "D"), (node_mapping["D_prime"], "D'")]:
                edge_key = frozenset([node_mapping["core_table"], target_node])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": node_mapping["core_table"],
                        "target": target_node,
                        "label": {"formatter": f"拆分生成{target_name}"}
                    })
                    edge_pairs.add(edge_key)

            # 6.2 对比数据集 → 脱敏后数据集（D→D_safe，D'→D'_safe）
            for src_node, target_node, src_name in [
                (node_mapping["D"], node_mapping["D_safe"], "D"),
                (node_mapping["D_prime"], node_mapping["D_prime_safe"], "D'")
            ]:
                edge_key = frozenset([src_node, target_node])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": src_node,
                        "target": target_node,
                        "label": {"formatter": f"脱敏生成{src_name}_safe"}
                    })
                    edge_pairs.add(edge_key)

            # 6.3 对比数据集 → 目标记录（D包含目标，D'不包含）
            # D→目标记录（包含）
            edge_key_d_target = frozenset([node_mapping["D"], node_mapping["target_record"]])
            if edge_key_d_target not in edge_pairs:
                links.append({
                    "scene": scene,
                    "source": node_mapping["D"],
                    "target": node_mapping["target_record"],
                    "label": {"formatter": "包含目标记录"}
                })
                edge_pairs.add(edge_key_d_target)

            # D'→目标记录（不包含）
            edge_key_dp_target = frozenset([node_mapping["D_prime"], node_mapping["target_record"]])
            if edge_key_dp_target not in edge_pairs:
                links.append({
                    "scene": scene,
                    "source": node_mapping["D_prime"],
                    "target": node_mapping["target_record"],
                    "label": {"formatter": "剔除目标记录"}
                })
                edge_pairs.add(edge_key_dp_target)

            # 6.4 准标识符 → 脱敏数据集（QID是脱敏差异的核心）
            for qid, qid_id in node_mapping["qid"].items():
                for safe_node in [node_mapping["D_safe"], node_mapping["D_prime_safe"]]:
                    edge_key = frozenset([qid_id, safe_node])
                    if edge_key not in edge_pairs:
                        links.append({
                            "scene": scene,
                            "source": qid_id,
                            "target": safe_node,
                            "label": {"formatter": "作为脱敏对比依据"}
                        })
                        edge_pairs.add(edge_key)

            # 6.5 敏感属性 → 差分风险节点（风险对应具体SA）
            for metric in top_sa_metrics:
                sa = metric["sa"]
                sa_id = node_mapping["sa"].get(sa)
                risk_id = node_mapping["diff_risk"].get(sa)
                if not sa_id or not risk_id:
                    continue
                edge_key = frozenset([sa_id, risk_id])
                if edge_key not in edge_pairs:
                    links.append({
                        "scene": scene,
                        "source": sa_id,
                        "target": risk_id,
                        "label": {"formatter": f"还原率：{metric['还原率(%)']}%"}
                    })
                    edge_pairs.add(edge_key)

            # 6.6 脱敏数据集 → 差分风险节点（差异导致风险）
            edge_key_diff_risk = frozenset([node_mapping["D_safe"], node_mapping["D_prime_safe"]])
            if edge_key_diff_risk not in edge_pairs:
                links.append({
                    "scene": scene,
                    "source": node_mapping["D_safe"],
                    "target": node_mapping["D_prime_safe"],
                    "label": {"formatter": f"脱敏差异组：{len(diff_groups)}个"} if has_diff else {"formatter": "无脱敏差异"},
                    "差异组数量": len(diff_groups) if has_diff else 0
                })
                edge_pairs.add(edge_key_diff_risk)

            # 7. 组装图谱结果（仅nodes和links）
            graph_data = {
                "nodes": nodes,
                "links": links
            }

            # 8. 存储并返回JSON
            json_str = json.dumps(graph_data, ensure_ascii=False, indent=2)
            self.store_graph_data(json_str, graph_id)
            print(f"差分攻击图谱生成完成！整体还原率：{overall_rate}%，风险最高的{len(top_sa_metrics)}个敏感属性已展示")
            return json_str

        except Exception as e:
            error_msg = f"生成差分攻击图谱时发生错误: {str(e)}"
            print(error_msg)
            return json.dumps({"error": error_msg})

    def refactoring(self):
        from tqdm import tqdm
        import random
        # 1. 定义所有任务：将每个绘图任务封装为列表元素（便于统计总数和循环执行）
        tasks = [
            # 任务格式：(任务名称, 随机数范围, 执行的方法)
            ("构建准标识符维度图", (45, 55), self.buildQuasiRepresentationDimensionGraph),
            ("构建敏感属性图", (45, 55), self.buildSensitiveAttributesGraph),
            ("构建最小等价类图", (45, 55), self.buildSmallestEquivalenceClasses),
            ("构建固有隐私图", None, self.buildInherentPrivacyGraph),  # 无随机数的任务
            ("获取等价类基础数据", None, self.getEquivalenceClassBasis),
            ("构建最小敏感属性组图", (15, 25), self.buildMinSensitiveAttributeGroups),
            ("构建敏感频率Top/Bottom组图", (15, 25), self.buildSensitiveFreqTopBottomGroupsGraph),
            ("构建Top敏感比例组图", (15, 25), self.buildTopSensitiveRatioGroups),
            ("构建Top CDM贡献组图", (45, 55), self.buildTopCDMContributionGroups),
            ("构建支持率图", None, self.buildSupRatioGraph),
            ("构建平均泛化程度图", (15, 25), self.buildAvgGeneralizationDegreeGraph),
            ("构建可用性平均泛化程度图", (15, 25), self.buildAverageGeneralizationDegreeUsability),
            ("构建唯一性比例图", (15, 25), self.buildUniquenessRatioGraph),
            ("构建NCP图", (15, 25), self.buildNCPGraph),
            ("构建基于熵的损失图", (15, 25), self.buildEntropyBasedLossGraph),
            ("构建分布泄露图", (10, 15), self.buildDistributionLeakageGraph),
            ("构建熵泄露图", (8, 15), self.buildEntropyLeakageGraph),
            ("构建隐私增益图", (10, 15), self.buildPrivacyGainGraph),
            ("构建特定格式KL散度图", (15, 25), self.buildKLDivergenceGraphWithSpecFormat),
            ("构建重识别风险图", (40, 60), self.buildReidentificationRiskGraph),
            ("构建QID风险图", (45, 55), self.buildQIDRiskGraph),
            ("构建整体风险图", None, self.buildOverallRiskGraph),
            ("构建重识别率图", (15, 25), self.buildReidentificationRateGraph),
            ("构建简化推断图", (15, 25), self.buildSimplifiedInferenceGraph),  # 你之前改进的方法
            ("构建差分攻击图", (15, 25), self.buildDifferentialAttackGraph)
        ]

        # 2. 初始化进度条：总数为任务列表长度，显示任务名称
        with tqdm(total=len(tasks), desc="隐私图谱构建进度", unit="任务") as pbar:
            for task_name, rand_range, func in tasks:
                # 更新进度条描述，显示当前执行的任务
                pbar.set_postfix({"当前任务": task_name})
                
                # 根据任务是否需要随机数，执行对应方法
                if rand_range:
                    # 生成指定范围的随机整数
                    random_int = random.randint(rand_range[0], rand_range[1])
                    func(random_int)  # 带随机数参数的方法
                else:
                    func()  # 无参数的方法
                
                # 每个任务完成后，进度条+1
                pbar.update(1)
        
        # 所有任务完成后，输出提示
        print("=" * 50)
        print("✅ 所有隐私图谱构建任务已全部完成！")
        print("=" * 50)

    def get_graph(self, graph_id: int) -> str:
        match graph_id:
            case 7:
                return self.buildTopCDMContributionGroups()
            case 8:
                return self.buildAverageGeneralizationDegreeUsability()
            case 9:
                return self.buildNCPGraph()
            case 10:
                return self.buildSupRatioGraph()
            case 11:
                return self.buildEntropyBasedLossGraph()
            case 12:
                return self.buildUniquenessRatioGraph()
            case 13:
                return self.buildQuasiRepresentationDimensionGraph()
            case 14:
                return self.buildSensitiveAttributesGraph()
            case 15:
                return self.buildAvgGeneralizationDegreeGraph()
            case 16:
                return self.buildInherentPrivacyGraph()
            case 18:
                return self.buildReidentificationRateGraph()
            case 19:
                return self.buildSimplifiedInferenceGraph()
            case 20:
                return self.buildDifferentialAttackGraph()
            case 21:
                return self.buildDistributionLeakageGraph()
            case 22:
                return self.buildEntropyLeakageGraph()
            case 23:
                return self.buildPrivacyGainGraph()
            case 24:
                return self.buildKLDivergenceGraphWithSpecFormat()
            case 25:
                return self.buildReidentificationRiskGraph()
            case 26:
                return self.buildQIDRiskGraph()
            case 27:
                return self.buildOverallRiskGraph()
            case 28:
                return self.getEquivalenceClassBasis()
            case 29:
                return self.buildMinSensitiveAttributeGroups()
            case 30:
                print("Building Sensitive Frequency Top/Bottom Groups Graph...")
                return self.buildSensitiveFreqTopBottomGroupsGraph()
            case _:
                # 处理其他 graph_id 的默认情况
                return f"Unknown graph with id: {graph_id}"


if __name__ == '__main__':
    parse_parameters("12321f04-ae5a-4158-9256-faf390d41379")