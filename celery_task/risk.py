import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
import Levenshtein
import glob
import math
import matplotlib.pyplot as plt
import numpy as np
import pymysql
from sqlalchemy import create_engine,text
""""
    隐私风险度量
        目前假定数据源格式为json
        重标识攻击：
            背景知识假设：
                不同的攻击意图，背景知识也可以不同
                先横着截取，之后再挖空
                背景知识水平
                    一级：行采样率：0.0005  列残缺率0.5 
            节点相似性计算：
                1.距离dis
                2.向量？
"""


# 设置随机种子，确保每次运行结果一致
#np.random.seed(0)

##  刚需原始文件
##  

QIDs={
    'journ_att':['uuid','sex','age','id_number','phone'],
    'mark_att':{},
    'prose_att':{},
    # 'all':['name','sex','id_number','phone','marital_status','email','bank_number']
    'all':['sex', 'age', 'id_number','phone']
}
SA={
    # 'info':['address']
    'info':['']
}

def uniqueness_assess(to_url,qid,sa):
    ##  计算数据唯一性
    ##  对单个文件操作
    df=pd.read_json(to_url)
    uniqueness_df=df[df.duplicated(subset=qid,keep=False)]
    group=df.groupby(qid)
    #print(uniqueness_df)
    print(group.any())
    l_values=group[sa].nunique()
    print("重识别风险评估任务执行完成，脱敏方案：",to_url.split('/')[-1],"l-多样性:",l_values.min())
    # 控制打印的数据唯一性指标的精度
    magnitude = math.floor(math.log10(abs(uniqueness_df.shape[0]+1)))
    p="数据唯一性：{:."+str(magnitude)+"f}%"
    print(p.format(((1-uniqueness_df.shape[0]/df.shape[0])*100)))
    l=l_values.min## l-多样性
    uniqueness=(1-uniqueness_df.shape[0]/df.shape[0])*100 ## 数据唯一性
    return l,uniqueness
    

def test_uniqueness_assess():
    ### 测试函数
    # pattern='./data/to_assess/info_*.json'
    # 数据路径↓
    pattern='./data/json/3/hotel_*.json'
    print("唯一性评估测试：")
    print("唯一性说明：")
    print("             （1-重复元组数/总元组数）*100%")
    file_names=glob.glob(pattern)
    qid=['sex', 'age', 'id_number','phone']
    sa=['hotel_name']
    for file in file_names:
        uniqueness_assess(file,qid,sa)
        

def link_risk_assess(src_url,bg_url):
    """
        链接攻击
        处理json数据，csv和json还不一样，淦
    """
    df_bg=pd.read_json(bg_url)
    df_at=pd.read_json(src_url)

    # 获取两个数据框的列名的交集作为连接的列
    common_columns = list(set(df_at.columns) & set(df_bg.columns))
    print(common_columns)
    # 使用 merge 函数将两个数据框连接起来
    merged_data = pd.merge(df_bg, df_at, on=common_columns)

    # 返回成功链接的结果
    print(merged_data)
    
def test_link_risk_assess():
    ### 测试函数
    pattern='./data/json/3/hotel_*.json'
    file_names=glob.glob(pattern)
    for file in file_names:
        link_risk_assess('./data/to_assess/info_0_0_0.json',file)

def reidentity_risk_assess(worker_id,src_dburl,src_table,to_dburl,to_table,qids,sa,id,k,sampling_ratio,nan_ratio):
    """
        重识别风险计算，采用距离向量
        config:
            QIDs list
            SA   list
            ID   list
            k    表示最近k个候选键
            
        age 要特殊处理
    """
    print("评估开始：","任务id",worker_id,"qids:",qids,"sa",sa,id,'原始文件',src_table,'脱敏后文件',to_table)

    ### 创建数据库引擎并连接数据库
    query = f"SELECT * FROM {src_table}"
    # 创建SQLAlchemy的Engine对象
    engine = create_engine(src_dburl).connect()
    df=pd.read_sql(sql=text(query),con=engine)

    query = f"SELECT * FROM {to_table}"
    engine=create_engine(to_dburl).connect()
    tuomindf=pd.read_sql(sql=text(query),con=engine)
    # 使用列表推导式来选取存在的列
    ##2024.12.30 对于 qq 中的每一个列名 col，如果 col 在 df 的列名中，则保留该列，最终得到一个新的 DataFrame j_df
    qq=qids+id
    j_df = df[[col for col in qq if col in df.columns]]
    hdf = tuomindf[[col for col in qq if col in tuomindf.columns]]
    
    bg=j_df.sample(frac=sampling_ratio)
    bg=bg.astype(object)

    str_nan = int(len(bg) * nan_ratio)
    for col in bg.columns:
        ##  理论上没问题
        if col not in id:
            random_rows = np.random.choice(bg.index, size=str_nan, replace=False)
            bg.loc[random_rows, col] = np.nan
    
    bg=bg.astype(object)
    hdf=hdf.astype(object)
    # 计算距离矩阵
    distances = cdist(bg.loc[:,qids], hdf.loc[:,qids],lambda x, y: distance(x, y))
    # 获取每个 bg 数据与 df 中最近的 k 个数据的索引

    top_k_indices = distances.argsort(axis=1)[:, :k]
    # 提取最近的 k 个数据
    candidates=[]

    for bg_index in range(len(bg)):
        top_indices = top_k_indices[bg_index]
        candidates_index=[]
        for ind in top_indices:
            candidates_index.append(ind)
        candidates.append(candidates_index)
    #计算采样率
    sum=0
    non_re_id_index=[]
    for bg_index in range(len(bg)):
        candidates_bg_index=hdf.loc[candidates[bg_index]]
        tmp=sum
        for _,row in candidates_bg_index.iterrows():
            for i in id:
                if bg.iloc[bg_index][i]==row[i]:
                    sum=sum+1
        if tmp == sum:
            non_re_id_index.append(bg_index)
            tmp=sum
    #计算统计推断效率
    if len(non_re_id_index)==0:
        statistics_risk=0
    else:
        possible=0
        for bg_index in non_re_id_index:
            candidates_bg_index=tuomindf.loc[candidates[bg_index]]
            for _,row in candidates_bg_index.iterrows():
                i=0
                for a in sa:
                    if df.iloc[bg_index][a]==row[a]:
                        i=i+1
                        possible=possible+1
                if i == len(sa):
                    i=i
        statistics_risk=possible/(len(non_re_id_index)*k)

    print("重识别风险评估任务执行完成，脱敏方案：",to_table,"采样",sampling_ratio*j_df.shape[0],"个数据","背景信息残缺率为：",nan_ratio,"重识别率为：{:.5f}%".format(sum/(sampling_ratio*j_df.shape[0])*100))
    print("未重识别的数据统计推断攻击还原率为：",statistics_risk)
    risk=sum/(sampling_ratio*j_df.shape[0])
    # risk是重识别lv
    # statistics_risk是统计推断还原率
    from .config import Send_result
    Send_result(worker_id=worker_id,res_dict={
        "重识别率":risk,
        "统计推断还原率":statistics_risk
    })
    return risk,statistics_risk

def distance(x,y):
    ssum=0
    for s1,s2 in zip(np.nditer(x, flags=['refs_ok']), np.nditer(y, flags=['refs_ok'])):
        # 将numpy对象转换为字符串
        str1, str2 = str(s1), str(s2)
        # 使用zip和sum计算匹配的字符数
        ssum += sum(1 for c1, c2 in zip(str1, str2) if c1 != c2)
    return ssum

def check_in(uuid,df)->bool:
    for _,row in df.iterrows():
        if row['uuid']==uuid:
            #print("针对",uuid,"的用户信息",row,"已被重识别：")
            return True
    return False

def test_reid_risk():
    ### 测试函数
    
    sampling_ratio=[0.0001,0.0002,0.0003,0.0004,0.0005,0.0006,0.0007,0.0008,0.0009,0.0010]
    nan_ratio = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]  # 设置为 60%
    q=['sex','age','id_number','phone']
    idd=['uuid']
    sa=['hotel_name']
    url='mysql+pymysql://root:784512@localhost:3306/local_test'
    table_name = "local_test_hotel_1_5_2"
    un_table_name = "local_test_hotel_0_0_0"
    res,num=reidentity_risk_assess(1213123,url,un_table_name,url,table_name,q,sa,idd,2,0.0004,0)
    print('重识别率',res,'统计推断还原率',num)

if __name__=='__main__':
    # test_uniqueness_assess()
    # test_link_risk_assess()
    test_reid_risk()
