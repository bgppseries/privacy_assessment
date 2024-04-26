import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
import Levenshtein
import glob
import math
import matplotlib.pyplot as plt
import numpy as np
#from .config import Config
#from celery_task.config import Config
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
    'journ_att':['uuid','sex','id_number','phone'],
    'mark_att':{},
    'prose_att':{},
    'all':['name','sex','id_number','phone','marital_status','email','bank_number']
}
SA={
    'info':['address']
}

class Risk:
    
    def __init__(self,Config) -> None:
        self.Config=Config


def uniqueness_assess(src_url,to_url):
    df=pd.read_json(to_url)
    uniqueness_df=df[df.duplicated(subset=QIDs['all'],keep=False)]
    group=df.groupby(QIDs['all'])
    #print(uniqueness_df)
    print(group.any())
    l_values=group[SA['info']].nunique()
    print("重识别风险评估任务执行完成，脱敏方案：",to_url.split('/')[-1],"l-多样性:",l_values.min())
    magnitude = math.floor(math.log10(abs(uniqueness_df.shape[0]+1)))
    p="数据唯一性：{:."+str(magnitude)+"f}%"
    print(p.format(((1-uniqueness_df.shape[0]/df.shape[0])*100)))
    

def test_uniqueness_assess():
    ### 测试函数
    # pattern='./data/to_assess/info_*.json'
    pattern='./data/json/3/hotel_*.json'
    print("唯一性评估测试：")
    print("唯一性说明：")
    print("             （1-重复元组数/总元组数）*100%")
    file_names=glob.glob(pattern)
    for file in file_names:
        uniqueness_assess("./data/to_assess/info.json",file)

def statistics_risk_assess(df):
    """
        df: 数据流，或者分布直方图
    """

def link_risk_assess(src_url,bg_url):
    """
        链接攻击
    """
    

def reidentity_risk_assess(src_url,to_url,sampling_ratio,nan_ratio):
    """
        重识别风险计算，采用距离向量
        config:
            QIDs list
            SA   list
            ID   list
        age 要特殊处理
    """
    df=pd.read_json(src_url)
    hdf=pd.read_json(to_url)
    # 使用列表推导式来选取存在的列
    j_df = df[[col for col in QIDs['journ_att'] if col in df.columns]]
    hdf = hdf[[col for col in QIDs['journ_att'] if col in hdf.columns]]
    
    bg=j_df.sample(frac=sampling_ratio)
    bg=bg.astype(object)

    str_nan = int(len(bg) * nan_ratio)
    for col in bg.columns:
        if col!='uuid':
            random_rows = np.random.choice(bg.index, size=str_nan, replace=False)
            bg.loc[random_rows, col] = np.nan
    bg=bg.astype(object)
    hdf=hdf.astype(object)
    print("背景知识库构建完成：",bg)
    # 计算距离矩阵
    distances = cdist(bg.loc[:,bg.columns!='uuid'], hdf.loc[:,hdf.columns!='uuid'],lambda x, y: distance(x, y))
    # 获取每个 bg 数据与 df 中最近的 k 个数据的索引
    k = 3
    top_k_indices = distances.argsort(axis=1)[:, :k]
    # 提取最近的 k 个数据
    candidates_index=[]
    for bg_index in range(len(bg)):
        top_indices = top_k_indices[bg_index]
        #candidate = hdf.iloc[[top_indices]]
        for ind in top_indices:
            candidates_index.append(ind)
    candidates=hdf.loc[candidates_index]
    #计算采样率
    sum=0
    for _,row in candidates.iterrows():
        if check_in(row['uuid'],bg):
            print("识别成功",row)
            sum=sum+1
    to_url=to_url.split('/')[-1]
    print("记者模型重识别风险评估任务执行完成，脱敏方案：",to_url,"采样",sampling_ratio*j_df.shape[0],"个数据","背景信息残缺率为：",nan_ratio,"重识别率为：{:.5f}%".format(sum/(sampling_ratio*j_df.shape[0])*100))
    risk=sum/(sampling_ratio*j_df.shape[0])
    return risk,sum

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
    
    # pattern='./data/to_assess/info_*.json'
    pattern='./data/json/3/hotel_*.json'
    file_names=glob.glob(pattern)
    sampling_ratio=[0.0001,0.0002,0.0003,0.0004,0.0005,0.0006,0.0007,0.0008,0.0009,0.0010]
    nan_ratio = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]  # 设置为 60%
    for file in file_names:
        risk=[]
        nums=[]
        r=[]
        num_m=[]
        for s in sampling_ratio:
            for i in range(5):
                res,num=reidentity_risk_assess("./data/to_assess/info.json",file,s,0)
                print(i,'重识别率',res,'数量',num)
                r.append(res)
                num_m.append(num)
            risk.append(np.mean(r))
            nums.append(np.mean(num_m))
        # plt.plot(nan_ratio,risk,marker='o')
        title='脱敏方案：'+file+'列缺损率与风险值关系图'
        # plt.title(title)
        # plt.xlabel('采样率')
        # plt.ylabel('重识别率')
        # plt.grid(True)
        # plt.show()
        print(title,risk,np.mean(risk),nums,np.mean(nums))

if __name__=='__main__':
    #Config()
    #test_uniqueness_assess()

    test_reid_risk()

    # pattern='./data/to_assess/info_*.json'
    # file_names=glob.glob(pattern)
    
    # for file in file_names:
    #     df=pd.read_json(file)
    #     stats=df.describe(include=['object'])
    #     group=df.groupby(QIDs['all'])#.size().reset_index(name='count')
    #     for key, value in group:
    #         #print(key)
    #         print(value)
    #         print(value.size)
    #         print("end")
