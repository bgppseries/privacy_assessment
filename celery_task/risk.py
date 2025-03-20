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
    # print("评估开始：","任务id",worker_id,"qids:",qids,"sa",sa,id,'原始文件',src_table,'脱敏后文件',to_table)

    
    query = f"SELECT * FROM {src_table}"
    # 创建SQLAlchemy的Engine对象
    engine = create_engine(src_dburl).connect()
    df=pd.read_sql(sql=text(query),con=engine)

    query = f"SELECT * FROM {to_table}"
    engine=create_engine(to_dburl).connect()
    tuomindf=pd.read_sql(sql=text(query),con=engine)
    # 使用列表推导式来选取存在的列
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

    # print("重识别风险评估任务执行完成，脱敏方案：",to_table,"采样",sampling_ratio*j_df.shape[0],"个数据","背景信息残缺率为：",nan_ratio,"重识别率为：{:.5f}%".format(sum/(sampling_ratio*j_df.shape[0])*100))
    # print("未重识别的数据统计推断攻击还原率为：",statistics_risk)
    risk=sum/(sampling_ratio*j_df.shape[0])
    # risk是重识别lv
    # statistics_risk是统计推断还原率
    from .config import Send_result
    Send_result(worker_id=worker_id,res_dict={
        "重识别率":risk,
        "统计推断还原率":statistics_risk,
        "背景知识水平假设":sampling_ratio*j_df.shape[0],
        "背景属性残缺率假设":nan_ratio,
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


BACKGROUND_KNOWLEDGE=[
  {
    "user_id": 554469169,
    "user_name": "彭杰伟",
    "id_number": "621100199303070018",
    "sex": "男性",
    "age": 31,
    "mobile_phone_number": "188012225783"
  },
  {
    "user_id": 369859394,
    "user_name": "赵文华",
    "id_number": "411600199107110017",
    "sex": "男性",
    "age": 33,
    "mobile_phone_number": "185452867627"
  },
  {
    "user_id": 861097543,
    "user_name": "乔诗珊",
    "id_number": "110111198902200025",
    "sex": "女性",
    "age": 35,
    "mobile_phone_number": "135522941560"
  },
  {
    "user_id": 856955609,
    "user_name": "温小雨",
    "id_number": "653000199109220029",
    "sex": "女性",
    "age": 33,
    "mobile_phone_number": "181376709779"
  },
  {
    "user_id": 982333831,
    "user_name": "杜咏亮",
    "id_number": "140500198511100017",
    "sex": "男性",
    "age": 39,
    "mobile_phone_number": "157038507706"
  },
  {
    "user_id": 409903778,
    "user_name": "罗建华",
    "id_number": "222400200007120015",
    "sex": "男性",
    "age": 24,
    "mobile_phone_number": "135915781130"
  }
]
GOD_VERSION_DATA = {
    'user_name': ['彭杰伟', '赵文华', '乔诗珊', '温小雨', '杜咏亮', '罗建华'],
    'user_id': [554469169, 369859394, 861097543, 856955609, 982333831, 409903778],
    'id_number': ['621100199303070018', '411600199107110017', '110111198902200025', '653000199109220029', '140500198511100017', '222400200007120015'],
    'sex': ['男性', '男性', '女性', '女性', '男性', '男性'],
    'age': [31, 33, 35, 33, 39, 24],
    'birthday': ['19930307', '19910711', '19890220', '19910922', '19851110', '20000712'],
    'mobile_phone_number': ['188012225783', '185452867627', '135522941560', '181376709779', '157038507706', '135915781130'],
    'province_of_domicile': ['甘肃省', '河南省', '北京市', '新疆维吾尔自治区', '山西省', '吉林省'],
    'city_of_domicile': ['定西市', '周口市', '房山区', '克孜勒苏柯尔克孜自治州', '晋城市', '延边朝鲜族自治州'],
    'registration_time': ['2016-04-16 00:52', '2018-02-05 03:42', '2016-08-15 14:15', '2017-05-18 17:22', '2016-01-01 10:44', '2016-01-08 20:56'],
    'package_id': [12, 17, 3, 18, 21, 16],
    'package_monthly_rent': [29, 29, 29, 29, 29, 29],
    'subscription_time': ['2020-05-06 20:31', '2020-07-18 01:13', '2022-10-24 10:47', '2021-02-15 01:08', '2020-11-28 10:13', '2020-06-13 02:28'],
    'package_term': ['20年有效', '2年可续', '2年可续', '20年有效', '2年可续', '2年有效'],
    'estimated_time_of_maturity': ['2040-05-06 20:31 20:31', '2022-07-18 01:13 01:13', '2024-10-24 10:47 10:47', '2041-02-15 01:08 01:08', '2022-11-28 10:13 10:13', '2022-06-13 02:28 02:28'],
    'account_balance': [90.00, 30.00, 97.00, 134.50, 167.50, 56.00]
}

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

def show_risk():
    qids=['sex','age','user_name','id_number','mobile_phone_number']
    idd=['user_id']
    src_dburl='mysql+pymysql://root:784512@localhost:3306/local_test'
    src_table='user_basic_info_9'

    query = f"SELECT * FROM {src_table}"
    engine=create_engine(src_dburl).connect()
    tuomindf=pd.read_sql(sql=text(query),con=engine)

    engine.close()
    df=pd.DataFrame(BACKGROUND_KNOWLEDGE)
    qq=qids+idd
    j_df = df[[col for col in qq if col in df.columns]]
    hdf = tuomindf[[col for col in qq if col in tuomindf.columns]]
    
    bg=j_df
    bg=bg.astype(object)

    
    bg=bg.astype(object)
    hdf=hdf.astype(object)
    # 计算距离矩阵
    distances = cdist(bg.loc[:,qids], hdf.loc[:,qids],lambda x, y: distance(x, y))
    # 获取每个 bg 数据与 df 中最近的 k 个数据的索引
    k=5
    top_k_indices = distances.argsort(axis=1)[:, :k]
    # 提取最近的 k 个数据
    candidates=[]

    for bg_index in range(len(bg)):
        top_indices = top_k_indices[bg_index]
        candidates_index=[]
        for ind in top_indices:
            candidates_index.append(ind)
        candidates.append(candidates_index)
    print(candidates)
    non_re_id_index=[]## 存bg_index
    #计算采样率
    sum=0
    for bg_index in range(len(bg)):
        cand=tuomindf.iloc[candidates[bg_index]]
        print(bg.iloc[bg_index])
        print(cand)
        a=cand.iloc[0]['user_id']
        b=bg.iloc[bg_index]['user_id']
        a=str(a)
        b=str(b)
        if a==b:
            sum=sum+1
            print('用户重识别攻击成功，获得的完整信息如下：')
            print(cand.iloc[0])
            print('#########################################################################')
        else:
            non_re_id_index.append(bg_index)
    
    print(sum)
    # 获取 DataFrame 的所有属性集合
    all_attributes = tuomindf.columns.tolist()

    # 计算集合差集
    sa = [attr for attr in all_attributes if attr not in qids + idd]
    statistics_risk=0
    # #计算统计推断效率
    if len(non_re_id_index)==0:
        statistics_risk=0
    else:
        possible=0
        for bg_index in non_re_id_index:
            true_info=pd.DataFrame(GOD_VERSION_DATA).iloc[bg_index]
            cand=tuomindf.iloc[candidates[bg_index]]  
            for a in sa:
                a_sum=0
                for _,crow in cand.iterrows():
                    if crow[a]==true_info[a]:
                        a_sum=a_sum+1
                if a_sum>0:
                    possible=a_sum/5
                    print('对于数据：(user_id)',true_info['user_id'],'攻击者有',possible,'的概率推断出其属性',a,'为：',true_info[a])
                    if possible>=0.4:
                        statistics_risk=statistics_risk+1        


    print("重识别风险评估任务执行完成","采样",j_df.shape[0],"个数据","重识别率为：{:.5f}%".format(sum/(j_df.shape[0])*100))
    print('统计推断攻击数据还原率为：{:.5f}%'.format((statistics_risk / (len(sa) * (j_df.shape[0]-2))) * 100))
    # print("未重识别的数据统计推断攻击还原率为：",statistics_risk)
    # risk=sum/(j_df.shape[0])
    # risk是重识别lv
    # statistics_risk是统计推断还原率

import re

def normalize_data(data):
    """
    去除数据中的非字母数字字符，用于比较。
    """
    return re.sub(r'\D', '', data)

def is_data_match(data1, data2):
    """
    判断两个脱敏数据是否匹配。
    这里假设如果它们在去除非数字字符后部分匹配，则认为它们相同。
    """
    normalized_data1 = normalize_data(data1)
    normalized_data2 = normalize_data(data2)
    
    # 检查部分匹配（可以调整匹配逻辑）
    return normalized_data1.startswith(normalized_data2) or normalized_data2.startswith(normalized_data1)

def show_muti_risk():
    dburl='mysql+pymysql://root:784512@localhost:3306/local_test'
    user_basic_info='user_basic_information_7'
    user_billing='user_billing_data_new_5'

    query='''
SELECT
		u.user_id AS table_basicinfo_user_id,
		b.user_id AS table_bill_user_id,
		u.mobile_phone_number AS table_basicinfo_phone,
		b.mobile_phone_number AS table_bill_phone,
		b.`account_balance (RMB)` AS account_balance,
        u.user_name,
		u.sex,
		u.age,
		u.birthday,
		u.id_number,
		u.province_of_domicile,
		u.city_of_domicile,
		u.estimated_time_of_maturity,
        --     u.`account_balance (RMB)` AS original_account_balance,
        u.`package_monthly_rent (RMB)`,
		u.package_term,
		u.subscription_time,
        b.bill_id,
        b.`package_monthly_rent (RMB)` AS bill_package_monthly_rent,
    b.`extra_fee (RMB)` AS bill_extra_fee,
    b.`total_cost (RMB)` AS bill_total_cost,
    b.deduct_time AS bill_deduct_time
FROM user_basic_information_7 u
JOIN user_billing_data_new b
ON u.user_id=b.user_id
AND LEFT(u.mobile_phone_number, 2) = LEFT(b.mobile_phone_number, 2)
AND u.`account_balance (RMB)` = b.`account_balance (RMB)`;
    '''
    engine=create_engine(dburl).connect()
    df=pd.read_sql(con=engine,sql=text(query))
    print(df)

    # 从第一个表（基本信息数据表）读取数据
    
    query1='''
SELECT
		u.user_id AS table_basicinfo_user_id,
		b.user_id AS table_bill_user_id,
		u.mobile_phone_number AS table_basicinfo_phone,
		b.mobile_phone_number AS table_bill_phone,
		b.`account_balance (RMB)` AS account_balance,
        u.user_name,
		u.sex,
		u.age,
		u.birthday,
		u.id_number,
		u.province_of_domicile,
		u.city_of_domicile,
		u.estimated_time_of_maturity,
        --     u.`account_balance (RMB)` AS original_account_balance,
        u.`package_monthly_rent (RMB)`,
		u.package_term,
		u.subscription_time,
        b.bill_id,
        b.`package_monthly_rent (RMB)` AS bill_package_monthly_rent,
    b.`extra_fee (RMB)` AS bill_extra_fee,
    b.`total_cost (RMB)` AS bill_total_cost,
    b.deduct_time AS bill_deduct_time
FROM user_basic_information_7 u
JOIN user_billing_data_new_7 b
ON LEFT(u.user_id, 3) = LEFT(b.user_id, 3)
AND LEFT(u.mobile_phone_number, 2) = LEFT(b.mobile_phone_number, 2)
AND u.`account_balance (RMB)` = b.`account_balance (RMB)`;
    '''

    engine=create_engine(dburl).connect()
    df_binfo=pd.read_sql(sql=text(query1),con=engine)
    
    print(df_binfo)

    # 及时关闭数据库连接
    engine.close()



if __name__=='__main__':
    # test_uniqueness_assess()
    # test_link_risk_assess()
    # test_reid_risk()
    # show_risk()
    show_muti_risk()