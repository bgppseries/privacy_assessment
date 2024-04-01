import pandas as pd
import numpy as np
""""
    隐私风险度量
        目前假定数据源格式为json
        重标识攻击：
            背景知识假设：
                先横着截取，之后再挖空
            节点相似性计算：
                1.距离dis
                2.向量？
"""

sampling_ratio=0.0005
nan_ratio = 0.2  # 设置为 80%
# 设置随机种子，确保每次运行结果一致
np.random.seed(0)

def risk_assess(url):
    df=pd.read_json(url)
    print(df.columns)
    ##去除uuid
    # df=df[1:]
    # print(df.columns)
    
    bg=df.sample(frac=sampling_ratio)
    print(bg.columns)
    bg=bg.astype(object)
    # # 对数字列进行掩码，将部分值置为 NaN
    # numeric_cols = bg.select_dtypes(include='number').columns
    # num_nan = int(len(bg) * nan_ratio)
    # for col in numeric_cols:
    #     random_rows = np.random.choice(bg.index, size=num_nan, replace=False)
    #     bg.loc[random_rows, col] = np.nan

    # # 对字符串列进行掩码，将部分值置为 NaN
    # string_cols = bg.select_dtypes(include='object').columns
    str_nan = int(len(bg) * nan_ratio)
    for col in bg.columns:
        random_rows = np.random.choice(bg.index, size=str_nan, replace=False)
        bg.loc[random_rows, col] = np.nan
    bg=bg.astype(object)
    print(bg.dtypes)
    print(bg)

if __name__=='__main__':
    risk_assess("./data/to_assess/hotel.json")