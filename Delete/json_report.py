from sqlalchemy import create_engine,text
import pandas as pd
import json

json_name = "hotel_0_0_0"
# 读取JSON文件
with open(f"./Delete/data3/{json_name}.json", 'r', encoding='utf-8') as file:
    data = json.load(file)

df = pd.DataFrame(data)
print(df)

url='mysql+pymysql://root:784512@localhost:3306/Ind_fusion'
table_name = f"local_test_{json_name}"
# query = f"SELECT * FROM {table_name}"
# 创建SQLAlchemy的Engine对象
engine = create_engine(url).connect()

df.to_sql(name=table_name, con=engine, if_exists='replace', index=True)

print("数据已成功导入到MySQL数据库")

