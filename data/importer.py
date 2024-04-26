import pandas as pd
import mysql.connector

def import_json_to_mysql(json_file, table_name, host, user, password,database):
    # 读取 JSON 文件
    df = pd.read_json(json_file, orient='records')
    columns = df.columns.tolist()

    # 连接到 MySQL 数据库
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    # 创建表
    create_table_sql = "CREATE TABLE IF NOT EXISTS {} ({})".format(
        table_name,
        ', '.join(['{} VARCHAR(255)'.format(col) for col in columns])  # 假设所有列类型都为 VARCHAR(255)
    )
    cursor.execute(create_table_sql)

    # 插入数据
    insert_sql = "INSERT INTO {} ({}) VALUES ({})".format(
        table_name,
        ', '.join(df.columns),
        ', '.join(['%s'] * len(df.columns))
    )
    data_tuples = [tuple(row) for row in df.values]
    cursor.executemany(insert_sql, data_tuples)

    # 提交事务并关闭连接
    conn.commit()
    conn.close()

# 使用示例
if __name__ == "__main__":
    json_file = "./json/3/hotel_1_5_2.json"  # JSON 文件路径
    table_name = "local_test_hotel_1_5_2"  # 数据表名称
    host = "localhost"  # MySQL 主机名
    user = "root"  # MySQL 用户名
    password = "784512"  # MySQL 密码
    database_name="local_test"

    import_json_to_mysql(json_file, table_name, host, user, password,database_name)
