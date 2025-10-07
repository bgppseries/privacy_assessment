
from neo4j import GraphDatabase
import configparser
from py2neo import Graph
from py2neo.bulk import merge_nodes
import pandas as pd
import os
import time
import subprocess
import platform

##
##导入csv↓

def csv_importer(file,docker_id,ID,QIDs,SA,logger):
    system_platform = platform.system()
    # 获取文件名
    filename=os.path.basename(file)
    if system_platform=="Windows":
        ##todo
        ##windows环境还没想好
        pass
    elif system_platform=="Linux":
        # 构建docker cp命令
        docker_cp_command = f"docker cp {file} {docker_id}:/var/lib/neo4j/import/"
        # 执行docker cp命令
        result = run_command(docker_cp_command,logger)
        logger.info(result)
        # 返回命令执行的结果
        # 执行导入函数
        apoc_load_csv_file(filename,ID,QIDs,SA,logger)
    else:
        logger.error('csv_importer 识别OS出错 目前只支持Linux和windows')

def apoc_load_csv_file(file,ID,QIDs,SA,logger):
    #多场景下的json导入，导入方式为 apoc插件 SQL语句
    ## merge比较慢
    query=generate_apoc_json_import_statements(file,QIDs,ID,SA)
    config = configparser.ConfigParser()
    # 只供测试
    config.read('../setting/set.config')
    url = config.get('neo4j', 'url')

    driver = GraphDatabase.driver(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    
    with driver.session() as session:
        result = session.run(query)
    return result

def generate_apoc_csv_import_statements(csv_filepath,QIDs,ID,SA):
    ## apoc csv 导入语句生成脚本
    import_statements=[]
    i=1
    v=generate_string(i)
    i+=1
    usr=generate_string(i)
    i+=1
    query_f=f"""
    CALL apoc.load.csv('file:///{csv_filepath}') YIELD value AS {v}
    """
    import_statements.append(query_f)
    
    for col in ID:
        ##创建用户节点
        ## 直接标志符必须唯一
        COL=col.upper()
        user_node_s=f"""
        CREATE ({usr}:{COL} {{value: {v}.{col}}})
        """
        import_statements.append(user_node_s)
    for col in QIDs:
        j=generate_string(i)
        COL=col.upper()
        ## 创建间接标志符节点
        qid_node_s=f"""
        MERGE ({j}:QID_{COL} {{value: {v}.{col}}})
        """
        import_statements.append(qid_node_s)
        ## 创建用户与直接标志符之间的边
        qid_user_edge_s=f"""
        MERGE ({usr})-[:HAX_QID_{COL}]->({j})
        """
        import_statements.append(qid_user_edge_s)
        i+=1
    for col in SA:
        j=generate_string(i)
        COL=col.upper()
        sa_node_s=f"""
        MERGE({j}:SA_{COL} {{value: {v}.{col}}})
        """
        import_statements.append(sa_node_s)
        sa_user_edge_s=f"""
        MERGE ({usr})-[:HAX_SA_{COL}]->({j})
        """
        import_statements.append(sa_user_edge_s)
        i+=1
    import_statements=''.join(import_statements).strip()
    return import_statements

###
### json导入↓


def json_importer(file,docker_id,ID,QIDs,SA,logger):
    system_platform = platform.system()
    # 获取文件名
    filename=os.path.basename(file)
    if system_platform=="Windows":
        ##todo
        ##windows环境还没想好
        pass
    elif system_platform=="Linux":
        # 构建docker cp命令
        docker_cp_command = f"docker cp {file} {docker_id}:/var/lib/neo4j/import/"
        # 执行docker cp命令
        result = run_command(docker_cp_command,logger)
        logger.info(result)
        # 返回命令执行的结果
        # 执行导入函数
        apoc_load_json_file(filename,ID,QIDs,SA,logger)
        
    else:
        logger.error('json_importer 识别OS出错 目前只支持Linux和windows')


def run_command(command,logger):
    """
    执行命令并返回输出结果
    """
    system_platform = platform.system()
    try:
        if system_platform == "Windows":
            # 在Windows系统上执行命令
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
        elif system_platform == "Linux":
            # 在Linux系统上执行命令
            result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        else:
            raise OSError(f"Unsupported operating system: {system_platform}")
        # 返回命令执行的结果
        logger.info(f"{command}命令执行成功")
        ##print(result.stdout.strip())
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        # 如果命令执行失败，返回错误信息
        logger.error(f"{command}命令执行失败, {e.stderr.strip()}")
        return f"Error: {e.stderr.strip()}"

def apoc_load_json_file(file_path,ID,QIDs,SA,logger):
    #多场景下的json导入，导入方式为 apoc插件 SQL语句
    ## merge比较慢
    query=generate_apoc_json_import_statements(file_path,QIDs,ID,SA)
    config = configparser.ConfigParser()
    # 只供测试
    config.read('../setting/set.config')
    url = config.get('neo4j', 'url')

    driver = GraphDatabase.driver(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    
    with driver.session() as session:
        result = session.run(query)
    print('hotel场景下的json读取 读取方式为 apoc插件 SQL')
    return result


def generate_apoc_json_import_statements(json_filepath,QIDs,ID,SA):
    ## apoc json 导入语句生成脚本
    import_statements=[]
    i=1
    v=generate_string(i)
    i+=1
    usr=generate_string(i)
    i+=1
    query_f=f"""
    CALL apoc.load.json('file:///{json_filepath}') YIELD value AS {v}
    """
    import_statements.append(query_f)
    
    for col in ID:
        ##创建用户节点
        ## 直接标志符必须唯一
        COL=col.upper()
        user_node_s=f"""
        CREATE ({usr}:{COL} {{value: {v}.{col}}})
        """
        import_statements.append(user_node_s)
    for col in QIDs:
        j=generate_string(i)
        COL=col.upper()
        ## 创建间接标志符节点
        qid_node_s=f"""
        MERGE ({j}:QID_{COL} {{value: {v}.{col}}})
        """
        import_statements.append(qid_node_s)
        ## 创建用户与直接标志符之间的边
        qid_user_edge_s=f"""
        MERGE ({usr})-[:HAX_QID_{COL}]->({j})
        """
        import_statements.append(qid_user_edge_s)
        i+=1
    for col in SA:
        j=generate_string(i)
        COL=col.upper()
        sa_node_s=f"""
        MERGE({j}:SA_{COL} {{value: {v}.{col}}})
        """
        import_statements.append(sa_node_s)
        sa_user_edge_s=f"""
        MERGE ({usr})-[:HAX_SA_{COL}]->({j})
        """
        import_statements.append(sa_user_edge_s)
        i+=1
    import_statements=''.join(import_statements).strip()
    return import_statements

def generate_string(n):
    # 函数示例
    # print(generate_string(1))  # 输出: a
    # print(generate_string(27)) # 输出: aa
    # 字母表字符串
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    result = ''
    
    # 当n小于等于26时，直接返回对应的字母
    if n <= 26:
        return alphabet[n-1]
    else:
        # 当n大于26时，计算需要追加的次数和最后一个字母
        while n > 0:
            n -= 1  # 调整n的值以适应0-25的索引
            result = alphabet[n % 26] + result
            n //= 26
    
    return result






### hotel场景下的json读取 读取方式为 py2neo
def load_json_file1p(file):
    #hotel
    config = configparser.ConfigParser()
    # 只供测试
    config.read('./setting/set.config')
    url = config.get('neo4j', 'url')
    
    graph = Graph(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    
    with open(file, 'r', encoding='UTF-8') as f:
        data = pd.read_json(f)

    for i in data.columns:
        print(i)
    # node_u = [
    #     {"uuid": uuid} for uuid in data['uuid'].tolist()
    # ]
    node = [[uuid] for uuid in data['uuid'].tolist()]
    print(node)
    lable_u = "uuid"
    keys = ('uuid', 'uuid')
    key = ['uuid']
    merge_nodes(graph.auto(), node, labels={lable_u}, merge_key=keys, keys=key)
    # create_nodes(graph.auto(),node_u,labels={lable_u})
    print("hotel场景下的json读取 读取方式为 py2neo ")

def load_json_file2(file):
    #info
    # todo 感觉还是得配置文件读取 先todo吧
    query_f = f"""
        CALL apoc.load.json('{file}') YIELD value AS v
        """
    query_s = """
        create (a:UUID {value: v.uuid})
        MERGE (n:Person {name: v.name})
        MERGE (s:Sex {value: v.sex})
        MERGE (b:Birthday {value: v.birthday})
        MERGE (g:Age {value: v.age})
        MERGE (m:Marital_status {value: v.marital_status})
        MERGE (e:Email {value: v.email})
        MERGE (u:Bank_num {value: v.bank_number})
        MERGE (r:Address {value: v.address})
        MERGE (i:Id_num {value: v.id_number})
        MERGE (p:Phone {value: v.phone})

        MERGE (a)-[:has_sex]->(s)
        MERGE (a)-[:has_name]->(n)
        MERGE (a)-[:has_bir]->(b)
        MERGE (a)-[:has_age]->(g)
        MERGE (a)-[:has_mar]->(m)
        MERGE (a)-[:has_email]->(e)
        MERGE (a)-[:has_bank]->(u)
        MERGE (a)-[:has_addr]->(r)
        MERGE (a)-[:has_id]->(i)
        MERGE (a)-[:has_phone]->(p)
        """
    query = query_f + query_s
    config = configparser.ConfigParser()
    # 只供测试
    config.read('../setting/set.config')
    url = config.get('neo4j', 'url')
    driver = GraphDatabase.driver(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    with driver.session() as session:
        result = session.run(query)
    print(result)


if __name__=='__main__':
    
    file='file:///info_low.json'
    time_s=time.time()
    # load_json_file2(file)
    QIDs=['sex','phone','email','id_number']
    SA=['address']
    ID=['uuid']
    s=generate_apoc_json_import_statements('info.json',QIDs,ID,SA)
    print(s)
    apoc_load_json_file('info.json')
    time_e=time.time()
    total_time = time_s-time_e
    print('程序运行时间为：', total_time, '秒')




