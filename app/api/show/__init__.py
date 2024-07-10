# -*- coding: utf-8 -*-

from datetime import datetime
from flask import Blueprint, render_template, jsonify
from flask import Flask, url_for

from neo4j import GraphDatabase
import json
import configparser

from pandas.io.formats import string
import redis

api_show = Blueprint('api_show', __name__)

##根据关系名得到属性节点的categories值
dict={
    "has_addr":1,
    "has_age":2,
    "has_bank":3,
    "has_bir":4,
    "has_email":5,
    "has_id":6,
    "has_mar":7,
    "has_name":8,
    "has_sex":9,
    "has_phone":10
}
kind=[
    {
        "name":"唯一标志符"
    },
        {
            "name": "个人住址"
        },
        {
            "name": "年龄"
        },
        {
            "name": "银行卡号"
        },
        {
            "name": "出生日期"
        },
        {
            "name": "邮箱"
        },
        {
            "name": "身份证号"
        },
        {
            "name": "婚配情况"
        },
        {
            "name": "姓名"
        },
    {
        "name":"性别"
    },
    {
        "name":"电话号码"
    }
]


@api_show.route('/metric',methods=['post','GET'])
def get_metric():
    query='MATCH p=()-[r:has]->() RETURN p'
    res=get_metric_result(query)
    print(res)
    return res


@api_show.route('/data', methods=['post', 'GET'])
def get_neo4j_result():
    query = 'match p=(n:UUID)<-->(b) return p limit 1000'
    res = show_neo4j(query)
    return res


##  api返回主机资源使用情况
import psutil
import time
# 用于存储上一次调用的网络流量数据
last_net_io = psutil.net_io_counters()
last_time = time.time()
@api_show.route('/table/sysinfo',methods=['post', 'GET'])
def get_system_status():
    global last_net_io, last_time
    current_time = time.time()
    # 获取 CPU 使用率
    cpu_usage = psutil.cpu_percent(interval=1)
    # 获取内存使用率
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent
    # 获取当前网络流量
    current_net_io = psutil.net_io_counters()
    # 计算时间间隔
    interval = current_time - last_time
    # 计算网络流量（MB/s）
    network_traffic = ((current_net_io.bytes_sent + current_net_io.bytes_recv) -
                       (last_net_io.bytes_sent + last_net_io.bytes_recv)) / interval / (1024 * 1024)
    # 更新 last_net_io 和 last_time
    last_net_io = current_net_io
    last_time = current_time

    components = [{
        "name": "系统组件",
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "network_traffic": round(network_traffic, 4),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]

    data = {
        "components": components
    }
    return jsonify(data)

#   api返回任务相关信息
@api_show.route('/workerinfo',methods=["POST","GET"])
def get_worker_info():
    # 假数据
    tasks = [
        {
            'id': 'task_be5de1d3-f564-4ad4-a978-7158b74e271f',
            'status': 'completed',
            'parameters': {
                'src_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'un_table_name': 'table1',
                'to_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'table_name': 'processed_table1',
                'QIDs': ['name', 'age', 'address'],
                'SA': ['disease'],
                'ID': ['ssn'],
                'k': 3,
                'l': 2,
                't': 0.9,
                'data_scene':'网购服务'
            },
            'results': [
                {'type': 'Risk Assessment', 'rank':69,'value': 'Low Risk'},
                {'type': 'Compliance Assessment', 'rank':75,'value': 'Compliant'},
                {'type': 'Usability Assessment', 'rank':77,'value': 'High Usability'},
                {'type': 'Anonymization Data Characteristics','rank':80, 'value': 'Good Quality'},
                {'type': 'Anonymization Data Quality', 'rank':88,'value': 'Excellent'},
                {'type': 'Privacy Protection Metrics', 'rank':40,'value': 'Strong Protection'}
            ],
            'description':'Performing data analysis on the given datasets to extract meaningful insights.'
        },
        {
            'id': 'task_e0f3cfaa-39d0-4bab-a6e7-7e32299abad3',
            'status': 'in_progress',
            'parameters': {
                'src_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'un_table_name': 'table2',
                'to_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'table_name': 'processed_table2',
                'QIDs': ['name', 'age', 'location'],
                'SA': ['income'],
                'ID': ['passport_number'],
                'k': 5,
                'l': 3,
                't': 0.8,
                'data_scene':'网购服务'
            },
            'results': [],
            'description':'Performing data analysis on the given datasets to extract meaningful insights.'
        },
        {
            'id': 'task_abb66df4-b0d8-4105-b640-ac59126cbee9',
            'status': 'completed',
            'parameters': {
                'src_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'un_table_name': 'table3',
                'to_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'table_name': 'processed_table3',
                'QIDs': ['job', 'zip_code'],
                'SA': ['salary'],
                'ID': ['national_id'],
                'k': 4,
                'l': 2,
                't': 0.95,
                'data_scene':'医疗卫生'
            },
            'results': [
                {'type': 'Risk Assessment', 'rank':79,'value': 'Medium Risk'},
                {'type': 'Compliance Assessment', 'rank':79,'value': 'Partially Compliant'},
                {'type': 'Usability Assessment', 'rank':79,'value': 'Medium Usability'},
                {'type': 'Anonymization Data Characteristics', 'rank':79,'value': 'Average Quality'},
                {'type': 'Anonymization Data Quality', 'rank':79,'value': 'Good'},
                {'type': 'Privacy Protection Metrics', 'rank':79,'value': 'Moderate Protection'}
            ],
            'description':'Performing data analysis on the given datasets to extract meaningful insights.'
        },
        {
            'id': 'task_a8dfcacf-b6d2-4f4a-bee6-3d6f06d0892a',
            'status': 'completed',
            'parameters': {
                'src_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'un_table_name': 'table4',
                'to_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'table_name': 'processed_table4',
                'QIDs': ['gender', 'birth_date'],
                'SA': ['health_status'],
                'ID': ['driver_license'],
                'k': 3,
                'l': 2,
                't': 0.85,
                'data_scene':'教育服务'
            },
            'results': [
                {'type': 'Risk Assessment', 'rank':79,'value': 'High Risk'},
                {'type': 'Compliance Assessment', 'rank':59,'value': 'Non-Compliant'},
                {'type': 'Usability Assessment', 'rank':46,'value': 'Low Usability'},
                {'type': 'Anonymization Data Characteristics', 'rank':89,'value': 'Poor Quality'},
                {'type': 'Anonymization Data Quality', 'rank':75,'value': 'Below Average'},
                {'type': 'Privacy Protection Metrics', 'rank':84,'value': 'Weak Protection'}
            ],
            'description':'Performing data analysis on the given datasets to extract meaningful insights.'
        },
        {
            'id': 'task_97d83d9b-6707-45a4-a2fc-8fe9459561d0',
            'status': 'in_progress',
            'parameters': {
                'src_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'un_table_name': 'table5',
                'to_url': 'mysql+pymysql://root:784512@localhost:3306/local_test',
                'table_name': 'processed_table5',
                'QIDs': ['ethnicity', 'marital_status'],
                'SA': ['political_views'],
                'ID': ['tax_id'],
                'k': 6,
                'l': 4,
                't': 0.7,
                'data_scene':'网购服务'
            },
            'results': [],
            'description':'Performing data analysis on the given datasets to extract meaningful insights.'
        }
    ]
    r = redis.StrictRedis(host='localhost', port=5678, db=5, password='qwb',decode_responses=True)
    # 从 Redis 获取以 "-0" 结尾的键值对
    # 从 Redis 获取以 "-0" 结尾的键值对
    # 从 Redis 获取以 "-0" 结尾的键值对
    keys = r.keys('*-0')
    for key in keys:
        value = r.get(key)
        if value:
            tasks.append(json.loads(value))
    return jsonify(tasks)


def show_neo4j(query):
    config = configparser.ConfigParser()
    # 只供测试
    config.read('./setting/set.config')
    url = config.get('neo4j', 'url')
    driver = GraphDatabase.driver(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    # 连接到neo4j数据库
    session = driver.session()
    # 执行查询
    result = session.run(query)
    links = []
    nodes = []
    id=1
    s_id={}
    # 将结果转换为JSON格式
    for record in result:
        re = record.data()['p']
        print(re)
        obj = str(re)
        #print(obj)
        a = obj.split(",")[0].split(":")[1][2:-2]
        b = obj.split(",")[1][2:-1]
        c = obj.split(",")[2][:-2].split(":")[1].strip('}')[2:-1]
        d = obj.split(",")[2][:-2].split(":")[0].strip('{')[3:-1]
        if a in s_id:
            sid=s_id[a]
        else:
            sid=id
            s_id[a]=sid
            id=id+1
            n1 = {
                "id": f"{sid}",
                "name": a,
                "type": "user",
                "size": 16,
                "color": 1,
                "category":0
            }
            nodes.append(n1)
        if c in s_id:
            tid = s_id[c]
        else:
            tid = id
            s_id[c] = tid
            id = id + 1
            type=kind[dict[b]]["name"]
            n2 = {
                "id": f"{tid}",
                "name": c,
                "type": type,
                "size": 8,
                "color": 2,
                "category":dict[b]
            }
            nodes.append(n2)
        data = {
            "source": f"{sid}",
            "target": f"{tid}",
            "relation": b,
            "type":kind[dict[b]]["name"]
        }
        links.append(data)
    a = {"links": links, "nodes": nodes,"categories":kind}
    print("关系:",len(links),"个,节点:",len(nodes))

    # 关闭会话和驱动程序
    session.close()
    driver.close()
    return a

def get_metric_result(query):
    config = configparser.ConfigParser()
    # 只供测试 todo
    config.read('./setting/set.config')
    url = config.get('neo4j', 'url')
    driver = GraphDatabase.driver(url, auth=(config.get('neo4j', 'user'), config.get('neo4j', 'password')))
    # 连接到neo4j数据库
    session = driver.session()
    # 执行查询
    print(query)
    result = session.run(query)
    children=[]
    links=[]
    id=0
    for record in result:
        print(record.value)
        path = record['p']
        # 访问路径中的节点和关系
        nodes = list(path.nodes)
        relationships_in_path = list(path.relationships)
        
        # 确保路径中至少有一个关系
        if relationships_in_path:
            start_node = nodes[0]
            relationship = relationships_in_path[0]
            end_node = nodes[1]
            
            l = {
                "father": start_node._properties['name'],
                "father_desc": start_node._properties.get('desc', ''),
                "children": end_node._properties['name'],
                "children_desc": end_node._properties.get('desc', '')
            }
        # link=record.data()['p']
        # print(link)
        # f=link[0]['name']
        # f_d=link[0]['desc']
        # c=link[2]['name']
        # c_d=link[2]['desc']
        # l={
        #     "father":f,
        #     "father_desc":f_d,
        #     "children":c,
        #     "children_desc":c_d
        # }
        links.append(l)
    # 创建一个空字典，用于存储子节点
    child_dict = {}
    # 遍历关系列表
    for relationship in links:
        parent = relationship['father']
        child = relationship['children']
        # 如果父节点已经在字典中，将子节点添加到该父节点的列表中
        if parent not in child_dict:
            c = {
                'name': parent,
                'children': [
                    {'name': child, 'desc': relationship['children_desc']}
                ],
                'desc': relationship['father_desc']
            }
            child_dict[parent] = c
        # 如果父节点不在字典中，创建一个新的列表并将子节点添加到其中
        else:
            c = {
                'name': child,
                'desc': relationship['children_desc']
            }
            child_dict[parent]['children'].append(c)
    dict_list=list(child_dict.values())
    root={}
    for i in dict_list:
        if i['name']=='隐私效果评估指标体系':
            print(i)
            root=i
    a=[]
    for i in root['children']:
        a.append(handle(i,child_dict))
    session.close()
    driver.close()
    data={
        'name': '隐私效果评估指标体系',
        'children': a,
        'desc':''
    }
    return data
def handle(dict,child_dict):
    if dict['name'] not in child_dict.keys():
        #print(dict['name'])
        ##说明是根节点
        return dict
    l=[]
    for i in child_dict[dict['name']]['children']:
        l.append(handle(i,child_dict))
    a = {
        'name': dict['name'],
        'desc': dict['desc'],
        'children': l
    }
    return a