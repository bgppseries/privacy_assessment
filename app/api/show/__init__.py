# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, jsonify
from flask import Flask, url_for

from neo4j import GraphDatabase
import json
import configparser

from pandas.io.formats import string

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
    result = session.run(query)
    df=result.keys()
    #print(df)
    children=[]
    links=[]
    id=0
    for record in result:
        link=record.data()['p']
        #print(link)
        f=link[0]['name']
        f_d=link[0]['desc']
        c=link[2]['name']
        c_d=link[2]['desc']
        l={
            "father":f,
            "father_desc":f_d,
            "children":c,
            "children_desc":c_d
        }
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
# if __name__=='__main__':
#     query = 'match p=(n:UUID)<-->(b) return p limit 1000'
#     res = show_neo4j(query)
#     print(res)