# -*- coding: utf-8 -*-
import re
from collections import OrderedDict
from datetime import datetime
from flask import Blueprint, render_template, jsonify, request, Response
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


# api 返回任务数据 以及 样本数据展示
@api_show.route('/datainfo',methods=["POST","GET"])
def get_data_show():
    # 先获取最近的几个任务id
    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    try:
        # 建立到 Redis 的连接
        redis_client = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                                         db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        # 检查连接是否成功
        redis_client.ping()
        print("Connected to Redis server.")
    except redis.ConnectionError as e:
        # 处理连接错误
        print("Failed to connect to Redis server:", e)
    except Exception as e:
        # 处理其他异常
        print("An error occurred:", e)
    #  连接建立后，根据workerid判断是否已完成全部任务
    #  放在调度接口



#   api返回任务相关信息
@api_show.route('/workerinfo',methods=["POST","GET"])
def get_worker_info():

    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    r=redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                             db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 从 Redis 获取以 "-0" 结尾的键值对
    # 获取最近创建的10个任务的worker_id
    zset_key = "tasks"  # 假设这是 zset 的 key
    latest_task_ids = r.zrevrange(zset_key, 0, 9)  # 获取分数最大的前10个

    if not latest_task_ids:
        return jsonify({"error": "No tasks found in Redis"}), 404

    # 构造响应数据
    tasks_data = []

    for worker_id in latest_task_ids:
        w=worker_id.decode('utf-8')+'-0'
        task_data = r.get(w)  # 根据worker_id获取数据
        if task_data:
            task_data = json.loads(task_data)

            # 提取 `rank` 字典
            rank_dict = task_data.get("rank", {})

            # 定义分数评价规则
            def get_value(score):
                if score <= 20:
                    return "Very Low"
                elif score <= 40:
                    return "Low"
                elif score <= 60:
                    return "Medium"
                elif score <= 80:
                    return "Good"
                else:
                    return "Excellent"

            # 构造 `results`
            results = []
            for key, value in rank_dict.items():
                score = round(value * 100)  # 转换为百分制
                results.append({
                    "type": key,
                    "rank": score,
                    "value": get_value(score)
                })

            # 更新任务数据
            task_data["results"] = results
            task_data["evaluate"] = "Task successfully evaluated"
            task_data["cost_time"] = "calculated_duration"  # 可计算具体时长
            tasks_data.append(task_data)

    # 返回所有任务数据
    return jsonify(tasks_data)








# API 返回任务报告
@api_show.route('/report_info',methods=["POST","GET"])
def get_report():
    """
        根据主线程任务ID获取各个子线程的任务结果。
        """
    data = request.json
    worker_id = data['Worker_Id']
    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    try:
        # 建立到 Redis 的连接
        redis_client = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                                         db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        # 检查连接是否成功
        redis_client.ping()
        print("Connected to Redis server.")
    except redis.ConnectionError as e:
        # 处理连接错误
        print("Failed to connect to Redis server:", e)
    except Exception as e:
        # 处理其他异常
        print("An error occurred:", e)
    #  连接建立后，根据workerid判断是否已完成全部任务
    #  放在调度接口

    #  连接建立后，根据workerid取数据
    try:
        # 使用主线程ID作为前缀从Redis中获取任务结果
        keys = redis_client.keys(f'{worker_id}-*')
        # 存储结果的字典
        results = {}
        # 遍历结果并获取任务结果
        for key in keys:
            # 提取子线程ID
            child_process_id = key.decode('utf-8')
            # 排除后缀为 '-0' 的键
            if child_process_id.endswith('-0'):
                continue
            # 获取并解码结果JSON
            result_json = redis_client.get(child_process_id).decode('utf-8')
            result_dict = json.loads(result_json)
            # 使用子线程ID作为键存储结果
            results[child_process_id] = result_dict
        ## 支持可扩展性，将评估逻辑持久化
        report = fillter(replace_keys(results), worker_id)
        return report, 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def connect_redis():
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    return redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                                         db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)

def replace_keys(raw_data):
    replaced_data = {}
    for key, value in raw_data.items():
        # 替换 key 的后缀
        if key.endswith("-0"):
            worker_id=key-'-0'
            replaced_key = "评估任务的相关参数"
        elif key.endswith("-1"):
            replaced_key = "风险评估结果"
        elif key.endswith("-2"):
            replaced_key = "合规性评估结果"
        elif key.endswith("-3"):
            replaced_key = "可用性评估结果"
        elif key.endswith("-4"):
            replaced_key = "匿名集数据特征评估结果"
        elif key.endswith("-5"):
            replaced_key = "匿名集数据质量评估结果"
        elif key.endswith("-6"):
            replaced_key = "隐私保护性度量评估结果"
        else:
            replaced_key = key  # 保留其他未匹配的 key

        replaced_data[replaced_key] = value
    return replaced_data

#   fillter 辅助函数
##
# 从 Redis 获取任务配置
def get_task_config(redis_client, worker_id):
    config = redis_client.get(worker_id)
    if config:
        return json.loads(config)  # 将JSON 字符串转换为 Python 字典
    else:
        raise ValueError(f"未找到键为 {worker_id} 的任务配置")


def generate_privacy_report(worker_id,report):
    # 本函数内 worker_id会加'-0'
    # 初始化 Redis 连接
    worker_id=worker_id+'-0'
    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    redis_client = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                                     db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 获取 worker_id 对应的内容
    try:
        redis_data = redis_client.get(worker_id)
        if not redis_data:
            raise ValueError(f"No data found for worker_id: {worker_id}")
        # 将 Redis 的数据转换为字典
        data = json.loads(redis_data)
        data=data["evaluate"]
        # 将 Redis 数据写入 report 的对应位置

        print("HHHHHH")
        report["隐私评估报告"]["数据特征"]["评估结果"]=data["数据特征结果"]["评估结果"]
        report["隐私评估报告"]["隐私风险度量"]["评估结果"] = data["隐私风险度量"]["评估结果"]
        report["隐私评估报告"]["可用性评估"]["评估结果"] = data["可用性结果"]["评估结果"]
        report["隐私评估报告"]["数据质量评估"]["评估结果"] = data["数据质量结果"]["评估结果"]
        report["隐私评估报告"]["隐私保护性度量"]["评估结果"] = data["安全性结果"]["评估结果"]
        report["隐私评估报告"]["数据合规性"]["评估结果"] = data["合规性结果"]["评估结果"]

        report["隐私评估报告"]["报告摘要"]["评估得分"]=data["评估得分"]["rank"]

        return report
    except Exception as e:
        print(f"Error fetching or parsing data from Redis: {e}")
        return None

def fillter(data,worker_id):
    report = {
        "隐私评估报告": {
            "评估任务ID":f"{worker_id}",
            "报告摘要": {
                "评估得分": {},
                "目的": "隐私保护效果评估指标体系是评估隐私保护措施的有效性的重要工具。一方面，建立隐私保护效果评估指标体系，可以支撑隐私保护策略的多维度量化与自动化筛选。另一方面，针对海量的大数据隐私信息以及时空场景关联的特点，数据脱敏不仅要确保敏感信息被去除，还需充分考虑脱敏花费、实际业务需求等因素。数据保护与数据挖掘是一对矛盾体，既要使数据潜在价值得到充分应用，又要确保敏感信息不被泄露。因此，在进行数据脱敏时，应依据场景需要明确脱敏数据范围、脱敏需求以及脱敏后的数据用途。",
                "评价标准": {
                    "隐私风险评估":"隐私风险评估是指",
                    "隐私保护性度量": "隐私保护性度量是指隐私保护算法执行后，隐私信息的被还原能力。具体是指：攻击者/第三方从所观测到的隐私信息分量推断出隐私信息的能力。若能准确推断出，则安全性较差，反之，则安全性较好。",
                    "隐私合规性": "合规性是指隐私保护算法执行后得到的数据是否符合行业标准、内部规章制度以及其他相关要求的能力和状态。具体是指：隐私保护算法执行后，数据是否符合相关要求以及隐私保护算法所保证的安全能力，以此来保证数据最基本的安全性。如满足相关要求，则具备合规性，否则不具备合规性。",
                    "数据可用性": "可用性是指原始数据在经过隐私保护算法作用后得到的新数据对系统功能或分析研究的影响。具体是指：数据经过隐私保护算法作用后，原始数据与隐私保护后数据的偏差。偏差越大，可用性越低，偏差越小，可用性越高。",
                    "数据特征": "数据特征是指数据本身所具有的属性和状态。具体是指：数据本身具备的一些基本性质和属性，为数据的分析和系统应用提供基础。",
                    "数据质量评估": "数据质量评估是指综合评估隐私保护算法对数据的作用和影响。具体是指：从数据综合应用的角度考虑，对信息和数据进行全面的考察和评价，从而，提高信息和数据的可信度和有效度，为决策提供更有利的基础。"
                }
            },
            "隐私风险度量":{
                "评估结果":{},
            },
            "可用性评估": {
                "评估结果": {},
            },
            "隐私保护性度量": {
                "评估结果": {},
            },
            "数据质量评估": {
                "评估结果": {},
            },
            "数据合规性": {
                "评估结果": {},
            },
            "数据特征": {
                "评估结果": {},
            }
        }
    }

    if "风险评估结果" in data:
        item = data["风险评估结果"]
        # "重识别率": risk,
        # "统计推断还原率": statistics_risk,
        # "背景知识水平假设": sampling_ratio * j_df.shape[0],
        # "背景属性残缺率假设": nan_ratio,
        report["隐私评估报告"]["隐私风险度量"] ={
            "摘要": (
                "本指标旨在度量隐私风险，特别是在遭遇攻击时，分析数据表中匿名信息的重识别与统计推断攻击风险。"
                "攻击者利用一些已知用户信息（如姓名、身份证号、性别、年龄、手机号等）进行攻击，目的是重识别匿名化的用户数据或进行统计推断，以推测出用户的私人属性。"
            ),
            "攻击模型": {
                "总体描述": "基于三种攻击方式：重识别攻击、统计推断攻击以及差分攻击。",
                "详细说明": {
                    "重识别攻击": (
                        "攻击者根据背景知识中已知的用户信息，选取匿名数据表中与背景知识最接近的五个候选项，并假设候选的第一项数据为攻击结果。"
                        "攻击成功的标准为通过直接标识符（如用户ID）确认重识别结果是否与背景知识一致。如果一致，则认为重识别攻击成功，否则攻击失败。"
                    ),
                    "统计推断攻击": (
                        "若重识别攻击失败，攻击者将进一步对候选数据进行统计分析，基于统计结果推测用户的属性。"
                        "通过分析多样本的属性关联，实现差分攻击，并通过知识图谱数据层与数据集进行连接，统计差分攻击的重识别率。"
                    ),
                    "差分攻击": (
                        "将多表数据进行链接攻击，以此来推断在多场景动态发布数据情况下，数据的隐私泄露风险。"
                    ),
                },
            },
            "背景知识水平假设": (
                f"为了模拟攻击者的背景知识水平，我们假设其对用户信息的了解程度为 {item['背景知识水平假设']}。"
                "这反映了攻击者已知的样本数据数量，更高的背景知识水平会提高攻击成功的概率。"
            ),
            "评估方法": {
                "重识别率": (
                    "通过模拟攻击者对匿名化数据的重识别尝试，计算重识别成功的比例。"
                    "重识别率的高低反映了匿名化数据表在直接标识符保护上的安全性。"
                ),
                "统计推断还原率": (
                    "分析统计推断攻击下，攻击者通过属性关联推测敏感信息的能力，"
                    "还原率的高低表示统计推断攻击的有效性。"
                ),
            },
            "评估结果": {
                "重识别率": item["重识别率"],
                "统计推断还原率": item["统计推断还原率"],
            },
            "建议与改进措施": (
                "1. 提高匿名化数据的保护水平，减少重识别成功的可能性；\n"
                "2. 针对属性关联较强的数据集，进行更严格的去标识化处理；\n"
                "3. 通过差分隐私方法增加噪声，降低统计推断攻击的有效性；\n"
                "4. 实施分级管理，对数据集的发布进行严格审批，以降低多表链接攻击的可能性。"
            )
        }

    # 可用性评估填充
    if "可用性评估结果" in data:
        item = data["可用性评估结果"]
        report["隐私评估报告"]["可用性评估"] = {
            "数据可辨别度": {
                "指标摘要": f"本报告旨在评估数据集在隐私保护技术应用后的可辨别度。通过计算数据可辨别度指标，衡量隐私保护措施对数据区分能力的影响。本次评估的结果显示数据可辨别度为 {item['数据可辨别度']}。",
                "数据可辨别度定义": "数据可辨别度是衡量在应用隐私保护技术后，数据记录之间仍能保持区分能力的程度。较高的可辨别度值表示数据记录之间仍能较好地区分，而较低的可辨别度值则表示数据记录之间的差异性减小。对于某条记录t，如果发布的数据表中存在另外s条记录和t的非敏感属性相同，则t的信息损失记为s。对于在数据表中被隐匿的记录l，则l的信息损失为原始数据表T中的所有记录数目。",
                "结果分析": {
                    "较高的可辨别度": f"{item['数据可辨别度']} 这一数值相对较高，表明在隐私保护技术应用后，数据记录之间仍具有较高的区分能力。",
                    "信息损失控制": "信息损失总量较小，表明隐私保护措施有效控制了信息丢失，保留了数据的辨别能力。",
                    "等价组影响": "等价组的大小和数量对可辨别度有显著影响，较小的等价组和较少的隐匿记录有助于提高数据的可辨别度。",
                    "结论": f"通过评估可辨别度指标，我们可以得出结论，当前的隐私保护措施在确保隐私的同时，较好地保留了数据记录之间的区分能力。{item['数据可辨别度']} 的可辨别度值显示出数据集在保护隐私的同时，仍然具有较高的实用性。",
                    "改进建议": [
                        "优化等价组划分：通过调整等价组的划分策略，减少信息损失。",
                        "改进隐私保护算法：采用更为精细的隐私保护算法，在保证隐私的前提下，最大程度保留数据的可辨别度。",
                        "持续监控和评估：定期进行可辨别度评估，及时发现和改进隐私保护措施中的不足。"
                    ]
                }
            },
            "数据记录匿名率": {
                "指标摘要": f"本报告旨在评估数据集在隐私保护技术应用后的数据记录匿名率。数据记录匿名率衡量数据集中被隐藏或删除的记录比例。评估结果显示，数据记录匿名率为 {item['数据记录匿名率']}。",
                "数据记录匿名率定义": "在隐私保护执行过程中，有些元组由于不符合隐私保护条件一般进行删除舍弃，这样就产生了信息缺失，删除的记录一般被称为隐匿记录。数据记录匿名率是指在隐私保护过程中，被隐藏或删除的记录数量占总记录数量的比例。",
                "结果分析": {
                    "隐匿记录数量": "被隐匿的记录数 𝑛𝑠为 0，表明没有记录被隐藏或删除。",
                    "数据完整性": "数据记录匿名率为 0.0% 表示所有记录均保留，数据的完整性得到了充分保障。",
                    "信息损失": "由于没有记录被隐匿，因此不存在因隐匿导致的信息损失。",
                    "结论": "数据记录匿名率为 0.0% 表明隐私保护措施在不删除任何记录的情况下，实现了隐私保护目标。这意味着数据的完整性和可用性得到了最大程度的保留，隐私保护技术的应用对数据的实际影响最小。",
                    "改进建议": [
                        "提高隐私保护精度：在保证数据完整性的前提下，采用更为先进的隐私保护算法，进一步降低隐私泄露风险。",
                        "持续监控和评估：定期对隐私保护效果进行评估，确保隐私保护措施持续有效，并及时调整策略应对新的隐私风险。",
                        "用户隐私需求调研：了解用户对隐私保护的具体需求和期望，制定更具针对性的隐私保护措施。"
                    ]
                }
            },
            "平均泛化程度": {
                "指标摘要": f"本报告旨在评估数据集在隐私保护技术应用后的平均泛化程度。平均泛化程度用于衡量数据在隐私保护过程中被泛化后的平均等价组大小。本次评估的结果显示，平均泛化程度为 {item['归一化平均等价组大小度量']}。",
                "平均泛化程度定义": "平均泛化程度指的是数据集的元组总数除以所有等价组的个数，表示每个等价组中的平均记录数量。通过统计数据集中的总记录数以及等价组的数量，计算每个等价组的平均记录数，从而得出平均泛化程度。较高的平均泛化程度值可能意味着更高的隐私保护水平，但也可能导致数据的精确性下降。",
                "结果分析": {
                    "等价组数量": "等价组的个数 ∣𝐸∣ 相对较少，导致每个等价组包含更多的记录。",
                    "隐私保护效果": "较高的平均泛化程度表明，数据记录在隐私保护过程中被聚合成较大的等价组，这有助于提高隐私保护水平，减少重识别风险。",
                    "数据实用性": "虽然较高的平均泛化程度有助于保护隐私，但也可能导致数据的精确性和可用性下降，影响分析结果的细粒度。",
                    "结论": "平均泛化程度为 189.3939 表示数据集在隐私保护过程中，每个等价组平均包含 189.3939 条记录。这个值反映了隐私保护措施对数据记录的聚合效果，较高的等价组大小有助于提高隐私保护水平，但也需要平衡数据的实用性和精确性。",
                    "改进建议": [
                        "优化等价组划分策略：通过更细粒度的划分策略，减少等价组的大小，提高数据的精确性。",
                        "采用混合隐私保护技术：结合泛化、扰动、数据屏蔽等多种技术，提升隐私保护效果的同时，保留数据的实用性。",
                        "动态调整隐私保护参数：根据具体应用场景和数据特点，动态调整隐私保护参数，找到隐私保护和数据可用性之间的最佳平衡点。"
                    ]
                }
            },
            "基于熵的平均数据损失度": {
                "指标摘要": f"本报告旨在评估数据集在隐私保护技术应用后的基于熵的数据损失度。通过计算隐私量的变化，衡量隐私保护技术对个人信息可识别性增加所带来的隐私损失。本次评估结果显示，平均隐私降低程度为 {item['基于熵的平均数据损失度']}。",
                "基于熵的平均数据损失度定义": "基于熵的平均数据损失度通过计算初始隐私量和更新后隐私量的差值来衡量，将现有熵值 / 初始熵，即为数据损失度。越接近1，损失度越大；越接近0，损失度越小。初始熵表示原始数据的熵值，表示原数据的不确定性；现有熵并非香农熵，而是表明现有数据的不确定性。",
                "结果分析": {
                    "隐私降低程度": "48.43% 的隐私降低程度表明，隐私保护技术对数据集的安全性有显著影响，但同时也对数据可用性有影响，导致隐私损失接近一半。",
                    "结论": "平均隐私降低程度为48.43% 表明隐私保护技术对数据集的隐私性有一定影响，但损失度相对较低，接近0。这表明隐私保护措施在保护隐私的同时，尽可能减少了数据的损失，保持了数据的实用性。",
                    "改进建议": [
                        "优化数据泛化策略：通过更细粒度的泛化策略，减少数据的不确定性，提高数据的精确性。",
                        "提高数据匿名化水平：通过更高水平的匿名化技术，保护个人隐私不被轻易识别。"
                    ]
                }
            }
        }

    # 合规性性评估填充，就先这样吧，2024年11月12日19:30:28   仅做了填充，未对其分析
    if "合规性评估结果" in data:
        item = data["合规性评估结果"]
        # 提取最后一个数字的通用函数
        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)  # 匹配整数或小数
            return numbers[-1] if numbers else None  # 取最后一个数字

        # 提取 K, L, T 的最后一个数字
        K_me = extract_last_number(str(item['K-Anonymity']))
        l_value = extract_last_number(str(item['L-Diversity']))
        T_close = extract_last_number(str(item['T-Closeness']))

        # 从redis中获取SA列值
        redis_client=connect_redis()
        task_config = get_task_config(redis_client, worker_id+'-0')

        # 从任务配置中提取相关参数
        sa_columns = task_config["parameters"]["SA"]
        sa_list = ", ".join(sa_columns)

        # 提取(α,k)-匿名性
        AnonymityandLvalue=tuple(map(float, (re.search(r'\((.*?)\)', item['(α,k)-Anonymity'][0]).group(1)).split(',')))
        # 提取是否满足
        satisfaction_match = re.search(r'(满足|不满足)', item['(α,k)-Anonymity'][0])
        satisfaction = satisfaction_match.group(1) if satisfaction_match else None

        # 脱敏策略描述
        desensitization_strategy = (
            "本次数据脱敏采用了 K-匿名性、L-多样性、(α,k)-匿名性和 T-紧密性技术的综合应用，"
            ""
            "通过分组、随机化、泛化等方式，确保数据隐私性。"
            "根据合规性检测，数据处理符合 GDPR 和《个人信息保护法》中的数据匿名化要求，"
            "保障数据在共享和分析过程中的安全性。"
        )

        report["隐私评估报告"]["数据合规性"] = {
            "脱敏策略合规性综述": {
                "技术描述": desensitization_strategy,
                "合规性检测": "已通过内部合规性检测，符合 GDPR 第 25 条中关于数据保护的设计要求，以及《个人信息保护法》中对于匿名化处理的规范。",
            },
            "K-Anonymity": {
                "指标摘要": f"本报告旨在评估数据集的 K-匿名性，特别关注是否满足 2-匿名性要求。K-匿名性是指每个等价类中至少包含 K 个记录，从而保证数据的匿名性和隐私保护。本次评估结果显示，数据集{item['K-Anonymity']}。",
                "K-Anonymity定义": "K-匿名性是指数据集被划分为多个等价类，每个等价类中至少包含 K 个记录。具体来说，如果一个数据集满足 K-匿名性，那么对于任何一个记录，至少有 K - 1 个其他记录具有相同的准标识符值。",
                "结果分析": f"根据评估结果，数据集{item['K-Anonymity']}。这意味着每个等价类中至少包含 {K_me} 个记录，从而保证了数据的匿名性和隐私保护。",
                "改进建议": [
                    f"提高匿名级别：如果数据集中的隐私风险较高，可以考虑提高 K 值，例如实现 {int(K_me) + 1}-匿名或更高的匿名性。",
                    "优化等价类划分策略：通过更合理的等价类划分策略，确保每个等价类中的记录数量更加均衡，进一步降低重识别攻击的风险。"
                ],
                "合规性要求": "数据处理符合 GDPR 第 5 条关于数据最小化原则的要求，确保隐私风险可控。"
            },
            "L-Diversity": {
                "指标摘要": f"本报告旨在评估数据集中不同隐私属性的 L-多样性，特别关注属性 {sa_list} 是否满足 {l_value}-多样性要求。"
                            f"L-多样性是一种增强的隐私保护方法，通过确保每个等价类中包含至少 L 个不同的敏感值，从而增强数据的隐私保护。",
                "L-Diversity定义": "L-多样性是指每个等价类中包含至少 L 个不同的敏感值。具体来说，如果一个数据集满足 L-多样性，那么对于任何一个等价类，至少包含 L 个不同的敏感值。",
                "结果分析": f"这些结果表明，属性 {sa_list} 在隐私保护措施应用后，均满足 {l_value}-多样性。这意味着每个等价类中至少包含 {l_value} 个不同的敏感值，从而增强了数据的隐私保护。",
                "改进建议": [
                    f"提高多样性级别：如果数据集中的隐私风险较高，可以考虑提高 L 值，例如实现 {int(l_value) + 1}-多样性或更高的多样性。",
                    "优化等价类划分策略：通过更合理的等价类划分策略，确保每个等价类中的敏感值更加多样化，进一步降低隐私泄露风险。",
                    "动态调整隐私保护措施：根据数据发布后的实际使用情况，动态调整隐私保护措施，以应对潜在的隐私泄露风险。"
                ],
                "合规性要求": "已通过合规性检测，符合 GDPR 和《个人信息保护法》中关于敏感属性匿名化的要求。"
            },
            "(α,k)-匿名性": {
                "指标摘要": f"本报告旨在评估数据集的 (α,k)-匿名性，特别关注隐私属性 {sa_list} 是否满足 ({AnonymityandLvalue[0]},{AnonymityandLvalue[1]})-匿名性要求。(α,k)-匿名性是一种增强的隐私保护方法，通过确保每个等价类中，敏感属性的频率不超过总记录数的 α 倍，且每个等价类中至少包含 k 个记录，从而增强数据的隐私保护。本次评估结果显示，隐私属性 day_start、day_end 和 hotel_name 均满足 ({AnonymityandLvalue[0]},{AnonymityandLvalue[1]})-匿名性。",
                "(α,k)-匿名性定义": f"(α,k)-匿名性是指每个等价类中，敏感属性的频率不超过总记录数的 α 倍，且每个等价类中至少包含 k 个记录。",
                "结果分析": f"这些结果表明，隐私属性 {sa_list} 在隐私保护措施应用后，{satisfaction} ({AnonymityandLvalue[0]},{AnonymityandLvalue[1]})-匿名性。这意味着每个等价类中至少包含 {AnonymityandLvalue[1]} 个记录，且敏感属性的频率不超过总记录数的 {AnonymityandLvalue[0]} 倍，从而增强了数据的隐私保护。",
                "改进建议": [
                    f"提高匿名级别：如果数据集中的隐私风险较高，可以考虑提高 k 值和降低 α 值，例如实现 ({float(AnonymityandLvalue[0])/2},{AnonymityandLvalue[1]+1})-匿名或更高的匿名性。",
                    "频率限制：对等价类中的敏感属性频率进行限制，确保其不超过 α 倍的总记录数。",
                    "分布优化：调整等价类划分策略，优化敏感属性的频率分布，增强匿名性效果。"
                ],
                "合规性要求": "数据处理符合 GDPR 中的隐私保护设计原则，同时满足行业内常见的数据匿名化标准。"
            },
            "T-Closeness": {
                "指标摘要": f"本报告旨在评估数据集中不同隐私属性的 T-紧密性，特别关注属性 day_start、day_end 和 hotel_name是否满足 {T_close}-紧密性要求。T-紧密性是一种增强的隐私保护方法，通过确保敏感属性的分布在等价类和整个数据集之间的差异不超过某一阈值，从而增强数据的隐私保护。本次评估结果显示，等价组 ('男', '50-100', '43****************', '14********') 的隐私属性 day_start 和 day_end 不满足 0.95-紧密性，而隐私属性 hotel_name 满足 0.95-紧密性。",
                "T-Closeness定义": f"T-紧密性是指敏感属性的分布在等价类和整个数据集之间的差异不超过某一阈值t。具体来说，对于一个等价类和敏感属性，如果敏感属性在等价类中的分布和在整个数据集中的分布之间的距离不超过阈值t，则该等价类满足 T-紧密性。",
                "结果分析": f"{item['T-Closeness'][0]}。T-紧密性不足表示敏感属性在等价类和整个数据集之间的分布差异较大，存在较高的隐私泄露风险。",
                "改进建议": [
                    "增强隐私保护技术：采用更先进的隐私保护技术，减少敏感属性在等价类和整个数据集之间的分布差异。",
                    "分布对齐：优化等价类划分，使敏感属性的分布尽可能接近全局分布。",
                    "动态调整：根据隐私需求动态调整紧密性阈值，降低隐私泄露风险。"
                ],
                "合规性要求": "已通过合规性检测，符合 GDPR 和《个人信息保护法》中对敏感属性分布差异的限制要求。"
            }
        }

    # 数据质量评估填充
    if "匿名集数据质量评估结果" in data:
        item = data["匿名集数据质量评估结果"]

        # 从redis中获取SA列值
        redis_client = connect_redis()
        task_config = get_task_config(redis_client, worker_id + '-0')

        # 从任务配置中提取相关参数 SA
        sa_columns = task_config["parameters"]["SA"]
        sa_list = ", ".join(sa_columns)

        # 合并列表元素为一个字符串
        distribution_leakage_list = item["分布泄露"]
        combined_string = " ".join(distribution_leakage_list)  # 用空格合并，也可以改成其他分隔符

        distribution_leakage_list = item["熵泄露"]
        shangxielou_string = " ".join(distribution_leakage_list)  # 用空格合并，也可以改成其他分隔符

        distribution_leakage_list = item["正面信息披露"]
        zhengmian_string = " ".join(distribution_leakage_list)  # 用空格合并，也可以改成其他分隔符

        distribution_leakage_list = item["KL_Divergence"]
        sandu_string = " ".join(distribution_leakage_list)  # 用空格合并，也可以改成其他分隔符

        report["隐私评估报告"]["数据质量评估"] ={
            "分布泄露": {
                "指标摘要": f"本报告旨在评估数据集中不同属性的分布泄露，特别关注属性 {sa_list}。通过计算欧几里得距离，评估这些属性在隐私保护技术应用后分布的变化情况。本次评估结果显示， {combined_string}。",
                "分布泄露定义": f"对于某一个敏感属性，确定某个等价组和原始数据在发布前和发布后的概率分布，计算两者之间的欧几里得距离。分布泄漏可以看作是属性值分布从一种状态到另一种状态的总体发散度的度量。对于每个给定的等价类，测量原始数据集和已发布数据集中敏感属性值的原始分布之间的泄漏。",
                "结果分析": f"评估结果表明，{combined_string}，XXX",
                "结论": f"分布泄露的计算结果表明，{combined_string}。保持分布的一致性是隐私保护技术的目标之一，较小的分布泄露值表明数据发布后的隐私泄露风险较低。",
                "改进建议": [
                    "增强隐私保护技术：采用更先进的隐私保护技术，减少原始数据与发布数据之间的分布差异。",
                    "优化数据发布策略：通过合理的数据发布策略，保持数据分布的一致性，减少分布泄露。",
                    "动态调整等价类：根据实际数据情况，动态调整等价类的划分策略，减少分布泄露风险。"
                ]
            },
            "熵泄露": {
                "指标摘要": f"本报告旨在评估数据集中不同属性的熵泄露，特别关注属性 {sa_list}。通过计算熵泄露，评估这些属性在隐私保护技术应用后分布变化带来的隐私泄露情况。本次评估结果显示，{shangxielou_string}",
                "熵泄露定义": f"对于某一个敏感属性，确定某个等价组和原始数据在发布前和发布后的概率分布，初始分布和发布分布之间的熵差异。",
                "结果分析": f"这些结果表明，{shangxielou_string}熵泄露值越高，表示原始分布与发布分布之间的差异越大，隐私泄露风险越高。",
                "结论": f"熵泄露的计算结果表明，{shangxielou_string}。保持分布的一致性是隐私保护技术的目标之一，较高的熵泄露值表明数据发布后的隐私泄露风险较高。",
                "改进建议": [
                    "增强隐私保护技术：采用更先进的隐私保护技术，减少原始数据与发布数据之间的分布差异。",
                    "优化数据发布策略：通过合理的数据发布策略，保持数据分布的一致性，减少熵泄露。",
                    "动态调整等价类：根据实际数据情况，动态调整等价类的划分策略，减少分布泄露风险。"
                ]
            },
            "正面信息披露": {
                "指标摘要": f"本报告旨在评估数据集中不同属性的正面信息披露，特别关注属性 {sa_list}。通过计算正面信息披露量，评估这些属性在隐私保护技术应用后，公开信息可能带来的隐私泄露风险。本次评估结果显示，{zhengmian_string}",
                "正面信息披露定义": f"正面信息披露量衡量的是发布信息导致敏感信息泄露的概率增加量。",
                "结果分析": f"这些结果表明，{zhengmian_string}",
                "结论": f"正面信息披露的计算结果表明，{zhengmian_string}，仍需关注其潜在的隐私风险。",
                "改进建议": [
                    "增强隐私保护技术：采用更先进的隐私保护技术，减少发布信息与敏感信息之间的关联性。",
                    "优化数据发布策略：通过合理的数据发布策略，控制正面信息披露量，减少隐私泄露风险。",
                    "动态调整发布信息：根据实际数据情况，动态调整发布信息的内容和方式，减少正面信息披露风险。"
                ]
            },
            "KL-Divergence": {
                "指标摘要": f"本报告旨在评估数据集中不同属性的 Kullback-Leibler (KL) 散度，特别关注属性 {sa_list}。KL 散度用于衡量两个频率分布之间的距离，表示使用整个数据集中敏感属性的分布来近似等价类中相同属性的分布时的信息损失量。本次评估结果显示，{sandu_string}。",
                "KL-Divergence定义": f"确定敏感属性在原始数据集和隐私保护技术应用后的数据集中的分布，计算两者之间的KL散度。",
                "结果分析": f"这些结果表明，{sandu_string}。KL 散度值越高，表示原始分布与发布分布之间的差异越大，信息损失越大。",
                "结论": f"KL 散度的计算结果表明，{sandu_string}。较高的 KL 散度值表明数据发布后的信息损失较大。",
                "改进建议": [
                    "增强隐私保护技术：采用更先进的隐私保护技术，减少原始数据与发布数据之间的分布差异。",
                    "优化数据发布策略：通过合理的数据发布策略，控制 KL 散度，减少信息损失。",
                    "动态调整等价类：根据实际数据情况，动态调整等价类的划分策略，减少分布泄露风险。"
                ]
            },
            "唯一性": {
                "指标摘要": f"本报告旨在评估数据集中的唯一性。唯一性是指数据集中唯一记录的占比，即在等价类中仅包含一个记录的比例。唯一记录比非唯一记录更容易被重新识别，极易遭受重标识攻击和偏斜攻击。本次评估结果显示，数据集中唯一记录的占比为 {item['唯一性']}。",
                "唯一性定义": f"唯一性是指数据集中唯一记录的占比。在最简单的定义中，如果等价类中只有一个记录，那么该记录被认为是唯一的。",
                "结果分析": f"根据评估结果，数据集中唯一记录的占比为 {item['唯一性']}。这表明数据集中不存在唯一记录，所有记录都属于多个等价类。尽管唯一记录更容易被重新识别，但本数据集由于没有唯一记录，从理论上讲，重新识别的风险较低。然而，这并不意味着数据集完全安全，因为重标识攻击和偏斜攻击依然可能通过其他途径实现。",
                "结论": f"数据集中的唯一记录占比为 {item['唯一性']}，表明数据集中不存在唯一记录，所有记录都在多个等价类中。然而，这并不完全消除隐私泄露的风险。虽然唯一记录更容易被重新识别，但其他类型的攻击仍然可能威胁数据隐私。",
                "改进建议": [
                    "增强隐私保护技术：采用差分隐私等先进的隐私保护技术，以增加数据的匿名性和安全性。",
                    "优化等价类划分策略：通过更合理的等价类划分策略，确保每个等价类中的记录数量尽可能多，减少重标识攻击的可能性。",
                    "动态调整隐私保护措施：根据数据发布后的实际使用情况，动态调整隐私保护措施，以应对潜在的隐私泄露风险。"
                ]
            }
        }

    # 数据特征填充
    if "匿名集数据特征评估结果" in data:
            item = data["匿名集数据特征评估结果"]
            # print(item)
            sensitive_attribute_dimension = item["敏感属性维数"]
            quasi_identifier_dimension = item["准标识符维数"]
            sensitive_attribute_dimension = item["敏感属性维数"]
            inherent_privacy = item["固有隐私"]
            summary_of_report = ""
            resultanalysis = " "
            for key in item["敏感属性种类"]:
                summary_of_report += key + " "
                resultanalysis += key + " "
            average_degree_of_generalization = item["平均泛化程度"]
            report["隐私评估报告"]["数据特征"] = {
                "准标识符维数": {
                    "报告摘要": f"本报告旨在评估数据集中的准标识符维数。准标识符维数是指用于标识个体身份的属性数量。本次评估结果显示，数据集的准标识符维数为 {sensitive_attribute_dimension}。",
                    "准标识符维数定义": "准标识符（Quasi-Identifier, QI）是指那些能够单独或联合识别个体身份的属性。准标识符维数是指数据集中用作准标识符的属性数量。",
                    "结果分析": f"通过统计以上准标识符属性的数量，得出数据集的准标识符维数为 {quasi_identifier_dimension}。",
                    "结论": f"数据集的准标识符维数为 {quasi_identifier_dimension}，这意味着数据集中有 {quasi_identifier_dimension} 个属性可以用于标识个体身份。这些属性包括性别、年龄范围、部分身份证号码和部分电话号码。了解准标识符维数有助于评估数据集的隐私泄露风险，并为进一步的隐私保护措施提供依据。",
                    "改进建议": [
                        "减少准标识符维数：通过泛化或删除某些准标识符属性，减少准标识符维数，从而降低隐私泄露风险。",
                        "增强隐私保护技术：采用更先进的隐私保护技术，如差分隐私，以保护准标识符属性的隐私。",
                        "优化数据发布策略：通过合理的数据发布策略，确保发布的数据在保持实用性的同时，最大限度地保护隐私。"
                    ]
                },
                "敏感属性维数": {
                    "报告摘要": f"本报告旨在评估数据集中的敏感属性维数。敏感属性维数是指数据集中涉及隐私保护的敏感属性数量。本次评估结果显示，数据集的敏感属性维数为 {sensitive_attribute_dimension}。",
                    "敏感属性维数定义": "敏感属性（Sensitive Attribute, SA）是指那些包含个人隐私信息或需要严格保护的信息，如医疗记录、财务信息等。敏感属性维数是指数据集中用作敏感属性的数量。",
                    "结果分析": f"通过统计以上敏感属性的数量，得出数据集的敏感属性维数为 {sensitive_attribute_dimension}。",
                    "结论": f"数据集的敏感属性维数为 {sensitive_attribute_dimension}，这意味着数据集中有 {sensitive_attribute_dimension} 个属性需要进行隐私保护。这些属性包括开始日期、结束日期和酒店名称。了解敏感属性维数有助于评估数据集的隐私泄露风险，并为进一步的隐私保护措施提供依据。",
                    "改进建议": [
                        "增加敏感属性保护措施：通过加密、泛化或扰动等技术，对敏感属性进行保护，以减少隐私泄露风险。",
                        "优化数据发布策略：通过合理的数据发布策略，确保发布的数据在保持实用性的同时，最大限度地保护敏感属性的隐私。",
                        "定期评估和更新保护措施：根据数据使用情况和隐私保护技术的发展，定期评估和更新敏感属性的保护措施。"
                    ]
                },
                "敏感属性种类": {
                    "报告摘要": f"本报告旨在评估数据集中的敏感属性种类。敏感属性是指那些包含个人隐私信息或需要严格保护的信息。本次评估结果显示，数据集中的敏感属性种类包括 {summary_of_report}",
                    "敏感属性种类定义": "敏感属性（Sensitive Attribute, SA）是指那些包含个人隐私信息或需要严格保护的信息。识别数据集中的敏感属性是数据隐私保护的首要任务。",
                    "结果分析": f"根据评估结果，数据集中的敏感属性种类包括以下 {sensitive_attribute_dimension} 个：{resultanalysis}",
                    "结论": f"数据集中的敏感属性种类包括{resultanalysis}。这些属性包含个人隐私信息，需在数据发布和使用过程中进行严格保护。了解敏感属性种类有助于评估数据集的隐私泄露风险，并为进一步的隐私保护措施提供依据。",
                    "改进建议": [
                        "增强敏感属性保护措施：采用加密、泛化或扰动等技术，对敏感属性进行保护，以减少隐私泄露风险。",
                        "优化数据发布策略：通过合理的数据发布策略，确保发布的数据在保持实用性的同时，最大限度地保护敏感属性的隐私。",
                        "定期评估和更新保护措施：根据数据使用情况和隐私保护技术的发展，定期评估和更新敏感属性的保护措施。"
                    ]
                },
                "归一化平均等价组大小度量": {
                    "报告摘要": f"本报告旨在评估数据集的归一化平均等价组大小度量。归一化平均等价组大小是指平均等价组大小与 K 值的比值，用于衡量数据集的隐私保护效果。本次评估结果显示，数据集的归一化平均等价组大小度量为 {average_degree_of_generalization}。",
                    "归一化平均等价组大小定义": "归一化平均等价组大小度量的定义为平均等价组大小与 K 值的比值。其中，平均等价组大小是数据集中所有等价组大小的平均值，K 值是给定的K-Anonymity阈值。",
                    "结果分析": f"数据集的归一化平均等价组大小度量为 {average_degree_of_generalization}。数值比较大，表明数据的处理效果远超出预期。",
                    "结论": f"数据集的归一化平均等价组大小度量为 {average_degree_of_generalization}，表明每个等价组中记录的平均数量相对于 K 值较大。这意味着数据集的等价组划分较为均匀，隐私保护效果较好，但也需要进一步分析具体等价组的分布情况，以确保数据的隐私保护水平。",
                    "改进建议": [
                        "优化等价组划分策略：通过更合理的等价组划分策略，确保每个等价组中的记录数量更加均衡，进一步降低隐私泄露风险。",
                        "增强隐私保护技术：采用差分隐私等先进的隐私保护技术，以增加数据的匿名性和安全性。",
                        "定期评估和更新保护措施：根据数据使用情况和隐私保护技术的发展，定期评估和更新隐私保护措施。"
                    ]
                },
                "固有隐私": {
                    "报告摘要": f"本报告旨在评估数据集的固有隐私（Inherent Privacy）。固有隐私通过熵来衡量随机变量在面对攻击者时的隐私保护程度，表示攻击者需要回答的二元问题的数量。本次评估结果显示，数据集的固有隐私为 {inherent_privacy}。",
                    "固有隐私定义": "即与数据集概率分布的不确定性相同的均匀分布区间长度，通过熵来描述随机变量（这里指隐私信息）的保护程度。",
                    "结果分析": f"通过计算，数据集的固有隐私为{inherent_privacy}。这意味着在没有其他条件信息的情况下，攻击者需要回答 {inherent_privacy}个二元问题才能确定随机变量 的具体值。",
                    "结论": f"数据集的固有隐私为 {inherent_privacy}，表明数据集在隐私保护方面具有较高的强度。在没有其他条件信息的情况下，攻击者需要回答大量的二元问题才能识别出敏感信息，从而有效保护了数据的隐私性。",
                    "改进建议": [
                        "减少条件信息的影响：通过限制条件信息的发布，减少其对隐私属性的影响，增加固有隐私的保护强度。",
                        "增强隐私保护技术：采用差分隐私等先进的隐私保护技术，以增加数据的匿名性和安全性。",
                        "定期评估和更新保护措施：根据数据使用情况和隐私保护技术的发展，定期评估和更新隐私保护措施。"
                    ]
                }
            }

    if "隐私保护性度量评估结果" in data:
            item = data["隐私保护性度量评估结果"]

            sensitive_attribute = []
            for attribute in item["敏感属性的重识别风险"]:
                attribute_a = {}
                attribute_a["敏感属性"] = re.search('(?<=敏感属性)[a-z_]*', attribute).group()
                attribute_a["重识别风险"] = re.search('(?<=重识别风险为：)[0-9\.\%]*', attribute).group()
                attribute_a["等价组"] = re.search('(?<=等价组为).*', attribute).group()
                sensitive_attribute.append(attribute_a)

            Summary_of_report = "本报告旨在评估数据集中敏感属性的重识别风险，评估这些属性在数据发布后的重识别风险，并提出相应的建议。本次评估结果显示，特别关注属性为"
            Result_analysis = ""
            conclusion = ""
            Re_identification = item["基于熵的重识别风险"]
            Reidentification_risk = re.search('(?<=重识别风险为：)[0-9\.\%]*', item["基于熵的重识别风险"]).group()
            Equivalence_group = re.search('(?<=等价组为).*', item["基于熵的重识别风险"]).group()

            for attribute in sensitive_attribute:
                Summary_of_report += attribute["敏感属性"] + "," + attribute["敏感属性"] + "的重识别风险为 " + \
                                     attribute["重识别风险"]
                Result_analysis += attribute_a["敏感属性"] + "的重识别风险较高,为" + attribute_a["重识别风险"]
                conclusion += attribute_a["敏感属性"] + "在等价组" + attribute_a["等价组"] + "的重识别风险较高。"

            sensitive_attribute = []
            for attribute in item["单个属性的重识别风险"]:
                attribute_a = {}
                attribute_a["标识符属性"] = re.search('(?<=标识符属性)[a-z_]*', attribute).group()
                attribute_a["重识别风险"] = re.search('(?<=重识别风险为：)[0-9\.\%]*', attribute).group()
                attribute_a["属性值"] = re.search('(?<=对应属性值为).*', attribute).group()
                sensitive_attribute.append(attribute_a)

            Summary_of_report = ""
            Result_analysis = ""
            conclusion = ""
            for attribute in sensitive_attribute:
                Summary_of_report += attribute["标识符属性"] + "为" + attribute["重识别风险"] + ","
                if float(attribute["重识别风险"].replace('%', '')) / 100 > 0.0030:
                    Result_analysis += attribute["标识符属性"] + "的重识别风险较高，为：" + attribute["重识别风险"] + "。"
                    conclusion += attribute["标识符属性"] + "具有较高的重识别风险" + "，"
                else:
                    Result_analysis += attribute["标识符属性"] + "的重识别风险较低，为：" + attribute["重识别风险"] + "。"
                    conclusion += attribute["标识符属性"] + "具有较低的重识别风险" + "，"

            report["隐私评估报告"]["隐私保护性度量"] = {
                "敏感属性的重识别风险": {
                    "报告摘要": f"{Summary_of_report}",
                    "敏感属性的重识别风险定义": "该算法通过计算隐私保护技术应用后的最大熵和相对熵来量化敏感属性的重识别风险。其中最大熵值表示信息的不确定性，相对熵值表示隐私保护技术对数据的安全保护影响。通过评估敏感属性的重识别风险，可以更好地了解数据发布后的隐私风险，并采取适当的措施进行保护。",
                    "结果分析": f"{Result_analysis}",
                    "结论": f"{conclusion}",
                    "改进建议": [
                        "增强隐私保护技术：采用更先进的隐私保护技术，如差分隐私，以降低重识别风险。",
                        "优化数据泛化策略：通过更细粒度的泛化策略，减少数据的不确定性，提高数据的准确性。",
                        "提高数据匿名化水平：通过更高水平的匿名化技术，保护个人隐私不被轻易识别。",
                        "动态调整等价组：根据实际数据情况，动态调整等价组，以降低特定组合的重识别风险。"
                    ]
                },
                "整体的重识别风险": {
                    "报告摘要": f"本报告旨在评估数据集中所有敏感属性的整体重识别风险。通过基于熵的重识别风险测量模型，评估数据集中所有敏感属性在数据发布后的重识别风险。本次评估结果显示，{Re_identification} ",
                    "整体的重识别风险定义": "整体的重识别风险定义与敏感属性的重识别风险定义基本相同，唯一不同的地方在于，其关注的是所有的敏感属性，评估所有敏感属性的整体风险。",
                    "结果分析": f"通过评估所有敏感属性，计算得到整体重识别风险。{Re_identification}",
                    "结论": f"整体重识别风险为 {Reidentification_risk}，表明数据集中所有敏感属性在隐私保护措施应用后，仍存在较高的重识别风险。高风险等价组{Equivalence_group}显示在这些属性组合下，个人信息的重识别概率较大。",
                    "改进建议": [
                        "增强隐私保护技术：采用更先进的隐私保护技术，如差分隐私，以降低重识别风险。",
                        "优化数据泛化策略：通过更细粒度的泛化策略，减少数据的不确定性，提高数据的准确性。",
                        "提高数据匿名化水平：通过更高水平的匿名化技术，保护个人隐私不被轻易识别。",
                        "动态调整等价组：根据实际数据情况，动态调整等价组，以降低特定组合的重识别风险。"
                    ]
                },
                "单个属性的重识别风险": {
                    "报告摘要": f"本报告旨在评估数据集中每个准标识符属性的重识别风险。与整体重识别风险和敏感属性的重识别风险不同，本报告聚焦于单个准标识符属性，并评估攻击者利用其在数据发布后可能造成的重识别风险。本次评估结果显示，不同准标识符属性的基于熵的重识别风险如下：{Summary_of_report}",
                    "单个属性的重识别风险定义": "单个属性的重识别风险和上述两个指标的在于，其不会以等价组的角度考虑重识别风险，而是等价组中的某一个具体的准标识符属性考虑数据的重识别风险。即，聚焦于单个准标识符属性，评估攻击者利用其在数据发布后可能造成的重识别风险。",
                    "结果分析": f"结果表明，不同属性的重识别风险存在显著差异。其中，{Result_analysis}",
                    "结论": f"单个属性的重识别风险评估结果表明，{conclusion}这需要在数据发布时特别关注高风险属性，采取更严格的隐私保护措施。",
                    "改进建议": [
                        "增强隐私保护技术：采用更先进的隐私保护技术，如差分隐私，以降低高风险属性的重识别风险。",
                        "优化数据泛化策略：通过更细粒度的泛化策略，减少高风险属性的不确定性，提高数据的准确性。",
                        "提高数据匿名化水平：通过更高水平的匿名化技术，保护高风险属性的个人隐私不被轻易识别。",
                        "动态调整属性保护策略：根据实际数据情况，动态调整高风险属性的保护策略，以降低特定属性的重识别风险。"
                    ]
                }
            }
    # 返回有序的 JSON 响应
    report = generate_privacy_report(worker_id, report)

    return jsonify(report)




## Page 1 任务总数







## Page 2 指标体系可视化

# 数据图谱
# 标签到种类的映射
# 数据：call  移动通信场景
label_to_category = {
    "User": "用户id",
    "Calling_party_number": "主叫号码",
    "Calling_party_province": "主叫省份",
    "Calling_party_city": "主叫城市",
    "Called_party_number": "被叫号码",
    "Called_party_province": "被叫省份",
    "Called_party_city": "被叫城市",
    "Talk_time": "通话时间",
    "Call_duration": "通话时长"
}
lp_to_category = {
    "Bill_id": "账单id",
    "User": "用户id",
    "Mobile_phone_number": "电话号码",
    "Package_monthly_rent": "订单种类",
    "Extra_fee": "增值费",
    "Total_cost": "总金额",
    "Deduct_time": "交易时间",
    "Account_balance": "账户余额"
}
ls_to_category = {
    "User": "用户id",
    "Sender_number": "发送方电话",
    "Sender_province": "发送方省份",
    "Sender_city": "发送方市区",
    "Receiver_number": "接收方电话",
    "Receiver_province": "接收方省份",
    "Receiver_city": "接收方市区",
    "Sending_time": "短信时间",
    "Sms_content": "短信内容",
}
lm_to_category = {
    "User": "姓名",
    "Sex": "性别",
    "Birthday": "生日",
    "Age": "年龄",
    "Id_number": "身份证号",
    "Phone": "电话号码",
    "Day": "诊疗日期",
    "Address": "家庭住址",
    "Hospital_department": "科室",
    "Symptom": "症状"
}

@api_show.route('/telecom_graph', methods=['GET'])
def get_telecom_graph():
    # Cypher 查询
    query = """
    MATCH (n)-[r]->(m)
    WHERE n.scene = '移动通信'
    RETURN labels(n) AS source_labels, n AS source_node,
           labels(m) AS target_labels, m AS target_node, r AS relationship
    LIMIT 100
    """

    # 加载文件内容到内存
    with open("./telem.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    # 使用字典存储节点和列表存储关系
    nodes = {}
    links = []

    # 遍历查询结果
    for record in result:
        # 提取源节点、目标节点和关系
        source_node = record["source_node"]
        target_node = record["target_node"]
        relationship = record["relationship"]

        # 提取节点标签
        source_label = record["source_labels"][0]  # 假设每个节点只有一个标签
        target_label = record["target_labels"][0]

        # 根据标签获取种类
        source_category = label_to_category.get(source_label, "未知种类")
        target_category = label_to_category.get(target_label, "未知种类")

        # 添加源节点到 nodes（避免重复）
        if source_node["identity"] not in nodes:
            nodes[source_node["identity"]] = {
                "id": source_node["identity"],  # 使用 identity 作为唯一标识
                "name": source_node["properties"].get("user_id", source_node["properties"].get("value", "未初始化")),
                "category": source_category,  # 分类
                "symbolSize": 40 if source_category == "用户id" else 20,  # 根据类型调整大小
                "scene": source_node["properties"].get("scene", "无场景信息的属性节点")  # 场景信息
            }

        # 添加目标节点到 nodes（避免重复）
        if target_node["identity"] not in nodes:
            nodes[target_node["identity"]] = {
                "id": target_node["identity"],
                "name": target_node["properties"].get("value", target_node["properties"].get("name", "未初始化")),
                "category": target_category,
                "symbolSize": 40 if target_category == "用户id" else 20,
                "scene": target_node["properties"].get("scene", "座机通话")
            }

        # 添加关系到 links
        links.append({
            "source": source_node["identity"],  # 源节点 ID
            "target": target_node["identity"],  # 目标节点 ID
            "value": relationship["type"],  # 关系类型
            "scene": relationship["properties"].get("scene", "未知场景")  # 场景信息
        })

    # 转换为 ECharts 支持的格式
    return jsonify({
        "nodes": list(nodes.values()),  # 将字典转换为列表
        "links": links
    })


# 医疗卫生
@api_show.route('/medical_graph', methods=['GET'])
def get_medical_graph():
    # Cypher 查询
    query = """
    MATCH (n)-[r]->(m)
    WHERE n.scene = '医疗卫生'
    RETURN labels(n) AS source_labels, n AS source_node,
           labels(m) AS target_labels, m AS target_node, r AS relationship
    LIMIT 100
    """

    # 加载文件内容到内存
    with open("./medical.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    # 使用字典存储节点和列表存储关系
    nodes = {}
    links = []

    # 遍历查询结果
    for record in result:
        # 提取源节点、目标节点和关系
        source_node = record["source_node"]
        target_node = record["target_node"]
        relationship = record["relationship"]

        # 提取节点标签
        source_label = record["source_labels"][0]  # 假设每个节点只有一个标签
        target_label = record["target_labels"][0]

        # 根据标签获取种类
        source_category = lm_to_category.get(source_label, "未知种类")
        target_category = lm_to_category.get(target_label, "未知种类")

        # 添加源节点到 nodes（避免重复）
        if source_node["identity"] not in nodes:
            nodes[source_node["identity"]] = {
                "id": source_node["identity"],  # 使用 identity 作为唯一标识
                "name": source_node["properties"].get("user_id", source_node["properties"].get("value", "未初始化")),
                "category": source_category,  # 分类
                "symbolSize": 40 if source_category == "姓名" else 20,  # 根据类型调整大小
                "scene": source_node["properties"].get("scene", "无场景信息的属性节点")  # 场景信息
            }

        # 添加目标节点到 nodes（避免重复）
        if target_node["identity"] not in nodes:
            nodes[target_node["identity"]] = {
                "id": target_node["identity"],
                "name": target_node["properties"].get("value", target_node["properties"].get("name", "未初始化")),
                "category": target_category,
                "symbolSize": 40 if target_category == "姓名" else 20,
                "scene": target_node["properties"].get("scene", "医疗卫生")
            }

        # 添加关系到 links
        links.append({
            "source": source_node["identity"],  # 源节点 ID
            "target": target_node["identity"],  # 目标节点 ID
            "value": relationship["type"],  # 关系类型
            "scene": relationship["properties"].get("scene", "未知场景")  # 场景信息
        })

    # 转换为 ECharts 支持的格式
    return jsonify({
        "nodes": list(nodes.values()),  # 将字典转换为列表
        "links": links
    })

# 短信业务
@api_show.route('/sms_graph', methods=['GET'])
def get_sms_graph():
    # Cypher 查询
    query = """
        MATCH (n)-[r]->(m)
        WHERE n.scene = '短信业务'
        RETURN labels(n) AS source_labels, n AS source_node,
               labels(m) AS target_labels, m AS target_node, r AS relationship
        LIMIT 300
        """
    # 加载文件内容到内存
    with open("./sms.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    # 使用字典存储节点和列表存储关系
    nodes = {}
    links = []

    # 遍历查询结果
    for record in result:
        # 提取源节点、目标节点和关系
        source_node = record["source_node"]
        target_node = record["target_node"]
        relationship = record["relationship"]

        # 提取节点标签
        source_label = record["source_labels"][0]  # 假设每个节点只有一个标签
        target_label = record["target_labels"][0]

        # 根据标签获取种类
        source_category = ls_to_category.get(source_label, "未知种类")
        target_category = ls_to_category.get(target_label, "未知种类")

        # 添加源节点到 nodes（避免重复）
        if source_node["identity"] not in nodes:
            nodes[source_node["identity"]] = {
                "id": source_node["identity"],  # 使用 identity 作为唯一标识
                "name": source_node["properties"].get("user_id", source_node["properties"].get("value", "未初始化")),
                "category": source_category,  # 分类
                "symbolSize": 40 if source_category == "用户id" else 20,  # 根据类型调整大小
                "scene": source_node["properties"].get("scene", "无场景信息的属性节点")  # 场景信息
            }

        # 添加目标节点到 nodes（避免重复）
        if target_node["identity"] not in nodes:
            nodes[target_node["identity"]] = {
                "id": target_node["identity"],
                "name": target_node["properties"].get("value", target_node["properties"].get("name", "未初始化")),
                "category": target_category,
                "symbolSize": 40 if target_category == "用户id" else 20,
                "scene": target_node["properties"].get("scene", "短信业务")
            }

        # 添加关系到 links
        links.append({
            "source": source_node["identity"],  # 源节点 ID
            "target": target_node["identity"],  # 目标节点 ID
            "value": relationship["type"],  # 关系类型
            "scene": relationship["properties"].get("scene", "未知场景")  # 场景信息
        })

    # 转换为 ECharts 支持的格式
    return jsonify({
        "nodes": list(nodes.values()),  # 将字典转换为列表
        "links": links
    })
# 网络支付
@api_show.route('/purch_graph', methods=['GET'])
def get_telecm_graph():
    # Cypher 查询
    query = """
    MATCH (n)-[r]->(m)
    WHERE n.scene = '网络支付'
    RETURN labels(n) AS source_labels, n AS source_node,
           labels(m) AS target_labels, m AS target_node, r AS relationship
    LIMIT 100
    """
    # 加载文件内容到内存
    with open("./purch.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    # 使用字典存储节点和列表存储关系
    nodes = {}
    links = []

    # 遍历查询结果
    for record in result:
        # 提取源节点、目标节点和关系
        source_node = record["source_node"]
        target_node = record["target_node"]
        relationship = record["relationship"]

        # 提取节点标签
        source_label = record["source_labels"][0]  # 假设每个节点只有一个标签
        target_label = record["target_labels"][0]

        # 根据标签获取种类
        source_category = lp_to_category.get(source_label, "未知种类")
        target_category = lp_to_category.get(target_label, "未知种类")

        # 添加源节点到 nodes（避免重复）
        if source_node["identity"] not in nodes:
            nodes[source_node["identity"]] = {
                "id": source_node["identity"],  # 使用 identity 作为唯一标识
                "name": source_node["properties"].get("user_id", source_node["properties"].get("value", "未初始化")),
                "category": source_category,  # 分类
                "symbolSize": 40 if source_category == "用户id" else 20,  # 根据类型调整大小
                "scene": source_node["properties"].get("scene", "无场景信息的属性节点")  # 场景信息
            }

        # 添加目标节点到 nodes（避免重复）
        if target_node["identity"] not in nodes:
            nodes[target_node["identity"]] = {
                "id": target_node["identity"],
                "name": target_node["properties"].get("value", target_node["properties"].get("name", "未初始化")),
                "category": target_category,
                "symbolSize": 40 if target_category == "用户id" else 20,
                "scene": target_node["properties"].get("scene", "网络支付")
            }

        # 添加关系到 links
        links.append({
            "source": source_node["identity"],  # 源节点 ID
            "target": target_node["identity"],  # 目标节点 ID
            "value": relationship["type"],  # 关系类型
            "scene": relationship["properties"].get("scene", "未知场景")  # 场景信息
        })

    # 转换为 ECharts 支持的格式
    return jsonify({
        "nodes": list(nodes.values()),  # 将字典转换为列表
        "links": links
    })

#  多样本属性关联

@api_show.route('/link_graph', methods=['GET'])
def get_link_graph():
    ## 进行重识别
    q="""
    MATCH (u1:User)-[r1]->(other1), (u2:User)-[r2]->(other2)
    WHERE u1.user_id = u2.user_id AND u1.scene <> u2.scene
    WITH u1, u2, COLLECT(DISTINCT r1) AS relations1, COLLECT(DISTINCT r2) AS relations2, other1, other2
    RETURN 
        labels(u1) AS source_labels1, 
        u1 AS source_node1,
        labels(u2) AS source_labels2, 
        u2 AS source_node2,
        labels(other1) AS target_labels1, 
        other1 AS target_node1,
        labels(other2) AS target_labels2, 
        other2 AS target_node2,
        relations1 AS relationships_from_u1, 
        relations2 AS relationships_from_u2,
        {source: u1.user_id, target: u2.user_id, relationshipType: "LOGICAL_LINK", style: "dashed"} AS virtual_relationship
    LIMIT 100
    """
    """
    MATCH (u:User)-[r]->(b)
    WHERE r.scene IN ["基础信息", "医疗卫生"]
    WITH b, COLLECT(DISTINCT r.scene) AS relatedScenes, COLLECT(DISTINCT {user: u, relation: r}) AS details
    WHERE SIZE(relatedScenes) > 1  // 筛选出与多个场景相关的目标节点
    UNWIND details AS detail
    RETURN 
    labels(detail.user) AS source_labels, 
    detail.user AS source_node,
    labels(b) AS target_labels, 
    b AS target_node, 
    detail.relation AS relationship
    """
    # 加载文件内容到内存
    with open("./link.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    # 使用字典存储节点和列表存储关系
    # with open("re.json","r",encoding="utf-8-sig") as f:
    #     result+=json.load(f)
    nodes = {}
    links = []

    # 遍历查询结果
    for record in result:
        # 提取源节点、目标节点和关系
        source_node = record["source_node"]
        target_node = record["target_node"]
        relationship = record["relationship"]

        # 提取节点标签
        source_label = record["source_labels"][0]  # 假设每个节点只有一个标签
        target_label = record["target_labels"][0]

        # 根据标签获取种类
        s_kind=(
                label_to_category.get(source_label) or
                lp_to_category.get(source_label) or
                ls_to_category.get(source_label) or
                lm_to_category.get(source_label) or
                "未知种类"  # 默认值
        )
        t_kind=(
            label_to_category.get(target_label) or
            lp_to_category.get(target_label) or
            ls_to_category.get(target_label) or
            lm_to_category.get(target_label) or
            "未知种类"  # 默认值
        )
        source_category = source_node["properties"].get("scene", "无场景信息的属性节点")
        target_category = target_node["properties"].get("scene", "无场景信息的属性节点")

        # 添加源节点到 nodes（避免重复）
        if source_node["identity"] not in nodes:
            nodes[source_node["identity"]] = {
                "id": source_node["identity"],  # 使用 identity 作为唯一标识
                "name": source_node["properties"].get("user_id", source_node["properties"].get("value", "未初始化")),
                "category": source_category,  # 场景分类
                "symbolSize": 40 if target_category == "用户id" else 20, # 根据类型调整大小
                "attr":s_kind,
                "scene": source_node["properties"].get("scene", "无场景信息的属性节点")  # 场景信息
            }

        # 添加目标节点到 nodes（避免重复）
        if target_node["identity"] not in nodes:
            nodes[target_node["identity"]] = {
                "id": target_node["identity"],
                "name": target_node["properties"].get("value", target_node["properties"].get("name", "未初始化")),
                "category": target_category,
                "attr": t_kind,
                "symbolSize": 40 if target_category == "用户id" else 20,
                "scene": target_node["properties"].get("scene", "属性节点")
            }

        # 添加关系到 links
        links.append({
            "source": source_node["identity"],  # 源节点 ID
            "target": target_node["identity"],  # 目标节点 ID
            "value": relationship["type"],  # 关系类型
            "scene": relationship["properties"].get("scene", "未知场景")  # 场景信息
        })

    # 转换为 ECharts 支持的格式
    return jsonify({
        "nodes": list(nodes.values()),  # 将字典转换为列表
        "links": links
    })


# 转换为 ECharts 格式的函数
def process_to_echart_format(query_results):
    with open("re.json", "r", encoding="utf-8-sig") as file:
        result = json.load(file)  # 加载 JSON 文件内容
    nodes = []
    links = []

    # 用于去重，避免重复节点或边
    added_nodes = set()
    added_links = set()

    for record in result:
        # 处理源节点 u1 和 u2
        for node, label in [("source_node1", "source_labels1"), ("source_node2", "source_labels2")]:
            node_id = record[node]["identity"]
            if node_id not in added_nodes:
                nodes.append({
                    "id": node_id,
                    "name": f"{node_id} ({record[node]['scene']})",
                    "category": record[label][0] if record[label] else "Unknown",
                    "symbolSize": 30  # 节点大小，可调整
                })
                added_nodes.add(node_id)

        # 处理目标节点 other1 和 other2
        for target, label in [("target_node1", "target_labels1"), ("target_node2", "target_labels2")]:
            target_id = str(record[target])  # 假设 target_node 没有明确 ID，用整个对象作为标识
            if target_id not in added_nodes:
                nodes.append({
                    "id": target_id,
                    "name": target_id,
                    "category": record[label][0] if record[label] else "Unknown",
                    "symbolSize": 20  # 节点大小，可调整
                })
                added_nodes.add(target_id)

        # 处理关系 links
        for relation_list, source in [("relationships_from_u1", "source_node1"), ("relationships_from_u2", "source_node2")]:
            for relation in record[relation_list]:
                link_id = f"{record[source]['identity']}->{relation['type']}"
                if link_id not in added_links:
                    links.append({
                        "source": record[source]["user_id"],
                        "target": str(record[relation_list][0]),  # 假设关系指向了 target
                        "value": relation["type"],  # 关系类型
                        "lineStyle": {"type": "solid"}  # 实线
                    })
                    added_links.add(link_id)

        # 添加虚拟关系
        virtual_rel = record["virtual_relationship"]
        virtual_id = f"{virtual_rel['source']}->{virtual_rel['target']}"
        if virtual_id not in added_links:
            links.append({
                "source": virtual_rel["source"],
                "target": virtual_rel["target"],
                "value": virtual_rel["relationshipType"],
                "lineStyle": {"type": virtual_rel["style"]}  # 虚拟关系样式
            })
            added_links.add(virtual_id)

    return {"nodes": nodes, "links": links}

@api_show.route('/re_graph', methods=['GET'])
def get_re_graph():
    # 模拟处理逻辑并返回
    query=''
    echart_data = process_to_echart_format(query)
    return jsonify(echart_data)




# 知识图谱





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