# -*- coding: utf-8 -*-
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from flask import Blueprint, make_response, render_template, jsonify, request, Response
from flask import Flask, url_for

from neo4j import GraphDatabase
import json
import configparser
import os

from pandas.io.formats import string
import redis

from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, HRFlowable
from io import BytesIO
from pathlib import Path
import builtins
from logging import INFO
from mylog.logger import mylogger, set_logger


logger = mylogger(__name__, INFO)
set_logger(logger)

api_show = Blueprint('api_show', __name__)

##根据关系名得到属性节点的categories值
dict = {
    "has_addr": 1,
    "has_age": 2,
    "has_bank": 3,
    "has_bir": 4,
    "has_email": 5,
    "has_id": 6,
    "has_mar": 7,
    "has_name": 8,
    "has_sex": 9,
    "has_phone": 10
}
kind = [
    {
        "name": "唯一标志符"
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
        "name": "性别"
    },
    {
        "name": "电话号码"
    }
]


@api_show.route('/receive_data', methods=['POST'])
def receive_data():
    """接收前端场景数据并存储到场景库"""
    try:
        raw_data = request.get_json()
        scene = raw_data["data_scene"]
        name = f"scene_template:{scene}"
        num = redis_client.hlen(name)
        redis_client.hset(name, num, json.dumps(raw_data))
        return jsonify({
            "status": "success",
            "message": "模板已成功保存",
        }), 200
    except Exception as e:
        # app.logger.error(f"保存场景失败: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"保存场景失败: {str(e)}",
            "received_at": datetime.utcnow().isoformat() if 'received_at' not in locals() else "received_at"
        }), 500


@api_show.route('/scenarios', methods=['GET'])
def get_all_scene_name():
    keys = redis_client.keys('scene_template:*')
    pattern = re.compile(rb':(.*)')
    extracted_data = []
    for item in keys:
        match = pattern.search(item)
        if match:
            extracted_data.append(match.group(1).decode('utf-8'))  # 解码为字符串
    print(extracted_data)
    return jsonify(extracted_data)


@api_show.route('/scenarios/<scenario>', methods=['GET'])
def get_scenarios(scenario):
    try:
        data = redis_client.hget(f"scene_template:{scenario}", '0')
        return jsonify(json.loads(data.decode('utf-8'))), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


def is_file_completed(filename):
    """
    检查 Redis 中是否存在指定的 filename 键，并获取其值
    """
    # 判断键是否存在
    filename = f"{filename}-0"
    if redis_client.exists(filename):
        value = redis_client.get(filename)
        json_value = json.loads(value) if value else None
        return json_value['status']
    else:
        print(f"键 '{filename}' 不存在")
        return None


@api_show.route('/metric', methods=['post', 'GET'])
def get_metric():
    query = 'MATCH p=()-[r:has]->() RETURN p'
    res = get_metric_result(query)
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


@api_show.route('/table/sysinfo', methods=['post', 'GET'])
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
@api_show.route('/datainfo', methods=["POST", "GET"])
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
@api_show.route('/workerinfo', methods=["POST", "GET"])
def get_worker_info():
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT, TASKS_ZSET_KEY
    r = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                          db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 从 Redis 获取以 "-0" 结尾的键值对
    # 获取最近创建的10个任务的worker_id
    zset_key = TASKS_ZSET_KEY  # 这是 任务时间排序集合zset 的 key
    latest_task_ids = r.zrevrange(zset_key, 0, 9)  # 获取分数最大的前10个

    if not latest_task_ids:
        return jsonify({"error": "No tasks found in Redis"}), 404

    # 构造响应数据
    tasks_data = []

    for worker_id in latest_task_ids:
        w = worker_id.decode('utf-8') + '-0'
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
            # todo 根据任务时长，判断是否任务失败
            tasks_data.append(task_data)

    # 返回所有任务数据
    return jsonify(tasks_data)

#   api返回所有任务相关信息
@api_show.route('/allinfo', methods=["POST", "GET"])
def get_all_info():
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT, TASKS_ZSET_KEY
    r = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                          db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    # 从 Redis 获取以 "-0" 结尾的键值对
    # 获取最近创建的10个任务的worker_id
    zset_key = TASKS_ZSET_KEY  # 这是 任务时间排序集合zset 的 key
    latest_task_ids = r.zrevrange(zset_key, 0, -1)

    if not latest_task_ids:
        return jsonify({"error": "No tasks found in Redis"}), 404

    # 构造响应数据
    tasks_data = []

    for worker_id in latest_task_ids:
        w = worker_id.decode('utf-8') + '-0'
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
            # todo 根据任务时长，判断是否任务失败
            tasks_data.append(task_data)

    # 返回所有任务数据
    return jsonify(tasks_data)

# API 返回任务报告
@api_show.route('/report_info', methods=["POST", "GET"])
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
        print("Fetched result")
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
        print("Fetched result")
        try:
            report = fillter(replace_keys(results), worker_id)
        except Exception as e:
            print("Exception caught:", e)
        return report, 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_show.route('/generate_json', methods=["POST"])
def generate_json():
    """
    根据主线程任务ID获取各个子线程的任务结果并生成JSON报告。
    """
    data = request.json
    worker_id = data.get('Worker_Id')

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
        return jsonify({'error': f"Redis连接失败: {str(e)}"}), 500
    except Exception as e:
        # 处理其他异常
        print("An error occurred:", e)
        return jsonify({'error': f"发生异常: {str(e)}"}), 500

    # 连接建立后，根据workerid取数据
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

        report_data = fillter_pdf(replace_keys(results), worker_id)

        # 返回JSON格式的报告数据
        response = make_response(json.dumps(report_data, ensure_ascii=False, indent=2))
        response.headers['Content-Type'] = 'application/json'
        response.headers['Content-Disposition'] = f'attachment; filename={worker_id}_report.json'
        return response

    except Exception as e:
        print("Exception caught:", e)
        return jsonify({'error': str(e)}), 500


# API 返回任务报告pdf
@api_show.route('/generate_pdf', methods=["POST"])
def generate_pdf():
    """
    根据主线程任务ID获取各个子线程的任务结果并生成PDF报告。
    """
    data = request.json
    worker_id = data.get('Worker_Id')
    import redis
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    try:
        # 建立到 Redis 的连接
        redis_client = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                                         db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
        # 检查连接是否成功
        redis_client.ping()
        logger.info("Connected to Redis server.")
    except redis.ConnectionError as e:
        # 处理连接错误
        logger.info("Failed to connect to Redis server:", e)
    except Exception as e:
        # 处理其他异常
        logger.info("An error occurred:", e)

    # 连接建立后，根据workerid取数据
    try:
        # 使用主线程ID作为前缀从Redis中获取任务结果
        keys = redis_client.keys(f'{worker_id}-*')
        # 存储结果的字典
        results = {}
        # 遍历结果并获取任务结果
        logger.info("Fetched result")
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
            print("Fetched result")
        print("Fetched result")
        report_data = fillter_pdf(replace_keys(results), worker_id)
        pdf_buffer = create_pdf_report(report_data, worker_id)

        # 获取PDF数据并创建响应
        pdf_data = pdf_buffer.getvalue()
        pdf_buffer.close()

        # 设置响应头
        response = make_response(pdf_data)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'attachment; filename={worker_id}_report.pdf'

        return response
    except Exception as e:
        print("Exception caught:", e)
        return jsonify({'error': str(e)}), 500


def create_pdf_report(report_data, worker_id):
    pdf_buffer = BytesIO()

    # 准备中文字体
    current = Path(__file__).resolve()
    great_parent = current.parent.parent.parent
    font_path_1 = great_parent / 'static/font/SimSun.ttf'
    font_path_2 = great_parent / 'static/font/SimSun-Bold.ttf'

    # 注册中文字体
    pdfmetrics.registerFont(TTFont('SimSun', str(font_path_1)))
    pdfmetrics.registerFont(TTFont('SimSun-Bold', str(font_path_2)))

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='ReportTitle', fontName='SimSun-Bold', fontSize=16,
                              alignment=TA_CENTER, spaceAfter=12))
    styles.add(ParagraphStyle(name='Level1Title', fontName='SimSun-Bold', fontSize=14, alignment=TA_CENTER, spaceBefore=20,
                              spaceAfter=8))
    styles.add(ParagraphStyle(name='Level2Title', fontName='SimSun-Bold', fontSize=12, alignment=TA_LEFT, spaceBefore=18,
                              spaceAfter=4))
    styles.add(ParagraphStyle(name='Level3Title', fontName='SimSun-Bold', fontSize=10, alignment=TA_LEFT, spaceBefore=6,
                              spaceAfter=3))
    styles.add(ParagraphStyle(name='Level4Title', fontName='SimSun-Bold', fontSize=10, alignment=TA_LEFT, spaceBefore=6,
                              spaceAfter=3))
    styles.add(ParagraphStyle(name='Content', fontName='SimSun', fontSize=10,
                              leading=14, spaceAfter=4, wordWrap='CJK'))
    styles.add(ParagraphStyle(name='BulletList', fontName='SimSun', fontSize=10,
                              spaceBefore=4, spaceAfter=4))
    styles.add(ParagraphStyle(name='Footer', fontName='SimSun', fontSize=9,
                              alignment=TA_CENTER, textColor=colors.grey))
    styles.add(ParagraphStyle(name='PageNumber', fontName='SimSun', fontSize=9,
                              alignment=TA_CENTER, textColor=colors.grey))

    # 定义处理嵌套指标的函数
    def process_metric(story, metric_data, styles, level=0):
        max_level = 4  # 最多支持四级标题

        for key, value in metric_data.items():
            # 根据层级选择标题样式
            title_style = f'Level{level + 1}Title' if level + 1 <= max_level else 'Content'
            story.append(Paragraph(key, styles[title_style]))


            # 处理值的内容
            if isinstance(value, builtins.dict):
                # 嵌套字典：递归处理，层级 +1
                process_metric(story, value, styles, level + 1)

            elif isinstance(value, list):
                # 列表：添加项目符号列表
                for item in value:
                    story.append(Paragraph(f"• {item}", styles['BulletList']))

            else:
                # 基本类型（字符串、数字等）：直接显示值
                story.append(Paragraph(str(value), styles['Content']))

    # 添加页码功能
    def add_page_number(canvas, doc):
        page_num = canvas.getPageNumber()
        if page_num > 1:
            text = f"第 {page_num - 1} 页"
            canvas.saveState()
            canvas.setFont('SimSun', 9)
            canvas.setFillColor(colors.grey)
            canvas.drawCentredString(doc.pagesize[0] / 2, 20, text)
            canvas.restoreState()

    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=40,
                            title=f'隐私评估报告-{worker_id}',
                            onFirstPage=add_page_number,
                            onLaterPages=add_page_number)

    # 转换单位
    inch = 72

    # 构建PDF内容
    story = []

    # === 封面页 ===
    story.append(Spacer(1, 2 * inch))
    story.append(Paragraph("隐私评估报告", styles['ReportTitle']))
    story.append(Spacer(1, 0.5 * inch))
    story.append(Paragraph(f"评估任务ID: {worker_id}",
                           ParagraphStyle(name='TaskID', fontName='SimSun-Bold', fontSize=12,
                                          alignment=TA_CENTER, spaceAfter=0.5 * inch)))
    story.append(Spacer(1, 5 * inch))
    date_paragraph = Paragraph(datetime.now().strftime("%Y年%m月%d日"),
                               ParagraphStyle(name='CoverDate', fontName='SimSun', fontSize=10,
                                              alignment=TA_CENTER))
    story.append(date_paragraph)
    story.append(PageBreak())

    # === 报告摘要 ===
    report_content = report_data.get("隐私评估报告", {})
    summary = report_content.get("报告摘要", {})

    story.append(Paragraph("报告摘要", styles['Level1Title']))
    story.append(Spacer(1, 0.1 * inch))


    # 评估目的
    if "评估目的" in summary:
        story.append(Paragraph("评估目的", styles['Level2Title']))
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph(summary["评估目的"], styles['Content']))
        story.append(Spacer(1, 0.2 * inch))

    # 评估标准
    if "评估标准" in summary:
        story.append(Paragraph("评估标准", styles['Level2Title']))
        story.append(Spacer(1, 0.1 * inch))
        standards = summary["评估标准"]
        if isinstance(standards, list):
            for standard in standards:
                story.append(Paragraph(f"• {standard}", styles['BulletList']))
        else:
            story.append(Paragraph(standards, styles['Content']))
        story.append(Spacer(1, 0.2 * inch))

    # 总体评估得分
    if "总体评估得分" in summary:
        story.append(Paragraph("总体评估得分", styles['Level2Title']))
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph(f"{summary['总体评估得分']}", styles['Content']))
        story.append(Spacer(1, 0.3 * inch))

    story.append(PageBreak())

    # === 主要评估部分 ===
    sections = [
        ("隐私风险度量", "隐私风险度量"),
        ("可用性度量", "可用性度量"),
        ("隐私保护性度量", "隐私保护性度量"),
        ("数据合规性", "数据合规性"),
        ("数据特征", "数据特征")
    ]

    for section_key, section_title in sections:
        if section_key in report_content:
            story.append(Paragraph(section_title, styles['Level1Title']))
            story.append(Spacer(1, 0.1 * inch))
            section_data = report_content[section_key]
            process_metric(story, section_data, styles, level=1)  # 初始层级为1
            story.append(PageBreak())

    # === 页脚 ===
    story.append(Spacer(1, 0.5 * inch))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey))
    story.append(Paragraph(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Footer']))
    story.append(Paragraph("本报告由隐私评估系统自动生成", styles['Footer']))

    # 构建PDF文档
    doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
    return pdf_buffer


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
            worker_id = key - '-0'
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


def generate_privacy_report(worker_id, report):
    # 本函数内 worker_id会加'-0'
    # 初始化 Redis 连接
    worker_id = worker_id + '-0'
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
        data = data["evaluate"]
        # 将 Redis 数据写入 report 的对应位置
        report["隐私评估报告"]["数据特征"]["评估结果"] = data["数据特征结果"]["评估结果"]
        report["隐私评估报告"]["隐私风险度量"]["评估结果"] = data["隐私风险度量"]["评估结果"]
        # report["隐私评估报告"]["可用性评估"]["评估结果"] = data["可用性结果"]["评估结果"]
        report["隐私评估报告"]["隐私保护性度量"]["评估结果"] = data["安全性结果"]["评估结果"]
        report["隐私评估报告"]["数据合规性"]["评估结果"] = data["合规性结果"]["评估结果"]

        report["隐私评估报告"]["报告摘要"]["总体评估得分"] = data["评估得分"]["rank"]

        return report
    except Exception as e:
        print(f"Error fetching or parsing data from Redis: {e}")
        return None


def fillter(data, worker_id):
    report = {
        "隐私评估报告": {
            "评估任务ID": f"{worker_id}",
            "报告摘要": {
                "总体评估得分": {},
                "目的": "隐私保护效果评估指标体系是评估隐私保护措施的有效性的重要工具。一方面，建立隐私保护效果评估指标体系，可以支撑隐私保护策略的多维度量化与自动化筛选。另一方面，针对海量的大数据隐私信息以及时空场景关联的特点，数据脱敏不仅要确保敏感信息被去除，还需充分考虑脱敏花费、实际业务需求等因素。数据保护与数据挖掘是一对矛盾体，既要使数据潜在价值得到充分应用，又要确保敏感信息不被泄露。因此，在进行数据脱敏时，应依据场景需要明确脱敏数据范围、脱敏需求以及脱敏后的数据用途。",
                "评价标准": {
                    "隐私风险度量": "隐私风险度量是指在数据处理过程中，数据在不被授权访问或非法推理中可能暴露敏感信息的风险。用于识别隐私泄露的潜在路径与攻击方式，并衡量数据暴露程度，以便采取相应的保护措施。",

                    "可用性度量": "数据可用性是指原始数据在经过隐私保护处理后所保留的信息是否仍可满足业务功能或分析需求。用于衡量隐私保护处理带来的数据偏差大小，偏差越小，则说明可用性越高；偏差越大，说明处理影响越大，可用性越低。",

                    "隐私保护性度量": "隐私保护性度量是指隐私保护算法执行后，隐私信息被还原的难易程度，攻击者或第三方通过观测到的部分信息来推断隐私信息的能力越强，说明安全性越差；反之，说明保护效果越好。",

                    "数据合规性": "数据合规性是指经过隐私保护处理后的数据是否满足相关法律法规、行业标准和组织内部规范的要求。隐私保护措施是否能够支撑对数据最基本的合规性需求，包括数据处理原则、访问控制、安全机制等内容。",

                    "数据特征": "数据特征是指数据本身所具有的属性，以及其在应用中的完整性、准确性、一致性和稳定性。评估隐私保护措施对数据的基础属性（如类型、分布、完整性）是否造成显著影响，进而影响其可分析性和可信度，从而反映数据整体质量水平。"
                }
            },
            "隐私风险度量": {
                "评估结果": {},
            },
            "可用性评估": {
                "评估结果": {},
            },
            "隐私保护性度量": {
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
        print("风险评估结果...")
        item = data["风险评估结果"]
        reid_rate = safe_to_float(item.get("重识别率"))
        infer_rate = safe_to_float(item.get("统计推断还原率"))
        cf = safe_to_float(item.get("差分攻击还原率"))
        report["隐私评估报告"]["隐私风险度量"] = {
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
            "评估结果得分": {
                "重识别率": f"{reid_rate:.2f}",
                "统计推断还原率": f"{infer_rate:.2f}",
                "差分攻击还原率": f"{cf:.2f}"
            },
            "改进建议": [
                "提高匿名化数据的保护水平，减少重识别成功的可能性。",
                "针对属性关联较强的数据集，进行更严格的去标识化处理。",
                "通过差分隐私方法增加噪声，降低统计推断攻击的有效性。",
                "实施分级管理，对数据集的发布进行严格审批，以降低多表链接攻击的可能性。"
            ]
        }

    # 可用性评估填充
    if "可用性评估结果" in data:
        print("可用性评估结果...")
        item = data["可用性评估结果"]
        results = item.get("results", {})
        norm = item.get("norm", {})
        data_distinct = safe_to_float(item.get("数据可辨别度"))
        norm_distinct = safe_to_float(norm.get("数据可辨别度"))
        res_distinct = safe_to_float(results.get("数据可辨别度"), data_distinct)
        report["隐私评估报告"]["可用性评估"] = {
            "数据可辨别度": {
                "指标摘要": f"数据可辨别度衡量经过隐私保护后数据记录之间的区分能力，本次评估数据可辨别度为 {data_distinct:.2f}（归一化分数：{norm_distinct:.2f}）。",
                "定义": "数据可辨别度反映数据记录在隐私保护下依然被区分的能力，数值越高表明信息保留越充分。",
                "结果分析": {
                    "分析": (
                            f"可辨别度原始值为 {results.get('数据可辨别度', item['数据可辨别度'])}，"
                            f"归一化分数为 {norm.get('数据可辨别度', 'N/A')}。"
                            + (
                                "说明数据区分性较好，隐私保护对实用性影响有限。"
                                if norm.get('数据可辨别度', 0) > 0.8 else
                                "说明数据区分性有一定下降，需评估是否满足业务需求。"
                            )
                    ),
                    "改进建议": [
                        "根据业务需求调整隐私保护算法，确保关键记录可辨别性。",
                        "监控该指标在持续保护过程中的变化，动态优化策略。"
                    ]
                }
            },
            "数据记录匿名率": {
                "指标摘要": f"数据记录匿名率为 {item['数据记录匿名率']}（归一化分数：{norm.get('数据记录匿名率', 'N/A')}），反映被隐匿或删除记录的比例。",
                "定义": "数据记录匿名率是被隐匿或删除的记录数量占总记录数量的比例，反映信息缺失情况。",
                "结果分析": {
                    "分析": (
                            f"匿名率为 {item['数据记录匿名率']}，"
                            f"原始值为 {results.get('数据记录匿名率', 'N/A')}，"
                            f"归一化分数为 {norm.get('数据记录匿名率', 'N/A')}。"
                            + (
                                "说明数据完整性很高，无信息缺失。"
                                if str(item['数据记录匿名率']) in ['0', '0.0%'] else
                                "说明有部分记录被隐匿，需关注信息丢失。"
                            )
                    ),
                    "改进建议": [
                        "优先采用低损失的隐私保护方式，减少删除或隐匿记录的比例。"
                    ]
                }
            },
            "平均泛化程度": {
                "指标摘要": f"平均泛化程度为 {item['平均泛化程度']}（归一化分数：{norm.get('平均泛化程度', 'N/A')}），即平均每个等价组包含的记录数。",
                "定义": "平均泛化程度反映数据聚合程度，数值越大，数据被泛化越充分，隐私性越高但精度降低。",
                "结果分析": {
                    "分析": (
                            f"平均泛化程度为 {results.get('平均泛化程度', item['平均泛化程度'])}，"
                            f"归一化分数为 {norm.get('平均泛化程度', 'N/A')}。"
                            + (
                                "较高的泛化程度有利于提升隐私保护水平，但可能影响数据分析精度。"
                                if norm.get('平均泛化程度', 0) > 0.8 else
                                "较低的泛化程度说明信息较细致，需关注隐私风险。"
                            )
                    ),
                    "改进建议": [
                        "根据隐私保护和业务需求动态调整泛化粒度。",
                        "探索多种泛化与聚合策略，平衡数据实用性与安全性。"
                    ]
                }
            },
            "数据损失度": {
                "指标摘要": f"数据损失度为 {item['数据损失度']}（归一化分数：{norm.get('数据损失度', 'N/A')}），表示整体信息丢失比例。",
                "定义": "数据损失度衡量数据在隐私保护后的总体信息损失，数值越大，损失越多。",
                "结果分析": {
                    "分析": (
                            f"损失度原始值为 {results.get('数据损失度', item['数据损失度'])}，"
                            f"归一化分数为 {norm.get('数据损失度', 'N/A')}。"
                            + (
                                "信息损失控制较好，数据可用性较高。"
                                if norm.get('数据损失度', 1) < 0.5 else
                                "需进一步关注信息损失，优化数据保护方式。"
                            )
                    ),
                    "改进建议": [
                        "细化隐私保护参数，权衡损失与隐私水平。"
                    ]
                }
            },
            "基于熵的平均数据损失度": {
                "指标摘要": f"基于熵的平均数据损失度为 {item['基于熵的平均数据损失度']}（原始值：{results.get('基于熵的平均数据损失度', 'N/A')}，归一化分数：{norm.get('基于熵的平均数据损失度', 'N/A')}）。",
                "定义": "通过熵变化反映数据不确定性变化，熵值损失越大，隐私提升，但数据精度降低。",
                "结果分析": {
                    "分析": (
                            f"平均熵损失度为 {results.get('基于熵的平均数据损失度', 'N/A')}，"
                            f"归一化分数为 {norm.get('基于熵的平均数据损失度', 'N/A')}。"
                            + (
                                "熵损失度偏低，数据实用性较强。"
                                if norm.get('基于熵的平均数据损失度', 1) < 0.5 else
                                "熵损失度较高，建议关注数据分析需求的影响。"
                            )
                    ),
                    "改进建议": [
                        "优化泛化与扰动策略，降低熵损失。",
                        "结合业务场景动态调整熵相关参数。"
                    ]
                }
            },
            "唯一记录占比": {
                "指标摘要": f"唯一记录占比为 {item['唯一记录占比']}（归一化分数：{norm.get('唯一记录占比', 'N/A')}），反映可唯一识别记录的比例。",
                "定义": "唯一记录占比表示数据集中可被唯一标识的记录比例，值越高隐私风险越大。",
                "结果分析": {
                    "分析": (
                            f"唯一记录占比为 {item['唯一记录占比']}，"
                            f"原始值为 {results.get('唯一记录占比', 'N/A')}，"
                            f"归一化分数为 {norm.get('唯一记录占比', 'N/A')}。"
                            + (
                                "无唯一记录，隐私保护非常充分。"
                                if str(item['唯一记录占比']) in ['0', '0.0%'] else
                                "存在唯一记录，建议加强隐私保护。"
                            )
                    ),
                    "改进建议": [
                        "进一步聚合处理敏感数据，避免唯一标识。",
                        "关注唯一记录对高风险场景的影响。"
                    ]
                }
            },
            "可用性综合得分": {
                "指标得分": item['rank']['可用性综合结果'],
                "结论": (
                    f"本次可用性评估综合得分为 {item['rank']['可用性综合结果']}，代表整体数据实用性水平。"
                    "分数越高，该数据在隐私保护下可用性较强。"
                    if item['rank']['可用性综合结果'] > 0.7 else
                    "分数偏低，保护措施影响了数据的实用性，建议结合实际需求权衡隐私与可用性。"
                )
            }
        }

    # 合规性性评估填充
    if "合规性评估结果" in data:
        print("合规性评估结果...")
        item = data["合规性评估结果"]

        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)
            return numbers[-1] if numbers else None

        def extract_attribute_name(string):
            match = re.search(r'隐私属性(.+?)(不)?满足', string)
            return match.group(1) if match else None

        K_value = extract_last_number(str(item.get('K-Anonymity', '')))
        L_string = item.get('L-Diversity', [''])[0]
        L_value = extract_last_number(L_string)
        L_attr = extract_attribute_name(L_string)
        T_string = item.get('T-Closeness', [''])[0]
        T_value = extract_last_number(T_string)
        T_attr = extract_attribute_name(T_string)
        AK_string = item.get('(α,k)-Anonymity', [''])[0]
        AK_tuple = re.findall(r'(\d*\.?\d+)', AK_string)
        if len(AK_tuple) >= 2:
            AK_alpha, AK_k = AK_tuple[:2]
        else:
            AK_alpha, AK_k = None, None
        AK_attr = extract_attribute_name(AK_string)
        AK_satisfaction = "满足" if "满足" in AK_string else "不满足"

        # 组合隐私属性列表
        sa_columns = []
        for s in [L_attr, T_attr, AK_attr]:
            if s and s not in sa_columns:
                sa_columns.append(s)
        sa_list = ", ".join(sa_columns) if sa_columns else "敏感属性"

        # 综述描述
        desensitization_strategy = (
            "本次数据脱敏采用了 K-匿名度、L-多样性、T-相近性和 (α,k)-匿名度等综合隐私保护技术，"
            "通过分组、泛化、频率约束等方式提升数据安全性。"
            "合规性评估表明数据处理已达到相关法规如GDPR、《个人信息保护法》对匿名化的要求，"
            "有力保障了数据在共享与分析过程中的隐私安全。"
        )
        norm_score = item.get('norm', {}).get('合规性结果', 0)
        if norm_score < 0.6:
            compliance_level = "较低"
        elif norm_score < 0.8:
            compliance_level = "中等"
        elif norm_score < 0.95:
            compliance_level = "良好"
        else:
            compliance_level = "优秀"
        report["隐私评估报告"]["数据合规性"] = {
            "合规性检测得分": f"{norm_score:.3f}",
            "技术描述": desensitization_strategy,
            "合规性等级": compliance_level,
            "K-匿名度": {
                "指标摘要": f"{item['K-Anonymity']}",
                "定义": "K-匿名度要求每个等价类中至少有K条记录，降低单条记录被重识别风险。",
                "结果分析": (
                    f"当前K-匿名度为{K_value}，满足匿名化标准。所有等价类均包含不少于{K_value}条记录，"
                    "数据匿名性较好，能够有效防止针对个体的直接重识别。"
                ),
                "改进建议": [
                    f"如需进一步提升隐私保护，可考虑提高K值（如{int(K_value) + 1}-匿名）。",
                    "持续优化等价类划分算法，保持等价类数量均衡，进一步降低风险。"
                ],
                "合规性要求": "满足GDPR第5条数据最小化原则与国内法规对数据匿名化的要求。"
            },
            "L-多样性": {
                "指标摘要": f"隐私属性{L_attr or sa_list}评估结果：{L_string}",
                "定义": "L-多样性要求每个等价类中敏感属性的取值不少于L种，以防止敏感信息被集中推断。",
                "结果分析": (
                    f"当前属性{L_attr or sa_list}的等价类均含有至少{L_value}种敏感取值，显著提升了数据集对多样化攻击的抵抗能力。"
                ),
                "改进建议": [
                    f"如对隐私攻击风险敏感，可考虑提升L值（如{int(L_value) + 1}-多样性）。",
                    "加强等价类敏感属性分布的均衡，防止某些值过于集中。"
                ],
                "合规性要求": "符合GDPR及《个人信息保护法》中对敏感数据多样性的保护要求。"
            },
            "(α,k)-匿名度": {
                "指标摘要": f"隐私属性{AK_attr or sa_list}评估结果：{AK_string}，"
                            f"（α={AK_alpha}, k={AK_k}），{AK_satisfaction}。",
                "定义": "(α,k)-匿名度要求等价类中敏感属性的频率不超过α，同时等价类中记录不少于k条，从而抑制频率攻击。",
                "结果分析": (
                    f"当前属性{AK_attr or sa_list}在各等价类中敏感属性最大频率不超过α={AK_alpha}，"
                    f"且每组不少于k={AK_k}条，综合满足(α,k)-匿名度。"
                ),
                "改进建议": [
                    "可适当降低α、提升k，强化匿名性。",
                    "对频率分布进行动态监控，防止敏感值聚集。"
                ],
                "合规性要求": "符合国际及国内关于敏感属性频率限制的隐私保护法规要求。"
            },
            "T-相近性": {
                "指标摘要": f"隐私属性{T_attr or sa_list}评估结果：{T_string}，满足T={T_value}-紧密性。",
                "定义": "T-相近性要求等价类中敏感属性的分布与全局分布的距离不超过T，降低推断攻击风险。",
                "结果分析": (
                    f"{T_string}。等价类中敏感属性的分布与整体分布的差异在T={T_value}阈值内，有效防止属性推断攻击。"
                ),
                "改进建议": [
                    f"根据隐私威胁模型，可尝试收紧T值（如T={float(T_value) * 0.8:.2f}），提升防护强度。",
                    "持续关注等价类的分布变化，动态调整保护参数。"
                ],
                "合规性要求": "满足GDPR及相关数据保护法中对敏感属性分布的限制标准。"
            }
        }

    # 数据特征填充
    if "匿名集数据特征评估结果" in data:
        print("匿名集数据特征评估结果...")
        item = data["匿名集数据特征评估结果"]
        quasi_identifier_dimension = item.get("准标识符维数", "未知")
        sensitive_attribute_dimension = item.get("敏感属性维数", "未知")
        inherent_privacy = item.get("固有隐私", "未知")
        sensitive_attrs = item.get("敏感属性种类", [])
        sensitive_attrs_str = ", ".join(sensitive_attrs) if sensitive_attrs else "未知"
        average_degree_of_generalization = item.get("平均泛化程度", "未知")
        norm_score = item.get("norm", {}).get("数据特征结果", None)

        # 描述性标签（可选，便于后续自动分级）
        if norm_score is not None:
            if norm_score < 0.6:
                feature_level = "较低"
            elif norm_score < 0.8:
                feature_level = "中等"
            elif norm_score < 0.95:
                feature_level = "良好"
            else:
                feature_level = "优秀"
        else:
            feature_level = "未知"

        report["隐私评估报告"]["数据特征"] = {
            "指标说明": f"本部分为描述性指标，主要展现数据本身结构及基本属性，帮助理解数据集的整体特征和隐私保护的基础环境。",
            "特征评价等级": feature_level,
            "准标识符维数": {
                "报告摘要": f"数据集的准标识符维数为 {quasi_identifier_dimension}，即有 {quasi_identifier_dimension} 个字段（如性别、年龄段等）能联合用于间接标识个体。",
                "定义": "准标识符是指单独或联合使用时能唯一标识用户身份的字段。维数越高，数据被间接识别的风险越高。",
                "结果分析": f"当前准标识符维数为 {quasi_identifier_dimension}，建议结合具体业务场景持续关注相关字段在不同匿名策略下的表现。"
            },
            "敏感属性维数": {
                "报告摘要": f"敏感属性维数为 {sensitive_attribute_dimension}，即有 {sensitive_attribute_dimension} 个需要重点隐私保护的敏感字段。",
                "定义": "敏感属性一般指包含个人隐私、财务、健康等信息的字段。敏感属性维数越多，保护难度和合规压力也会增加。",
                "结果分析": f"本数据集主要敏感属性包括：{sensitive_attrs_str}，需重点防护。"
            },
            "敏感属性种类": {
                "报告摘要": f"本数据集包含的敏感属性有：{sensitive_attrs_str}。",
                "定义": "敏感属性是指数据中需要重点保护、易导致隐私泄露的字段类型。",
                "结果分析": f"这些字段在数据共享、分析及脱敏过程中需优先考虑保护方案。"
            },
            "平均泛化程度": {
                "报告摘要": f"数据集的平均泛化程度为 {average_degree_of_generalization}。此值反映每个等价组中平均记录数量，数值较大表明分组更粗，有利于隐私但可能影响精度。",
                "定义": "平均泛化程度=总记录数/等价组数。该指标描述等价组划分的均匀程度，是K匿名等机制的基础描述。",
                "结果分析": f"本次评估中，平均泛化程度为 {average_degree_of_generalization}，表明当前等价组划分粒度较粗。"
            },
            "固有隐私": {
                "报告摘要": f"数据集的固有隐私为 {inherent_privacy}。该指标表征在无任何外部辅助信息下，攻击者需要多少二元提问才能确定敏感值。",
                "定义": "固有隐私可视为数据的‘理论安全下限’，与数据的熵有关。",
                "结果分析": f"固有隐私值为 {inherent_privacy}，意味着理论上有较强的隐私保护能力，但实际效果还需结合数据发布方式综合评估。"
            },
            "综合特征分析": (
                f"从各项描述性数据特征来看，当前数据集结构清晰、敏感属性和准标识符数量适中，"
                f"平均泛化程度和固有隐私等指标也处于{feature_level}水平。"
                "建议在数据分析和开放前，持续关注数据特征指标的变化，作为调整隐私保护策略的重要依据。"
            )
        }

    if "隐私保护性度量评估结果" in data:
        item = data["隐私保护性度量评估结果"]
        results = item.get("results", {})
        norm = item.get("norm", {})
        rank = item.get("rank", {})

        def safe_first(x):
            if isinstance(x, list) and x:
                return x[0]
            if isinstance(x, str):
                return x
            return ""

        # 归一化等级
        security_score = rank.get("安全性综合结果", 0)
        if security_score >= 0.95:
            security_level = "优秀"
        elif security_score >= 0.8:
            security_level = "良好"
        elif security_score >= 0.6:
            security_level = "中等"
        else:
            security_level = "较低"

        # 敏感属性重识别风险
        sensitive_attr_risk_list = []
        for attr in item.get("敏感属性的重识别风险", []):
            attribute_a = {}
            m1 = re.search(r'敏感属性([a-zA-Z0-9_]+)', attr)
            m2 = re.search(r'重识别风险为：([0-9\.%]+)', attr)
            m3 = re.search(r'等价组为(.+)', attr)
            attribute_a["敏感属性"] = m1.group(1) if m1 else "未知"
            attribute_a["重识别风险"] = m2.group(1) if m2 else "未知"
            attribute_a["等价组"] = m3.group(1) if m3 else "未知"
            sensitive_attr_risk_list.append(attribute_a)

        
        # 单个属性重识别风险
        single_attr_risk_list = []
        for attr in item.get("单个属性的重识别风险", []):
            attribute_a = {}
            m1 = re.search(r'标识符属性([a-zA-Z0-9_]+)', attr)
            m2 = re.search(r'重识别风险为：([0-9\.%]+)', attr)
            m3 = re.search(r'对应属性值为(.+)', attr)
            attribute_a["标识符属性"] = m1.group(1) if m1 else "未知"
            attribute_a["重识别风险"] = m2.group(1) if m2 else "未知"
            attribute_a["属性值"] = m3.group(1) if m3 else "未知"
            single_attr_risk_list.append(attribute_a)
        # 敏感属性汇总
        sensitive_attr_summary = "，".join([
            f"{attr['敏感属性']}的重识别风险为{attr['重识别风险']}，等价组为{attr['等价组']}"
            for attr in sensitive_attr_risk_list
        ])
        sensitive_attr_analysis = ""
        sensitive_attr_conclusion = ""
        for attr in sensitive_attr_risk_list:
            risk = float(attr["重识别风险"].replace('%', '')) if '%' in attr["重识别风险"] else float(
                attr["重识别风险"])
            if risk > 1.0:  # 可调阈值
                sensitive_attr_analysis += f"{attr['敏感属性']}在等价组{attr['等价组']}的重识别风险较高（{attr['重识别风险']}）。"
                sensitive_attr_conclusion += f"{attr['敏感属性']}为高风险敏感属性，"
            else:
                sensitive_attr_analysis += f"{attr['敏感属性']}在等价组{attr['等价组']}的重识别风险较低（{attr['重识别风险']}）。"
                sensitive_attr_conclusion += f"{attr['敏感属性']}为低风险敏感属性，"

        # 单个属性汇总
        single_attr_summary = "，".join([
            f"{attr['标识符属性']}为{attr['重识别风险']}"
            for attr in single_attr_risk_list
        ])
        single_attr_analysis = ""
        single_attr_conclusion = ""
        for attr in single_attr_risk_list:
            risk = float(attr["重识别风险"].replace('%', ''))
            if risk > 0.3:
                single_attr_analysis += f"{attr['标识符属性']}的重识别风险较高，为：{attr['重识别风险']}。"
                single_attr_conclusion += f"{attr['标识符属性']}具有较高的重识别风险，"
            else:
                single_attr_analysis += f"{attr['标识符属性']}的重识别风险较低，为：{attr['重识别风险']}。"
                single_attr_conclusion += f"{attr['标识符属性']}具有较低的重识别风险，"

        # 整体重识别风险
        print("隐私保护性度量评估结果6...")
        overall_reid_text = item.get("基于熵的重识别风险", "")
        m1 = re.search(r'重识别风险为：([0-9\.%]+)', overall_reid_text)
        m2 = re.search(r'等价组为(.+)', overall_reid_text)
        overall_risk_val = m1.group(1) if m1 else "未知"
        overall_equiv_group = m2.group(1) if m2 else "未知"

        report["隐私评估报告"]["隐私保护性度量"] = {
            "安全性综述": {
                "指标得分": f"{security_score:.4f}",
                "安全等级": security_level,
                "指标摘要": (
                    f"本节综合评估了数据集的分布泄露、熵泄露、隐私增益、KL散度、敏感属性和单个属性的重识别风险，以及整体重识别风险等多项隐私保护性度量。"
                )
            },
            "分布泄露": {
                "报告摘要": safe_first(item.get("分布泄露")),
                "定义": "分布泄露衡量敏感属性在等价组与全局分布的差异，分布差异小则隐私保护效果更好。",
                "数值": f"{results.get('分布泄露', ['未知'])[0]:.5f}",
                "归一化分数": norm.get('分布泄露', "未知"),
                "分析": (
                    f"{safe_first(item.get('分布泄露'))} 分布泄露为{results.get('分布泄露', ['未知'])[0]:.5f}，归一化分数为{norm.get('分布泄露', '未知')}。"
                    "得分接近1表示泄露风险低。"
                ),
                "改进建议": [
                    "进一步优化等价组的划分，缩小敏感属性在各组与全局分布的差异。",
                    "关注分布异常的敏感属性，加强针对性脱敏处理。",
                    "定期评估数据发布过程中的分布变化，动态调整保护策略。"
                ]
            },
            "熵泄露": {
                "报告摘要": safe_first(item.get("熵泄露")),
                "定义": "熵泄露度量脱敏后敏感属性的不确定性变化。熵泄露低表示攻击者推断敏感信息的难度大。",
                "数值": f"{results.get('熵泄露', ['未知'])[0]:.5f}",
                "归一化分数": norm.get('熵泄露', "未知"),
                "分析": (
                    f"{safe_first(item.get('熵泄露'))} 熵泄露为{results.get('熵泄露', ['未知'])[0]:.5f}，归一化分数为{norm.get('熵泄露', '未知')}。"
                ),
                "改进建议": [
                    "提升脱敏强度，增加数据的不确定性，降低熵泄露水平。",
                    "采用多样化的泛化和扰动方法，提升敏感属性的熵值。",
                    "关注熵泄露高的等价组，针对性增加保护措施。"
                ]
            },
            "隐私增益": {
                "报告摘要": safe_first(item.get("隐私增益")),
                "定义": "隐私增益反映脱敏处理提升的隐私安全性，数值越大表示越难推断敏感信息。",
                "数值": f"{results.get('隐私增益', ['未知'])[0]:.6f}",
                "归一化分数": norm.get('隐私增益', "未知"),
                "分析": (
                    f"{safe_first(item.get('隐私增益'))} 隐私增益为{results.get('隐私增益', ['未知'])[0]:.6f}，归一化分数为{norm.get('隐私增益', '未知')}。"
                ),
                "改进建议": [
                    "加强脱敏手段，提高敏感属性的隐私增益。",
                    "结合实际业务场景调整匿名化和扰动参数，提升保护强度。",
                    "监控隐私增益指标，及时优化脱敏策略。"
                ]
            },
            "KL散度": {
                "报告摘要": safe_first(item.get("KL_Divergence")),
                "定义": "KL散度度量等价组敏感属性分布与全局分布的差异，KL散度越小说明隐私保护越好。",
                "数值": f"{safe_to_float(results.get('KL_Divergence', [0])[0]):.6f}",  # 用safe_to_float转义，默认0
                "归一化分数": f"{safe_to_float(norm.get('KL_Divergence', 0)):.6f}" if norm.get('KL_Divergence') else "未知",  # 归一化分数也转float
                "分析": (
                    f"{safe_first(item.get('KL_Divergence'))} KL散度为{safe_to_float(results.get('KL_Divergence', [0])[0]):.6f}，"
                    f"归一化分数为{safe_to_float(norm.get('KL_Divergence', 0)):.6f}。"
                ),
                "改进建议": [
                    "优化等价组的敏感属性分布，减少与全局分布的差异。",
                    "采用动态分组策略，降低KL散度异常的风险。",
                    "对分布差异显著的等价组加强脱敏和泛化处理。"
                ]
            },
            "敏感属性的重识别风险": {
                "报告摘要": (
                    f"敏感属性重识别风险评估发现：{sensitive_attr_summary}"
                ),
                "定义": "评估敏感信息在特定等价组中被识别的风险。风险高说明隐私保护需加强。",
                "分析": sensitive_attr_analysis,
                "结论": sensitive_attr_conclusion,
                "改进建议": [
                    "对高风险敏感属性采用更严格的脱敏措施和分组策略。",
                    "持续跟踪敏感属性的风险变化，动态优化隐私保护机制。"
                ]
            },
            "单个属性的重识别风险": {
                "报告摘要": (
                    f"单个准标识符重识别风险评估发现：{single_attr_summary}"
                ),
                "定义": "评估单个准标识符被攻击时的风险高低，是寻找潜在薄弱环节的重要依据。",
                "分析": single_attr_analysis,
                "结论": single_attr_conclusion,
                "改进建议": [
                    "加强对高风险标识符属性的泛化与分组。",
                    "针对特定风险字段采用扰动或加密等方式提升安全性。"
                ]
            },
            "整体的重识别风险": {
                "报告摘要": (
                    f"整体敏感属性的重识别风险为{overall_risk_val}，主要风险等价组为{overall_equiv_group}。"
                ),
                "定义": "整体重识别风险是全局视角下所有敏感属性的最大被识别概率。",
                "分析": (
                    f"基于熵模型测算，数据集整体重识别风险为{overall_risk_val}，"
                    f"主要高风险集中于等价组{overall_equiv_group}。"
                ),
                "结论": (
                    f"当前整体重识别风险为{overall_risk_val}。建议重点关注高风险等价组，持续优化数据发布与脱敏策略。"
                ),
                "改进建议": [
                    "加强高风险等价组的保护措施，如更细分的分组、增强扰动。",
                    "结合业务场景灵活调整数据处理参数，降低整体风险水平。"
                ]
            }
        }
    # 返回有序的 JSON 响应
    print("报告生成完成。")
    report = generate_privacy_report(worker_id, report)
    return jsonify(report)

# 辅助函数：安全转换为浮点数（处理字符串、空值、百分比、非数字）
def safe_to_float(value, default=0.0):
    if not value:
        return default
    # 处理百分比字符串（如 "0.85%" -> 0.85）
    if isinstance(value, str):
        value = value.replace('%', '').strip()
        # 只提取字符串中的数字部分（避免非数字字符干扰）
        match = re.search(r'\d+\.?\d*', value)
        if match:
            value = match.group()
        else:
            return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def fillter_pdf(data, worker_id):
    report = {
        "隐私评估报告": {
            "评估任务ID": f"{worker_id}",
            "报告摘要": {
                "评估目的": "隐私保护效果评估指标体系是评估隐私保护措施的有效性的重要工具。一方面，建立隐私保护效果评估指标体系，可以支撑隐私保护策略的多维度量化与自动化筛选。另一方面，针对海量的大数据隐私信息以及时空场景关联的特点，数据脱敏不仅要确保敏感信息被去除，还需充分考虑脱敏花费、实际业务需求等因素。数据保护与数据挖掘是一对矛盾体，既要使数据潜在价值得到充分应用，又要确保敏感信息不被泄露。因此，在进行数据脱敏时，应依据场景需要明确脱敏数据范围、脱敏需求以及脱敏后的数据用途。",
                "评估标准": [
                    "隐私风险度量：隐私风险评估是指在数据处理过程中，数据在不被授权访问或非法推理中可能暴露敏感信息的风险。用于识别隐私泄露的潜在路径与攻击方式，并衡量数据暴露程度，以便采取相应的保护措施。",
                    "可用性度量：数据可用性是指原始数据在经过隐私保护处理后所保留的信息是否仍可满足业务功能或分析需求。用于衡量隐私保护处理带来的数据偏差大小，偏差越小，则说明可用性越高；偏差越大，说明处理影响越大，可用性越低。",
                    "隐私保护性度量：隐私保护性度量是指隐私保护算法执行后，隐私信息被还原的难易程度，攻击者或第三方通过观测到的部分信息来推断隐私信息的能力越强，说明安全性越差；反之，说明保护效果越好。",
                    "数据合规性：数据合规性是指经过隐私保护处理后的数据是否满足相关法律法规、行业标准和组织内部规范的要求。隐私保护措施是否能够支撑对数据最基本的合规性需求，包括数据处理原则、访问控制、安全机制等内容。",
                    "数据特征：数据特征是指数据本身所具有的属性，以及其在应用中的完整性、准确性、一致性和稳定性。评估隐私保护措施对数据的基础属性（如类型、分布、完整性）是否造成显著影响，进而影响其可分析性和可信度，从而反映数据整体质量水平。"
                ],
                "总体评估得分": {}
            },
            "隐私风险度量": {},
            "可用性度量": {},
            "隐私保护性度量": {},
            "数据合规性": {},
            "数据特征": {}
        }
    }

    if "风险评估结果" in data:
        item = data["风险评估结果"]
        report["隐私评估报告"]["隐私风险度量"] = {
            "指标得分": (
                f"本次评估重识别率为 {item['重识别率']}，统计推断还原率为 {item['统计推断还原率']}，差分攻击还原率为{item['差分攻击还原率']}。"),
            "评估结果": {},
            "指标摘要": (
                "本指标旨在度量隐私风险，特别是在遭遇攻击时，分析数据表中匿名信息的重识别与统计推断攻击风险。"
                "攻击者利用一些已知用户信息（如姓名、身份证号、性别、年龄、手机号等）进行攻击，目的是重识别匿名化的用户数据或进行统计推断，以推测出用户的私人属性。"
            ),
            "攻击模型": {
                "总体描述": "基于三种攻击方式：重识别攻击、统计推断攻击以及差分攻击。",
                "详细说明":["差分攻击:将多表数据进行链接攻击，以此来推断在多场景动态发布数据情况下，数据的隐私泄露风险。",
                            "统计推断攻击:若重识别攻击失败，攻击者将进一步对候选数据进行统计分析，基于统计结果推测用户的属性。通过分析多样本的属性关联，实现差分攻击，并通过知识图谱数据层与数据集进行连接，统计差分攻击的重识别率。",
                            "重识别攻击:攻击者根据背景知识中已知的用户信息，选取匿名数据表中与背景知识最接近的五个候选项，并假设候选的第一项数据为攻击结果。攻击成功的标准为通过直接标识符（如用户ID）确认重识别结果是否与背景知识一致。如果一致，则认为重识别攻击成功，否则攻击失败。"],
            },
            "背景知识水平假设": (
                f"为了模拟攻击者的背景知识水平，我们假设其对用户信息的了解程度为 {item['背景知识水平假设']}。"
                "这反映了攻击者已知的样本数据数量，更高的背景知识水平会提高攻击成功的概率。"
            ),
            "改进建议": [
                "提高匿名化数据的保护水平，减少重识别成功的可能性。",
                "针对属性关联较强的数据集，进行更严格的去标识化处理。",
                "通过差分隐私方法增加噪声，降低统计推断攻击的有效性。",
                "实施分级管理，对数据集的发布进行严格审批，以降低多表链接攻击的可能性。"
            ]
        }

    if "可用性评估结果" in data:
        item = data["可用性评估结果"]
        results = item.get("results", {})
        norm = item.get("norm", {})
        report["隐私评估报告"]["可用性度量"] = {
            "指标得分": item['rank']['可用性综合结果'],
            "评估结果": (
                "分数较高，该数据在隐私保护下可用性较强。"
                if item['rank']['可用性综合结果'] > 0.7 else
                "分数偏低，保护措施影响了数据的实用性，建议结合实际需求权衡隐私与可用性。"
            ),
            "数据可辨别度": {
                "指标摘要": f"数据可辨别度衡量经过隐私保护后数据记录之间的区分能力，本次评估数据可辨别度为 {item['数据可辨别度']}（归一化分数：{norm.get('数据可辨别度', 'N/A')}）。",
                "定义": "数据可辨别度反映数据记录在隐私保护下依然被区分的能力，数值越高表明信息保留越充分。",
                "结果分析": (
                    f"可辨别度原始值为 {results.get('数据可辨别度', item['数据可辨别度'])}，"
                    f"归一化分数为 {norm.get('数据可辨别度', 'N/A')}。"
                    + (
                        "说明数据区分性较好，隐私保护对实用性影响有限。"
                        if norm.get('数据可辨别度', 0) > 0.8 else
                        "说明数据区分性有一定下降，需评估是否满足业务需求。"
                    )
                ),
                "改进建议": [
                    "根据业务需求调整隐私保护算法，确保关键记录可辨别性。",
                    "监控该指标在持续保护过程中的变化，动态优化策略。"
                ]
            },
            "数据记录匿名率": {
                "指标摘要": f"数据记录匿名率为 {item['数据记录匿名率']}（归一化分数：{norm.get('数据记录匿名率', 'N/A')}），反映被隐匿或删除记录的比例。",
                "定义": "数据记录匿名率是被隐匿或删除的记录数量占总记录数量的比例，反映信息缺失情况。",
                "结果分析": (
                    f"匿名率为 {item['数据记录匿名率']}，"
                    f"原始值为 {results.get('数据记录匿名率', 'N/A')}，"
                    f"归一化分数为 {norm.get('数据记录匿名率', 'N/A')}。"
                    + (
                        "说明数据完整性很高，无信息缺失。"
                        if str(item['数据记录匿名率']) in ['0', '0.0%'] else
                        "说明有部分记录被隐匿，需关注信息丢失。"
                    )
                ),
                "改进建议": [
                    "优先采用低损失的隐私保护方式，减少删除或隐匿记录的比例。"
                ]
            },
            "平均泛化程度": {
                "指标摘要": f"平均泛化程度为 {item['平均泛化程度']}（归一化分数：{norm.get('平均泛化程度', 'N/A')}），即平均每个等价组包含的记录数。",
                "定义": "平均泛化程度反映数据聚合程度，数值越大，数据被泛化越充分，隐私性越高但精度降低。",
                "结果分析": (
                    f"平均泛化程度为 {results.get('平均泛化程度', item['平均泛化程度'])}，"
                    f"归一化分数为 {norm.get('平均泛化程度', 'N/A')}。"
                    + (
                        "较高的泛化程度有利于提升隐私保护水平，但可能影响数据分析精度。"
                        if norm.get('平均泛化程度', 0) > 0.8 else
                        "较低的泛化程度说明信息较细致，需关注隐私风险。"
                    )
                ),
                "改进建议": [
                    "根据隐私保护和业务需求动态调整泛化粒度。",
                    "探索多种泛化与聚合策略，平衡数据实用性与安全性。"
                ]
            },
            "数据损失度": {
                "指标摘要": f"数据损失度为 {item['数据损失度']}（归一化分数：{norm.get('数据损失度', 'N/A')}），表示整体信息丢失比例。",
                "定义": "数据损失度衡量数据在隐私保护后的总体信息损失，数值越大，损失越多。",
                "结果分析": (
                    f"损失度原始值为 {results.get('数据损失度', item['数据损失度'])}，"
                    f"归一化分数为 {norm.get('数据损失度', 'N/A')}。"
                    + (
                        "信息损失控制较好，数据可用性较高。"
                        if norm.get('数据损失度', 1) < 0.5 else
                        "需进一步关注信息损失，优化数据保护方式。"
                    )
                ),
                "改进建议": [
                    "细化隐私保护参数，权衡损失与隐私水平。"
                ]
            },
            "唯一记录占比": {
                "指标摘要": f"唯一记录占比为 {item['唯一记录占比']}（归一化分数：{norm.get('唯一记录占比', 'N/A')}），反映可唯一识别记录的比例。",
                "定义": "唯一记录占比表示数据集中可被唯一标识的记录比例，值越高隐私风险越大。",
                "结果分析": (
                    f"唯一记录占比为 {item['唯一记录占比']}，"
                    f"原始值为 {results.get('唯一记录占比', 'N/A')}，"
                    f"归一化分数为 {norm.get('唯一记录占比', 'N/A')}。"
                    + (
                        "无唯一记录，隐私保护非常充分。"
                        if str(item['唯一记录占比']) in ['0', '0.0%'] else
                        "存在唯一记录，建议加强隐私保护。"
                    )
                ),
                "改进建议": [
                    "进一步聚合处理敏感数据，避免唯一标识。",
                    "关注唯一记录对高风险场景的影响。"
                ]
            },
            "基于熵的平均数据损失度": {
                "指标摘要": f"基于熵的平均数据损失度为 {item['基于熵的平均数据损失度']}（原始值：{results.get('基于熵的平均数据损失度', 'N/A')}，归一化分数：{norm.get('基于熵的平均数据损失度', 'N/A')}）。",
                "定义": "通过熵变化反映数据不确定性变化，熵值损失越大，隐私提升，但数据精度降低。",
                "结果分析": (
                    f"平均熵损失度为 {results.get('基于熵的平均数据损失度', 'N/A')}，"
                    f"归一化分数为 {norm.get('基于熵的平均数据损失度', 'N/A')}。"
                    + (
                        "熵损失度偏低，数据实用性较强。"
                        if norm.get('基于熵的平均数据损失度', 1) < 0.5 else
                        "熵损失度较高，建议关注数据分析需求的影响。"
                    )
                ),
                "改进建议": [
                    "优化泛化与扰动策略，降低熵损失。",
                    "结合业务场景动态调整熵相关参数。"
                ]
            }
        }

    if "隐私保护性度量评估结果" in data:
        item = data["隐私保护性度量评估结果"]
        results = item.get("results", {})
        norm = item.get("norm", {})
        rank = item.get("rank", {})

        def safe_first(x):
            if isinstance(x, list) and x:
                return x[0]
            if isinstance(x, str):
                return x
            return ""

        # 归一化等级
        security_score = rank.get("安全性综合结果", 0)
        if security_score >= 0.95:
            security_level = "优秀"
        elif security_score >= 0.8:
            security_level = "良好"
        elif security_score >= 0.6:
            security_level = "中等"
        else:
            security_level = "较低"

        # 敏感属性重识别风险
        sensitive_attr_risk_list = []
        for attr in item.get("敏感属性的重识别风险", []):
            attribute_a = {}
            m1 = re.search(r'敏感属性([a-zA-Z0-9_]+)', attr)
            m2 = re.search(r'重识别风险为：([0-9\.%]+)', attr)
            m3 = re.search(r'等价组为(.+)', attr)
            attribute_a["敏感属性"] = m1.group(1) if m1 else "未知"
            attribute_a["重识别风险"] = m2.group(1) if m2 else "未知"
            attribute_a["等价组"] = m3.group(1) if m3 else "未知"
            sensitive_attr_risk_list.append(attribute_a)

        # 单个属性重识别风险
        single_attr_risk_list = []
        for attr in item.get("单个属性的重识别风险", []):
            attribute_a = {}
            m1 = re.search(r'标识符属性([a-zA-Z0-9_]+)', attr)
            m2 = re.search(r'重识别风险为：([0-9\.%]+)', attr)
            m3 = re.search(r'对应属性值为(.+)', attr)
            attribute_a["标识符属性"] = m1.group(1) if m1 else "未知"
            attribute_a["重识别风险"] = m2.group(1) if m2 else "未知"
            attribute_a["属性值"] = m3.group(1) if m3 else "未知"
            single_attr_risk_list.append(attribute_a)

        # 敏感属性汇总
        sensitive_attr_summary = "，".join([
            f"{attr['敏感属性']}的重识别风险为{attr['重识别风险']}，等价组为{attr['等价组']}"
            for attr in sensitive_attr_risk_list
        ])
        sensitive_attr_analysis = ""
        sensitive_attr_conclusion = ""
        for attr in sensitive_attr_risk_list:
            risk = float(attr["重识别风险"].replace('%', '')) if '%' in attr["重识别风险"] else float(
                attr["重识别风险"])
            if risk > 1.0:  # 可调阈值
                sensitive_attr_analysis += f"{attr['敏感属性']}在等价组{attr['等价组']}的重识别风险较高（{attr['重识别风险']}）。"
                sensitive_attr_conclusion += f"{attr['敏感属性']}为高风险敏感属性，"
            else:
                sensitive_attr_analysis += f"{attr['敏感属性']}在等价组{attr['等价组']}的重识别风险较低（{attr['重识别风险']}）。"
                sensitive_attr_conclusion += f"{attr['敏感属性']}为低风险敏感属性，"

        # 单个属性汇总
        single_attr_summary = "，".join([
            f"{attr['标识符属性']}为{attr['重识别风险']}"
            for attr in single_attr_risk_list
        ])
        single_attr_analysis = ""
        single_attr_conclusion = ""
        for attr in single_attr_risk_list:
            risk = float(attr["重识别风险"].replace('%', ''))
            if risk > 0.3:
                single_attr_analysis += f"{attr['标识符属性']}的重识别风险较高，为：{attr['重识别风险']}。"
                single_attr_conclusion += f"{attr['标识符属性']}具有较高的重识别风险，"
            else:
                single_attr_analysis += f"{attr['标识符属性']}的重识别风险较低，为：{attr['重识别风险']}。"
                single_attr_conclusion += f"{attr['标识符属性']}具有较低的重识别风险，"

        # 整体重识别风险
        overall_reid_text = item.get("基于熵的重识别风险", "")
        m1 = re.search(r'重识别风险为：([0-9\.%]+)', overall_reid_text)
        m2 = re.search(r'等价组为(.+)', overall_reid_text)
        overall_risk_val = m1.group(1) if m1 else "未知"
        overall_equiv_group = m2.group(1) if m2 else "未知"

        report["隐私评估报告"]["隐私保护性度量"] = {
            "指标得分": f"{security_score:.4f}",
            "安全等级": security_level,
            "评估结果": {},
            "指标摘要": (
                f"本节综合评估了数据集的分布泄露、熵泄露、隐私增益、KL散度、敏感属性和单个属性的重识别风险，以及整体重识别风险等多项隐私保护性度量。"
            ),
            "单个属性的重识别风险": {
                "报告摘要": (
                    f"单个准标识符重识别风险评估发现：{single_attr_summary}"
                ),
                "定义": "评估单个准标识符被攻击时的风险高低，是寻找潜在薄弱环节的重要依据。",
                "分析": single_attr_analysis,
                "结论": single_attr_conclusion,
                "改进建议": [
                    "加强对高风险标识符属性的泛化与分组。",
                    "针对特定风险字段采用扰动或加密等方式提升安全性。"
                ]
            },
            "分布泄露": {
                "报告摘要": safe_first(item.get("分布泄露")),
                "定义": "分布泄露衡量敏感属性在等价组与全局分布的差异，分布差异小则隐私保护效果更好。",
                "数值": f"{results.get('分布泄露', ['未知'])[0]:.5f}",
                "归一化分数": norm.get('分布泄露', "未知"),
                "分析": (
                    f"{safe_first(item.get('分布泄露'))} 分布泄露为{results.get('分布泄露', ['未知'])[0]:.5f}，归一化分数为{norm.get('分布泄露', '未知')}。"
                    "得分接近1表示泄露风险低。"
                ),
                "改进建议": [
                    "进一步优化等价组的划分，缩小敏感属性在各组与全局分布的差异。",
                    "关注分布异常的敏感属性，加强针对性脱敏处理。",
                    "定期评估数据发布过程中的分布变化，动态调整保护策略。"
                ]
            },
            "熵泄露": {
                "报告摘要": safe_first(item.get("熵泄露")),
                "定义": "熵泄露度量脱敏后敏感属性的不确定性变化。熵泄露低表示攻击者推断敏感信息的难度大。",
                "数值": f"{results.get('熵泄露', ['未知'])[0]:.5f}",
                "归一化分数": norm.get('熵泄露', "未知"),
                "分析": (
                    f"{safe_first(item.get('熵泄露'))} 熵泄露为{results.get('熵泄露', ['未知'])[0]:.5f}，归一化分数为{norm.get('熵泄露', '未知')}。"
                ),
                "改进建议": [
                    "提升脱敏强度，增加数据的不确定性，降低熵泄露水平。",
                    "采用多样化的泛化和扰动方法，提升敏感属性的熵值。",
                    "关注熵泄露高的等价组，针对性增加保护措施。"
                ]
            },
            "隐私增益": {
                "报告摘要": safe_first(item.get("隐私增益")),
                "定义": "隐私增益反映脱敏处理提升的隐私安全性，数值越大表示越难推断敏感信息。",
                "数值": f"{results.get('隐私增益', ['未知'])[0]:.6f}",
                "归一化分数": norm.get('隐私增益', "未知"),
                "分析": (
                    f"{safe_first(item.get('隐私增益'))} 隐私增益为{results.get('隐私增益', ['未知'])[0]:.6f}，归一化分数为{norm.get('隐私增益', '未知')}。"
                ),
                "改进建议": [
                    "加强脱敏手段，提高敏感属性的隐私增益。",
                    "结合实际业务场景调整匿名化和扰动参数，提升保护强度。",
                    "监控隐私增益指标，及时优化脱敏策略。"
                ]
            },
            "KL散度": {
                "报告摘要": safe_first(item.get("KL_Divergence")),
                "定义": "KL散度度量等价组敏感属性分布与全局分布的差异，KL散度越小说明隐私保护越好。",
                "数值": f"{results.get('KL_Divergence', ['未知'])[0]:.6f}",
                "归一化分数": norm.get('KL_Divergence', "未知"),
                "分析": (
                    f"{safe_first(item.get('KL_Divergence'))} KL散度为{results.get('KL_Divergence', ['未知'])[0]:.6f}，归一化分数为{norm.get('KL_Divergence', '未知')}。"
                ),
                "改进建议": [
                    "优化等价组的敏感属性分布，减少与全局分布的差异。",
                    "采用动态分组策略，降低KL散度异常的风险。",
                    "对分布差异显著的等价组加强脱敏和泛化处理。"
                ]
            },
            "敏感属性的重识别风险": {
                "报告摘要": (
                    f"敏感属性重识别风险评估发现：{sensitive_attr_summary}"
                ),
                "定义": "评估敏感信息在特定等价组中被识别的风险。风险高说明隐私保护需加强。",
                "分析": sensitive_attr_analysis,
                "结论": sensitive_attr_conclusion,
                "改进建议": [
                    "对高风险敏感属性采用更严格的脱敏措施和分组策略。",
                    "持续跟踪敏感属性的风险变化，动态优化隐私保护机制。"
                ]
            },
            "整体的重识别风险": {
                "报告摘要": (
                    f"整体敏感属性的重识别风险为{overall_risk_val}，主要风险等价组为{overall_equiv_group}。"
                ),
                "定义": "整体重识别风险是全局视角下所有敏感属性的最大被识别概率。",
                "分析": (
                    f"基于熵模型测算，数据集整体重识别风险为{overall_risk_val}，"
                    f"主要高风险集中于等价组{overall_equiv_group}。"
                ),
                "结论": (
                    f"当前整体重识别风险为{overall_risk_val}。建议重点关注高风险等价组，持续优化数据发布与脱敏策略。"
                ),
                "改进建议": [
                    "加强高风险等价组的保护措施，如更细分的分组、增强扰动。",
                    "结合业务场景灵活调整数据处理参数，降低整体风险水平。"
                ]
            },
        }

    if "合规性评估结果" in data:
        item = data["合规性评估结果"]

        def extract_last_number(string):
            numbers = re.findall(r'\d+\.?\d*', string)
            return numbers[-1] if numbers else None

        def extract_attribute_name(string):
            match = re.search(r'隐私属性(.+?)满足', string)
            return match.group(1) if match else None

        K_value = extract_last_number(str(item.get('K-Anonymity', '')))
        L_string = item.get('L-Diversity', [''])[0]
        L_value = extract_last_number(L_string)
        L_attr = extract_attribute_name(L_string)
        T_string = item.get('T-Closeness', [''])[0]
        T_value = extract_last_number(T_string)
        T_attr = extract_attribute_name(T_string)
        AK_string = item.get('(α,k)-Anonymity', [''])[0]
        AK_tuple = re.findall(r'(\d*\.?\d+)', AK_string)
        if len(AK_tuple) >= 2:
            AK_alpha, AK_k = AK_tuple[:2]
        else:
            AK_alpha, AK_k = None, None
        AK_attr = extract_attribute_name(AK_string)
        AK_satisfaction = "不满足" if "不满足" in AK_string else "满足"

        # 组合隐私属性列表
        sa_columns = []
        for s in [L_attr, T_attr, AK_attr]:
            if s and s not in sa_columns:
                sa_columns.append(s)
        sa_list = ", ".join(sa_columns) if sa_columns else "敏感属性"

        # 综述描述
        desensitization_strategy = (
            "本次数据脱敏采用了 K-匿名度、L-多样性、T-相近性和 (α,k)-匿名度等综合隐私保护技术，"
            "通过分组、泛化、频率约束等方式提升数据安全性。"
            "合规性评估表明数据处理已达到相关法规如GDPR、《个人信息保护法》对匿名化的要求，"
            "有力保障了数据在共享与分析过程中的隐私安全。"
        )
        norm_score = item.get('norm', {}).get('合规性结果', 0)
        if norm_score < 0.6:
            compliance_level = "较低"
        elif norm_score < 0.8:
            compliance_level = "中等"
        elif norm_score < 0.95:
            compliance_level = "良好"
        else:
            compliance_level = "优秀"

        report["隐私评估报告"]["数据合规性"] = {
            "指标得分": f"{norm_score:.3f}",
            "合规性等级": compliance_level,
            "评估结果": {},
            "技术描述": desensitization_strategy,
            "(α,k)-匿名度": {
                "指标摘要": f"隐私属性{AK_attr or sa_list}评估结果：{AK_string}，"
                            f"（α={AK_alpha}, k={AK_k}），{AK_satisfaction}。",
                "定义": "(α,k)-匿名度要求等价类中敏感属性的频率不超过α，同时等价类中记录不少于k条，从而抑制频率攻击。",
                "结果分析": (
                    f"当前属性{AK_attr or sa_list}在各等价类中敏感属性最大频率不超过α={AK_alpha}，"
                    f"且每组不少于k={AK_k}条，综合满足(α,k)-匿名度。"
                ),
                "改进建议": [
                    "可适当降低α、提升k，强化匿名性。",
                    "对频率分布进行动态监控，防止敏感值聚集。"
                ],
                "合规性要求": "符合国际及国内关于敏感属性频率限制的隐私保护法规要求。"
            },
            "K-匿名度": {
                "指标摘要": f"{item['K-Anonymity']}",
                "定义": "K-匿名度要求每个等价类中至少有K条记录，降低单条记录被重识别风险。",
                "结果分析": (
                    f"当前K-匿名度{K_value}，满足匿名化标准。所有等价类均包含不少于{K_value}条记录，"
                    "数据匿名性较好，能够有效防止针对个体的直接重识别。"
                ),
                "改进建议": [
                    f"如需进一步提升隐私保护，可考虑提高K值（如{int(K_value) + 1}-匿名）。",
                    "持续优化等价类划分算法，保持等价类数量均衡，进一步降低风险。"
                ],
                "合规性要求": "满足GDPR第5条数据最小化原则与国内法规对数据匿名化的要求。"
            },
            "L-多样性": {
                "指标摘要": f"隐私属性{L_attr or sa_list}评估结果：{L_string}",
                "定义": "L-多样性要求每个等价类中敏感属性的取值不少于L种，以防止敏感信息被集中推断。",
                "结果分析": (
                    f"当前属性{L_attr or sa_list}的等价类均含有至少{L_value}种敏感取值，显著提升了数据集对多样化攻击的抵抗能力。"
                ),
                "改进建议": [
                    f"如对隐私攻击风险敏感，可考虑提升L值（如{int(L_value) + 1}-多样性）。",
                    "加强等价类敏感属性分布的均衡，防止某些值过于集中。"
                ],
                "合规性要求": "符合GDPR及《个人信息保护法》中对敏感数据多样性的保护要求。"
            },
            "T-多样性": {
                "指标摘要": f"隐私属性{T_attr or sa_list}评估结果：{T_string}，满足T={T_value}-紧密性。",
                "定义": "T-多样性要求等价类中敏感属性的分布与全局分布的距离不超过T，降低推断攻击风险。",
                "结果分析": (
                    f"{T_string}。等价类中敏感属性的分布与整体分布的差异在T={T_value}阈值内，有效防止属性推断攻击。"
                ),
                "改进建议": [
                    f"根据隐私威胁模型，可尝试收紧T值（如T={float(T_value) * 0.8:.2f}），提升防护强度。",
                    "持续关注等价类的分布变化，动态调整保护参数。"
                ],
                "合规性要求": "满足GDPR及相关数据保护法中对敏感属性分布的限制标准。"
            }
        }

    if "匿名集数据特征评估结果" in data:
        item = data["匿名集数据特征评估结果"]
        quasi_identifier_dimension = item.get("准标识符维数", "未知")
        sensitive_attribute_dimension = item.get("敏感属性维数", "未知")
        inherent_privacy = item.get("固有隐私", "未知")
        sensitive_attrs = item.get("敏感属性种类", [])
        sensitive_attrs_str = ", ".join(sensitive_attrs) if sensitive_attrs else "未知"
        average_degree_of_generalization = item.get("平均泛化程度", "未知")
        norm_score = item.get("norm", {}).get("数据特征结果", None)

        # 描述性标签（可选，便于后续自动分级）
        if norm_score is not None:
            if norm_score < 0.6:
                feature_level = "较低"
            elif norm_score < 0.8:
                feature_level = "中等"
            elif norm_score < 0.95:
                feature_level = "良好"
            else:
                feature_level = "优秀"
        else:
            feature_level = "未知"

        report["隐私评估报告"]["数据特征"] = {
            "特征评价等级": feature_level,
            "综合特征分析": (
                f"从各项描述性数据特征来看，当前数据集结构清晰、敏感属性和准标识符数量适中，"
                f"平均泛化程度和固有隐私等指标也处于{feature_level}水平。"
                "建议在数据分析和开放前，持续关注数据特征指标的变化，作为调整隐私保护策略的重要依据。"
            ),
            "评估结果": {},
            "指标摘要": f"本部分为描述性指标，主要展现数据本身结构及基本属性，帮助理解数据集的整体特征和隐私保护的基础环境。",
            "准标识符维数": {
                "指标摘要": f"数据集的准标识符维数为 {quasi_identifier_dimension}，即有 {quasi_identifier_dimension} 个字段（如性别、年龄段等）能联合用于间接标识个体。",
                "定义": "准标识符是指单独或联合使用时能唯一标识用户身份的字段。维数越高，数据被间接识别的风险越高。",
                "结果分析": f"当前准标识符维数为 {quasi_identifier_dimension}，建议结合具体业务场景持续关注相关字段在不同匿名策略下的表现。"
            },
            "固有隐私": {
                "报告摘要": f"数据集的固有隐私为 {inherent_privacy}。该指标表征在无任何外部辅助信息下，攻击者需要多少二元提问才能确定敏感值。",
                "定义": "固有隐私可视为数据的‘理论安全下限’，与数据的熵有关。",
                "结果分析": f"固有隐私值为 {inherent_privacy}，意味着理论上有较强的隐私保护能力，但实际效果还需结合数据发布方式综合评估。"
            },
            "平均泛化程度": {
                "报告摘要": f"数据集的平均泛化程度为 {average_degree_of_generalization}。此值反映每个等价组中平均记录数量，数值较大表明分组更粗，有利于隐私但可能影响精度。",
                "定义": "平均泛化程度=总记录数/等价组数。该指标描述等价组划分的均匀程度，是K匿名等机制的基础描述。",
                "结果分析": f"本次评估中，平均泛化程度为 {average_degree_of_generalization}，表明当前等价组划分粒度较粗。"
            },
            "敏感属性种类": {
                "指标摘要": f"本数据集包含的敏感属性有：{sensitive_attrs_str}。",
                "定义": "敏感属性是指数据中需要重点保护、易导致隐私泄露的字段类型。",
                "结果分析": f"这些字段在数据共享、分析及脱敏过程中需优先考虑保护方案。"
            },
            "敏感属性维数": {
                "指标摘要": f"敏感属性维数为 {sensitive_attribute_dimension}，即有 {sensitive_attribute_dimension} 个需要重点隐私保护的敏感字段。",
                "定义": "敏感属性一般指包含个人隐私、财务、健康等信息的字段。敏感属性维数越多，保护难度和合规压力也会增加。",
                "结果分析": f"本数据集主要敏感属性包括：{sensitive_attrs_str}，需重点防护。"
            }
        }

    report = generate_privacy_report(worker_id, report)
    return report


## page 3 任务提交相关接口


## Page 1 任务总数
def inc_distribution(distribution, rank):
    for item in distribution:
        if item['name'] == rank:
            item['value'] += 1
            return


@api_show.route('/system_info', methods=['GET'])
def get_task_count():
    """
    获取任务总数
    """
    # 函数获取任务总数
    # 1. 获取所有以 '-0' 结尾的 key
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    r = redis.StrictRedis(port=WORKER_ID_MAP_REDISPORT, host=WORKER_ID_MAP_REDISADDRESS,
                          db=WORKER_ID_MAP_REDISDBNUM, password=WORKER_ID_MAP_REDISPASSWORD)
    all_keys = r.keys('*-0')

    tasks = []
    for k in all_keys:
        try:
            task_str = r.get(k)
            if not task_str:
                continue
            # 2. 解析json字符串
            task_info = json.loads(task_str)
            tasks.append(task_info)
        except Exception as e:
            # 可选: 记录日志
            continue

    # 3. 数据聚合示例
    total_evaluations = len(tasks)
    avg_time = 0
    scenarios = set()
    system_status = "正常"
    online_tasks = 0
    error_rate = 0
    metric_count = 0
    personal_info_count = 0
    risk_distribution = [
        {"value": 0, "name": "较低"},
        {"value": 0, "name": "中等"},
        {"value": 0, "name": "良好"},
        {"value": 0, "name": "优秀"}
    ]
    RANK_SET = {"较低", "中等", "良好", "优秀"}
    # 简单统计示例（你可根据实际字段调整聚合逻辑）
    total_cost_time = 0
    valid_costs = 0

    for task in tasks:
        # 统计评估时间
        cost = 0
        try:
            # 解析 "cost_time" 例如 "0h 0m 15s"
            cost_time = task.get("cost_time", "")
            h, m, s = 0, 0, 0
            if cost_time:
                if 'h' in cost_time:
                    h = int(cost_time.split('h')[0].strip())
                    cost_time = cost_time.split('h')[1]
                if 'm' in cost_time:
                    m = int(cost_time.split('m')[0].strip())
                    cost_time = cost_time.split('m')[1]
                if 's' in cost_time:
                    s = int(cost_time.split('s')[0].strip())
                cost = h * 60 + m + (s // 60)
                if cost == 0:
                    cost = s
            total_cost_time += cost
            valid_costs += 1
        except:
            pass

        # 场景统计
        try:
            scene = task.get("parameters", {}).get("data_scene", None)
            if scene:
                scenarios.add(scene)
        except:
            pass

        # 状态统计
        if task.get("status") == "in_progress":
            online_tasks += 1

        # 错误率统计（这里假设超时五天的为错误）
        start_time_str = task.get("start_time")
        is_error = False
        if task.get("status") == "in_progress" and start_time_str:
            try:
                start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                if datetime.now() - start_time > timedelta(days=5):
                    is_error = True
            except Exception:
                pass
        if is_error:
            error_rate += 1

        # 评估指标总数（现在2025年6月15日 总共25个）

        metric_count = 25

        # 评估个人信息总数（默认十万行数据）
        personal_info_count = str(total_evaluations * 100)

        # 风险分布统计
        task_rank = (
            task.get("evaluate", {})
            .get("评估得分", {})
            .get("rank", None)
        )
        # 只统计四种合法等级
        if task_rank in RANK_SET:
            inc_distribution(risk_distribution, task_rank)

    avg_time = round(total_cost_time / valid_costs, 2) if valid_costs else 0
    error_rate_percent = round(error_rate / total_evaluations * 100, 2) if total_evaluations else 0

    result = {
        "total_evaluations": total_evaluations,
        "avg_time": avg_time,
        "scenarios": len(scenarios),
        "system_status": system_status,
        "risk_distribution": risk_distribution,
        "online_tasks": online_tasks,
        "error_rate": error_rate_percent,
        "metric_count": metric_count,
        "personal_info_count": personal_info_count
    }
    return make_response(jsonify(result), 200)


## Page 2 指标体系可视化

# 历史案例对比图谱
@api_show.route('/all_scenes', methods=['GET'])
def all_scenes():
    """
    获取所有场景，以及每个场景下的所有任务ID
    返回格式: [{"scene": "hotel", "task_ids": ["xxx", ...]}, ...]
    """
    result = []
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    redis_client = redis.StrictRedis(
        port=WORKER_ID_MAP_REDISPORT,
        host=WORKER_ID_MAP_REDISADDRESS,
        db=WORKER_ID_MAP_REDISDBNUM,
        password=WORKER_ID_MAP_REDISPASSWORD
    )
    # 查找所有scene:xxx的key
    for key in redis_client.scan_iter("scene:*"):
        scene = key.decode().split("scene:")[1]
        result.append(scene)
        # 如果你需要带id列表: result.append({"scene": scene, "task_ids": task_ids})
    # 如果只是简单场景列表
    return jsonify(result)


@api_show.route('/tasks_by_scene', methods=['GET'])
def tasks_by_scene():
    """
    给定 scene，返回所有 task_id
    参数: ?scene=hotel
    返回: ["id1", "id2", ...]
    """
    scene = request.args.get('scene')
    if not scene:
        return jsonify([]), 400
    key = f'scene:{scene}'
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    redis_client = redis.StrictRedis(
        port=WORKER_ID_MAP_REDISPORT,
        host=WORKER_ID_MAP_REDISADDRESS,
        db=WORKER_ID_MAP_REDISDBNUM,
        password=WORKER_ID_MAP_REDISPASSWORD
    )
    if not redis_client.exists(key):
        return jsonify([])
    # 只要走到这里，task_ids 一定被赋值
    task_ids = list(redis_client.hkeys(key))
    task_ids = [tid.decode() for tid in task_ids]
    return jsonify(task_ids)


@api_show.route('/task_detail', methods=['GET'])
def task_detail():
    """
    给定任务id，返回其五个维度的得分
    返回: {id:..., rank: {维度1: 得分, ...}, evaluate:{...}}
    """
    task_id = request.args.get('id')
    print(f"task_id: {task_id}")
    if not task_id:
        return jsonify({'error': 'missing id'}), 400
    from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT
    redis_client = redis.StrictRedis(
        port=WORKER_ID_MAP_REDISPORT,
        host=WORKER_ID_MAP_REDISADDRESS,
        db=WORKER_ID_MAP_REDISDBNUM,
        password=WORKER_ID_MAP_REDISPASSWORD
    )
    val = redis_client.get(task_id + '-0')
    if not val:
        return jsonify({'error': 'not found'}), 404

    # 解析任务JSON
    try:
        task = json.loads(val)
    except Exception:
        return jsonify({'error': 'json parse error'}), 500

    # rank字段直接返回
    # 有些任务的维度在 evaluate 字段内
    res = {
        "id": task.get("id", task_id),
        "rank": task.get("rank", {}),
        "evaluate": task.get("evaluate", {})
    }
    return jsonify(res)


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


# 调试用
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
    # 修改了一下加载文件地址
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, 'data.json')
    with open(json_path, 'r', encoding='utf-8-sig') as file:
        result = json.load(file)
    return result


    """
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
    """


# 改的这个医疗卫生来测试效果
from celery_task.config import WORKER_ID_MAP_REDISADDRESS, WORKER_ID_MAP_REDISDBNUM, WORKER_ID_MAP_REDISPASSWORD, \
        WORKER_ID_MAP_REDISPORT

# 创建 Redis 客户端
redis_client = redis.StrictRedis(
    host=WORKER_ID_MAP_REDISADDRESS,
    port=WORKER_ID_MAP_REDISPORT,
    db=WORKER_ID_MAP_REDISDBNUM,
    password=WORKER_ID_MAP_REDISPASSWORD
)


def is_file_completed(filename):
    """
    检查 Redis 中是否存在指定的 filename 键，并获取其值
    """
    # 判断键是否存在
    filename = f"{filename}-0"
    if redis_client.exists(filename):
        value = redis_client.get(filename)
        json_value = json.loads(value) if value else None
        return json_value['status']
    else:
        print(f"键 '{filename}' 不存在")
        return None

def write_file_value(filename):
    json_0 = json.loads(redis_client.get(filename + '-0').decode('utf-8'))
    json_1 = json.loads(redis_client.get(filename + '-1').decode('utf-8'))
    json_2 = json.loads(redis_client.get(filename + '-2').decode('utf-8'))
    json_3 = json.loads(redis_client.get(filename + '-3').decode('utf-8'))
    json_4 = json.loads(redis_client.get(filename + '-4').decode('utf-8'))
    json_5 = json.loads(redis_client.get(filename + '-5').decode('utf-8'))
    nodes = [
        {
            "category": "任务名称",
            "id": 1,
            "name": json_0["id"],
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 60
        },
        {
            "category": "可用性",
            "id": 2,
            "name": "可用性结果：" + str(json_0["rank"]["可用性结果"]),
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 40
        },
        {
            "category": "数据特征",
            "id": 3,
            "name": "数据特征结果：" + str(json_0["rank"]["数据特征结果"]),
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 40
        },
        {
            "category": "隐私风险",
            "id": 4,
            "name": "隐私风险结果：" + str(json_0["rank"]["隐私风险度量"]),
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 40
        },
        {
            "category": "安全性",
            "id": 5,
            "name": "安全性结果：" + str(json_0["rank"]["安全性结果"]),
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 40
        },
        {
            "category": "合规性",
            "id": 6,
            "name": "合规性结果：" + str(json_0["rank"]["合规性结果"]),
            "scene": json_0["parameters"]["data_scene"],
            "symbolSize": 40
        },
        {
            "category": "可用性",
            "id": 7,
            "name": "数据可辨别度：" + str(json_3["norm"]["平均泛化程度"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "可用性",
            "id": 8,
            "name": "平均泛化程度:" + str(json_3["norm"]["平均泛化程度"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "可用性",
            "id": 9,
            "name": "数据损失度：" + str(json_3["norm"]["数据损失度"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "可用性",
            "id": 10,
            "name": "数据记录匿名率：" + str(json_3["norm"]["数据记录匿名率"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "可用性",
            "id": 11,
            "name": "基于熵的平均数据损失度：" + str(json_3["norm"]["基于熵的平均数据损失度"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "可用性",
            "id": 12,
            "name": "唯一记录占比：" + str(json_3["norm"]["唯一记录占比"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "数据特征",
            "id": 13,
            "name": "准标识符维数：" + str(json_4["results"]["准标识符维数"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "数据特征",
            "id": 14,
            "name": "敏感属性维数：" + str(json_4["results"]["敏感属性维数"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "数据特征",
            "id": 15,
            "name": "平均泛化程度：" + str(json_4["results"]["平均泛化程度"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "数据特征",
            "id": 16,
            "name": "固有隐私：" + str(json_4["results"]["固有隐私"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "隐私风险",
            "id": 18,
            "name": "重识别率：" + str(json_1["重识别率"]),
            "scene":  str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "隐私风险",
            "id": 19,
            "name": "统计推断还原率：" + str(json_1["统计推断还原率"]),
            "scene":  str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "隐私风险",
            "id": 20,
            "name": "差分还原率：" + str(json_1["差分攻击还原率"]),
            "scene":  str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
                },{
            "category": "安全性",
            "id": 21,
            "name":  "分布泄露：" + str(json_5["norm"]["分布泄露"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 22,
            "name":  "熵泄露：" + str(json_5["norm"]["熵泄露"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 23,
            "name":  "隐私增益：" + str(json_5["norm"]["隐私增益"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 24,
            "name":  "KL_Divergence：" + str(json_5["norm"]["KL_Divergence"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 25,
            "name":  "敏感属性重识别风险：" + str(json_5["norm"]["敏感属性重识别风险"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 26,
            "name":  "准标识符重识别风险：" + str(json_5["norm"]["准标识符重识别风险"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "安全性",
            "id": 27,
            "name":  "整体重识别风险：" + str(json_5["norm"]["整体重识别风险"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "合规性",
            "id": 28,
            "name": "k:" + str(json_0["parameters"]["k"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "合规性",
            "id": 29,
            "name": "l" + str(json_0["parameters"]["l"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        },
        {
            "category": "合规性",
            "id": 30,
            "name": "t：" + str(json_0["parameters"]["t"]),
            "scene": str(json_0["parameters"]["data_scene"]),
            "symbolSize": 20
        }
    ]
    lines = [
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 1,
            "target": 2,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 1,
            "target": 3,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 1,
            "target": 4,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 1,
            "target": 5,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 1,
            "target": 6,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 7,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 8,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 9,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 10,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 11,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 2,
            "target": 12,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 3,
            "target": 13,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 3,
            "target": 14,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 3,
            "target": 15,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 3,
            "target": 16,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 4,
            "target": 17,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 4,
            "target": 18,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 4,
            "target": 19,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 4,
            "target": 20,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 21,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 22,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 23,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 24,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 25,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 26,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 5,
            "target": 27,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 6,
            "target": 28,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 6,
            "target": 29,
            "value": ""
        },
        {
            "scene": str(json_0["parameters"]["data_scene"]),
            "source": 6,
            "target": 30,
            "value": ""
        }
    ]
    val = {
        "links":lines,
        "nodes": nodes
    }
    redis_client.set(filename + '-6', json.dumps(val))

def get_or_generate_file_value(filename):
    # 优先检查目标键是否已存在（直接返回-6数据）
    target_key = f"{filename}-6"
    if redis_client.exists(target_key):
        value = redis_client.get(target_key)
        return json.loads(value) if value else None
    
    # 目标键不存在，执行生成逻辑
    # 先检查文件是否完成（符合原get_file_value的前置条件）
    if is_file_completed(filename) != "completed":
        return None
    
    # 调用write_file_value生成数据（需补全原函数缺失的参数名）
    write_file_value(filename)  # 原函数定义缺少参数名，此处按调用逻辑补全
    
    # 生成后读取并返回
    value = redis_client.get(target_key)
    return json.loads(value) if value else None



@api_show.route('/main_graph', methods=['GET'])
def get_main_graph():
    # 1. 校验必填参数 id
    id = request.args.get('id')
    if not id:
        return jsonify({
            "code": 400,
            "message": "参数 id 不能为空"
        }), 400
    
    # 3. 调用封装函数：有-6数据直接返回，无则生成后返回
    result = get_or_generate_file_value(id)
    
    # 4. 处理返回结果（统一格式，便于前端解析）
    if result:
        return jsonify(result), 200
    else:
        return jsonify({
            "code": 500,
            "message": "文件未完成或数据生成失败"
        }), 500

    """
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
"""

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
    q = """
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
        s_kind = (
                label_to_category.get(source_label) or
                lp_to_category.get(source_label) or
                ls_to_category.get(source_label) or
                lm_to_category.get(source_label) or
                "未知种类"  # 默认值
        )
        t_kind = (
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
                "symbolSize": 40 if target_category == "用户id" else 20,  # 根据类型调整大小
                "attr": s_kind,
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
        for relation_list, source in [("relationships_from_u1", "source_node1"),
                                      ("relationships_from_u2", "source_node2")]:
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
    query = ''
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
    id = 1
    s_id = {}
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
            sid = s_id[a]
        else:
            sid = id
            s_id[a] = sid
            id = id + 1
            n1 = {
                "id": f"{sid}",
                "name": a,
                "type": "user",
                "size": 16,
                "color": 1,
                "category": 0
            }
            nodes.append(n1)
        if c in s_id:
            tid = s_id[c]
        else:
            tid = id
            s_id[c] = tid
            id = id + 1
            type = kind[dict[b]]["name"]
            n2 = {
                "id": f"{tid}",
                "name": c,
                "type": type,
                "size": 8,
                "color": 2,
                "category": dict[b]
            }
            nodes.append(n2)
        data = {
            "source": f"{sid}",
            "target": f"{tid}",
            "relation": b,
            "type": kind[dict[b]]["name"]
        }
        links.append(data)
    a = {"links": links, "nodes": nodes, "categories": kind}
    print("关系:", len(links), "个,节点:", len(nodes))

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
    children = []
    links = []
    id = 0
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
    dict_list = list(child_dict.values())
    root = {}
    for i in dict_list:
        if i['name'] == '隐私效果评估指标体系':
            print(i)
            root = i
    a = []
    for i in root['children']:
        a.append(handle(i, child_dict))
    session.close()
    driver.close()
    data = {
        'name': '隐私效果评估指标体系',
        'children': a,
        'desc': ''
    }
    return data


def handle(dict, child_dict):
    if dict['name'] not in child_dict.keys():
        #print(dict['name'])
        ##说明是根节点
        return dict
    l = []
    for i in child_dict[dict['name']]['children']:
        l.append(handle(i, child_dict))
    a = {
        'name': dict['name'],
        'desc': dict['desc'],
        'children': l
    }
    return a
