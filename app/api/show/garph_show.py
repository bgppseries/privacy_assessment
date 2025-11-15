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
from typing import Optional, Dict, Any
from celery_task.graph_generation import Graph

# 定义蓝图（名称必须与注册时一致）
api_graph_show = Blueprint('api_graph_show', __name__)

import redis
from flask import Blueprint, jsonify
from datetime import datetime

# 定义蓝图
api_graph_show = Blueprint('api_graph_show', __name__)

def connect_redis():
    """创建Redis连接，从配置中获取连接参数"""
    try:
        from celery_task.config import (
            WORKER_ID_MAP_REDISADDRESS,
            WORKER_ID_MAP_REDISDBNUM,
            WORKER_ID_MAP_REDISPASSWORD,
            WORKER_ID_MAP_REDISPORT
        )
        # 创建并返回Redis连接
        return redis.StrictRedis(
            host=WORKER_ID_MAP_REDISADDRESS,
            port=WORKER_ID_MAP_REDISPORT,
            db=WORKER_ID_MAP_REDISDBNUM,
            password=WORKER_ID_MAP_REDISPASSWORD,
            decode_responses=True  # 自动解码为字符串，可选
        )
    except ImportError as e:
        raise Exception(f"Redis配置导入失败: {str(e)}")
    except Exception as e:
        raise Exception(f"Redis连接创建失败: {str(e)}")

def get_redis_parsed_json(key: str):
    """
    通过connect_redis获取连接，从Redis指定key获取数据并解析为JSON字典
    参数:
        key: 要查询的Redis键名
    返回:
        解析后的JSON字典；若任何步骤失败则返回None
    """
    try:
        # 1. 获取Redis连接（复用现有连接函数）
        redis_client = connect_redis()
        if not isinstance(redis_client, redis.StrictRedis):
            raise Exception("获取的Redis连接无效")
        
        # 2. 从Redis获取原始数据
        raw_data = redis_client.get(key)
        if raw_data is None:
            return None  # key不存在时返回None
        
        # 3. 仅解析为JSON（不额外校验类型，保持灵活性）
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError as e:
            raise Exception(f"JSON解析失败: {str(e)}")
            
    except redis.RedisError as e:
        # 捕获Redis操作相关错误（如连接中断、权限问题等）
        raise Exception(f"Redis操作错误: {str(e)}")
    except Exception as e:
        # 捕获其他所有异常（连接失败、配置错误等）
        raise Exception(f"获取并解析JSON失败: {str(e)}")

def parse_parameters(uuid:str) -> Graph:
    """根据uuid解析参数并返回Graph实例"""
    # 这里假设有一个函数可以根据uuid获取参数字典
    params = get_redis_parsed_json(f"{uuid}-0")
    
    # 创建Graph实例
    graph_instance = Graph(
        k=params["parameters"]['k'],
        l=params["parameters"]['k'],
        t = params['parameters']['t'],
        url=params['parameters']['src_url'],
        address=params['parameters']['table_name'],
        worker_uuid = params['id'],
        QIDs=list(params['parameters']['QIDs']),
        SA=list(params['parameters']['SA']),
        ID=list(params['parameters']['ID']),
        bg_url=params['parameters']['un_table_name'],
        scene=params['parameters']['data_scene'],
    )
    
    return graph_instance

def get_json_file_content(id: str, num: int) -> Optional[Any]:
    """
    读取 data/{id}/{num}.json 文件内容，文件不存在时自动创建父目录并返回None
    
    参数:
        id: 目录ID（字符串）
        num: 文件编号（整数）
        
    返回:
        若文件存在且合法：返回解析后的JSON数据
        若文件不存在：创建 data/{id} 目录后返回None
        若发生错误：返回None
    """
    try:
        # 1. 定位文件路径
        base_dir = Path(__file__).resolve().parents[3]  # 与app同层目录
        json_file = base_dir / "data" / str(id) / f"{num}.json"
        
        # 2. 确保父目录存在（不存在则创建）
        json_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 3. 检查文件是否存在
        if not json_file.exists() or not json_file.is_file():
            graph_now = parse_parameters(id)
            graph_data = graph_now.get_graph(num)
        
        # 4. 读取并解析JSON内容
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    except json.JSONDecodeError:
        print(f"data/{id}/{num}.json 格式错误，无法解析")
        return None
    except Exception as e:
        print(f"读取文件失败: {str(e)}")
        return None
   
@api_graph_show.route('/graph_data', methods=['GET'])
def get_graph_data():
    """示例接口：获取Redis数据并返回"""
    try:
        data = get_redis_parsed_json("b8402d55-5835-4bb7-a34b-4ff2d38c6d8d-0")
        
        return jsonify(data), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"操作失败: {str(e)}",
            "timestamp": datetime.utcnow().isoformat()
        }), 500


@api_graph_show.route('/privacy_graph', methods=['POST'])
def privacy_graph():
    """
    获取隐私信息相关图谱接口
    请求参数：
        - task_id: str （任务ID，对应data下的子目录名）
        - graph_id: int （图谱ID，对应JSON文件名）
    返回：
        JSON格式的图谱数据（节点和关系）
    """
    try:
        # 获取请求参数
        req_data = request.get_json()
        if not req_data:
            return jsonify({
                "status": "error",
                "message": "请求参数为空，请传入JSON数据"
            }), 400
        
        # 提取并验证参数（修正参数名不一致问题）
        task_id = req_data.get('task_id')
        graph_id = req_data.get('graph_id')
        
        # 校验task_id（字符串非空）
        if not isinstance(task_id, str) or not task_id.strip():
            return jsonify({
                "status": "error",
                "message": "参数task_id必须为非空字符串"
            }), 400
        
        # 校验graph_id（正整数）
        if not isinstance(graph_id, int):
            return jsonify({
                "status": "error",
                "message": "参数graph_id必须为整数"
            }), 400
        
        # 调用函数获取图谱数据（使用修正后的参数）
        graph_data = get_json_file_content(task_id.strip(), graph_id)
        
        # 处理文件不存在的情况
        if graph_data is None:
            return jsonify({
                "status": "warning",
                "message": f"未找到图谱数据: data/{task_id}/{graph_id}.json",
                "task_id": task_id,
                "graph_id": graph_id
            }), 404
        
        # 返回成功结果
        return jsonify(graph_data), 200
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500

@api_graph_show.route('/graph_econstruction', methods=['POST'])
def generate_privacy_graph():
    """
    生成隐私图谱并存储到JSON文件接口
    请求参数：
        - task_id: str （任务ID，对应data下子目录名，需与Redis中参数uuid一致）
    返回：
        生成结果及文件存储路径
    """
    try:
        req_data = request.get_json()
        if not req_data:
            return jsonify({
                "status": "error",
                "message": "请求参数为空，请传入JSON数据"
            }), 400

        # 提取并验证参数
        task_id = req_data.get('task_id')

        if not isinstance(task_id, str) or not task_id.strip():
            return jsonify({
                "status": "error",
                "message": "参数task_id必须为非空字符串"
            }), 400

        graph = parse_parameters(task_id.strip())
        graph.refactoring()

        return jsonify({
            "status": "success",
            "message": "隐私图谱生成并存储成功",
            "data": {
                "task_id": task_id
            },
            "timestamp": datetime.utcnow().isoformat()
        }), 201  # 201表示资源创建成功

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"生成隐私图谱失败: {str(e)}",
            "timestamp": datetime.utcnow().isoformat()
        }), 500