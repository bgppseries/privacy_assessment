from celery_task.func import anfis
import redis
import json
import pickle
import numpy as np

# 与写入函数保持一致的 Redis 连接常量
WORKER_ID_MAP_REDISADDRESS = '192.168.1.107'
WORKER_ID_MAP_REDISPORT   = 5678
WORKER_ID_MAP_REDISDBNUM  = 5
WORKER_ID_MAP_REDISPASSWORD = 'qwb'

def getvalue(worker_id: str, key: str | None = None):
    """
    从 Redis 读取指定 worker_id 下 "norm" 字段的内容。

    参数:
        worker_id (str): 需要读取的 worker_id。
        key (str | None): 若为 None，返回整个 "norm" 字典；
                          若为具体小指标名，只返回该指标的值。
    返回:
        dict | int | float | str | list: 读取到的结果，保持原始类型。
    异常:
        ValueError: 当 worker_id 不存在、"norm" 字段不存在、
                    或 key 不存在时抛出。
    """
    try:
        redis_conn = redis.StrictRedis(
            host=WORKER_ID_MAP_REDISADDRESS,
            port=WORKER_ID_MAP_REDISPORT,
            db=WORKER_ID_MAP_REDISDBNUM,
            password=WORKER_ID_MAP_REDISPASSWORD)

        current_value = redis_conn.get(worker_id)
        if not current_value:
            raise ValueError(f"No data found for worker_id '{worker_id}'")

        # 解析 JSON
        data = json.loads(current_value)

        # 校验 norm 字段
        norm_data = data.get("norm")
        if norm_data is None or not isinstance(norm_data, dict):
            raise ValueError(f"No 'norm' field found for worker_id '{worker_id}'")

        # 如果只想拿到某个具体 key
        if key is not None:
            if key not in norm_data:
                raise ValueError(f"Key '{key}' not found in 'norm' for worker_id '{worker_id}'")
            return norm_data[key]

        # 否则返回整个 norm 字典
        return norm_data

    except Exception as e:
        print(f"Error occurred while reading: {e}")
        raise


def sendvalue3(worker_id, key, value, valuetype):
    """
    向 Redis 添加 key 和 value，支持存储单一类型数据或列表数据，并确保数据类型被保留。
    即存储int，float或者str类型的单个数据，或者多个int，float或者str类型的列表数据
    写入归一化值
    参数:
        worker_ud (str): Redis 连接字符串。
        key (str): 要存储的小指标的名称。
        value (int, float, str): 要存储的小指标结果值Value，可以是单一值或单一类型的列表。
        valuetype (type): 值的类型（int 或 float 或 str）。
    """
    try:
        redis_conn = redis.StrictRedis(
            host=WORKER_ID_MAP_REDISADDRESS,
            port=WORKER_ID_MAP_REDISPORT,
            db=WORKER_ID_MAP_REDISDBNUM,
            password=WORKER_ID_MAP_REDISPASSWORD)

        current_value = redis_conn.get(worker_id)
        if not current_value:
            raise ValueError(f"No data found for worker_id '{worker_id}'")

        # 解析现有数据
        data = json.loads(current_value)

        # 确保 "rank" 字段存在并为字典
        if "rank" not in data or not isinstance(data["rank"], dict):
            data["rank"] = {}

        # 验证值的类型
        if isinstance(value, list):
            # 如果是列表，验证每个元素是否匹配指定类型
            if not all(isinstance(v, valuetype) for v in value):
                raise ValueError(f"All elements in the list must be of type {valuetype}")
        else:
            # 如果是单一值，确保值的类型匹配指定类型
            if not isinstance(value, valuetype):
                raise ValueError(f"Value must be of type {valuetype}, but got {type(value)}")

        # 添加或更新 "rank" 中的键值
        data["rank"][key] = value

        # 保存修改后的数据回 Redis
        redis_conn.set(worker_id, json.dumps(data))
        print(f"Updated 'rank' field for worker_id '{worker_id}': {data['rank']}")

    except Exception as e:
        print(f"Error occurred: {e}", key, " ", value, " ", valuetype)


def fuse_availability_score(pkl_path: str,
                            norm_metrics: dict,
                            metric_order: list[str] | None = None,
                            best_epoch: int = 88):
    """
    使用 PKL 文件中指定 epoch（默认 88）保存的 ANFIS 模型，
    对 `norm_metrics` 做指标融合并返回预测结果。

    参数
    ----
    pkl_path : str
        `model_history_*.pkl` 的完整路径。
    norm_metrics : dict
        getvalue2() 返回的指标准备字典，例如
        {
            "数据损失度": 0.04,
            "数据可辨别度": 0.91,
            ...
        }
    metric_order : list[str] | None
        若为 None，则使用训练脚本里的默认顺序；否则按给定顺序取值。
    best_epoch : int
        **真实训练轮次编号**（从 1 开始计数）。例如 88 表示脚本里 `epoch == 87`。

    返回
    ----
    float
        预测得到的可用性融合结果。
    """
    # 1. 读取 PKL 中的 model_history 列表
    with open(pkl_path, "rb") as f:
        model_history = pickle.load(f)

    # 2. 将人类编号 (1‑based) 转成 0‑based 索引
    epoch_idx = best_epoch - 1
    if epoch_idx < 0 or epoch_idx >= len(model_history):
        raise IndexError(f"epoch {best_epoch} 超出 PKL 记录范围 (1‑{len(model_history)})")

    # 3. 取出对应模型
    model_entry = model_history[epoch_idx]
    best_model = model_entry["model"]

    # 4. 构造输入特征向量
    if metric_order is None:
        metric_order = [
            "数据损失度",
            "数据可辨别度",
            "平均泛化程度",
            "数据记录匿名率",
            "唯一记录占比",
            "基于熵的平均数据损失度",
        ]
    try:
        features = [float(norm_metrics[k]) for k in metric_order]
    except KeyError as missing:
        raise KeyError(f"norm_metrics 缺失指标: {missing}") from None

    X = np.array([features], dtype=float)   # shape: (1, 6)
    y_pred =  anfis.predict(best_model, X)

    # 推理方法通常返回数组；取第一个元素即融合结果
    return float(np.asarray(y_pred).ravel()[0])




def fuse_Security_score(pkl_path: str,
                            norm_metrics: dict,
                            metric_order: list[str] | None = None,
                            best_epoch: int = 176):
    """
    使用 PKL 文件中指定 epoch（默认 176）保存的 ANFIS 模型，
    对 `norm_metrics` 做指标融合并返回预测结果。

    返回
    ----
    float
        预测得到的可用性融合结果。
    """
    # 1. 读取 PKL 中的 model_history 列表
    with open(pkl_path, "rb") as f:
        model_history = pickle.load(f)

    # 2. 将人类编号 (1‑based) 转成 0‑based 索引
    epoch_idx = best_epoch - 1
    if epoch_idx < 0 or epoch_idx >= len(model_history):
        raise IndexError(f"epoch {best_epoch} 超出 PKL 记录范围 (1‑{len(model_history)})")

    # 3. 取出对应模型
    model_entry = model_history[epoch_idx]
    best_model = model_entry["model"]

    # 4. 构造输入特征向量
    if metric_order is None:
        metric_order = [
            "分布泄露",
            "熵泄露",
            "隐私增益",
            "KL_Divergence",
            "敏感属性重识别风险",
            "整体重识别风险",
            "准标识符重识别风险"
        ]
    try:
        features = [float(norm_metrics[k]) for k in metric_order]
    except KeyError as missing:
        raise KeyError(f"norm_metrics 缺失指标: {missing}") from None

    X = np.array([features], dtype=float)   # shape: (1, 7)
    y_pred =  anfis.predict(best_model, X)

    # 推理方法通常返回数组；取第一个元素即融合结果
    return float(np.asarray(y_pred).ravel()[0])


def GetAvailability(worker_uuid: str):
    # 读取 Redis 中的指标
    all_norm = getvalue(worker_uuid)

    # 路径与文件名请按实际情况修改
    import os
    pkl_path = os.path.join(os.path.dirname(__file__), "model_history_G3_Availability_100.pkl")
    score = fuse_availability_score(
        pkl_path=pkl_path,
        norm_metrics=all_norm,
        best_epoch=88,  # 第 88 轮是最优
    )
    score = round(score,6)
    # 将数据写入redis中
    sendvalue3(worker_uuid, "可用性综合结果", score, float)
    return score

def GetSecurity(worker_uuid: str):
    # 读取 Redis 中的指标
    all_norm = getvalue(worker_uuid)
    # print(all_norm)
    # 路径与文件名请按实际情况修改
    import os
    pkl_path = os.path.join(os.path.dirname(__file__), "model_history_G3_Security_200.pkl")
    score = fuse_Security_score(
        pkl_path=pkl_path,
        norm_metrics=all_norm,
        best_epoch=176,  # 第 176 轮是最优
    )
    score = round(score, 6)
    # 将数据写入redis中
    sendvalue3(worker_uuid, "安全性综合结果", score, float)
    return score


def Getcompliance(worker_uuid: str):
    # 读取 Redis 中的指标
    all_norm = getvalue(worker_uuid)
    # print(all_norm)
    score = all_norm["合规性结果"]
    # 将数据写入redis中
    sendvalue3(worker_uuid, "可用性综合结果", score, float)
    return score

def GetCharacter(worker_uuid: str):
    # 读取 Redis 中的指标
    all_norm = getvalue(worker_uuid)
    # print(all_norm)
    score = all_norm["数据特征结果"]
    # 将数据写入redis中
    sendvalue3(worker_uuid, "可用性综合结果", score, float)
    return score


if __name__ == '__main__':
    work_id='1ce3b1da-04e1-4224-b5c5-de611bca2d34'
    #合规性结果
    num1 = Getcompliance(work_id+'-1')
    # 数据特征结果
    num2 = GetCharacter(work_id+'-2')
    # 可用性结果
    num3 = GetAvailability(work_id+'-3')
    # 安全性结果
    num4 = GetSecurity(work_id+'-4')

    FinalNum = num1*0.1 + num2*0.3 + num3*0.3 + num4*0.3
    print(FinalNum)





