import pandas as pd
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import numpy as np

class DataAnalyzer:
    def __init__(self, connection_string, table_name, columns, sample_ratios, population_ratios=None):
        self.src_url = connection_string
        self.table_name = table_name
        self.QIDs = columns  # 假设传入的columns对应QIDs
        self.SA = []  # 如果SA有具体值，需要从外部传入
        self.sample_ratios = sample_ratios
        self.population_ratios = population_ratios or {}  # 存储总体比例
        
    def get_data(self):
        """从数据库获取数据"""
        try:
            columns = self.QIDs + self.SA
            query = f"SELECT {', '.join([f'`{col}`' for col in columns])}  FROM {self.json_address}"
            ### query = f"SELECT {', '.join(columns)} FROM {self.table_name}"
            
            with create_engine(self.src_url).connect() as engine:
                print(f"执行查询: {query}")
                return pd.read_sql(sql=text(query), con=engine)
                
        except SQLAlchemyError as e:
            print(f"SQLAlchemy错误: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            print(f"获取数据错误: {e}")
            return pd.DataFrame()
    
    def calculate_column_ratios(self, data):
        """计算采样比例、与总体比例的差值及置信区间，并过滤异常值"""
        if data.empty:
            print("没有数据可分析")
            return {}
            
        results = {}
        
        # 先计算总体比例（如果未提供）
        if not self.population_ratios:
            self._calculate_population_ratios(data)
            
        for ratio in self.sample_ratios:
            sample_size = int(len(data) * ratio)
            if sample_size == 0:
                sample_size = 1  # 确保至少采样1行
            
            sample_data = data.head(sample_size)
            column_analysis = {}
            
            for col in self.QIDs + self.SA:
                if col in sample_data.columns and col in self.population_ratios:
                    # 计算采样比例
                    sample_ratio = sample_data[col].value_counts(normalize=True)
                    top_10 = sample_ratio.head(10).to_dict()
                    
                    col_analysis = {"采样比例": top_10, "置信区间": {}}
                    
                    # 计算每个值的置信区间
                    valid_ci_values = []  # 存储有效置信区间（用于均值计算）
                    for value, sample_pct in top_10.items():
                        if value in self.population_ratios[col]:
                            pop_pct = self.population_ratios[col][value]
                            
                            # 计算绝对差值
                            abs_diff = abs(sample_pct - pop_pct)
                            
                            # 按公式计算置信区间
                            confidence_interval = (abs_diff / pop_pct * 100) * 100 if pop_pct > 0 else 0
                            
                            # 过滤置信区间大于500%的数据
                            if confidence_interval <= 500:
                                # 使用Sigmoid函数压缩置信区间到0-1范围
                                compressed_ci = self._sigmoid(confidence_interval / 100)
                                
                                col_analysis["置信区间"][value] = {
                                    "比例差值": abs_diff,
                                    "原始置信区间": f"{confidence_interval:.2f}%",
                                    "压缩后置信区间": compressed_ci
                                }
                                valid_ci_values.append(compressed_ci)
                            else:
                                print(f"警告: {col}列中值 '{value}' 的置信区间为 {confidence_interval:.2f}%，已超过500%，不参与计算")
                    
                    # 计算该列所有有效值的压缩后置信区间的均值
                    if valid_ci_values:
                        col_analysis["平均压缩置信区间"] = sum(valid_ci_values) / len(valid_ci_values)
                    else:
                        col_analysis["平均压缩置信区间"] = 0  # 没有有效数据时设为0
                    
                    column_analysis[col] = col_analysis
            
            results[ratio] = column_analysis
        
        return results
    
    def _sigmoid(self, x):
        """Sigmoid函数，将任意实数压缩到(0,1)区间"""
        return 1 / (1 + np.exp(-x))
    
    def _calculate_population_ratios(self, data):
        """计算总体数据的比例（若未提供外部数据）"""
        self.population_ratios = {}
        for col in self.QIDs + self.SA:
            if col in data.columns:
                self.population_ratios[col] = data[col].value_counts(normalize=True).to_dict()

def parse_arguments():
    """解析命令行参数，新增总体比例输入"""
    parser = argparse.ArgumentParser(description='计算采样比例与总体比例的差值及置信区间')
    parser.add_argument('--connection', required=True, help='数据库连接字符串')
    parser.add_argument('--table', required=True, help='要查询的表名')
    parser.add_argument('--columns', nargs='+', required=True, help='指定分析列名')
    parser.add_argument('--ratios', type=float, nargs='+', default=[0.3, 0.5, 0.7], help='采样比例')
    parser.add_argument('--population', nargs='+', default=None, 
                        help='总体比例参数，格式: col1:value1:ratio col1:value2:ratio ...')
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # 解析总体比例参数
    population_ratios = {}
    if args.population:
        for param in args.population:
            parts = param.split(':')
            if len(parts) == 3:
                col, value, ratio = parts[0], parts[1], float(parts[2])
                if col not in population_ratios:
                    population_ratios[col] = {}
                population_ratios[col][value] = ratio / 100  # 转换为小数
    
    analyzer = DataAnalyzer(
        connection_string=args.connection,
        table_name=args.table,
        columns=args.columns,
        sample_ratios=args.ratios,
        population_ratios=population_ratios
    )
    
    data = analyzer.get_data()
    if data.empty:
        return
        
    results = analyzer.calculate_column_ratios(data)
    
    # 计算跨采样率的总体均值
    all_sample_means = []
    for ratio, col_results in results.items():
        col_means = []
        for col, analysis in col_results.items():
            col_means.append(analysis["平均压缩置信区间"])
        if col_means:
            sample_mean = sum(col_means) / len(col_means)
            all_sample_means.append(sample_mean)
    
    # 计算所有采样率的总体均值
    overall_mean = sum(all_sample_means) / len(all_sample_means) if all_sample_means else 0
    print(f"所有采样率的总体均值: {overall_mean:.4f}")

if __name__ == "__main__":
    main()