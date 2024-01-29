import csv
import os.path
import re
import shlex
import json
import uuid
import pandas as pd
import subprocess
import platform


def execute_real_time_command(cmd):
    try:
        p = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print('--->' + out.decode())
        res = 'failed' if p.returncode else 'sucess'
        print('---->' + res)
        return p.returncode
    except Exception as e:
        print('---->function exec_real_time error:%s' % str(e))
        return 648


def execute_command(command):
    try:
        po = os.popen(command)
        msg = po.buffer.read().decode('utf-8')
        return msg
    except subprocess.CalledProcessError as e:
        return e.output


def import_neo4j_prepro():
    """
    todo 目前还是写死
    基本思路是执行脚本，neo4j -v获取版本号，以此搜索文件目录，但实用性不强，考虑到neo4j安装目录可以修改名称
    还是写死好
    :return:
    """
    end_neo4j()
    clear_folder("C:\\neo4j\\neo4j-community-4.4.17\data\databases\gdbs")
    clear_folder("C:\\neo4j\\neo4j-community-4.4.17\data\\transactions\gdbs")


def importer():
    current_os = platform.system()
    if current_os == 'Windows':
        print("正在执行数据库重构")
        import_neo4j_prepro()
        print("数据库重构完成")
        # todo 下面的代码待封装
        command = 'neo4j-admin.bat import  --database=gdbs '
        folder = get_current_parentpath() + '\data\\test'
        foldert = folder + '\\'
        node_file_list = []
        rela_file_list = []
        for _, _, files in os.walk(folder):
            for file in files:
                if re.search("node", file):
                    node_file_list.append(file)
                elif re.search("relationship", file):
                    rela_file_list.append(file)
        for file in node_file_list:
            command = command + '--nodes=' + foldert + file + ' '
        for file in rela_file_list:
            command = command + '--relationships=' + foldert + file + ' '
        print(command)
        # todo log
        print(execute_command(command))
        print("import 命令执行完成")
        delete_files_in_folder(folder)
        print("文件回收完成,正在启动neo4j")
        start_neo4j()
    elif current_os == 'Linux':
        command = ''
    else:
        print("err: unsported operating system")
        return


def delete_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            ##todo log


def read_csv(path, out):
    df = pd.read_csv(path)
    other_columns = df.columns[0:]
    u_node = []
    A_node = []
    A_U_link = []
    uuid_column = [str(uuid.uuid4()) for _ in range(df.shape[0])]
    node_id = 0
    for l, row in df.iterrows():
        u_node.append((uuid_column[l], "user"))
        for column in other_columns:
            other_value = row[column]
            A_node.append((node_id, other_value, f"{column}"))
            A_U_link.append((uuid_column[l], node_id, f"{column}"))
            node_id = node_id + 1
    unode = pd.DataFrame(u_node, columns=['uuid:ID', ':LABEL'])
    anode = pd.DataFrame(A_node, columns=['node_id:ID', 'value', ':LABEL'])
    A_U = pd.DataFrame(A_U_link, columns=[':START_ID', ':END_ID', ':TYPE'])
    out1 = out + "u_nodes.csv"
    out2 = out + "a_nodes.csv"
    out3 = out + "au_relationship.csv"
    unode.to_csv(out1, index=False, encoding='UTF-8')
    anode.to_csv(out2, index=False, encoding='UTF-8')
    A_U.to_csv(out3, index=False, encoding='UTF-8')


def readline_count(file_name):
    return len(open(file_name).readlines())


def read_json(path):
    # JSON文件 字符集必须是UTF-8
    with open(path,'r',encoding='UTF-8') as file:
        data=pd.read_json(path)
        print(data.info)
        u_node = []
        A_node = []
        A_U_link = []
        uuid_column = [str(uuid.uuid4()) for _ in range(data.shape[0])]
        node_id = 0
        for l, row in data.iterrows():
            u_node.append((uuid_column[l], "user"))
            for column in data.columns:
                other_value = row[column]
                A_node.append((node_id, other_value, f"{column}"))
                A_U_link.append((uuid_column[l], node_id, f"{column}"))
                node_id = node_id + 1
        unode = pd.DataFrame(u_node, columns=['uuid:ID', ':LABEL'])
        anode = pd.DataFrame(A_node, columns=['node_id:ID', 'value', ':LABEL'])
        A_U = pd.DataFrame(A_U_link, columns=[':START_ID', ':END_ID', ':TYPE'])
        print(unode.info)
        print(anode.info)
        print(A_U)


def clear_folder(folder_path):
    """
    删除文件夹
    :param folder_path:要删除的文件夹目录
    :return:
    """
    # 遍历文件夹中的所有文件和子文件夹
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        # 判断是否为文件
        if os.path.isfile(file_path):
            # 删除文件
            os.remove(file_path)
        # 判断是否为文件夹
        elif os.path.isdir(file_path):
            # 递归调用清空文件夹函数
            clear_folder(file_path)
            # 删除空文件夹
            os.rmdir(file_path)
    os.rmdir(folder_path)


def get_current_dirpath():
    return os.path.dirname(os.path.abspath(__file__))


def get_current_filepath():
    return os.path.abspath(__file__)


def get_current_parentpath():
    current_file = os.path.abspath(__file__)
    directory = os.path.dirname(current_file)
    parent_directory = os.path.dirname(directory)
    return parent_directory


def start_neo4j():
    current_os = platform.system()
    if current_os == 'Windows':
        result = execute_command("neo4j.bat start")
    elif current_os == 'Linux':
        result = execute_command("neo4j start")
    return result


def end_neo4j():
    current_os = platform.system()
    if current_os == 'Windows':
        result = execute_command("neo4j.bat stop")
    elif current_os == 'Linux':
        result = execute_command("neo4j stop")
    return result


if __name__ == '__main__':
    path = '../data/unhandeled/hotel.json'
    out = '../data/to_assess/'
    s = get_current_parentpath()##项目目录
    t = get_current_dirpath()
    


