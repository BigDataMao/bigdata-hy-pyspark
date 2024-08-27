# -*- coding: utf-8 -*-
import os
import re
from collections import defaultdict


def find_spark_tables(directory):
    table_names = defaultdict(list)  # 使用字典分类表名
    pattern = re.compile(r'spark\.table\("([^"]+)"\)')  # 正则表达式匹配表名

    # 遍历目录下的所有文件
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):  # 只处理 Python 文件
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = pattern.findall(content)  # 查找所有匹配的表名
                    for match in matches:
                        print(f"Found table: {match}")
                        # 根据表名中间的部分进行分类
                        parts = match.split('.')
                        if len(parts) >= 2:  # 确保有点分隔
                            first_part = parts[0]  # 取第一个部分作为类别
                            table_names[first_part].append(match)  # 将表名添加到对应的类别中

    return table_names


if __name__ == "__main__":
    current_directory = os.getcwd()  # 获取当前目录
    categorized_tables = find_spark_tables(current_directory)

    # 打印分类后的表名
    for category, tables in categorized_tables.items():
        print(f"Category: {category}")
        for table in set(tables):  # 去重后打印
            print(f"  - {table}")
