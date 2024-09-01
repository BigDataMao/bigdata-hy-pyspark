# -*- coding: utf-8 -*-
import os
import re
from collections import defaultdict


def find_spark_tables(directory):
    table_names = defaultdict(list)  # 使用字典分类表名
    # 正则表达式匹配表名格式为 spark.table("表名")
    pattern_read = re.compile(r'spark\.table\("([^"]+)"\)')
    # 正则表达式匹配表名格式为 return_to_hive(..., target_table="表名"...)
    pattern_write = re.compile(r'.*target_table\s*=\s*"([^"]+)"')

    # 遍历目录下的所有文件
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):  # 只处理 Python 文件
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches_read = pattern_read.findall(content)  # 查找所有匹配的表名
                    matches_write = pattern_write.findall(content)
                    print("matches_write: ", matches_write)
                    matches_total = matches_read + matches_write
                    for match in matches_total:
                        # 根据表名中间的部分进行分类
                        parts = match.split('.')
                        if len(parts) >= 2:  # 确保有点分隔
                            first_part = parts[0]  # 取第一个部分作为类别
                            table_names[first_part].append(match)  # 将表名添加到对应的类别中

    return table_names


if __name__ == "__main__":
    current_directory = os.getcwd()  # 获取当前目录
    # 获取上级目录下的ads目录
    ads_directory = os.path.join(current_directory, '..', 'ads')
    print(f"Searching for Spark tables in: {ads_directory}")
    categorized_tables = find_spark_tables(ads_directory)

    # 打印分类后的表名
    for category, tables in categorized_tables.items():
        print(f"Category: {category}")
        # 去重并排序
        sorted_tables = sorted(set(tables))
        for table in sorted_tables:  # 去重后打印
            print(f"    {table}")
