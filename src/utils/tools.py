import csv
from typing import List, Dict, Any, OrderedDict


def clean_table_info(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    清洗表信息
    :param data: 从csv文件中读取的表信息
    :return table_info: 清洗后的表信息
        只保留oracle_db, oracle_table, hive_table_fullname, is_partition, partition_col
    """
    table_info = []
    for item in data:
        if not item.get('oracle_table_fullname') or not item.get('hive_table_fullname'):
            continue
        oracle_info = item.get('oracle_table_fullname').strip().split('.')
        oracle_db = oracle_info[0].upper()
        oracle_table = oracle_info[1].upper()
        hive_table_fullname = item.get('hive_table_fullname').strip().lower()
        partition_col = item.get('partition_col').lower().strip().split(',') if item.get('partition_col') else []
        is_partition = True if partition_col else False

        table_info.append({
            'oracle_db': oracle_db,
            'oracle_table': oracle_table,
            'hive_table_fullname': hive_table_fullname,
            'is_partition': is_partition,
            'partition_col': partition_col
        })
    return table_info


def csv_to_dict_list(file_path) -> List[Dict[str, Any]]:
    """
    读取 CSV 文件并转换为字典列表
    :param file_path: 文件路径
    :return: 字典列表
    """
    data: List[Dict[str, Any]] = []
    with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        item: OrderedDict[str, Any]
        for item in csv_reader:
            data.append(dict(item))
    return data


