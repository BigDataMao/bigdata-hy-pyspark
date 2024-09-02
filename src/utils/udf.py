# -*- coding: utf-8 -*-
from data.dictionaries.dictionary import map_table


def f_get_multi_key_note(i_key_value: str, i_key_name: str):
    """
    获取多个键值对的注释
    :param i_key_value: 以逗号隔开的键值,例如: '1,2,3'
    :param i_key_name: 父键名,例如: 'report.wh_risk_level'
    :return: 以逗号隔开的键值对应的注释,例如: '低风险,中风险,高风险'
    """
    key_value_list = i_key_value.split(',')
    note_list = [map_table.get(i_key_name).get(key) for key in key_value_list]
    # 去除空值
    note_list = [note for note in note_list if note]
    return ','.join(note_list)


if __name__ == '__main__':
    # 用于测试,打印结果应该是: "C4"
    print(f_get_multi_key_note('3', 'report.wh_risk_level'))

    # 用于测试,打印结果应该是: "C2,C3,C4"
    print(f_get_multi_key_note('1,2,3', 'report.wh_risk_level'))
