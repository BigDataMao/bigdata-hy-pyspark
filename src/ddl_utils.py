# -*- coding: utf-8 -*-
"""
据说hive社区bug: https://issues.apache.org/jira/browse/HIVE-11837
完整探讨连接如下: https://blog.51cto.com/u_12902/6698107
导致即便设置好了元数据中文编码为UTF-8, 但是在Hive中还是会出现乱码,体现在show create table时的表注释

但在Dbeaver等工具中,设置好连接参数,可以正常显示中文注释

在MySQL中执行:
use metastore; 或者 use hive;(取决于你的hive配置)
alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8;
alter table TABLE_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;

hive连接参数添加:
characterEncoding=UTF-8
"""
import json

import oracledb
from pyhive import hive

from src.env.config import Config

if __name__ == '__main__':
    oracledb.init_oracle_client(lib_dir=r'/usr/lib/oracle/11.2/client64/lib')
    oracle_conn = Config().get('oracle')
    pool = oracledb.create_pool(**oracle_conn)
    oracle_pool = pool.acquire()

    oracle_user = 'CF_BUSIMG'
    hive_user = 'DDW'
    table_info = {}

    with oracle_pool.cursor() as cursor:
        try:
            # 获取指定用户下的所有表
            cursor.execute(f"SELECT table_name FROM all_tables WHERE owner = '{oracle_user}'")
            tables = [row[0] for row in cursor.fetchall()]
            # 循环处理每个表
            for table in tables:
                # 获取表的表名注释
                cursor.execute(
                    f"SELECT comments FROM all_tab_comments WHERE table_name = '{table}' AND owner = '{oracle_user}'"
                )
                table_comment = cursor.fetchone()[0]
                # 加入字典
                table_info[table] = {'table_comment': table_comment}
                # 获取表的列名和列注释
                cursor.execute(
                    f"SELECT column_name, comments FROM all_col_comments WHERE table_name = '{table}' AND owner = '{oracle_user}'"
                )
                columns = {row[0]: row[1] for row in cursor.fetchall()}
                # 加入字典
                table_info[table]['columns'] = columns

            print(json.dumps(table_info['T_COCKPIT_PROJ_INDEX_RELA'], ensure_ascii=False, indent=4))
        except oracledb.DatabaseError as e:
            error, = e.args
            print("处理表时发生错误:", error.message)

    pool.release(oracle_pool)
    pool.close()

    # 建立hive连接
    hive_conn_config = Config().get('hive')
    hive_host = hive_conn_config.get('host')
    hive_port = hive_conn_config.get('port')
    hive_username = hive_conn_config.get('username')
    hive_conn = hive.Connection(host=hive_host, port=hive_port, username=hive_username)

    with hive_conn.cursor() as cursor:
        # 基于字典更新表注释
        for table, info in table_info.items():
            table_comment = info.get('table_comment')
            if table_comment:
                print(f"表 {table} 的注释为{table_comment}")
                try:
                    cursor.execute(f"ALTER TABLE {hive_user}.{table} SET TBLPROPERTIES ('comment' = '{table_comment}')")
                    print(f"表 {table} 的注释已更新")
                except Exception as e:
                    print(f"处理表 {table} 时发生错误:")

            try:
                # 基于字典更新列注释
                columns: dict = info.get('columns')
                print(columns)
                # 获取列类型
                cursor.execute(f"DESCRIBE {hive_user}.{table}")
                column_types = {}
                for row in cursor.fetchall():
                    if row[0].upper() in columns.keys():
                        column_types[row[0].upper()] = row[1]

                print(column_types)
                for column, comment in columns.items():
                    # 更新列注释
                    sql = f"ALTER TABLE {hive_user}.{table} CHANGE COLUMN {column} {column} {column_types[column]} COMMENT '{comment}'"
                    print(sql)
                    cursor.execute(sql)
            except Exception as e:
                print(f"修改列注释 {table} 时发生错误:")
    hive_conn.close()
