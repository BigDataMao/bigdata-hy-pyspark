from pyspark.sql.functions import col, sum as spark_sum

from src.env.task_env import create_env
from tests.match_test.check_list import check_list

# 创建SparkSession
spark = create_env()

# 从Oracle表读取数据
jdbc_url = "jdbc:oracle:thin:@192.168.25.15:1521:wolf"
jdbc_user = "wolf"
jdbc_password = "wolf"
jdbc_driver_name = "oracle.jdbc.driver.OracleDriver"

connection_properties = {
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": jdbc_driver_name
}


def sum_numeric_columns(o_name: str, h_name: str, filter_str: str = "1 = 1") -> (dict, dict, dict):
    # 从Oracle表读取数据
    oracle_df = spark.read.jdbc(url=jdbc_url, table=o_name, properties=connection_properties).filter(filter_str)
    # 从Hive表读取数据
    hive_df = spark.sql(f"SELECT * FROM {h_name}").filter(filter_str)

    # 获取所有数字列
    numeric_columns = [
        field.name for field in hive_df.schema.fields
        if field.dataType.simpleString() in ["int", "bigint", "double", "float", "decimal", "decimal(19,6)"]
    ]

    oracle_sums = oracle_df.select([spark_sum(col(c)).alias(c) for c in numeric_columns]).collect()[0].asDict()
    hive_sums = hive_df.select([spark_sum(col(c)).alias(c) for c in numeric_columns]).collect()[0].asDict()
    # 解决空值问题
    for c in numeric_columns:
        if oracle_sums[c] is None:
            oracle_sums[c] = 0
        if hive_sums[c] is None:
            hive_sums[c] = 0

    # 计算差额
    differences = {
        c: (float(oracle_sums[c]) - float(hive_sums[c]) if abs(float(oracle_sums[c]) - float(hive_sums[c])) >= 0.1 else 0)
        for c in numeric_columns
    }

    # 以下为格式化输出内容
    # 获取最大key长度
    max_key_length = max(len(key) for key in differences.keys())
    # 打印表头
    print(f"{'Key':<{max_key_length}} : {'Difference':>20}   {oracle_table_name:>35}   {hive_table_name:>35}")
    # 打印数据
    for key, value in differences.items():
        print(
            f"{key:<{max_key_length}} : {round(value,1):>20}   {round(oracle_sums[key],1):>35}   {round(hive_sums[key],1):>35}")

    return oracle_sums, hive_sums, differences


for oracle_table_name, hive_table_name, filter_condition in check_list:
    # 计算数字列的和与差额
    sum_numeric_columns(oracle_table_name, hive_table_name, filter_condition)
    print("\n")
    print("=" * 130)
    print("\n")

# 停止SparkSession
spark.stop()
