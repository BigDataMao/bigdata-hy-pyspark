# -*- coding: utf-8 -*-
import argparse
import datetime
import functools

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, coalesce, expr, when

from src.env.config import Config
from src.utils.logger_uitls import to_color_str

config = Config()
logger = config.get_logger()
log_config = config.get("log")
is_count = log_config.get("is_count")

hive_config = config.get("hive")
if not hive_config:
    logger.error(to_color_str("未找到hive配置信息", "red"))
    raise ValueError("未找到hive配置信息")


# spark入口
def create_env():
    builder = SparkSession.builder \
        .appName("HiveTest") \
        .enableHiveSupport()
    for k, v in hive_config.items():
        builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args():
    # 解析命令行所传参数
    parser = argparse.ArgumentParser()  # 创建解析参数的对象
    parser.add_argument('--busi_date', help='business date parameter', default=None)  # 添加参数细节
    args, unknown = parser.parse_known_args()  # 获取参数
    return args.busi_date


def log(func):
    """
    装饰器，用于在函数调用前后打印日志
    :param func: 被装饰的函数
    :return: wrapper: 装饰后的函数
    """
    @functools.wraps(func)  # 这句前面不能有任何空行,否则解释器不会认为这是装饰器?
    def wrapper(*args, **kwargs):
        func_comment = func.__doc__
        func_name = func.__name__

        begin_time = datetime.datetime.now()
        logger.info("函数 %s 开始执行", func_name)
        if func_comment:
            logger.info("函数 %s 它的功能是: %s", func_name, func_comment.split("\n")[1].strip())
        else:
            logger.warning("没有找到%s函数的功能注释。", func_name)

        try:
            result_func = func(*args, **kwargs)
        except Exception as e:
            logger.error(to_color_str("函数 {} 执行出错: {}".format(func_name, str(e)[:200]), "red"))
            raise

        end_time = datetime.datetime.now()
        duration = end_time - begin_time
        duration = divmod(duration.seconds, 60)
        logger.info(
            to_color_str(
                f"函数 {func_name} 执行完成, 耗时{duration[0]}分{duration[1]}秒",
                "green"
            )
        )

        return result_func

    return wrapper


@log
def return_to_hive(spark: SparkSession, df_result: DataFrame, target_table, insert_mode, partition_column=None, partition_value=None):
    """
    用于将数据返回hive或hive分区表,
    不需要指定是否分区表,因为会开启动态分区
    :param spark: SparkSession
    :param df_result: DataFrame
    :param target_table: 目标表名
    :param insert_mode: 插入模式,可选值为"overwrite"和"append",为"overwrite"时,仅仅会覆盖分区数据,不会覆盖全表数据
    :param partition_column: 可自定义分区列名
    :param partition_value: 可自定义分区值
    :return: none
    """

    # 判断是否覆盖写
    if_overwrite = insert_mode == "overwrite"

    # 强制转换df_result中的列名为小写
    df_result = df_result.toDF(*[c.lower() for c in df_result.columns])

    # 获取目标表的元数据信息
    target_columns = [c.name for c in spark.table(target_table).schema]

    # 添加缺失的列并设置默认值
    for c in target_columns:
        if c not in df_result.columns:
            df_result = df_result.withColumn(c, lit(None))

    # 如果是分区表，添加分区列和值
    if isinstance(partition_column, str):
        partition_column = [partition_column]
    if isinstance(partition_value, str):
        partition_value = [partition_value]

    if partition_column and partition_value:
        for column, value in zip(partition_column, partition_value):
            df_result = df_result.withColumn(column, lit(value))

    """
    以下代码用于处理df_result中的列数量比Hive表中的列多的情况
    """
    # 获取Hive表的列名
    hive_table_columns = spark \
        .sql("DESCRIBE {}".format(target_table)) \
        .select("col_name") \
        .rdd \
        .map(lambda r: r[0]) \
        .collect()
    # 选择与Hive表列名匹配的列
    selected_columns = list(filter(lambda col_check: col_check in hive_table_columns, df_result.columns))
    df_result = df_result.select(selected_columns)

    logger.info("目标表为: %s", target_table)
    # 记录df_result中的总条数
    if is_count == "true":
        logger.info("正在查询df_result中的总条数......")
        logger.info("本次写入总条数: %s", df_result.count())
    # 插入数据
    df_result.select(target_columns).write.insertInto(target_table, overwrite=if_overwrite)
    logger.info("数据已写入表: %s", target_table)


def update_dataframe(df_to_update, df_use_me, join_columns, update_columns, filter_condition=None):
    """
    用df_use_me中的数据更新df_to_update中的数据
    :param df_to_update: 被更新的DataFrame,会被alias为"a"
    :param df_use_me: 用于更新的DataFrame,会被alias为"b"
    :param join_columns: 用于连接的列,为list
    :param update_columns: 需要更新的列,为list
    :param filter_condition: 过滤条件,为str,会被expr()处理, 其中的列名需要加上"a."或"b."
    :return: DataFrame
    """

    df_to_update = df_to_update.alias("a")
    df_use_me = df_use_me.alias("b")
    join_condition = " and ".join(["a.{} = b.{}".format(column, column) for column in join_columns])
    df_result = df_to_update.join(df_use_me, expr(join_condition), "left")

    # 创建更新表达式
    selection_expr = []
    for column in df_to_update.columns:
        if column in update_columns:
            updated_column = when(
                expr(join_condition) & (expr(filter_condition) if filter_condition else lit(True)),
                coalesce(col("b." + column), col("a." + column))
            ).otherwise(col("a." + column)).alias(column)
            selection_expr.append(updated_column)
        else:
            selection_expr.append(col("a." + column))  # 保留未更新的列

    # 提取结果，避免重复列
    return df_result.select(*selection_expr)


def update_dataframe_ai(df_to_update, df_use_me, join_columns, update_columns, filter_condition=None):
    """
    用df_use_me中的数据更新df_to_update中的数据 TODO AI给的版本
    :param df_to_update: 被更新的DataFrame,会被alias为"a"
    :param df_use_me: 用于更新的DataFrame,会被alias为"b"
    :param join_columns: 用于连接的列,为list
    :param update_columns: 需要更新的列,为list
    :param filter_condition: 过滤条件,为str,会被expr()处理, 其中的列名需要加上"a."或"b."
    :return: DataFrame
    """
    df_to_update = df_to_update.alias("a")
    df_use_me = df_use_me.alias("b")

    join_condition = " and ".join(["a.%s = b.%s" % (column, column) for column in join_columns])
    df_result = df_to_update.join(df_use_me, expr(join_condition), "left_outer")

    for column in update_columns:
        new_col_name = column + "_new"
        update_expr = coalesce(col("b.%s" % column), col("a.%s" % column))
        if filter_condition:
            update_expr = expr("CASE WHEN %s THEN %s ELSE a.%s END" % (filter_condition, update_expr, column))
        df_result = df_result.withColumn(new_col_name, update_expr).drop(column).withColumnRenamed(new_col_name, column)

    # 删除df_result中属于df_use_me的列
    df_result = df_result.select(
        [col("a.%s" % c) for c in df_to_update.columns] +
        [col("b.%s" % c) for c in df_use_me.columns if c not in join_columns])

    return df_result
