# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00128_data(spark: SparkSession, busi_date: str):
    """
    投资者保障基金调整表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_client_revenue").alias("t") \
        .filter(
        (col("t.month_id") == v_month_id) &
        (col("t.is_main") == "1")
    ).join(
        spark.table("ddw.t_ctp_branch_oa_rela").alias("x"),
        col("t.branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).groupBy(
        col("t.month_id").alias("month_id"),
        col("t.branch_id").alias("ctp_branch_id"),
        col("x.oa_branch_id").alias("oa_branch_id"),
    ).agg(
        sum(col("t.done_money")).alias("done_money"),  # 成交金额
        sum(col("t.secu_fee")).alias("bzjj"),  # 保证基金
    ).fillna(0)

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00128_data",
        insert_mode="overwrite"
    )
