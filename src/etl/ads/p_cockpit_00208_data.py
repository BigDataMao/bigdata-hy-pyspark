# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00208_data(spark: SparkSession, busi_date: str):
    """
    场外期权清算台账-数据落地
    :param spark:
    :param busi_date:
    :return: None
    """

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_00207").alias("t") \
        .filter(
        col("t.add_date").substr(1, 6) == v_month_id
    ).select(
        lit(v_month_id).alias("month_id"),
        col("t.add_date"),
        col("t.client_id"),
        col("t.client_name"),
        col("t.open_date"),
        col("t.branch_id"),
        col("t.branch_name"),
        col("t.introductor_id"),
        col("t.introductor"),
        col("t.trade_begin_date"),
        col("t.trade_orders"),
        col("t.premium_abs"),
        col("t.actual_notional_principal"),
        col("t.sale_income"),
        col("t.colla_pricing"),
        col("t.total_income")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00208",
        insert_mode="overwrite"
    )
