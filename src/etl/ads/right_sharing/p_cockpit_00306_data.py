# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, when

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00306_data(spark: SparkSession, busi_date: str):
    """
    权益溯源_其他调整-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()

    df_result = spark.table("ddw.t_cockpit_00306").alias("t").filter(
        col("month_id") == lit(v_month_id)
    ).select(
        col("t.month_id"),
        col("t.adjust_proj_id"),
        col("t.adjust_proj"),
        col("t.src_branch_id"),
        col("t.src_branch_name"),
        col("t.branch_id"),
        col("t.branch_name"),
        col("t.index_id"),
        col("t.index_name"),
        col("t.adjust_value")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00301",
        insert_mode="overwrite"
    )
