# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00304_data(spark: SparkSession, busi_date: str):
    """
    权益溯源_资管产品-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()

    df_result = spark.table("ddw.t_cockpit_00203").alias("t").filter(
        col("t.busi_date").substr(1, 6) == lit(v_month_id)
    ).groupBy(
        col("t.busi_date").substr(1, 6).alias("month_id"),
        col("t.product_name"),
        col("t.broker_branch")
    ).agg(
        sum("t.broker_rights").alias("broker_rights")
    ).alias("tmp").join(
        spark.table("ddw.t_cockpit_proj_index_rela").alias("a"),
        col("tmp.product_name") == col("a.adjust_proj"),
        "left"
    ).join(
        spark.table("ddw.t_oa_branch").alias("b"),
        col("tmp.broker_branch") == col("b.shortname"),
        "inner"
    ).filter(
        (col("tmp.month_id") == lit(v_month_id)) &
        (col("a.adjust_proj_id").substr(1, 20) == lit(v_op_object))
    ).select(
        col("tmp.month_id"),
        col("a.adjust_proj_id"),
        col("tmp.product_name").alias("adjust_proj"),
        col("a.src_branch_id").alias("src_branch_id"),
        col("a.src_branch_name").alias("src_branch_name"),
        col("b.departmentid").alias("branch_id"),
        col("tmp.broker_branch").alias("branch_name"),
        col("a.index_id").alias("index_id"),
        col("a.index_name").alias("index_name"),
        col("tmp.broker_rights").alias("adjust_value")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00301",
        insert_mode="overwrite"
    )
