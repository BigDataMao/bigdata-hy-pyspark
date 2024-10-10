# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00303_data(spark: SparkSession, busi_date: str):
    """
    权益溯源_ib协同-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()

    df_result = spark.table("ddw.t_cockpit_00125").alias("t").filter(
        col("month_id") == v_month_id
    ).crossJoin(
        spark.table("ddw.t_cockpit_proj_index_rela").alias("a").filter(
            col("adjust_proj_id") == lit(v_op_object)
        )
    ).join(
        spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        col("t.ctp_branch_id") == col("c.ctp_branch_id"),
        "inner"
    ).groupBy(
        col("t.month_id"),
        col("a.adjust_proj_id"),
        col("a.adjust_proj"),
        col("c.oa_branch_id"),
        col("c.oa_branch_name"),
        col("a.index_id"),
        col("a.index_name")
    ).agg(
        sum(
            when(col("a.index_id") == "01", col("t.avg_rights"))
            .when(col("a.index_id") == "02", col("t.end_rights"))
            .when(col("a.index_id") == "03", col("t.begin_rights"))
        ).alias("adjust_value")
    ).select(
        col("t.month_id"),
        col("a.adjust_proj_id"),
        col("a.adjust_proj"),
        lit("").alias("src_branch_id"),
        lit("").alias("src_branch_name"),
        col("c.oa_branch_id"),
        col("c.oa_branch_name"),
        col("a.index_id"),
        col("a.index_name"),
        col("adjust_value")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00301",
        insert_mode="overwrite",
    )
