# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum

from src.env.task_env import log, logger, return_to_hive
from src.utils.date_utils import get_date_period_and_days


@log
def p_cockpit_00301_data(spark: SparkSession, busi_date: str):
    """
    权益溯源_部门间数据调整-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month = busi_date[:6]
    v_begin_date, v_end_date, v_trade_days = get_date_period_and_days(
        busi_month=v_month
    )
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    logger.info(f"v_begin_date: {v_begin_date}, v_end_date: {v_end_date}, v_trade_days: {v_trade_days}, v_op_object: {v_op_object}")

    df_result = spark.table("ddw.t_cockpit_00114_data").alias("t").filter(
        col("t.busi_date").substr(1, 6) == v_month
    ).crossJoin(
        spark.table("ddw.t_cockpit_proj_index_rela").alias("a").filter(
            col("a.adjust_proj_id") == lit(v_op_object)
        )
    ).groupBy(
        lit(v_month).alias("month_id"),
        col("a.adjust_proj_id"),
        col("a.adjust_proj"),
        col("t.out_oa_branch_id").alias("src_branch_id"),
        col("t.out_oa_branch_name").alias("src_branch_name"),
        col("t.in_oa_branch_id").alias("branch_id"),
        col("t.in_oa_branch_name").alias("branch_name"),
        col("a.index_id"),
        col("a.index_name")
    ).agg(
        when(
            col("a.index_id") == "01",
            sum(col("t.allocat_end_rights")) / lit(v_trade_days)
        ).when(
            col("a.index_id") == "02",
            sum(
                when(
                    col("t.busi_date") == lit(v_end_date),
                    col("t.allocat_end_rights")
                ).otherwise(0)
            )
        ).when(
            col("a.index_id") == "04",
            sum(col("t.allocat_done_amount"))
        ).when(
            col("a.index_id") == "05",
            sum(col("t.allocat_done_money"))
        ).when(
            col("a.index_id") == "06",
            sum(col("t.allocat_remain_transfee"))
        ).otherwise(lit(0)).alias("adjust_value")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00301",
        insert_mode="overwrite"
    )
