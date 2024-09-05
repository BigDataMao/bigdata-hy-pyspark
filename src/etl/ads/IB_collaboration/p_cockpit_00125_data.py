# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sum, concat, when

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00125_data(spark: SparkSession, busi_date: str):
    """
    ib协同收入调整表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_client_revenue").alias("t") \
        .filter(
        col("t.month_id") == v_month_id
    ).join(
        spark.table("ddw.t_cockpit_00107").alias("a"),
        (col("t.oa_broker_name") == col("a.futu_service_name")) &
        (col("t.fund_account_id") == col("a.fund_account_id")),
        "inner"
    ).select(
        col("t.month_id"),  # 月份
        col("a.ib_branch_id"),  # ib分支机构id
        col("a.ib_branch_name"),  # ib分支机构
        col("t.fund_account_id"),  # 资金账号
        col("t.client_name"),  # 客户名称
        col("t.oa_broker_id").alias("broker_id"),  # 经纪人id
        col("t.oa_broker_name").alias("broker_name"),  # 经纪人
        col("t.branch_id").alias("ctp_branch_id"),  # ctp分支机构id
        col("t.branch_name").alias("ctp_branch_name"),  # ctp分支机构
        col("t.yes_rights").alias("begin_rights"),  # 开始权益
        col("t.end_rights").alias("end_rights"),  # 结束权益
        col("t.avg_rights").alias("avg_rights"),  # 平均权益
        col("t.remain_transfee").alias("remain_transfee"),  # 剩余交易费
        (
                coalesce(col("t.ib_rebate"), lit(0)) +
                coalesce(col("t.ib_ret"), lit(0))
        ).alias("ibxt_income"),  # ib协同收入
        (
                col("t.ib_interest") /
                when((col("a.coope_income_reate") == 0) | (col("a.coope_income_reate").isNull()), 1).otherwise(col("a.coope_income_reate"))
        ).alias("interest_clear_income"),  # 利息清算收入
        (
                col("t.ib_ret") /
                when((col("a.coope_income_reate") == 0) | (col("a.coope_income_reate").isNull()), 1).otherwise(col("a.coope_income_reate"))
        ).alias("market_reduct_income"),  # 市场回撤收入
        col("a.coope_income_reate").alias("ibxt_income_reate"),  # ib协同收入比例
        col("t.ib_interest").alias("ibxt_interest_clear_income"),  # ib协同利息清算收入
        col("t.ib_ret").alias("ibxt_market_reduct_income")  # ib协同市场回撤收入
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00125",
        insert_mode="overwrite"
    )