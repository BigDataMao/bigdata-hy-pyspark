# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00114_data(spark: SparkSession, busi_date: str):
    """
    部门间数据调整表-数据落地
    :param spark: spark session
    :param busi_date: 业务日期
    :return: None
    """

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        col("t.busi_date") == busi_date
    ).join(
        spark.table("ddw.t_cockpit_00114").alias("a"),
        (col("t.fund_account_id") == col("a.fund_account_id")) &
        lit(busi_date).between(col("a.begin_date"), col("a.end_date")),
        "inner"
    ).join(
        spark.table("edw.h12_fund_account").alias("b"),
        col("a.fund_account_id") == col("b.fund_account_id"),
        "inner"
    ).select(
        col("t.fund_account_id"),
        col("b.client_name"),
        col("t.rights").alias("end_rights"),
        (
            coalesce(col("t.transfee"), lit(0)) +
            coalesce(col("t.delivery_transfee"), lit(0)) +
            coalesce(col("t.strikefee"), lit(0)) -
            coalesce(col("t.market_transfee"), lit(0)) -
            coalesce(col("t.market_delivery_transfee"), lit(0)) -
            coalesce(col("t.market_strikefee"), lit(0))
        ).alias("remain_transfee")
    )

    df_result = tmp.alias("t").join(
        spark.table("ddw.t_cockpit_00114").alias("a"),
        col("t.fund_account_id") == col("a.fund_account_id"),
        "inner"
    ).select(
        lit(busi_date).alias("busi_date"),
        col("t.fund_account_id"),  # 资金账号
        col("b.client_name"),  # 客户名称
        col("t.end_rights"),  # 期末权益
        col("t.remain_transfee"),  # 留存手续费
        col("a.fund_rate"),  # 比例
        col("a.out_oa_branch_id"),
        col("a.out_oa_branch_name"),  # 划出部门
        col("a.in_oa_branch_id"),
        col("a.in_oa_branch_name"),  # 划入部门
        (col("t.end_rights") * col("a.fund_rate")).alias("allocat_end_rights"),  # 分配期末权益
        (col("t.remain_transfee") * col("a.fund_rate")).alias("allocat_remain_transfee")  # 分配留存手续费
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="cf_busimg.t_cockpit_00114_data",
        insert_mode="overwrite"
    )
