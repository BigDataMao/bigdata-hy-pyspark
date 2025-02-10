# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00140_data(spark: SparkSession, busi_date: str):
    """
    ib驻点收入调整表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_client_revenue").alias("t") \
        .filter(
        col("t.month_id") == v_month_id
    ).join(
        spark.table("ods.t_ds_crm_broker_investor_rela").alias("a"),
        (col("t.oa_broker_id") == col("a.broker_id")) &
        (col("t.fund_account_id") == col("a.investor_id")) &
        (
                col("t.rela_type") ==
                when(col("a.broker_rela_typ") == "301", lit("居间关系"))
                .when(col("a.broker_rela_typ") == "001", lit("开发关系"))
                .when(col("a.broker_rela_typ") == "002", lit("服务关系"))
                .when(col("a.broker_rela_typ") == "003", lit("维护关系"))
                .otherwise(lit("-"))),
        "left"
    ).join(
        spark.table("ddw.t_cockpit_00110").alias("b"),
        col("t.oa_broker_id") == col("b.broker_id"),
        "inner"
    ).filter(
        (col("a.broker_id").like("ZD%")) &
        (col("a.rela_sts") == "A") &
        (col("a.approve_sts") == "0")
    ).select(
        lit(v_month_id).alias("month_id"),
        lit("").alias("ib_branch_id"),
        lit("").alias("ib_branch_name"),
        col("t.fund_account_id"),
        col("t.client_name"),
        col("t.oa_broker_id").alias("broker_id"),
        col("t.oa_broker_name").alias("broker_name"),
        col("b.broker_branch_id").alias("oa_branch_id"),
        col("b.broker_branch_name").alias("oa_branch_name"),
        col("t.yes_rights").alias("begin_rights"),
        col("t.end_rights"),
        col("t.avg_rights"),
        col("t.clear_remain_transfee").alias("remain_transfee"),
        (
            (
                coalesce(col("t.NET_INTEREST_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_INTEREST"), lit(0))
            ) *
            (
                coalesce(col("a.data_pct"), lit(1))
            ) *
            (
                when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1))
            )
            +
            (
                coalesce(col("t.MARKET_RET_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_RET"), lit(0))
            ) *
            (
                coalesce(col("a.data_pct"), lit(1)) * when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1))
            )
            +
            (
                coalesce(col("t.clear_remain_transfee"), lit(0)) - coalesce(col("t.CSPERSON_REBATE"), lit(0))
            ) *
            (
                coalesce(col("a.data_pct"), lit(1)) * when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1))
            )
        ).alias("ibzd_income"),

        coalesce(col("t.NET_INTEREST_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_INTEREST"), lit(0)).alias("interest_clear_income"),

        coalesce(col("t.MARKET_RET_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_RET"), lit(0)).alias("market_reduct_income"),

        coalesce(col("t.clear_remain_transfee"), lit(0)) - coalesce(col("t.CSPERSON_REBATE"), lit(0)).alias("clear_remain_transfee"),

        coalesce(col("a.data_pct"), lit(1)).alias("ibzd_income_reate"),

        (coalesce(col("t.NET_INTEREST_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_INTEREST"), lit(0))) *
        coalesce(col("a.data_pct"), lit(1)) *
        when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1)).alias("ibzd_interest_clear_income"),

        (coalesce(col("t.MARKET_RET_REDUCE"), lit(0)) - coalesce(col("t.CSPERSON_RET"), lit(0))) *
        coalesce(col("a.data_pct"), lit(1)) *
        when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1)).alias("ibzd_market_reduct_income"),

        (coalesce(col("t.clear_remain_transfee"), lit(0)) - coalesce(col("t.CSPERSON_REBATE"), lit(0))) *
        coalesce(col("a.data_pct"), lit(1)) *
        when(col("t.branch_name").substr(1, 4) == "SWHY", lit(0.45)).otherwise(lit(1)).alias("ibzd_clear_remain_transfee"),

        col("t.done_amount"),
        col("t.done_money")
    )


    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00140",
        insert_mode="overwrite"
    )
