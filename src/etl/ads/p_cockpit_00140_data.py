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
        (col("t.month_id") == v_month_id) &
        (col("t.is_main") == "1")
    ).join(
        spark.table("ods.t_ds_crm_broker_investor_rela").alias("a"),
        (col("t.oa_broker_id") == col("a.broker_id")) &
        (col("t.fund_account_id") == col("a.investor_id")) &
        (
                col("t.rela_type") ==
                when(col("a.broker_rela_typ") == "301", "居间关系")
                .when(col("a.broker_rela_typ") == "001", "开发关系")
                .when(col("a.broker_rela_typ") == "002", "服务关系")
                .when(col("a.broker_rela_typ") == "003", "维护关系")
                .otherwise("-")),
        "left"
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
        col("t.branch_id"),
        col("t.branch_name"),
        col("t.yes_rights").alias("begin_rights"),
        col("t.end_rights"),
        col("t.avg_rights"),
        col("t.remain_transfee"),
        (
                coalesce(col("t.ib_rebate"), lit(0)) +
                coalesce(col("t.ib_ret"), lit(0)) +
                coalesce(col("t.ib_interest"), lit(0))
        ).alias("ibzd_income"),
        (
                col("t.ib_interest") /
                when((col("a.data_pct") == 0) | (col("a.data_pct").isNull()), 1).otherwise(col("a.data_pct"))
        ).alias("interest_clear_income"),
        (
                col("t.ib_ret") /
                when((col("a.data_pct") == 0) | (col("a.data_pct").isNull()), 1).otherwise(col("a.data_pct"))
        ).alias("market_reduct_income"),
        (
                col("t.ib_rebate") /
                when((col("a.data_pct") == 0) | (col("a.data_pct").isNull()), 1).otherwise(col("a.data_pct"))
        ).alias("clear_remain_transfee"),
        col("a.data_pct").alias("ibzd_income_reate"),
        col("t.ib_interest").alias("ibzd_interest_clear_income"),
        col("t.ib_ret").alias("ibzd_market_reduct_income"),
        col("t.ib_rebate").alias("ib_rebate")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00140",
        insert_mode="overwrite"
    )
