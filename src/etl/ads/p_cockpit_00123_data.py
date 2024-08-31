# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

from src.env.task_env import log
from src.utils.udf import f_get_multi_key_note


@log
def p_cockpit_00123_data(spark: SparkSession, busi_date: str):
    """
    投资咨询内核收入分配表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期 yyyyMMdd
    :return: None
    """

    udf_get_multi_key_note = spark.udf.register("udf_get_multi_key_note", f_get_multi_key_note, StringType())

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_00122").alias("t") \
        .filter(
        col("t.busi_month") == v_month_id
    ).join(
        spark.table("ddw.t_cockpit_00122_1").alias("a"),
        (col("t.busi_month") == col("a.busi_month")) &
        (col("t.client_id") == col("a.client_id")) &
        (col("t.product_name") == col("a.product_name")),
        "left"
    ).select(
        col("t.busi_month").alias("month_id"),  # 月份
        col("t.client_id"),  # 客户编号
        col("t.client_name"),  # 客户名称
        col("t.mobile"),  # 联系电话
        col("t.contract_number"),  # 合同编号
        col("t.product_name"),  # 产品名称
        col("t.product_type"),  # 产品类型
        col("t.product_risk_level"),  # 产品风险等级
        col("t.client_risk_level"),  # 客户风险等级
        udf_get_multi_key_note(col("t.product_risk_level"), lit("report.wh_risk_level")).alias("client_risk_level_name"),  # 产品风险等级名称
        col("t.contract_begin_date"),  # 合同开始时间
        col("t.contract_end_date"),  # 合同结束时间
        col("t.collection_time"),  # 收款时间
        col("t.invest_total_service_fee"),  # 投资咨询服务费总额(元)
        col("t.kernel_total_rate"),  # 内核总分配比例
        col("a.alloca_oa_branch_type"),  # 分配部门类型
        col("a.alloca_oa_branch_id"),  # 分配部门
        col("a.alloca_oa_branch_name"),  # 分配部门
        col("a.alloca_kernel_rate"),  # 部门内核分配比例
        (
            col("t.invest_total_service_fee") *
            col("t.kernel_total_rate") *
            col("a.alloca_kernel_rate")
        ).alias("alloca_income")
    )

    df_result.show(5, truncate=True)
