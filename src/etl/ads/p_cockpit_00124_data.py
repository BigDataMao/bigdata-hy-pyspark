# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00124_data(spark: SparkSession, busi_date: str):
    """
    投资咨询绩效提成分配表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    """
    入参条件
    月份
    客户编号/客户名称
    产品类型
    分配部门类型
    分配部门
    合同开始时间起始日
    合同开始时间结束日
    合同结束时间起始日
    合同结束时间结束日
    收款时间开始时间
    收款时间结束时间
    
    cf_busimg.t_cockpit_00122    投资咨询基本信息维护参数表-主表
    cf_busimg.t_cockpit_00122_1  投资咨询基本信息-内核分配比例-表1
    cf_busimg.t_cockpit_00122_2  投资咨询基本信息-绩效分配比例-表2
    """

    v_month_id = busi_date[:6]

    df_result = spark.table("ddw.t_cockpit_00122").alias("t") \
        .filter(
        col("t.busi_month") == v_month_id
    ).join(
        spark.table("ddw.t_cockpit_00122_2").alias("a"),
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
        col("t.contract_begin_date"),  # 合同开始时间
        col("t.contract_end_date"),  # 合同结束时间
        col("t.collection_time"),  # 收款时间
        col("t.invest_total_service_fee"),  # 投资咨询服务费总额(元)
        col("t.perfor_total_rate"),  # 绩效总分配比例
        col("a.alloca_oa_branch_type"),  # 分配部门类型
        col("a.alloca_oa_branch_id"),  # 分配部门
        col("a.alloca_oa_branch_name"),  # 分配部门
        col("a.alloca_perfor_rate"),  # 部门绩效分配比例
        col("a.broker_id"),  # 分配人员id
        col("a.broker_name"),  # 分配人员
        col("a.broker_perfor_rate"),  # 人员绩效分配比例
        (
            col("t.invest_total_service_fee") *
            col("t.perfor_total_rate") *
            col("a.alloca_perfor_rate") *
            col("a.broker_perfor_rate")
        ).alias("broker_income")  # 人员奖励 =投资咨询服务费总额(元)*绩效总分配比例*部门绩效分配比例*人员绩效分配比例
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00124_data",
        insert_mode="overwrite"
    )


