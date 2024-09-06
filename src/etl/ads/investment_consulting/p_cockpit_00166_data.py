# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, concat, when, sum, coalesce

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00166_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_投资咨询内核表 -数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期 yyyyMMdd
    :return: None
    """

    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    v_month_id = busi_date[:6]
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    df_result = spark.table("ddw.t_cockpit_00123").alias("t") \
        .filter(
        (col("t.month_id") == v_month_id)
    ).crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela").alias("a").filter(
            col("a.func_id") == v_op_object
        )
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("c"),
        (col("c.fee_type") == "1004") &
        (lit(busi_date) >= col("c.begin_date")) &
        (lit(busi_date) <= col("c.end_date")),
        "inner"
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("d"),
        (col("d.fee_type") == "1006") &
        (lit(busi_date) >= col("d.begin_date")) &
        (lit(busi_date) <= col("d.end_date")),
        "inner"
    ).groupBy(
        col("t.month_id").alias("month_id"),
        col("a.traceability_dept_id").alias("traceability_dept_id"),
        col("a.traceability_dept").alias("traceability_dept"),
        col("t.alloca_oa_branch_id").alias("undertake_dept_id"),
        col("t.alloca_oa_branch_name").alias("undertake_dept"),
        col("a.account_code").alias("account_code"),
        col("a.account_name").alias("account_name"),
        col("t.contract_number").alias("contract_number"),
        col("a.func_name").alias("allocated_project"),
        lit(sys_date).alias("allocated_date"),
        concat(lit("合同编号"), col("contract_number")).alias("allocated_peoject_detail"),
        lit("admin").alias("allocated_user"),
        col("c.para_value"),
        col("d.para_value"),
    ).agg(
        when(
            col("a.account_code") == "6021",
            sum(col("t.alloca_income")) / (lit(1) + coalesce(col("c.para_value"), lit(0)))
        ).when(
            col("a.account_code") == "6403",
            sum(col("t.alloca_income")) / (lit(1) + coalesce(col("c.para_value"), lit(0))) * col("d.para_value")
        ).otherwise(lit(0)).alias("allocated_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00166",
        insert_mode="overwrite"
    )
