# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, concat

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00165_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_投资者保障基金 -数据落地
    :param spark:
    :param busi_date:
    :return:
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    df_result = spark.table("ddw.t_cockpit_00128_data").crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela")
    ).filter(
        (col("month_id") == v_month_id) &
        (col("func_id") == v_op_object)
    ).alias("t").join(
        spark.table("ddw.t_ctp_branch_oa_rela").alias("x"),
        col("t.ctp_branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).groupBy(
        col("t.month_id").alias("month_id"),
        col("t.traceability_dept_id").alias("traceability_dept_id"),
        col("t.traceability_dept").alias("traceability_dept"),
        col("t.oa_branch_id").alias("UNDERTAKE_DEPT_ID"),
        col("x.oa_branch_name").alias("undertake_dept"),
        col("t.account_code").alias("ACCOUNT_CODE"),
        col("t.account_name").alias("ACCOUNT_NAME"),
        lit("admin").alias("ALLOCATED_USER"),  # 默认admin
    ).agg(
        sum(col("bzjj")).alias("allocated_money"),
        lit(sys_date).alias("allocated_date"),
        concat(lit(v_month_id), col("t.account_name")).alias("allocated_project"),
        lit(None).alias("allocated_peoject_detail")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00165",
        insert_mode="overwrite"
    )



