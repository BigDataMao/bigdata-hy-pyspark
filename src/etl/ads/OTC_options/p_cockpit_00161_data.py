# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sum, concat, when

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00161_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_场外期权清算台账-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    df_result = spark.table("ddw.t_cockpit_00208").alias("t") \
        .filter(
        col("t.add_date").substr(1, 6) == v_month_id
    ).crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela").alias("a")
    ).filter(
        col("a.func_id") == v_op_object
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("d"),
        (col("d.fee_type") == "1006") &
        (lit(busi_date) >= col("d.begin_date")) &
        (lit(busi_date) <= col("d.end_date")),
        "inner"
    ).groupBy(
        col("t.month_id"),
        col("a.traceability_dept_id"),
        col("a.traceability_dept"),
        col("t.branch_id").alias("undertake_dept_id"),
        col("t.branch_name").alias("undertake_dept"),
        col("a.account_code"),
        col("a.account_name"),
        col("d.para_value"),
        lit(sys_date).alias("allocated_date"),
        col("a.func_name").alias("allocated_project"),
        lit(None).alias("allocated_peoject_detail")
    ).agg(
        when(
            col("a.account_code") == "6051",  # 其他业务收入
            sum(col("t.total_income"))
        ).when(
            col("a.account_code") == "6403",  # 税金及附加
            sum(col("t.total_income")) * col("d.para_value")
        ).otherwise(lit(0)).alias("allocated_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00161",
        insert_mode="overwrite"
    )