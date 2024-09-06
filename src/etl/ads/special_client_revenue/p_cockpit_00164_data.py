# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when, sum

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00164_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_特殊客户收入调整表-数据落地
    :param spark:
    :param busi_date:
    :return:
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    df_result = spark.table("ddw.t_cockpit_00156").alias("t") \
        .filter(
        col("t.month_id") == v_month_id
    ).crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela").alias("a")
    ).filter(
        (col("a.func_id") == v_op_object)
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("b"),
        (col("b.fee_type") == "1002") &
        (lit(busi_date) >= col("b.begin_date")) &
        (lit(busi_date) <= col("b.end_date")),
        "inner"
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
        col("t.department").alias("traceability_dept_id"),
        col("t.allocation_dept").alias("undertake_dept_id"),
        col("a.account_code").alias("account_code"),
        col("a.account_name").alias("account_name"),
        lit(sys_date).alias("allocated_date"),
        col("a.func_name").alias("allocated_project"),
        lit(None).alias("allocated_peoject_detail"),
        lit("admin").alias("allocated_user"),  # 默认admin
        col("b.para_value"),
        col("c.para_value"),
        col("d.para_value")
    ).agg(
        when(
            col("a.account_code") == "6021",  # 手续费及佣金收入
            sum(col("t.remain_transfee_money")) / (lit(1) + coalesce(col("c.para_value"), lit(0)))
        ).when(
            col("a.account_code") == "6111",  # 投资收益
            sum(col("t.jian_mian_money")) / (lit(1) + coalesce(col("c.para_value"), lit(0)))
        ).when(
            col("a.account_code") == "6403",  # 税金及附加
            (sum(col("t.jian_mian_money")) + sum(col("t.remain_transfee_money"))) /
            (lit(1) + coalesce(col("c.para_value"), lit(0))) *
            col("d.para_value")
        ).when(
            col("a.account_code") == "660199",  # 其他费用
            (sum(col("t.jian_mian_money")) + sum(col("t.remain_transfee_money"))) /
            (lit(1) + coalesce(col("c.para_value"), lit(0))) *
            col("b.para_value")
        ).otherwise(lit(0)).alias("allocated_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00164",
        insert_mode="overwrite"
    )
