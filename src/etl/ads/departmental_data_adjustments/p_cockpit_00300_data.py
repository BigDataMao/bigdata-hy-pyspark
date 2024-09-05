# -*- coding: utf-8 -*-
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sum, concat, when

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00300_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_部门间数据调整-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = "P_COCKPIT_00165_DATA"
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    df_result = spark.table("ddw.t_cockpit_00114_data").alias("t") \
        .filter(
        col("t.busi_month").substr(1, 6) == v_month_id
    ).groupBy(
        col("t.busi_month").substr(1, 6).alias("month_id"),
        col("t.out_oa_branch_id").alias("traceability_dept_id"),
        col("t.out_oa_branch_name").alias("traceability_dept"),
        col("t.in_oa_branch_id").alias("UNDERTAKE_DEPT_ID"),
        col("t.in_oa_branch_name").alias("undertake_dept")
    ).agg(
        sum(col("allocat_remain_transfee")).alias("allocated_money")
    ).alias("t").crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela").alias("a")
    ).filter(
        (col("month_id") == v_month_id) &
        (col("func_id") == v_op_object)
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
        spark.table("ddw.t_cockpit_00202").alias("e"),
        (col("e.fee_type") == "1005") &
        (lit(busi_date) >= col("e.begin_date")) &
        (lit(busi_date) <= col("e.end_date")),
        "inner"
    ).select(
        col("t.month_id"),
        col("t.traceability_dept_id"),
        col("t.traceability_dept"),
        col("t.UNDERTAKE_DEPT_ID"),
        col("t.undertake_dept"),
        col("a.account_code"),
        col("a.account_name"),
        lit("admin").alias("allocated_user"),  # 默认admin
        when(
            col("a.account_code") == "6021",
            col("t.allocat_remain_transfee") * col("c.para_value")  # 手续费及佣金收入
        ).when(
            col("a.account_code") == "6403",
            col("t.allocat_remain_transfee") * col("e.para_value")  # 税金及附加
        ).when(
            col("a.account_code") == "660199",
            col("t.allocat_remain_transfee") * col("b.para_value")  # 其他费用
        ).otherwise(lit(0)).alias("allocated_money"),
        lit(sys_date).alias("allocated_date"),
        col("a.func_name").alias("allocated_project"),
        lit(None).alias("allocated_peoject_detail")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00300",
        insert_mode="overwrite"
    )