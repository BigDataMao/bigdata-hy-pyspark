# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit

from src.env.task_env import log, return_to_hive


@log
def p_cockpit_00305_data(spark: SparkSession, busi_date: str):
    """
    其他产品-权益数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]

    df_tmp = spark.table("ddw.t_cockpit_00308").alias("t").filter(
        col("t.month_id") == v_month_id
    ).join(
        spark.table("ddw.t_cockpit_00309").alias("a"),
        (col("t.month_id") == col("a.month_id")) &
        (col("t.src_branch_id") == col("a.src_branch_id")) &
        (col("t.adjust_proj_id") == col("a.adjust_proj_id")),
        "left"
    ).select(
        col("t.adjust_proj_id").alias("adjust_proj_id"),
        col("t.adjust_proj").alias("adjust_proj"),
        col("t.src_branch_id").alias("src_branch_id"),
        col("t.src_branch_name").alias("src_branch_name"),
        col("a.branch_id").alias("branch_id"),
        col("a.branch_name").alias("branch_name"),
        col("a.assignee_id").alias("assignee_id"),
        col("a.assignee").alias("assignee"),
        col("a.rights_rate").alias("rights_rate"),
        col("a.done_rate").alias("done_rate"),
        col("a.income_rate").alias("income_rate"),
        col("t.avg_rights").alias("avg_rights"),
        col("t.end_rights").alias("end_rights"),
        col("t.begin_rights").alias("begin_rights"),
        col("t.done_amount").alias("done_amount"),
        col("t.done_money").alias("done_money"),
        col("t.remain_transfee").alias("remain_transfee")
    )

    dict_index = {
        "01": {
            "index_id": "01",
            "index_name": "日均权益",
            "index_value": "avg_rights",
            "index_rate": "rights_rate"
        },
        "02": {
            "index_id": "02",
            "index_name": "期末权益",
            "index_value": "end_rights",
            "index_rate": "rights_rate"
        },
        "03": {
            "index_id": "03",
            "index_name": "期初权益",
            "index_value": "begin_rights",
            "index_rate": "rights_rate"
        },
        "04": {
            "index_id": "04",
            "index_name": "成交手数",
            "index_value": "done_amount",
            "index_rate": "done_rate"
        },
        "05": {
            "index_id": "05",
            "index_name": "成交额",
            "index_value": "done_money",
            "index_rate": "done_rate"
        },
        "06": {
            "index_id": "06",
            "index_name": "留存手续费",
            "index_value": "remain_transfee",
            "index_rate": "income_rate"
        }
    }

    df_result = None

    for k, v in dict_index.items():
        df_for_combine = df_tmp.groupBy(
            lit(v_month_id).alias("month_id"),
            col("adjust_proj_id"),
            col("adjust_proj"),
            col("src_branch_id"),
            col("src_branch_name"),
            col("branch_id"),
            col("branch_name"),
            col("assignee_id"),
            col("assignee"),
            lit(v["index_id"]).alias("index_id"),
            lit(v["index_name"]).alias("index_name"),
        ).agg(
            sum(col(v["index_value"])).alias("index_value"),
            sum(col(v["index_value"]) * col(v["index_rate"])).alias("adjust_value")
        )

        if df_result is None:
            df_result = df_for_combine
        else:
            df_result = df_result.union(df_for_combine)

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00306",
        insert_mode="overwrite"
    )
