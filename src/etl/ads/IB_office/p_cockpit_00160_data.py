# -*- coding: utf-8 -*-
import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sum, concat, when

from src.env.task_env import return_to_hive, log


@log
def p_cockpit_00160_data(spark: SparkSession, busi_date: str):
    """
    溯源表模板_ib驻点收入调整表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    v_month_id = busi_date[:6]
    v_op_object = os.path.splitext(os.path.basename(__file__))[0].upper()
    sys_date = datetime.datetime.now().strftime("%Y%m%d")

    """
      v_sql := 'insert into CF_BUSIMG.T_COCKPIT_00160
    (month_id,
     traceability_dept_id,
     traceability_dept,
     undertake_dept_id,
     undertake_dept,
     account_code,
     account_name,
     allocated_money,
     allocated_date,
     allocated_project,
     allocated_peoject_detail)
    select t.month_id,
           a.traceability_dept_id,
           a.traceability_dept,
           t.ctp_branch_id,
           t.ctp_branch_name,
           a.account_code,
           a.account_name,' || v_sql_text ||
               'to_char(sysdate,''yyyymmdd''),
           a.func_name as allocated_project,
           null
      from cf_busimg.T_COCKPIT_00140 t,
           cf_busimg.t_cockpit_acount_func_rela a,
           CF_BUSIMG.T_COCKPIT_00202 c,
           CF_BUSIMG.T_COCKPIT_00202 d
     where t.month_id = ' || v_month_id ||
               ' and a.func_id = ''' || v_op_object || '''
        and a.account_code = ' || x.account_code ||
               'and c.fee_type = ''1004''
       and ' || i_busi_date || ' between c.begin_date and c.end_date
       and d.fee_type = ''1006''
       and ' || i_busi_date || ' between d.begin_date and d.end_date
     group by t.month_id,
              a.traceability_dept_id,
              a.traceability_dept,
              t.ctp_branch_id,
              t.ctp_branch_name,
              a.account_code,
              a.account_name,
              a.func_name,
              c.para_value,
              d.para_value ';
    """

    df_result = spark.table("ddw.t_cockpit_00140").alias("t") \
        .filter(
        col("t.month_id") == v_month_id
    ).crossJoin(
        spark.table("ddw.t_cockpit_acount_func_rela").alias("a")
    ).filter(
        col("a.func_id") == v_op_object
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("c"),
        (col("c.fee_type") == "1004") &
        (col("c.begin_date") <= lit(busi_date)) &
        (col("c.end_date") >= lit(busi_date)),
        "inner"
    ).join(
        spark.table("ddw.t_cockpit_00202").alias("d"),
        (col("d.fee_type") == "1006") &
        (col("d.begin_date") <= lit(busi_date)) &
        (col("d.end_date") >= lit(busi_date)),
        "inner"
    ).groupBy(
        col("t.month_id"),
        col("a.traceability_dept_id"),
        col("a.traceability_dept"),
        col("t.ctp_branch_id").alias("undertake_dept_id"),
        col("t.ctp_branch_name").alias("undertake_dept"),
        col("a.account_code"),
        col("a.account_name"),
        lit(sys_date).alias("allocated_date"),
        col("a.func_name").alias("allocated_project"),
        lit(None).alias("allocated_peoject_detail"),
        col("c.para_value"),
        col("d.para_value")
    ).agg(
        when(
            col("a.account_code") == "6011",  # 利息收入
            sum(col("t.ibzd_interest_clear_income"))
        ).when(
            col("a.account_code") == "6021",  # 手续费及佣金收入
            sum(col("t.ibzd_clear_remain_transfee")) / (lit(1) + coalesce(col("c.para_value"), lit(0)))
        ).when(
            col("a.account_code") == "6111",  # 投资收益
            sum(col("t.ibzd_market_reduct_income")) / (lit(1) + coalesce(col("c.para_value"), lit(0)))
        ).when(
            col("a.account_code") == "6403",  # 税金及附加
            sum(
                coalesce(col("t.ibzd_market_reduct_income"), lit(0)) +
                coalesce(col("t.ibzd_clear_remain_transfee"), lit(0))
            ) / col("d.para_value")
        ).otherwise(0).alias("allocated_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_00160",
        insert_mode="overwrite"
    )