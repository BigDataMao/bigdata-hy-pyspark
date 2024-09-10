# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, regexp_replace, when, round, lit, coalesce, row_number, expr

from src.env.config import Config
from src.env.task_env import update_dataframe, return_to_hive, log
from src.utils.date_utils import get_date_period_and_days, get_month_str, get_day_last_month
from src.utils.logger_uitls import to_color_str


@log
def p_cockpit_client_revenue(spark: SparkSession, busi_date: str):
    """
    客户创收台账 数据落地
    :param spark: spark执行环境
    :param busi_date: 日期字符串,格式:yyyymmdd
    :return: DataFrame
    """

    config = Config()
    logger = config.get_logger()
    # 1. 定义变量

    v_busi_month = busi_date[:6]
    v_ds_begin_busi_date = busi_date[:4] + "-" + busi_date[4:6] + "-01"
    v_last_month = get_month_str(v_busi_month, -1)
    v_last_ds_begin_busi_date = v_last_month[:4] + "-" + v_last_month[4:6] + "-01"

    v_begin_date, v_end_date, v_trade_days = get_date_period_and_days(
        busi_month=v_busi_month,
        begin_date='19000101',  # 1900-01-01,基于开始日期和结束日期进行过滤,所以这里设置为最小日期
        end_date=busi_date,
        is_trade_day=True
    )

    v_end_busi_date = get_date_period_and_days(
        busi_month=v_busi_month,
        is_trade_day=False
    )[1]

    v_ds_end_busi_date = v_end_busi_date[:4] + "-" + v_end_busi_date[4:6] + "-" + v_end_busi_date[6:8]

    v_last_ds_end_busi_date = get_day_last_month(v_end_busi_date, "%Y%m%d", "%Y-%m-%d")

    logger.info(
        f"v_busi_month: {v_busi_month}, v_ds_begin_busi_date: {v_ds_begin_busi_date}, "
        f"v_last_month: {v_last_month}, v_last_ds_begin_busi_date: {v_last_ds_begin_busi_date}, "
        f"v_begin_date: {v_begin_date}, v_end_date: {v_end_date}, "
        f"v_trade_days: {v_trade_days}, v_end_busi_date: {v_end_busi_date}, "
        f"v_ds_end_busi_date: {v_ds_end_busi_date}, v_last_ds_end_busi_date: {v_last_ds_end_busi_date}"
    )

    # 2. 业务逻辑

    # 先缓存基础数据
    df_investor = spark.table("ods.t_ds_dc_investor").select(["investor_id", "orig_department_id", "investor_nam"]).cache()  # 缓存
    df_org = spark.table("ods.t_ds_dc_org").cache()  # 缓存
    df_oa_rela = spark.table("ddw.t_ctp_branch_oa_rela").cache()  # 缓存
    df_202 = spark.table("ddw.t_cockpit_00202").cache()  # 缓存
    df_fund_account = spark.table("edw.h12_fund_account").select("fund_account_id", "branch_id").cache()  # 缓存

    df_investor_value = spark.table("ods.t_ds_adm_investor_value").filter(
        col("date_dt") == v_ds_begin_busi_date
    ).cache()  # 缓存

    # 2.1 基础数据
    """
    交易所返还收入
    减免返还收入”取自：crm系统的“内核表-投资者交易所返还计算-二次开发”字段“交易所减收”；
    """

    logger.info(to_color_str("开始计算交易所返还收入", "green"))
    tmp = spark.table("ods.t_ds_ret_exchange_retfee2").alias("a") \
        .filter(
        (
                (col("tx_dt").between(v_last_ds_begin_busi_date, v_last_ds_end_busi_date)) |
                (col("tx_dt").between(v_ds_begin_busi_date, v_ds_end_busi_date))
        )
    ).join(
        df_org.alias("b"),
        col("a.orig_department_id") == col("b.department_id")
    ).join(
        df_investor.alias("ff"),
        col("a.investor_id") == col("ff.investor_id")
    ).groupBy(
        col("a.investor_id").alias("fund_account_id"),
        regexp_replace("a.tx_dt", "-", "").alias("busi_date")
    ).agg(
        sum(
            round(
                when(
                    col("a.tx_dt") >= v_ds_begin_busi_date,
                    col("a.exchange_txfee_amt")
                ).otherwise(0), 2)
        ).alias("exchange_txfee_amt"),
        sum(
            round(
                when(
                    col("a.tx_dt") >= v_ds_begin_busi_date,
                    col("a.ret_fee_amt_tx")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_czce")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_czce"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce"),
        sum(
            round(
                when(
                    col("a.tx_dt") >= v_ds_begin_busi_date,
                    col("a.ret_fee_amt_shfe")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_shfe"),
        sum(
            round(
                when(
                    (col("a.tx_dt") < '2022-05-01') &
                    (col("a.tx_dt") <= v_last_ds_end_busi_date),
                    col("a.ret_fee_amt_shfe1")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_shfe1"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_cffex")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_cffex"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_cffex2021")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_cffex2021"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce31")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce31"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce32")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce32"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce33")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce33"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce1")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce1"),
        sum(
            round(
                when(
                    col("a.tx_dt") <= v_last_ds_end_busi_date,
                    col("a.ret_fee_amt_dce2")
                ).otherwise(0), 4)
        ).alias("ret_fee_amt_dce2"),
        sum(
            col("a.investor_ret_amt")
        ).alias("investor_ret_amt")
    )

    df_tmp_1 = tmp.alias("a").groupBy(
        col("a.fund_account_id").alias("fund_account_id"),
        col("a.busi_date").alias("busi_date")
    ).agg(
        sum("exchange_txfee_amt").alias("exchange_txfee_amt"),
        sum("ret_fee_amt").alias("ret_fee_amt"),
        sum("ret_fee_amt_czce").alias("ret_fee_amt_czce"),
        sum("ret_fee_amt_dce").alias("ret_fee_amt_dce"),  # 大连近月
        sum("ret_fee_amt_cffex").alias("ret_fee_amt_cffex"),
        sum("ret_fee_amt_cffex2021").alias("ret_fee_amt_cffex2021"),
        sum("ret_fee_amt_dce31").alias("ret_fee_amt_dce31"),
        sum("ret_fee_amt_dce32").alias("ret_fee_amt_dce32"),
        sum("ret_fee_amt_dce33").alias("ret_fee_amt_dce33"),
        sum(
            round("ret_fee_amt_dce1", 4)
        ).alias("ret_fee_amt_dce1"),
        sum(
            round("ret_fee_amt_dce2", 4)
        ).alias("ret_fee_amt_dce2"),
        sum(
            round("ret_fee_amt_shfe", 4)
        ).alias("ret_fee_amt_shfe"),
        sum(
            round("ret_fee_amt_shfe1", 4)
        ).alias("ret_fee_amt_shfe1"),
        sum("investor_ret_amt").alias("investor_ret_amt")
    ).withColumn(
        "order_seq",
        lit(1)
    ).fillna(0).select(
        col("busi_date"),
        col("fund_account_id"),
        (
                col("ret_fee_amt") +
                col("ret_fee_amt_czce") +
                col("ret_fee_amt_dce") +
                col("ret_fee_amt_cffex") +
                col("ret_fee_amt_cffex2021") +
                col("ret_fee_amt_shfe") +
                col("ret_fee_amt_shfe1") +
                col("ret_fee_amt_dce1") +
                col("ret_fee_amt_dce2") +
                col("ret_fee_amt_dce31") +
                col("ret_fee_amt_dce32") +
                col("ret_fee_amt_dce33")
        ).alias("market_reduct")  # 交易所减收
    )

    # 交易所返还支出 TODO 该条基本上没有记录,为空的df,所以后面的需要coalesce处理occur_money
    logger.info(to_color_str("开始计算交易所返还支出", "green"))
    df_tmp_2 = spark.table("edw.h14_fund_jour").alias("a") \
        .filter(
        (col("a.fund_type") == "3") &
        (col("a.fund_direct") == "1") &
        (col("a.busi_date").between(v_begin_date, v_end_date))
    ).groupBy(
        col("a.fund_account_id").alias("fund_account_id"),
        col("a.busi_date").alias("busi_date")
    ).agg(
        sum("a.occur_money").alias("occur_money")
    ).fillna(0)

    # 交易所净返还（扣客户交返）=交易所返还收入-交易所返还支出
    logger.info(to_color_str("开始计算交易所净返还", "green"))
    df_tmp_3 = df_tmp_1.alias("t").join(
        df_tmp_2.alias("t1"),
        (col("t.fund_account_id") == col("t1.fund_account_id")) &
        (col("t.busi_date") == col("t1.busi_date")),
        "left"
    ).join(
        df_fund_account.alias("t2"),
        col("t.fund_account_id") == col("t2.fund_account_id"),
        "left"
    ).join(
        df_oa_rela.alias("x"),
        col("t2.branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).join(
        df_202.alias("c"),
        expr("instr(c.branch_id, x.oa_branch_id) > 0") &
        (col("c.fee_type") == "1004") &
        (col("t.busi_date").between(col("c.begin_date"), col("c.end_date"))),
        "left"
    ).join(
        df_202.alias("d"),
        expr("instr(d.branch_id, x.oa_branch_id) > 0") &
        (col("d.fee_type") == "1005") &
        (col("t.busi_date").between(col("d.begin_date"), col("d.end_date"))),
        "left"
    ).join(
        df_202.alias("e"),
        expr("instr(e.branch_id, x.oa_branch_id) > 0") &
        (col("e.fee_type") == "1002") &
        (col("t.busi_date").between(col("e.begin_date"), col("e.end_date"))),
        "left"
    ).select(
        col("t.busi_date").alias("busi_date"),
        col("t.fund_account_id").alias("fund_account_id"),
        (
                col("t.MARKET_REDUCT") - coalesce(col("t1.occur_money"), lit(0))
        ).alias("market_ret_reduce"),
        (
                (col("t.MARKET_REDUCT") - coalesce(col("t1.occur_money"), lit(0))) /
                (1 + coalesce(col("c.para_value"), lit(0)))
        ).alias("market_ret_reduce_after_tax"),
        (
                (col("t.MARKET_REDUCT") - coalesce(col("t1.occur_money"), lit(0))) *
                coalesce(col("d.para_value"), lit(0))
        ).alias("market_ret_add_tax"),
        (
                (col("t.MARKET_REDUCT") - coalesce(col("t1.occur_money"), lit(0))) *
                coalesce(col("e.para_value"), lit(0))
        ).alias("market_ret_risk_fund")
    ).groupBy(
        col("fund_account_id").alias("fund_account_id")
    ).agg(
        sum("market_ret_reduce").alias("market_ret_reduce"),
        sum("market_ret_reduce_after_tax").alias("market_ret_reduce_after_tax"),
        sum("market_ret_add_tax").alias("market_ret_add_tax"),
        sum("market_ret_risk_fund").alias("market_ret_risk_fund")
    ).fillna(0)

    # 自然日均可用资金*年利率*统计周期内自然天数/年天数——资金对账表对应数据+系统内维护的年利率计算得到
    logger.info(to_color_str("开始计算自然日均可用资金", "green"))
    df_tmp_4 = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        df_fund_account.alias("t2"),
        col("t.fund_account_id") == col("t2.fund_account_id"),
        "left"
    ).join(
        df_oa_rela.alias("x"),
        col("t2.branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).groupBy(
        col("t.busi_date").alias("busi_date"),
        col("t.fund_account_id").alias("fund_account_id"),
        col("x.oa_branch_id").alias("oa_branch_id")
    ).agg(
        (
            sum("t.rights") -
            sum(
                when(
                    col("t.impawn_money") > col("t.margin"),
                    col("t.impawn_money")
                ).otherwise(col("t.margin"))
            )
        ).alias("interest_base")
    ).fillna(0).alias("t").join(
        spark.table("ddw.t_cockpit_date_nature").alias("b"),
        col("t.busi_date") == col("b.busi_date"),
        "inner"
    ).join(
        df_202.alias("c"),
        expr("instr(c.branch_id, t.oa_branch_id) > 0") &
        (col("t.busi_date").between(col("c.begin_date"), col("c.end_date"))) &
        (col("c.fee_type") == "1001"),
        "left"
    ).groupBy(
        col("t.busi_date").alias("busi_date"),
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum("t.interest_base").alias("interest_base"),
        (sum(col("t.interest_base") * coalesce(col("c.para_value"), lit(0))) / 360).alias("interest_income")
    ).fillna(0)

    # 利息收入
    # 利息收入_不含税
    # 利息收入_增值税及附加
    # 利息收入_风险金
    # 自然日均可用
    logger.info(to_color_str("开始计算利息收入", "green"))
    df_tmp_5 = df_tmp_4.alias("t").join(
        df_fund_account.alias("t2"),
        col("t.fund_account_id") == col("t2.fund_account_id"),
        "left"
    ).join(
        df_oa_rela.alias("x"),
        col("t2.branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).join(
        df_202.alias("c"),
        expr("instr(c.branch_id, x.oa_branch_id) > 0") &
        (col("t.busi_date").between(col("c.begin_date"), col("c.end_date"))) &
        (col("c.fee_type") == "1004"),
        "left"
    ).join(
        df_202.alias("d"),
        expr("instr(d.branch_id, x.oa_branch_id) > 0") &
        (col("t.busi_date").between(col("d.begin_date"), col("d.end_date"))) &
        (col("d.fee_type") == "1005"),
        "left"
    ).join(
        df_202.alias("e"),
        expr("instr(e.branch_id, x.oa_branch_id) > 0") &
        (col("t.busi_date").between(col("e.begin_date"), col("e.end_date"))) &
        (col("e.fee_type") == "1002"),
        "left"
    ).groupBy(
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum("t.interest_income").alias("accrued_interest"),
        sum(col("t.interest_income") / (1 + coalesce(col("c.para_value"), lit(0)))).alias("accrued_interest_after_tax"),
        sum(col("t.interest_income") * coalesce(col("d.para_value"), lit(0))).alias("accrued_interest_add_tax"),
        sum(col("t.interest_income") * coalesce(col("e.para_value"), lit(0))).alias("accrued_interest_risk_fund"),
        sum("t.interest_base").alias("avg_open_pre_nature")
    ).fillna(0)

    # 资金账号相关数据：
    # 软件使用费
    # 留存手续费
    # 交易所返还
    # 留存手续费_不含税
    # 留存_增值税及附加
    # 留存_风险金
    # 利息积数
    # 客户结息
    # 客户结息_不含税
    # 客户结息_增值税及附加
    # 客户结息_风险金
    # 其他收入
    # 其他收入_不含税
    # 其他收入增值税及附加
    # 其他收入风险金
    # 客户交返
    # 客户手续费返还 = 保留字段，目前值为0
    logger.info(to_color_str("开始计算资金账号相关数据", "green"))
    df_tmp_6 = df_investor_value.alias("t").join(
        df_investor.alias("b"),
        col("t.investor_id") == col("b.investor_id"),
        "inner"
    ).join(
        df_oa_rela.alias("x"),
        col("b.orig_department_id") == col("x.ctp_branch_id"),
        "inner"
    ).join(
        df_202.alias("c"),
        expr("instr(c.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("t.date_dt"), "-", "").between(col("c.begin_date"), col("c.end_date"))) &
        (col("c.fee_type") == "1004"),
        "left"
    ).join(
        df_202.alias("d"),
        expr("instr(d.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("t.date_dt"), "-", "").between(col("d.begin_date"), col("d.end_date"))) &
        (col("d.fee_type") == "1005"),
        "left"
    ).join(
        df_202.alias("e"),
        expr("instr(e.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("t.date_dt"), "-", "").between(col("e.begin_date"), col("e.end_date"))) &
        (col("e.fee_type") == "1002"),
        "left"
    ).groupBy(
        col("t.investor_id").alias("fund_account_id")
    ).agg(
        round(sum("t.soft_amt"), 2).alias("transfee_reward_soft"),
        round(sum("t.subsistence_fee_amt"), 2).alias("remain_transfee"),
        round(sum("t.exchangeret_amt"), 2).alias("market_ret"),
        round(sum(col("t.subsistence_fee_amt") / (lit(1) + coalesce(col("c.para_value"), lit(0)))), 2).alias("remain_transfee_after_tax"),
        round(sum(col("t.subsistence_fee_amt") * coalesce(col("d.para_value"), lit(0))), 2).alias("remain_transfee_add_tax"),
        round(sum(col("t.subsistence_fee_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("remain_risk_fund"),
        round(sum("t.calint_amt"), 2).alias("interest_base"),
        round(sum("t.i_int_amt"), 2).alias("client_interest_settlement"),
        round(sum(col("t.i_int_amt") / (lit(1) + coalesce(col("c.para_value"), lit(0)))), 2).alias("client_interest_after_tax"),
        round(sum(col("t.i_int_amt") * coalesce(col("d.para_value"), lit(0))), 2).alias("client_interest_add_tax"),
        round(sum(col("t.i_int_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("client_interest_risk_fund"),
        round(sum("t.oth_amt"), 2).alias("other_income"),
        round(sum(col("t.oth_amt") / (1 + coalesce(coalesce(col("c.para_value"), lit(0)), lit(0)))), 2).alias("other_income_after_tax"),
        round(sum(col("t.oth_amt") * coalesce(col("d.para_value"), lit(0))), 2).alias("other_income_add_tax"),
        round(sum(col("t.oth_amt") * coalesce(col("e.para_value"), lit(0)))).alias("other_income_risk_fund"),
        round(sum("t.i_exchangeret_amt"), 2).alias("market_ret_client"),
        lit(0).alias("transfee_reward_client")
    ).fillna(0)

    """
    净利息收入（扣客户结息）=利息收入-客户结息   NET_INTEREST_REDUCE
    净利息收入（扣客户结息）_不含税  NET_INTEREST_REDUCE_AFTER_TAX
    利息增值税及附加  INTEREST_ADD_TAX
    利息风险金  INTEREST_RISK_FUND
    """
    logger.info(to_color_str("开始计算净利息收入", "green"))
    df_tmp_7 = df_fund_account.alias("x") \
        .join(
        df_tmp_5.alias("t"),
        col("x.fund_account_id") == col("t.fund_account_id"),
        "left"
    ).join(
        df_tmp_6.alias("t1"),
        col("x.fund_account_id") == col("t1.fund_account_id"),
        "left"
    ).select(
        col("x.fund_account_id").alias("fund_account_id"),
        (
                col("t.accrued_interest") - col("t1.client_interest_settlement")
        ).alias("net_interest_reduce"),
        (
                col("t.accrued_interest_after_tax") - col("t1.client_interest_after_tax")
        ).alias("net_interest_reduce_after_tax"),
        (
                col("t.accrued_interest_add_tax") - col("t1.client_interest_add_tax")
        ).alias("interest_add_tax"),
        (
                col("t.accrued_interest_risk_fund") - col("t1.client_interest_risk_fund")
        ).alias("interest_risk_fund")
    ).fillna(0)

    """
    业务人员相关数据：
    居间返佣
    居间返佣_不含税
    居间留存增值税附加税
    居间留存风险金
    
    居间交返
    居间交返_不含税
    居间交返增值税附加税
    居间交返风险金
    
    居间利息
    居间利息_不含税
    居间利息增值税附加税
    居间利息风险金
    
    IB返佣
    IB返佣_不含税
    IB留存增值税附加税
    IB留存风险金
    
    IB交返
    IB交返_不含税
    IB交返增值税附加税
    IB交返风险金
    
    IB利息
    IB利息_不含税
    IB利息增值税附加税
    IB利息风险金
    
    员工留存提成
    员工留存提成_不含税
    员工留存增值税附加税
    员工留存风险金
    
    员工交返
    员工交返_不含税
    员工交返增值税附加税
    员工交返风险金
    
    员工利息
    员工利息_不含税
    员工利息增值税附加税
    员工利息风险金
    
    其他支出
    其他支出_不含税
    其他支出增值税附加税
    其他支出风险金
    """
    logger.info(to_color_str("开始计算业务人员相关数据", "green"))
    df_tmp_8 = df_investor_value.alias("a").join(
        df_investor.alias("b"),
        col("a.investor_id") == col("b.investor_id"),
        "inner"
    ).join(
        df_org.alias("c"),
        col("b.orig_department_id") == col("c.department_id"),
        "inner"
    ).join(
        spark.table("ods.t_ds_adm_brokerdata_detail").alias("a2"),
        (col("a.date_dt") == col("a2.tx_dt")) &
        (col("a.investor_id") == col("a2.investor_id")) &
        (col("a2.rec_freq") == "M"),
        "left"
    ).join(
        df_oa_rela.alias("x"),
        col("b.orig_department_id") == col("x.ctp_branch_id"),
        "inner"
    ).join(
        df_202.alias("d"),
        expr("instr(d.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("a.date_dt"), "-", "").between(col("d.begin_date"), col("d.end_date"))) &
        (col("d.fee_type") == "1004"),  # 增值税税率
        "left"
    ).join(
        df_202.alias("e"),
        expr("instr(e.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("a.date_dt"), "-", "").between(col("e.begin_date"), col("e.end_date"))) &
        (col("e.fee_type") == "1006"),  # 增值税附加税税率
        "left"
    ).join(
        df_202.alias("f"),
        expr("instr(f.branch_id, x.oa_branch_id) > 0") &
        (regexp_replace(col("a.date_dt"), "-", "").between(col("f.begin_date"), col("f.end_date"))) &
        (col("f.fee_type") == "1002"),  # 风险金比例
        "left"
    ).groupBy(
        coalesce(col("a2.staff_id"), lit("-")).alias("oa_broker_id"),
        when(
            col("a2.srela_typ") == "301", "居间关系"
        ).when(
            col("a2.srela_typ") == "001", "开发关系"
        ).when(
            col("a2.srela_typ") == "002", "服务关系"
        ).when(
            col("a2.srela_typ") == "003", "维护关系"
        ).otherwise("-").alias("rela_type"),
        col("a.investor_id").alias("fund_account_id")
    ).agg(
        round(sum("a.broker_amt"), 2).alias("cs_person_rebate"),
        round(sum(col("a.broker_amt") / (1 + coalesce(col("d.para_value"), lit(0)))), 2).alias("cs_person_rebate_after_tax"),
        round(sum(col("a.broker_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("cs_person_remain_add_tax"),
        round(sum(col("a.broker_amt") * coalesce(col("f.para_value"), lit(0))), 2).alias("cs_person_remain_risk_fund"),
        round(sum("a.broker_eret_amt"), 2).alias("cs_person_ret"),
        round(sum(col("a.broker_eret_amt") / (1 + coalesce(col("d.para_value"), lit(0)))), 2).alias("cs_person_ret_after_tax"),
        round(sum(col("a.broker_eret_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("cs_person_ret_add_tax"),
        round(sum(col("a.broker_eret_amt") * coalesce(col("f.para_value"), lit(0))), 2).alias("cs_person_ret_risk_fund"),
        round(sum("a.broker_int_amt"), 2).alias("cs_person_interest"),
        round(sum(col("a.broker_int_amt") / (1 + coalesce(col("d.para_value"), lit(0)))), 2).alias("cs_person_interest_after_tax"),
        round(sum(col("a.broker_int_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("cs_person_interest_add_tax"),
        round(sum(col("a.broker_int_amt") * coalesce(col("f.para_value"), lit(0))), 2).alias("cs_person_interest_risk_fund"),
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_amt")
            ).otherwise(0)
        ), 2).alias("ib_rebate"),  # IB返佣
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_amt") / (1 + coalesce(col("d.para_value"), lit(0)))
            ).otherwise(0)
        ), 2).alias("ib_rebate_after_tax"),  # IB返佣_不含税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_amt") * coalesce(col("e.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_rebate_add_tax"),  # IB留存增值税附加税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_amt") * coalesce(col("f.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_rebate_risk_fund"),  # IB留存风险金
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_eret_amt")
            ).otherwise(0)
        ), 2).alias("ib_ret"),  # IB交返
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_eret_amt") / (1 + coalesce(col("d.para_value"), lit(0)))
            ).otherwise(0)
        ), 2).alias("ib_ret_after_tax"),  # IB交返_不含税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_eret_amt") * coalesce(col("e.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_ret_add_tax"),  # IB交返增值税附加税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_eret_amt") * coalesce(col("f.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_ret_risk_fund"),  # IB交返风险金
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_int_amt")
            ).otherwise(0)
        ), 2).alias("ib_interest"),  # IB利息
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_int_amt") / (1 + coalesce(col("d.para_value"), lit(0)))
            ).otherwise(0)
        ), 2).alias("ib_interest_after_tax"),  # IB利息_不含税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_int_amt") * coalesce(col("e.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_interest_add_tax"),  # IB利息增值税附加税
        round(sum(
            when(
                col("a2.staff_id").like("ZD%"),
                col("a.broker_int_amt") * coalesce(col("f.para_value"), lit(0))
            ).otherwise(0)
        ), 2).alias("ib_interest_risk_fund"),  # IB利息风险金
        round(sum(
            coalesce(col("a2.ib_amt"), col("a.staff_amt"))
        ), 2).alias("staff_remain_comm"),  # 员工留存提成
        round(sum(
            coalesce(col("a2.ib_amt"), col("a.staff_amt")) / (1 + coalesce(col("d.para_value"), lit(0)))
        ), 2).alias("staff_remain_comm_after_tax"),  # 员工留存提成_不含税
        round(sum(
            coalesce(col("a2.ib_amt"), col("a.staff_amt")) * coalesce(col("e.para_value"), lit(0))
        ), 2).alias("staff_remain_comm_add_tax"),  # 员工留存增值税附加税
        round(sum(
            coalesce(col("a2.ib_amt"), col("a.staff_amt")) * coalesce(col("f.para_value"), lit(0))
        ), 2).alias("staff_remain_comm_risk_fund"),  # 员工留存风险金
        round(sum(
            col("a.staff_eret_amt")
        ), 2).alias("staff_ret"),  # 员工交返
        round(sum(
            col("a.staff_eret_amt") / (1 + coalesce(col("d.para_value"), lit(0)))
        ), 2).alias("staff_ret_after_tax"),  # 员工交返_不含税
        round(sum(
            col("a.staff_eret_amt") * coalesce(col("e.para_value"), lit(0))
        ), 2).alias("staff_ret_add_tax"),  # 员工交返增值税附加税
        round(sum(
            col("a.staff_eret_amt") * coalesce(col("f.para_value"), lit(0))
        ), 2).alias("staff_ret_risk_fund"),  # 员工交返风险金
        round(sum(
            coalesce(col("a2.staff_int_amt"), col("a.staff_int_amt"))
        ), 2).alias("staff_interest"),  # 员工利息
        round(sum(
            coalesce(col("a2.staff_int_amt"), col("a.staff_int_amt")) / (1 + coalesce(col("d.para_value"), lit(0)))
        ), 2).alias("staff_interest_after_tax"),  # 员工利息_不含税
        round(sum(
            coalesce(col("a2.staff_int_amt"), col("a.staff_int_amt")) * coalesce(col("e.para_value"), lit(0))
        ), 2).alias("staff_interest_add_tax"),  # 员工利息增值税附加税
        round(sum(
            coalesce(col("a2.staff_int_amt"), col("a.staff_int_amt")) * coalesce(col("f.para_value"), lit(0))
        ), 2).alias("staff_interest_risk_fund"),  # 员工利息风险金
        round(sum("a.i_oth_amt"), 2).alias("other_pay"),  # 其他支出
        round(sum(col("a.i_oth_amt") / (1 + coalesce(col("d.para_value"), lit(0)))), 2).alias("other_pay_after_tax"),  # 其他支出_不含税
        round(sum(col("a.i_oth_amt") * coalesce(col("e.para_value"), lit(0))), 2).alias("other_pay_add_tax"),  # 其他支出增值税附加税
        round(sum(col("a.i_oth_amt") * coalesce(col("f.para_value"), lit(0))), 2).alias("other_pay_risk_fund")  # 其他支出风险金
    ).fillna(0)

    logger.info(to_color_str("基础数据计算结束", "green"))

    # 初始化数据
    logger.info(to_color_str("开始初始化数据", "green"))
    df_result = df_investor_value.alias("a").join(
        df_investor.alias("b"),
        col("a.investor_id") == col("b.investor_id"),
        "inner"
    ).join(
        df_org.alias("c"),
        col("b.orig_department_id") == col("c.department_id"),
        "inner"
    ).join(
        spark.table("ods.t_ds_adm_brokerdata_detail").alias("a2"),
        (col("a.date_dt") == col("a2.tx_dt")) &
        (col("a.investor_id") == col("a2.investor_id")) &
        (col("a2.rec_freq") == "M"),
        "left"
    ).select(
        lit(v_busi_month).alias("month_id"),
        col("c.department_id").alias("branch_id"),
        col("c.department_nam").alias("branch_name"),
        coalesce(col("a2.staff_id"), lit("-")).alias("oa_broker_id"),
        coalesce(col("a2.staff_nam"), lit("-")).alias("oa_broker_name"),
        when(
            col("a2.srela_typ") == "301", "居间关系"
        ).when(
            col("a2.srela_typ") == "001", "开发关系"
        ).when(
            col("a2.srela_typ") == "002", "服务关系"
        ).when(
            col("a2.srela_typ") == "003", "维护关系"
        ).otherwise("-").alias("rela_type"),
        coalesce(col("a2.Broker_Nam"), lit("-")).alias("csperson_name"),
        col("a.investor_id").alias("fund_account_id"),
        col("b.investor_nam").alias("client_name")
    ).distinct().withColumn(
        "is_main",
        row_number().over(
            Window.partitionBy("fund_account_id").orderBy("fund_account_id")
        )
    )

    # # 写入结果表,并重新加载,已获得完整的表结构
    # logger.info(to_color_str("开始初始化结果表", "green"))
    # return_to_hive(
    #     spark=spark,
    #     df_result=df_result,
    #     target_table="ddw.t_cockpit_client_revenue",
    #     insert_mode="overwrite"
    # )
    #
    # df_result = spark.table("ddw.t_cockpit_client_revenue").alias("t").filter(
    #     col("t.month_id") == v_busi_month
    # )

    # df_result不具备ddw.t_cockpit_client_revenue完整的表结构,需要读取ddw.t_cockpit_client_revenue的schema

    result_columns = df_result.schema.names
    revenue_columns = spark.table("ddw.t_cockpit_client_revenue").columns
    select_exprs = [col(col_name).alias(col_name) for col_name in result_columns] + \
        [lit(None).alias(col_name) for col_name in revenue_columns if col_name not in result_columns]
    df_result = df_result.select(*select_exprs)

    """
    期初权益 YES_RIGHTS
    期末权益 END_RIGHTS
    日均权益 AVG_RIGHTS
    总盈亏 TOTAL_PROFIT
    手续费 TRANSFEE
    上交手续费 MARKET_TRANSFEE
    """
    logger.info(to_color_str("开始merge期初权益", "green"))
    df_merge_source = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        col("t.busi_date").between(v_begin_date, v_end_date)
    ).groupBy(
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum(
            when(
                col("t.busi_date") == v_begin_date,
                col("t.yes_rights")
            ).otherwise(0)
        ).alias("yes_rights"),
        sum(
            when(
                col("t.busi_date") == v_end_date,
                col("t.rights")
            ).otherwise(0)
        ).alias("end_rights"),
        sum(
            when(
                lit(v_trade_days) > 0,
                col("t.rights") / v_trade_days
            ).otherwise(0)
        ).alias("avg_rights"),
        sum(col("t.hold_profit") + col("t.close_profit")).alias("total_profit"),
        sum(
            col("t.transfee") + col("t.delivery_transfee") + col("t.strikefee")
        ).alias("transfee"),
        sum(
            col("t.market_transfee") + col("t.market_delivery_transfee") + col("t.market_strikefee")
        ).alias("market_transfee")
    ).fillna(0)

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "yes_rights", "end_rights", "avg_rights", "total_profit",
            "transfee", "market_transfee"
        ]
    )

    """
    成交金额  DONE_MONEY
    成交手数  DONE_AMOUNT
    保障基金 =成交金额*投资者保障基金比例 SECU_FEE
    """
    logger.info(to_color_str("开始merge成交金额", "green"))
    df_merge_source = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        col("t.busi_date").between(v_begin_date, v_end_date)
    ).join(
        df_oa_rela.alias("x"),
        col("t.branch_id") == col("x.ctp_branch_id"),
        "inner"
    ).join(
        df_202.alias("c"),
        expr("instr(c.branch_id, x.oa_branch_id) > 0") &
        (col("t.busi_date").between(col("c.begin_date"), col("c.end_date"))) &
        (col("c.fee_type") == "1003"),
        "left"
    ).withColumn(
        "tmp_secu_fee",
        col("t.done_sum") * coalesce(col("c.para_value"), lit(0))
    ).groupBy(
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum(col("t.done_amt")).alias("done_amount"),
        sum(col("t.done_sum")).alias("done_money"),
        sum(col("tmp_secu_fee")).alias("secu_fee")
    ).fillna(0)

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "done_money", "done_amount", "secu_fee"
        ]
    )

    """
    软件使用费  TRANSFEE_REWARD_SOFT
    留存手续费  REMAIN_TRANSFEE
    交易所返还   MARKET_RET

    留存手续费_不含税=留存手续费/（1+增值税税率） REMAIN_TRANSFEE_AFTER_TAX
    留存增值税及附加=留存手续费*增值税及附加税税率 REMAIN_TRANSFEE_ADD_TAX
    留存风险金=留存手续费*风险金率  REMAIN_RISK_FUND

    利息积数 INTEREST_BASE

    客户结息  CLIENT_INTEREST_SETTLEMENT
    客户交返  MARKET_RET_CLIENT
    客户手续费返还 TRANSFEE_REWARD_CLIENT
    客户返还汇总=客户手续费返还+客户交返+客户结息 TOTLA_CLIENT_RET


    其他收入  OTHER_INCOME
    其他收入_不含税 OTHER_INCOME_AFTER_TAX
    其他收入增值税及附加 OTHER_INCOME_ADD_TAX
    其他收入风险金 OTHER_INCOME_RISK_FUND
    """
    logger.info(to_color_str("开始merge软件使用费", "green"))
    df_merge_source = df_tmp_6.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.transfee_reward_soft"),
        col("t.remain_transfee"),
        col("t.remain_transfee_after_tax"),
        col("t.remain_transfee_add_tax"),
        col("t.remain_risk_fund"),
        col("t.interest_base"),
        col("t.market_ret"),
        col("t.client_interest_settlement"),
        col("t.market_ret_client"),
        col("t.transfee_reward_client"),
        (
                col("t.transfee_reward_client") +
                col("t.market_ret_client") +
                col("t.client_interest_settlement")
        ).alias("totla_client_ret"),
        col("t.other_income"),
        col("t.other_income_after_tax"),
        col("t.other_income_add_tax"),
        col("t.other_income_risk_fund")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "transfee_reward_soft", "remain_transfee", "remain_transfee_after_tax",
            "remain_transfee_add_tax", "remain_risk_fund", "interest_base",
            "market_ret", "client_interest_settlement", "market_ret_client",
            "transfee_reward_client", "totla_client_ret", "other_income",
            "other_income_after_tax", "other_income_add_tax", "other_income_risk_fund"
        ]
    )

    """
    交易所净返还（扣客户交返）
    交易所总减免=交易所返还收入-交易所返还支出
    减免返还收入”取自：crm系统的“内核表-投资者交易所返还计算-二次开发”字段“交易所减收”；
    “减免返还支出”取自：crm系统的“客户出入金流水”，筛选字段“资金类型”为公司调整后，取字段“入金金额”

    交易所净返还（扣客户交返）=交易所返还收入-交易所返还支出    MARKET_RET_REDUCE
    交易所净返还（扣客户交返）_不含税  MARKET_RET_REDUCE_AFTER_TAX
    交返增值税及附加 MARKET_RET_ADD_TAX
    交返风险金 MARKET_RET_RISK_FUND
    """
    logger.info(to_color_str("开始merge交易所净返还", "green"))
    df_merge_source = df_tmp_3.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.market_ret_reduce"),
        col("t.market_ret_reduce_after_tax"),
        col("t.market_ret_add_tax"),
        col("t.market_ret_risk_fund")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "market_ret_reduce", "market_ret_reduce_after_tax",
            "market_ret_add_tax", "market_ret_risk_fund"
        ]
    )

    """
    自然日均可用  AVG_OPEN_PRE_NATURE
    应计利息  ACCRUED_INTEREST
    """
    logger.info(to_color_str("开始merge自然日均可用", "green"))
    df_merge_source = df_tmp_5.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.avg_open_pre_nature"),
        col("t.accrued_interest")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "avg_open_pre_nature", "accrued_interest"
        ]
    )

    """
    净利息收入（扣客户结息）=利息收入-客户结息   NET_INTEREST_REDUCE
    净利息收入（扣客户结息）_不含税  NET_INTEREST_REDUCE_AFTER_TAX
    利息增值税及附加  INTEREST_ADD_TAX
    利息风险金  INTEREST_RISK_FUND
    """
    logger.info(to_color_str("开始merge净利息收入", "green"))
    df_merge_source = df_tmp_7.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.net_interest_reduce"),
        col("t.net_interest_reduce_after_tax"),
        col("t.interest_add_tax"),
        col("t.interest_risk_fund")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "net_interest_reduce", "net_interest_reduce_after_tax",
            "interest_add_tax", "interest_risk_fund"
        ]
    )

    """
    经纪业务总收入=留存手续费+交易所返还+应计利息 TOTAL_INCOME
    """
    logger.info(to_color_str("开始merge经纪业务总收入", "green"))
    df_merge_source = df_fund_account.alias("x") \
        .join(
        df_tmp_6.alias("b"),
        col("x.fund_account_id") == col("b.fund_account_id"),
        "left"
    ).join(
        df_tmp_5.alias("c"),
        col("x.fund_account_id") == col("c.fund_account_id"),
        "left"
    ).select(
        col("x.fund_account_id"),
        (
                col("b.remain_transfee") + col("b.market_ret") +
                col("c.accrued_interest")
        ).alias("total_income")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "total_income"
        ]
    )

    """
    TOTAL_INCOME_AFTER_TAX 经纪业务总净收入_不含税=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]/(1+增值税税率)
    TOTAL_INCOME_ADD_TAX   经纪业务增值税及附加=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]*（1+增值税税率）
    TOTAL_INCOME_RISK_FUND 经纪业务风险金=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]*风险金比例
    """
    logger.info(to_color_str("开始merge经纪业务总净收入", "green"))
    df_merge_source = df_fund_account.alias("x") \
        .join(
        df_tmp_6.alias("t"),
        col("x.fund_account_id") == col("t.fund_account_id"),
        "left"
    ).join(
        df_tmp_3.alias("t1"),
        col("x.fund_account_id") == col("t1.fund_account_id"),
        "left"
    ).join(
        df_tmp_7.alias("t2"),
        col("x.fund_account_id") == col("t2.fund_account_id"),
        "left"
    ).select(
        col("x.fund_account_id").alias("fund_account_id"),
        (
                coalesce(col("t.REMAIN_TRANSFEE_AFTER_TAX"), lit(0)) +
                coalesce(col("t1.MARKET_RET_REDUCE_AFTER_TAX"), lit(0)) +
                coalesce(col("t2.NET_INTEREST_REDUCE_AFTER_TAX"), lit(0))
        ).alias("total_income_after_tax"),  # 经纪业务总净收入_不含税
        (
                coalesce(col("t.REMAIN_TRANSFEE_ADD_TAX"), lit(0)) +
                coalesce(col("t1.MARKET_RET_ADD_TAX"), lit(0)) +
                coalesce(col("t2.INTEREST_ADD_TAX"), lit(0))
        ).alias("total_income_add_tax"),  # 经纪业务增值税及附加
        (
                coalesce(col("t.REMAIN_RISK_FUND"), lit(0)) +
                coalesce(col("t1.MARKET_RET_RISK_FUND"), lit(0)) +
                coalesce(col("t2.INTEREST_RISK_FUND"), lit(0))
        ).alias("total_income_risk_fund")  # 经纪业务风险金
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id"],
        update_columns=[
            "total_income_after_tax", "total_income_add_tax",
            "total_income_risk_fund"
        ]
    )

    """
    居间返佣  CSPERSON_REBATE
    居间返佣_不含税 CSPERSON_REBATE_AFTER_TAX
    居间留存增值税附加税 CSPERSON_REMAIN_ADD_TAX
    居间留存风险金 CSPERSON_REMAIN_RISK_FUND

    居间交返  CSPERSON_RET
    居间交返_不含税 CSPERSON_RET_AFTER_TAX
    居间交返增值税附加税 CSPERSON_RET_ADD_TAX
    居间交返风险金 CSPERSON_RET_RISK_FUND

    居间利息  CSPERSON_INTEREST
    居间利息_不含税  CSPERSON_INTEREST_AFTER_TAX
    居间利息增值税附加税  CSPERSON_INTEREST_ADD_TAX
    居间利息风险金  CSPERSON_INTEREST_RISK_FUND

    居间支出汇总=居间返佣+居间交返+居间利息  TOTAL_CSPER_EXPEND
    居间支出汇总_不含税=居间返佣_不含税+居间交返_不含税+居间利息_不含税  TOTAL_CSPER_EXPEND_AFTER_TAX
    居间支出汇总增值税附加税=居间留存增值税附加税+居间交返增值税附加税+居间利息增值税附加税  TOTAL_CSPER_EXPEND_ADD_TAX
    居间支出汇总风险金=居间留存风险金+居间交返风险金+居间利息风险金  TOTAL_CSPER_EXPEND_RISK_FUND

    IB返佣   IB_REBATE
    IB返佣_不含税  IB_REBATE_AFTER_TAX
    IB留存增值税附加税  IB_REBATE_ADD_TAX
    IB留存风险金  IB_REBATE_RISK_FUND

    IB交返  IB_RET
    IB交返_不含税 IB_RET_AFTER_TAX
    IB交返增值税附加税 IB_RET_ADD_TAX
    IB交返风险金 IB_RET_RISK_FUND

    IB利息 IB_INTEREST
    IB利息_不含税 IB_INTEREST_AFTER_TAX
    IB利息增值税附加税 IB_INTEREST_ADD_TAX
    IB利息风险金 IB_INTEREST_RISK_FUND

    IB支出汇总=IB返佣+IB交返+IB利息  TOTAL_IB_EXPEND
    IB支出汇总_不含税=IB返佣_不含税+IB交返_不含税+IB利息_不含税 TOTAL_IB_EXPEND_AFTER_TAX
    IB支出汇总增值税附加税=IB留存增值税附加税+IB交返增值税附加税+IB利息增值税附加税  TOTAL_IB_EXPEND_ADD_TAX
    IB支出汇总风险金=IB留存风险金+IB交返风险金+IB利息风险金  TOTAL_IB_EXPEND_RISK_FUND

    员工留存提成  STAFF_REMAIN_COMM
    员工留存提成_不含税 STAFF_REMAIN_COMM_AFTER_TAX
    员工留存增值税附加税 STAFF_REMAIN_COMM_ADD_TAX
    员工留存风险金STAFF_REMAIN_COMM_RISK_FUND

    员工交返  STAFF_RET
    员工交返_不含税  STAFF_RET_AFTER_TAX
    员工交返增值税附加税 STAFF_RET_ADD_TAX
    员工交返风险金  STAFF_RET_RISK_FUND

    员工利息  STAFF_INTEREST
    员工利息_不含税  STAFF_INTEREST_AFTER_TAX
    员工利息增值税附加税 STAFF_INTEREST_ADD_TAX
    员工利息风险金  STAFF_INTEREST_RISK_FUND

    其他支出  OTHER_PAY
    其他支出_不含税  OTHER_PAY_AFTER_TAX
    其他支出增值税附加税  OTHER_PAY_ADD_TAX
    其他支出风险金  OTHER_PAY_RISK_FUND

    员工支出汇总=员工留存提成+员工交返+员工利息+其他支出
    员工支出汇总_不含税=员工留存提成_不含税+员工交返_不含税+员工利息_不含税+其他支出_不含税
    员工支出汇总增值税附加税=员工留存增值税附加税+员工交返增值税附加税+员工利息增值税附加税+其他支出增值税附加税
    员工支出汇总风险金=员工留存风险金+员工交返风险金+员工利息风险金+其他支出风险金
    """
    logger.info(to_color_str("开始merge最后的汇总", "green"))
    df_merge_source = df_tmp_8.alias("t") \
        .select(
        col("t.oa_broker_id").alias("oa_broker_id"),
        col("t.rela_type").alias("rela_type"),
        col("t.fund_account_id").alias("fund_account_id"),
        col("t.cs_person_rebate").alias("csperson_rebate"),
        col("t.cs_person_rebate_after_tax").alias("csperson_rebate_after_tax"),
        col("t.cs_person_remain_add_tax").alias("csperson_remain_add_tax"),
        col("t.cs_person_remain_risk_fund").alias("csperson_remain_risk_fund"),
        col("t.cs_person_ret").alias("csperson_ret"),
        col("t.cs_person_ret_after_tax").alias("csperson_ret_after_tax"),
        col("t.cs_person_ret_add_tax").alias("csperson_ret_add_tax"),
        col("t.cs_person_ret_risk_fund").alias("csperson_ret_risk_fund"),
        col("t.cs_person_interest").alias("csperson_interest"),
        col("t.cs_person_interest_after_tax").alias("csperson_interest_after_tax"),
        col("t.cs_person_interest_add_tax").alias("csperson_interest_add_tax"),
        col("t.cs_person_interest_risk_fund").alias("csperson_interest_risk_fund"),
        (
                col("t.cs_person_rebate") +
                col("t.cs_person_ret") +
                col("t.cs_person_interest")
        ).alias("total_csper_expend"),
        (
                col("t.cs_person_rebate_after_tax") +
                col("t.cs_person_ret_after_tax") +
                col("t.cs_person_interest_after_tax")
        ).alias("total_csper_expend_after_tax"),
        (
                col("t.cs_person_remain_add_tax") +
                col("t.cs_person_ret_add_tax") +
                col("t.cs_person_interest_add_tax")
        ).alias("total_csper_expend_add_tax"),
        (
                col("t.cs_person_remain_risk_fund") +
                col("t.cs_person_ret_risk_fund") +
                col("t.cs_person_interest_risk_fund")
        ).alias("total_csper_expend_risk_fund"),
        col("t.ib_rebate").alias("ib_rebate"),
        col("t.ib_rebate_after_tax").alias("ib_rebate_after_tax"),
        col("t.ib_rebate_add_tax").alias("ib_rebate_add_tax"),
        col("t.ib_rebate_risk_fund").alias("ib_rebate_risk_fund"),
        col("t.ib_ret").alias("ib_ret"),
        col("t.ib_ret_after_tax").alias("ib_ret_after_tax"),
        col("t.ib_ret_add_tax").alias("ib_ret_add_tax"),
        col("t.ib_ret_risk_fund").alias("ib_ret_risk_fund"),
        col("t.ib_interest").alias("ib_interest"),
        col("t.ib_interest_after_tax").alias("ib_interest_after_tax"),
        col("t.ib_interest_add_tax").alias("ib_interest_add_tax"),
        col("t.ib_interest_risk_fund").alias("ib_interest_risk_fund"),
        (
                col("t.ib_rebate") +
                col("t.ib_ret") +
                col("t.ib_interest")
        ).alias("total_ib_expend"),
        (
                col("t.ib_rebate_after_tax") +
                col("t.ib_ret_after_tax") +
                col("t.ib_interest_after_tax")
        ).alias("total_ib_expend_after_tax"),
        (
                col("t.ib_rebate_add_tax") +
                col("t.ib_ret_add_tax") +
                col("t.ib_interest_add_tax")
        ).alias("total_ib_expend_add_tax"),
        (
                col("t.ib_rebate_risk_fund") +
                col("t.ib_ret_risk_fund") +
                col("t.ib_interest_risk_fund")
        ).alias("total_ib_expend_risk_fund"),
        col("t.staff_remain_comm").alias("staff_remain_comm"),
        col("t.staff_remain_comm_after_tax").alias("staff_remain_comm_after_tax"),
        col("t.staff_remain_comm_add_tax").alias("staff_remain_comm_add_tax"),
        col("t.staff_remain_comm_risk_fund").alias("staff_remain_comm_risk_fund"),
        col("t.staff_ret").alias("staff_ret"),
        col("t.staff_ret_after_tax").alias("staff_ret_after_tax"),
        col("t.staff_ret_add_tax").alias("staff_ret_add_tax"),
        col("t.staff_ret_risk_fund").alias("staff_ret_risk_fund"),
        col("t.staff_interest").alias("staff_interest"),
        col("t.staff_interest_after_tax").alias("staff_interest_after_tax"),
        col("t.staff_interest_add_tax").alias("staff_interest_add_tax"),
        col("t.staff_interest_risk_fund").alias("staff_interest_risk_fund"),
        col("t.other_pay").alias("other_pay"),
        col("t.other_pay_after_tax").alias("other_pay_after_tax"),
        col("t.other_pay_add_tax").alias("other_pay_add_tax"),
        col("t.other_pay_risk_fund").alias("other_pay_risk_fund"),
        (
                col("t.staff_remain_comm") +
                col("t.staff_ret") +
                col("t.staff_interest") +
                col("t.other_pay")
        ).alias("total_staff_expend"),
        (
                col("t.staff_remain_comm_after_tax") +
                col("t.staff_ret_after_tax") +
                col("t.staff_interest_after_tax") +
                col("t.other_pay_after_tax")
        ).alias("total_staff_expend_after_tax"),
        (
                col("t.staff_remain_comm_add_tax") +
                col("t.staff_ret_add_tax") +
                col("t.staff_interest_add_tax") +
                col("t.other_pay_add_tax")
        ).alias("total_staff_expend_add_tax"),
        (
                col("t.staff_remain_comm_risk_fund") +
                col("t.staff_ret_risk_fund") +
                col("t.staff_interest_risk_fund") +
                col("t.other_pay_risk_fund")
        ).alias("total_staff_expend_risk_fund")
    )

    df_result = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_merge_source,
        join_columns=["fund_account_id", "oa_broker_id", "rela_type"],
        update_columns=[
            "csperson_rebate", "csperson_rebate_after_tax",
            "csperson_remain_add_tax", "csperson_remain_risk_fund",
            "csperson_ret", "csperson_ret_after_tax",
            "csperson_ret_add_tax", "csperson_ret_risk_fund",
            "csperson_interest", "csperson_interest_after_tax",
            "csperson_interest_add_tax", "csperson_interest_risk_fund",
            "total_csper_expend", "total_csper_expend_after_tax",
            "total_csper_expend_add_tax", "total_csper_expend_risk_fund",
            "ib_rebate", "ib_rebate_after_tax",
            "ib_rebate_add_tax", "ib_rebate_risk_fund",
            "ib_ret", "ib_ret_after_tax",
            "ib_ret_add_tax", "ib_ret_risk_fund",
            "ib_interest", "ib_interest_after_tax",
            "ib_interest_add_tax", "ib_interest_risk_fund",
            "total_ib_expend", "total_ib_expend_after_tax",
            "total_ib_expend_add_tax", "total_ib_expend_risk_fund",
            "staff_remain_comm", "staff_remain_comm_after_tax",
            "staff_remain_comm_add_tax", "staff_remain_comm_risk_fund",
            "staff_ret", "staff_ret_after_tax",
            "staff_ret_add_tax", "staff_ret_risk_fund",
            "staff_interest", "staff_interest_after_tax",
            "staff_interest_add_tax", "staff_interest_risk_fund",
            "other_pay", "other_pay_after_tax",
            "other_pay_add_tax", "other_pay_risk_fund",
            "total_staff_expend", "total_staff_expend_after_tax",
            "total_staff_expend_add_tax", "total_staff_expend_risk_fund"
        ]
    )

    # 净贡献 = 经纪业务总收入 + 其他收入 - 软件费 - 投资者保障基金 - 客户总返还 - 居间总支出 - IB总支出 - 员工总支出
    logger.info(to_color_str("开始计算净贡献", "green"))
    df_result = df_result.withColumn(
        "net_contribution",
        coalesce(col("total_income"), lit(0)) +
        coalesce(col("other_income"), lit(0)) -
        coalesce(col("transfee_reward_soft"), lit(0)) -
        coalesce(col("secu_fee"), lit(0)) -
        coalesce(col("totla_client_ret"), lit(0)) -
        coalesce(col("total_csper_expend"), lit(0)) -
        coalesce(col("total_ib_expend"), lit(0)) -
        coalesce(col("total_staff_expend"), lit(0))
    )

    logger.info(to_color_str("全部计算结束,开始写入hive", "green"))

    if config.get("log").get("is_count") == "true":
        count = df_result.count()
        logger.info(to_color_str("本次写入总条数为:{}".format(count), "green"))

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_client_revenue",
        insert_mode="overwrite"
    )
