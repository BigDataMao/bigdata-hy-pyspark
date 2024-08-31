# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, regexp_replace, when, sum, round

from src.env.config import Config
from src.env.task_env import return_to_hive, log
from src.utils.date_utils import get_month_str, get_date_period_and_days, get_day_last_month
from src.utils.logger_uitls import to_color_str


@log
def p_cockpit_00156_data(spark: SparkSession, busi_date: str):
    """
    特殊客户收入调整表-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期
    :return: None
    """

    """
      v_month_id := substr(I_BUSI_DATE, 1, 6);

  v_ds_begin_busi_date := substr(i_busi_date, 1, 4) || '-' ||
                          substr(i_busi_date, 5, 2) || '-01';

  v_last_ds_begin_busi_date := to_char(ADD_MONTHS(to_date(v_ds_begin_busi_date,
                                                          'YYYY-MM-DD'),
                                                  -1),
                                       'YYYY-MM-DD');

  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_date, v_end_date, v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_month_id
     and t.market_no = '1'
     and t.trade_flag = '1'
     and t.busi_date <= i_busi_date;

  select max(t.busi_date)
    into v_end_busi_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_month_id
     and t.market_no = '1';
  v_ds_end_busi_date := substr(v_end_busi_date, 1, 4) || '-' ||
                        substr(v_end_busi_date, 5, 2) || '-' ||
                        substr(v_end_busi_date, 7, 2);

  v_last_ds_end_busi_date := to_char(ADD_MONTHS(to_date(v_end_busi_date,
                                                        'YYYY-MM-DD'),
                                                -1),
                                     'YYYY-MM-DD');

  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00156_1';
  insert into CF_BUSIMG.TMP_COCKPIT_00156_1
    (BUSI_DATE, FUND_ACCOUNT_ID, MARKET_REDUCT)

    with tmp as
     (select replace(a.tx_dt, '-', '') as busi_date,
             a.INVESTOR_ID as fund_account_id,
             sum(round(case
                         when a.tx_dt >= v_ds_begin_busi_date then
                          a.EXCHANGE_TXFEE_AMT
                         else
                          0
                       end,
                       2)) as EXCHANGE_TXFEE_AMT,
             sum(round(case
                         when a.tx_dt >= v_ds_begin_busi_date then
                          a.RET_FEE_AMT_tx
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          a.RET_FEE_AMT_czce
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_czce,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          a.RET_FEE_AMT_dce
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_dce,
             sum(round(case
                         when a.tx_dt >= v_ds_begin_busi_date then
                          RET_FEE_AMT_shfe
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_shfe,
             sum(round(case
                         when a.tx_dt < '2022-05-01' then
                          case
                            when a.tx_dt <= v_last_ds_end_busi_date then
                             RET_FEE_AMT_shfe1
                            else
                             0
                          end
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_shfe1,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_cffex
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_cffex,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_cffex2021
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_cffex2021,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_dce31
                         else
                          '0'
                       end,
                       4)) as RET_FEE_AMT_dce31,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_dce32
                         else
                          '0'
                       end,
                       4)) as RET_FEE_AMT_dce32,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_dce33
                         else
                          '0'
                       end,
                       4)) as RET_FEE_AMT_dce33,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          a.RET_FEE_AMT_dce1
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_dce1,
             sum(round(case
                         when a.tx_dt <= v_last_ds_end_busi_date then
                          RET_FEE_AMT_dce2
                         else
                          0
                       end,
                       4)) as RET_FEE_AMT_dce2,
             sum(round(nvl(case
                             when a.tx_dt >= v_ds_begin_busi_date then
                              a.investor_ret_amt
                             else
                              0
                           end,
                           0),
                       4)) as investor_ret_amt
        from CTP63.T_DS_RET_EXCHANGE_RETFEE2 a
       inner join CTP63.T_DS_DC_ORG b
          on a.orig_department_id = b.department_id
       inner join CTP63.T_DS_DC_INVESTOR ff
          on a.investor_id = ff.investor_id
       where ((a.tx_dt between v_last_ds_begin_busi_date and
             v_last_ds_end_busi_date) or
             (a.tx_dt between v_ds_begin_busi_date and v_ds_end_busi_date))
       group by a.INVESTOR_ID, replace(a.tx_dt, '-', ''))
    select a.busi_date,
           a.fund_account_id,
           RET_FEE_AMT + RET_FEE_AMT_czce + RET_FEE_AMT_dce +
           RET_FEE_AMT_cffex + RET_FEE_AMT_cffex2021 + RET_FEE_AMT_shfe +
           RET_FEE_AMT_shfe1 + RET_FEE_AMT_dce1 + RET_FEE_AMT_dce2 +
           RET_FEE_AMT_dce31 + RET_FEE_AMT_dce32 + RET_FEE_AMT_dce33 as market_reduct --交易所减收
      from (select a.fund_account_id,
                   a.busi_date,
                   sum(EXCHANGE_TXFEE_AMT) EXCHANGE_TXFEE_AMT,
                   sum(RET_FEE_AMT) RET_FEE_AMT,
                   sum(RET_FEE_AMT_czce) RET_FEE_AMT_czce,
                   sum(RET_FEE_AMT_dce) RET_FEE_AMT_dce, --大连近月

                   sum(RET_FEE_AMT_cffex) RET_FEE_AMT_cffex,
                   sum(RET_FEE_AMT_cffex2021) RET_FEE_AMT_cffex2021,
                   sum(RET_FEE_AMT_dce31) RET_FEE_AMT_dce31,
                   sum(RET_FEE_AMT_dce32) RET_FEE_AMT_dce32,
                   sum(RET_FEE_AMT_dce33) RET_FEE_AMT_dce33,
                   sum(round(RET_FEE_AMT_dce1, 4)) RET_FEE_AMT_dce1,
                   sum(round(RET_FEE_AMT_dce2, 4)) RET_FEE_AMT_dce2,
                   sum(round(RET_FEE_AMT_shfe, 4)) RET_FEE_AMT_shfe,
                   sum(round(RET_FEE_AMT_shfe1, 4)) RET_FEE_AMT_shfe1,
                   sum(investor_ret_amt) investor_ret_amt,
                   1 order_seq
              from tmp a
             group by a.fund_account_id, a.busi_date) a

    ;
  commit;

  --交易所返还支出
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00156_2';
  insert into CF_BUSIMG.TMP_COCKPIT_00156_2
    (BUSI_DATE, FUND_ACCOUNT_ID, OCCUR_MONEY)
    select t.busi_date,
           t.fund_account_id,
           sum(t.occur_money) as occur_money
      from CF_SETT.T_FUND_JOUR t
     where t.fund_type = '3' --公司调整
       and t.fund_direct = '1' --入金
       and t.busi_date between v_begin_date and v_end_date
     group by t.fund_account_id, t.busi_date;
  commit;

  --交易所净返还（扣客户交返）=交易所返还收入-交易所返还支出
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00156_3';
  insert into CF_BUSIMG.TMP_COCKPIT_00156_3
    (busi_date, FUND_ACCOUNT_ID, MARKET_RET_REDUCE)

    select t.busi_date,
           t.fund_account_id,
           sum(t.MARKET_REDUCT - nvl(t1.OCCUR_MONEY, 0)) as MARKET_RET_REDUCE --交易所净返还（扣客户交返）

      from CF_BUSIMG.TMP_COCKPIT_00156_1 t
      left join CF_BUSIMG.TMP_COCKPIT_00156_2 t1
        on t.busi_date = t1.busi_date
       and t.fund_account_id = t1.fund_account_id
     inner join CF_BUSIMG.T_COCKPIT_00153 a
        on t.fund_account_id = a.client_id
     where t.busi_date between a.begin_date and a.end_date
     group by t.busi_date, t.fund_account_id;
  commit;

  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00156_4';
  INSERT INTO CF_BUSIMG.TMP_COCKPIT_00156_4
    (busi_date, fund_account_id, remain_transfee)
    select t.n_busi_date,
           t.fund_account_id,
           sum(t.remain_transfee) as remain_transfee
      from cf_stat.t_rpt_06008 t
     inner join CF_BUSIMG.T_COCKPIT_00153 a
        on t.fund_account_id = a.client_id
     where t.n_busi_date between v_begin_date and v_end_date
       and t.n_busi_date between a.begin_date and a.end_date
     group by t.n_busi_date, t.fund_account_id;

  delete from CF_BUSIMG.T_COCKPIT_00156 t where t.month_id = v_month_id;
  commit;

  INSERT INTO CF_BUSIMG.T_COCKPIT_00156
    (month_id,
     client_id,
     client_name,
     remain_transfee_total,
     jian_mian_total,
     department,
     allocation_dept,
     remain_transfee_rate,
     remain_transfee_money,
     jian_mian_rate,
     jian_mian_money)
    select substr(t.busi_date, 1, 6),
           a.client_id,
           a.client_name,
           sum(t1.remain_transfee),
           sum(t.market_ret_reduce),
           a.branch_id,
           a.coll_branch_id,
           a.remain_rate,
           sum(t1.remain_transfee * a.remain_rate),
           a.jm_rate,
           sum(t.market_ret_reduce * jm_rate)
      from CF_BUSIMG.TMP_COCKPIT_00156_3 t
      left join CF_BUSIMG.TMP_COCKPIT_00156_4 t1
        on t.busi_date = t1.busi_date
       and t.fund_account_id = t1.fund_account_id
     inner join CF_BUSIMG.T_COCKPIT_00153 a
        on t.fund_account_id = a.client_id
     group by substr(t.busi_date, 1, 6),
              a.client_id,
              a.client_name,
              a.branch_id,
              a.coll_branch_id,
              a.remain_rate,
              a.jm_rate;
  COMMIT;
    """

    config = Config()
    logger = config.get_logger()
    # 1. 定义变量

    v_busi_month = busi_date[:6]
    v_ds_begin_busi_date = busi_date[:4] + "-" + busi_date[4:6] + "-01"
    v_last_month = get_month_str(v_busi_month, -1)
    v_last_ds_begin_busi_date = v_last_month[:4] + "-" + v_last_month[4:6] + "-01"
    v_begin_date, v_end_date, v_trade_days = get_date_period_and_days(
        spark=spark,
        begin_month=v_busi_month,
        begin_date='19000101',  # 1900-01-01,基于开始日期和结束日期进行过滤,所以这里设置为最小日期
        end_date=busi_date,
        is_trade_day=True
    )

    v_end_busi_date = get_date_period_and_days(
        spark=spark,
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

    # 2. 计算逻辑
    logger.info(to_color_str("开始计算交易所返还收入", "green"))
    tmp = spark.table("ods.t_ds_ret_exchange_retfee2").alias("a") \
        .filter(
        (
                (col("tx_dt").between(v_last_ds_begin_busi_date, v_last_ds_end_busi_date)) |
                (col("tx_dt").between(v_ds_begin_busi_date, v_ds_end_busi_date))
        )
    ).join(
        spark.table("ods.t_ds_dc_org").alias("b"),
        col("a.orig_department_id") == col("b.department_id")
    ).join(
        spark.table("ods.t_ds_dc_investor").alias("ff"),
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
    logger.info(to_color_str("开始计算交易所净返还（扣客户交返）", "green"))
    df_tmp_3 = df_tmp_1.alias("t").join(
        df_tmp_2.alias("t1"),
        (col("t.busi_date") == col("t1.busi_date")) &
        (col("t.fund_account_id") == col("t1.fund_account_id")),
        "left"
    ).join(
        spark.table("ddw.cockpit_00153").alias("a"),
        col("t.fund_account_id") == col("a.client_id")
    ).filter(
        (col("t.busi_date").between(col("a.begin_date"), col("a.end_date")))
    ).groupBy(
        col("t.busi_date").alias("busi_date"),
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum(
            col("t.market_reduct") - coalesce(col("t1.occur_money"), lit(0))
        ).alias("market_ret_reduce")  # 交易所净返还（扣客户交返）
    )

    df_tmp_4 = spark.table("ddw.rpt_06008").alias("t") \
        .join(
        spark.table("ddw.cockpit_00153").alias("a"),
        col("t.fund_account_id") == col("a.client_id")
    ).filter(
        (col("t.n_busi_date").between(v_begin_date, v_end_date)) &
        (col("t.n_busi_date").between(col("a.begin_date"), col("a.end_date")))
    ).groupBy(
        col("t.n_busi_date").alias("busi_date"),
        col("t.fund_account_id").alias("fund_account_id")
    ).agg(
        sum("t.remain_transfee").alias("remain_transfee")
    )

    # 3. 数据落地
    logger.info(to_color_str("开始数据落地", "green"))
    df_result = df_tmp_3.alias("t").join(
        df_tmp_4.alias("t1"),
        (col("t.busi_date") == col("t1.busi_date")) &
        (col("t.fund_account_id") == col("t1.fund_account_id")),
        "left"
    ).join(
        spark.table("ddw.cockpit_00153").alias("a"),
        col("t.fund_account_id") == col("a.client_id"),
        "inner"
    ).groupBy(
        col("t.busi_date").substr(1, 6).alias("month_id"),
        col("a.client_id").alias("client_id"),
        col("a.client_name").alias("client_name"),
        col("a.branch_id").alias("department"),
        col("a.coll_branch_id").alias("allocation_dept"),
        col("a.remain_rate").alias("remain_transfee_rate"),
        col("a.jm_rate").alias("jian_mian_rate")
    ).agg(
        sum("t1.remain_transfee").alias("remain_transfee_total"),
        sum("t.market_ret_reduce").alias("jian_mian_total"),
        sum(
            col("t1.remain_transfee") * col("a.remain_rate")
        ).alias("remain_transfee_money"),
        sum(
            col("t.market_ret_reduce") * col("a.jm_rate")
        ).alias("jian_mian_money")
    ).select(
        col("month_id"),
        col("client_id"),
        col("client_name"),
        col("remain_transfee_total"),
        col("jian_mian_total"),
        col("department"),
        col("allocation_dept"),
        col("remain_transfee_rate"),
        col("remain_transfee_money"),
        col("jian_mian_rate"),
        col("jian_mian_money")
    )

    # 4. 数据写入
    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.cockpit_00156",
        insert_mode="overwrite"
    )
