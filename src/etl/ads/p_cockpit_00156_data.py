# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

from src.env.task_env import return_to_hive, log


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