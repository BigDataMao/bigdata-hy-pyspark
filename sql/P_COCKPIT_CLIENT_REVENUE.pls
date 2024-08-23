create or replace procedure cf_busimg.P_COCKPIT_CLIENT_REVENUE(i_busi_date   in varchar2,
                                                               o_return_code out integer,
                                                               o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:客户创收台账 数据落地
  --模块名称:
  --模块编号:
  --模块描述:落地表 CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE
  --开发人员:zhongying.zhang
  --目前版本:1.0
  --创建时间:20240820
  --版    权:
  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_CLIENT_REVENUE'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_busi_month    varchar2(6); --月份
  v_begin_date    varchar2(8); --开始日期
  v_end_date      varchar2(8); --结束日期
  v_trade_days    number; --交易日天数
  v_end_busi_date varchar2(8); --当月月底日期

  v_ds_begin_busi_date      varchar2(10); --德所开始日期
  v_last_ds_begin_busi_date varchar2(10); --德所上个月开始日期
  v_ds_end_busi_date        varchar2(10); --德所结束日期
  v_last_ds_end_busi_date   varchar2(10); --德所上个月结束日期

begin
  v_busi_month         := substr(i_busi_date, 1, 6);
  v_ds_begin_busi_date := substr(i_busi_date, 1, 4) || '-' ||
                          substr(i_busi_date, 5, 2) || '-01';

  v_last_ds_begin_busi_date := to_char(ADD_MONTHS(to_date(v_ds_begin_busi_date,
                                                          'YYYY-MM-DD'),
                                                  -1),
                                       'YYYY-MM-DD');

  select min(t.busi_date), max(t.busi_date),count(1)
    into v_begin_date, v_end_date,v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_busi_month
     and t.market_no = '1'
     and t.trade_flag = '1'
     and t.busi_date <= i_busi_date;

  select max(t.busi_date)
    into v_end_busi_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_busi_month
     and t.market_no = '1';
  v_ds_end_busi_date := substr(v_end_busi_date, 1, 4) || '-' ||
                        substr(v_end_busi_date, 5, 2) || '-' ||
                        substr(v_end_busi_date, 7, 2);

  v_last_ds_end_busi_date := to_char(ADD_MONTHS(to_date(v_end_busi_date,
                                                        'YYYY-MM-DD'),
                                                -1),
                                     'YYYY-MM-DD');

  ----------------------------基础数据处理begin----------------------------------------------
  /*
  交易所返还收入
  减免返还收入”取自：crm系统的“内核表-投资者交易所返还计算-二次开发”字段“交易所减收”；
  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_1';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_1
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
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_2';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_2
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
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_3';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_3
    (FUND_ACCOUNT_ID,
     MARKET_RET_REDUCE,
     MARKET_RET_REDUCE_AFTER_TAX,
     MARKET_RET_ADD_TAX,
     MARKET_RET_RISK_FUND)
    with tmp as
     (select t.busi_date,
             t.fund_account_id,
             (t.MARKET_REDUCT - nvl(t1.OCCUR_MONEY, 0)) as MARKET_RET_REDUCE, --交易所净返还（扣客户交返）
             (t.MARKET_REDUCT - nvl(t1.OCCUR_MONEY, 0)) /
             (1 + nvl(c.para_value, 0)) as MARKET_RET_REDUCE_AFTER_TAX, --交易所净返还（扣客户交返）_不含税
             (t.MARKET_REDUCT - nvl(t1.OCCUR_MONEY, 0)) *
             nvl(d.para_value, 0) as MARKET_RET_ADD_TAX, --交返增值税及附加
             (t.MARKET_REDUCT - nvl(t1.OCCUR_MONEY, 0)) *
             nvl(e.para_value, 0) as MARKET_RET_RISK_FUND --交返风险金
        from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_1 t
        left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_2 t1
          on t.busi_date = t1.busi_date
         and t.fund_account_id = t1.fund_account_id
        left join cf_sett.t_fund_account t2
          on t.fund_account_id = t2.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela x
          on t2.branch_id = x.ctp_branch_id
        left join CF_BUSIMG.T_COCKPIT_00202 c
          on (instr(c.branch_id, x.oa_branch_id) > 0)
         and c.fee_type = '1004' --增值税税率
         and t.busi_date between c.BEGIN_DATE and c.end_date
        left join CF_BUSIMG.T_COCKPIT_00202 d
          on (instr(d.branch_id, x.oa_branch_id) > 0)
         and d.fee_type = '1005' --增值税及附加税税率
         and t.busi_date between d.BEGIN_DATE and d.end_date
        left join CF_BUSIMG.T_COCKPIT_00202 e
          on (instr(e.branch_id, x.oa_branch_id) > 0)
         and e.fee_type = '1002' --风险金比例
         and t.busi_date between e.BEGIN_DATE and e.end_date

      )
    select t.fund_account_id,
           sum(t.MARKET_RET_REDUCE) as MARKET_RET_REDUCE,
           sum(t.MARKET_RET_REDUCE_AFTER_TAX) as MARKET_RET_REDUCE_AFTER_TAX,
           sum(t.MARKET_RET_ADD_TAX) as MARKET_RET_ADD_TAX,
           sum(t.MARKET_RET_RISK_FUND) as MARKET_RET_RISK_FUND
      from tmp t
     group by t.fund_account_id;
  commit;

  /*
  自然日均可用资金*年利率*统计周期内自然天数/年天数——资金对账表对应数据+系统内维护的年利率计算得到
  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_4';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_4
    (BUSI_DATE, FUND_ACCOUNT_ID, INTEREST_BASE, INTEREST_BASE_INCOME)
    with tmp_client as
     (select t.busi_date,
             t.fund_account_id,
             x.oa_branch_id,
             sum(t.rights) - sum(case
                                   when t.impawn_money > t.margin then
                                    t.impawn_money
                                   else
                                    t.margin
                                 end) as interest_base
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account t2
          on t.fund_account_id = t2.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela x
          on t2.branch_id = x.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
       group by t.busi_date, t.fund_account_id, x.oa_branch_id)
    select t.busi_date,
           t.fund_account_id,
           sum(t.interest_base) as interest_base,
           sum(t.interest_base * nvl(c.PARA_VALUE, 0)) / 360 as interest_income
      from tmp_client t
     inner join cf_busimg.t_cockpit_date_nature b
        on t.busi_date = b.busi_date
      left join CF_BUSIMG.T_COCKPIT_00202 c
        on (instr(c.branch_id, t.oa_branch_id) > 0)
       and t.busi_date between c.begin_date and c.end_date
       and c.fee_type = '1001' --利息收入（公司）年利率
     group by t.busi_date, t.fund_account_id;
  commit;

  /*
  利息收入
  利息收入_不含税
  利息收入_增值税及附加
  利息收入_风险金
  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5
    (FUND_ACCOUNT_ID,
     ACCRUED_INTEREST,
     ACCRUED_INTEREST_AFTER_TAX,
     ACCRUED_INTEREST_ADD_TAX,
     ACCRUED_INTEREST_RISK_FUND,
     AVG_OPEN_PRE_NATURE)
    select t.fund_account_id,
           sum(t.interest_base_income) as ACCRUED_INTEREST, --利息收入
           sum(t.interest_base_income / (1 + nvl(c.para_value, 0))) as ACCRUED_INTEREST_after_tax, --利息收入_不含税
           sum(t.interest_base_income * nvl(d.para_value, 0)) as ACCRUED_INTEREST_add_tax, --利息收入_增值税及附加
           sum(t.interest_base_income * nvl(e.para_value, 0)) as ACCRUED_INTEREST_risk_fund, --利息收入_风险金
           sum(t.interest_base) as AVG_OPEN_PRE_NATURE --自然日均可用
      from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_4 t
      left join cf_sett.t_fund_account t2
        on t.fund_account_id = t2.fund_account_id
     inner join cf_busimg.t_ctp_branch_oa_rela x
        on t2.branch_id = x.ctp_branch_id
      left join CF_BUSIMG.T_COCKPIT_00202 c
        on (instr(c.branch_id, x.oa_branch_id) > 0)
       and c.fee_type = '1004' --增值税税率
       and t.busi_date between c.BEGIN_DATE and c.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 d
        on (instr(d.branch_id, x.oa_branch_id) > 0)
       and d.fee_type = '1005' --增值税及附加税税率
       and t.busi_date between d.BEGIN_DATE and d.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 e
        on (instr(e.branch_id, x.oa_branch_id) > 0)
       and e.fee_type = '1002' --风险金比例
       and t.busi_date between e.BEGIN_DATE and e.end_date
     group by t.fund_account_id;
  commit;

  /*
  资金账号相关数据：
  软件使用费
  留存手续费
  交易所返还
  留存手续费_不含税
  留存_增值税及附加
  留存_风险金
  利息积数
  客户结息
  客户结息_不含税
  客户结息_增值税及附加
  客户结息_风险金
  客户交返
  客户手续费返还=保留字段，目前值为0

  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_6
    (FUND_ACCOUNT_ID,
     TRANSFEE_REWARD_SOFT,
     REMAIN_TRANSFEE,
     MARKET_RET,
     REMAIN_TRANSFEE_AFTER_TAX,
     REMAIN_TRANSFEE_ADD_TAX,
     REMAIN_RISK_FUND,
     INTEREST_BASE,
     CLIENT_INTEREST_SETTLEMENT,
     CLIENT_INTEREST_AFTER_TAX,
     CLIENT_INTEREST_ADD_TAX,
     CLIENT_INTEREST_RISK_FUND,
     OTHER_INCOME,
     OTHER_INCOME_AFTER_TAX,
     OTHER_INCOME_ADD_TAX,
     OTHER_INCOME_RISK_FUND,
     MARKET_RET_CLIENT,
     TRANSFEE_REWARD_CLIENT --客户手续费返还=保留字段，目前值为0
     )
    select t.investor_id as FUND_ACCOUNT_ID,
           round(sum(t.soft_amt), 2) as TRANSFEE_REWARD_SOFT, --软件使用费
           round(sum(t.subsistence_fee_amt), 2) as REMAIN_TRANSFEE, --留存手续费
           round(sum(t.exchangeret_amt), 2) as MARKET_RET, --交易所返还
           round(sum(t.subsistence_fee_amt / (1 + nvl(c.para_value, 0))), 2) as REMAIN_TRANSFEE_AFTER_TAX, --留存手续费_不含税
           round(sum(t.subsistence_fee_amt * nvl(d.para_value, 0)), 2) as REMAIN_TRANSFEE_ADD_TAX, --留存增值税及附加
           round(sum(t.subsistence_fee_amt * nvl(e.para_value, 0)), 2) as REMAIN_RISK_FUND, --留存风险金
           round(sum(t.calint_amt), 2) as INTEREST_BASE, --利息积数
           round(sum(t.i_int_amt), 2) as CLIENT_INTEREST_SETTLEMENT, --客户结息
           round(sum(t.i_int_amt / (1 + nvl(c.para_value, 0))), 2) as CLIENT_INTEREST_after_tax, --客户结息_不含税
           round(sum(t.i_int_amt * nvl(d.para_value, 0)), 2) as CLIENT_INTEREST_add_tax, --客户结息_增值税及附加
           round(sum(t.i_int_amt * nvl(e.para_value, 0)), 2) as CLIENT_INTEREST_risk_fund, --客户结息_风险金
           round(sum(t.oth_amt), 2) as OTHER_INCOME, --其他收入
           round(sum(t.oth_amt / (1 + nvl(c.para_value, 0))), 2) as OTHER_INCOME_AFTER_TAX, --其他收入_不含税
           round(sum(t.oth_amt * nvl(d.para_value, 0)), 2) as OTHER_INCOME_ADD_TAX, --其他收入增值税及附加
           round(sum(t.oth_amt * nvl(e.para_value, 0))) as OTHER_INCOME_RISK_FUND, --其他收入风险金
           round(sum(t.i_exchangeret_amt), 2) as MARKET_RET_CLIENT, --客户交返
           0 as TRANSFEE_REWARD_CLIENT --客户手续费返还=保留字段，目前值为0

      from CTP63.T_DS_ADM_INVESTOR_VALUE t
     inner join CTP63.T_DS_DC_INVESTOR b
        on t.investor_id = b.investor_id
     inner join cf_busimg.t_ctp_branch_oa_rela x
        on b.orig_department_id = x.ctp_branch_id
      left join CF_BUSIMG.T_COCKPIT_00202 c
        on (instr(c.branch_id, x.oa_branch_id) > 0)
       and c.fee_type = '1004' --增值税税率
       and replace(t.date_dt, '-', '') between c.BEGIN_DATE and c.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 d
        on (instr(d.branch_id, x.oa_branch_id) > 0)
       and d.fee_type = '1005' --增值税及附加税税率
       and replace(t.date_dt, '-', '') between d.BEGIN_DATE and d.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 e
        on (instr(e.branch_id, x.oa_branch_id) > 0)
       and e.fee_type = '1002' --风险金比例
       and replace(t.date_dt, '-', '') between e.BEGIN_DATE and e.end_date
     where t.date_dt = v_ds_begin_busi_date
     group by t.investor_id;
  commit;
  /*
  净利息收入（扣客户结息）=利息收入-客户结息   NET_INTEREST_REDUCE
  净利息收入（扣客户结息）_不含税  NET_INTEREST_REDUCE_AFTER_TAX
  利息增值税及附加  INTEREST_ADD_TAX
  利息风险金  INTEREST_RISK_FUND
  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_7';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_7
    select t.fund_account_id,
           (t.accrued_interest - nvl(t1.CLIENT_INTEREST_SETTLEMENT, 0)) as NET_INTEREST_REDUCE,
           (t.accrued_interest_after_tax -
           nvl(t1.client_interest_after_tax, 0)) as NET_INTEREST_REDUCE_AFTER_TAX,
           (t.accrued_interest_add_tax - nvl(t1.client_interest_add_tax, 0)) as INTEREST_ADD_TAX,
           (t.ACCRUED_INTEREST_risk_fund -
           nvl(t1.client_interest_risk_fund, 0)) as INTEREST_RISK_FUND
      from cf_sett.t_fund_account x
      left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5 t --利息收入
        on x.fund_account_id = t.fund_account_id
      left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_6 t1 --客户结息
        on x.fund_account_id = t1.fund_account_id;
  commit;

  /*
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
  */
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_8';
  insert into CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_8
    (OA_BROKER_ID,
     RELA_TYPE,
     FUND_ACCOUNT_ID,
     CSPERSON_REBATE,
     CSPERSON_REBATE_AFTER_TAX,
     CSPERSON_REMAIN_ADD_TAX,
     CSPERSON_REMAIN_RISK_FUND,
     CSPERSON_RET,
     CSPERSON_RET_AFTER_TAX,
     CSPERSON_RET_ADD_TAX,
     CSPERSON_RET_RISK_FUND,
     CSPERSON_INTEREST, --居间利息
     CSPERSON_INTEREST_AFTER_TAX, --居间利息_不含税
     CSPERSON_INTEREST_ADD_TAX, --居间利息增值税附加税
     CSPERSON_INTEREST_RISK_FUND, --居间利息风险金
     IB_REBATE, --IB返佣
     IB_REBATE_AFTER_TAX, --IB返佣_不含税
     IB_REBATE_ADD_TAX, --IB留存增值税附加税
     IB_REBATE_RISK_FUND, --IB留存风险金
     IB_RET, --IB交返
     IB_RET_AFTER_TAX, --IB交返_不含税
     IB_RET_ADD_TAX, --IB交返增值税附加税
     IB_RET_RISK_FUND, --IB交返风险金
     IB_INTEREST, --IB利息
     IB_INTEREST_AFTER_TAX, --IB利息_不含税
     IB_INTEREST_ADD_TAX, --IB利息增值税附加税
     IB_INTEREST_RISK_FUND, --IB利息风险金

     STAFF_REMAIN_COMM, --员工留存提成
     STAFF_REMAIN_COMM_AFTER_TAX, --员工留存提成_不含税
     STAFF_REMAIN_COMM_ADD_TAX, --员工留存增值税附加税
     STAFF_REMAIN_COMM_RISK_FUND, --员工留存风险金

     STAFF_RET, --员工交返
     STAFF_RET_AFTER_TAX, --员工交返_不含税
     STAFF_RET_ADD_TAX, --员工交返增值税附加税
     STAFF_RET_RISK_FUND, --员工交返风险金

     STAFF_INTEREST, --员工利息
     STAFF_INTEREST_AFTER_TAX, --员工利息_不含税
     STAFF_INTEREST_ADD_TAX, --员工利息增值税附加税
     STAFF_INTEREST_RISK_FUND, --员工利息风险金

     OTHER_PAY, --其他支出
     OTHER_PAY_AFTER_TAX, --其他支出_不含税
     OTHER_PAY_ADD_TAX, --其他支出增值税附加税
     OTHER_PAY_RISK_FUND --其他支出风险金
     )
    select nvl(a2.staff_id, '-') as OA_BROKER_ID,
           decode(a2.srela_typ,
                  '301',
                  '居间关系',
                  '001',
                  '开发关系',
                  '002',
                  '服务关系',
                  '003',
                  '维护关系',
                  '-') as rela_type,
           a.investor_id as FUND_ACCOUNT_ID,
           round(sum(a.broker_amt), 2) as CSPERSON_REBATE, --居间返佣
           round(sum(a.broker_amt / (1 + nvl(d.para_value, 0))), 2) as CSPERSON_REBATE_AFTER_TAX, --居间返佣_不含税
           round(sum(a.broker_amt * nvl(e.para_value, 0)), 2) as CSPERSON_REMAIN_ADD_TAX, --居间留存增值税附加税
           round(sum(a.broker_amt * nvl(f.para_value, 0)), 2) as CSPERSON_REMAIN_RISK_FUND, --居间留存风险金
           round(sum(a.broker_eret_amt), 2) as CSPERSON_RET, --居间交返
           round(sum(a.broker_eret_amt / (1 + nvl(d.para_value, 0))), 2) as CSPERSON_RET_AFTER_TAX, --居间交返_不含税
           round(sum(a.broker_eret_amt * nvl(e.para_value, 0)), 2) as CSPERSON_RET_ADD_TAX, --居间交返增值税附加税
           round(sum(a.broker_eret_amt * nvl(f.para_value, 0)), 2) as CSPERSON_RET_RISK_FUND, --居间交返风险金
           round(sum(a.broker_int_amt), 2) as CSPERSON_INTEREST, --居间利息
           round(sum(a.broker_int_amt / (1 + nvl(d.para_value, 0))), 2) as CSPERSON_INTEREST_AFTER_TAX, --居间利息_不含税
           round(sum(a.broker_int_amt * nvl(e.para_value, 0)), 2) as CSPERSON_INTEREST_ADD_TAX, --居间利息增值税附加税
           round(sum(a.broker_int_amt * nvl(f.para_value, 0)), 2) as CSPERSON_INTEREST_RISK_FUND, --居间利息风险金
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_amt
                       ELSE
                        0
                     END),
                 2) AS IB_REBATE, --IB返佣
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_amt / (1 + nvl(d.para_value, 0))
                       ELSE
                        0
                     END),
                 2) AS IB_REBATE_AFTER_TAX, --IB返佣_不含税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_amt * nvl(e.para_value, 0)
                       ELSE
                        0
                     END),
                 2) AS IB_REBATE_ADD_TAX, --IB留存增值税附加税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_amt * nvl(f.para_value, 0)
                       ELSE
                        0
                     END),
                 2) AS IB_REBATE_RISK_FUND, --IB留存风险金
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_eret_amt
                       ELSE
                        0
                     END),
                 2) as IB_RET, -- IB交返
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_eret_amt / (1 + nvl(d.para_value, 0))
                       ELSE
                        0
                     END),
                 2) as IB_RET_AFTER_TAX, -- IB交返_不含税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_eret_amt * nvl(e.para_value, 0)
                       ELSE
                        0
                     END),
                 2) as IB_RET_ADD_TAX, -- IB交返增值税附加税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_eret_amt * nvl(f.para_value, 0)
                       ELSE
                        0
                     END),
                 2) as IB_RET_RISK_FUND, -- IB交返风险金

           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_int_amt
                       ELSE
                        0
                     END),
                 2) as IB_INTEREST, -- IB利息
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_int_amt / (1 + nvl(d.para_value, 0))
                       ELSE
                        0
                     END),
                 2) as IB_INTEREST_AFTER_TAX, -- IB利息_不含税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_int_amt * nvl(e.para_value, 0)
                       ELSE
                        0
                     END),
                 2) as IB_INTEREST_ADD_TAX, -- IB利息增值税附加税
           round(sum(CASE
                       WHEN A2.STAFF_ID LIKE 'IB%' THEN
                        a.broker_int_amt * nvl(f.para_value, 0)
                       ELSE
                        0
                     END),
                 2) as IB_INTEREST_RISK_FUND, -- IB利息风险金
           round(sum(nvl(a2.ib_amt, a.STAFF_AMT)), 2) as STAFF_REMAIN_COMM, --员工留存提成
           round(sum(nvl(a2.ib_amt, a.STAFF_AMT) /
                     (1 + nvl(d.para_value, 0))),
                 2) as STAFF_REMAIN_COMM_AFTER_TAX, --员工留存提成_不含税
           round(sum(nvl(a2.ib_amt, a.STAFF_AMT) * nvl(e.para_value, 0)), 2) as STAFF_REMAIN_COMM_ADD_TAX, --员工留存增值税附加税
           round(sum(nvl(a2.ib_amt, a.STAFF_AMT) * nvl(f.para_value, 0)), 2) as STAFF_REMAIN_COMM_RISK_FUND, --员工留存风险金

           round(sum(a.staff_eret_amt), 2) as STAFF_RET, --员工交返
           round(sum(a.staff_eret_amt / (1 + nvl(d.para_value, 0))), 2) as STAFF_RET_AFTER_TAX, --员工交返_不含税
           round(sum(a.staff_eret_amt * nvl(e.para_value, 0)), 2) as STAFF_RET_ADD_TAX, --员工交返增值税附加税
           round(sum(a.staff_eret_amt * nvl(f.para_value, 0)), 2) as STAFF_RET_RISK_FUND, --员工交返风险金

           round(sum(nvl(a2.STAFF_INT_AMT, a.STAFF_INT_AMT)), 2) as STAFF_INTEREST, --员工利息
           round(sum(nvl(a2.STAFF_INT_AMT, a.STAFF_INT_AMT) /
                     (1 + nvl(d.para_value, 0))),
                 2) as STAFF_INTEREST_AFTER_TAX, --员工利息_不含税
           round(sum(nvl(a2.STAFF_INT_AMT, a.STAFF_INT_AMT) *
                     nvl(e.para_value, 0)),
                 2) as STAFF_INTEREST_ADD_TAX, --员工利息增值税附加税
           round(sum(nvl(a2.STAFF_INT_AMT, a.STAFF_INT_AMT) *
                     nvl(f.para_value, 0)),
                 2) as STAFF_INTEREST_RISK_FUND, --员工利息风险金

           round(sum(a.i_oth_amt), 2) as OTHER_PAY, --其他支出
           round(sum(a.i_oth_amt / (1 + nvl(d.para_value, 0))), 2) as OTHER_PAY_AFTER_TAX, --其他支出_不含税
           round(sum(a.i_oth_amt * nvl(e.para_value, 0)), 2) as OTHER_PAY_ADD_TAX, --其他支出增值税附加税
           round(sum(a.i_oth_amt * nvl(f.para_value, 0)), 2) as OTHER_PAY_RISK_FUND --其他支出风险金
      from CTP63.T_DS_ADM_INVESTOR_VALUE a
      join CTP63.T_DS_DC_INVESTOR b
        on a.investor_id = b.investor_id
      join CTP63.T_DS_DC_ORG c
        on b.orig_department_id = c.department_id
      left JOIN CTP63.T_DS_ADM_BROKERDATA_DETAIL a2
        on a.date_dt = a2.tx_dt
       and a.investor_id = a2.investor_id
       and a2.rec_freq = 'M'
     inner join cf_busimg.t_ctp_branch_oa_rela x
        on b.orig_department_id = x.ctp_branch_id
      left join CF_BUSIMG.T_COCKPIT_00202 d
        on (instr(d.branch_id, x.oa_branch_id) > 0)
       and d.fee_type = '1004' --增值税税率
       and replace(a.date_dt, '-', '') between d.BEGIN_DATE and d.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 e
        on (instr(e.branch_id, x.oa_branch_id) > 0)
       and e.fee_type = '1006' --增值税附加税税率
       and replace(a.date_dt, '-', '') between e.BEGIN_DATE and e.end_date
      left join CF_BUSIMG.T_COCKPIT_00202 f
        on (instr(f.branch_id, x.oa_branch_id) > 0)
       and f.fee_type = '1002' --风险金比例
       and replace(a.date_dt, '-', '') between f.BEGIN_DATE and f.end_date
     where a.date_dt = v_ds_begin_busi_date
     group by nvl(a2.staff_id, '-'),
              decode(a2.srela_typ,
                     '301',
                     '居间关系',
                     '001',
                     '开发关系',
                     '002',
                     '服务关系',
                     '003',
                     '维护关系',
                     '-'),
              a.investor_id;
  commit;
  ----------------------------基础数据处理end-------------------------------------------------

  delete from CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE t
   where t.month_id = v_busi_month

  ;
  commit;

  --初始化数据
  insert into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE
    (MONTH_ID,
     BRANCH_ID,
     BRANCH_NAME,
     OA_BROKER_ID,
     OA_BROKER_NAME,
     RELA_TYPE,
     CSPERSON_NAME,
     FUND_ACCOUNT_ID,
     CLIENT_NAME)
    select v_busi_month,
           c.department_id as BRANCH_ID,
           c.department_nam as BRANCH_NAME,
           nvl(a2.staff_id, '-') as OA_BROKER_ID,
           nvl(a2.staff_nam, '-') as OA_BROKER_NAME,
           decode(a2.srela_typ,
                  '301',
                  '居间关系',
                  '001',
                  '开发关系',
                  '002',
                  '服务关系',
                  '003',
                  '维护关系',
                  '-') as rela_type,
           nvl(a2.Broker_Nam, '-') as CSPERSON_NAME,
           a.investor_id as FUND_ACCOUNT_ID,
           b.investor_nam as CLIENT_NAME
      from CTP63.T_DS_ADM_INVESTOR_VALUE a
      join CTP63.T_DS_DC_INVESTOR b
        on a.investor_id = b.investor_id
      join CTP63.T_DS_DC_ORG c
        on b.orig_department_id = c.department_id
      left JOIN CTP63.T_DS_ADM_BROKERDATA_DETAIL a2
        on a.date_dt = a2.tx_dt
       and a.investor_id = a2.investor_id
       and a2.rec_freq = 'M'
     where a.date_dt = v_ds_begin_busi_date
     group by c.department_id,
              c.department_nam,
              nvl(a2.staff_id, '-'),
              nvl(a2.staff_nam, '-'),
              decode(a2.srela_typ,
                     '301',
                     '居间关系',
                     '001',
                     '开发关系',
                     '002',
                     '服务关系',
                     '003',
                     '维护关系',
                     '-'),
              nvl(a2.Broker_Nam, '-'),
              a.investor_id,
              b.investor_nam;

  commit;

  /*
  期初权益 YES_RIGHTS
  期末权益 END_RIGHTS
  日均权益 AVG_RIGHTS
  总盈亏 TOTAL_PROFIT
  手续费 TRANSFEE
  上交手续费 MARKET_TRANSFEE
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.fund_account_id,
                sum(case
                      when t.busi_date = v_begin_date then
                       t.yes_rights
                      else
                       0
                    end) as YES_RIGHTS,
                sum(case
                      when t.busi_date = v_end_date then
                       t.rights
                      else
                       0
                    end) as END_RIGHTS,
                sum(case
                      when v_trade_days > 0 then
                       t.rights / v_trade_days
                      else
                       0
                    end) as AVG_RIGHTS,
                sum(t.TODAY_PROFIT) as TOTAL_PROFIT,

                sum(t.transfee + t.delivery_transfee + t.strikefee) as TRANSFEE,
                sum(t.market_transfee + t.market_delivery_transfee +
                    t.market_strikefee) as MARKET_TRANSFEE
           from cf_sett.t_client_sett t
          where t.busi_date between v_begin_date and v_end_date
          group by t.FUND_ACCOUNT_ID) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.yes_rights      = y.YES_RIGHTS,
           a.END_RIGHTS      = y.END_RIGHTS,
           a.AVG_RIGHTS      = y.AVG_RIGHTS,
           a.TOTAL_PROFIT    = y.TOTAL_PROFIT,
           a.TRANSFEE        = y.TRANSFEE,
           a.MARKET_TRANSFEE = y.MARKET_TRANSFEE;
  commit;

  /*
  成交金额  DONE_MONEY
  成交手数  DONE_AMOUNT
  保障基金 =成交金额*投资者保障基金比例 SECU_FEE
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.fund_account_id,
                sum(t.done_amt) as done_amount,
                sum(t.done_sum) as DONE_MONEY,
                sum(t.done_sum * nvl(c.para_value, 0)) as SECU_FEE
           from cf_sett.t_hold_balance t
          inner join cf_busimg.t_ctp_branch_oa_rela x
             on t.branch_id = x.ctp_branch_id
           left join CF_BUSIMG.T_COCKPIT_00202 c
             on (instr(c.branch_id, x.oa_branch_id) > 0)
            and c.fee_type = '1003' --投保基金比例
            and t.busi_date between c.BEGIN_DATE and c.end_date
          where t.busi_date between v_begin_date and v_end_date
          group by t.FUND_ACCOUNT_ID) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.done_amount = y.done_amount,
           a.DONE_MONEY  = y.DONE_MONEY,
           a.SECU_FEE    = y.SECU_FEE;
  commit;

  /*
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

  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.FUND_ACCOUNT_ID,
                t.TRANSFEE_REWARD_SOFT, --软件使用费
                t.REMAIN_TRANSFEE, --留存手续费
                t.REMAIN_TRANSFEE_AFTER_TAX, --留存手续费_不含税
                t.REMAIN_TRANSFEE_ADD_TAX, --留存增值税及附加
                t.REMAIN_RISK_FUND, --留存风险金
                t.INTEREST_BASE, --利息积数
                t.MARKET_RET, --交易所返还
                t.CLIENT_INTEREST_SETTLEMENT, --客户结息
                t.MARKET_RET_CLIENT, --客户交返
                t.TRANSFEE_REWARD_CLIENT, --客户手续费返还
                (t.TRANSFEE_REWARD_CLIENT + t.MARKET_RET_CLIENT +
                t.CLIENT_INTEREST_SETTLEMENT) as TOTLA_CLIENT_RET, --客户返还汇总
                t.other_income, --其他收入
                t.other_income_after_tax, --其他收入_不含税
                t.other_income_add_tax, --其他收入增值税及附加
                t.other_income_risk_fund --其他收入风险金
           from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_6 t

         ) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.TRANSFEE_REWARD_SOFT       = y.TRANSFEE_REWARD_SOFT,
           a.REMAIN_TRANSFEE            = y.REMAIN_TRANSFEE,
           a.REMAIN_TRANSFEE_AFTER_TAX  = y.REMAIN_TRANSFEE_AFTER_TAX,
           a.REMAIN_TRANSFEE_ADD_TAX    = y.REMAIN_TRANSFEE_ADD_TAX,
           a.REMAIN_RISK_FUND           = y.REMAIN_RISK_FUND,
           a.MARKET_RET                 = y.MARKET_RET,
           a.INTEREST_BASE              = y.INTEREST_BASE,
           a.CLIENT_INTEREST_SETTLEMENT = y.CLIENT_INTEREST_SETTLEMENT,
           a.MARKET_RET_CLIENT          = y.MARKET_RET_CLIENT,
           a.TRANSFEE_REWARD_CLIENT     = y.TRANSFEE_REWARD_CLIENT,
           a.TOTLA_CLIENT_RET           = y.TOTLA_CLIENT_RET,
           a.OTHER_INCOME               = y.OTHER_INCOME,
           a.OTHER_INCOME_AFTER_TAX     = y.OTHER_INCOME_AFTER_TAX,
           a.OTHER_INCOME_ADD_TAX       = y.OTHER_INCOME_ADD_TAX,
           a.OTHER_INCOME_RISK_FUND     = y.OTHER_INCOME_RISK_FUND;
  commit;

  --------------------------------------------------------------------------------------------------------
  /*
    交易所净返还（扣客户交返）
    交易所总减免=交易所返还收入-交易所返还支出
    减免返还收入”取自：crm系统的“内核表-投资者交易所返还计算-二次开发”字段“交易所减收”；
  “减免返还支出”取自：crm系统的“客户出入金流水”，筛选字段“资金类型”为公司调整后，取字段“入金金额”

   交易所净返还（扣客户交返）=交易所返还收入-交易所返还支出    MARKET_RET_REDUCE
   交易所净返还（扣客户交返）_不含税  MARKET_RET_REDUCE_AFTER_TAX
   交返增值税及附加 MARKET_RET_ADD_TAX
   交返风险金 MARKET_RET_RISK_FUND
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (

         select t.fund_account_id,
                 t.MARKET_RET_REDUCE,
                 t.MARKET_RET_REDUCE_AFTER_TAX,
                 t.MARKET_RET_ADD_TAX,
                 t.MARKET_RET_RISK_FUND
           from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_3 t) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.MARKET_RET_REDUCE           = y.MARKET_RET_REDUCE,
           a.MARKET_RET_REDUCE_AFTER_TAX = y.MARKET_RET_REDUCE_AFTER_TAX,
           a.MARKET_RET_ADD_TAX          = y.MARKET_RET_ADD_TAX,
           a.MARKET_RET_RISK_FUND        = y.MARKET_RET_RISK_FUND;
  commit;

  /*
  自然日均可用  AVG_OPEN_PRE_NATURE
  应计利息  ACCRUED_INTEREST
  */

  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.fund_account_id, t.AVG_OPEN_PRE_NATURE, t.ACCRUED_INTEREST
           from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5 t) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.AVG_OPEN_PRE_NATURE = y.AVG_OPEN_PRE_NATURE,
           a.ACCRUED_INTEREST    = y.ACCRUED_INTEREST;
  commit;

  /*
  净利息收入（扣客户结息）=利息收入-客户结息   NET_INTEREST_REDUCE
  净利息收入（扣客户结息）_不含税  NET_INTEREST_REDUCE_AFTER_TAX
  利息增值税及附加  INTEREST_ADD_TAX
  利息风险金  INTEREST_RISK_FUND
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.fund_account_id,
                t.NET_INTEREST_REDUCE,
                t.NET_INTEREST_REDUCE_AFTER_TAX,
                t.INTEREST_ADD_TAX,
                t.INTEREST_RISK_FUND
           from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_7 t) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.NET_INTEREST_REDUCE           = y.NET_INTEREST_REDUCE,
           a.NET_INTEREST_REDUCE_AFTER_TAX = y.NET_INTEREST_REDUCE_AFTER_TAX,
           a.INTEREST_ADD_TAX              = y.INTEREST_ADD_TAX,
           a.INTEREST_RISK_FUND            = y.INTEREST_RISK_FUND;
  commit;

  /*
  经纪业务总收入=留存手续费+交易所返还+应计利息 TOTAL_INCOME
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select x.fund_account_id,
                (nvl(b.remain_transfee, 0) + nvl(b.market_ret, 0) +
                nvl(c.accrued_interest, 0)) as TOTAL_INCOME
           from cf_sett.t_fund_account x
           left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_6 b
             on x.fund_account_id = b.fund_account_id
           left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_5 c
             on x.fund_account_id = c.fund_account_id) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update set a.TOTAL_INCOME = y.TOTAL_INCOME;
  commit;

  /*
  TOTAL_INCOME_AFTER_TAX 经纪业务总净收入_不含税=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]/(1+增值税税率)
  TOTAL_INCOME_ADD_TAX   经纪业务增值税及附加=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]*（1+增值税税率）
  TOTAL_INCOME_RISK_FUND 经纪业务风险金=[留存手续费+交易所净返还（扣客户交返）+净利息收入（扣客户结息）]*风险金比例
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select x.fund_account_id,
                (nvl(t.REMAIN_TRANSFEE_AFTER_TAX, 0) +
                nvl(t1.MARKET_RET_REDUCE_AFTER_TAX, 0) +
                nvl(t2.NET_INTEREST_REDUCE_AFTER_TAX, 0)) as TOTAL_INCOME_AFTER_TAX, --经纪业务总净收入_不含税
                (nvl(t.REMAIN_TRANSFEE_ADD_TAX, 0) +
                nvl(t1.MARKET_RET_ADD_TAX, 0) + nvl(t2.INTEREST_ADD_TAX, 0)) AS TOTAL_INCOME_ADD_TAX, --经纪业务增值税及附加
                (nvl(t.REMAIN_RISK_FUND, 0) +
                nvl(t1.MARKET_RET_RISK_FUND, 0) +
                nvl(t2.INTEREST_RISK_FUND, 0)) as TOTAL_INCOME_RISK_FUND --经纪业务风险金
           from cf_sett.t_fund_account x
           left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_6 t
             on x.fund_account_id = t.fund_account_id
           left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_3 t1
             on x.fund_account_id = t1.fund_account_id
           left join CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_7 t2
             on x.fund_account_id = t2.fund_account_id

         ) y
  on (a.MONTH_ID = v_busi_month and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.TOTAL_INCOME_AFTER_TAX = y.TOTAL_INCOME_AFTER_TAX,
           a.TOTAL_INCOME_ADD_TAX   = y.TOTAL_INCOME_ADD_TAX,
           a.TOTAL_INCOME_RISK_FUND = y.TOTAL_INCOME_RISK_FUND;
  commit;

  /*
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
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE a
  using (select t.oa_broker_id,
                t.rela_type,
                t.fund_account_id,
                t.csperson_rebate,
                t.csperson_rebate_after_tax,
                t.csperson_remain_add_tax,
                t.csperson_remain_risk_fund,
                t.CSPERSON_RET,
                t.CSPERSON_RET_AFTER_TAX,
                t.CSPERSON_RET_ADD_TAX,
                t.CSPERSON_RET_RISK_FUND,
                t.CSPERSON_INTEREST,
                t.CSPERSON_INTEREST_AFTER_TAX,
                t.CSPERSON_INTEREST_ADD_TAX,
                t.CSPERSON_INTEREST_RISK_FUND,
                (t.CSPERSON_REBATE + t.CSPERSON_RET + t.CSPERSON_INTEREST) as TOTAL_CSPER_EXPEND,
                (t.CSPERSON_REBATE_AFTER_TAX + t.CSPERSON_RET_AFTER_TAX +
                t.CSPERSON_INTEREST_AFTER_TAX) as TOTAL_CSPER_EXPEND_AFTER_TAX,
                (t.CSPERSON_REMAIN_ADD_TAX + t.CSPERSON_RET_ADD_TAX +
                t.CSPERSON_INTEREST_ADD_TAX) as TOTAL_CSPER_EXPEND_ADD_TAX,
                (t.CSPERSON_REMAIN_RISK_FUND + t.CSPERSON_RET_RISK_FUND +
                t.CSPERSON_INTEREST_RISK_FUND) as TOTAL_CSPER_EXPEND_RISK_FUND,
                t.IB_REBATE,
                t.IB_REBATE_AFTER_TAX,
                t.IB_REBATE_ADD_TAX,
                t.IB_REBATE_RISK_FUND,
                t.IB_RET,
                t.IB_RET_AFTER_TAX,
                t.IB_RET_ADD_TAX,
                t.IB_RET_RISK_FUND,
                t.IB_INTEREST,
                t.IB_INTEREST_AFTER_TAX,
                t.IB_INTEREST_ADD_TAX,
                t.IB_INTEREST_RISK_FUND,
                (T.IB_REBATE + T.IB_RET + T.IB_INTEREST) as TOTAL_IB_EXPEND,
                (T.IB_REBATE_AFTER_TAX + T.IB_RET_AFTER_TAX +
                T.IB_INTEREST_AFTER_TAX) AS TOTAL_IB_EXPEND_AFTER_TAX,
                (T.IB_REBATE_ADD_TAX + T.IB_RET_ADD_TAX +
                T.IB_INTEREST_ADD_TAX) AS TOTAL_IB_EXPEND_ADD_TAX,
                (T.IB_REBATE_RISK_FUND + T.IB_RET_RISK_FUND +
                T.IB_INTEREST_RISK_FUND) AS TOTAL_IB_EXPEND_RISK_FUND,
                T.STAFF_REMAIN_COMM,
                T.STAFF_REMAIN_COMM_AFTER_TAX,
                T.STAFF_REMAIN_COMM_ADD_TAX,
                T.STAFF_REMAIN_COMM_RISK_FUND,
                T.STAFF_RET,
                T.STAFF_RET_AFTER_TAX,
                T.STAFF_RET_ADD_TAX,
                T.STAFF_RET_RISK_FUND,
                T.STAFF_INTEREST,
                T.STAFF_INTEREST_AFTER_TAX,
                T.STAFF_INTEREST_ADD_TAX,
                T.STAFF_INTEREST_RISK_FUND,
                T.OTHER_PAY,
                T.OTHER_PAY_AFTER_TAX,
                T.OTHER_PAY_ADD_TAX,
                T.OTHER_PAY_RISK_FUND,
                (T.STAFF_REMAIN_COMM + T.STAFF_RET + T.STAFF_INTEREST +
                T.OTHER_PAY) as TOTAL_STAFF_EXPEND,
                (T.STAFF_REMAIN_COMM_AFTER_TAX + T.STAFF_RET_AFTER_TAX +
                T.STAFF_INTEREST_AFTER_TAX + T.OTHER_PAY_AFTER_TAX) as TOTAL_STAFF_EXPEND_AFTER_TAX,
                (T.STAFF_REMAIN_COMM_ADD_TAX + T.STAFF_RET_ADD_TAX +
                T.STAFF_INTEREST_ADD_TAX + T.OTHER_PAY_ADD_TAX) as TOTAL_STAFF_EXPEND_ADD_TAX,
                (T.STAFF_REMAIN_COMM_RISK_FUND + T.STAFF_RET_RISK_FUND +
                T.STAFF_INTEREST_RISK_FUND + T.OTHER_PAY_RISK_FUND) as TOTAL_STAFF_EXPEND_RISK_FUND
           from CF_BUSIMG.TMP_COCKPIT_CLIENT_REVENUE_8 t) y
  on (a.MONTH_ID = v_busi_month and a.OA_BROKER_ID = y.OA_BROKER_ID and a.RELA_TYPE = y.RELA_TYPE and a.FUND_ACCOUNT_ID = y.FUND_ACCOUNT_ID)
  when matched then
    update
       set a.CSPERSON_REBATE              = y.CSPERSON_REBATE,
           a.CSPERSON_REBATE_AFTER_TAX    = y.CSPERSON_REBATE_AFTER_TAX,
           a.CSPERSON_REMAIN_ADD_TAX      = y.CSPERSON_REMAIN_ADD_TAX,
           a.CSPERSON_REMAIN_RISK_FUND    = y.CSPERSON_REMAIN_RISK_FUND,
           a.CSPERSON_RET                 = y.CSPERSON_RET,
           a.CSPERSON_RET_AFTER_TAX       = y.CSPERSON_RET_AFTER_TAX,
           a.CSPERSON_RET_ADD_TAX         = y.CSPERSON_RET_ADD_TAX,
           a.CSPERSON_RET_RISK_FUND       = y.CSPERSON_RET_RISK_FUND,
           a.CSPERSON_INTEREST            = y.CSPERSON_INTEREST,
           a.CSPERSON_INTEREST_AFTER_TAX  = y.CSPERSON_INTEREST_AFTER_TAX,
           a.CSPERSON_INTEREST_ADD_TAX    = y.CSPERSON_INTEREST_ADD_TAX,
           a.CSPERSON_INTEREST_RISK_FUND  = y.CSPERSON_INTEREST_RISK_FUND,
           a.TOTAL_CSPER_EXPEND           = y.TOTAL_CSPER_EXPEND,
           a.TOTAL_CSPER_EXPEND_AFTER_TAX = y.TOTAL_CSPER_EXPEND_AFTER_TAX,
           a.TOTAL_CSPER_EXPEND_ADD_TAX   = y.TOTAL_CSPER_EXPEND_ADD_TAX,
           a.TOTAL_CSPER_EXPEND_RISK_FUND = y.TOTAL_CSPER_EXPEND_RISK_FUND,
           a.IB_REBATE                    = y.IB_REBATE,
           a.IB_REBATE_AFTER_TAX          = y.IB_REBATE_AFTER_TAX,
           a.IB_REBATE_ADD_TAX            = y.IB_REBATE_ADD_TAX,
           a.IB_REBATE_RISK_FUND          = y.IB_REBATE_RISK_FUND,
           a.IB_RET                       = y.IB_RET,
           a.IB_RET_AFTER_TAX             = y.IB_RET_AFTER_TAX,
           a.IB_RET_ADD_TAX               = y.IB_RET_ADD_TAX,
           a.IB_RET_RISK_FUND             = y.IB_RET_RISK_FUND,
           a.IB_INTEREST                  = y.IB_INTEREST,
           a.IB_INTEREST_AFTER_TAX        = y.IB_INTEREST_AFTER_TAX,
           a.IB_INTEREST_ADD_TAX          = y.IB_INTEREST_ADD_TAX,
           a.IB_INTEREST_RISK_FUND        = y.IB_INTEREST_RISK_FUND,
           A.TOTAL_IB_EXPEND              = Y.TOTAL_IB_EXPEND,
           a.TOTAL_IB_EXPEND_AFTER_TAX    = y.TOTAL_IB_EXPEND_AFTER_TAX,
           a.TOTAL_IB_EXPEND_ADD_TAX      = y.TOTAL_IB_EXPEND_ADD_TAX,
           a.TOTAL_IB_EXPEND_RISK_FUND    = y.TOTAL_IB_EXPEND_RISK_FUND,
           A.STAFF_REMAIN_COMM            = Y.STAFF_REMAIN_COMM,
           a.STAFF_REMAIN_COMM_AFTER_TAX  = y.STAFF_REMAIN_COMM_AFTER_TAX,
           a.STAFF_REMAIN_COMM_ADD_TAX    = y.STAFF_REMAIN_COMM_ADD_TAX,
           a.STAFF_REMAIN_COMM_RISK_FUND  = y.STAFF_REMAIN_COMM_RISK_FUND,
           A.STAFF_RET                    = Y.STAFF_RET,
           a.STAFF_RET_AFTER_TAX          = y.STAFF_RET_AFTER_TAX,
           a.STAFF_RET_ADD_TAX            = y.STAFF_RET_ADD_TAX,
           a.STAFF_RET_RISK_FUND          = y.STAFF_RET_RISK_FUND,
           A.STAFF_INTEREST               = Y.STAFF_INTEREST,
           a.STAFF_INTEREST_AFTER_TAX     = y.STAFF_INTEREST_AFTER_TAX,
           a.STAFF_INTEREST_ADD_TAX       = y.STAFF_INTEREST_ADD_TAX,
           a.STAFF_INTEREST_RISK_FUND     = y.STAFF_INTEREST_RISK_FUND,
           A.OTHER_PAY                    = Y.OTHER_PAY,
           a.OTHER_PAY_AFTER_TAX          = y.OTHER_PAY_AFTER_TAX,
           a.OTHER_PAY_ADD_TAX            = y.OTHER_PAY_ADD_TAX,
           a.OTHER_PAY_RISK_FUND          = y.OTHER_PAY_RISK_FUND,
           A.TOTAL_STAFF_EXPEND           = Y.TOTAL_STAFF_EXPEND,
           a.TOTAL_STAFF_EXPEND_AFTER_TAX = y.TOTAL_STAFF_EXPEND_AFTER_TAX,
           a.TOTAL_STAFF_EXPEND_ADD_TAX   = y.TOTAL_STAFF_EXPEND_ADD_TAX,
           a.TOTAL_STAFF_EXPEND_RISK_FUND = y.TOTAL_STAFF_EXPEND_RISK_FUND;
  commit;

  /*
  净贡献=经纪业务总收入+其他收入-软件费-投资者保障基金-客户总返还-居间总支出-IB总支出-员工总支出
  */
  update CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE t
     set t.net_contribution = t.TOTAL_INCOME + t.OTHER_INCOME -
                              t.TRANSFEE_REWARD_SOFT - t.SECU_FEE -
                              t.TOTLA_CLIENT_RET - t.TOTAL_CSPER_EXPEND -
                              t.TOTAL_IB_EXPEND - t.TOTAL_STAFF_EXPEND
   where t.month_id = v_busi_month;
   commit;
  --------------------------------------------------------------------------------------------------------------
  O_RETURN_CODE := 0;
  O_RETURN_MSG  := '执行成功';
EXCEPTION
  when v_userException then
    ROLLBACK;
    v_error_msg  := o_return_msg;
    v_error_code := o_return_code;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );

  WHEN OTHERS THEN
    O_RETURN_CODE := SQLCODE;
    O_RETURN_MSG  := O_RETURN_MSG || SQLERRM;
    V_ERROR_CODE  := SQLCODE;
    V_ERROR_MSG   := SQLERRM;
    WOLF.P_ERROR_LOG('admin',
                     V_OP_OBJECT,
                     V_ERROR_CODE,
                     V_ERROR_MSG,
                     '',
                     '',
                     O_RETURN_MSG,
                     O_RETURN_CODE);
    COMMIT;
END;
