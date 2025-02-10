CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00140_DATA(I_BUSI_DATE   IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.2
  -- func_name
  -- func_remark            ib驻点收入调整表-数据落地
  -- create_date            20240826
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量

  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00140_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userException exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id varchar2(6);
  ---------------------------------------------------------------------------------------
BEGIN
  v_month_id := substr(I_BUSI_DATE, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00140 t where t.month_id = v_month_id;
  commit;

  INSERT INTO CF_BUSIMG.T_COCKPIT_00140
    (month_id,
     ib_branch_id,
     ib_branch_name,
     fund_account_id,
     client_name,
     broker_id,
     broker_name,
     oa_branch_id,
     oa_branch_name,
     begin_rights,
     end_rights,
     avg_rights,
     remain_transfee,
     ibzd_income,
     interest_clear_income,
     market_reduct_income,
     clear_remain_transfee,
     ibzd_income_reate,
     ibzd_interest_clear_income,
     ibzd_market_reduct_income,
     ibzd_clear_remain_transfee,
     done_amount,
     done_money)
    select t.month_id,
           '',
           '',
           t.fund_account_id,
           t.client_name,
           t.oa_broker_id,
           t.oa_broker_name,
           b.broker_branch_id,
           b.broker_branch_name,
           t.yes_rights,
           t.end_rights,
           t.avg_rights,
           t.clear_remain_transfee,
           (nvl(t.NET_INTEREST_REDUCE, 0) - nvl(t.CSPERSON_INTEREST, 0)) *
           nvl(nvl(a.data_pct, 1), 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end) + (nvl(t.MARKET_RET_REDUCE, 0) - nvl(t.CSPERSON_RET, 0)) *
           nvl(a.data_pct, 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end) + (nvl(t.clear_remain_transfee, 0) -
           nvl(t.CSPERSON_REBATE, 0)) * nvl(a.data_pct, 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end),
           nvl(t.NET_INTEREST_REDUCE, 0) - nvl(t.CSPERSON_INTEREST, 0),
           nvl(t.MARKET_RET_REDUCE, 0) - nvl(t.CSPERSON_RET, 0),
           nvl(t.clear_remain_transfee, 0) - nvl(t.CSPERSON_REBATE, 0),
           nvl(a.data_pct, 1),
           (nvl(t.NET_INTEREST_REDUCE, 0) - nvl(t.CSPERSON_INTEREST, 0)) *
           nvl(a.data_pct, 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end),
           (nvl(t.MARKET_RET_REDUCE, 0) - nvl(t.CSPERSON_RET, 0)) *
           nvl(a.data_pct, 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end),
           (nvl(t.clear_remain_transfee, 0) - nvl(t.CSPERSON_REBATE, 0)) *
           nvl(a.data_pct, 1) * (case
             when substr(t.branch_name, 1, 4) = 'SWHY' then
              0.45
             else
              1
           end),
           t.done_amount,
           t.done_money
      from CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE t
      left join CTP63.T_DS_CRM_BROKER_INVESTOR_RELA a
        on t.oa_broker_id = a.broker_id
       and t.fund_account_id = a.investor_id
       and t.rela_type = decode(a.broker_rela_typ,
                                '301',
                                '居间关系',
                                '001',
                                '开发关系',
                                '002',
                                '服务关系',
                                '003',
                                '维护关系',
                                '-')
     inner join CF_BUSIMG.T_COCKPIT_00110 b
        on t.oa_broker_id = b.broker_id
     where t.month_id = v_month_id
       and a.broker_id like 'ZD%'
       and a.rela_sts = 'A'
       and a.approve_sts = '0';
  COMMIT;
  -------------------------------------------------------------
  o_return_code := 0;
  o_return_msg  := '执行成功';
  ---------------------------------------------------------------------------------------
  --错误处理部分
  ---------------------------------------------------------------------------------------
EXCEPTION
  when v_userException then
    o_return_code := v_error_code;
    o_return_msg  := v_error_msg;
    ROLLBACK;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '1',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
  when OTHERS then
    o_return_code := SQLCODE;
    o_return_msg  := o_return_msg || SQLERRM;
    ROLLBACK;
    v_error_msg  := o_return_msg;
    v_error_code := o_return_code;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '2',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
end;
