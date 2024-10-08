create or replace procedure cf_busimg.p_cockpit_00305_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00305_data
  -- version                1.0
  -- func_name
  -- func_remark            其他产品-权益数据落地
  -- create_date            20240904
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00305_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userexception exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id   varchar2(6);
  v_begin_date varchar2(8);
  v_end_date   varchar2(8);
  v_trade_days integer;
  ---------------------------------------------------------------------------------------

begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00306 t where t.month_id = v_month_id;
  commit;
  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_date, v_end_date, v_trade_days
    from cf_sett.t_pub_date t
   where t.busi_date like v_month_id || '%'
     and t.market_no = '1'
     and t.trade_flag = '1';

  insert into CF_BUSIMG.T_COCKPIT_00306
    (month_id,
     adjust_proj_id,
     adjust_proj,
     src_branch_id,
     src_branch_name,
     branch_id,
     branch_name,
     assignee_id,
     assignee,
     index_id,
     index_name,
     adjust_rate,
     index_value,
     adjust_value)
    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '01',
           '日均权益',
           a.rights_rate,
           sum(t.avg_rights),
           sum(t.avg_rights * a.rights_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.rights_rate

    union all

    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '02',
           '期末权益',
           a.rights_rate,
           sum(t.end_rights),
           sum(t.end_rights * a.rights_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.rights_rate

    union all

    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '03',
           '期初权益',
           a.rights_rate,
           sum(t.begin_rights),
           sum(t.begin_rights * a.rights_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.rights_rate

    union all

    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '04',
           '成交手数',
           a.done_rate,
           sum(t.done_amount),
           sum(t.done_amount * a.done_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.done_rate

    union all

    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '05',
           '成交额',
           a.done_rate,
           sum(t.done_money),
           sum(t.done_money * a.done_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.done_rate

    union all

    select v_month_id,
           t.adjust_proj_id,
           t.adjust_proj,
           t.src_branch_id,
           t.src_branch_name,
           a.branch_id,
           a.branch_name,
           a.assignee_id,
           a.assignee,
           '06',
           '留存手续费',
           a.done_rate,
           sum(t.remain_transfee),
           sum(t.remain_transfee * a.income_rate)
      from CF_BUSIMG.T_COCKPIT_00308 t
      left join CF_BUSIMG.T_COCKPIT_00309 a
        on t.month_id = a.month_id
       and t.src_branch_id = a.src_branch_id
       and t.adjust_proj_id = a.adjust_proj_id
     where t.month_id = v_month_id
     group by t.adjust_proj_id,
              t.adjust_proj,
              t.src_branch_id,
              t.src_branch_name,
              a.branch_id,
              a.branch_name,
              a.assignee_id,
              a.assignee,
              a.income_rate;

              commit;

  -------------------------------------------------------------
  o_return_code := 0;
  o_return_msg  := '执行成功';
  ---------------------------------------------------------------------------------------
  --错误处理部分
  ---------------------------------------------------------------------------------------
exception
  when v_userexception then
    o_return_code := v_error_code;
    o_return_msg  := v_error_msg;
    rollback;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '1',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
  when others then
    o_return_code := sqlcode;
    o_return_msg  := o_return_msg || sqlerrm;
    rollback;
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
