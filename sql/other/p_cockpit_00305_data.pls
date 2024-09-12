create or replace procedure cf_busimg.p_cockpit_00305_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00306_data
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
  CURSOR c_cockpit_00305 IS
    SELECT t.pk_id FROM CF_BUSIMG.T_COCKPIT_00305 t;
begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00306 t where t.month_id = v_month_id;
  commit;
  for x in c_cockpit_00305 loop
    select min(t.busi_date), max(t.busi_date), count(1)
      into v_begin_date, v_end_date, v_trade_days
      from cf_sett.t_pub_date t, CF_BUSIMG.T_COCKPIT_00305 a
     where t.busi_date like v_month_id || '%'
       and t.market_no = '1'
       and t.trade_flag = '1'
       and a.pk_id = x.pk_id
       and t.busi_date between a.begin_date and a.end_date;

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
       begin_date,
       end_date,
       begin_rights,
       end_rights,
       avg_rights,
       done_amount,
       done_money)
      select v_month_id,
             t.adjust_proj_id,
             t.adjust_proj,
              t.src_branch_id,
             t.src_branch_name,
             t.branch_id,
             t.branch_name,
             t.assignee_id,
             t.assignee,
             t.index_id,
             t.index_name,
             t.adjust_rate,
             t.begin_date,
             t.end_date,
             sum(case
                   when b.n_busi_date = v_begin_date then
                    b.yes_rights
                   else
                    0
                 end) as begin_rights,
             sum(case
                   when b.n_busi_date = v_end_date then
                    b.rights
                   else
                    0
                 end) as end_rights,
             sum(b.rights) / v_trade_days as avg_rights,
             sum(b.done_amount) as done_amount,
             sum(b.done_money) as done_money
        from CF_BUSIMG.T_COCKPIT_00305 t
       inner join cf_busimg.t_ctp_branch_oa_rela a
          on t.src_branch_id = a.ctp_branch_id
        left join cf_stat.t_rpt_06008 b
          on a.ctp_branch_id = b.branch_id
       where b.n_busi_date between t.begin_date and t.end_date
         and b.n_busi_date between v_begin_date and v_end_date
         and b.trade_flag = '1'
       group by t.adjust_proj_id,
                t.adjust_proj ,
                t.src_branch_id,
                t.src_branch_name,
                t.branch_id,
                t.branch_name,
                t.assignee_id,
                t.assignee,
                t.index_id,
                t.index_name,
                t.adjust_rate,
                t.begin_date,
                t.end_date;

    commit;
  end loop;
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
