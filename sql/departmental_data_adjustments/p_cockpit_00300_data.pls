create or replace procedure cf_busimg.p_cockpit_00300_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00300_data
  -- version                1.0
  -- func_name
  -- func_remark            溯源表模板_部门间数据调整-数据落地
  -- create_date            20240828
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00300_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userexception exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id varchar2(6);
  v_sql_text varchar2(4000);
  v_sql      clob;
  ---------------------------------------------------------------------------------------

  CURSOR c_cockpit_account IS
    SELECT t.account_code
      FROM CF_BUSIMG.t_cockpit_acount_func_rela t
     where t.func_id = v_op_object;

begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00300 t where t.month_id = v_month_id;
  commit;

  for x in c_cockpit_account loop
    select t.cal_column
      into v_sql_text
      from cf_busimg.t_cockpit_acount_func_rela t
     where t.func_id = v_op_object
       and t.account_code = x.account_code;

    v_sql := 'insert into CF_BUSIMG.T_COCKPIT_00300
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
     with tmp as
     (select substr(t.busi_date, 1, 6) as month_id,
             t.out_oa_branch_id,
             t.out_oa_branch_name,
             t.in_oa_branch_id,
             t.in_oa_branch_name,
             sum(t.allocat_remain_transfee) allocat_remain_transfee
        from CF_BUSIMG.T_COCKPIT_00114_DATA t
       where substr(t.busi_date, 1, 6) =' || v_month_id ||
             ' group by substr(t.busi_date, 1, 6),
                t.out_oa_branch_id,
                t.out_oa_branch_name,
                t.in_oa_branch_id,
                t.in_oa_branch_name)
    select month_id,
           t.out_oa_branch_id,
           t.out_oa_branch_name,
           t.in_oa_branch_id,
           t.in_oa_branch_name,
           a.account_code,
           a.account_name,' || v_sql_text ||
             'to_char(sysdate,''yyyymmdd''),
           a.func_name as allocated_project,
           null
      from tmp t,
           cf_busimg.t_cockpit_acount_func_rela a,
           CF_BUSIMG.T_COCKPIT_00202 b,
           CF_BUSIMG.T_COCKPIT_00202 c,
           CF_BUSIMG.T_COCKPIT_00202 e
     where a.func_id = '''||v_op_object||'''
        and a.account_code = ' || x.account_code || 'and b.fee_type = ''1002''
       and ' || i_busi_date || 'between b.begin_date and b.end_date
       and c.fee_type = ''1004''
       and ' || i_busi_date || ' between c.begin_date and c.end_date
       and e.fee_type = ''1005''
       and ' || i_busi_date || ' between e.begin_date and e.end_date
      ';
    dbms_output.put_line(v_sql);
    execute immediate v_sql;
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
