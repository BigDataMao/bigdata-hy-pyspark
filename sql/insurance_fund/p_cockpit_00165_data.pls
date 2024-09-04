create or replace procedure cf_busimg.p_cockpit_00165_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00166_data
  -- version                1.0
  -- func_name
  -- func_remark            溯源表模板_投资者保障基金 -数据落地
  -- create_date            20240827
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00165_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userexception exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id          varchar2(6);

  ---------------------------------------------------------------------------------------

begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00165 t where t.month_id = v_month_id;
  commit;
  INSERT INTO CF_BUSIMG.T_COCKPIT_00165
    (month_id,
     TRACEABILITY_DEPT_ID,
     traceability_dept,
     UNDERTAKE_DEPT_ID,
     undertake_dept,
     ACCOUNT_CODE,
     ACCOUNT_NAME,
     allocated_money,
     allocated_date,
     allocated_project,
     allocated_peoject_detail)
    select t.month_id as busi_month,
           t.traceability_dept_id,
           t.traceability_dept as traceability_dept,
           t.oa_branch_id,
           x.oa_branch_name as undertake_dept,
           t.account_code,
           t.account_name as ACCOUNT_NAME,
           sum(t.bzjj) as allocated_money,
           to_char(sysdate,'yyyymmdd') as allocated_date,
           v_month_id || t.account_name as allocated_project,
           null as allocated_peoject_detail
      from (select *
              from CF_BUSIMG.T_COCKPIT_00128_DATA,
                   cf_busimg.t_cockpit_acount_func_rela) t
     INNER JOIN cf_busimg.t_ctp_branch_oa_rela x
        on t.ctp_branch_id = x.ctp_branch_id
     where t.month_id = v_month_id
       and t.func_id = v_op_object
     group by t.month_id,
              t.traceability_dept_id,
              t.traceability_dept,
              t.oa_branch_id,
              x.oa_branch_name,
              t.account_code,
              t.account_name;
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
