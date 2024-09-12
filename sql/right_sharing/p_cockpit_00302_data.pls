create or replace procedure cf_busimg.p_cockpit_00302_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00302_data
  -- version                1.0
  -- func_name
  -- func_remark            权益溯源_ib驻点-数据落地
  -- create_date            20240904
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00302_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userexception exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id   varchar2(6);
  ---------------------------------------------------------------------------------------

begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00301 t
   where t.month_id = v_month_id
     and t.adjust_proj_id = v_op_object;
  commit;
  insert into CF_BUSIMG.T_COCKPIT_00301
    (month_id,
     adjust_proj_id,
     adjust_proj,
     src_branch_id,
     src_branch_name,
     branch_id,
     branch_name,
     index_id,
     index_name,
     adjust_value)
    select t.month_id,
           a.adjust_proj_id,
           a.adjust_proj,
           '',
           '',
           c.oa_branch_id,
           c.oa_branch_name,
           a.index_id,
           a.index_name,
           case
             when a.index_id = '01' then
              sum(t.avg_rights)
             when a.index_id = '02' then
              sum(t.end_rights)
             when a.index_id = '03' then
              sum(t.begin_rights)
           end
      from CF_BUSIMG.T_COCKPIT_00140           t,
           CF_BUSIMG.T_COCKPIT_PROJ_INDEX_RELA a,
           cf_busimg.t_ctp_branch_oa_rela      c
     where t.month_id = v_month_id
       and a.adjust_proj_id = v_op_object
       and t.ctp_branch_id = c.ctp_branch_id
     group by t.month_id,
              a.adjust_proj_id,
              a.adjust_proj,
              c.oa_branch_id,
              c.oa_branch_name,
              a.index_id,
              a.index_name;

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
