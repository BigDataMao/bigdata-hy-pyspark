CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00128_DATA(I_BUSI_DATE   IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.2
  -- func_name
  -- func_remark            投资者保障基金调整表-数据落地
  -- create_date            20240826
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量

  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00128_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userException exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id varchar2(6);
  ---------------------------------------------------------------------------------------
BEGIN
  v_month_id := substr(I_BUSI_DATE, 1, 6);
  delete from CF_BUSIMG.T_COCKPIT_00128_data t
   where t.month_id = v_month_id;
  commit;
  INSERT INTO CF_BUSIMG.T_COCKPIT_00128_data
    (month_id, ctp_branch_id, oa_branch_id, done_money, bzjj)
    select T.Month_Id, --月份
           t.branch_id, --部门ID
           x.oa_branch_id, --部门
           sum(NVL(t.DONE_MONEY, 0)) AS DONE_MONEY, --成交金额
           sum(nvl(t.secu_fee, 0)) as bzjj
      from CF_BUSIMG.T_COCKPIT_CLIENT_REVENUE t
     INNER JOIN cf_busimg.t_ctp_branch_oa_rela x
        on t.branch_id = x.ctp_branch_id
     WHERE T.Month_Id = v_month_id
       and t.is_main = '1'
     group by t.month_id, t.branch_id, x.oa_branch_id;
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
