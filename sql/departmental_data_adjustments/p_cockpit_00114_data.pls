CREATE OR REPLACE PROCEDURE CF_BUSIMG.p_cockpit_00114_data(i_busi_date   IN VARCHAR2,
                                                           o_return_msg  OUT VARCHAR2, --返回消息
                                                           o_return_code OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.2
  -- func_name
  -- func_remark            部门间数据调整表-数据落地
  -- create_date            20240826
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00114_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userexception EXCEPTION;
  ---------------------------------------------------------------------------------------
  --业务变量
  ---------------------------------------------------------------------------------------
BEGIN
  DELETE FROM cf_busimg.t_cockpit_00114_data t
   WHERE t.busi_date = i_busi_date;
  COMMIT;
  INSERT INTO cf_busimg.t_cockpit_00114_data
    (busi_date,
     fund_account_id,
     client_name,
     end_rights,
     remain_transfee,
     done_amount,
     done_money,
     fund_rate,
     rights_rate,
     done_rate,
     out_oa_branch_id,
     out_oa_branch_name,
     in_oa_branch_id,
     in_oa_branch_name,
     allocat_end_rights,
     allocat_remain_transfee,
     allocat_done_amount,
     allocat_done_money)
    WITH tmp AS
     (SELECT t.fund_account_id, --资金账号
             b.client_name, --客户名称
             t.rights AS end_rights, --期末权益
             t.remain_transfee  ,--留存手续费
             t.done_amount,--成交手数
             t.done_money --成交额
        FROM cf_stat.t_rpt_06008 t
       INNER JOIN cf_busimg.t_cockpit_00114 a
          ON t.fund_account_id = a.fund_account_id
         AND i_busi_date BETWEEN a.begin_date AND a.end_date
       INNER JOIN cf_sett.t_fund_account b
          ON a.fund_account_id = b.fund_account_id
       WHERE t.n_busi_date = i_busi_date)
    SELECT i_busi_date,
           t.fund_account_id, --资金账号
           t.client_name, --客户名称
           t.end_rights, --期末权益
           t.remain_transfee, --留存手续费,
           t.done_amount,
           t.done_money,
           a.fund_rate, --收入分配比例
           a.rights_rate,--权益分配比例
           a.done_rate,
           a.out_oa_branch_id,
           a.out_oa_branch_name, --划出部门
           a.in_oa_branch_id,
           a.in_oa_branch_name, --划入部门
           (t.end_rights * a.rights_rate) AS allocat_end_rights, --分配期末权益
           (t.remain_transfee * a.fund_rate) AS allocat_remain_transfee, --分配留存手续费
           (t.done_amount * a.done_rate) , -- 分配成交手数
           (t.done_money * a.done_rate) --分配成交额
      FROM tmp t
     INNER JOIN cf_busimg.t_cockpit_00114 a
        ON t.fund_account_id = a.fund_account_id;
  COMMIT;
  -------------------------------------------------------------
  o_return_code := 0;
  o_return_msg  := '执行成功';
  ---------------------------------------------------------------------------------------
  --错误处理部分
  ---------------------------------------------------------------------------------------
EXCEPTION
  WHEN v_userexception THEN
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
  WHEN OTHERS THEN
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
END;
