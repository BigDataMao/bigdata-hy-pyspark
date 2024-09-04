CREATE OR REPLACE PROCEDURE cf_busimg.p_cockpit_00208_data(i_busi_date   IN VARCHAR2,
                                                           o_return_msg  OUT VARCHAR2, --返回消息
                                                           o_return_code OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.2
  -- func_name
  -- func_remark            场外期权清算台账-数据落地
  -- create_date            20240826
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object VARCHAR2(50) DEFAULT 'P_COCKPIT_00208_DATA'; -- '操作对象';
  v_error_msg VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userexception EXCEPTION;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id VARCHAR2(6);
  ---------------------------------------------------------------------------------------
BEGIN
  v_month_id := substr(i_busi_date, 1, 6);
  DELETE FROM cf_busimg.t_cockpit_00208 t WHERE t.month_id = v_month_id;
  COMMIT;
  INSERT INTO cf_busimg.t_cockpit_00208
    (month_id,
     add_date,
     client_id,
     client_name,
     open_date,
     branch_id,
     branch_name,
     introductor_id,
     introductor,
     trade_begin_date,
     trade_orders,
     premium_abs,
     actual_notional_principal,
     sale_income,
     colla_pricing,
     total_income)
    SELECT v_month_id,
           t.add_date,
           t.client_id,
           t.client_name,
           t.open_date,
           t.branch_id,
           t.branch_name,
           t.introductor_id,
           t.introductor,
           t.trade_begin_date,
           t.trade_orders,
           t.premium_abs,
           t.actual_notional_principal,
           t.sale_income,
           t.colla_pricing,
           t.total_income
    FROM   cf_busimg.t_cockpit_00207 t
    WHERE  substr(t.add_date, 1, 6) = v_month_id;
  COMMIT;
  -------------------------------------------------------------
  o_return_code := 0;
  o_return_msg := '执行成功';
  ---------------------------------------------------------------------------------------
  --错误处理部分
  ---------------------------------------------------------------------------------------
EXCEPTION
  WHEN v_userexception THEN
    o_return_code := v_error_code;
    o_return_msg := v_error_msg;
    ROLLBACK;
    wolf.p_error_log('admin'
                    , -- '操作人';
                     v_op_object
                    , -- '操作对象';
                     v_error_code
                    , --'错误代码';
                     v_error_msg
                    , -- '错误信息';
                     ''
                    ,'1'
                    ,o_return_msg
                    , --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
  WHEN OTHERS THEN
    o_return_code := SQLCODE;
    o_return_msg := o_return_msg || SQLERRM;
    ROLLBACK;
    v_error_msg := o_return_msg;
    v_error_code := o_return_code;
    wolf.p_error_log('admin'
                    , -- '操作人';
                     v_op_object
                    , -- '操作对象';
                     v_error_code
                    , --'错误代码';
                     v_error_msg
                    , -- '错误信息';
                     ''
                    ,'2'
                    ,o_return_msg
                    , --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
END;
