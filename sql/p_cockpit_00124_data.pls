create or replace procedure cf_busimg.p_cockpit_00124_data(i_busi_date   in varchar2,
                                                           o_return_msg  out varchar2, --返回消息
                                                           o_return_code out integer --返回代码
                                                           ) is
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                p_cockpit_00124_data
  -- version                1.0
  -- func_name
  -- func_remark            投资咨询绩效提成分配表-数据落地
  -- create_date            20240827
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  /*
  入参条件
  月份
  客户编号/客户名称
  产品类型
  分配部门类型
  分配部门
  合同开始时间起始日
  合同开始时间结束日
  合同结束时间起始日
  合同结束时间结束日
  收款时间开始时间
  收款时间结束时间

  cf_busimg.t_cockpit_00122    投资咨询基本信息维护参数表-主表
  cf_busimg.t_cockpit_00122_1  投资咨询基本信息-内核分配比例-表1
  cf_busimg.t_cockpit_00122_2  投资咨询基本信息-绩效分配比例-表2
  */
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00124_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userexception exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_month_id varchar2(6);
  ---------------------------------------------------------------------------------------

begin
  v_month_id := substr(i_busi_date, 1, 6);
  delete from cf_busimg.t_cockpit_00124 t where t.month_id = v_month_id;
  commit;
  insert into cf_busimg.t_cockpit_00124
    (month_id,
     client_id,
     client_name,
     mobile,
     contract_number,
     product_name,
     product_type,
     product_risk_level,
     client_risk_level,
     contract_begin_date,
     contract_end_date,
     collection_time,
     invest_total_service_fee,
     perfor_total_rate,
     alloca_oa_branch_type,
     alloca_oa_branch_id,
     alloca_oa_branch_name,
     alloca_perfor_rate,
     broker_id,
     broker_name,
     broker_perfor_rate,
     broker_income)
    select t.busi_month, ----月份
           t.client_id, --客户编号
           t.client_name, --客户名称
           t.mobile, --联系电话
           t.contract_number, --合同编号
           t.product_name, --产品名称
           t.product_type, --产品类型
           t.product_risk_level, --产品风险等级
           t.client_risk_level, --客户风险等级
           t.contract_begin_date, --合同开始时间
           t.contract_end_date, --合同结束时间
           t.collection_time, --收款时间
           t.invest_total_service_fee, --投资咨询服务费总额(元)
           t.perfor_total_rate, --绩效总分配比例
           a.alloca_oa_branch_type, --分配部门类型
           a.alloca_oa_branch_id, --分配部门
           a.alloca_oa_branch_name, --分配部门
           a.alloca_perfor_rate, --部门绩效分配比例
           a.broker_id, --分配人员id
           a.broker_name, --分配人员
           a.broker_perfor_rate, --人员绩效分配比例
           t.invest_total_service_fee * t.perfor_total_rate *
           a.alloca_perfor_rate * a.broker_perfor_rate as broker_income --人员奖励 =投资咨询服务费总额(元)*绩效总分配比例*部门绩效分配比例*人员绩效分配比例
      from cf_busimg.t_cockpit_00122 t
      left join cf_busimg.t_cockpit_00122_2 a
        on t.busi_month = a.busi_month
       and t.client_id = a.client_id
       and t.product_name = a.product_name
     where (t.busi_month = v_month_id );
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
