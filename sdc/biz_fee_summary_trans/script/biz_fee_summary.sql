use ods;
drop table if  exists ods.odstmp_bpms_biz_fee_summary_common;
CREATE TABLE ods.odstmp_bpms_biz_fee_summary_common (
   apply_no string comment '订单编号' ,
   product_term string comment '产品期限' ,
   is_same_bank string comment '是否同行' ,
   premium_summary double comment '汇总保费' ,
   premium_payment_party string comment '保费缴纳方' ,
   remove_insurance_time timestamp comment '解保时间' ,
   net_income_amount double comment '大道净收入(元)' ,
   service_fee_amount double comment '大道服务费' ,
   service_payment_party string comment '服务费缴纳方' ,
   total_amount double comment '缴费金额合计（元）' ,
   payment_way string comment '收费方式' ,
   has_invoice string comment '有无专票（0无1有）' ,
   price_tag string comment '价格标签' ,
   remark string comment '备注' ,
   create_user_id string comment '创建人id' ,
   update_user_id string comment '更新人id' ,
   create_time timestamp comment '记录创建时间' ,
   update_time timestamp comment '记录更新时间' ,
   rev bigint comment '版本号' ,
   delete_flag string comment '记录删除标识(1：删除；0：有效记录)' ,
   turnover_amount double comment '周转金额' ,
   borrowing_amount double comment '借款金额' ,
   ransom_borrow_amount double comment '赎楼金额' ,
   own_fund_amount double comment '卖方自有资金' ,
   refund_source string comment '还款来源' ,
   borrowing_value_date timestamp comment '借款起息日' ,
   borrowing_due_date timestamp comment '借款到期日' ,
   platform_due_date string comment '平台到期日' ,
   platform_value_date string comment '平台起息日' ,
   show_due_date timestamp comment '展示到期日' ,
   fund_use_date_num bigint comment '资金实质使用天数' ,
   contract_rate double comment '合同费率' ,
   contract_rate_total double comment '合同费率合计' ,
   floor_price_rate double comment '底价费率' ,
   floor_price_rate_total double comment '底价费率合计' ,
   fixed_term bigint comment '固定期限' ,
   product_term_and_charge_way string comment '产品期限及收费方式选择' ,
   rabate_rate double comment '返佣率' ,
   rate double comment '费率' ,
   approve_amount double comment '审批金额（真融宝）' ,
   pre_ransom_borrow_amount double comment '预计借款（赎楼）' ,
   pre_use_amount_date timestamp comment '预计用款时间' ,
   guarantee_amount double comment '保障金额（元）' ,
   down_payment_amount double comment '买方首付款金额（元）' ,
   risk_grade string comment '风险等级' ,
   end_bearing string comment '头尾计息（0否1是）' ,
   use_amount_date timestamp comment '用款时间' ,
   random_pay_mode string comment '赎楼款支付方式' ,
   tail_pay_mode string comment '尾款支付方式' ,
   tail_account_type string comment '尾款收款账户类型' ,
   seller_account_type string comment '卖方收款账户类型' ,
   tail_amount double comment '尾款金额' ,
   expect_use_time timestamp comment '客户预计用款时间' ,
   price_discount string comment '定价折扣' ,
   risk_coefficient string comment '风险系数' ,
   expect_cust_pay_day timestamp comment '预计借款回款日（客户）' ,
   fund_package_code string comment '资金包编码' ,
   insured_amount_cardinality string comment '保额计费基数' ,
   insurance_premium_cardinality string comment '保费计费基数' ,
   charging_cardinality string comment '收费计费基数',
   payment_way_name string comment '收费方式'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_fee_summary_common
SELECT
   bowd.apply_no
   ,bowd.product_term
   ,bowd.is_same_bank
   ,bowd.premium_summary
   ,bowd.premium_payment_party
   ,bowd.remove_insurance_time
   ,bowd.net_income_amount
   ,bowd.service_fee_amount
   ,bowd.service_payment_party
   ,bowd.total_amount
   ,bowd.payment_way
   ,bowd.has_invoice
   ,bowd.price_tag
   ,bowd.remark
   ,bowd.create_user_id
   ,bowd.update_user_id
   ,bowd.create_time
   ,bowd.update_time
   ,bowd.rev
   ,bowd.delete_flag
   ,bowd.turnover_amount
   ,bowd.borrowing_amount
   ,bowd.ransom_borrow_amount
   ,bowd.own_fund_amount
   ,bowd.refund_source
   ,bowd.borrowing_value_date
   ,bowd.borrowing_due_date
   ,bowd.platform_due_date
   ,bowd.platform_value_date
   ,bowd.show_due_date
   ,bowd.fund_use_date_num
   ,bowd.contract_rate
   ,bowd.contract_rate_total
   ,bowd.floor_price_rate
   ,bowd.floor_price_rate_total
   ,bowd.fixed_term
   ,bowd.product_term_and_charge_way
   ,bowd.rabate_rate
   ,bowd.rate
   ,bowd.approve_amount
   ,bowd.pre_ransom_borrow_amount
   ,bowd.pre_use_amount_date
   ,bowd.guarantee_amount
   ,bowd.down_payment_amount
   ,bowd.risk_grade
   ,bowd.end_bearing
   ,bowd.use_amount_date
   ,bowd.random_pay_mode
   ,bowd.tail_pay_mode
   ,bowd.tail_account_type
   ,bowd.seller_account_type
   ,bowd.tail_amount
   ,bowd.expect_use_time
   ,bowd.price_discount
   ,bowd.risk_coefficient
   ,bowd.expect_cust_pay_day
   ,bowd.fund_package_code
   ,bowd.insured_amount_cardinality
   ,bowd.insurance_premium_cardinality
   ,bowd.charging_cardinality
   ,osys.payment_way_name
FROM
   ods_bpms_biz_fee_summary bowd
   left join (select key_,name_new_ payment_way_name from ods.ods_bpms_sys_dic_common where type_id_='10000010570024') osys on osys.key_=lower(bowd.payment_way);
drop table if exists ods_bpms_biz_fee_summary_common;
ALTER TABLE odstmp_bpms_biz_fee_summary_common RENAME TO ods_bpms_biz_fee_summary_common;