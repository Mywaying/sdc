use ods;
drop table if  exists ods.odstmp_bims_biz_isr_mixed_common;
CREATE TABLE ods.odstmp_bims_biz_isr_mixed_common (
  id STRING COMMENT '主键id',
   apply_no STRING COMMENT '订单编号',
   contract_no STRING,
   is_need_mortgage STRING COMMENT '是否需我司办理按揭',
   tail_release_node STRING COMMENT '尾款释放',
   tail_release_node_name STRING COMMENT '尾款释放名称',
   charging_time TIMESTAMP COMMENT '收费时间',
   issue_receipt_time TIMESTAMP COMMENT '开具收据时间',
   arrival_amount DOUBLE COMMENT '入账金额（元）',
   fund_type STRING COMMENT '资金类型',
   arrival_fund DOUBLE COMMENT '到账资金',
   arrival_time TIMESTAMP COMMENT '到账时间',
   remark STRING COMMENT '备注',
   risk_level STRING COMMENT '风险等级',
   risk_level_name STRING COMMENT '风险等级名称',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   bank_loan_amount DOUBLE COMMENT '银行放款金额',
   bank_loan_reasult STRING COMMENT '银行放款结果Y/N',
   bank_loan_reason STRING COMMENT '未放款原因',
   bank_loan_time TIMESTAMP,
   partners_user_name STRING COMMENT '合作机构用户名(注册用户名)',
   partners_account STRING COMMENT '合作机构电子账号',
   is_prestore STRING COMMENT '是否预存',
   prestore_day STRING COMMENT '预存天数',
   start_time TIMESTAMP COMMENT '发送放款指令的开始时间',
   end_time TIMESTAMP COMMENT '发送放款指令的结束时间',
   loan_expect_time TIMESTAMP COMMENT '预计放款时间（真融宝）',
   insurance_release_time TIMESTAMP COMMENT '保险责任解除时间',
   is_priority STRING COMMENT '是否加急',
   bank_tail_loan_amount DOUBLE COMMENT '尾款放款金额',
   bank_tail_loan_reasult STRING COMMENT '尾款放款结果',
   bank_tail_loan_reason STRING COMMENT '尾款未放款原因',
   tail_start_time TIMESTAMP COMMENT '尾款放款时间',
   tail_end_time TIMESTAMP COMMENT '尾款放款有效期',
   approve_result STRING COMMENT '审批结果',
   approve_info STRING COMMENT '审批信息',
   apply_loan_amount DOUBLE COMMENT '申请放款金额',
   apply_loan_tail_amount DOUBLE COMMENT '申请尾款释放金额',
   aaply_loan_tail_amount DOUBLE COMMENT '申请尾款释放金额',
   need_platform_send STRING COMMENT '是否需要平台放款',
   dataupdate_reject STRING COMMENT '是否通过数据修改流程做的驳回，Y:是，N:否',
   zlwc_remark STRING COMMENT '核保/审批资料外传节点的备注',
   is_automated_approve_done STRING COMMENT '判断资料外传节点的自动审批是否做了，使用01来判断',
   apply_order_attach_complete STRING COMMENT '订单所需资料是否齐全 0 不全 1 全',
   partner_attach_complete STRING COMMENT '合作机构所需资料是否齐全 0 不全 1 全',
   need_check STRING COMMENT '是否需要审批 Y 需要 N 不需要 默认为N',
   investigate_remark STRING COMMENT '审查意见和情况说明',
   available_loan_amount DOUBLE COMMENT '可贷额度',
   final_apply_loan_amount DOUBLE COMMENT '客户最终申请贷款额度',
   refuse_reason STRING COMMENT '否决原因',
   supply_reason STRING COMMENT '补件原因',
   dashu_approve_remark STRING COMMENT '大数审批备注信息',
   channel_mgr_no STRING COMMENT '半刻渠道经理编码',
   channel_mgr_name STRING COMMENT '半刻渠道经理名称',
   channel_mgr_team_no STRING COMMENT '半刻渠道经理单位编码',
   channel_mgr_team_name STRING COMMENT '半刻渠道经理单位名称',
   materials_upload_comment STRING COMMENT '资料上传意见',
   materials_upload_status STRING COMMENT '资料上传状态',
   overall_state STRING COMMENT '放款后整体状态',
   is_tfb STRING COMMENT '是否提放保业务',
   evaluate_value STRING COMMENT '半刻房产评估价格',
   appcredit_invert_time TIMESTAMP COMMENT '预约陪打征信时间?',
   credit_report_id STRING COMMENT '半刻征信报告ID',
   merchant_id STRING COMMENT '商户号',
   channel_mgr_tel STRING COMMENT '渠道经理手机号',
   apply_order_attach_complete_time TIMESTAMP COMMENT '订单所需资料齐全时间',
   partner_attach_complete_time TIMESTAMP COMMENT '合作机构所需资料齐全时间',
   loan_result STRING COMMENT '非对接机构放款结果',
   contract_amt DOUBLE COMMENT '合同金额',
   final_payment_arrival_fund DOUBLE COMMENT '尾款到账金额',
   final_payment_arrival_time TIMESTAMP COMMENT '尾款到账时间',
   is_partner_ding_msg_sent STRING COMMENT '资料齐全钉钉消息是否发送标记（0未发送、1已发送）',
   send_partner_ding_msg_time TIMESTAMP COMMENT '机构资料齐全钉钉消息发送时间',
   moo_inputcomple_date TIMESTAMP COMMENT '通知录入完成时间',
   has_moo_inputcomple STRING COMMENT '是否通知录入完成 0 未通知 1 已通知',
   account_password STRING COMMENT '账户密码',
   org_approve_result STRING COMMENT '非对接机构审批结果',
   org_approve_remark STRING COMMENT '非对接机构审批备注信息',
   interview_save_time TIMESTAMP COMMENT '面签保存时间',
   is_rob_red STRING COMMENT '是否已抢单池标红 1-标红',
   rob_red_time TIMESTAMP COMMENT '抢单池标红时间',
   is_rob_send_msg1 STRING COMMENT '是否已发消息给外勤岗 预约时间小于60分 1-已发送',
   is_rob_send_msg2 STRING COMMENT '是否已发消息给主管，预约时间小于45分 1-已发送',
   is_rob_send_msg3 STRING COMMENT '是否已发消息给主管，预约时间超过24小时 1-已发送',
   to_use_amount_flag STRING COMMENT '发送放款指令/放款指令推送待办列表变红标识',
   man_check_result STRING COMMENT '人工审核结果(中原银行)',
   finance_file_lock_time TIMESTAMP COMMENT '财务归档锁定时间',
   credit_channel STRING COMMENT '征信解析渠道',
   credit_unfinish STRING COMMENT '所有申请人征信是否查询未完成（0 均已完成 1 存在一个没完成）',
   rule_level STRING COMMENT '规则等级',
   investigate_order bigint comment '审查待认领事项排序字段',
   is_first_rob STRING COMMENT '是否第一次进入抢单池',
   has_gzstr STRING COMMENT '是否含有公证受托人',
   insurance_delivery_house_address STRING COMMENT '核保资料外传房产地址',
   insurance_delivery_house_cert_no STRING COMMENT '核保资料外传房产证号',
   contract_type STRING COMMENT '合同类型',
   is_turn_line STRING COMMENT '订单机构是否线下转线上（N：否，Y:是）',
   is_finished_outwork STRING COMMENT '是否完结外勤事项',
   is_send_cust_sign STRING COMMENT '是否发送客户签约合同成功（1成功，2失败，3合同撤销失败，null 未处理）' )  STORED AS parquet;
insert overwrite table odstmp_bims_biz_isr_mixed_common
select
a.`id`,
a.`apply_no`,
a.`contract_no`,
a.`is_need_mortgage`,
a.`tail_release_node`,
b.NAME_ tail_release_node_name,
a.`charging_time`,
a.`issue_receipt_time`,
a.`arrival_amount`,
a.`fund_type`,
a.`arrival_fund`,
a.`arrival_time`,
a.`remark`,
a.`risk_level`,
regexp_replace(regexp_replace(a.risk_level,'bzyw','	风险标准件'),'fbj','风险非标件') risk_level_name,
a.`create_user_id`,
a.`update_user_id`,
a.`create_time`,
a.`update_time`,
a.`rev`,
a.`delete_flag`,
a.`bank_loan_amount`,
a.`bank_loan_reasult`,
a.`bank_loan_reason`,
a.`bank_loan_time`,
a.`partners_user_name`,
a.`partners_account`,
a.`is_prestore`,
a.`prestore_day`,
a.`start_time`,
a.`end_time`,
a.`loan_expect_time`,
a.`insurance_release_time`,
a.`is_priority`,
a.`bank_tail_loan_amount`,
a.`bank_tail_loan_reasult`,
a.`bank_tail_loan_reason`,
a.`tail_start_time`,
a.`tail_end_time`,
a.`approve_result`,
a.`approve_info`,
a.`apply_loan_amount`,
a.`apply_loan_tail_amount`,
a.`aaply_loan_tail_amount`,
a.`need_platform_send`,
a.`dataupdate_reject`,
a.`zlwc_remark`,
a.`is_automated_approve_done`,
a.`apply_order_attach_complete`,
a.`partner_attach_complete`,
a.`need_check`,
a.`investigate_remark`,
a.`available_loan_amount`,
a.`final_apply_loan_amount`,
a.`refuse_reason`,
a.`supply_reason`,
a.`dashu_approve_remark`,
a.`channel_mgr_no`,
a.`channel_mgr_name`,
a.`channel_mgr_team_no`,
a.`channel_mgr_team_name`,
a.`materials_upload_comment`,
a.`materials_upload_status`,
a.`overall_state`,
a.`is_tfb`,
a.`evaluate_value`,
a.`appcredit_invert_time`,
a.`credit_report_id`,
a.`merchant_id`,
a.`channel_mgr_tel`,
a.`apply_order_attach_complete_time`,
a.`partner_attach_complete_time`,
a.`loan_result`,
a.`contract_amt`,
a.`final_payment_arrival_fund`,
a.`final_payment_arrival_time`,
a.`is_partner_ding_msg_sent`,
a.`send_partner_ding_msg_time`,
a.`moo_inputcomple_date`,
a.`has_moo_inputcomple`,
a.`account_password`,
a.`org_approve_result`,
a.`org_approve_remark`,
a.`interview_save_time`,
a.`is_rob_red`,
a.`rob_red_time`,
a.`is_rob_send_msg1`,
a.`is_rob_send_msg2`,
a.`is_rob_send_msg3`,
a.`to_use_amount_flag`,
a.`man_check_result`,
a.`finance_file_lock_time`,
a.`credit_channel`,
a.`credit_unfinish`,
a.`rule_level`,
a.`investigate_order`,
a.`is_first_rob`,
a.`has_gzstr`,
a.`insurance_delivery_house_address`,
a.`insurance_delivery_house_cert_no`,
a.`contract_type`,
a.`is_turn_line`,
a.`is_finished_outwork`,
a.`is_send_cust_sign`
FROM ods_bims_biz_isr_mixed a
left join (
select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where type_id_='10000022881903' or type_id_='10000039360006' or type_id_='10000039360008' or type_id_='10000039360009' or type_id_='10000039360010' or type_id_='10000047750219' or type_id_='10000026360013' or type_id_='10000027530016' or type_id_='10000027530016' or type_id_='10000027530011' or type_id_='10000027530018' or type_id_='10000027530014' or type_id_='10000047750219' or type_id_='10000096080009' or type_id_='10000026360013')b
on b.KEY_=lower(a.tail_release_node);

drop table if exists ods_bims_biz_isr_mixed_common;
ALTER TABLE odstmp_bims_biz_isr_mixed_common RENAME TO ods_bims_biz_isr_mixed_common;