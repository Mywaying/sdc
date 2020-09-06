use ods;
drop table if  exists ods.odstmp_bpms_biz_apply_order_extend_common;
CREATE TABLE ods.odstmp_bpms_biz_apply_order_extend_common (
   id string comment 'id' ,
   apply_no string comment '订单编号' ,
   create_time timestamp comment '创建时间' ,
   update_time timestamp comment '更新时间' ,
   create_user_id string comment '创建用户ID' ,
   update_user_id string comment '更新用户Id' ,
   rev bigint comment '版本' ,
   delete_flag string comment '删除标识（0 未删除 1已删除）' ,
   has_new_loan_apply string comment '是否已做新贷款申请（Y：是 N：否）' ,
   accompany_new_loan_apply string comment '是否需要我司陪同新贷款申请（Y：是 N：否）' ,
   handle_entrust_notarial string comment '是否办理委托公证(Y：是，N：否)' ,
   deposit double comment '定金' ,
   tail_payment double comment '尾款' ,
   first_payment double comment '首付款' ,
   loan_offer double comment '贷款申报价（元）' ,
   transfer_offer double comment '过户申报价（元）' ,
   identity_untrusteeship_policy string comment '是否符合身份证免托管政策(Y:是，N:否)' ,
   identity_untrusteeship_apply string comment '是否申请身份证免托管(Y:是，N:否)' ,
   certificate_keep_location string comment '产证保管位置' ,
   certificate_keep string comment '产证保管 (1:自留，2:银行/其他机构)' ,
   house_limit_year bigint comment '房产年限（年）' ,
   sale_cause string comment '售房原因(1: 改善性需求 2 生意周转 0: 其他)' ,
   other_sale_cause string comment '其他销售原因' ,
   before_approve_result string comment '微众额度激活前审核结果(核验通过：passed、核验否决：refused、补件：supplyed)' ,
   activate_time timestamp comment '激活时间' ,
   activate_refuse_reason string comment '微众额度激活否决原因' ,
   supply_materials string comment '补件内容' ,
   expect_limit double comment '预估额度' ,
   wdd_expect_limit double comment '微抵贷预估额度' ,
   wdd_limit double comment '微抵贷额度' ,
   final_limit double comment '终审额度' ,
   approval_auto_rule string comment '最近一次自动审批的方法' ,
   apply_source string comment '进件来源' ,
   profit_exceed double comment '财务归档的金额校验' ,
   financial_time timestamp comment '原财务归档时间' ,
   append_guarantee_id string comment '追加担保人id逗号隔开' ,
   append_guarantee_name string comment '追加担保人姓名逗号隔开',
   certificate_keep_name string comment '产证保管 (1:自留，2:银行/其他机构)'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_apply_order_extend_common
SELECT
   bowd.id
   ,bowd.apply_no
   ,bowd.create_time
   ,bowd.update_time
   ,bowd.create_user_id
   ,bowd.update_user_id
   ,bowd.rev
   ,bowd.delete_flag
   ,bowd.has_new_loan_apply
   ,bowd.accompany_new_loan_apply
   ,bowd.handle_entrust_notarial
   ,bowd.deposit
   ,bowd.tail_payment
   ,bowd.first_payment
   ,bowd.loan_offer
   ,bowd.transfer_offer
   ,bowd.identity_untrusteeship_policy
   ,bowd.identity_untrusteeship_apply
   ,bowd.certificate_keep_location
   ,bowd.certificate_keep
   ,bowd.house_limit_year
   ,bowd.sale_cause
   ,bowd.other_sale_cause
   ,bowd.before_approve_result
   ,bowd.activate_time
   ,bowd.activate_refuse_reason
   ,bowd.supply_materials
   ,bowd.expect_limit
   ,bowd.wdd_expect_limit
   ,bowd.wdd_limit
   ,bowd.final_limit
   ,bowd.approval_auto_rule
   ,bowd.apply_source
   ,bowd.profit_exceed
   ,bowd.financial_time
   ,bowd.append_guarantee_id
   ,bowd.append_guarantee_name
   ,replace(replace(bowd.certificate_keep,'1','自留'),'2','银行/其他机构')
FROM
   ods_bpms_biz_apply_order_extend bowd;
drop table if exists ods_bpms_biz_apply_order_extend_common;
ALTER TABLE odstmp_bpms_biz_apply_order_extend_common RENAME TO ods_bpms_biz_apply_order_extend_common;