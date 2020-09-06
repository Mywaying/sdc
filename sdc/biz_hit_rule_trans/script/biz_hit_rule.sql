use ods;
drop table if exists odstmp_bpms_biz_hit_rule_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_hit_rule_common (
   id STRING COMMENT '主键id',
   apply_no STRING COMMENT '订单编号',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   rule_name STRING COMMENT '命中规则名称',
   rule_name_tag bigint COMMENT '命中规则名称标签',
   hit_message STRING,
   rule_type STRING COMMENT '命中规则类型',
   prompt_message STRING COMMENT '提示信息',
   flow_node STRING COMMENT '流程节点编码'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_hit_rule_common
SELECT
a.id,
a.apply_no,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.rule_name,
case
when a.rule_name='【基本信息】证件有效期低于6个月（交易类）' then 1
when a.rule_name='【房产】房产份额总和不等于100%' then 2
when a.rule_name='【诉讼】个人存在诉讼信息（单篇）' then 3
when a.rule_name='【基本信息】征信报告解析失败' then 4
when a.rule_name='【征信】信贷存在当期逾期（贷款）' then 5
when a.rule_name='【征信】信贷存在当期逾期（贷记卡）' then 6
when a.rule_name='【征信】信贷存在当期逾期（准贷记卡）' then 7
when a.rule_name='【征信】担保贷款五级分类状态异常' then 8
when a.rule_name='【征信】信贷状态异常（贷款状态）' then 9
when a.rule_name='【征信】信贷状态异常（贷记卡）' then 10
when a.rule_name='【征信】信贷状态异常（准贷记卡）' then 11
when a.rule_name='【征信】信贷近24期内还款状态出现D/G/Z（贷款状态）' then 12
when a.rule_name='【征信】信贷近24期内还款状态出现D/G/Z（贷记卡）' then 13
when a.rule_name='【 赎楼E（交易）】一年贷款内存在M2以上的逾期' then 14
when a.rule_name='【 赎楼E（交易）】一年贷记卡内存在M2以上的逾期' then 15
when a.rule_name='【 赎楼E（交易）】近二年贷款存在M4以上逾期' then 16
when a.rule_name='【 赎楼E（交易）】近二年贷记卡存在M4以上逾期' then 17
when a.rule_name='【 赎楼E（交易）】近二年准贷记卡存在M4以上逾期' then 18
when a.rule_name='【基本信息】卖方存在融资、小贷、担保、典当、资产管理、投资的从业记录' then 19
when a.rule_name='【赎楼E（有交易）】赎楼金额超过原贷款剩余本金的105%' then 20
when a.rule_name='【赎楼E（有交易）】赎楼金额150万元（含）以下，赎楼金额占交易房产成交总价比例高于70%' then 21
when a.rule_name='【诉讼】身份证上的居住地址分词匹配（全文）' then 22
when a.rule_name='【诉讼】单位名称分词匹配（全文）' then 23
when a.rule_name='【负债】综合负债超标（现金类）' then 24
when a.rule_name='【交易】|成交价格-评估价格|/成交价格>0.2且信用卡负债超标（交易类）' then 25
when a.rule_name='【赎楼E（有交易）】赎楼金额高于新贷款金额' then 26
when a.rule_name='【交易保（有赎楼）】赎楼金额不得超过原贷款剩余本金的105%' then 27
when a.rule_name='【诉讼】公司存在诉讼信息（单篇）' then 28
when a.rule_name='【征信】未结清贷款的到期日期早于当前时间且本金余额不等于0' then 29
when a.rule_name='【交易】卖方与买方的工作单位相同（交易类）' then 30
when a.rule_name='【交易保（有赎楼）】一年贷记卡内存在M2以上的逾期' then 31
when a.rule_name='【交易保（有赎楼）】近二年贷记卡存在M4以上逾期' then 32
when a.rule_name='[TD_FLOW]3个月内申请人在多个平台申请借款超过2次' then 33
when a.rule_name='【诉讼】身份证出生日期全文匹配（全文）' then 34
when a.rule_name='【负债】（近3月存在小贷借款）且（近6月信用卡透支20万且比例超50%）' then 35
when a.rule_name='【基本信息】证件有效期低于3个月（非交易类）' then 36
when a.rule_name='【赎楼E（非交易）】赎楼金额超过原贷款剩余本金的105%' then 37
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额300万）' then 38
when a.rule_name='[TD_FLOW]借款人（身份证+姓名）命中法院结案模糊名单' then 39
when a.rule_name='【交易保（有赎楼）】单笔贷款半年内 M1超过2次' then 40
when a.rule_name='【交易保（有赎楼）】赎楼金额200万元（不含）以上，赎楼金额占交易房产成交总价比例不得高于55%' then 41
when a.rule_name='【赎楼E（非交易）】赎楼金额占交易房产评估价比例高于50%' then 42
when a.rule_name='【交易保（有赎楼）】赎楼金额300万元（不含）以上，赎楼金额占交易房产成交总价比例超过55%' then 43
when a.rule_name='[TD_FLOW]借款人（身份证+姓名）命中法院失信模糊名单' then 44
when a.rule_name='【赎楼E（非交易）】赎楼金额高于新贷款金额' then 45
when a.rule_name='[TD_FLOW]3个月内第一联系人在多个平台被放款_不包含本合作方' then 46
when a.rule_name='[TD_FLOW]3个月内第一联系人在多个平台申请借款超过2次' then 47
when a.rule_name='【交易】卖买双方存在身份证号地域信息一致（交易类）' then 48
when a.rule_name='【负债】（近3月存在小贷借款）或（近6月信用卡透支20万且比例超50%）' then 49
when a.rule_name='【交易保（有赎楼）】近二年贷款存在M4以上逾期' then 50
when a.rule_name='【 赎楼E（交易）】单笔贷款半年内 M1超过2次' then 51
when a.rule_name='【赎楼E（有交易）】赎楼金额200万元（不含）以上，赎楼金额占交易房产成交总价比例高于55%' then 52
when a.rule_name='【赎楼E（有交易）】赎楼金额300万元（不含）以上，赎楼金额占交易房产成交总价比例高于55%' then 53
when a.rule_name='[TD_FLOW]3个月内第二联系人在多个平台被放款_不包含本合作方' then 54
when a.rule_name='【负债】近3月存在小贷借款' then 55
when a.rule_name='【交易】|成交价格-评估价格|/成交价格>0.2且近3个月存在小贷借款（交易类）' then 56
when a.rule_name='【及时贷（交易）】单笔贷款半年内 M1超过1次' then 57
when a.rule_name='[TD_FLOW]3个月内第二联系人在多个平台申请借款超过2次' then 58
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额150万）' then 59
when a.rule_name='【赎楼E（非交易）】赎楼金额占交易房产评估价比例高于70%' then 60
when a.rule_name='【基本信息】年龄小于18或者大于65周岁（交易类）' then 61
when a.rule_name='【交易】原贷款机构不为银行或公积金中心' then 62
when a.rule_name='【 赎楼E（非交易）】一年内贷记卡存在 M2及以上' then 63
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额500万）' then 64
when a.rule_name='【及时贷（交易）】借款金额超标（限额300万）' then 65
when a.rule_name='[TD_FLOW]3个月内第三联系人在多个平台被放款_不包含本合作方' then 66
when a.rule_name='【征信】近三个月内征信查询次数超过5次' then 67
when a.rule_name='【及时贷（交易）】借款金额超过买方按揭金额' then 68
when a.rule_name='【交易保（有赎楼）】单笔贷款半年内 M1超过1次' then 69
when a.rule_name='【及时贷（交易）】近一年内贷记卡存在M2以上的逾期' then 70
when a.rule_name='【及时贷（交易）】近二年贷记卡存在M4以上逾期' then 71
when a.rule_name='【及时贷（交易）】借款金额占交易房产成交总价比例不得高于70%' then 72
when a.rule_name='【赎楼E（有交易）】借款金额超标（限额300万）' then 73
when a.rule_name='【房产】房产共有人数大于等于3人' then 74
when a.rule_name='【赎楼E（有交易）】赎楼金额200万元（含）以下，赎楼金额占交易房产成交总价比例高于70%' then 75
when a.rule_name='【赎楼E（非交易）】房产类型为商铺、别墅、写字楼时，赎楼金额占房产评估价比例高于50%' then 76
when a.rule_name='【负债】近6月信用卡透支20万且比例超50%' then 77
when a.rule_name='[TD_FLOW]3个月内第四联系人在多个平台被放款_不包含本合作方' then 78
when a.rule_name='[TD_FLOW]3个月内第四联系人在多个平台申请借款超过2次' then 79
when a.rule_name='【 赎楼E（交易）】单笔贷记卡半年内 M1超过2次' then 80
when a.rule_name='【交易保（有赎楼）】单笔贷记卡半年内 M1超过1次' then 81
when a.rule_name='【 赎楼E（非交易）】单笔贷款一年内 M1超过3次' then 82
when a.rule_name='【交易保（有赎楼）】赎楼金额+按揭尾款之和占交易房产成交总价比例不得高于70%' then 83
when a.rule_name='【 赎楼E（非交易）】二年内准贷记卡存在 M3及以上' then 84
when a.rule_name='【交易保（有赎楼）】房产为住宅，赎楼金额+按揭尾款之和占交易房产成交总价比例超过70%' then 85
when a.rule_name='【赎楼E（有交易）】赎楼金额150万元（不含）以上，赎楼金额占交易房产成交总价比例高于55%' then 86
when a.rule_name='【负债】综合负债超标（保险类）' then 87
when a.rule_name='[TD_FLOW]借款人（身份证+姓名）命中法院执行模糊名单' then 88
when a.rule_name='【及时贷（交易）】借款金额300万元（不含）以上，借款金额占交易房产成交总价比例不得高于55%' then 89
when a.rule_name='【 赎楼E（非交易）】二年内贷记卡存在 M4及以上' then 90
when a.rule_name='【及时贷（交易）】单笔贷记卡半年内 M1超过1次' then 91
when a.rule_name='【 赎楼E（非交易）】一年内贷款存在 M2及以上' then 92
when a.rule_name='【 赎楼E（非交易）】二年内贷款存在 M3及以上' then 93
when a.rule_name='[TD_FLOW]第一联系人（身份证）命中法院执行名单' then 94
when a.rule_name='【赎楼E（有交易）】交易房产为住宅，赎楼金额占交易房产成交总价比例高于70%' then 95
when a.rule_name='【及时贷（交易）】借款金额150万元（不含）以上，借款金额占交易房产成交总价比例不得高于55%' then 96
when a.rule_name='[TD_FLOW]共同借款人（身份证+姓名）命中法院结案模糊名单' then 97
when a.rule_name='【赎楼E（有交易）】赎楼金额300万元（含）以下，赎楼金额占交易房产成交总价比例高于70%' then 98
when a.rule_name='【交易保（有赎楼）】赎楼金额+按揭尾款之和超标（限额300万）' then 99
when a.rule_name='[TD_FLOW]共同借款人（身份证+姓名）命中法院执行模糊名单' then 100
when a.rule_name='【赎楼E（非交易）】房产类型为住宅时，赎楼金额占房产评估价比例高于60%' then 101
when a.rule_name='[TD_FLOW]3个月内共同借款人在多个平台被放款_不包含本合作方' then 102
when a.rule_name='【房产】房产类型为商铺' then 103
when a.rule_name='[TD_FLOW]第一联系人（身份证+姓名）命中法院执行模糊名单' then 104
when a.rule_name='[TD_FLOW]3个月内第三联系人在多个平台申请借款超过2次' then 105
when a.rule_name='【征信】存在客户解析征信报告失败' then 106
when a.rule_name='【交易保（有赎楼）】赎楼金额150万元（不含）以上，赎楼金额占交易房产成交总价比例不得高于55%' then 107
when a.rule_name='【交易保（有赎楼）】赎楼金额150万元（含）以下，赎楼金额占交易房产成交总价比例超过70%' then 108
when a.rule_name='【赎楼E（有交易）】借款金额超标（限额1000万）' then 109
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额600万）' then 110
when a.rule_name='【交易保（无赎楼）】申请金额超标（上限800万）' then 111
when a.rule_name='【基本信息】年龄小于18或者大于70周岁（非交易类）' then 112
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额200万）' then 113
when a.rule_name='【综合】客户近三个月内小贷查询次数超过2次' then 114
when a.rule_name='【其他】3个月内申请人在多个平台申请借款超过2次' then 115
when a.rule_name='【提放保（无赎楼）】申请金额高于700万元' then 116
when a.rule_name='【及时贷（交易）】借款金额超标（限额500万）' then 117
when a.rule_name='【及时贷（交易）】借款金额200万元（不含）以上，借款金额占交易房产成交总价比例不得高于55%' then 118
when a.rule_name='【及时贷（交易）】借款金额超标（限额800万）' then 119
when a.rule_name='【赎楼E（有交易）】赎楼金额占交易房产成交总价比例高于70%' then 120
when a.rule_name='【及时贷（交易）】借款金额150万元（含）以下，借款金额占交易房产成交总价比例超过70%' then 121
when a.rule_name='【基本信息】证件类型不为大陆身份证或军官证' then 122
when a.rule_name='【提放保（有赎楼）】赎楼金额占房产评估价比例不得高于50%' then 123
when a.rule_name='【交易保（有赎楼）】赎楼金额300万元（含）以下，赎楼金额占交易房产成交总价比例超过70%' then 124
when a.rule_name='【赎楼E（有交易）】借款金额超标（限额500万）' then 125
when a.rule_name='【其他】借款人（身份证+姓名）命中法院结案模糊名单' then 126
when a.rule_name='【及时贷（交易）】借款金额200万元（含）以下，借款金额占交易房产成交总价比例超过70%' then 127
when a.rule_name='【交易保（有赎楼）】一年贷款内存在M2以上的逾期' then 128
when a.rule_name='【交易保（有赎楼）】赎楼金额+按揭尾款之和超标（限额800万）' then 129
when a.rule_name='【 赎楼E（非交易）】单笔贷记卡一年内 M1超过3次' then 130
when a.rule_name='【其他】借款人（身份证+姓名）命中法院执行模糊名单' then 131
when a.rule_name='【赎楼E（非交易）】借款金额超标（限额400万）' then 132
when a.rule_name='【其他】借款人（身份证+姓名）命中法院失信模糊名单' then 133
when a.rule_name='【其他】3个月内借款人在多个平台被放款_不包含本合作方' then 134
when a.rule_name='【征信】征信报告创建时间过期' then 135
when a.rule_name='【及时贷（交易）】借款金额300万元（含）以下，借款金额占交易房产成交总价比例超过70%' then 136
when a.rule_name='【交易保（有赎楼）】赎楼金额200万元（含）以下，赎楼金额占交易房产成交总价比例超过70%' then 137
when a.rule_name='【提放保（有赎楼）】一年贷记卡内存在M2以上的逾期' then 138
when a.rule_name='【提放保（有赎楼）】近二年贷记卡存在M4以上逾期' then 139
when a.rule_name='【提放保（有赎楼）】赎楼金额超过原贷款剩余本金的105%' then 140
when a.rule_name='【及时贷（非交易）】及时贷金额+赎楼金额之和超过新贷款金额的90%' then 141
when a.rule_name='【及时贷（非交易）】及时贷金额+赎楼金额总额超标（限额300万）' then 142
when a.rule_name='【征信】信贷状态异常（五级分类状态非正常贷款）' then 143
when a.rule_name='【及时贷（非交易）】及时贷金额+赎楼金额总额超标（限额400万）' then 144
when a.rule_name='【交易保（无赎楼）】申请金额超标（上限500万）' then 145
when a.rule_name='【提放保（有赎楼）】房产类型为住宅，赎楼金额占房产评估价比例超过60%' then 146
when a.rule_name='【及时贷（非交易）】及时贷金额+赎楼金额之和超过房产评估价70%' then 147
when a.rule_name='【及时贷（交易）】借款金额超过买方按揭贷款金额+监管资金金额' then 148
when a.rule_name='【负债】综合负债超标' then 149
when a.rule_name='【交易】房价/评估价>1.2' then 150
when a.rule_name='【基本信息】证件类型不为大陆身份证或军官证或士兵证' then 151
when a.rule_name='【其他】借款人（身份证）命中法院执行名单' then 152
when a.rule_name='【提放保（有赎楼）】赎楼金额+尾款之和不得高于700万元' then 153
when a.rule_name='【买付保】单笔贷款半年内 M1超过2次' then 154
when a.rule_name='【 赎楼E（非交易）】二年内贷记卡存在 M3及以上' then 155
when a.rule_name='【房产】房产类型为商铺（买付保）' then 156
when a.rule_name='【订单信息】房价/评估价超标' then 157
when a.rule_name='【订单信息】赎楼金额（或及时贷借款金额）占回款来源比例超标' then 158
when a.rule_name='【订单信息】原贷款机构不为银行或公积金中心' then 159
when a.rule_name='【基本信息】证件有效期过低' then 160
when a.rule_name='【订单信息】赎楼金额（或及时贷借款金额）占交易房产成交总价比例超标' then 161
when a.rule_name='【综合】有异常负债情况' then 162
when a.rule_name='【订单信息】单笔金额超上限' then 163
when a.rule_name='【房产】房产类型不符合规定' then 164
when a.rule_name='【基本信息】申请人存在融资、小贷、担保、典当、资产管理、投资的从业记录' then 165
when a.rule_name='【法诉】失信老赖名单——存在失信老赖名单' then 166
when a.rule_name='【法诉】执行公开信息——“立案时间”小于2年,“执行标的”＞20000元' then 167
when a.rule_name='【订单信息】赎楼金额不得超过原贷款剩余本金的105%' then 168
when a.rule_name='【法诉】执行公开信息——“立案时间”小于5年，大于2年,“执行标的”＞50000元' then 169
when a.rule_name='【基本信息】年龄超标' then 170
when a.rule_name='【征信】单笔贷款存在 M1超标' then 171
when a.rule_name='【征信】单笔贷记卡存在 M2超标' then 172
when a.rule_name='【法诉】执行公开信息——“立案时间”大于5年,“执行标的”＞100000元' then 173
when a.rule_name='【基本信息】证件类型不为大陆身份证' then 174
when a.rule_name='【法诉】法诉审判信息——四年内存在审判流程' then 175
when a.rule_name='【法诉】法诉裁判文书——“结案时间”为三年内,诉讼地位为被告，非借贷类案件“涉案金额”大于等于5万元' then 176
when a.rule_name='【法诉】犯罪嫌疑人名单——存在犯罪嫌疑人名单' then 177
when a.rule_name='【征信】单笔贷记卡存在 M1超标' then 178
when a.rule_name='【法诉】法诉裁判文书——“结案时间”为三年内,诉讼地位为被告，存在借贷类案件' then 179
when a.rule_name='【征信】近二年准贷记卡存在M4以上逾期' then 180
when a.rule_name='【征信】单笔贷款存在 M2超标' then 181
when a.rule_name='【征信】近二年贷款存在M4以上逾期' then 182
when a.rule_name='【征信】近二年贷记卡存在M4以上逾期' then 183
when a.rule_name='【其他】借款人（身份证）命中法院失信名单' then 184
when a.rule_name='【前海】查询失败' then 185
when a.rule_name='命中社工库号码' then 186
when a.rule_name='命中空号（非正常短信语音号码）' then 187
when a.rule_name='【法诉】限制高消费名单——存在限制高消费名单' then 188
when a.rule_name='【法诉全文】法诉裁判文书——未找到诉讼地位' then 189
when a.rule_name='【法诉全文】法诉裁判文书——“结案时间”为三年内,诉讼地位为被告' then 190
when a.rule_name='【法诉全文】存在未知案件类型' then 191
when a.rule_name='【法诉全文】法诉裁判文书——“结案时间”为三年内,诉讼地位为原告，借贷类案件大于一件' then 192
when a.rule_name='【法诉全文】法诉审判信息——未找到案由' then 193
when a.rule_name='【法诉全文】法诉审判信息——四年内存在审判流程，且为被告' then 194
when a.rule_name='【法诉全文】执行公开信息——“立案时间”小于2年,“执行标的”＞20000元' then 195
when a.rule_name='【法诉全文】限制高消费名单——存在限制高消费名单' then 196
when a.rule_name='【法诉全文】执行公开信息——“立案时间”小于5年，大于2年,“执行标的”＞50000元' then 197
when a.rule_name='【法诉全文】法诉审判信息——四年内存在审判流程，且为借贷类案件' then 198
when a.rule_name='命中第三方标注黑名单' then 199
when a.rule_name='【法诉全文】执行公开信息——未找到执行标的信息' then 200
when a.rule_name='【法诉全文】查询异常' then 201
when a.rule_name='【法诉】查询异常' then 202
when a.rule_name='【法诉全文】存在关注案件类型' then 203
when a.rule_name='【前海】近三个月机构查询次数大于两次（不包含银行）' then 204
when a.rule_name='【法诉全文】法诉裁判文书——未找到案由' then 205
when a.rule_name='房产所在区域与报单城市不一致' then 206
when a.rule_name='【房产】土地类型非住宅用地' then 207
when a.rule_name='【基本信息】卖买双方存在身份证号地域信息一致' then 208
when a.rule_name='【前海】近三个月机构查询次数大于五次（不包含银行）' then 209
when a.rule_name='【法诉全文】执行公开信息——“立案时间”大于5年,“执行标的”＞100000元' then 210
when a.rule_name='命中欺诈号码' then 211
when a.rule_name='【订单信息】新贷款借款人非个人' then 212
when a.rule_name='【订单信息】原贷款借款人非个人' then 213
when a.rule_name='【订单信息】新贷款机构为非银行' then 214
when a.rule_name='【订单信息】原贷款余额大于等于新贷款金额' then 215
when a.rule_name='【征信】征信无相应贷款' then 216
when a.rule_name='【其他】客户近三个月小贷放款次数超标' then 217
when a.rule_name='【其他】客户近三个月小贷查询次数超过2次' then 218
when a.rule_name='【订单信息】及时贷非交易赎楼、提放放款节点为审批即放（无同贷）' then 219
when a.rule_name='【订单信息】产权人非个人' then 220
when a.rule_name='【订单信息】借款金额高于新贷款金额（保险类）' then 221
when a.rule_name='【其他】客户近三个月小贷查询次数超过6次' then 222
when a.rule_name='【非标件】借款金额上限大于500万' then 223
when a.rule_name='【非标件】借款金额上限大于800万' then 224
when a.rule_name='【订单信息】原贷款余额大于新贷款金额' then 225
when a.rule_name='【法诉】客户名下公司存在疑似法诉信息' then 226
when a.rule_name='【征信】客户征信信息不存在' then 227
when a.rule_name='【法诉】欠款欠费名单——存在欠款欠费名单' then 228
when a.rule_name='【其他】客户近三个月小贷查询次数超标' then 229
when a.rule_name='【红线】信贷当期存在M2以上逾期（贷款）' then 230
when a.rule_name='命中收码平台号码' then 231
when a.rule_name='【红线】征信存在担保代偿信息' then 232
when a.rule_name='【红线】信贷当期存在M2以上逾期（贷记卡）' then 233
when a.rule_name='【红线】贷款五级分类状态异常' then 234
when a.rule_name='【红线】近三个月内征信查询次数超过10次' then 235
when a.rule_name='【订单信息】原贷款机构类型不符' then 236
when a.rule_name='【公司诉讼】存在诉讼公司' then 237
when a.rule_name='【基本信息】证件类型非大陆身份证' then 238
when a.rule_name='【红线】信贷状态存在呆账（贷记卡）' then 239
when a.rule_name='【法诉单篇】存在执行信息' then 240
when a.rule_name='【多头借贷】客户小贷查询次数超4次' then 241
when a.rule_name='【多头借贷】客户小贷查询次数超10次' then 242
when a.rule_name='【法诉单篇】命中失信名单' then 243
when a.rule_name='【法诉单篇】存在借贷类案件' then 244
when a.rule_name='【法诉全文】存在借贷类案件' then 245
when a.rule_name='【基本信息】客户年龄超上限' then 246
when a.rule_name='【多头借贷】客户小贷放款笔数超1笔' then 247
when a.rule_name='【法诉全文】存在全文执行信息' then 248
when a.rule_name='【房产】房产共有人数超标' then 249
when a.rule_name='【房产】房产性质不符合规定' then 250
when a.rule_name='【征信】贷款五级分类状态异常' then 251
when a.rule_name='【订单信息】赎楼金额超过原贷款剩余本金的105%' then 252
when a.rule_name='【多头借贷】客户近三个月小贷查询次数为4-10次' then 253
when a.rule_name='【房产】房产共有人数超限' then 254
when a.rule_name='【订单信息】原贷款机构为非银机构' then 255
when a.rule_name='【征信】贷款存在当期逾期' then 256
when a.rule_name='【征信】贷款状态异常' then 257
when a.rule_name='【房产】土地类型不符合准入标准' then 258
when a.rule_name='【多头借贷】客户近三个月小贷查询次数超过10次' then 259
when a.rule_name='【基本信息】客户年龄超下限' then 260
when a.rule_name='【征信】准贷记卡状态异常' then 261
when a.rule_name='【多头借贷】客户近三个月小贷放款次数为1-3笔' then 262
when a.rule_name='【征信】贷记卡存在当期逾期' then 263
when a.rule_name='【订单信息】商业贷款金额超房产评估价7成' then 264
when a.rule_name='【征信】贷记卡状态异常' then 265
when a.rule_name='【企业法诉】客户名下企业存在诉讼信息' then 266
when a.rule_name='【多头借贷】小贷当前存在逾期' then 267
when a.rule_name='【法诉全文】存在执行信息' then 268
when a.rule_name='【征信】准贷记卡存在当期逾期' then 269
when a.rule_name='【红线】信贷状态存在呆账（贷款状态）' then 270
when a.rule_name='【订单信息】新贷款金额超过房产实际成交价的7成' then 271
when a.rule_name='【规则】综合负债比房产实际成交价大于等于1小于等于1.3' then 272
when a.rule_name='【订单信息】原贷款剩余本金超房产实际成交价6成' then 273
when a.rule_name='【订单信息】借款金额超过回款来源的9成' then 274
when a.rule_name='【企业法诉】客户名下企业存在法诉信息' then 275
when a.rule_name='【多头借贷】客户近三个月小贷查询次数超过15次' then 276
when a.rule_name='【订单信息】偏离度大于120%' then 277
when a.rule_name='【订单信息】原贷款剩余本金超过房产实际成交价的7成' then 278
when a.rule_name='【负债】综合负债超房产实际成交价的1.3倍' then 279
when a.rule_name='【基本信息】产权人年龄超上限' then 280
when a.rule_name='【征信】近三个月存在2笔及以上的贷款或信用卡存在M1逾期' then 281
when a.rule_name='【上海资信】贷款存在当期逾期' then 282
when a.rule_name='【负债】综合负债超房产实际成交价的2倍' then 283
when a.rule_name='【征信】贷款存在当期M2及以上逾期' then 284
when a.rule_name='【征信】近三个月内贷款存在M2及以上逾期' then 285
when a.rule_name='【征信】近三个月内贷记卡存在M2及以上逾期' then 286
when a.rule_name='【多头借贷】客户近三个月小贷查询10-15次' then 287
when a.rule_name='【订单信息】买、卖方户籍地一致' then 288
when a.rule_name='【订单信息】偏离度低于80%' then 289
when a.rule_name='【企业法诉】客户名下企业存在法诉' then 290
when a.rule_name='【订单信息】借款金额超过原贷款剩余本金的105%' then 291
when a.rule_name='【订单信息】卖方非本地户籍' then 292
when a.rule_name='【征信】近半年内贷记卡存在M4及以上逾期' then 293
when a.rule_name='【订单信息】借款金额超过房产实际成交价的7成' then 294
when a.rule_name='【多头借贷】客户小贷放款笔数超3笔' then 295
when a.rule_name='【征信】近半年内贷款存在M4及以上逾期' then 296
when a.rule_name='【征信】近7至12月内贷款存在M4及以上逾期' then 297
when a.rule_name='【红线】贷记卡账户状态为呆账' then 298
when a.rule_name='【基本信息】产权人未成年' then 299
when a.rule_name='【企业法诉】执行公开信息——“执行标的”＞200,000元' then 300
when a.rule_name='【企业法诉】法诉审判信息——存在三年内的审判流程信息' then 301
when a.rule_name='【基本信息】客户年龄超标' then 302
when a.rule_name='【基本信息】客户年龄较小' then 303
when a.rule_name='【订单信息】借款金额超过原贷款剩余本金的1.05倍' then 304
when a.rule_name='【企业法诉】命中失信老赖名单' then 305
when a.rule_name='【订单信息】借款金额超过房产评估价的7成' then 306
when a.rule_name='【征信】近三个月内征信查询次数为6-10次' then 307
when a.rule_name='【订单信息】新贷款机构为非银机构' then 308
when a.rule_name='【多头借贷】申请人近三个月小贷放款次数超过3笔' then 309
when a.rule_name='【企业法诉】法诉裁判文书——“结案时间”为两年内,存在借贷类案件' then 310
when a.rule_name='【多头借贷】申请人近三个月小贷查询次数超过10次' then 311
when a.rule_name='【征信】征信中出现担保代偿信息' then 312
when a.rule_name='【企业法诉】命中限制高消费名单' then 313
when a.rule_name='【企业法诉】法诉裁判文书——“结案时间”为两年内,诉讼地位为被告，非借贷类案件“涉案金额”大于等于20万元' then 314
when a.rule_name='【订单信息】单笔金额超上限-1' then 315
when a.rule_name='【上海资信】单笔贷款存在 M1超标' then 316
when a.rule_name='【订单信息】单笔金额超上限-深圳-其他' then 317
when a.rule_name='【订单信息】客户征信报告解析失败-太原' then 318
when a.rule_name='【企业法诉】查询异常' then 319
when a.rule_name='【订单信息】单笔金额超上限-其他' then 320
when a.rule_name='【征信】近三个月内征信查询次数超过10次' then 321
when a.rule_name='【法诉】法诉裁判文书——“结案时间”为三年内,诉讼地位为原告，借贷类案件大于一件' then 322
when a.rule_name='【订单信息】单笔金额超上限-广州-其他' then 323
when a.rule_name='【红线】贷款五级分类为红线' then 324
when a.rule_name='【多头借贷】客户近三个月内小贷放款笔数为1-3笔' then 325
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-10次' then 326
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-10次且近三个月内小贷放款笔数为1-3笔' then 327
when a.rule_name='[TD_FLOW]第一联系人（身份证+姓名）命中法院失信模糊名单' then 328
when a.rule_name='【法诉单篇】存在刑事案件' then 329
when a.rule_name='【征信】近7至12个月内贷记卡存在M4及以上逾期' then 330
when a.rule_name='【征信】贷款近24期内还款状态出现D/G/Z' then 331
when a.rule_name='【多头借贷】客户近三个月小贷放款次数超过3笔' then 332
when a.rule_name='【上海资信】近三个月内贷款存在M2及以上逾期' then 333
when a.rule_name='【上海资信】近半年内贷款存在M4及以上逾期' then 334
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-15次' then 335
when a.rule_name='【上海资信】近二年贷款存在M4以上逾期' then 336
when a.rule_name='【上海资信】近7至12个月内贷款存在M4及以上逾期' then 337
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-15次且近三个月内小贷放款笔数为1-3笔' then 338
when a.rule_name='【多头借贷】客户近三个月小贷查询次数为4-15次' then 339
when a.rule_name='【上海资信】贷款存在当期M2及以上逾期' then 340
when a.rule_name='【上海资信】近三个月内贷款存在M2及以上逾期' then 333
when a.rule_name='【上海资信】近半年内贷款存在M4及以上逾期' then 334
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-15次' then 335
when a.rule_name='【上海资信】近二年贷款存在M4以上逾期' then 336
when a.rule_name='【上海资信】近7至12个月内贷款存在M4及以上逾期' then 337
when a.rule_name='【多头借贷】客户近三个月内小贷查询次数4-15次且近三个月内小贷放款笔数为1-3笔' then 338
when a.rule_name='【多头借贷】客户近三个月小贷查询次数为4-15次' then 339
when a.rule_name='【上海资信】贷款存在当期M2及以上逾期' then 340
when a.rule_name='【订单信息】单笔金额超上限-北京' then 341
when a.rule_name='【订单信息】单笔金额超上限-上海' then 342
when a.rule_name='【订单信息】单笔金额超上限-广州' then 343
when a.rule_name='【订单信息】单笔金额超上限-深圳' then 344
when a.rule_name='【订单信息】单笔金额超上限-北京-其他' then 345
when a.rule_name='【订单信息】单笔金额超上限-上海-其他' then 346
else NULL
end rule_name_tag,
a.hit_message,
a.rule_type,
a.prompt_message,
a.flow_node
from ods_bpms_biz_hit_rule a;
drop table if exists ods_bpms_biz_hit_rule_common;
ALTER TABLE odstmp_bpms_biz_hit_rule_common RENAME TO ods_bpms_biz_hit_rule_common;