use ods;

CREATE TABLE if not exists ods.odstmp_bims_order_fees_common_wx (   apply_no STRING,
  channelprice_fee_value DOUBLE,
  contractprice_fee_value DOUBLE,
  othersubstitutefee_fee_value DOUBLE,
  evaluationfee_fee_value DOUBLE,
  notarizationservicefee_fee_value DOUBLE,
  substitutefeecount_fee_value DOUBLE,
  mortgageservicefee_fee_value DOUBLE,
  notarizationfee_fee_value DOUBLE,
  stamptaxfee_fee_value DOUBLE,
  otherservicefee_fee_value DOUBLE,
  insurancefee_fee_value DOUBLE,
  accessorialservicefeecount_fee_value DOUBLE,
  baseservicefee_fee_value DOUBLE,
  remotetrafficfee_fee_value DOUBLE,
  rakebackfee_fee_value DOUBLE,
  basefee_fee_value DOUBLE,
  mortgagerateby90_fee_value DOUBLE,
  totalreceivablefee_fee_value DOUBLE,
  rakeback_fee_value DOUBLE,
  periodterm_fee_value DOUBLE,
  annualizedincome_fee_value DOUBLE,
  mortgagerateby180_fee_value DOUBLE,
  individualloanguaranteeinsurancefee90_fee_value DOUBLE,
  mortgageamt_fee_value DOUBLE,
  secondpremiumrate_fee_value DOUBLE,
  mortgagemonth_fee_value DOUBLE,
  mortgagerate_fee_value DOUBLE,
  individualloanguaranteeamount90_fee_value DOUBLE,
  secondhandtransactionperformanceinsurancefee90_fee_value DOUBLE,
  premiumrate_fee_value DOUBLE,
  defaultinterestrate_fee_value DOUBLE,
  foreclosureamt_fee_value DOUBLE,
  secondhandtransactionperformanceamount90_fee_value DOUBLE,
  mortgageamtby90_fee_value DOUBLE,
  mortgageamtby180_fee_value DOUBLE,
  retailpricefixedterm_fee_value DOUBLE,
  channelpricefixedterm_fee_value DOUBLE,
  basefee_mo_fee_value DOUBLE,
  floatfee_mo_fee_value DOUBLE,
  channelprice_mo_fee_value DOUBLE,
  retailprice_mo_fee_value DOUBLE,
  collectioncommission_mo_fee_value DOUBLE,
  contractprice_mo_fee_value DOUBLE,
  outsidepricecommission_fee_value DOUBLE,
  retailpricecalculatedaily_fee_value DOUBLE,
  channelpricecalculatedaily_fee_value DOUBLE,
  contractpricecalculatedaily_fee_value DOUBLE,
  commissioncountcalculatedaily_fee_value DOUBLE,
  contractfeeratecountcalculatedaily_fee_value DOUBLE,
  basefeecalculatedaily_fee_value DOUBLE,
  rakebackfeecalculatedaily_fee_value DOUBLE,
  overduerateperday_fee_value DOUBLE,
  basefeefixedterm_fee_value DOUBLE,
  rakebackfeefixedterm_fee_value DOUBLE,
  annualizedloanrateforplatform_fee_value DOUBLE,
  contractpricefixedterm_fee_value DOUBLE,
  commissioncountfixedterm_fee_value DOUBLE,
  contractfeeratecountfixedterm_fee_value DOUBLE,
  feerateperday_fee_value DOUBLE,
  feerateperdayfirstpart_fee_value DOUBLE,
  feerateperdaysecondpart_fee_value DOUBLE,
  contractprice_mo__fee_value DOUBLE,
  retailprice_mo__fee_value DOUBLE,
  channelprice_mo__fee_value DOUBLE,
  collectioncommission_mo__fee_value DOUBLE,
  evaluationservicefee_fee_value DOUBLE,
  impawnregisterfee_fee_value DOUBLE,
  channelcommission_guaranty_fee_value DOUBLE,
  agentcommissionperorder_fee_value DOUBLE
) STORED AS parquet;

insert overwrite table  odstmp_bims_order_fees_common_wx
select t.`apply_no`,
max((case when lower(t.`fee_define_no`)='channelprice' then t.`fee_value` else null end)) channelprice_fee_value ,
max((case when lower(t.`fee_define_no`)='contractprice' then t.`fee_value` else null end)) contractprice_fee_value ,
max((case when lower(t.`fee_define_no`)='othersubstitutefee' then t.`fee_value` else null end)) othersubstitutefee_fee_value ,
max((case when lower(t.`fee_define_no`)='evaluationfee' then t.`fee_value` else null end)) evaluationfee_fee_value ,
max((case when lower(t.`fee_define_no`)='notarizationservicefee' then t.`fee_value` else null end)) notarizationservicefee_fee_value ,
max((case when lower(t.`fee_define_no`)='substitutefeecount' then t.`fee_value` else null end)) substitutefeecount_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgageservicefee' then t.`fee_value` else null end)) mortgageservicefee_fee_value ,
max((case when lower(t.`fee_define_no`)='notarizationfee' then t.`fee_value` else null end)) notarizationfee_fee_value ,
max((case when lower(t.`fee_define_no`)='stamptaxfee' then t.`fee_value` else null end)) stamptaxfee_fee_value ,
max((case when lower(t.`fee_define_no`)='otherservicefee' then t.`fee_value` else null end)) otherservicefee_fee_value ,
max((case when lower(t.`fee_define_no`)='insurancefee' then t.`fee_value` else null end)) insurancefee_fee_value ,
max((case when lower(t.`fee_define_no`)='accessorialservicefeecount' then t.`fee_value` else null end)) accessorialservicefeecount_fee_value ,
max((case when lower(t.`fee_define_no`)='baseservicefee' then t.`fee_value` else null end)) baseservicefee_fee_value ,
max((case when lower(t.`fee_define_no`)='remotetrafficfee' then t.`fee_value` else null end)) remotetrafficfee_fee_value ,
max((case when lower(t.`fee_define_no`)='rakebackfee' then t.`fee_value` else null end)) rakebackfee_fee_value ,
max((case when lower(t.`fee_define_no`)='basefee' then t.`fee_value` else null end)) basefee_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgagerateby90' then t.`fee_value` else null end)) mortgagerateby90_fee_value ,
max((case when lower(t.`fee_define_no`) in ('totalreceivablefee','feetotal') then t.`fee_value` else null end)) totalreceivablefee_fee_value ,
max((case when lower(t.`fee_define_no`)='rakeback' then t.`fee_value` else null end)) rakeback_fee_value ,
max((case when lower(t.`fee_define_no`)='periodterm' then t.`fee_value` else null end)) periodterm_fee_value ,
max((case when lower(t.`fee_define_no`)='annualizedincome' then t.`fee_value` else null end)) annualizedincome_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgagerateby180' then t.`fee_value` else null end)) mortgagerateby180_fee_value ,
max((case when lower(t.`fee_define_no`)='individualloanguaranteeinsurancefee90' then t.`fee_value` else null end)) individualloanguaranteeinsurancefee90_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgageamt' then t.`fee_value` else null end)) mortgageamt_fee_value ,
max((case when lower(t.`fee_define_no`)='secondpremiumrate' then t.`fee_value` else null end)) secondpremiumrate_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgagemonth' then t.`fee_value` else null end)) mortgagemonth_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgagerate' then t.`fee_value` else null end)) mortgagerate_fee_value ,
max((case when lower(t.`fee_define_no`)='individualloanguaranteeamount90' then t.`fee_value` else null end)) individualloanguaranteeamount90_fee_value ,
max((case when lower(t.`fee_define_no`)='secondhandtransactionperformanceinsurancefee90' then t.`fee_value` else null end)) secondhandtransactionperformanceinsurancefee90_fee_value ,
max((case when lower(t.`fee_define_no`)='premiumrate' then t.`fee_value` else null end)) premiumrate_fee_value ,
max((case when lower(t.`fee_define_no`)='defaultinterestrate' then t.`fee_value` else null end)) defaultinterestrate_fee_value ,
max((case when lower(t.`fee_define_no`)='foreclosureamt' then t.`fee_value` else null end)) foreclosureamt_fee_value ,
max((case when lower(t.`fee_define_no`)='secondhandtransactionperformanceamount90' then t.`fee_value` else null end)) secondhandtransactionperformanceamount90_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgageamtby90' then t.`fee_value` else null end)) mortgageamtby90_fee_value ,
max((case when lower(t.`fee_define_no`)='mortgageamtby180' then t.`fee_value` else null end)) mortgageamtby180_fee_value ,
max((case when lower(t.`fee_define_no`)='retailpricefixedterm' then t.`fee_value` else null end)) retailpricefixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='channelpricefixedterm' then t.`fee_value` else null end)) channelpricefixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='basefee_mo' then t.`fee_value` else null end)) basefee_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='floatfee_mo' then t.`fee_value` else null end)) floatfee_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='channelprice_mo' then t.`fee_value` else null end)) channelprice_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='retailprice_mo' then t.`fee_value` else null end)) retailprice_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='collectioncommission_mo' then t.`fee_value` else null end)) collectioncommission_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='contractprice_mo' then t.`fee_value` else null end)) contractprice_mo_fee_value ,
max((case when lower(t.`fee_define_no`)='outsidepricecommission' then t.`fee_value` else null end)) outsidepricecommission_fee_value ,
max((case when lower(t.`fee_define_no`)='retailpricecalculatedaily' then t.`fee_value` else null end)) retailpricecalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='channelpricecalculatedaily' then t.`fee_value` else null end)) channelpricecalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='contractpricecalculatedaily' then t.`fee_value` else null end)) contractpricecalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='commissioncountcalculatedaily' then t.`fee_value` else null end)) commissioncountcalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='contractfeeratecountcalculatedaily' then t.`fee_value` else null end)) contractfeeratecountcalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='basefeecalculatedaily' then t.`fee_value` else null end)) basefeecalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='rakebackfeecalculatedaily' then t.`fee_value` else null end)) rakebackfeecalculatedaily_fee_value ,
max((case when lower(t.`fee_define_no`)='overduerateperday' then t.`fee_value` else null end)) overduerateperday_fee_value ,
max((case when lower(t.`fee_define_no`)='basefeefixedterm' then t.`fee_value` else null end)) basefeefixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='rakebackfeefixedterm' then t.`fee_value` else null end)) rakebackfeefixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='annualizedloanrateforplatform' then t.`fee_value` else null end)) annualizedloanrateforplatform_fee_value ,
max((case when lower(t.`fee_define_no`)='contractpricefixedterm' then t.`fee_value` else null end)) contractpricefixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='commissioncountfixedterm' then t.`fee_value` else null end)) commissioncountfixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='contractfeeratecountfixedterm' then t.`fee_value` else null end)) contractfeeratecountfixedterm_fee_value ,
max((case when lower(t.`fee_define_no`)='feerateperday' then t.`fee_value` else null end)) feerateperday_fee_value ,
max((case when lower(t.`fee_define_no`)='feerateperdayfirstpart' then t.`fee_value` else null end)) feerateperdayfirstpart_fee_value ,
max((case when lower(t.`fee_define_no`)='feerateperdaysecondpart' then t.`fee_value` else null end)) feerateperdaysecondpart_fee_value ,
max((case when lower(t.`fee_define_no`)='contractprice_mo_' then t.`fee_value` else null end)) contractprice_mo__fee_value ,
max((case when lower(t.`fee_define_no`)='retailprice_mo_' then t.`fee_value` else null end)) retailprice_mo__fee_value ,
max((case when lower(t.`fee_define_no`)='channelprice_mo_' then t.`fee_value` else null end)) channelprice_mo__fee_value ,
max((case when lower(t.`fee_define_no`)='collectioncommission_mo_' then t.`fee_value` else null end)) collectioncommission_mo__fee_value ,
max((case when lower(t.`fee_define_no`)='evaluationservicefee' then t.`fee_value` else null end)) evaluationservicefee_fee_value ,
max((case when lower(t.`fee_define_no`)='impawnregisterfee' then t.`fee_value` else null end)) impawnregisterfee_fee_value ,
max((case when lower(t.`fee_define_no`)='channelcommission_guaranty' then t.`fee_value` else null end)) channelcommission_guaranty_fee_value,
max((case when lower(t.`fee_define_no`)='agentcommissionperorder' then t.`fee_value` else null end)) agentcommissionperorder_fee_value
from ods_bims_biz_fee_detial t group by t.`apply_no`;

drop table if exists ods_bims_order_fees_common_wx;
ALTER TABLE odstmp_bims_order_fees_common_wx RENAME TO ods_bims_order_fees_common_wx;