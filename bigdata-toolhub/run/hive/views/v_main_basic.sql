use zhgl;

CREATE VIEW if not exists V_MAIN_BASIC AS
SELECT CUSTNUM,
            SUM (RECORDTYPE) RECORDTYPE,                 
            SUM (EVENTTYPE) EVENTTYPE,                   
            SUM (ExtractType) ExtractType,
            MAX (PAST13_AC_LINS_FLG + 0) AC_LINS_FLG,
            MAX (CU_LINS_CUR_FLG + 0) CU_LINS_CUR_FLG,
            MAX (LINS_LIMIT + 0) LINS_LIMIT,
            SUM (LINS_SH_UNPAY_PRINCIPAL_BIG) LINS_SH_UNPAY_PRINCIPAL_BIG, 
            SUM (LINS_SH_UNPAY_BAL) LINS_SH_UNPAY_BAL,      
            SUM (MonthlyPayCC) MonthlyPayCC,                
            MAX (CURR_OVD_STATUS + 0) CURR_OVD_STATUS,
            SUM (LINS_CUR_SH_AMT) LINS_CUR_SH_AMT,          
            MAX (HISTUpdatedLimitType + 0) HISTUpdatedLimitType,
            MAX (TERMS + 0) TERMS,
            SUM (STMTFeeAccour_L12M) STMTFeeAccour_L12M, 
            SUM (PAST12_GET_CASH_COUNT) PAST12_GET_CASH_COUNT,  
            MAX (CollectTemplate + 0) CollectTemplate,
            MAX (PAST12_MAX_DUE_MTHS + 0) PAST12_MAX_DUE_MTHS,
            MAX (STMTBALINCREASEMONTH_L12M + 0) STMTBALINCREASEMONTH_L12M,
            SUM (PAST3_DUE_COUNT) PAST3_DUE_COUNT,         
            MAX (PAST3_CREDIT_USE_RATIO_MAX + 0) PAST3_CREDIT_USE_RATIO_MAX,
            MIN (PAST3_PAY_AMT_MTHS + 0) PAST3_PAY_AMT_MTHS,
            SUM (LAST_3MON_CASH_TIMES_1) LAST_3MON_CASH_TIMES_1, 
            SUM (LAST_3MON_CASH_TIMES_2) LAST_3MON_CASH_TIMES_2, 
            SUM (LAST_3MON_CASH_TIMES_3) LAST_3MON_CASH_TIMES_3, 
            MAX (STMTCASHINCREASERATIO + 0) STMTCASHINCREASERATIO,
            MAX (PAST3_GET_CASH_RATIO + 0) PAST3_GET_CASH_RATIO,
            MAX (STMTREPAYRATIO10MONTH_L6M + 0) STMTREPAYRATIO10MONTH_L6M,
            SUM (STMTREWARD_AVE_L6M) STMTREWARD_AVE_L6M,        
            SUM (STMTINCOME_AVE_L6M) STMTINCOME_AVE_L6M,        
            MAX (PAST6_FEE_COUNT + 0) PAST6_FEE_COUNT,
            MAX (STMTFEEINCREASEMONTH_L6M + 0) STMTFEEINCREASEMONTH_L6M,SUM (STMTSUMCASH_L6M) STMTSUMCASH_L6M,MIN (STMTCASHADVANCEMONTH_L6M + 0) STMTCASHADVANCEMONTH_L6M,SUM (STMTSUMCONSUME_L6M) STMTSUMCONSUME_L6M,MAX (PAST6_DUE_COUNT_MAX) PAST6_DUE_COUNT_MAX,MAX (ARREARINCREASE_L12M) ARREARINCREASE_L12M,SUM (LAST_12MON_CONSUME_AMT_1) LAST_12MON_CONSUME_AMT_1, SUM (LAST_12MON_CONSUME_AMT_2) LAST_12MON_CONSUME_AMT_2, SUM (LAST_12MON_CONSUME_AMT_3) LAST_12MON_CONSUME_AMT_3,  SUM (LAST_12MON_CONSUME_AMT_4) LAST_12MON_CONSUME_AMT_4,
            SUM (LAST_12MON_CONSUME_AMT_5) LAST_12MON_CONSUME_AMT_5, SUM (LAST_12MON_CONSUME_AMT_6) LAST_12MON_CONSUME_AMT_6, SUM (LAST_12MON_CONSUME_AMT_7) LAST_12MON_CONSUME_AMT_7, SUM (LAST_12MON_CONSUME_AMT_8) LAST_12MON_CONSUME_AMT_8, SUM (LAST_12MON_CONSUME_AMT_9) LAST_12MON_CONSUME_AMT_9, 
            SUM (LAST_12MON_CONSUME_AMT_10) LAST_12MON_CONSUME_AMT_10, 
            SUM (LAST_12MON_CONSUME_AMT_11) LAST_12MON_CONSUME_AMT_11, 
            SUM (LAST_12MON_CONSUME_AMT_12) LAST_12MON_CONSUME_AMT_12, 
            SUM (LAST_13MON_REPAYMENT_1) LAST_13MON_REPAYMENT_1, 
            SUM (LAST_13MON_REPAYMENT_2) LAST_13MON_REPAYMENT_2, 
            SUM (LAST_13MON_REPAYMENT_3) LAST_13MON_REPAYMENT_3, 
            SUM (LAST_13MON_REPAYMENT_4) LAST_13MON_REPAYMENT_4, 
            SUM (LAST_13MON_REPAYMENT_5) LAST_13MON_REPAYMENT_5, 
            SUM (LAST_13MON_REPAYMENT_6) LAST_13MON_REPAYMENT_6, 
            SUM (LAST_13MON_REPAYMENT_7) LAST_13MON_REPAYMENT_7, 
            SUM (LAST_13MON_REPAYMENT_8) LAST_13MON_REPAYMENT_8, 
            SUM (LAST_13MON_REPAYMENT_9) LAST_13MON_REPAYMENT_9, 
            SUM (LAST_13MON_REPAYMENT_10) LAST_13MON_REPAYMENT_10, 
            SUM (LAST_13MON_REPAYMENT_11) LAST_13MON_REPAYMENT_11, 
            SUM (LAST_13MON_REPAYMENT_12) LAST_13MON_REPAYMENT_12, 
            SUM (LAST_13MON_REPAYMENT_13) LAST_13MON_REPAYMENT_13, 
            SUM (LAST_13MON_CASH_1) LAST_13MON_CASH_1,    
            SUM (LAST_13MON_CASH_2) LAST_13MON_CASH_2,    
            SUM (LAST_13MON_CASH_3) LAST_13MON_CASH_3,    
            SUM (LAST_13MON_CASH_4) LAST_13MON_CASH_4,    
            SUM (LAST_13MON_CASH_5) LAST_13MON_CASH_5,    
            SUM (LAST_13MON_CASH_6) LAST_13MON_CASH_6,    
            SUM (LAST_13MON_CASH_7) LAST_13MON_CASH_7,    
            SUM (LAST_13MON_CASH_8) LAST_13MON_CASH_8,    
            SUM (LAST_13MON_CASH_9) LAST_13MON_CASH_9,    
            SUM (LAST_13MON_CASH_10) LAST_13MON_CASH_10,  
            SUM (LAST_13MON_CASH_11) LAST_13MON_CASH_11,  
            SUM (LAST_13MON_CASH_12) LAST_13MON_CASH_12,  
            SUM (LAST_13MON_CASH_13) LAST_13MON_CASH_13,  
            MAX (LAST_13MON_CREDIT_AMT_1 + 0) LAST_13MON_CREDIT_AMT_1,
            MAX (LAST_13MON_CREDIT_AMT_2 + 0) LAST_13MON_CREDIT_AMT_2,
            MAX (LAST_13MON_CREDIT_AMT_3 + 0) LAST_13MON_CREDIT_AMT_3,
            MAX (LAST_13MON_CREDIT_AMT_4 + 0) LAST_13MON_CREDIT_AMT_4,
            MAX (LAST_13MON_CREDIT_AMT_5 + 0) LAST_13MON_CREDIT_AMT_5,
            MAX (LAST_13MON_CREDIT_AMT_6 + 0) LAST_13MON_CREDIT_AMT_6,
            MAX (LAST_13MON_CREDIT_AMT_7 + 0) LAST_13MON_CREDIT_AMT_7,
            MAX (LAST_13MON_CREDIT_AMT_8 + 0) LAST_13MON_CREDIT_AMT_8,
            MAX (LAST_13MON_CREDIT_AMT_9 + 0) LAST_13MON_CREDIT_AMT_9,
            MAX (LAST_13MON_CREDIT_AMT_10 + 0) LAST_13MON_CREDIT_AMT_10,
            MAX (LAST_13MON_CREDIT_AMT_11 + 0) LAST_13MON_CREDIT_AMT_11,
            MAX (LAST_13MON_CREDIT_AMT_12 + 0) LAST_13MON_CREDIT_AMT_12,
            MAX (LAST_13MON_CREDIT_AMT_13 + 0) LAST_13MON_CREDIT_AMT_13,
            SUM (LINS_SH_UNPAY_BAL_1) LINS_SH_UNPAY_BAL_1, 
            SUM (LINS_SH_UNPAY_BAL_2) LINS_SH_UNPAY_BAL_2, 
            SUM (LINS_SH_UNPAY_BAL_3) LINS_SH_UNPAY_BAL_3, 
            SUM (LINS_SH_UNPAY_BAL_4) LINS_SH_UNPAY_BAL_4, 
            SUM (LINS_SH_UNPAY_BAL_5) LINS_SH_UNPAY_BAL_5, 
            SUM (LINS_SH_UNPAY_BAL_6) LINS_SH_UNPAY_BAL_6, 
            SUM (LINS_SH_UNPAY_BAL_7) LINS_SH_UNPAY_BAL_7, 
            SUM (LINS_SH_UNPAY_BAL_8) LINS_SH_UNPAY_BAL_8, 
            SUM (LINS_SH_UNPAY_BAL_9) LINS_SH_UNPAY_BAL_9, 
            SUM (LINS_SH_UNPAY_BAL_10) LINS_SH_UNPAY_BAL_10, 
            SUM (LINS_SH_UNPAY_BAL_11) LINS_SH_UNPAY_BAL_11, 
            SUM (LINS_SH_UNPAY_BAL_12) LINS_SH_UNPAY_BAL_12, 
            SUM (LINS_SH_UNPAY_BAL_13) LINS_SH_UNPAY_BAL_13, 
            SUM (LAST_13MON_TEMP_AMT_1) LAST_13MON_TEMP_AMT_1, 
            SUM (LAST_13MON_TEMP_AMT_2) LAST_13MON_TEMP_AMT_2, 
            SUM (LAST_13MON_TEMP_AMT_3) LAST_13MON_TEMP_AMT_3, 
            SUM (LAST_13MON_TEMP_AMT_4) LAST_13MON_TEMP_AMT_4, 
            SUM (LAST_13MON_TEMP_AMT_5) LAST_13MON_TEMP_AMT_5, 
            SUM (LAST_13MON_TEMP_AMT_6) LAST_13MON_TEMP_AMT_6, 
            SUM (LAST_13MON_TEMP_AMT_7) LAST_13MON_TEMP_AMT_7, 
            SUM (LAST_13MON_TEMP_AMT_8) LAST_13MON_TEMP_AMT_8, 
            SUM (LAST_13MON_TEMP_AMT_9) LAST_13MON_TEMP_AMT_9, 
            SUM (LAST_13MON_TEMP_AMT_10) LAST_13MON_TEMP_AMT_10, 
            SUM (LAST_13MON_TEMP_AMT_11) LAST_13MON_TEMP_AMT_11, 
            SUM (LAST_13MON_TEMP_AMT_12) LAST_13MON_TEMP_AMT_12, 
            SUM (LAST_13MON_TEMP_AMT_13) LAST_13MON_TEMP_AMT_13, 
            SUM (LAST_13MON_BILL_AMT_1) LAST_13MON_BILL_AMT_1, 
            SUM (LAST_13MON_BILL_AMT_2) LAST_13MON_BILL_AMT_2, 
            SUM (LAST_13MON_BILL_AMT_3) LAST_13MON_BILL_AMT_3, 
            SUM (LAST_13MON_BILL_AMT_4) LAST_13MON_BILL_AMT_4, 
            SUM (LAST_13MON_BILL_AMT_5) LAST_13MON_BILL_AMT_5, 
            SUM (LAST_13MON_BILL_AMT_6) LAST_13MON_BILL_AMT_6, 
            SUM (LAST_13MON_BILL_AMT_7) LAST_13MON_BILL_AMT_7, 
            SUM (LAST_13MON_BILL_AMT_8) LAST_13MON_BILL_AMT_8, 
            SUM (LAST_13MON_BILL_AMT_9) LAST_13MON_BILL_AMT_9, 
            SUM (LAST_13MON_BILL_AMT_10) LAST_13MON_BILL_AMT_10, 
            SUM (LAST_13MON_BILL_AMT_11) LAST_13MON_BILL_AMT_11, 
            SUM (LAST_13MON_BILL_AMT_12) LAST_13MON_BILL_AMT_12, 
            SUM (LAST_13MON_BILL_AMT_13) LAST_13MON_BILL_AMT_13, 
            SUM (LAST_13MON_LOWEST_REPAY_1) LAST_13MON_LOWEST_REPAY_1, 
            SUM (LAST_13MON_LOWEST_REPAY_2) LAST_13MON_LOWEST_REPAY_2, 
            SUM (LAST_13MON_LOWEST_REPAY_3) LAST_13MON_LOWEST_REPAY_3, 
            SUM (LAST_13MON_LOWEST_REPAY_4) LAST_13MON_LOWEST_REPAY_4, 
            SUM (LAST_13MON_LOWEST_REPAY_5) LAST_13MON_LOWEST_REPAY_5, 
            SUM (LAST_13MON_LOWEST_REPAY_6) LAST_13MON_LOWEST_REPAY_6, 
            SUM (LAST_13MON_LOWEST_REPAY_7) LAST_13MON_LOWEST_REPAY_7, 
            SUM (LAST_13MON_LOWEST_REPAY_8) LAST_13MON_LOWEST_REPAY_8, 
            SUM (LAST_13MON_LOWEST_REPAY_9) LAST_13MON_LOWEST_REPAY_9, 
            SUM (LAST_13MON_LOWEST_REPAY_10) LAST_13MON_LOWEST_REPAY_10, 
            SUM (LAST_13MON_LOWEST_REPAY_11) LAST_13MON_LOWEST_REPAY_11, 
            SUM (LAST_13MON_LOWEST_REPAY_12) LAST_13MON_LOWEST_REPAY_12, 
            SUM (LAST_13MON_LOWEST_REPAY_13) LAST_13MON_LOWEST_REPAY_13, 
            MAX (ArrearIncrease_L24M + 0) ArrearIncrease_L24M,
            MAX (LAST_24MON_OVD_FLAG_1 + 0) LAST_24MON_OVD_FLAG_1,
            MAX (LAST_24MON_OVD_FLAG_2 + 0) LAST_24MON_OVD_FLAG_2,
            MAX (LAST_24MON_OVD_FLAG_3 + 0) LAST_24MON_OVD_FLAG_3,
            MAX (LAST_24MON_OVD_FLAG_4 + 0) LAST_24MON_OVD_FLAG_4,
            MAX (LAST_24MON_OVD_FLAG_5 + 0) LAST_24MON_OVD_FLAG_5,
            MAX (LAST_24MON_OVD_FLAG_6 + 0) LAST_24MON_OVD_FLAG_6,
            MAX (LAST_24MON_OVD_FLAG_7 + 0) LAST_24MON_OVD_FLAG_7,
            MAX (LAST_24MON_OVD_FLAG_8 + 0) LAST_24MON_OVD_FLAG_8,
            MAX (LAST_24MON_OVD_FLAG_9 + 0) LAST_24MON_OVD_FLAG_9,
            MAX (LAST_24MON_OVD_FLAG_10 + 0) LAST_24MON_OVD_FLAG_10,
            MAX (LAST_24MON_OVD_FLAG_11 + 0) LAST_24MON_OVD_FLAG_11,
            MAX (LAST_24MON_OVD_FLAG_12 + 0) LAST_24MON_OVD_FLAG_12,
            SUM (LAST_24MON_OVD_FLAG_13) LAST_24MON_OVD_FLAG_13,
            SUM (LAST_24MON_OVD_FLAG_14) LAST_24MON_OVD_FLAG_14,
            MAX (LAST_24MON_OVD_FLAG_15 + 0) LAST_24MON_OVD_FLAG_15,
            SUM (LAST_24MON_OVD_FLAG_16) LAST_24MON_OVD_FLAG_16,
            MAX (LAST_24MON_OVD_FLAG_17 + 0) LAST_24MON_OVD_FLAG_17,
            SUM (LAST_24MON_OVD_FLAG_18) LAST_24MON_OVD_FLAG_18,
            SUM (LAST_24MON_OVD_FLAG_19) LAST_24MON_OVD_FLAG_19,
            SUM (LAST_24MON_OVD_FLAG_20) LAST_24MON_OVD_FLAG_20,
            SUM (LAST_24MON_OVD_FLAG_21) LAST_24MON_OVD_FLAG_21,
            SUM (LAST_24MON_OVD_FLAG_22) LAST_24MON_OVD_FLAG_22,
            SUM (LAST_24MON_OVD_FLAG_23) LAST_24MON_OVD_FLAG_23,
            SUM (LAST_24MON_OVD_FLAG_24) LAST_24MON_OVD_FLAG_24, 
            MAX (LAST_6MON_INACTIVE_FLAG + 0) LAST_6MON_INACTIVE_FLAG,
            MAX (ArrearIncrease_L6M + 0) ArrearIncrease_L6M,
            MAX (CHEAT_FLG + 0) CHEAT_FLG,
            MAX (ACCTCASHADVANCE + 0) ACCTCASHADVANCE,
            SUM (LINS_REM_UNSH_PRINCIPAL) LINS_REM_UNSH_PRINCIPAL,   
            SUM (LINS_REM_UNSH_FEE) LINS_REM_UNSH_FEE,               
            MAX (ANNUAL_FEE_OVD_FLAG + 0) ANNUAL_FEE_OVD_FLAG,
            MAX (ACCTSTATUS + 0) ACCTSTATUS,
            MAX (LIMITCC + 0) LIMITCC,
            SUM (LAST_CYCLEDAY_CASH_TIMES) LAST_CYCLEDAY_CASH_TIMES, 
            MAX (APP_CCVALIDDATE + 0) APP_CCVALIDDATE,
            MAX (PRO_STATUS_ACCT + 0) PRO_STATUS_ACCT,
            SUM (LINS_SH_UNPAY_PRINCIPAL) LINS_SH_UNPAY_PRINCIPAL, 
            SUM (CollectMethods) CollectMethods,                   
            MAX (CollectTimes + 0) CollectTimes,
            SUM (PBOC_ACCTNUMB_UTILIZATION50UP) PBOC_ACCTNUMB_UTILIZATION50UP 
                                                                             ,
            SUM (LAST_24MON_OVD_TERM_1) LAST_24MON_OVD_TERM_1 
                                                             ,
            SUM (LAST_24MON_OVD_TERM_2) LAST_24MON_OVD_TERM_2 
                                                             ,
            SUM (LAST_24MON_OVD_TERM_3) LAST_24MON_OVD_TERM_3 
                                                             ,
            SUM (LAST_24MON_OVD_TERM_4) LAST_24MON_OVD_TERM_4 
                                                             ,
            SUM (LAST_24MON_OVD_TERM_5) LAST_24MON_OVD_TERM_5 
                                                             ,
            SUM (LAST_24MON_OVD_TERM_6) LAST_24MON_OVD_TERM_6,SUM (LAST_24MON_OVD_TERM_7) LAST_24MON_OVD_TERM_7,SUM (LAST_24MON_OVD_TERM_8) LAST_24MON_OVD_TERM_8,
            SUM (LAST_24MON_OVD_TERM_9) LAST_24MON_OVD_TERM_9,SUM (LAST_24MON_OVD_TERM_10) LAST_24MON_OVD_TERM_10 
                                                               ,SUM (LAST_24MON_OVD_TERM_11) LAST_24MON_OVD_TERM_11,
            SUM (LAST_24MON_OVD_TERM_12) LAST_24MON_OVD_TERM_12 
                                                               ,
            MAX (LAST_24MON_OVD_TERM_13 + 0) LAST_24MON_OVD_TERM_13
                                                               ,
            MAX (LAST_24MON_OVD_TERM_14 + 0) LAST_24MON_OVD_TERM_14
                                                               ,
            MAX (LAST_24MON_OVD_TERM_15 + 0) LAST_24MON_OVD_TERM_15
                                                               ,
            MAX (LAST_24MON_OVD_TERM_16 + 0) LAST_24MON_OVD_TERM_16
                                                               ,
            MAX (LAST_24MON_OVD_TERM_17 + 0) LAST_24MON_OVD_TERM_17
                                                               ,
            MAX (LAST_24MON_OVD_TERM_18 + 0) LAST_24MON_OVD_TERM_18
                                                               ,
            MAX (LAST_24MON_OVD_TERM_19 + 0) LAST_24MON_OVD_TERM_19
                                                               ,
            MAX (LAST_24MON_OVD_TERM_20 + 0) LAST_24MON_OVD_TERM_20
                                                               ,
            MAX (LAST_24MON_OVD_TERM_21 + 0) LAST_24MON_OVD_TERM_21
                                                               ,
            MAX (LAST_24MON_OVD_TERM_22 + 0) LAST_24MON_OVD_TERM_22
                                                               ,
            MAX (LAST_24MON_OVD_TERM_23 + 0) LAST_24MON_OVD_TERM_23
                                                               ,
            MAX (LAST_24MON_OVD_TERM_24 + 0) LAST_24MON_OVD_TERM_24,
            MAX (AC_LINS_FLG_ALL + 0) AC_LINS_FLG_ALL,
            SUM (ncsndamount) ncsndamount,                      
            SUM (iandamount) iandamount,                        
            MAX (COLLECTMETHODS + 0) delayamount6mon,
            SUM (BALANCE) BALANCE,                              
            MIN (PRO_AMOUNT_LOAN_SME + 0) PRO_AMOUNT_LOAN_SME,
            MIN (PRO_AMOUNT_LOAN + 0) PRO_AMOUNT_LOAN ,
            MIN (PBOC_ACCTAGE_HIGHEST_HEALTHY + 0) PBOC_ACCTAGE_HIGHEST_HEALTHY,
            SUM(untrl_pc_amt) untrl_pc_amt,
            SUM(untrl_bad_pc_amt) untrl_bad_pc_amt,
            SUM(untrl_ca_amt) untrl_ca_amt,
            SUM(untrl_top3_amt) untrl_top3_amt,
            SUM(untrl_mcht_sum_amt) untrl_mcht_sum_amt,
            SUM(untrl_pc_counts) untrl_pc_counts,
            SUM(untrl_pc_net_counts) untrl_pc_net_counts,
            SUM(untrl_max_pc_amt) untrl_max_pc_amt,
            SUM(untrl_pc_mcht_counts) untrl_pc_mcht_counts,
            SUM(Res1 ) Res1 ,
            SUM(Res2 ) Res2 ,
            SUM(Res3 ) Res3 ,
            SUM(Res4 ) Res4 ,
            SUM(Res5 ) Res5 ,
            SUM(Res6 ) Res6 ,
            MIN(Res7 + 0 ) Res7 ,
            SUM(Res8 ) Res8 ,
            SUM(Res9 ) Res9 ,
            SUM(Res10) Res10,
            SUM(Res11) Res11,
            SUM(Res12) Res12,
            SUM(Res13) Res13,
            SUM(Res14) Res14,
            SUM(Res15) Res15,
            SUM(Res16) Res16,
            SUM(Res17) Res17,
            SUM(Res18) Res18,
            SUM(Res19) Res19,
            SUM(Res20) Res20,
            SUM(Res21) Res21,
            SUM(Res22) Res22,
            SUM(Res23) Res23,
            SUM(Res24) Res24,
            SUM(Res25) Res25,
            SUM(Res26) Res26,
            SUM(Res27) Res27,
            SUM(Res28) Res28,
            SUM(Res29) Res29,
            SUM(Res30) Res30,
            SUM(Res31) Res31,
            SUM(Res32) Res32,
            SUM(Res33) Res33,
            SUM(Res34) Res34,
            SUM(Res35) Res35,
            SUM(Res36) Res36,
            SUM(Res37) Res37,
            SUM(Res38) Res38,
            SUM(Res39) Res39,
            SUM(Res40) Res40,
            SUM(Res41) Res41,
            SUM(Res42) Res42,
            SUM(Res43) Res43,
            SUM(Res44) Res44,
            SUM(Res45) Res45,
            SUM(Res46) Res46,
            SUM(Res47) Res47,
            SUM(Res48) Res48,
            SUM(Res49) Res49,
            SUM(Res50) Res50,
            SUM(Res51) Res51,
            SUM(Res52) Res52,
            SUM(Res53) Res53,
            SUM(Res54) Res54,
            SUM(Res55) Res55,
            SUM(Res56) Res56,
            SUM(Res57) Res57,
            SUM(Res58) Res58,
            SUM(Res59) Res59,
            SUM(Res60) Res60,
            SUM(Res61) Res61,
            SUM(Res62) Res62,
            SUM(Res63) Res63,
            SUM(Res64) Res64,
            SUM(Res65) Res65,
            SUM(Res66) Res66,
            SUM(Res67) Res67,
            SUM(Res68) Res68
       FROM account
   GROUP BY CUSTNUM;
