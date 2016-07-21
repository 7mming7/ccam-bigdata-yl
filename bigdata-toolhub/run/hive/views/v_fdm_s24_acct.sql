use zhgl;

CREATE VIEW if not exists V_FDM_S24_ACCT AS
SELECT END_DATE,
          START_DATE,
          XACCOUNT,
          BANK,
          BUSINESS,
          CATEGORY,
          CUSTR_NBR,
          CYCLE_NBR,
          DAY_OPENED,
          ACC_NAME1,
          ACC_TYPE,
          ADD_CHGDAY,
          ADDR_TYPE,
          ADDR_TYPE2,
          AGE1,
          AGE2,
          AGE3,
          AGE4,
          AGE5,
          AGE6,
          APP_SOURCE,
          AUTH_CASH,
          AUTH_OVER,
          AUTHOV_YN,
          AUTHS_AMT,
          BAL_CMPINT,
          BAL_FREE,
          BAL_INT,
          BAL_INTFLAG,
          BAL_NOINT,
          BAL_ORINT,
          BANKACCT1,
          BANKCODE1,
          BANKACCT2,
          BANKCODE2,
          BANKACCT3,
          BANKCODE3,
          BANKACCT4,
          BANKCODE4,
          BRANCH,
          BRANCH_DAY,
          CA_PCNT,
          CARD_FEES,
          CARDS_CANC,
          CARDS_ISSD,
          CASH_1ST,
          CASH_ADFEE,
          CASH_ADVCE,
          CASH_TDAY,
          CASH1STAC,
          CLASS_CHDY,
          CLASS_CODE,
          CLOSE_CHDY,
          CLOSE_CODE,
          COLLS_DAY,
          CRDACT_DAY,
          CRDNXT_DAY,
          CRED_ACTIV,
          CRED_ADJ,
          CRED_CHDAY,
          CRED_LIMIT,
          CRED_VOUCH,
          CREDLIM_X,
          CURR_NUM,
          CUTOFF_DAY,
          CY_CHGCNT,
          CY_CHGDAY,
          CY_EFFDAY,
          CY_YRCNT,
          CYCLE_NEW,
          CYCLE_PRV,
          DEBIT_ADJ,
          DISHNRDAY,
          DUTY_CREDT,
          DUTY_DEBIT,
          EXCH_CODE,
          EXCH_FLAG,
          EXCH_PERC,
          EXCH_RTDT,
          FEE_MONTH,
          FEES_TAXES,
          FIX_EXAMT,
          GUARN_FLAG,
          HI_CASHADV,
          HI_CASMMYY,
          HI_CRDMMYY,
          HI_CREDIT,
          HI_DEBIT,
          HI_DEBMMYY,
          HI_MP_PUR,
          HI_MPMMYY,
          HI_OLIMIT,
          HI_OLIMMYY,
          HI_PURCHSE,
          HI_PURMMYY,
          INT_CASH,
          INT_CHDCMP,
          INT_CHDY,
          INT_CHGD,
          INT_CMPOND,
          INT_CODE,
          INT_CUNOT,
          INT_CURCMP,
          INT_EARNED,
          INT_NOTION,
          INT_PROCDY,
          INT_RATE,
          INT_RATECR,
          INT_UPTODY,
          LANG_CODE,
          LAST_TRDAY,
          LASTAUTHDY,
          LASTNTDATE,
          LASTPAYAMT,
          LASTPAYDAY,
          LOSSES,
          MONTH_NBR,
          MONTR_CHDY,
          MONTR_CODE,
          MP_AM_TDY,
          MP_AUTHS,
          MP_BAL,
          MP_BIL_AMT,
          MP_LIMIT,
          MP_NO_TDY,
          MP_REM_PPL,
          MPLM_CHDAY,
          MTHS_ODUE,
          NBR_CASHAD,
          NBR_FEEDTY,
          NBR_OLIMIT,
          NBR_OTHERS,
          NBR_PAYMNT,
          NBR_PURCH,
          NBR_TRANS,
          OCT_COUNT,
          OCT_DAYIN,
          ODUE_FLAG,
          ODUE_HELD,
          OLFLAG,
          OTHER_FEES,
          PAY_FLAG,
          PAY1ST_IND,
          PAYMT_CLRD,
          PAYMT_TDAY,
          PAYMT_UNCL,
          PEN_CHRG,
          PENCHG_ACC,
          POINT_ADJ,
          PT_ADJFLAG,
          POINT_CLM,
          POINT_CUM,
          PT_CUMFLAG,
          POINT_CUM2,
          POINT_EAR,
          POINT_FREZ,
          POINT_FZDA,
          POST_DD,
          PREV_BRDAY,
          PREV_BRNCH,
          PURCHASES,
          QUERY_AMT,
          QUERY_CODE,
          QUERY_STMT,
          RECLA_CHDY,
          RECLA_CODE,
          RECVRY_AMT,
          REPAY_AMT,
          REPAY_AMTX,
          REPAY_CODE,
          REPAY_CODX,
          REPAY_DAY,
          REPAY_PCT,
          REPAY_PCTX,
          REPY_CHGDY,
          SCORE_PTS,
          STATEMENTS,
          STM_AMT_OL,
          STM_BALFRE,
          STM_BALINT,
          STMBALINTFLAG,
          STM_BALNCE,
          STM_BALFLAG,
          STM_BALORI,
          STM_CLOSDY,
          STM_CODE,
          STM_INSTL,
          STM_MINDUE,
          STM_NOINT,
          STM_OLFLAG,
          STM_OVERDU,
          STM_PAY_UN,
          STM_QRYAMT,
          STM_REPAY,
          STMT_DD,
          STMT_PULL,
          TODAY_AMT,
          TODAY_AMTFLAG,
          TODAY_REL,
          UNCL_PCT,
          WROFF_CHDY,
          WROFF_CODE,
          PROD_LEVEL,
          PROD_NBR,
          ACCT_PRSTS,
          ACCT_STS,
          APP_APQUE,
          POINT_ENC,
          POINT_IMP,
          POINT_EXP,
          PT_EXPFLAG,
          RISK_COUNT,
          RISK_DAYIN,
          BAL_MP,
          BAL_MPFLAG,
          STM_BALMP,
          STM_BMFLAG,
          LMT_RSN,
          POINT_CARD,
          PT_CDFLAG,
          TEMP_LIMIT,
          TLMT_BEG,
          TLMT_END,
          TLMT_NO,
          CANCL_RESN,
          SHADOW_PEN,
          SHADOW_INT,
          SHADOW_CMP,
          BAL_NINT01,
          BAL_NINT02,
          BAL_NINT03,
          BAL_NINT04,
          BAL_NINT05,
          BAL_NINT06,
          BAL_NINT07,
          BAL_NINT08,
          BAL_NINT09,
          BAL_NINT10,
          STM_NINT01,
          STM_NINT02,
          STM_NINT03,
          STM_NINT04,
          STM_NINT05,
          STM_NINT06,
          STM_NINT07,
          STM_NINT08,
          STM_NINT09,
          STM_NINT10,
          CURR_NUM2,
          WROF_FLAG,
          LAYERCODER1,
          LAYERCODER2,
          MCNTRL_YN,
          NCRED_RSN,
          BSC_CRED,
          CUSTR_REF,
          PBC_BRNCH,
          STOPMP_YN,
          CRED_LMT2,
          BAL_CMPFEE
     FROM FDM_S24_ACCT
   UNION ALL
   SELECT from_unixtime(unix_timestamp()),
          p.START_DATE,
          p.XACCOUNT,
          p.BANK,
          p.BUSINESS,
          p.CATEGORY,
          p.CUSTR_NBR,
          p.CYCLE_NBR,
          p.DAY_OPENED,
          p.ACC_NAME1,
          p.ACC_TYPE,
          p.ADD_CHGDAY,
          p.ADDR_TYPE,
          p.ADDR_TYPE2,
          p.AGE1,
          p.AGE2,
          p.AGE3,
          p.AGE4,
          p.AGE5,
          p.AGE6,
          p.APP_SOURCE,
          p.AUTH_CASH,
          p.AUTH_OVER,
          p.AUTHOV_YN,
          p.AUTHS_AMT,
          p.BAL_CMPINT,
          p.BAL_FREE,
          p.BAL_INT,
          p.BAL_INTFLAG,
          p.BAL_NOINT,
          p.BAL_ORINT,
          p.BANKACCT1,
          p.BANKCODE1,
          p.BANKACCT2,
          p.BANKCODE2,
          p.BANKACCT3,
          p.BANKCODE3,
          p.BANKACCT4,
          p.BANKCODE4,
          p.BRANCH,
          p.BRANCH_DAY,
          p.CA_PCNT,
          p.CARD_FEES,
          p.CARDS_CANC,
          p.CARDS_ISSD,
          p.CASH_1ST,
          p.CASH_ADFEE,
          p.CASH_ADVCE,
          p.CASH_TDAY,
          p.CASH1STAC,
          p.CLASS_CHDY,
          p.CLASS_CODE,
          p.CLOSE_CHDY,
          p.CLOSE_CODE,
          p.COLLS_DAY,
          p.CRDACT_DAY,
          p.CRDNXT_DAY,
          p.CRED_ACTIV,
          p.CRED_ADJ,
          p.CRED_CHDAY,
          p.CRED_LIMIT,
          p.CRED_VOUCH,
          p.CREDLIM_X,
          p.CURR_NUM,
          p.CUTOFF_DAY,
          p.CY_CHGCNT,
          p.CY_CHGDAY,
          p.CY_EFFDAY,
          p.CY_YRCNT,
          p.CYCLE_NEW,
          p.CYCLE_PRV,
          p.DEBIT_ADJ,
          p.DISHNRDAY,
          p.DUTY_CREDT,
          p.DUTY_DEBIT,
          p.EXCH_CODE,
          p.EXCH_FLAG,
          p.EXCH_PERC,
          p.EXCH_RTDT,
          p.FEE_MONTH,
          p.FEES_TAXES,
          p.FIX_EXAMT,
          p.GUARN_FLAG,
          p.HI_CASHADV,
          p.HI_CASMMYY,
          p.HI_CRDMMYY,
          p.HI_CREDIT,
          p.HI_DEBIT,
          p.HI_DEBMMYY,
          p.HI_MP_PUR,
          p.HI_MPMMYY,
          p.HI_OLIMIT,
          p.HI_OLIMMYY,
          p.HI_PURCHSE,
          p.HI_PURMMYY,
          p.INT_CASH,
          p.INT_CHDCMP,
          p.INT_CHDY,
          p.INT_CHGD,
          p.INT_CMPOND,
          p.INT_CODE,
          p.INT_CUNOT,
          p.INT_CURCMP,
          p.INT_EARNED,
          p.INT_NOTION,
          p.INT_PROCDY,
          p.INT_RATE,
          p.INT_RATECR,
          p.INT_UPTODY,
          p.LANG_CODE,
          p.LAST_TRDAY,
          p.LASTAUTHDY,
          p.LASTNTDATE,
          p.LASTPAYAMT,
          p.LASTPAYDAY,
          p.LOSSES,
          p.MONTH_NBR,
          p.MONTR_CHDY,
          p.MONTR_CODE,
          p.MP_AM_TDY,
          p.MP_AUTHS,
          p.MP_BAL,
          p.MP_BIL_AMT,
          p.MP_LIMIT,
          p.MP_NO_TDY,
          p.MP_REM_PPL,
          p.MPLM_CHDAY,
          p.MTHS_ODUE,
          p.NBR_CASHAD,
          p.NBR_FEEDTY,
          p.NBR_OLIMIT,
          p.NBR_OTHERS,
          p.NBR_PAYMNT,
          p.NBR_PURCH,
          p.NBR_TRANS,
          p.OCT_COUNT,
          p.OCT_DAYIN,
          p.ODUE_FLAG,
          p.ODUE_HELD,
          p.OLFLAG,
          p.OTHER_FEES,
          p.PAY_FLAG,
          p.PAY1ST_IND,
          p.PAYMT_CLRD,
          p.PAYMT_TDAY,
          p.PAYMT_UNCL,
          p.PEN_CHRG,
          p.PENCHG_ACC,
          p.POINT_ADJ,
          p.PT_ADJFLAG,
          p.POINT_CLM,
          p.POINT_CUM,
          p.PT_CUMFLAG,
          p.POINT_CUM2,
          p.POINT_EAR,
          p.POINT_FREZ,
          p.POINT_FZDA,
          p.POST_DD,
          p.PREV_BRDAY,
          p.PREV_BRNCH,
          p.PURCHASES,
          p.QUERY_AMT,
          p.QUERY_CODE,
          p.QUERY_STMT,
          p.RECLA_CHDY,
          p.RECLA_CODE,
          p.RECVRY_AMT,
          p.REPAY_AMT,
          p.REPAY_AMTX,
          p.REPAY_CODE,
          p.REPAY_CODX,
          p.REPAY_DAY,
          p.REPAY_PCT,
          p.REPAY_PCTX,
          p.REPY_CHGDY,
          p.SCORE_PTS,
          p.STATEMENTS,
          p.STM_AMT_OL,
          p.STM_BALFRE,
          p.STM_BALINT,
          p.STMBALINTFLAG,
          p.STM_BALNCE,
          p.STM_BALFLAG,
          p.STM_BALORI,
          p.STM_CLOSDY,
          p.STM_CODE,
          p.STM_INSTL,
          p.STM_MINDUE,
          p.STM_NOINT,
          p.STM_OLFLAG,
          p.STM_OVERDU,
          p.STM_PAY_UN,
          p.STM_QRYAMT,
          p.STM_REPAY,
          p.STMT_DD,
          p.STMT_PULL,
          p.TODAY_AMT,
          p.TODAY_AMTFLAG,
          p.TODAY_REL,
          p.UNCL_PCT,
          p.WROFF_CHDY,
          p.WROFF_CODE,
          p.PROD_LEVEL,
          p.PROD_NBR,
          p.ACCT_PRSTS,
          p.ACCT_STS,
          p.APP_APQUE,
          p.POINT_ENC,
          p.POINT_IMP,
          p.POINT_EXP,
          p.PT_EXPFLAG,
          p.RISK_COUNT,
          p.RISK_DAYIN,
          p.BAL_MP,
          p.BAL_MPFLAG,
          p.STM_BALMP,
          p.STM_BMFLAG,
          p.LMT_RSN,
          p.POINT_CARD,
          p.PT_CDFLAG,
          p.TEMP_LIMIT,
          p.TLMT_BEG,
          p.TLMT_END,
          p.TLMT_NO,
          p.CANCL_RESN,
          p.SHADOW_PEN,
          p.SHADOW_INT,
          p.SHADOW_CMP,
          p.BAL_NINT01,
          p.BAL_NINT02,
          p.BAL_NINT03,
          p.BAL_NINT04,
          p.BAL_NINT05,
          p.BAL_NINT06,
          p.BAL_NINT07,
          p.BAL_NINT08,
          p.BAL_NINT09,
          p.BAL_NINT10,
          p.STM_NINT01,
          p.STM_NINT02,
          p.STM_NINT03,
          p.STM_NINT04,
          p.STM_NINT05,
          p.STM_NINT06,
          p.STM_NINT07,
          p.STM_NINT08,
          p.STM_NINT09,
          p.STM_NINT10,
          p.CURR_NUM2,
          p.WROF_FLAG,
          p.LAYERCODER1,
          p.LAYERCODER2,
          p.MCNTRL_YN,
          p.NCRED_RSN,
          p.BSC_CRED,
          p.CUSTR_REF,
          p.PBC_BRNCH,
          p.STOPMP_YN,
          p.CRED_LMT2,
          p.BAL_CMPFEE
     FROM FDM_S24_CUR_ACCT p;

