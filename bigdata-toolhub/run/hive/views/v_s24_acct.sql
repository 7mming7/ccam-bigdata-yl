use zhgl;

CREATE VIEW if not exists V_S24_ACCT AS
SELECT XACCOUNT,
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
          AGE1 / 100 AS AGE1,
          AGE2 / 100 AS AGE2,
          AGE3 / 100 AS AGE3,
          AGE4 / 100 AS AGE4,
          AGE5 / 100 AS AGE5,
          AGE6 / 100 AS AGE6,
          APP_SOURCE,
          AUTH_CASH / 100 AS AUTH_CASH,
          AUTH_OVER / 100 AS AUTH_OVER,
          AUTHOV_YN,
          AUTHS_AMT / 100 AS AUTHS_AMT,
          BAL_CMPINT / 100 AS BAL_CMPINT,
          BAL_FREE / 100 AS BAL_FREE,
          BAL_INT / 100 AS BAL_INT,
          BAL_INTFLAG,
          BAL_NOINT / 100 AS BAL_NOINT,
          BAL_ORINT / 100 AS BAL_ORINT,
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
          CA_PCNT / 100 AS CA_PCNT,
          CARD_FEES / 100 AS CARD_FEES,
          CARDS_CANC,
          CARDS_ISSD,
          CASH_1ST,
          CASH_ADFEE / 100 AS CASH_ADFEE,
          CASH_ADVCE / 100 AS CASH_ADVCE,
          CASH_TDAY / 100 AS CASH_TDAY,
          CASH1STAC,
          CLASS_CHDY,
          CLASS_CODE,
          CLOSE_CHDY,
          CLOSE_CODE,
          COLLS_DAY,
          CRDACT_DAY,
          CRDNXT_DAY,
          CRED_ACTIV,
          CRED_ADJ / 100 AS CRED_ADJ,
          CRED_CHDAY,
          CRED_LIMIT,
          CRED_VOUCH / 100 AS CRED_VOUCH,
          CREDLIM_X,
          CURR_NUM,
          CUTOFF_DAY,
          CY_CHGCNT,
          CY_CHGDAY,
          CY_EFFDAY,
          CY_YRCNT,
          CYCLE_NEW,
          CYCLE_PRV,
          DEBIT_ADJ / 100 AS DEBIT_ADJ,
          DISHNRDAY,
          DUTY_CREDT / 100 AS DUTY_CREDT,
          DUTY_DEBIT / 100 AS DUTY_DEBIT,
          EXCH_CODE,
          EXCH_FLAG,
          EXCH_PERC,
          EXCH_RTDT,
          FEE_MONTH,
          FEES_TAXES / 100 AS FEES_TAXES,
          FIX_EXAMT / 100 AS FIX_EXAMT,
          GUARN_FLAG,
          HI_CASHADV,
          HI_CASMMYY,
          HI_CRDMMYY,
          HI_CREDIT,
          HI_DEBIT,
          HI_DEBMMYY,
          HI_MP_PUR / 100 AS HI_MP_PUR,
          HI_MPMMYY,
          HI_OLIMIT,
          HI_OLIMMYY,
          HI_PURCHSE,
          HI_PURMMYY,
          INT_CASH / 10000 AS INT_CASH,
          INT_CHDCMP / 10000 AS INT_CHDCMP,
          INT_CHDY,
          INT_CHGD / 10000 AS INT_CHGD,
          INT_CMPOND / 10000 AS INT_CMPOND,
          INT_CODE,
          INT_CUNOT / 10000 AS INT_CUNOT,
          INT_CURCMP / 10000 AS INT_CURCMP,
          INT_EARNED / 10000 AS INT_EARNED,
          INT_NOTION / 10000 AS INT_NOTION,
          INT_PROCDY,
          INT_RATE / 10000 AS INT_RATE,
          INT_RATECR / 10000 AS INT_RATECR,
          INT_UPTODY,
          LANG_CODE,
          LAST_TRDAY,
          LASTAUTHDY,
          LASTNTDATE,
          LASTPAYAMT / 100 AS LASTPAYAMT,
          LASTPAYDAY,
          LOSSES / 100 AS LOSSES,
          MONTH_NBR,
          MONTR_CHDY,
          MONTR_CODE,
          MP_AM_TDY / 100 AS MP_AM_TDY,
          MP_AUTHS / 100 AS MP_AUTHS,
          MP_BAL / 100 AS MP_BAL,
          MP_BIL_AMT / 100 AS MP_BIL_AMT,
          MP_LIMIT,
          MP_NO_TDY,
          MP_REM_PPL / 100 AS MP_REM_PPL,
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
          ODUE_HELD / 100 AS ODUE_HELD,
          OLFLAG,
          OTHER_FEES,
          PAY_FLAG,
          PAY1ST_IND,
          PAYMT_CLRD / 100 AS PAYMT_CLRD,
          PAYMT_TDAY / 100 AS PAYMT_TDAY,
          PAYMT_UNCL / 100 AS PAYMT_UNCL,
          PEN_CHRG / 100 AS PEN_CHRG,
          PENCHG_ACC / 100 AS PENCHG_ACC,
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
          PURCHASES / 100 AS PURCHASES,
          QUERY_AMT / 100 AS QUERY_AMT,
          QUERY_CODE,
          QUERY_STMT,
          RECLA_CHDY,
          RECLA_CODE,
          RECVRY_AMT / 100 AS RECVRY_AMT,
          REPAY_AMT / 100 AS REPAY_AMT,
          REPAY_AMTX / 100 AS REPAY_AMTX,
          REPAY_CODE,
          REPAY_CODX,
          REPAY_DAY,
          REPAY_PCT,
          REPAY_PCTX,
          REPY_CHGDY,
          SCORE_PTS,
          STATEMENTS,
          STM_AMT_OL / 100 AS STM_AMT_OL,
          STM_BALFRE / 100 AS STM_BALFRE,
          STM_BALINT / 100 AS STM_BALINT,
          STMBALINTFLAG,
          STM_BALNCE / 100 AS STM_BALNCE,
          STM_BALFLAG,
          STM_BALORI / 100 AS STM_BALORI,
          STM_CLOSDY,
          STM_CODE,
          STM_INSTL / 100 AS STM_INSTL,
          STM_MINDUE / 100 AS STM_MINDUE,
          STM_NOINT / 100 AS STM_NOINT,
          STM_OLFLAG,
          STM_OVERDU / 100 AS STM_OVERDU,
          STM_PAY_UN / 100 AS STM_PAY_UN,
          STM_QRYAMT / 100 AS STM_QRYAMT,
          STM_REPAY / 100 AS STM_REPAY,
          STMT_DD,
          STMT_PULL,
          TODAY_AMT / 100 AS TODAY_AMT,
          TODAY_AMTFLAG,
          TODAY_REL,
          UNCL_PCT / 100 AS UNCL_PCT,
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
          BAL_MP / 100 AS BAL_MP,
          BAL_MPFLAG,
          STM_BALMP / 100 AS STM_BALMP,
          STM_BMFLAG,
          LMT_RSN,
          POINT_CARD,
          PT_CDFLAG,
          TEMP_LIMIT,
          TLMT_BEG,
          TLMT_END,
          TLMT_NO,
          CANCL_RESN,
          SHADOW_PEN / 100 AS SHADOW_PEN,
          SHADOW_INT / 100 AS SHADOW_INT,
          SHADOW_CMP / 100 AS SHADOW_CMP,
          BAL_NINT01 / 100 AS BAL_NINT01,
          BAL_NINT02 / 100 AS BAL_NINT02,
          BAL_NINT03 / 100 AS BAL_NINT03,
          BAL_NINT04 / 100 AS BAL_NINT04,
          BAL_NINT05 / 100 AS BAL_NINT05,
          BAL_NINT06 / 100 AS BAL_NINT06,
          BAL_NINT07 / 100 AS BAL_NINT07,
          BAL_NINT08 / 100 AS BAL_NINT08,
          BAL_NINT09 / 100 AS BAL_NINT09,
          BAL_NINT10 / 100 AS BAL_NINT10,
          STM_NINT01 / 100 AS STM_NINT01,
          STM_NINT02 / 100 AS STM_NINT02,
          STM_NINT03 / 100 AS STM_NINT03,
          STM_NINT04 / 100 AS STM_NINT04,
          STM_NINT05 / 100 AS STM_NINT05,
          STM_NINT06 / 100 AS STM_NINT06,
          STM_NINT07 / 100 AS STM_NINT07,
          STM_NINT08 / 100 AS STM_NINT08,
          STM_NINT09 / 100 AS STM_NINT09,
          STM_NINT10 / 100 AS STM_NINT10,
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
          BAL_CMPFEE / 100 AS BAL_CMPFEE
     FROM S24_ACCT;

