use zhgl;

CREATE VIEW if not exists V_S24_STMT AS
SELECT XACCOUNT,
          BANK,
          CATEGORY,
          CYCLE_NBR,
          MONTH_NBR,
          POST_DD,
          AGE1 / 100 AS AGE1,
          AGE2 / 100 AS AGE2,
          AGE3 / 100 AS AGE3,
          AGE4 / 100 AS AGE4,
          AGE5 / 100 AS AGE5,
          AGE6 / 100 AS AGE6,
          BAL_CMPINT / 100 AS BAL_CMPINT,
          BAL_FREE / 100 AS BAL_FREE,
          BAL_INT / 100 AS BAL_INT,
          BALINTFLAG,
          BAL_NOINT / 100 AS BAL_NOINT,
          BAL_ORINT / 100 AS BAL_ORINT,
          BRANCH,
          BUSINESS,
          CARD_FEES / 100 AS CARD_FEES,
          CASH_ADFEE / 100 AS CASH_ADFEE,
          CASH_ADVCE / 100 AS CASH_ADVCE,
          CLOSE_BAL / 100 AS CLOSE_BAL,
          CLSBAL_FLAG,
          CLOSE_CODE,
          CLOSE_DATE,
          CRED_ADJ / 100 AS CRED_ADJ,
          CRED_LIMIT,
          CRED_VOUCH / 100 AS CRED_VOUCH,
          CREDLIM_X,
          CY_CHGCNT,
          CY_CHGDAY,
          CY_EFFDAY,
          CYCLE_NEW,
          DAYSPAY,
          DEBIT_ADJ / 100 AS DEBIT_ADJ,
          DUNCODE1,
          DUNCODE2,
          DUNCODEB,
          DUNLETR1,
          DUNLETR2,
          DUNLETRB,
          DUTY_CREDT / 100 AS DUTY_CREDT,
          DUTY_DEBIT / 100 AS DUTY_DEBIT,
          FEES_TAXES / 100 AS FEES_TAXES,
          INSTL_AMT / 100 AS INSTL_AMT,
          INT_CHDCMP / 10000 AS INT_CHDCMP,
          INT_CHGD / 10000 AS INT_CHGD,
          INT_CMPOND / 10000 AS INT_CMPOND,
          INT_CUNOT / 10000 AS INT_CUNOT,
          INT_CURCMP / 10000 AS INT_CURCMP,
          INT_EARNED / 10000 AS INT_EARNED,
          INT_RATE / 10000 AS INT_RATE,
          INT_TAXRTE / 100 AS INT_TAXRTE,
          LETR_CODE1,
          LETR_CODE2,
          LOSSES / 100 AS LOSSES,
          MIN_DUE / 100 AS MIN_DUE,
          MIN_DUE_DT,
          MINDUE_R / 100 AS MINDUE_R,
          MSG_KEY,
          NBR_CASHAD,
          NBR_FEEDTY,
          NBR_OTHERS,
          NBR_PAYMNT,
          NBR_PURCH,
          ODUE_FLAG,
          ODUE_HELD / 100 AS ODUE_HELD,
          OPEN_BAL / 100 AS OPEN_BAL,
          OPBAL_FLAG,
          OPEN_DATE,
          OTHER_FEES / 100 AS OTHER_FEES,
          PAYMENT / 100 AS PAYMENT,
          PAYMT_UNCL / 100 AS PAYMT_UNCL,
          PEN_CHRG / 100 AS PEN_CHRG,
          POINT_ADJ,
          PTADJ_FLAG,
          POINT_CLM,
          POINT_CUM,
          PTFLAG,
          POINT_EAR,
          POINT_ENC,
          POINT_IMP,
          POINT_EXP,
          PRIOR_NO,
          PURCHASES / 100 AS PURCHASES,
          QUERY_AMT / 100 AS QUERY_AMT,
          QUERY_CODE,
          RECVRY_AMT / 100 AS RECVRY_AMT,
          REVCRY_AMT / 100 AS REVCRY_AMT,
          STM_AMT_OL / 100 AS STM_AMT_OL,
          STMT_CODE,
          REPAY_AAMT / 100 AS REPAY_AAMT,
          REPAY_ACCT,
          REPAY_ADAY,
          REPAY_DAMT / 100 AS REPAY_DAMT,
          REPAY_DDAY,
          REPAY_IDNO,
          REPAY_IDTY,
          REPAY_NAME,
          REPAY_RESP,
          BAL_MP / 100 AS BAL_MP,
          POINT_CARD,
          PAY_FLAG,
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
          NBR_CASHAD_NEW,
          NBR_FEEDTY_NEW,
          NBR_OTHERS_NEW,
          NBR_PAYMNT_NEW,
          NBR_PURCH_NEW,
          BAL_CMPFEE / 100 AS BAL_CMPFEE
     FROM S24_STMT;
