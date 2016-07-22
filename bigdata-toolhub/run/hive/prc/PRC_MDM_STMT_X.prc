INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_STMT_X(IN ETL_DATE STRING, OUT RESULT STRING)
BEGIN

    DECLARE   v_sql   string;
    DECLARE   v_ETL_DATE string;

    DBMS_OUTPUT.PUT_LINE ('PRC_MDM_STMT_X START');
    DBMS_OUTPUT.PUT_LINE ('ETL_DATE ==' || ETL_DATE);

    SET RESULT = '0';

      IF ETL_DATE <> ' '
       THEN
          select todate(ETL_DATE,'yyyymmdd') into v_ETL_DATE from dual;
          DBMS_OUTPUT.PUT_LINE ('v_ETL_DATE==' || v_ETL_DATE);
       ELSE
          DBMS_OUTPUT.PUT_LINE ('V_ETL_DATE 为空');

          SELECT SYSDATE() INTO v_ETL_DATE FROM DUAL;
       END IF;

       --删除变动的数据
       DBMS_OUTPUT.PUT_LINE('START EXECUTE DELETE TABLE MDM_STMT_X');
       DBMS_OUTPUT.PUT_LINE('before:=========' || v_sql1);
       v_sql1 := 'TRUNCATE TABLE MDM_STMT_X';
       EXECUTE v_sql1;


       DBMS_OUTPUT.PUT_LINE('before:=========' || v_sql);

       --插入每天新增和变动的数据

    v_sql := ' INSERT INTO MDM_STMT_X ';
    v_sql := v_sql||' SELECT ';
    v_sql := v_sql||'     COAL(T1.XACCOUNT, T2.XACCOUNT)';
    v_sql := v_sql||'    ,COAL(T1.BANK,     T2.BANK) ';
    v_sql := v_sql||'    ,COAL(T1.CATEGORY, T2.CATEGORY) ';
    v_sql := v_sql||'    ,COAL(T1.CYCLE_NBR,T2.CYCLE_NBR) ';
    v_sql := v_sql||'    ,COAL(T1.MONTH_NBR,T2.MONTH_NBR) ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,  COAL(T1.AGE1 ,0) ';
    v_sql := v_sql||'     + COAL(T1.AGE2 ,0) ';
    v_sql := v_sql||'     + COAL(T1.AGE3 ,0) ';
    v_sql := v_sql||'     + COAL(T1.AGE4 ,0) ';
    v_sql := v_sql||'     + COAL(T1.AGE5 ,0) ';
    v_sql := v_sql||'     + COAL(T1.AGE6 ,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE1 ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE2 ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE3 ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE4 ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE5 ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'     + COAL(T2.AGE6 ,0) * COAL(B.RATE_VAL0,0)  AS AGE ';
    v_sql := v_sql||'    ,COAL(T1.BAL_CMPINT,0) + COAL(T2.BAL_CMPINT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.BAL_FREE,0) + COAL(T2.BAL_FREE ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,  CASE WHEN COAL(T1.BALINTFLAG,''0'') = ''+'' ';
    v_sql := v_sql||'            THEN   COAL(T1.BAL_INT ,0) ';
    v_sql := v_sql||'            ELSE   COAL(T1.BAL_INT ,0) * -1 END ';
    v_sql := v_sql||'     + CASE WHEN COAL(T2.BALINTFLAG,''0'') = ''+'' ';
    v_sql := v_sql||'            THEN  COAL(T2.BAL_INT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'            ELSE  COAL(T2.BAL_INT ,0) * COAL(B.RATE_VAL0,0) * -1 END ';
    v_sql := v_sql||'     AS    BAL_INT ';
    v_sql := v_sql||'    ,COAL(T1.BAL_NOINT,0)  + COAL(T2.BAL_NOINT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.BAL_ORINT,0)  + COAL(T2.BAL_ORINT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.BRANCH,T2.BRANCH) ';
    v_sql := v_sql||'    ,COAL(T1.BUSINESS,T2.BUSINESS) ';
    v_sql := v_sql||'    ,COAL(t1.BAL_NINT02,0) ';
    v_sql := v_sql||'    ,COAL(T1.CASH_ADFEE,0)  + COAL(T2.CASH_ADFEE ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.CASH_ADVCE,0)  + COAL(T2.CASH_ADVCE ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,  CASE WHEN T1.CLSBAL_FLAG = ''+'' ';
    v_sql := v_sql||'            THEN  T1.CLOSE_BAL ';
    v_sql := v_sql||'            ELSE  T1.CLOSE_BAL * -1 END ';
    v_sql := v_sql||'     + CASE WHEN T2.CLSBAL_FLAG = ''+'' ';
    v_sql := v_sql||'            THEN  COAL(T2.CLOSE_BAL ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'            ELSE  COAL(T2.CLOSE_BAL ,0) * COAL(B.RATE_VAL0,0) * -1 END ';
    v_sql := v_sql||'     AS    CLOSE_BAL ';
    v_sql := v_sql||'    ,COAL(T1.CLOSE_CODE,T2.CLOSE_CODE) ';
    v_sql := v_sql||'    ,COAL(T1.CLOSE_DATE,T2.CLOSE_DATE) ';
    v_sql := v_sql||'    ,COAL(T1.CRED_ADJ,0) + COAL(T2.CRED_ADJ ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.CRED_LIMIT,0) ';
    v_sql := v_sql||'    ,COAL(T1.CRED_VOUCH,0) ';
    v_sql := v_sql||'    ,COAL(T1.CREDLIM_X ,0) ';
    v_sql := v_sql||'    ,COAL(T1.CY_CHGCNT ,0) ';
    v_sql := v_sql||'    ,COAL(T1.CY_CHGDAY ,0) ';
    v_sql := v_sql||'    ,COAL(T1.CY_EFFDAY ,0) ';
    v_sql := v_sql||'    ,COAL(T1.CYCLE_NEW ,0) ';
    v_sql := v_sql||'    ,COAL(T1.DAYSPAY   ,T2.DAYSPAY) ';
    v_sql := v_sql||'    ,COAL(T1.DEBIT_ADJ,0) + COAL(T2.DEBIT_ADJ ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.DUNCODE1  ,T2.DUNCODE1) ';
    v_sql := v_sql||'    ,COAL(T1.DUNCODE2  ,T2.DUNCODE2) ';
    v_sql := v_sql||'    ,COAL(T1.DUNCODEB  ,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.DUNLETR1  ,T2.DUNLETR1) ';
    v_sql := v_sql||'    ,COAL(T1.DUNLETR2  ,T2.DUNLETR2) ';
    v_sql := v_sql||'    ,COAL(T1.DUNLETRB  ,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.DUTY_CREDT,0) + COAL(T2.DUTY_CREDT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.DUTY_DEBIT,0) + COAL(T2.DUTY_DEBIT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.FEES_TAXES,0) + COAL(T2.FEES_TAXES ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INSTL_AMT,0)  + COAL(T2.INSTL_AMT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_CHDCMP,0) + COAL(T2.INT_CHDCMP ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_CHGD,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_CMPOND,0) + COAL(T2.INT_CMPOND ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_CUNOT,0) + COAL(T2.INT_CUNOT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_CURCMP,0) + COAL(T2.INT_CURCMP ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_EARNED,0) + COAL(T2.INT_EARNED ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_RATE,0) ';
    v_sql := v_sql||'    ,COAL(T1.INT_TAXRTE,0) ';
    v_sql := v_sql||'    ,COAL(T1.LETR_CODE1,T2.LETR_CODE1) ';
    v_sql := v_sql||'    ,COAL(T1.LETR_CODE2,T2.LETR_CODE2) ';
    v_sql := v_sql||'    ,COAL(T1.LOSSES,0)  + COAL(T2.LOSSES ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.MIN_DUE,0) + COAL(T2.MIN_DUE ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.MIN_DUE_DT,T2.MIN_DUE_DT) ';
    v_sql := v_sql||'    ,COAL(T1.MINDUE_R,T2.MINDUE_R) ';
    v_sql := v_sql||'    ,COAL(T1.MSG_KEY, T2.MSG_KEY) ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,''0'' ';
    v_sql := v_sql||'    ,COAL(T2.NEW_EXAMT,0) ';
    v_sql := v_sql||'    ,CASE WHEN coal(T1.ODUE_FLAG,'' '') <> 0 THEN 1 ';
    v_sql := v_sql||'          WHEN coal(T2.ODUE_FLAG,'' '') <> 0 THEN 1 ';
    v_sql := v_sql||'          ELSE 0 END  ';
    v_sql := v_sql||'    ,T1.ODUE_HELD + COAL(T2.ODUE_HELD ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,  CASE WHEN T1.OPBAL_FLAG = ''+'' ';
    v_sql := v_sql||'            THEN  T1.OPEN_BAL ';
    v_sql := v_sql||'            ELSE  T1.OPEN_BAL * -1 END ';
    v_sql := v_sql||'     + CASE WHEN T2.OPBAL_FLAG = ''+'' ';
    v_sql := v_sql||'            THEN  COAL(T2.OPEN_BAL ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'            ELSE  COAL(T2.OPEN_BAL ,0) * COAL(B.RATE_VAL0,0) * -1 END ';
    v_sql := v_sql||'     AS    OPEN_BAL  ';
    v_sql := v_sql||'    ,COAL(T1.OPEN_DATE,T2.OPEN_DATE)  ';
    v_sql := v_sql||'    ,COAL(T1.BAL_NINT07,0) ';
    v_sql := v_sql||'    ,COAL(T1.PAYMENT,0)     + COAL(T2.PAYMENT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.PAYMT_UNCL,0)  + COAL(T2.PAYMT_UNCL ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(t1.BAL_NINT03,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_ADJ,0) ';
    v_sql := v_sql||'    ,COAL(T1.PTADJ_FLAG,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.POINT_CLM,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_CUM,0) ';
    v_sql := v_sql||'    ,COAL(T1.PTFLAG,'' '')  ';
    v_sql := v_sql||'    ,COAL(T1.POINT_EAR,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_ENC,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_IMP,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_EXP,0) ';
    v_sql := v_sql||'    ,COAL(T1.PRIOR_NO ,T2.PRIOR_NO) ';
    v_sql := v_sql||'    ,COAL(T1.PURCHASES,0)   + COAL(T2.PURCHASES ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.QUERY_AMT,0)   + COAL(T2.QUERY_AMT ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.QUERY_CODE,T2.QUERY_CODE) ';
    v_sql := v_sql||'    ,COAL(T1.RECVRY_AMT,T2.RECVRY_AMT) ';
    v_sql := v_sql||'    ,COAL(T1.REVCRY_AMT,T2.REVCRY_AMT) ';
    v_sql := v_sql||'    ,COAL(T1.STM_AMT_OL,0)  + COAL(T2.STM_AMT_OL ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T2.REM_EXAMT,0)    ';
    v_sql := v_sql||'    ,COAL(T1.STMT_CODE,'' '')  ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_AAMT,0)   ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_ACCT,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_ADAY,0)   ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_DAMT,0)   ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_DDAY,0)   ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_IDNO,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_IDTY,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_NAME,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.REPAY_RESP,'' '') ';
    v_sql := v_sql||'    ,COAL(T1.BAL_MP,0)      + COAL(T2.BAL_MP ,0) * COAL(B.RATE_VAL0,0) ';
    v_sql := v_sql||'    ,COAL(T1.POINT_CARD,0) ';
    v_sql := v_sql||'    ,COAL(T1.PAY_FLAG,T2.PAY_FLAG) ';
    v_sql := v_sql||'    ,COAL(t1.CASH_ADFEE,0) + COAL(t1.BAL_NINT03,0)';
    v_sql := v_sql||'    ,COAL(T1.NBR_CASHAD_NEW,0) + COAL(T2.NBR_CASHAD_NEW ,0) ';
    v_sql := v_sql||'    ,COAL(T1.NBR_FEEDTY_NEW,0) + COAL(T2.NBR_FEEDTY_NEW ,0) ';
    v_sql := v_sql||'    ,COAL(T1.NBR_OTHERS_NEW,0) + COAL(T2.NBR_OTHERS_NEW ,0) ';
    v_sql := v_sql||'    ,COAL(T1.NBR_PAYMNT_NEW,0) + COAL(T2.NBR_PAYMNT_NEW ,0) ';
    v_sql := v_sql||'    ,COAL(T1.NBR_PURCH_NEW,0)  + COAL(T2.NBR_PURCH_NEW ,0)  ';
    v_sql := v_sql||'    ,COAL(T1.BAL_CMPFEE,0) ';
    v_sql := v_sql||'    ,date_add(add_months(toDate(''19900101'',''yyyy-mm-dd''), T1.MONTH_NBR-1),T1.CYCLE_NBR-1) ';
    v_sql := v_sql||'    ,CASE WHEN COAL(T1.BAL_NINT02,0) > 0 AND COAL(T1.ODUE_FLAG,0) = 1 THEN ''1'' ';
    v_sql := v_sql||'          WHEN COAL(T2.BAL_NINT02,0) > 0 AND COAL(T2.ODUE_FLAG,0) = 1 THEN ''1''  ';
    v_sql := v_sql||'           ELSE ''0'' ';
    v_sql := v_sql||'           END AS ANNUAL_FEE_OVD_FLAG ';
    v_sql := v_sql||'    ,A.MTHS_ODUE       AS ODUE_MTHS  ';
    v_sql := v_sql||'    ,A.BAL_ADD14  ';
    v_sql := v_sql||'    ,CASE WHEN (A.TLMT_BEG + 0) <= ' ||v_ETL_DATE || ' AND ' ||v_ETL_DATE || ' < (A.TLMT_END + 0)  AND A.TEMP_LIMIT > 0 ';
    v_sql := v_sql||'          THEN A.TEMP_LIMIT ';
    v_sql := v_sql||'       ELSE A.CRED_LIMIT     END     AS C_LIMIT ';
    v_sql := v_sql||'    ,A.BAL ';
    v_sql := v_sql||'    ,CASE WHEN (A.TLMT_BEG + 0) <= ' ||v_ETL_DATE || ' AND ' ||v_ETL_DATE || ' < (A.TLMT_END + 0)  AND A.TEMP_LIMIT > 0 ';
    v_sql := v_sql||'          THEN A.TEMP_LIMIT ';
    v_sql := v_sql||'       ELSE A.CRED_LIMIT   END    AS  LIMIT  ';
    v_sql := v_sql||'    ,CASE WHEN (A.TLMT_BEG + 0) <= ' ||v_ETL_DATE || ' AND ' ||v_ETL_DATE || ' < (A.TLMT_END + 0)  AND A.TEMP_LIMIT > 0 ';
    v_sql := v_sql||'          THEN A.TEMP_LIMIT + A.MP_L_LMT ';
    v_sql := v_sql||'       ELSE A.CRED_LIMIT + A.MP_L_LMT    END    AS  LIMIT_MP ';
    v_sql := v_sql||'    ,A.BAL_WO_AUTHS ';
    v_sql := v_sql||' FROM V_S24_STMT T1 ';
    v_sql := v_sql||' FULL JOIN v_S24_STMX T2 ';
    v_sql := v_sql||'   ON T1.XACCOUNT = T2.XACCOUNT ';
    v_sql := v_sql||'      AND T1.MONTH_NBR = T2.MONTH_NBR ';
    v_sql := v_sql||' LEFT JOIN MDM_ACCT_X A ';
    v_sql := v_sql||'   ON ';
    v_sql := v_sql||'      A.XACCOUNT = COAL(T1.XACCOUNT ,T2.XACCOUNT ) ';
    v_sql := v_sql||' LEFT JOIN V_PRMXR B ';
    v_sql := v_sql||'   ON A.BANK = B.BANK ';
    v_sql := v_sql||'      AND A.CURR_NUM2 = B.CURR_NUM ';

    DBMS_OUTPUT.PUT_LINE('after:==========' || v_sql);

    execute immediate v_sql;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_STMT_X END');

SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_STMT_X(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;
