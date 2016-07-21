INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_CARD_INFO(IN ETL_DATE STRING, OUT RESULT STRING)

BEGIN

    DECLARE  v_sql      string;
    DECLARE  ETL_DATE   string;
    DECLARE  v_ETL_DATE       string;

    SET RESULT = '0';

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CARD_INFO START');
    DBMS_OUTPUT.PUT_LINE('ETL_DATE ==' || ETL_DATE);

    IF ETL_DATE <>' ' THEN
    select todate(ETL_DATE,'yyyymmdd') into v_ETL_DATE from dual;

    DBMS_OUTPUT.PUT_LINE('v_ETL_DATE2==' || v_ETL_DATE );

    ELSE
       DBMS_OUTPUT.PUT_LINE('ETL_DATE 为空'  );
       SELECT  SYSDATE() INTO v_ETL_DATE  FROM dual;
    END IF;


    --删除批次已经加载的数据
    EXECUTE IMMEDIATE('truncate table  MDM_CARD_INFO');

    DBMS_OUTPUT.PUT_LINE('before:=========' || v_sql);

    --汇总客户卡片信息数据
    v_sql := ' INSERT INTO MDM_CARD_INFO ';
    v_sql := v_sql||' SELECT ';
    v_sql := v_sql||'   T0.BANK ';
    v_sql := v_sql||'   ,T0.XACCOUNT ';
    v_sql := v_sql||'   ,T0.CUSTR_NBR ';
    v_sql := v_sql||'   ,COAL(T1.CARD_NBR,'' '') ';
    v_sql := v_sql||'   ,COAL(T2.CANCL_CODE,'' '') ';
    v_sql := v_sql||'   ,COAL(T2.PRODUCT,0) ';
    v_sql := v_sql||'   ,T0.DAY_OPENED ';
    v_sql := v_sql||'   ,COAL(T3.COUNT,1) ';
    v_sql := v_sql||'   ,CASE WHEN COAL(T4.COUNT,0) > 0 THEN ''1'' ELSE ''0'' END ';
    v_sql := v_sql||'   ,''0'' ';
    v_sql := v_sql||'   ,''0'' ';
    v_sql := v_sql||'   ,''0'' ';
    v_sql := v_sql||'   ,''0'' ';
    v_sql := v_sql||'   ,''0'' ';
    v_sql := v_sql||'   ,''1'' ';
    v_sql := v_sql||'   ,COAL(T5.APP_DAY,T0.DAY_OPENED) ';
    v_sql := v_sql||'   ,''1'' ';
    v_sql := v_sql||'   , datediff(toDate('' '||v_ETL_DATE||' '',''yyyy-mm-dd''), ';
    v_sql := v_sql||'   COAL(toDATE(T6.CANCL_DAY,''yyyy-mm-dd''),toDate('' '||v_ETL_DATE||' '',''yyyy-mm-dd''))) ';
    v_sql := v_sql||'   ,CONCAT(''20'',T2.EXPIRY_DTE,''01'' )';
    v_sql := v_sql||'   ,CASE WHEN T7.CANCL_CODE IN (''T'',''Q'') THEN ''1'' ELSE ''0'' END ';
    v_sql := v_sql||'   ,CASE WHEN T0.MTHS_ODUE > 0 THEN ''1'' ELSE ''0'' END ';
    v_sql := v_sql||' FROM FDM_S24_CUR_ACCT T0 ';
    v_sql := v_sql||' LEFT JOIN (SELECT ';
    v_sql := v_sql||'       XACCOUNT ';
    v_sql := v_sql||'       ,MAX(CARD_NBR +0) AS CARD_NBR ';
    v_sql := v_sql||'       FROM FDM_S24_CUR_CARD AA ';
    v_sql := v_sql||'       WHERE CARDHOLDER = ''1'' ';
    v_sql := v_sql||'       GROUP BY XACCOUNT) T1 ';
    v_sql := v_sql||'   ON  T0.XACCOUNT  = T1.XACCOUNT ';
    v_sql := v_sql||' LEFT JOIN FDM_S24_CUR_CARD T2 ';
    v_sql := v_sql||' ON  T1.CARD_NBR  = T2.CARD_NBR ';
    v_sql := v_sql||' LEFT JOIN (SELECT ACCOUNT ';
    v_sql := v_sql||'      ,COUNT(DISTINCT BRANCH) AS COUNT ';
    v_sql := v_sql||'       FROM FDM_S24_APMA BB ';
    v_sql := v_sql||'       WHERE DECCAN_CDE = ''A'' ';
    v_sql := v_sql||'       AND CRDLMT_REQ + CRLM_X_REQ  >= 100 ';
    v_sql := v_sql||'       GROUP BY ACCOUNT) T3 ';
    v_sql := v_sql||' ON  T0.XACCOUNT  = T3.ACCOUNT ';
    v_sql := v_sql||' LEFT JOIN (SELECT ';
    v_sql := v_sql||'      XACCOUNT ';
    v_sql := v_sql||'      ,COUNT(*) AS COUNT ';
    v_sql := v_sql||'      FROM FDM_S24_CUR_CARD CC ';
    v_sql := v_sql||'      WHERE CANCL_CODE = ''F'' ';
    v_sql := v_sql||'      GROUP BY BANK,XACCOUNT) T4  ';
    v_sql := v_sql||' ON  T0.XACCOUNT  = T4.XACCOUNT ';
    v_sql := v_sql||' LEFT JOIN (SELECT ';
    v_sql := v_sql||'      ACCOUNT ';
    v_sql := v_sql||'      ,MIN(APP_DAY) AS APP_DAY ';
    v_sql := v_sql||'      FROM FDM_S24_APMA  DD ';
    v_sql := v_sql||'      WHERE DECCAN_CDE = ''A'' AND ADDL_CARD <> ''1'' ';
    v_sql := v_sql||'      GROUP BY ACCOUNT) T5 ';
    v_sql := v_sql||'   ON  T0.XACCOUNT  = T5.ACCOUNT ';
    v_sql := v_sql||' LEFT JOIN (SELECT ';
    v_sql := v_sql||'      XACCOUNT ';
    v_sql := v_sql||'      ,MAX(CANCL_DAY) AS CANCL_DAY ';
    v_sql := v_sql||'      FROM FDM_S24_CUR_CARD EE ';
    v_sql := v_sql||'      WHERE CANCL_CODE IN (''F'',''Q'') ';
    v_sql := v_sql||'      GROUP BY BANK,XACCOUNT) T6 ';
    v_sql := v_sql||' ON  T0.XACCOUNT  = T6.XACCOUNT ';
    v_sql := v_sql||' LEFT JOIN FDM_S24_CUR_CARD T7 ';
    v_sql := v_sql||' ON  T1.CARD_NBR  = T7.CARD_NBR ';
    v_sql := v_sql||' AND T7.CARD_NBR = T7.CARD_ID ';

    DBMS_OUTPUT.PUT_LINE('after:==========' || v_sql);

    execute immediate v_sql;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CARD_INFO END');

    SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_CARD_INFO(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;
