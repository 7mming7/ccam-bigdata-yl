INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_CARD(IN ETL_DATE STRING, OUT RESULT STRING)

BEGIN
    DECLARE  v_ETL_DATE  string;

    DECLARE  v_sql  string;

    DBMS_OUTPUT.PUT_LINE ('PRC_MDM_CARD START');
    DBMS_OUTPUT.PUT_LINE ('ETL_DATE ==' || ETL_DATE);

    SET RESULT = '0';

    IF ETL_DATE <> ' '
    THEN
        select todate(ETL_DATE,'yyyymmdd') into v_ETL_DATE from dual;
        DBMS_OUTPUT.PUT_LINE ('v_ETL_DATE2==' || v_ETL_DATE);
    ELSE
        DBMS_OUTPUT.PUT_LINE ('V_ETL_DATE 为空');

        SELECT SYSDATE() INTO v_ETL_DATE FROM DUAL;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('v_ETL_DATE3 ==' || v_ETL_DATE);

   --删除变动的数据
   EXECUTE IMMEDIATE ('truncate table  MDM_CARD');

   DBMS_OUTPUT.PUT_LINE('before:=========' || v_sql);

   --插入每天新增和变动的数据
    v_sql := 'INSERT INTO MDM_CARD ';
    v_sql := v_sql||' SELECT AA.CARD_NBR,';
    v_sql := v_sql||'    AA.BANK,';
    v_sql := v_sql||'    AA.XACCOUNT,';
    v_sql := v_sql||'    AA.PRODUCT,';
    v_sql := v_sql||'    AA.CANCL_CODE,';
    v_sql := v_sql||'    AA.PRODUCT,';
    v_sql := v_sql||'    AA.ISSUE_DAY,';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    CASE AA.CANCL_CODE WHEN ''F'' THEN ''1'' ELSE ''0'' END AS QIZHA_FLAG,';
    v_sql := v_sql||'    '' '',';
    v_sql := v_sql||'    '' '',';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    ''1'',';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    ''1'',';
    v_sql := v_sql||'ROUND(MONTHS_BETWEEN(concat(toDate(AA.EXPIRY_DTE + 200000, ''yyyy-mm''),''-01''), SYSDATE()), 1),';
    v_sql := v_sql||'    AA.EXPIRY_DTE,';
    v_sql := v_sql||'    ''0'',';
    v_sql := v_sql||'    '' '',';
    v_sql := v_sql||'    AA.ACTIVE_DAY,';
    v_sql := v_sql||'    AA.CARDHOLDER,';
    v_sql := v_sql||'    AA.ISS_SERIAL ';
    v_sql := v_sql||' FROM FDM_S24_CUR_CARD AA ';
    v_sql := v_sql||'      INNER JOIN V_MAIN_ACCT main ON AA.XACCOUNT = main.XACCOUNT ';
    v_sql := v_sql||'      INNER JOIN ';
    v_sql := v_sql||'      (SELECT ROW_NUMBER() OVER (PARTITION BY t.XACCOUNT ';
    v_sql := v_sql||'                           ORDER BY t.ISS_SERIAL DESC) cn,';
    v_sql := v_sql||'              T.XACCOUNT,';
    v_sql := v_sql||'              T.CARD_NBR,';
    v_sql := v_sql||'              T.ACTIVE_DAY ';
    v_sql := v_sql||'         FROM FDM_S24_CUR_CARD t ';
    v_sql := v_sql||'WHERE ROUND (MONTHS_BETWEEN(concat(toDate(T.EXPIRY_DTE + 200000, ''yyyy-mm''),''-01''),SYSDATE()),';
    v_sql := v_sql||'1) >= 0 AND cardholder = ''1'') maincard';
    v_sql := v_sql||' ON AA.XACCOUNT = MAINCARD.XACCOUNT ';
    v_sql := v_sql||' AND AA.CARD_NBR = MAINCARD.CARD_NBR ';
    v_sql := v_sql||' AND maincard.cn = 1 ';

    DBMS_OUTPUT.PUT_LINE('after:==========' || v_sql);

    execute immediate v_sql;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CARD over:==========success!');
    SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_CARD(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;