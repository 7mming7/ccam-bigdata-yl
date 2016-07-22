INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_CARD_CUSTR_INFO(IN ETL_DATE STRING, OUT RESULT STRING)
BEGIN
    DECLARE v_ETL_DATE       STRING;    --ETL调度日期

    DECLARE v_sql            STRING;

    SET RESULT = '0';

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CARD_CUSTR_INFO START');
    DBMS_OUTPUT.PUT_LINE('ETL_DATE ==' || ETL_DATE);
    IF ETL_DATE <>' ' THEN
        SELECT toDate(ETL_DATE,'yyyymmdd') INTO v_ETL_DATE FROM DUAL;
        DBMS_OUTPUT.PUT_LINE('v_ETL_DATE2==' || v_ETL_DATE );
    ELSE
        DBMS_OUTPUT.PUT_LINE('ETL_DATE 为空');
        SELECT SYSDATE() INTO v_ETL_DATE  FROM dual;
    END IF;

    --删除批次已经加载的数据 MDM_CARD_CUSTR_INFO
    DBMS_OUTPUT.PUT_LINE('START EXECUTE TRUNCATE TABLE MDM_CARD_CUSTR_INFO');
    DECLARE TRUN_TBL_MDM_CARD_CUSTR_INFO STRING = 'TRUNCATE TABLE MDM_CARD_CUSTR_INFO';
    EXECUTE TRUN_TBL_MDM_CARD_CUSTR_INFO;

    DBMS_OUTPUT.PUT_LINE('START INSERT INTO MDM_CARD_CUSTR_INFO');
    DBMS_OUTPUT.PUT_LINE('v_sql before:=========' || v_sql);
    v_sql := 'INSERT INTO MDM_CARD_CUSTR_INFO  ';
    v_sql := v_sql||'      SELECT T0.BANK, ';
    v_sql := v_sql||'             T0.XACCOUNT, ';
    v_sql := v_sql||'             T0.CUSTR_NBR, ';
    v_sql := v_sql||'             COAL(T1.CARD_NBR, '' ''), ';
    v_sql := v_sql||'             COAL(T3.CARD_NBR, '' ''), ';
    v_sql := v_sql||'             COAL(T2.CARDHOLDER, 0), ';
    v_sql := v_sql||'             COAL(T4.DAY_BIRTH, 0), ';
    v_sql := v_sql||'             CASE WHEN T3.CARD_NBR IS NOT NULL THEN ''1'' ELSE '' '' END, ';
    v_sql := v_sql||'             COAL(T5.ADDR_LINE1, '' ''), ';
    v_sql := v_sql||'             t1.ACTIVE_DAY, ';
    v_sql := v_sql||'             t6.MO_PHONE ';
    v_sql := v_sql||'        FROM MDM_ACCT_X T0 ';
    v_sql := v_sql||'             LEFT JOIN mdm_card T1 ON T0.XACCOUNT = T1.XACCOUNT ';
    v_sql := v_sql||'             LEFT JOIN ';
    v_sql := v_sql||'             (  SELECT C0.XACCOUNT, C0.CARDHOLDER, MAX (CARD_NBR + 0) AS CARD_NBR ';
    v_sql := v_sql||'                  FROM V_FDM_S24_CUR_CARD_COM C0 ';
    v_sql := v_sql||'                       INNER JOIN ';
    v_sql := v_sql||'                       (  SELECT XACCOUNT, MAX (CARDHOLDER + 0) AS CARDHOLDER ';
    v_sql := v_sql||'                            FROM V_FDM_S24_CUR_CARD_COM cc                   ';
    v_sql := v_sql||'                           WHERE coal(CARDHOLDER,'' '') <> ''1'' ';
    v_sql := v_sql||'                        GROUP BY XACCOUNT) C1 ';
    v_sql := v_sql||'                          ON     C0.XACCOUNT = C1.XACCOUNT ';
    v_sql := v_sql||'                             AND C0.CARDHOLDER = C1.CARDHOLDER ';
    v_sql := v_sql||'              GROUP BY C0.XACCOUNT, C0.CARDHOLDER) T2 ';
    v_sql := v_sql||'                ON T0.XACCOUNT = T2.XACCOUNT ';
    v_sql := v_sql||'             LEFT JOIN V_FDM_S24_CUR_CARD_COM T3                             ';
    v_sql := v_sql||'                ON     T0.XACCOUNT = T3.XACCOUNT ';
    v_sql := v_sql||'                   AND T2.CARDHOLDER = T3.CARDHOLDER ';
    v_sql := v_sql||'                   AND T2.CARD_NBR = T3.CARD_NBR ';
    v_sql := v_sql||'                   AND T3.CANCL_CODE NOT IN (''O'', ';
    v_sql := v_sql||'                                             ''Q'', ';
    v_sql := v_sql||'                                             ''T'', ';
    v_sql := v_sql||'                                             ''Y'', ';
    v_sql := v_sql||'                                             ''L'', ';
    v_sql := v_sql||'                                             ''N'', ';
    v_sql := v_sql||'                                             ''P'', ';
    v_sql := v_sql||'                                             ''S'')    AND COAL(T3.CANCL_CODE,'' '') !='' '' ';
    v_sql := v_sql||'             LEFT JOIN FDM_S24_CUR_CUSTR T4                              ';
    v_sql := v_sql||'                                       ON T3.CUSTR_NBR = T4.CUSTR_NBR ';
    v_sql := v_sql||'             LEFT JOIN FDM_S24_CUR_ADDR T5                           ';
    v_sql := v_sql||'                ON T0.CUSTR_NBR = T5.CUSTR_NBR AND T5.ADDR_TYPE = ''B''  ';
    v_sql := v_sql||'             LEFT JOIN FDM_S24_CUR_CUSTR T6                              ';
    v_sql := v_sql||'                                           ON T0.CUSTR_NBR = T6.CUSTR_NBR ';
    DBMS_OUTPUT.PUT_LINE(v_sql);
    DBMS_OUTPUT.PUT_LINE('v_sql after:=========');
    EXECUTE IMMEDIATE v_sql;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CARD_CUSTR_INFO END');
    SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_CARD_CUSTR_INFO(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;