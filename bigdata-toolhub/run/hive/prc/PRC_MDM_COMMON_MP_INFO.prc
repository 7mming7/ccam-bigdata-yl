INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_COMMON_MP_INFO(IN ETL_DATE STRING, OUT RESULT STRING)
BEGIN
    DECLARE v_ETL_DATE       STRING;    --ETL调度日期
    DECLARE v_ETL_DATE_MM    STRING;    --月份日期
    DECLARE v_ETL_DATE_YYYY  STRING;    --年份日期

    DECLARE v_sql            STRING;
    DECLARE v_sql1           STRING;
    DECLARE v_sql2           STRING;

    SET RESULT = '0';

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_COMMON_MP_INFO START');
    DBMS_OUTPUT.PUT_LINE('ETL_DATE ==' || ETL_DATE);
    IF ETL_DATE <>' ' THEN
        SELECT toDate(ETL_DATE,'yyyymmdd') INTO v_ETL_DATE FROM DUAL;
        DBMS_OUTPUT.PUT_LINE('v_ETL_DATE2==' || v_ETL_DATE );
    ELSE
        DBMS_OUTPUT.PUT_LINE('ETL_DATE 为空');
        SELECT SYSDATE() INTO v_ETL_DATE  FROM dual;
    END IF;

    SELECT ROUNDMONTH(v_ETL_DATE) INTO v_ETL_DATE_MM FROM DUAL;
    SELECT toDate(v_ETL_DATE, 'yyyy') INTO v_ETL_DATE_YYYY FROM DUAL;
    DBMS_OUTPUT.PUT_LINE('v_ETL_DATE3 ==' || v_ETL_DATE);
    DBMS_OUTPUT.PUT_LINE('v_ETL_DATE_MM ==' || v_ETL_DATE_MM);
    DBMS_OUTPUT.PUT_LINE('v_ETL_DATE_YYYYMM ==' || v_ETL_DATE_YYYY);

    DBMS_OUTPUT.PUT_LINE('START EXECUTE TRUNCATE TABLE TEMP_ACCT_CDATE');
    DECLARE TRUN_TBL_CDATE STRING = 'TRUNCATE TABLE TEMP_ACCT_CDATE';
    EXECUTE TRUN_TBL_CDATE;

    DBMS_OUTPUT.PUT_LINE('START INSERT INTO TEMP_ACCT_CDATE');
    DBMS_OUTPUT.PUT_LINE('v_sql before:=========' || v_sql);
        v_sql := 'INSERT INTO TEMP_ACCT_CDATE ';
        v_sql := v_sql||'    SELECT  xaccount,                                                                             ';
        v_sql := v_sql||'        CYCLE_DATE,                                                                               ';
        v_sql := v_sql||'        (substr( CYCLE_DATE ,1,4) - 1990 ) * 12 + substr( CYCLE_DATE ,5,2)                     ';
        v_sql := v_sql||'    FROM                                                                                          ';
        v_sql := v_sql||'        (                                                                                         ';
        v_sql := v_sql||'        SELECT                                                                                    ';
        v_sql := v_sql||'            xaccount,                                                                             ';
        v_sql := v_sql||'            CASE                                                                                  ';
        v_sql := v_sql||'                WHEN                                                                              ';
        v_sql := v_sql||'                    T0.CYCLE_NBR + 0 >  SUBSTR ('||v_ETL_DATE||', 7, 2)                               ';
        v_sql := v_sql||'                    AND T0.CYCLE_NBR + 0 < 10                                                         ';
        v_sql := v_sql||'                THEN                                                                              ';
        v_sql := v_sql||'                    CONCAT(toDate ( ADD_MONTHS ( toDate ('||v_ETL_DATE||', ''yyyy-mm-dd'') , -1 ) , ''yyyymm'')  ';
        v_sql := v_sql||'                    ,''0''                                                                        ';
        v_sql := v_sql||'                    ,T0.CYCLE_NBR)                                                      ';
        v_sql := v_sql||'                WHEN                                                                              ';
        v_sql := v_sql||'                    T0.CYCLE_NBR + 0 > SUBSTR ('||v_ETL_DATE||', 7, 2)                                ';
        v_sql := v_sql||'                    AND T0.CYCLE_NBR + 0 >= 10                                                        ';
        v_sql := v_sql||'                THEN                                                                              ';
        v_sql := v_sql||'                     CONCAT(toDate ( ADD_MONTHS ( toDate ('||v_ETL_DATE||', ''yyyy-mm-dd'') , -1 ) , ''yyyymm'')  ';
        v_sql := v_sql||'                    ,T0.CYCLE_NBR)                                                      ';
        v_sql := v_sql||'                WHEN                                                                              ';
        v_sql := v_sql||'                    T0.CYCLE_NBR + 0 <  SUBSTR ('||v_ETL_DATE||', 7, 2)                               ';
        v_sql := v_sql||'                    AND T0.CYCLE_NBR + 0 < 10                                                         ';
        v_sql := v_sql||'                THEN                                                                              ';
        v_sql := v_sql||'                    CONCAT(SUBSTR ('||v_ETL_DATE||', 1, 6)   ';
        v_sql := v_sql||'                    ,''0''                                                                        ';
        v_sql := v_sql||'                    ,T0.CYCLE_NBR)                                                      ';
        v_sql := v_sql||'                WHEN                                                                              ';
        v_sql := v_sql||'                    T0.CYCLE_NBR + 0 <= SUBSTR ('||v_ETL_DATE||', 7, 2)                               ';
        v_sql := v_sql||'                    AND T0.CYCLE_NBR + 0 >= 10                                                        ';
        v_sql := v_sql||'                THEN                                                                              ';
        v_sql := v_sql||'                    CONCAT(SUBSTR ('||v_ETL_DATE||', 1, 6)                                        ';
        v_sql := v_sql||'                    ,T0.CYCLE_NBR)                                                                ';
        v_sql := v_sql||'            END                                                                                   ';
        v_sql := v_sql||'            AS CYCLE_DATE from fdm_s24_cur_acct T0                                                ';
        v_sql := v_sql||'        ) T1                                                                                      ';
    DBMS_OUTPUT.PUT_LINE(v_sql);
    DBMS_OUTPUT.PUT_LINE('v_sql after:=========');
    EXECUTE IMMEDIATE v_sql;

    DBMS_OUTPUT.PUT_LINE('START EXECUTE TRUNCATE TABLE MDM_COMMON_MP_INFO');
    DECLARE TRUC_TBL_MDM_COMMON_MP_INFO STRING = 'TRUNCATE TABLE MDM_COMMON_MP_INFO';
    EXECUTE TRUC_TBL_MDM_COMMON_MP_INFO;

    DBMS_OUTPUT.PUT_LINE('START EXECUTE TRUNCATE TABLE MDM_COMMON_MP_INFO_T');
    DECLARE TRUC_TBL_MDM_COMMON_MP_INFO_T STRING = 'TRUNCATE TABLE MDM_COMMON_MP_INFO_T';
    EXECUTE TRUC_TBL_MDM_COMMON_MP_INFO_T;

    DBMS_OUTPUT.PUT_LINE('START INSERT INTO MDM_COMMON_MP_INFO_T');
    DBMS_OUTPUT.PUT_LINE('v_sql1 before:=========' || v_sql1);
        v_sql1 := 'INSERT INTO MDM_COMMON_MP_INFO_T  ';
        v_sql1 := v_sql1||'SELECT   ';
        v_sql1 := v_sql1||'    T1.XACCOUNT  ';
        v_sql1 := v_sql1||'    ,'' '' ';
        v_sql1 := v_sql1||'    ,CASE WHEN T1.MP_TYPE in (''L'',''W'') THEN ''1'' ';
        v_sql1 := v_sql1||'         ELSE ''2'' ';
        v_sql1 := v_sql1||'     END                             ';
        v_sql1 := v_sql1||'    ,MAX(T1.NBR_MTHS + 0)        ';
        v_sql1 := v_sql1||'    ,MAX(COAL(T2.INP_SRC,''T''))      ';
        v_sql1 := v_sql1||'    ,''0'' ';
        v_sql1 := v_sql1||'    ,SUM(T1.REM_PPL + 0)     ';
        v_sql1 := v_sql1||'    ,SUM(T1.REM_FEE + 0)     ';
        v_sql1 := v_sql1||'    ,''0'' ';
        v_sql1 := v_sql1||'FROM FDM_S24_CUR_MPUR T1     ';
        v_sql1 := v_sql1||'LEFT JOIN FDM_S24_MPAU T2        ';
        v_sql1 := v_sql1||'ON  T1.CARD_NBR = T2.CARD_NBR        ';
        v_sql1 := v_sql1||'AND T1.APP_SDAY = T2.APP_SDAY        ';
        v_sql1 := v_sql1||'AND T1.APP_SEQ = T2.APP_SEQ      ';
        v_sql1 := v_sql1||'WHERE T1.STATUS IN (''N'',''A'',''R'',''X'') ';
        v_sql1 := v_sql1||'GROUP BY T1.XACCOUNT     ';
        v_sql1 := v_sql1||'         ,CASE WHEN T1.MP_TYPE in (''L'',''W'') THEN ''1'' ';
        v_sql1 := v_sql1||'          ELSE ''2'' ';
        v_sql1 := v_sql1||'          END';
    DBMS_OUTPUT.PUT_LINE(v_sql1);
    DBMS_OUTPUT.PUT_LINE('v_sql1 after:=========');
    EXECUTE IMMEDIATE v_sql1;

    DBMS_OUTPUT.PUT_LINE('START INSERT INTO MDM_COMMON_MP_INFO');
    DBMS_OUTPUT.PUT_LINE('v_sql2 before:=========' || v_sql2);
        v_sql2 := 'INSERT INTO MDM_COMMON_MP_INFO  ';
        v_sql2 := v_sql2||'            SELECT           ';
        v_sql2 := v_sql2||'                T0.XACCOUNT          ';
        v_sql2 := v_sql2||'                ,T0.CUSTR_NBR            ';
        v_sql2 := v_sql2||'                ,COAL(T1.PRODUCT,0)            ';
        v_sql2 := v_sql2||'                ,COAL(T1.TERMS + 0,0)            ';
        v_sql2 := v_sql2||'                ,COAL(T1.APP_CHANNEL,''T'')            ';
        v_sql2 := v_sql2||'                ,''0''           ';
        v_sql2 := v_sql2||'                ,COAL(T1.LINS_REM_UNSH_PRINCIPAL,0)          ';
        v_sql2 := v_sql2||'                ,COAL(T1.LINS_REM_UNSH_FEE,0)            ';
        v_sql2 := v_sql2||'                ,COAL(T7.BAL_MP,0) + COAL(T9.BAL_MP,0)*6         ';
        v_sql2 := v_sql2||'            FROM (SELECT AA.*,BB.CLOSE_CYCLE_NBR FROM FDM_S24_CUR_ACCT AA, TEMP_ACCT_CDATE BB            ';
        v_sql2 := v_sql2||'              WHERE  AA.XACCOUNT = BB.XACCOUNT  ) T0 ';
        v_sql2 := v_sql2||'            LEFT JOIN MDM_COMMON_MP_INFO_T T1    ';
        v_sql2 := v_sql2||'              ON T0.XACCOUNT = T1.XACCOUNT   ';
        v_sql2 := v_sql2||'             AND T1.PRODUCT = 2  ';
        v_sql2 := v_sql2||'            LEFT JOIN FDM_S24_STMT T7    ';
        v_sql2 := v_sql2||'            ON  T0.XACCOUNT = T7.XACCOUNT    ';
        v_sql2 := v_sql2||'        AND (T7.MONTH_NBR + 0) = T0.CLOSE_CYCLE_NBR    ';
        v_sql2 := v_sql2||'            AND T1.PRODUCT = 2   ';
        v_sql2 := v_sql2||'            LEFT JOIN FDM_S24_STMX T9    ';
        v_sql2 := v_sql2||'            ON  T0.XACCOUNT = T9.XACCOUNT    ';
        v_sql2 := v_sql2||'        AND (T9.MONTH_NBR + 0) = T0.CLOSE_CYCLE_NBR    ';
        v_sql2 := v_sql2||'            AND T1.PRODUCT = 2  ';
    DBMS_OUTPUT.PUT_LINE(v_sql2);
    DBMS_OUTPUT.PUT_LINE('v_sql2 after:=========');
    EXECUTE IMMEDIATE v_sql2;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_COMMON_MP_INFO END');
    SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_COMMON_MP_INFO(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;