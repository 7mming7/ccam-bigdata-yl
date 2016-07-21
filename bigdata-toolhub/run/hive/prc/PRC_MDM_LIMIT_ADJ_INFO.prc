INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_LIMIT_ADJ_INFO(IN ETL_DATE STRING, OUT RESULT STRING)
BEGIN
    DECLARE v_ETL_DATE   STRING;    --ETL调度日期

    DECLARE v_sql            STRING;
    DECLARE v_sql1           STRING;
    DECLARE v_sql2           STRING;
    DECLARE v_sql3           STRING;

    SET RESULT = '0';

    DBMS_OUTPUT.PUT_LINE ('PRC_MDM_LIMIT_ADJ_INFO START');
    DBMS_OUTPUT.PUT_LINE ('ETL_DATE ==' || ETL_DATE);
    IF ETL_DATE <>' ' THEN
        SELECT toDate(ETL_DATE,'yyyymmdd') INTO v_ETL_DATE FROM DUAL;
        DBMS_OUTPUT.PUT_LINE('v_ETL_DATE2==' || v_ETL_DATE );
    ELSE
        DBMS_OUTPUT.PUT_LINE('ETL_DATE 为空');
        SELECT SYSDATE() INTO v_ETL_DATE  FROM dual;
    END IF;

    DBMS_OUTPUT.PUT_LINE ('v_ETL_DATE3 ==' || v_ETL_DATE);

    DBMS_OUTPUT.PUT_LINE ('START EXECUTE TRUNCATE TABLE MDM_LIMIT_ADJ_INFO');
    EXECUTE IMMEDIATE ('truncate table MDM_LIMIT_ADJ_INFO');

    DBMS_OUTPUT.PUT_LINE ('START EXECUTE TRUNCATE TABLE TEMP_S24_CHGS_ADJ');
    EXECUTE IMMEDIATE ('truncate table TEMP_S24_CHGS_ADJ');


    DBMS_OUTPUT.PUT_LINE('v_sql before:=========' || v_sql);
        v_sql := 'INSERT INTO TEMP_S24_CHGS_ADJ SELECT * FROM V_S24_CHGS_ADJ';
    DBMS_OUTPUT.PUT_LINE(v_sql);
    DBMS_OUTPUT.PUT_LINE('v_sql after:=========');
    EXECUTE IMMEDIATE v_sql;

    DBMS_OUTPUT.PUT_LINE('v_sql3 before:=========' || v_sql3);
        v_sql3 := v_sql3||'INSERT OVERWRITE TABLE MDM_LIMIT_ADJ_INFO_T ';
        v_sql3 := v_sql3||'SELECT T.* FROM MDM_LIMIT_ADJ_INFO_T T LEFT JOIN TEMP_S24_CHGS_ADJ P ON  ';
        v_sql3 := v_sql3||'P.XACCOUNT = T.XACCOUNT ';
        v_sql3 := v_sql3||'AND P.HISTUPDATEDTYPE = T.HISTUPDATEDTYPE ';
        v_sql3 := v_sql3||'AND P.LIMIT_ADJ_SIGN = T.LIMIT_ADJ_SIGN ';
        v_sql3 := v_sql3||'AND P.LIMIT_ADJ_TYPE = T.LIMIT_ADJ_TYPE WHERE P.XACCOUNT IS NULL ';
    DBMS_OUTPUT.PUT_LINE(v_sql3);
    DBMS_OUTPUT.PUT_LINE('v_sql3 after:=========');
    EXECUTE IMMEDIATE v_sql3;

    DBMS_OUTPUT.PUT_LINE('v_sql1 before:=========' || v_sql1);
        v_sql1 := 'INSERT INTO MDM_LIMIT_ADJ_INFO_T         ';
        v_sql1 := v_sql1||'            SELECT '' '',XACCOUNT,   ';
        v_sql1 := v_sql1||'                LIMIT_ADJ_TYPE,  ';
        v_sql1 := v_sql1||'                LIMIT_ADJ_SIGN,  ';
        v_sql1 := v_sql1||'                HISTUPDATEDTYPE, ';
        v_sql1 := v_sql1||'                MAX (ENTRY_DAY)  ';
        v_sql1 := v_sql1||'            FROM V_S24_CHGS_ADJ adj  ';
        v_sql1 := v_sql1||'            GROUP BY ADJ.XACCOUNT,   ';
        v_sql1 := v_sql1||'                ADJ.LIMIT_ADJ_SIGN,  ';
        v_sql1 := v_sql1||'                ADJ.LIMIT_ADJ_TYPE,  ';
        v_sql1 := v_sql1||'                ADJ.HISTUPDATEDTYPE  ';
    DBMS_OUTPUT.PUT_LINE(v_sql1);
    DBMS_OUTPUT.PUT_LINE('v_sql1 after:=========');
    EXECUTE IMMEDIATE v_sql1;

    DBMS_OUTPUT.PUT_LINE('v_sql2 before:=========' || v_sql2);
        v_sql2 := 'INSERT INTO MDM_LIMIT_ADJ_INFO ';
        v_sql2 := v_sql2||'          SELECT T1.BANK,     ';
        v_sql2 := v_sql2||'                 T1.XACCOUNT,     ';
        v_sql2 := v_sql2||'                 T1.CUSTR_NBR,     ';
        v_sql2 := v_sql2||'                 COAL(T3.HISTUPDATEDTYPE, ''0''),     ';
        v_sql2 := v_sql2||'                 COAL(T3.HISTUPDATEDDATE, T1.CRED_CHDAY)     ';
        v_sql2 := v_sql2||'                                                             ,     ';
        v_sql2 := v_sql2||'                 ''0''     ';
        v_sql2 := v_sql2||'                  ,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN COAL(T6.HISTUPDATEDDATE, 0) + 0 >     ';
        v_sql2 := v_sql2||'                            COAL(T8.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       COAL(T6.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                       CASE     ';
        v_sql2 := v_sql2||'                          WHEN COAL(T8.HISTUPDATEDDATE, 0) > 0     ';
        v_sql2 := v_sql2||'                          THEN     ';
        v_sql2 := v_sql2||'                             COAL(T8.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                          ELSE     ';
        v_sql2 := v_sql2||'                             T1.DAY_OPENED     ';
        v_sql2 := v_sql2||'                       END     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'                    ,     ';
        v_sql2 := v_sql2||'                 T1.TLMT_END,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN COAL(T6.HISTUPDATEDDATE, 0) + 0 >     ';
        v_sql2 := v_sql2||'                            COAL(T8.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       COAL(T6.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                       CASE     ';
        v_sql2 := v_sql2||'                          WHEN COAL(T8.HISTUPDATEDDATE, 0) > 0     ';
        v_sql2 := v_sql2||'                          THEN     ';
        v_sql2 := v_sql2||'                             COAL(T8.HISTUPDATEDDATE, 0)     ';
        v_sql2 := v_sql2||'                          ELSE     ';
        v_sql2 := v_sql2||'                             T1.DAY_OPENED     ';
        v_sql2 := v_sql2||'                       END     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'                    ,     ';
        v_sql2 := v_sql2||'                 T1.TLMT_END,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN T5.HISTUPDATEDDATE = 0     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       9999     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                    CEIL(date_diff(SYSDATE(), toDate (T5.HISTUPDATEDDATE, ''yyyy-mm-dd'') ))     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'                    ,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN T6.HISTUPDATEDDATE = 0     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       9999     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                    CEIL(date_diff(SYSDATE(), toDate (T6.HISTUPDATEDDATE, ''yyyy-mm-dd'') ))     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'                    ,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN T7.HISTUPDATEDDATE = 0     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       9999     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                    CEIL(date_diff(SYSDATE(), toDate (T7.HISTUPDATEDDATE, ''yyyy-mm-dd'') ))     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'                    ,     ';
        v_sql2 := v_sql2||'                 CASE     ';
        v_sql2 := v_sql2||'                    WHEN T8.HISTUPDATEDDATE = 0     ';
        v_sql2 := v_sql2||'                    THEN     ';
        v_sql2 := v_sql2||'                       9999     ';
        v_sql2 := v_sql2||'                    ELSE     ';
        v_sql2 := v_sql2||'                    CEIL(date_diff(SYSDATE(), toDate (T8.HISTUPDATEDDATE, ''yyyy-mm-dd'') ))     ';
        v_sql2 := v_sql2||'                 END     ';
        v_sql2 := v_sql2||'            FROM FDM_S24_CUR_ACCT T1     ';
        v_sql2 := v_sql2||'                 LEFT JOIN     ';
        v_sql2 := v_sql2||'                 (SELECT *     ';
        v_sql2 := v_sql2||'                    FROM (SELECT src.XACCOUNT,     ';
        v_sql2 := v_sql2||'                                 ROW_NUMBER ()     ';
        v_sql2 := v_sql2||'                                 OVER (PARTITION BY src.XACCOUNT     ';
        v_sql2 := v_sql2||'                                       ORDER BY (src.HISTUPDATEDTYPE + 0))     ';
        v_sql2 := v_sql2||'                                    cn,     ';
        v_sql2 := v_sql2||'                                 src.HISTUPDATEDTYPE,     ';
        v_sql2 := v_sql2||'                                 SRC.HISTUPDATEDDATE     ';
        v_sql2 := v_sql2||'                            FROM MDM_LIMIT_ADJ_INFO_T src,     ';
        v_sql2 := v_sql2||'                                 (  SELECT XACCOUNT,     ';
        v_sql2 := v_sql2||'                                           MAX (HISTUPDATEDDATE + 0) AS HISTUPDATEDDATE     ';
        v_sql2 := v_sql2||'                                      FROM MDM_LIMIT_ADJ_INFO_T     ';
        v_sql2 := v_sql2||'                                  GROUP BY XACCOUNT) m     ';
        v_sql2 := v_sql2||'                           WHERE     SRC.XACCOUNT = M.XACCOUNT     ';
        v_sql2 := v_sql2||'                                 AND SRC.HISTUPDATEDDATE = M.HISTUPDATEDDATE) TM    ';
        v_sql2 := v_sql2||'                   WHERE cn = 1) T3     ';
        v_sql2 := v_sql2||'                    ON T1.XACCOUNT = T3.XACCOUNT     ';
        v_sql2 := v_sql2||'                 LEFT JOIN MDM_LIMIT_ADJ_INFO_T T5     ';
        v_sql2 := v_sql2||'                    ON     T1.XACCOUNT = T5.XACCOUNT     ';
        v_sql2 := v_sql2||'                       AND T5.LIMIT_ADJ_SIGN = ''2''     ';
        v_sql2 := v_sql2||'                       AND T5.LIMIT_ADJ_TYPE = ''1''     ';
        v_sql2 := v_sql2||'                       AND T5.HISTUPDATEDTYPE = ''1''     ';
        v_sql2 := v_sql2||'                 LEFT JOIN MDM_LIMIT_ADJ_INFO_T T6     ';
        v_sql2 := v_sql2||'                    ON     T1.XACCOUNT = T6.XACCOUNT     ';
        v_sql2 := v_sql2||'                       AND T6.LIMIT_ADJ_SIGN = ''1''     ';
        v_sql2 := v_sql2||'                       AND T6.LIMIT_ADJ_TYPE = ''1''     ';
        v_sql2 := v_sql2||'                       AND T6.HISTUPDATEDTYPE = ''1''     ';
        v_sql2 := v_sql2||'                 LEFT JOIN MDM_LIMIT_ADJ_INFO_T T7     ';
        v_sql2 := v_sql2||'                    ON     T1.XACCOUNT = T7.XACCOUNT     ';
        v_sql2 := v_sql2||'                       AND T7.LIMIT_ADJ_SIGN = ''2''     ';
        v_sql2 := v_sql2||'                       AND T7.LIMIT_ADJ_TYPE = ''1''     ';
        v_sql2 := v_sql2||'                       AND T7.HISTUPDATEDTYPE = ''2''     ';
        v_sql2 := v_sql2||'                 LEFT JOIN MDM_LIMIT_ADJ_INFO_T T8     ';
        v_sql2 := v_sql2||'                    ON     T1.XACCOUNT = T8.XACCOUNT     ';
        v_sql2 := v_sql2||'                       AND T8.LIMIT_ADJ_SIGN = ''1''     ';
        v_sql2 := v_sql2||'                       AND T8.LIMIT_ADJ_TYPE = ''1''     ';
        v_sql2 := v_sql2||'                       AND T8.HISTUPDATEDTYPE = ''2''     ';
    DBMS_OUTPUT.PUT_LINE(v_sql2);
    DBMS_OUTPUT.PUT_LINE('v_sql2 after:=========');
    EXECUTE IMMEDIATE v_sql2;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_LIMIT_ADJ_INFO END');
    SET RESULT = '1';
END;


DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_LIMIT_ADJ_INFO(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;