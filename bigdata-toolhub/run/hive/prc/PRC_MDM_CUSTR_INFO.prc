INCLUDE INCLUDE_HEAD.sql

CREATE OR REPLACE PROCEDURE PRC_MDM_CUSTR_INFO(IN ETL_DATE STRING, OUT RESULT STRING)

BEGIN
    DECLARE   v_sql         string;
    DECLARE   v_ETL_DATE    string;

    DBMS_OUTPUT.PUT_LINE ('PRC_MDM_Custr_info START');
    DBMS_OUTPUT.PUT_LINE ('ETL_DATE ==' || ETL_DATE);

       DBMS_OUTPUT.PUT_LINE ('v_ETL_DATE3 ==' || v_ETL_DATE);

       --删除变动的数据
       EXECUTE IMMEDIATE ('truncate table  MDM_CUSTR_INFO');

       DBMS_OUTPUT.PUT_LINE('before:=========' || v_sql);

    --汇总客户卡片信息数据

v_sql := ' INSERT INTO MDM_CUSTR_INFO ';
v_sql := v_sql||' SELECT ';
v_sql := v_sql||'     T0.BANK ';
v_sql := v_sql||'     ,T0.XACCOUNT ';
v_sql := v_sql||'     ,T0.CUSTR_NBR ';
v_sql := v_sql||'     ,COAL(SUBSTR(T1.DAY_BIRTH,1,6),190001) ';
v_sql := v_sql||'     ,COAL(T1.DAY_BIRTH,19000101) ';
v_sql := v_sql||'     ,COAL(T1.OCC_CATGRY,'' '') ';
v_sql := v_sql||'     ,''0'' ';
v_sql := v_sql||'     ,CASE WHEN T4.COUNT > 0 THEN ''1''  ELSE ''0'' END ';
v_sql := v_sql||'     ,COAL(T1.MAR_STATUS,''O'') ';
v_sql := v_sql||'     ,COAL(T1.EDUCA,'' '') ';
v_sql := v_sql||'     ,T0.CUSTR_NBR ';
v_sql := v_sql||'     ,COAL(T1.NATION,1) ';
v_sql := v_sql||'     ,COAL(T1.OCC_CODE,''16'') ';
v_sql := v_sql||'     ,CASE WHEN T1.COMP_NAME LIKE ''%钢贸%'' ';
v_sql := v_sql||'            OR T1.COMP_NAME LIKE ''%船舶%'' ';
v_sql := v_sql||'            OR T1.COMP_NAME LIKE ''%光伏%'' ';
v_sql := v_sql||'            OR T1.COMP_NAME LIKE ''%尚德%'' ';
v_sql := v_sql||'            OR T1.COMP_NAME LIKE ''%物资贸易%'' ';
v_sql := v_sql||'            OR SUBSTR(T0.CUSTR_NBR,1,3) = ''352'' ';
v_sql := v_sql||'               THEN ''1'' ';
v_sql := v_sql||'           ELSE ''0'' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,COAL(T1.OCC_CODE,''16'') ';
v_sql := v_sql||'     ,CASE WHEN T6.COUNT > 0 THEN ''1'' ';
v_sql := v_sql||'           ELSE ''0'' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,''0'' ';
v_sql := v_sql||'     ,COAL(T1.INCOME_ANN,0) ';
v_sql := v_sql||'     ,COAL(T1.INCOME_ANN,0) ';
v_sql := v_sql||'     ,CASE WHEN COAL(T11.GUARN_AMT,0) > 0 THEN ''1'' ELSE ''0'' END ';
v_sql := v_sql||'     ,CASE WHEN T7.GUARN_ID IS NOT NULL THEN ''1'' ';
v_sql := v_sql||'           ELSE ''0'' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,T0.CUSTR_NBR ';
v_sql := v_sql||'     ,COAL(T1.INCOME_ANN,0) ';
v_sql := v_sql||'     ,COAL(T8.ADDR_LINE1,'' '') ';
v_sql := v_sql||'     ,''0'' ';
v_sql := v_sql||'     ,CASE WHEN T9.HOME_CODE IN (''1'',''3'') THEN T9.PROP_AREA ';
v_sql := v_sql||'           ELSE 0 ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,CASE WHEN T1.NATION = ''1'' THEN ''0'' ';
v_sql := v_sql||'           ELSE ''1'' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,COAL(T1.GENDER,''M'') ';
v_sql := v_sql||'     ,CASE WHEN COAL(T10.AMT_INVLVD,0) > 0 THEN ''1'' ';
v_sql := v_sql||'           ELSE ''0'' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,COAL(T11.GUARN_AMT,0) ';
v_sql := v_sql||'     ,CASE WHEN COAL(T11.GUARN_AMT,0) > 0 THEN ''1'' ';
v_sql := v_sql||'           ELSE '' '' ';
v_sql := v_sql||'           END ';
v_sql := v_sql||'     ,COAL(T9.HOME_CODE,''6'') ';
v_sql := v_sql||'     ,T1.CRED_LIMIT ';
v_sql := v_sql||'     ,ceil (datediff(sysdate() ,todate(T12.notfy_send_time,''yyyy-mm-dd''))) ';
v_sql := v_sql||'     ,ceil (datediff(sysdate() ,todate(T13.notfy_send_time,''yyyy-mm-dd''))) ';
v_sql := v_sql||' FROM MDM_ACCT_X       T0 ';
v_sql := v_sql||' LEFT JOIN FDM_S24_CUR_CUSTR T1 ';
v_sql := v_sql||'   ON  T0.CUSTR_NBR  = T1.CUSTR_NBR ';
v_sql := v_sql||' LEFT JOIN (SELECT ';
v_sql := v_sql||'                   CUSTR_NBR ';
v_sql := v_sql||'                   ,COUNT(*) AS COUNT ';
v_sql := v_sql||'             FROM FDM_S24_CUR_CUNEG ';
v_sql := v_sql||'             WHERE STATUS <> ''D'' GROUP BY CUSTR_NBR ) T4 ';
v_sql := v_sql||'   ON  T0.CUSTR_NBR  = T4.CUSTR_NBR ';
v_sql := v_sql||' LEFT JOIN (SELECT ';
v_sql := v_sql||'                   ARTI_ID ';
v_sql := v_sql||'                   ,COUNT(*) AS COUNT ';
v_sql := v_sql||'             FROM FDM_S24_CUR_BUAPP ';
v_sql := v_sql||'             GROUP BY BANK,ARTI_ID) T6 ';
v_sql := v_sql||'   ON  T0.CUSTR_NBR  = T6.ARTI_ID ';
v_sql := v_sql||' LEFT JOIN (SELECT T01.BANK      AS BANK ';
v_sql := v_sql||'                   ,T01.GUARN_ID AS GUARN_ID ';
v_sql := v_sql||'              FROM FDM_S24_CUR_GUAR T01 ';
v_sql := v_sql||'             INNER JOIN FDM_S24_CUR_GUAR T02 ';
v_sql := v_sql||'                ON  T01.GUARN_ID = T01.CUSTR_ID ) T7 ';
v_sql := v_sql||'   ON  T0.CUSTR_NBR = T7.GUARN_ID ';
v_sql := v_sql||' LEFT JOIN FDM_S24_CUR_ADDR T8 ';
v_sql := v_sql||' ON  T0.CUSTR_NBR  = T8.CUSTR_NBR ';
v_sql := v_sql||' AND T8.ADDR_TYPE = ''F'' ';
v_sql := v_sql||' LEFT JOIN FDM_S24_CUR_CUSP T9 ';
v_sql := v_sql||' ON  T0.CUSTR_NBR  = T9.CUSTR_NBR ';
v_sql := v_sql||' LEFT JOIN (SELECT BANK ';
v_sql := v_sql||'                   ,CUSTR_NBR ';
v_sql := v_sql||'                   ,SUM(AMT_INVLVD + 0) AMT_INVLVD ';
v_sql := v_sql||'              FROM FDM_S24_NOTES ';
v_sql := v_sql||'              GROUP BY BANK,CUSTR_NBR  ) T10 ';
v_sql := v_sql||'  ON T0.CUSTR_NBR  = T10.CUSTR_NBR ';
v_sql := v_sql||' LEFT JOIN (SELECT ';
v_sql := v_sql||'                   CUSTR_ID ';
v_sql := v_sql||'                   ,SUM(GUARN_AMT + 0) GUARN_AMT ';
v_sql := v_sql||'              FROM FDM_S24_CUR_GUAR ';
v_sql := v_sql||'              WHERE GUARN_CODE = ''G'' ';
v_sql := v_sql||'              GROUP BY CUSTR_ID  ) T11 ';
v_sql := v_sql||'  ON  T0.CUSTR_NBR  = T11.CUSTR_ID ';
v_sql := v_sql||'  LEFT JOIN (select ta.custr_nbr, ta.bank_id,  max(ta.notfy_send_time + 0) as notfy_send_time ';
v_sql := v_sql||'             from adjust_send_record_t_h ta,tbl_banks tb where ta.bank_id = tb.bankid ';
v_sql := v_sql||'                  and  ta.adjust_type = ''1'' group by ta.custr_nbr ,ta.bank_id   ) T12 ';
v_sql := v_sql||'  ON T0.CUSTR_NBR = T12.custr_nbr ';
v_sql := v_sql||'  LEFT JOIN (select ta.custr_nbr, ta.bank_id,  max(ta.notfy_send_time + 0) as notfy_send_time ';
v_sql := v_sql||'             from adjust_send_record_t_h ta,tbl_banks tb where ta.bank_id = tb.bankid ';
v_sql := v_sql||'                  and  ta.adjust_type = ''2'' group by ta.custr_nbr ,ta.bank_id   ) T13 ';
v_sql := v_sql||'  ON T0.CUSTR_NBR = T13.custr_nbr ';

    DBMS_OUTPUT.PUT_LINE('after:==========' || v_sql);

    execute immediate v_sql;

    DBMS_OUTPUT.PUT_LINE('PRC_MDM_CUSTR_INFO END');

    SET RESULT = '1';
END;

DECLARE RESULT_OUT STRING;
DECLARE ETL_DATE STRING = '2015-12-14 00:00:00';
call PRC_MDM_CUSTR_INFO(ETL_DATE, RESULT_OUT);
PRING RESULT_OUT;
